"""vct_pilot — declared NE layer + in-RAM building footprints from the
public BEV vector tiles (vtcseamless ``bevdirect-serve``).  See
docs/vct-pilot.md.

Gated by ``config.BEV_VECTOR_CONTEXT`` (default off).  When on, the
processor's NE step builds the KG's *declared* NE layer (NEC1, k-anonymous
per-cell land-use statistics) locally with the pinned ``ne_cells`` package
from one ``/viewport`` per aligned 0.02° cell of the parent bbox, and the
same documents provide the building footprints the segmentation model uses
as ``building_mask`` context.

Rules (the VTC posture — vtcseamless README → Compliance posture):

* **Property-level data only in memory.**  bevdirect-serve keeps raw BEV
  tiles in an in-process RAM LRU (``-tile-cache-mb``, ``-tile-ttl 24h``; no
  .pbf is ever written) and the assembled cells in RAM (``-ttl 6h``); the
  viewport documents are parsed in-process into the aggregated NEC1 cells
  and dropped; the footprints reach the KG subprocess over an anonymous
  pipe and live only in that process.  Nothing property-shaped is written
  anywhere; only counts / digests / bytes land in
  ``data/austria_processor/vct_pilot.jsonl``.
* **Aligned cells, never a free viewport.**  One ``/viewport`` per aligned
  0.02° cell (``ix = floor(lon/0.02)``) intersecting the parent's bbox
  (+0.004°); ``-prefetch 0`` — no background ring warming.
* **Never interrupts the pipeline.**  Runs as a bounded helper subprocess
  (``VCT_PILOT_TIMEOUT_S``); a failure is a WARNING line + a jsonl row, and
  the KG continues without NE cells / building context.
"""
from __future__ import annotations

import json
import logging
import math
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import config

log = logging.getLogger("vct_pilot")

DATA_DIR = Path(os.environ.get("NE_DATA_DIR") or (Path(__file__).resolve().parent / "data"))
AP_DIR = DATA_DIR / "austria_processor"
LOG_FILE = AP_DIR / "vct_pilot.jsonl"          # ring of per-KG rows (counts/digests only)
LOG_KEEP = 200
REQ_PAD = 0.004                                 # == bevdirect cellPad: neighbours inside ne_cells' input pad
DEDUPE_S = 24 * 3600                            # one run per parent per day
TIMEOUT_S = int(os.environ.get("VCT_PILOT_TIMEOUT_S", "1500"))
TILE_CELLS = int(os.environ.get("VCT_TILE_CELLS", "300000"))   # est. product cells above which the NEC1 build is tiled (ne_build_tiled)
# QC: a build must come from ONE tile generation.  Every cell document must
# have been assembled (a) after the last BEV regeneration window AND (b) less
# than DOC_MAX_AGE_S ago, AND (c) no tile (z,x,y) may carry two different
# sha256 across the documents.  A violation restarts the server (RAM caches
# gone) and refetches once more.
DOC_MAX_AGE_S = int(os.environ.get("VCT_DOC_MAX_AGE_S", str(6 * 3600)))
INCOMPLETE_REFETCH = 2                          # re-request cells with tiles_fetched < tiles_expected this many times


# === SECTION: switches ===

def enabled() -> bool:
    """``config.BEV_VECTOR_CONTEXT`` (env ``SRTM_BEV_VECTOR_CONTEXT``)."""
    return bool(config.BEV_VECTOR_CONTEXT)


def set_enabled(on: bool) -> bool:
    """Kept for call compatibility: the switch is the environment flag
    ``SRTM_BEV_VECTOR_CONTEXT``; nothing is written.  Returns False."""
    if bool(on) != enabled():
        log.info("vct_pilot: set_enabled(%s) ignored — set SRTM_BEV_VECTOR_CONTEXT in the environment", on)
    return False


def _observer() -> str:
    import socket
    return f"observe-{socket.gethostname()}"[:64]


# === SECTION: jsonl ring ===

_lock = threading.Lock()

def rows(limit: int = LOG_KEEP) -> list[dict]:
    try:
        out = []
        for ln in LOG_FILE.read_text().splitlines()[-limit:]:
            try:
                out.append(json.loads(ln))
            except Exception:
                continue
        return out
    except FileNotFoundError:
        return []
    except Exception:
        return []


def record(row: dict) -> None:
    with _lock:
        try:
            LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
            cur = rows()
            cur.append(row)
            cur = cur[-LOG_KEEP:]
            tmp = LOG_FILE.with_suffix(".tmp")
            tmp.write_text("".join(json.dumps(r, separators=(",", ":"), default=str) + "\n" for r in cur))
            tmp.replace(LOG_FILE)
        except Exception as e:  # noqa: BLE001
            log.warning("vct_pilot record: %s", e)


def recent_for(parent: str, within_s: float = DEDUPE_S) -> dict | None:
    now = time.time()
    for r in reversed(rows()):
        if r.get("kg") == parent and now - float(r.get("ts") or 0) < within_s:
            return r
    return None


# === SECTION: the run (helper subprocess) ===

CELL_DEG = 0.02                                 # bevdirect-serve's assembly cell (aligned to 0)


def _cells(W: float, S: float, E: float, N: float) -> list[tuple[int, int]]:
    """``(ix, iy)`` of every aligned 0.02° bevdirect cell intersecting W,S,E,N
    (``ix = floor(lon/0.02 + 1e-9)``; a bbox ending exactly on a grid line
    does not include the next cell) — ``vtcseamless.bevdirect.cells_for``
    when the client is installed, the same rule inline otherwise."""
    try:
        from vtcseamless.bevdirect import cells_for
        return list(cells_for(W, S, E, N))
    except Exception:
        # +1e-9 cell units on the lower corner (vtcseamless 0.3.2 / server
        # v0.3.6): 15.54/0.02 = 776.999… would otherwise fold the previous
        # column in for ~7 % of grid lines
        ix0, ix1 = math.floor(W / CELL_DEG + 1e-9), math.floor((E - 1e-9) / CELL_DEG)
        iy0, iy1 = math.floor(S / CELL_DEG + 1e-9), math.floor((N - 1e-9) / CELL_DEG)
        return [(ix, iy) for ix in range(ix0, ix1 + 1) for iy in range(iy0, iy1 + 1)]


def _cell_bbox(ix: int, iy: int) -> tuple:
    """The UNPADDED aligned cell (west=ix·0.02, south=iy·0.02, +0.02) — asking
    ``/viewport`` for exactly this box makes the server compose one cached cell."""
    return (ix * CELL_DEG, iy * CELL_DEG, (ix + 1) * CELL_DEG, (iy + 1) * CELL_DEG)


def _grid(W: float, S: float, E: float, N: float) -> list[tuple]:
    """The aligned 0.02° bevdirect cells intersecting W,S,E,N — one
    ``/viewport`` per cell, exactly like the ``vtcseamless`` reference
    build.  A multi-cell ``/viewport`` keeps ONE truncated copy of any
    polygon wider than cell + pad, so a stitched viewport never equals the
    aligned-cell build; per cell the server assembles the same tiles, so
    the BEV traffic is identical and the requests are local."""
    return [_cell_bbox(ix, iy) for ix, iy in _cells(W, S, E, N)]


def cells_union_bbox(cells: list[tuple[int, int]]) -> tuple:
    """W,S,E,N of the union of aligned cells — the report's ``input_bbox``.
    Stable per cell block, so the operator's coverage rule can compare
    reports of the same block (identical bbox → most ``cells_n`` wins)."""
    xs = [c[0] for c in cells]; ys = [c[1] for c in cells]
    return (round(min(xs) * CELL_DEG, 6), round(min(ys) * CELL_DEG, 6),
            round((max(xs) + 1) * CELL_DEG, 6), round((max(ys) + 1) * CELL_DEG, 6))


def covers_full_cell(W: float, S: float, E: float, N: float) -> bool:
    """True when the viewport fully contains at least one aligned cell.  A
    build from a smaller viewport is a partial build of its cells and is
    flagged ``partial_viewport``."""
    ix0, iy0 = math.ceil(W / CELL_DEG - 1e-9), math.ceil(S / CELL_DEG - 1e-9)
    return (ix0 + 1) * CELL_DEG <= E + 1e-9 and (iy0 + 1) * CELL_DEG <= N + 1e-9


class UpstreamDownError(RuntimeError):
    """bevdirect-serve (v0.3.5+) reports ``upstream.state == "down"``: BEV
    itself is unreachable (TLS / 5xx).  Polling further cannot help — the KG
    fails fast with the server's ``last_error`` so the fleet log says *why*
    (2026-10-23 BEV TLS intermediate outage).  vtcseamless-py 0.3.1 raises
    the same-named error."""


def _upstream_of(r) -> dict:
    """``upstream`` object from a /viewport body (502 / pending / ready all
    carry it since bevdirect-serve v0.3.5; older servers → ``{}``)."""
    try:
        j = r.json()
        return j.get("upstream") if isinstance(j, dict) and isinstance(j.get("upstream"), dict) else {}
    except Exception:
        return {}


def _viewport(box: tuple, deadline: float) -> dict:
    import requests
    import ne_h3
    W, S, E, N = box
    params = {"west": f"{W:.6f}", "south": f"{S:.6f}", "east": f"{E:.6f}", "north": f"{N:.6f}",
              "layers": "parcels,footprints,landuse", "wait": "60"}
    while True:
        r = requests.get(f"http://{ne_h3.BEVDIRECT_ADDR}/viewport", params=params, timeout=180)
        up = _upstream_of(r)
        # v0.3.5 contract: pending/502 + upstream.ok → cold cells, keep polling;
        # upstream.state == "down" → BEV unreachable, stop polling, alert.
        if up.get("state") == "down":
            raise UpstreamDownError(
                f"BEV upstream down (tls_errors={up.get('tls_errors')}, http_5xx={up.get('http_5xx')}): "
                f"{up.get('last_error') or 'no detail'}")
        if r.status_code == 502 and time.time() < deadline:
            time.sleep(5)
            continue
        r.raise_for_status()
        doc = r.json()
        if doc.get("ready", True) and not doc.get("pending"):
            return doc
        if time.time() > deadline:
            raise RuntimeError(f"viewport still pending at deadline (upstream={up.get('state', 'unknown')})")
        time.sleep(float(doc.get("retry_after_s") or 5))



# === SECTION: building footprints for the KG pipeline ===

def footprints_from_docs(docs: list) -> dict:
    """Building footprints from the SAME viewport documents the NE build
    consumes: no second fetch, and the dict never touches a file system —
    the helper streams it to the KG subprocess over an anonymous pipe
    (``--context-fd``).  EVERY footprint of the padded bbox (own KG and
    neighbours), deduped by ``id``; geometry only — no owner-identifying
    attribute is carried.  The processor rasterises them into the per-tile
    ``building_mask`` and drops them."""
    fps: dict = {}
    for d in docs:
        for f in d.get("footprints") or []:
            if not f.get("geometry"):
                continue
            fid = str(f.get("id") or "")
            cur = fps.get(fid)
            if cur is None or float(f.get("area_sqm") or 0) > float(cur.get("area_sqm") or 0):
                fps[fid] = {"id": fid, "area_sqm": f.get("area_sqm"), "geometry": f["geometry"]}
    return {"footprints": list(fps.values()), "stats": {"footprints_bbox": len(fps)}}


def _context_to_fd(ctx: dict, fd: int) -> int:
    """gzip-JSON the context into an inherited pipe (RAM only — never a
    file, not even tmpfs); returns bytes written."""
    import gzip
    blob = gzip.compress(json.dumps(ctx, separators=(",", ":")).encode(), compresslevel=1)
    view = memoryview(blob)
    while view:
        n = os.write(fd, view)
        view = view[n:]
    os.close(fd)
    return len(blob)


def _context_from_fd(fd: int, out: dict) -> None:
    """Reader side (thread): drains the pipe until EOF → ``out['ctx']``."""
    import gzip
    chunks = []
    try:
        while True:
            b = os.read(fd, 1 << 20)
            if not b:
                break
            chunks.append(b)
        if chunks:
            out["ctx"] = json.loads(gzip.decompress(b"".join(chunks)))
    except Exception as e:  # noqa: BLE001
        out["error"] = str(e)[:200]
    finally:
        try:
            os.close(fd)
        except Exception:
            pass


def _fetched_after(iso, t: float) -> bool:
    """True when the document's ``fetched_at`` is newer than *t* (or unparseable)."""
    if not iso:
        return True
    try:
        from datetime import datetime
        s = str(iso).replace("Z", "+00:00")
        if "." in s:   # Go emits 9 fractional digits; Python takes 6
            head, _, tail = s.partition(".")
            frac = "".join(ch for ch in tail if ch.isdigit())[:6]
            tz = tail[len("".join(ch for ch in tail if ch.isdigit())):]
            s = f"{head}.{frac}{tz}"
        return datetime.fromisoformat(s).timestamp() >= t
    except Exception:
        return True


def _tile_conflicts(docs: list) -> int:
    """Number of (z,x,y) tiles that appear with ≠ sha256 in two documents of
    one build — i.e. two tile generations mixed (v0.3.4 ``tiles[]``)."""
    seen: dict = {}
    bad = 0
    for d in docs:
        for t in d.get("tiles") or []:
            try:
                k = (int(t["z"]), int(t["x"]), int(t["y"]))
            except Exception:
                continue
            h = str(t.get("sha256") or "")
            if k in seen and seen[k] != h:
                bad += 1
            seen.setdefault(k, h)
    return bad


def _doc_incomplete(d: dict) -> bool:
    st = d.get("stats") or {}
    return int(st.get("tiles_expected") or 0) > int(st.get("tiles_fetched") or 0)


def _prov_view(docs: list) -> dict:
    """The provenance-only view of the documents (``tiles[]`` + the two tile
    counts) that outlives ``del docs`` — ~50 B per tile, no geometry."""
    out = {}
    for i, d in enumerate(docs):
        st = d.get("stats") or {}
        out[i] = {"tiles": d.get("tiles"),
                  "stats": {"tiles_expected": st.get("tiles_expected"), "tiles_fetched": st.get("tiles_fetched")}}
    return out




def run(kg: str, bbox: tuple, observer: str | None = None, timeout_s: int = TIMEOUT_S,
        out_nec: "str | Path | None" = None, context_fd: int | None = None,
        context_only: bool = False) -> dict:
    """One parent KG, in-process.  Returns the telemetry row; errors land in
    ``row['error']``.  ``out_nec`` → the NEC1 section (statistics only —
    nothing property-shaped ever leaves RAM) is written there atomically;
    this is how the product gets its declared layer (``build_declared``).
    ``context_fd`` → the building footprints are streamed into that pipe."""
    import ne_h3
    t0 = time.time()
    deadline = t0 + timeout_s
    kg = str(kg).zfill(5)
    row: dict = {"ts": round(t0, 1), "kg": kg, "observer": observer or _observer(), "ok": False,
                 "bbox_in": [round(float(v), 6) for v in bbox]}
    try:
        from ne_cells import canon
        from ne_cells.algo import ALGO, H3_RES, K_RES, K, INPUT_PAD_DEG, INPUT_RULE, build_cells
        from ne_cells.pack import MAGIC_LU, lu_header, pack_records, pack_section, unpack_sections
        ne_h3._ensure_bevdirect_server()
        W, S, E, N = [float(v) for v in bbox]
        cells = _cells(W - REQ_PAD, S - REQ_PAD, E + REQ_PAD, N + REQ_PAD)
        boxes = [_cell_bbox(ix, iy) for ix, iy in cells]
        input_bbox = cells_union_bbox(cells)
        row["requests"] = len(boxes)
        row["input_bbox"] = list(input_bbox)
        row["cell_block"] = [min(c[0] for c in cells), min(c[1] for c in cells),
                             max(c[0] for c in cells), max(c[1] for c in cells)]
        if not covers_full_cell(*input_bbox):
            row["partial_viewport"] = True

        # --- fetch: one tile epoch only.  No fetch inside the nightly BEV
        # regeneration window; every document must have been assembled after
        # the last regeneration; a fetch that crossed into the window is
        # dropped (server restart → RAM caches gone) and redone once.
        def _fetch_all():
            docs, st = [], {"tiles_z15": 0, "tiles_z16": 0, "tile_errors": 0, "cache_hits": 0, "bytes": 0}
            net = cached_docs = 0
            t1 = time.time()
            epoch_end = ne_h3.bev_regen_last_end(t1)
            for b in boxes:
                d = _viewport(b, deadline)
                for _k in range(INCOMPLETE_REFETCH):
                    if not _doc_incomplete(d):
                        break
                    st["incomplete_refetch"] = st.get("incomplete_refetch", 0) + 1
                    time.sleep(2)
                    d = _viewport(b, deadline)
                if not _fetched_after(d.get("fetched_at"), epoch_end):
                    return None, "stale-epoch", t1
                if not _fetched_after(d.get("fetched_at"), t1 - DOC_MAX_AGE_S):
                    return None, "stale-doc", t1
                ds = d.get("stats") or {}
                for k in st:
                    st[k] += int(ds.get(k) or 0)
                if _fetched_after(d.get("fetched_at"), t1 - 2):
                    tl = int(ds.get("tiles_z15") or 0) + int(ds.get("tiles_z16") or 0)
                    net += int(int(ds.get("bytes") or 0) * (1.0 - int(ds.get("cache_hits") or 0) / tl)) if tl else 0
                else:
                    cached_docs += 1
                for _k in ("parcels", "footprints", "landuse"):
                    if d.get(_k) is None:
                        d[_k] = []
                docs.append(d)
            if time.time() >= ne_h3.bev_regen_window(t1)[0]:
                return None, "crossed-regen", t1
            if _tile_conflicts(docs):
                return None, "mixed-tiles", t1
            if any(_doc_incomplete(d) for d in docs) and not last_try:
                return None, "tiles-incomplete", t1
            return (docs, st, net, cached_docs), None, t1

        _w = ne_h3.wait_out_bev_regen(deadline + ne_h3.bev_regen_window()[1] - ne_h3.bev_regen_window()[0])
        row["regen_wait_s"] = round(_w)
        deadline += _w
        last_try = False
        for _try in range(3):
            last_try = _try == 2
            got, why, t1 = _fetch_all()
            if got is not None:
                break
            log.warning("vct_pilot: %s fetch %s — restarting bevdirect-serve for a single tile epoch (try %d)", kg, why, _try + 1)
            row["epoch_refetch"] = why
            _w = ne_h3.wait_out_bev_regen(deadline + ne_h3.bev_regen_window()[1] - ne_h3.bev_regen_window()[0])
            row["regen_wait_s"] = round(row.get("regen_wait_s", 0) + _w)
            deadline += _w
            ne_h3._stop_bevdirect_server()
            ne_h3._ensure_bevdirect_server()
        else:
            raise RuntimeError("could not fetch the cells inside one BEV tile epoch")
        docs, st, net, cached_docs = got
        row["tile_epoch"] = ne_h3.bev_epoch_day(t1)
        row["fetch_s"] = round(time.time() - t1, 1)
        try:
            _rd = ne_h3.bevdirect_ready()
            if (_rd.get("upstream") or {}).get("state"):
                row["upstream"] = _rd["upstream"]["state"]
            tc = (_rd.get("tile_cache") or {})
            if tc:
                row["lru_mb"] = round(int(tc.get("bytes") or 0) / 1e6, 1)
                row["lru_max_mb"] = round(int(tc.get("max_bytes") or 0) / 1e6)
                row["lru_hits"] = int(tc.get("hits") or 0)
        except Exception:
            pass
        tiles = st["tiles_z15"] + st["tiles_z16"]
        row.update(tiles=tiles, tile_errors=st["tile_errors"], cache_hits=st["cache_hits"], tile_bytes=st["bytes"],
                   net_bytes_est=net, cached_docs=cached_docs)
        if st.get("incomplete_refetch"):
            row["incomplete_refetch"] = st["incomplete_refetch"]
        inc_cells = [list(cells[i]) for i, d in enumerate(docs) if _doc_incomplete(d)]
        if inc_cells:
            row["tiles_incomplete"] = True
            row["tiles_incomplete_cells"] = inc_cells[:16]
            log.warning("vct_pilot: %s incomplete tile fetch in %d cell(s) %s after %d re-request(s) — "
                        "declared layer not written (rebuilt next pass)", kg, len(inc_cells), inc_cells[:4],
                        st.get("incomplete_refetch", 0))
        source = canon.bevdirect_source(docs)
        row["source"] = source
        # --- building context for the KG pipeline, streamed over a pipe (RAM only) ---
        if context_fd is not None and row.get("tiles_incomplete"):
            # missing tiles = missing footprints: nothing is streamed, the
            # processor runs without building context for this KG
            row["ctx"] = {"skipped": "tiles_incomplete"}
        elif context_fd is not None:
            ctx = footprints_from_docs(docs)
            row["ctx"] = dict(ctx["stats"], bytes=_context_to_fd(ctx, context_fd))
            del ctx
        if context_only:
            del docs
            row.update(ok=True, context_only=True, wall_s=round(time.time() - t0, 1), rss_peak_mb=_peak_rss_mb())
            return row
        # --- cells, in memory; the documents are dropped right after ---
        t2 = time.time()
        P, F, L = canon.from_bevdirect(docs)
        del docs
        # The input domain is the UNION OF THE ALIGNED CELLS we fetched so
        # the layer is stable per cell block and comparable across hosts.
        # Memory: build_cells holds ~3.4 KB per product cell; above
        # TILE_CELLS estimated cells the build runs per padded sub-domain
        # (byte-identical output, bounded peak — ne_build_tiled.py).
        import ne_build_tiled
        n_own = sum(1 for p in P if p.get("kg") == kg)
        est = ne_build_tiled.estimate_cells([p["geometry"] for p in P if p.get("kg") == kg], (S + N) / 2.0) if n_own else 0
        row["cells_est"] = est
        if est > TILE_CELLS:
            raw, n_cells, n_kcells, b_stats, n_tiles = ne_build_tiled.build_cells_tiled(
                kg, P, F, L, input_bbox=input_bbox, coverage_bbox=input_bbox)
            row["tiled"] = n_tiles
        else:
            res = build_cells(kg, P, F, L, coverage_bbox=input_bbox, input_bbox=input_bbox)
            raw, n_cells, n_kcells, b_stats = pack_records(res.cells, res.kcells), len(res.cells), len(res.kcells), res.stats
            del res
        if not n_cells:
            raise RuntimeError(f"0 cells built from {len(boxes)} viewport(s)")
        del P, F, L
        epoch = time.strftime("%Y-%m", time.gmtime())
        hdr = lu_header(kg, epoch, source, ALGO, H3_RES, K_RES, K, n_cells, n_kcells, b_stats,
                        input_bbox=input_bbox, coverage_bbox=input_bbox, input_pad_deg=INPUT_PAD_DEG,
                        input_rule=INPUT_RULE, inputs=[{"endpoint": "bevdirect-serve/viewport", "requests": len(boxes)}])
        blob = pack_section(MAGIC_LU, hdr, raw)
        sec = unpack_sections(blob)[0]
        row.update(build_s=round(time.time() - t2, 1), cells=n_cells, kcells=n_kcells,
                   digest=sec.header.get("digest"), epoch=epoch, algo=ALGO)
        if out_nec and row.get("tiles_incomplete"):
            row["declared"] = False
            row["nec_skipped"] = "tiles_incomplete"
        elif out_nec:
            op = Path(out_nec)
            op.parent.mkdir(parents=True, exist_ok=True)
            tmp = op.with_suffix(op.suffix + ".tmp")
            tmp.write_bytes(blob)
            tmp.replace(op)
            row["nec_out"] = str(op)
            row["declared"] = True
        row["ok"] = True
    except Exception as e:  # noqa: BLE001
        row["error"] = f"{type(e).__name__}: {str(e)[:200]}"
    row["wall_s"] = round(time.time() - t0, 1)
    row["rss_peak_mb"] = _peak_rss_mb()
    return row


def _peak_rss_mb() -> int:
    """Helper's own peak RSS (ru_maxrss, kB on Linux) — in every row, so a
    cgroup OOM kill (rc=-9, no stderr) can be read against the budget."""
    try:
        import resource
        return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024)
    except Exception:  # noqa: BLE001
        return 0


def _cgroup_mem_note() -> str:
    """'oom_kill=N peak=M MB max=X MB' from the processor's own cgroup (the
    helper shares it) — appended to a rc=-9 row so the cause is on the row."""
    try:
        cg = open("/proc/self/cgroup").read().strip().split(":")[-1]
        base = f"/sys/fs/cgroup{cg}"
        ev = dict(l.split() for l in open(f"{base}/memory.events").read().splitlines() if l.strip())
        def _mb(name):
            try:
                v = open(f"{base}/{name}").read().strip()
                return "max" if v == "max" else f"{int(v) // 2**20}MB"
            except Exception:  # noqa: BLE001
                return "?"
        return f"cgroup oom_kill={ev.get('oom_kill', '?')} peak={_mb('memory.peak')} max={_mb('memory.max')}"
    except Exception:  # noqa: BLE001
        return ""


def _regen_slack_s(now: float | None = None) -> float:
    """Extra seconds the helper may legitimately spend sleeping out the BEV
    tile regeneration window (``ne_h3.wait_out_bev_regen``): the whole
    window length when the helper's budget overlaps it, else 0."""
    try:
        import ne_h3
        now = now or time.time()
        a, b = ne_h3.bev_regen_window(now)
        if now < b and now + TIMEOUT_S > a:
            return float(b - a)
        a, b = a + 86400, b + 86400          # window of the next UTC day
        if now + TIMEOUT_S > a:
            return float(b - a)
    except Exception:  # noqa: BLE001
        pass
    return 0.0



def _run_helper(kg: str, bbox: tuple, extra: list[str] | None = None,
                context: dict | None = None) -> dict:
    """Run the bounded helper subprocess; always returns a row (recorded).
    With *context* (a dict to fill), the helper streams the bbox's building
    footprints back over an anonymous pipe → ``context['ctx']`` (never a
    file: the VTC posture)."""
    kg = str(kg).zfill(5)
    row = None
    rfd = wfd = None
    reader = None
    try:
        cmd = [sys.executable, str(Path(__file__).resolve()), "run", "--kg", kg,
               "--bbox", ",".join(f"{float(v):.6f}" for v in bbox), "--observer", _observer()]
        cmd += list(extra or [])
        pass_fds: tuple = ()
        if context is not None:
            rfd, wfd = os.pipe()
            cmd += ["--context-fd", str(wfd)]
            pass_fds = (wfd,)
            reader = threading.Thread(target=_context_from_fd, args=(rfd, context), daemon=True)
            reader.start()
        p = subprocess.run(cmd, capture_output=True, timeout=TIMEOUT_S + 60 + _regen_slack_s(),
                           cwd=str(Path(__file__).resolve().parent), pass_fds=pass_fds)
        if wfd is not None:
            os.close(wfd); wfd = None
        out = p.stdout.decode("utf-8", "replace").strip().splitlines()
        if out:
            row = json.loads(out[-1])
        else:
            err = p.stderr[-200:].decode("utf-8", "replace")
            if p.returncode == -9:
                err = (err.strip() + " SIGKILL (" + (_cgroup_mem_note() or "OOM?") + ")").strip()
            row = {"kg": kg, "ok": False, "ts": time.time(), "error": f"helper rc={p.returncode}: {err}"}
    except subprocess.TimeoutExpired:
        row = {"kg": kg, "ok": False, "ts": time.time(), "error": f"helper timeout {TIMEOUT_S}s"}
    except Exception as e:  # noqa: BLE001
        row = {"kg": kg, "ok": False, "ts": time.time(), "error": f"helper: {str(e)[:200]}"}
    finally:
        if wfd is not None:
            try:
                os.close(wfd)
            except Exception:
                pass
        if reader is not None:
            reader.join(timeout=60)
    if row.get("_recorded") is not True:
        record(row)
    return row


# === SECTION: declared layer for the product ===

NEC_DIR = AP_DIR / "ne_declared"                # <kg>.nec — NEC1 only (statistics), reused by retries ≤ NEC_KEEP_S
NEC_KEEP_S = 48 * 3600


class DeclaredDeficient(RuntimeError):
    """The bevdirect build of this parent would lose cells (an input polygon
    wider than a bevdirect cell + pad has no complete copy; alpine parents).
    Off by default (``VCT_LU_ON_CLIPPED=1`` re-enables the check)."""

    def __init__(self, kg: str, incomplete: int):
        super().__init__(f"{kg}: {incomplete} input polygon(s) clipped by the bevdirect cell grid")
        self.incomplete = incomplete


def build_declared(kg: str, bbox: tuple, context: dict | None = None, is_block: bool = False) -> bytes:
    """NEC1 bytes for *kg* from the public BEV vector tiles (one viewport
    grid, cells in memory), synchronously.  The section (statistics only) is
    kept on disk for ``NEC_KEEP_S`` so a retried KG (or a second block)
    reuses it.

    *context* (``austria_processor.fetch_building_context``): a dict the
    helper fills with ``ctx`` = the bbox's building footprints, over a pipe
    — the one viewport grid feeds both the NE build and the pipeline.  When
    it is requested the helper always runs (warm cells cost no BEV
    traffic); an existing fresh NEC1 is kept.

    Raises ``RuntimeError`` on failure; ``ne_h3.fetch_declared`` decides
    whether a fallback source exists."""
    kg = str(kg).zfill(5)
    fp = NEC_DIR / f"{kg}.nec"
    fresh = False
    try:
        fresh = fp.exists() and time.time() - fp.stat().st_mtime < NEC_KEEP_S and fp.stat().st_size > 64
    except Exception:
        fresh = False
    if fresh and context is None:
        log.info("vct_pilot: declared layer %s reused from %s (%.0f min old)", kg, fp,
                 (time.time() - fp.stat().st_mtime) / 60)
        return fp.read_bytes()
    if fresh:
        log.info("vct_pilot: declared layer %s reused from %s (%.0f min old) — helper streams the building context only",
                 kg, fp, (time.time() - fp.stat().st_mtime) / 60)
        row = _run_helper(kg, bbox, ["--context-only"], context=context)
        _log_row(row)
        if not row.get("ok"):
            raise RuntimeError(f"vct building-context fetch failed: {row.get('error') or 'helper failed'}")
        return fp.read_bytes()
    t_run = time.time()
    row = _run_helper(kg, bbox, ["--out", str(fp)], context=context)
    # Helper killed by TIMEOUT_S *after* it wrote the NEC1 (huge parents):
    # the layer on disk is complete (tmp → rename) — use it.
    if (not row.get("nec_out") and str(row.get("error") or "").startswith("helper timeout")):
        try:
            if fp.exists() and fp.stat().st_mtime >= t_run - 1 and fp.stat().st_size > 64:
                row.update(nec_out=str(fp), declared=True)
                log.warning("vct_pilot: %s helper timed out after writing the NEC1 (%.0f s) — declared layer kept",
                            kg, time.time() - t_run)
        except Exception:
            pass
    _log_row(row)
    if not row.get("nec_out") or not fp.exists():
        raise RuntimeError(f"vct declared build failed: {row.get('error') or 'no NEC1 written'}")
    return fp.read_bytes()


def fetch_context_only(kg: str, bbox: tuple, context: dict) -> None:
    """Building footprints for an arbitrary *bbox* — the helper's
    ``--context-only`` mode: viewport fetch (warm cells from the local
    bevdirect-serve), no NEC1 build.  Used by sliver-sweep units."""
    kg = str(kg).zfill(5)
    row = _run_helper(kg, bbox, ["--context-only"], context=context)
    _log_row(row)
    if not row.get("ok"):
        raise RuntimeError(f"vct building-context fetch failed: {row.get('error') or 'helper failed'}")


def _log_row(row: dict) -> None:
    kg = row.get("kg")
    if row.get("ok"):
        log.info("vct_pilot: %s %s cells=%s requests=%s tiles=%s tile_bytes=%.1fMB net~%.1fMB fetch=%ss build=%ss%s",
                 kg, row.get("source"), row.get("cells"), row.get("requests"), row.get("tiles"),
                 (row.get("tile_bytes") or 0) / 1e6, (row.get("net_bytes_est") or 0) / 1e6, row.get("fetch_s"),
                 row.get("build_s"), " (context only)" if row.get("context_only") else "")
    else:
        log.warning("vct_pilot: %s FAILED after %ss: %s", kg, row.get("wall_s"), row.get("error"))


# === SECTION: status summary ===

def compact(row: dict) -> dict:
    """Slim view of a row for the status API."""
    return {"kg": row.get("kg"), "ok": bool(row.get("ok")), "ts": int(float(row.get("ts") or 0)),
            "src": row.get("source"), "cells": row.get("cells"), "req": row.get("requests"),
            "tiles": row.get("tiles"), "tile_mb": round(float(row.get("tile_bytes") or 0) / 1e6, 1),
            "net_mb": round(float(row.get("net_bytes_est") or 0) / 1e6, 1), "wall_s": row.get("wall_s"),
            "lru_mb": row.get("lru_mb"), "lru_max_mb": row.get("lru_max_mb"),
            "rss_mb": row.get("rss_peak_mb"), "tiled": row.get("tiled"), "ctx_only": row.get("context_only"),
            "digest": row.get("digest"), "bbox": row.get("input_bbox"), "partial": row.get("partial_viewport"),
            "decl": bool(row.get("declared")) or None,
            "err": (row.get("error") or "")[:80] or None,
            "ctx": ({k: (row.get("ctx") or {}).get(k) for k in ("footprints_bbox",)} if row.get("ctx") else None),
            "tinc": True if row.get("tiles_incomplete") else None}


def summary(recent_n: int = 6) -> dict:
    rs = rows()
    now = time.time()
    d1 = [r for r in rs if now - float(r.get("ts") or 0) < 86400]
    ok = [r for r in d1 if r.get("ok")]
    out = {"enabled": enabled(), "n": len(rs), "n_24h": len(d1), "ok_24h": len(ok),
           "fail_24h": len(d1) - len(ok),
           "net_mb_24h": round(sum(float(r.get("net_bytes_est") or 0) for r in d1) / 1e6, 1),
           "tile_mb_24h": round(sum(float(r.get("tile_bytes") or 0) for r in d1) / 1e6, 1),
           "requests_24h": sum(int(r.get("requests") or 0) for r in d1),
           "net_mb_total": round(sum(float(r.get("net_bytes_est") or 0) for r in rs) / 1e6, 1),
           "tiles_incomplete_24h": len([r for r in d1 if r.get("tiles_incomplete")]),
           "declared_24h": len([r for r in d1 if r.get("declared")]),
           "context_24h": len([r for r in d1 if r.get("ctx")]),
           "recent": [compact(r) for r in rs[-recent_n:]]}
    try:
        import ne_h3
        bd = ne_h3.bevdirect_ready(probe_server=False)
        out["tooling"] = bool(bd.get("binary") and bd.get("package"))
        out["ne_source"] = ne_h3.ne_source()
    except Exception:
        pass
    return out


# === SECTION: CLI (the helper) ===

def _main(argv: list[str]) -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="vct_pilot")
    sub = ap.add_subparsers(dest="cmd")
    r = sub.add_parser("run")
    r.add_argument("--kg", required=True)
    r.add_argument("--bbox", required=True, help="W,S,E,N (parent KG bbox, WGS84)")
    r.add_argument("--observer")
    r.add_argument("--no-record", action="store_true")
    r.add_argument("--out", help="write the NEC1 section here (statistics only) — the product's declared layer")
    r.add_argument("--context-fd", type=int, help="stream the bbox's building footprints (gzip JSON) into this inherited pipe fd")
    r.add_argument("--context-only", action="store_true",
                   help="fresh kept NEC1: stream the building context, skip the cell build")
    sub.add_parser("status")
    sub.add_parser("rows")
    a = ap.parse_args(argv)
    if a.cmd == "run":
        logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
        row = run(a.kg, tuple(float(v) for v in a.bbox.split(",")), a.observer, out_nec=a.out,
                  context_fd=a.context_fd, context_only=a.context_only)
        if not a.no_record:
            record(row)
            row["_recorded"] = True
        print(json.dumps(row, separators=(",", ":"), default=str))
        return 0 if row.get("ok") else 1
    if a.cmd == "status":
        print(json.dumps(summary(), indent=1))
        return 0
    if a.cmd == "rows":
        for r in rows():
            print(json.dumps(r, separators=(",", ":")))
        return 0
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
