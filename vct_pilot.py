"""vct_pilot — the vtcseamless (VTC) cadastre pilot.  See docs/vct-pilot.md.

One (or a few) frontier peers additionally derive the KG's *declared* NE
layer from the public BEV vector tiles with the open-source vtcseamless
toolkit (``bevdirect-serve`` + umfeld's pinned ``ne_cells`` package) and
report the digests to umfeld-at (``POST /contrib/api/v1/ne/{kg}/report``).
The product itself keeps using umfeld's frozen ``lu`` layer, so the NEO1
``/obs`` round-trip (cell set == umfeld's) is untouched: the pilot can only
add telemetry, never break a KG.

Rules the pilot sticks to (the VTC posture — docs/v2.4-ne-proposal.md §10/11,
vtcseamless README → Compliance posture):

* **Cadastre only in memory.**  bevdirect-serve v0.3.1+ keeps raw BEV tiles
  in an in-process RAM LRU (``-tile-cache-mb``, ``-tile-ttl 24h``; no .pbf
  is ever written to disk — ``ne_h3._purge_legacy_tile_dirs`` removes the
  pre-v0.3.1 tmpfs dir) and the assembled cells in RAM
  (``-ttl 6h``); the viewport document is parsed in-process into the
  aggregated NEC1 cells (``ne_cells`` as a library) and dropped.  Nothing
  parcel-shaped is written anywhere; only counts / digests / bytes land in
  ``data/austria_processor/vct_pilot.jsonl``.
* **Aligned cells, never a free viewport.**  One ``/viewport`` per aligned
  0.02° cell (``ix = floor(lon/0.02)``) intersecting the parent's bbox
  (+0.004°); the build's ``input_bbox`` is the union of those cells, so the
  report's bbox is stable per cell block and comparable by the operator's
  coverage rule (``requests`` = cells — read it on ``/process.txt``).  A
  viewport that holds no full cell builds but does not report.  Blocks of a split parent on the
  same peer re-use the parent's run (24 h dedupe).  ``-prefetch 0`` — no
  background ring warming.
* **Never interrupts the herd.**  Runs as a bounded helper subprocess
  (``VCT_PILOT_TIMEOUT_S``) next to the KG; every failure is a WARNING line
  + a jsonl row, never an exception into the KG flow.

Telemetry (per KG row, ~300 B): bevdirect source tag, requests, tiles,
tile bytes (+ estimated network bytes = bytes × (1 − cache_hits/tiles)),
cells vs umfeld's declared layer (common / only-ours / only-umfeld, dominant
land-use agreement, share of common cells within umfeld's ±8/255 tolerance),
the ``/report`` answer (baseline, compared, chunks same/changed/unknown) and
wall time.  Peers ship a ~250 B ``vct_pilot`` summary in every full status
push; the director folds the fleet into the ``vct_pilot:`` block of
``/process.txt``.
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

log = logging.getLogger("vct_pilot")

DATA_DIR = Path(os.environ.get("NE_DATA_DIR") or (Path(__file__).resolve().parent / "data"))
AP_DIR = DATA_DIR / "austria_processor"
SIDECAR = AP_DIR / "vct_pilot"                 # exists + "1" → pilot on for this peer
LOG_FILE = AP_DIR / "vct_pilot.jsonl"          # ring of per-KG rows (counts/digests only)
LOG_KEEP = 200
MAX_SPAN = 0.045                                # bevdirect MaxSpan (° per request edge)
REQ_PAD = 0.004                                 # == bevdirect cellPad: neighbours inside ne_cells' input pad
DEDUPE_S = 24 * 3600                            # one run per parent per peer per day
TIMEOUT_S = int(os.environ.get("VCT_PILOT_TIMEOUT_S", "1500"))
TILE_CELLS = int(os.environ.get("VCT_TILE_CELLS", "300000"))   # est. product cells above which the NEC1 build is tiled (ne_build_tiled)
REPORT_PATH = "/contrib/api/v1/ne/{kg}/report"  # umfeld-at, our cadastre token reports too
CHUNKS_PATH = "/contrib/api/v1/ne/{kg}/chunks"  # change protocol step 2 (NECH body, vtcseamless-py >= 0.2.0)
CHUNKS_BODY_MAX = 1_500_000                     # umfeld: 2 MB per POST .../chunks
HEADER_FIRST = os.environ.get("VCT_HEADER_FIRST", "1") == "1"   # step 0 probe when WE reported this KG before
SAME_PASS_S = 20 * 3600                         # umfeld neSamePassWindow: a re-post by the same observer < 20 h is the same pass
LU_TOL = 8                                      # umfeld's documented source-noise tolerance (1/255)
SKIP_SUBCELL_KG = os.environ.get("VCT_SKIP_SUBCELL_KG", "0") == "1"   # rule 3 on the raw KG bbox (see run())
# QC (umfeld ne-change.md §5a, 2026-10-15): a build must come from ONE tile
# generation.  Every cell document of a report must have been assembled
# (a) after the last BEV regeneration window (tile epoch — already enforced)
# AND (b) less than DOC_MAX_AGE_S ago (bevdirect-serve keeps assembled cells
# ``-ttl 6h`` and raw tiles 24 h; the guard holds even if a longer TTL is
# configured), AND (c) no tile (z,x,y) may carry two different sha256 across
# the documents (overlapping pads make this a real cross-document check).
# A violation restarts the server (RAM caches gone) and refetches once more.
DOC_MAX_AGE_S = int(os.environ.get("VCT_DOC_MAX_AGE_S", str(6 * 3600)))
INCOMPLETE_REFETCH = 2                          # re-request cells with tiles_fetched < tiles_expected this many times


# === SECTION: peer-side switches ===

def enabled() -> bool:
    try:
        return SIDECAR.exists() and SIDECAR.read_text().strip() in ("1", "true", "on")
    except Exception:
        return False


def set_enabled(on: bool) -> bool:
    """Write/remove the sidecar (called by ``/api/v1/processing/start``).
    Returns True when the state changed."""
    try:
        cur = enabled()
        if on and not cur:
            SIDECAR.parent.mkdir(parents=True, exist_ok=True)
            SIDECAR.write_text("1")
            return True
        if not on and SIDECAR.exists():
            SIDECAR.unlink()
            return True
    except Exception as e:  # noqa: BLE001
        log.warning("vct_pilot sidecar: %s", e)
    return False


def _observer() -> str:
    try:
        d = json.loads((AP_DIR / "self.json").read_text())
        pid = str(d.get("id") or "").strip()
        if pid:
            return f"srtm-peer-{pid}"[:64]
    except Exception:
        pass
    return "srtm-peer-unknown"


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


def self_reported(parent: str, within_s: float = SAME_PASS_S) -> dict | None:
    """The newest row in which THIS peer actually posted *parent* to umfeld
    (full report 200 or header probe ``unchanged:true``) within *within_s*
    — first-hand knowledge, no director round-trip.  umfeld counts a second
    post by the same observer inside ``neSamePassWindow`` (20 h) as the same
    pass: it confirms nothing and re-sends the chunk list (40–90 KB).  Rows
    whose step 2 failed (``chg.ok`` False) do not count — the next build
    must send the rows again."""
    now = time.time()
    for r in reversed(rows()):
        if r.get("kg") != parent or now - float(r.get("ts") or 0) >= within_s:
            continue
        if r.get("skipped") or r.get("skipped_report") or not r.get("ok"):
            continue
        if r.get("report_http") != 200:
            continue
        ch = r.get("chg")
        if isinstance(ch, dict) and ch.get("ok") is False:
            continue
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
    ``/viewport`` per cell, exactly like umfeld's reference build and the
    ``vtcseamless`` client.  (Until 2026-10-07 this was the minimum grid of
    ≤0.045° boxes; a multi-cell ``/viewport`` keeps ONE truncated copy of a
    parcel wider than cell + pad — ``complete:false`` at the fetched tile
    edge — so a stitched viewport never equals the operator's aligned-cell
    build and every digest difference between two viewports of different
    width was an artefact, not land-use change (umfeld, 2026-10-08: all 678
    "changed" chunks we had produced).  Per cell the server assembles the
    same tiles, so the BEV traffic is identical; the requests are local."""
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
    report from a smaller viewport is a partial build of its cells — umfeld
    asks to skip it: one single-cell report is worth more to the change
    signal than many partial ones."""
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


# === SECTION: cadastre for the KG pipeline (bev mode) ===

def cadastre_from_docs(docs: list, kg: str) -> dict:
    """The KG pipeline's cadastre from the SAME viewport documents the NE
    build consumes (docs/vct-pilot.md → bev mode): no second fetch, and the
    dict never touches a file system — the helper streams it to the KG
    subprocess over an anonymous pipe (``--cadastre-fd``).

    * ``parcels`` — this KG's parcels (``kg_code == kg``), deduped by
      ``parcel_id``: the ``complete`` copy wins; a parcel wider than one
      bevdirect cell + pad (alpine Almen/forests, 75110: 13/2 643) has NO
      complete copy — its clipped pieces from every cell are unioned back
      into the full polygon (``restored``), area = union area.  umfeld's
      ``/k`` export answers the same ids (±1–6 border parcels, 2026-10-07).
      Properties kept: ``parcel_id, kg_code, gnr, ez, rstatus→status,
      area_sqm, lon, lat``.
    * ``footprints`` — EVERY footprint of the padded bbox (own KG and
      neighbours; bevdirect footprints carry no kg), deduped by ``id``;
      ``ns`` kept.  ``fetch_cadastre_data_bevdirect`` splits them into the
      KG's own set (centroid inside the own-parcel union) and the
      viewport set used for the per-tile raster mask (replaces the per-tile
      ``/k/spatial/footprints`` call).
    Fields the ``/k`` export had and bevdirect has not: ``kg_name``,
    ``landuse_summary`` (parcels), ``ns_recht`` (footprints) and the
    address-point layer — all unused since v2.4 (``CADASTRE_PRODUCTS=False``).
    """
    from shapely.geometry import shape, mapping
    from shapely.ops import unary_union
    kg = str(kg).zfill(5)
    best: dict = {}
    pieces: dict = {}
    for d in docs:
        for p in d.get("parcels") or []:
            if str(p.get("kg_code") or p.get("kg") or "").zfill(5) != kg or not p.get("geometry"):
                continue
            pid = str(p.get("parcel_id") or f"{kg}-{p.get('gnr', '')}")
            comp = bool(p.get("complete", True))
            cur = best.get(pid)
            if cur is None or (comp and not cur[1]) or (comp == cur[1] and float(p.get("area_sqm") or 0) > float(cur[0].get("area_sqm") or 0)):
                best[pid] = (p, comp)
            if not comp:
                pieces.setdefault(pid, []).append(p["geometry"])
    parcels = []
    restored = 0
    for pid, (p, comp) in best.items():
        geom = p["geometry"]
        area = float(p.get("area_sqm") or 0)
        if not comp and pieces.get(pid):
            try:
                u = unary_union([shape(g).buffer(0) for g in pieces[pid]])
                if not u.is_empty:
                    geom = mapping(u)
                    area = round(_area_sqm(u), 1)
                    restored += 1
            except Exception:  # noqa: BLE001
                pass
        parcels.append({"parcel_id": pid, "kg_code": kg, "gnr": p.get("gnr"), "ez": p.get("ez"),
                        "status": p.get("rstatus") or p.get("status"), "area_sqm": area,
                        "lon": p.get("lon"), "lat": p.get("lat"), "complete": comp, "geometry": geom})
    fps: dict = {}
    for d in docs:
        for f in d.get("footprints") or []:
            if not f.get("geometry"):
                continue
            fid = str(f.get("id") or "")
            cur = fps.get(fid)
            if cur is None or float(f.get("area_sqm") or 0) > float(cur.get("area_sqm") or 0):
                fps[fid] = {"id": fid, "ns": f.get("ns"), "ns_code": f.get("ns_code"), "parcel_id": f.get("parcel_id"),
                            "area_sqm": f.get("area_sqm"), "geometry": f["geometry"]}
    incomplete = sum(1 for _, c in best.values() if not c)
    return {"kg": kg, "parcels": parcels, "footprints": list(fps.values()),
            "stats": {"parcels_own": len(parcels), "incomplete_own": incomplete, "restored": restored,
                      "footprints_bbox": len(fps)}}


_TF_3035 = [None]


def _area_sqm(geom_wgs) -> float:
    from shapely.ops import transform as _stf
    if _TF_3035[0] is None:
        import pyproj
        _TF_3035[0] = pyproj.Transformer.from_crs(4326, 3035, always_xy=True).transform
    return float(_stf(_TF_3035[0], geom_wgs).area)


def _cadastre_to_fd(cad: dict, fd: int) -> int:
    """gzip-JSON the cadastre into an inherited pipe (RAM only — never a
    file, not even tmpfs); returns bytes written."""
    import gzip
    blob = gzip.compress(json.dumps(cad, separators=(",", ":")).encode(), compresslevel=1)
    view = memoryview(blob)
    while view:
        n = os.write(fd, view)
        view = view[n:]
    os.close(fd)
    return len(blob)


def _cadastre_from_fd(fd: int, out: dict) -> None:
    """Reader side (thread): drains the pipe until EOF → ``out['cad']``."""
    import gzip
    chunks = []
    try:
        while True:
            b = os.read(fd, 1 << 20)
            if not b:
                break
            chunks.append(b)
        if chunks:
            out["cad"] = json.loads(gzip.decompress(b"".join(chunks)))
    except Exception as e:  # noqa: BLE001
        out["error"] = str(e)[:200]
    finally:
        try:
            os.close(fd)
        except Exception:
            pass


REPORTED_FRESH_S = 30 * 86400                   # a fleet report younger than this = skip (blocks only)


def _director_get(path: str, params: dict | None = None, timeout=(3, 20)):
    """GET on the cluster director (the primary, or the HA stand-in) with the
    admin token — peers never ask umfeld's ``/k`` tier for fleet state."""
    import requests
    import director_ha
    base = director_ha.director_url() or "http://127.0.0.1:8000"
    h = {}
    try:
        h["X-Admin-Token"] = (DATA_DIR / "admin_token").read_text().strip()
    except Exception:
        pass
    return requests.get(base.rstrip("/") + path, params=params, headers=h, timeout=timeout)


def already_reported(kg: str, max_age_s: float = REPORTED_FRESH_S) -> dict | None:
    """Fleet-wide dedupe for split blocks: ask the director
    (``GET /api/v1/director/vct_reported?kg=``, read from ``vct_fleet.json``'s
    30-day ``reported`` map) whether any peer already reported this parent
    with our bevdirect tag.  A sibling block on another peer re-assembling
    the whole parent only to be stubbed by umfeld bought nothing (12× for
    87001).  ``None`` on any error (→ run normally)."""
    import ne_h3
    try:
        r = _director_get("/api/v1/director/vct_reported", {"kg": str(kg).zfill(5), "max_age_s": int(max_age_s),
                                                            "source": "bevdirect@" + ne_h3.BEVDIRECT_TAG})
        if not r.ok:
            return None
        rep = (r.json() or {}).get("reported")
        return rep if isinstance(rep, dict) and rep.get("digest") else None
    except Exception as e:  # noqa: BLE001
        log.debug("vct_pilot already_reported %s: %s", kg, e)
    return None


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


def compare(ours, umf) -> dict:
    """Cell-set + content agreement between our bevdirect NEC1 and umfeld's
    frozen index NEC1.  Digests differ by construction (different source /
    epoch); the signal is cell overlap + dominant-group agreement + the share
    of common cells inside umfeld's documented source-noise tolerance."""
    import numpy as np
    a, b = ours.lu_table(), umf.lu_table()
    ia = {int(c): i for i, c in enumerate(a["cell_id"].tolist())}
    ib = {int(c): i for i, c in enumerate(b["cell_id"].tolist())}
    common = sorted(ia.keys() & ib.keys())
    out = {"ours": len(ia), "umfeld": len(ib), "common": len(common), "only_ours": len(ia) - len(common),
           "only_umfeld": len(ib) - len(common), "dom_agree_pct": None, "within_tol_pct": None}
    if common:
        xa = np.array([ia[c] for c in common]); xb = np.array([ib[c] for c in common])
        lua = a["lu"][xa].astype(np.int16); lub = b["lu"][xb].astype(np.int16)
        da = lua.argmax(axis=1); db = lub.argmax(axis=1)
        has = (lua.sum(axis=1) > 0) | (lub.sum(axis=1) > 0)
        agree = ((da == db) | ~has).mean()
        diff = np.abs(lua - lub).max(axis=1)
        bdiff = np.abs(a["bld_cover"][xa].astype(np.int16) - b["bld_cover"][xb].astype(np.int16))
        within = ((diff <= LU_TOL) & (bdiff <= LU_TOL)).mean()
        out["dom_agree_pct"] = round(float(agree) * 100, 1)
        out["within_tol_pct"] = round(float(within) * 100, 1)
    return out


def _split_bodies(rows_: dict, limit: int = CHUNKS_BODY_MAX) -> list[dict]:
    """Chunk-row dicts whose packed NECH size stays under *limit* (chunk-id order)."""
    out, cur, size = [], {}, 6
    for c in sorted(rows_):
        n = 12 + len(rows_[c])
        if cur and size + n > limit:
            out.append(cur)
            cur, size = {}, 6
        cur[c] = rows_[c]
        size += n
    if cur:
        out.append(cur)
    return out


def _contrib_post(url: str, data, headers: dict, deadline: float, kg: str, what: str):
    """POST to umfeld /contrib with the retry ladder: 429 honours Retry-After
    (never retried early), 5xx / connection errors up to 4x inside the
    helper's deadline.  Returns ``(response, attempts)``; raises the last
    connection error."""
    import requests
    attempt, r = 0, None
    while True:
        attempt += 1
        try:
            r = requests.post(url, data=data, headers=headers, timeout=180)
        except requests.RequestException as e:
            if attempt >= 4 or time.time() + 30 > deadline:
                raise
            log.warning("vct_pilot: %s %s POST failed (attempt %d): %s — retrying", kg, what, attempt, str(e)[:120])
            time.sleep(15 * attempt)
            continue
        if r.status_code == 429 or 500 <= r.status_code < 600:
            wait = 15 * attempt
            if r.status_code == 429:
                try:
                    wait = max(wait, min(120, int(r.headers.get("Retry-After") or 0)))
                except Exception:
                    pass
            if attempt < 4 and time.time() + wait + 30 < deadline:
                log.warning("vct_pilot: %s %s HTTP %s (attempt %d) — retrying in %ds", kg, what, r.status_code, attempt, wait)
                time.sleep(wait)
                continue
        return r, attempt


def _json(r) -> dict:
    try:
        d = r.json() if r.content else {}
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _post_chunks(kg: str, observer: str, sec, want: list, headers: dict, deadline: float) -> dict:
    """Change protocol step 2: the stripped LU rows (register bytes zeroed,
    no K rows — ``ne_cells.change.chunk_rows``) of exactly the chunks umfeld
    asked for, as NECH bodies ≤ 1.5 MB, from the section already in memory.
    Nothing is written to disk.  Returns the ``chg`` telemetry dict."""
    from ne_cells.change import chunk_rows, pack_chunk_rows
    import umfeld
    out = {"want": len(want), "posted": 0, "stored": 0, "seen": 0, "rejected": 0, "deltas": 0,
           "bytes": 0, "parts": 0, "http": None, "ok": False,
           # umfeld §5a delta status: candidate | confirmed count as ``deltas``;
           # pipeline (same tiles, different rows) / jitter (below the
           # quantisation floor) are NEVER change and are tallied apart
           "deltas_pipeline": 0, "deltas_jitter": 0, "deltas_confirmed": 0}
    rows_ = chunk_rows(sec.raw, sec.header["cells_n"], want)
    out["posted"] = len(rows_)
    if not rows_:
        out["ok"] = True
        return out
    h = dict(headers)
    h["Content-Type"] = "application/octet-stream"
    url = f"https://{umfeld.HOST}" + CHUNKS_PATH.format(kg=kg)
    ok = True
    for part in _split_bodies(rows_):
        body = pack_chunk_rows(part)
        out["parts"] += 1
        out["bytes"] += len(body)
        r, _ = _contrib_post(url + "?observer=" + observer, body, h, deadline, kg, "chunks")
        out["http"] = r.status_code
        ans = _json(r)
        if not r.ok:
            ok = False
            out["error"] = f"chunks HTTP {r.status_code}: {r.text[:120]}"
            log.warning("vct_pilot: %s chunks POST HTTP %s: %s", kg, r.status_code, r.text[:160])
            break
        for k in ("stored", "seen", "rejected"):
            out[k] += int(ans.get(k) or 0)
        dl = ans.get("deltas")
        if isinstance(dl, list):
            for d in dl:
                stt = str((d or {}).get("status") or "candidate") if isinstance(d, dict) else "candidate"
                if stt == "pipeline":
                    out["deltas_pipeline"] += 1
                elif stt == "jitter":
                    out["deltas_jitter"] += 1
                else:
                    out["deltas"] += 1
                    if stt == "confirmed":
                        out["deltas_confirmed"] += 1
        else:
            out["deltas"] += int(dl or 0)
    if out["deltas_pipeline"]:
        log.warning("vct_pilot: %s %d delta(s) with status pipeline (same source tiles, different rows) — not change",
                    kg, out["deltas_pipeline"])
    out["ok"] = ok and out["rejected"] == 0
    return out


def run(kg: str, bbox: tuple, umfeld_nec1: bytes | None, observer: str | None = None,
        timeout_s: int = TIMEOUT_S, out_nec: "str | Path | None" = None,
        cadastre_fd: int | None = None, skip_report: str | None = None,
        header_first: bool = False, prev_digest: str | None = None, full_once: bool = False,
        cadastre_only: bool = False) -> dict:
    """The pilot for one parent KG, in-process.  Returns the telemetry row.
    Raises nothing it can report: errors land in ``row['error']``.
    ``out_nec`` → the NEC1 section (statistics only — no parcel ever leaves
    RAM) is written there atomically; this is how ``ne_source=bevdirect``
    gets its declared layer (``build_declared``)."""
    import ne_h3
    t0 = time.time()
    deadline = t0 + timeout_s
    kg = str(kg).zfill(5)
    row: dict = {"ts": round(t0, 1), "kg": kg, "observer": observer or _observer(), "ok": False,
                 "bbox_in": [round(float(v), 6) for v in bbox]}
    if full_once:
        row["full_once"] = True
    try:
        from ne_cells import canon
        from ne_cells.algo import ALGO, H3_RES, K_RES, K, INPUT_PAD_DEG, INPUT_RULE, build_cells
        from ne_cells.pack import MAGIC_LU, lu_header, pack_records, pack_section, unpack_sections, epoch_report
        ne_h3._ensure_bevdirect_server()
        W, S, E, N = [float(v) for v in bbox]
        cells = _cells(W - REQ_PAD, S - REQ_PAD, E + REQ_PAD, N + REQ_PAD)
        boxes = [_cell_bbox(ix, iy) for ix, iy in cells]
        input_bbox = cells_union_bbox(cells)
        row["requests"] = len(boxes)
        row["input_bbox"] = list(input_bbox)
        row["cell_block"] = [min(c[0] for c in cells), min(c[1] for c in cells),
                             max(c[0] for c in cells), max(c[1] for c in cells)]
        # umfeld rule 3: a viewport that covers less than one full aligned
        # cell makes a partial report → build the layer (bev mode needs it)
        # but do not report.  What we FETCH is always whole aligned cells
        # (the union is the input domain), so for a KG the fetched region
        # always holds ≥ 1 full cell and the rule never fires on the data;
        # 4 236 / 7 850 KG bboxes are themselves narrower than 0.02° in one
        # axis (23308: 0.0198×0.0132°) and would lose their report under a
        # literal KG-bbox reading — VCT_SKIP_SUBCELL_KG=1 selects that.
        probe = (W, S, E, N) if SKIP_SUBCELL_KG else input_bbox
        if not covers_full_cell(*probe):
            skip_report = skip_report or f"viewport<cell ({E - W:.4f}×{N - S:.4f}°)"
            row["partial_viewport"] = True
        # --- fetch: one tile epoch only (umfeld item 3).  No fetch inside the
        # nightly BEV regeneration window; every document must have been
        # assembled after the last regeneration; a fetch that crossed into
        # the window is dropped (server restart → RAM caches gone) and
        # redone once, so a build never mixes two tile epochs.
        def _fetch_all():
            docs, st = [], {"tiles_z15": 0, "tiles_z16": 0, "tile_errors": 0, "cache_hits": 0, "bytes": 0}
            net = cached_docs = 0
            t1 = time.time()
            epoch_end = ne_h3.bev_regen_last_end(t1)
            for b in boxes:
                d = _viewport(b, deadline)
                # QC: incomplete cells are never cached by the server — ask again
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
                # bevdirect-serve answers a warm cell from RAM with the ORIGINAL
                # assembly stats; only count network bytes for documents that
                # were assembled by this request (fetched_at >= our start).
                if _fetched_after(d.get("fetched_at"), t1 - 2):
                    tl = int(ds.get("tiles_z15") or 0) + int(ds.get("tiles_z16") or 0)
                    net += int(int(ds.get("bytes") or 0) * (1.0 - int(ds.get("cache_hits") or 0) / tl)) if tl else 0
                else:
                    cached_docs += 1
                # bevdirect-serve < v0.3.3 serialised an empty layer as null →
                # ne_cells' ``for r in doc.get("footprints", [])`` raised (57010)
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

        # The regeneration wait is NOT part of the budget: a helper started
        # inside the 00:45-01:15 UTC window used to sleep ≤30 min of its
        # 1500 s and time out on the fetch (06003 / 82002 / 49404 / 30012,
        # Oct 15-16).  The parent's subprocess timeout grows the same way
        # (``_regen_slack_s``).
        _w = ne_h3.wait_out_bev_regen(deadline + ne_h3.bev_regen_window()[1] - ne_h3.bev_regen_window()[0])
        row["regen_wait_s"] = round(_w)
        deadline += _w
        last_try = False
        for _try in range(3):
            last_try = _try == 2     # the last attempt keeps an incomplete fetch (reported as suspect, never a product)
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
        # v0.3.1+: the server's RAM tile LRU (/health tile_cache) — proof that
        # tiles live in memory only, and how full the budget is
        try:
            _rd = ne_h3.bevdirect_ready()
            if (_rd.get("upstream") or {}).get("state"):     # v0.3.5+
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
                   net_bytes_est=net, cached_docs=cached_docs,
                   parcels=sum(len(d.get("parcels") or []) for d in docs))
        if st.get("incomplete_refetch"):
            row["incomplete_refetch"] = st["incomplete_refetch"]
        # QC provenance (bevdirect-serve >= v0.3.4): keep only tiles[] + counts
        # past ``del docs``; cells still incomplete after the re-requests are
        # reported as is (umfeld: change_suspect tiles_incomplete — not change)
        # and the build is NOT the product's declared layer (lu fallback).
        prov = _prov_view(docs)
        inc_cells = [list(cells[i]) for i, d in enumerate(docs) if _doc_incomplete(d)]
        if inc_cells:
            row["tiles_incomplete"] = True
            row["tiles_incomplete_cells"] = inc_cells[:16]
            log.warning("vct_pilot: %s incomplete tile fetch in %d cell(s) %s after %d re-request(s) — reported as suspect, "
                        "declared layer not written (rebuilt next pass)", kg, len(inc_cells), inc_cells[:4],
                        st.get("incomplete_refetch", 0))
        source = canon.bevdirect_source(docs)
        row["source"] = source
        # --- bev mode: the KG pipeline's cadastre from the same documents,
        # streamed to the processor over a pipe (RAM only) ---
        if cadastre_fd is not None and row.get("tiles_incomplete"):
            # missing tiles = missing parcels/footprints: nothing is streamed,
            # the processor's attempt fails and the KG is deferred (like a
            # transient umfeld failure) instead of baking a hole into the product
            row["cad"] = {"skipped": "tiles_incomplete"}
        elif cadastre_fd is not None:
            cad = cadastre_from_docs(docs, kg)
            row["cad"] = dict(cad["stats"], bytes=_cadastre_to_fd(cad, cadastre_fd))
            del cad
        if cadastre_only:
            # a fresh NEC1 of this parent is kept on disk (another block / a
            # retry on this peer): the documents fed the pipeline, the build
            # (10+ min and GBs for a huge parent) and the report are skipped
            del docs
            row.update(ok=True, cadastre_only=True, skipped_report="nec reused (cadastre only)",
                       wall_s=round(time.time() - t0, 1), rss_peak_mb=_peak_rss_mb())
            return row
        # --- cells, in memory; the documents are dropped right after ---
        t2 = time.time()
        P, F, L = canon.from_bevdirect(docs)
        del docs
        # umfeld rule 2: the input domain is the UNION OF THE ALIGNED CELLS we
        # fetched (not bbox(own parcels)+pad, not the raw viewport) — the
        # report's bbox is then stable per cell block and comparable across
        # observers / epochs (cadastre-process-api docs/ne-cells.md §coverage)
        # Memory: build_cells holds ~3.4 KB per product cell (dense per-cell
        # accumulator + rows) — Sölden 80110 (1.5 M cells) needs 5–6 GB and
        # was SIGKILLed in the 8 GB processor cgroup (helper rc=-9, Oct 2026).
        # Above TILE_THRESHOLD estimated cells the build runs per padded
        # sub-domain and packs homed records as it goes — byte-identical
        # output, bounded peak (ne_build_tiled.py).
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
            raise RuntimeError(f"0 cells built from {len(P)} parcels / {len(boxes)} viewport(s) — report not sent")
        del P, F, L
        epoch = time.strftime("%Y-%m", time.gmtime())
        hdr = lu_header(kg, epoch, source, ALGO, H3_RES, K_RES, K, n_cells, n_kcells, b_stats,
                        input_bbox=input_bbox, coverage_bbox=input_bbox, input_pad_deg=INPUT_PAD_DEG,
                        input_rule=INPUT_RULE, inputs=[{"endpoint": "bevdirect-serve/viewport", "requests": len(boxes)}])
        blob = pack_section(MAGIC_LU, hdr, raw)
        sec = unpack_sections(blob)[0]                       # ne_cells' Section (for epoch_report)
        osec = ne_h3.Section(ne_h3.MAGIC_LU, sec.header, sec.raw)   # ours (lu_table)
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
            row["declared"] = True          # this run IS the product's declared layer
        # --- compare with umfeld's frozen declared layer ---
        if umfeld_nec1:
            try:
                usec = [s for s in ne_h3.unpack_sections(umfeld_nec1) if s.magic == ne_h3.MAGIC_LU][0]
                row["cmp"] = compare(osec, usec)
                row["umfeld_digest"] = usec.digest
            except Exception as e:  # noqa: BLE001
                row["cmp_error"] = str(e)[:120]
        # --- the contribution: digests only → umfeld-at /contrib ---
        # change protocol (vtcseamless-py >= 0.2.0, ne_cells/change.py — the
        # frozen files stay untouched): the report also carries chunks_ap
        # (sha256 of each chunk's rows with gk / n_parc zeroed); the answer
        # may ask for the rows of some chunks (step 2, _post_chunks).
        try:
            from ne_cells.change import epoch_report_ap, AP_VERSION
            rep = epoch_report_ap(sec, row["observer"])
            row["ap"] = rep.get("ap_version") or AP_VERSION
        except ImportError:
            rep = epoch_report(sec, row["observer"])
            row["ap"] = None
            log.warning("vct_pilot: ne_cells.change missing (vtcseamless-py < 0.2.0) — report without chunks_ap, "
                        "this peer does not feed umfeld's change series")
        row["chunks"] = len(rep.get("chunks") or {})
        row["report_bbox"] = rep.get("input_bbox") or rep.get("bbox") or list(input_bbox)
        # QC provenance (vtcseamless-py 0.3.0, umfeld §5a): tiles{expected,
        # fetched, hash} + chunks_tiles{chunk: sha256[:16] of the source tiles
        # covering it}.  Computed by the client package from tiles[] — only
        # hashes and counts.  Absent on an older server / package (optional
        # fields: umfeld then runs only the floor + clustering checks).
        try:
            from vtcseamless.observe import tile_provenance
            tq, ct = tile_provenance(prov, rep.get("chunks") or {})
            if tq is not None:
                rep["tiles"], rep["chunks_tiles"] = tq, ct
                row["tiles_qc"] = tq
                row["chunks_tiles"] = len(ct)
            else:
                row["prov_missing"] = f"server {source} sends no tiles[]"
        except ImportError:
            row["prov_missing"] = "vtcseamless < 0.3.0"
        except Exception as e:  # noqa: BLE001
            row["prov_missing"] = f"{type(e).__name__}: {str(e)[:80]}"
        if row.get("prov_missing"):
            log.warning("vct_pilot: %s no QC provenance (%s) — report without tiles/chunks_tiles", kg, row["prov_missing"])
        del prov
        if skip_report:
            # a sibling block already reported this parent (fleet-wide
            # dedupe via the director) — umfeld would only stub it anyway
            row["skipped_report"] = skip_report
            row["ok"] = True
            row["wall_s"] = round(time.time() - t0, 1)
            row["rss_peak_mb"] = _peak_rss_mb()
            return row
        import umfeld
        h = umfeld.headers()
        h["Content-Type"] = "application/json"
        h["User-Agent"] = "srtm-lidar-at/2.4 vct_pilot"
        url = f"https://{umfeld.HOST}" + REPORT_PATH.format(kg=kg)
        # umfeld: 2 POST/s per token, burst 30 → 429 + Retry-After (honour it,
        # never retry inside); 5xx / connection errors are transient — a lost
        # report would only come back at the KG's next pass (months).
        r, attempt, ans = None, 0, {}
        if header_first and HEADER_FIRST and row.get("ap"):
            # step 0 (optional): header-only report.  umfeld keys it like the
            # full path — token + source class (bevdirect@* = one class) +
            # algo + digest + epoch; observer and bbox are NOT in the key
            # (fixed 2026-10-08: it used to require a chunk-bearing prior
            # report with the same bbox and answered 202 forever after the
            # v0.3.3 bbox change).  Same digest as any earlier report of ours
            # → 200 unchanged:true + chunks_confirmed (a few hundred bytes
            # instead of the chunk table); otherwise 202 {need:"chunks"} and
            # the full report follows.
            hdr = {k: v for k, v in rep.items() if k not in ("chunks", "chunks_ap", "chunks_tiles")}
            r, attempt = _contrib_post(url, json.dumps(hdr, separators=(",", ":")), h, deadline, kg, "report(header)")
            ans = _json(r)
            row["header_probe"] = r.status_code
            if prev_digest:
                # we expected unchanged:true iff the digest umfeld last saw
                # from us equals this build's; a 202 then is a server-side
                # anomaly (header_anomaly) rather than a changed build
                row["header_expect_unchanged"] = (prev_digest == row.get("digest"))
            if not (r.status_code == 200 and ans.get("unchanged")):
                row["header_need"] = ans.get("need") or r.status_code
                if row.get("header_expect_unchanged"):
                    row["header_anomaly"] = True
                    log.warning("vct_pilot: %s header probe answered %s %s although umfeld last saw this digest %s from us",
                                kg, r.status_code, json.dumps(ans, separators=(",", ":"))[:160], prev_digest)
                r = None
        if r is None:
            r, a2 = _contrib_post(url, json.dumps(rep, separators=(",", ":")), h, deadline, kg, "report")
            attempt += a2
            ans = _json(r)
        row["report_http"] = r.status_code
        row["report_attempts"] = attempt
        if r.status_code == 429:
            row["report_retry_after"] = r.headers.get("Retry-After")
        # umfeld 2026-10-08: ``baseline_digest`` / ``baseline_bbox`` = the
        # first-seen *peer* report under the current bbox layout (epoch 0 is
        # re-anchored per chunk on it; ``our_digest`` is the same value, kept
        # for compat — it is NOT a build of umfeld's).  ``chunks_confirmed`` =
        # chunks whose statistics got their second sighting with this report
        # (§5: other observer at once / same observer >= 20 h later) — a
        # header-only ``unchanged:true`` confirms every chunk of our last
        # chunk-bearing report under that digest.
        keep = ("baseline", "compared", "identical_source", "chunks_compared", "chunks_same",
                "chunks_unknown_to_us", "chunks_baselined_now", "unchanged", "token", "our_digest",
                "identical_kg_digest", "baseline_digest", "baseline_bbox", "chunks_confirmed",
                "chunks_held", "header_only", "unchanged_since", "qc_same_input", "tiles_attached")
        rr = {k: ans.get(k) for k in keep if k in ans}
        for k in ("chunks_changed", "chunks_unknown_to_us", "chunks_baselined_now", "chunks_confirmed"):
            if isinstance(ans.get(k), list):
                rr[k] = len(ans[k])
        sl = ans.get("since_last")
        if isinstance(sl, dict):
            rr["since_last_changed"] = len(sl.get("changed") or []) if isinstance(sl.get("changed"), list) else sl.get("changed")
            if sl.get("compared") is not None:
                rr["since_last_compared"] = sl.get("compared")
        # umfeld rule 4: ``coverage{}`` (the operator's per-bbox coverage
        # verdict: best cells_n seen for this bbox, ours, lossy?) and
        # ``change_suspect`` (e.g. "coverage_lossy": the digest difference is
        # an artefact of a truncated build, NOT land-use change).  A report
        # that carries change_suspect never surfaces "change": the changed
        # counts are moved aside as ``*_raw`` and the row says why.
        cov = ans.get("coverage")
        if isinstance(cov, dict):
            rr["coverage"] = {k: cov.get(k) for k in list(cov)[:12]}
        suspect = ans.get("change_suspect")
        if suspect:
            rr["change_suspect"] = str(suspect)[:60]
            for k in ("chunks_changed", "since_last_changed"):
                if k in rr:
                    rr[k + "_raw"] = rr[k]
                    rr[k] = 0
            log.warning("vct_pilot: %s report carries change_suspect=%s (coverage=%s) — changed chunks "
                        "(%s / since_last %s) are NOT change and are suppressed", kg, suspect,
                        json.dumps(rr.get("coverage"), separators=(",", ":")) if rr.get("coverage") else None,
                        rr.get("chunks_changed_raw"), rr.get("since_last_changed_raw"))
        elif cov:
            log.info("vct_pilot: %s coverage=%s", kg, json.dumps(rr.get("coverage"), separators=(",", ":")))
        # umfeld §5a: ``qc_same_input`` = chunks whose digest_ap moved while
        # the source-tile hash did not — a nondeterministic build or a stale
        # cached cell mixed with fresh tiles on OUR side.  Never change; the
        # KG + bbox is what the operator wants to see.
        qsi = ans.get("qc_same_input")
        if isinstance(qsi, list):
            qsi = len(qsi)
        try:
            qsi = int(qsi or 0)
        except Exception:
            qsi = 0
        rr["qc_same_input"] = qsi
        if qsi:
            log.warning("vct_pilot: %s QC-SAME-INPUT: %d chunk(s) changed digest with identical source tiles "
                        "(bbox=%s tiles=%s cached_docs=%s refetch=%s) — nondeterministic build or stale cell mixed with fresh tiles",
                        kg, qsi, json.dumps([round(float(v), 5) for v in input_bbox]), json.dumps(row.get("tiles_qc")),
                        row.get("cached_docs"), row.get("epoch_refetch"))
        if ans.get("tiles_attached"):
            log.info("vct_pilot: %s tile hashes attached to %s held chunk version(s) (late attachment)", kg, ans.get("tiles_attached"))
        row["report"] = rr
        if os.environ.get("VCT_REPORT_RAW") == "1":      # debugging: the whole /contrib answer
            row["report_raw"] = ans
        row["ok"] = r.ok
        if not r.ok:
            row["error"] = f"report HTTP {r.status_code}: {r.text[:160]}"
        # --- change protocol step 2: the rows umfeld asked for ---
        want = ans.get("want_chunks") if r.ok else None
        if isinstance(want, list) and want:
            try:
                row["chg"] = _post_chunks(kg, row["observer"], sec, want, h, deadline)
            except Exception as e:  # noqa: BLE001
                row["chg"] = {"want": len(want), "posted": 0, "stored": 0, "ok": False,
                              "error": f"{type(e).__name__}: {str(e)[:120]}"}
            if not row["chg"].get("ok"):
                # the report itself landed; the chunk rows did not — the row
                # stays ok but is NOT a fleet-wide "reported" (vct_fleet
                # ignores chg_ok=False) so a sibling block / the next pass
                # sends them again
                log.warning("vct_pilot: %s change step 2 incomplete: %s", kg,
                            json.dumps(row["chg"], separators=(",", ":")))
        elif r.ok:
            row["chg"] = {"want": 0, "posted": 0, "stored": 0, "ok": True}
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


def _run_helper(kg: str, bbox: tuple, umfeld_nec1: bytes | None, extra: list[str] | None = None,
                cadastre: dict | None = None) -> dict:
    """Run the bounded helper subprocess; always returns a row (recorded).
    With *cadastre* (a dict to fill), the helper streams the KG's parcels +
    footprints back over an anonymous pipe → ``cadastre['cad']`` (never a
    file: the VTC posture)."""
    kg = str(kg).zfill(5)
    row = None
    rfd = wfd = None
    reader = None
    try:
        cmd = [sys.executable, str(Path(__file__).resolve()), "run", "--kg", kg,
               "--bbox", ",".join(f"{float(v):.6f}" for v in bbox), "--observer", _observer(), "--stdin-nec1"]
        cmd += list(extra or [])
        pass_fds: tuple = ()
        if cadastre is not None:
            rfd, wfd = os.pipe()
            cmd += ["--cadastre-fd", str(wfd)]
            pass_fds = (wfd,)
            reader = threading.Thread(target=_cadastre_from_fd, args=(rfd, cadastre), daemon=True)
            reader.start()
        p = subprocess.run(cmd, input=umfeld_nec1 or b"", capture_output=True,
                           timeout=TIMEOUT_S + 60 + _regen_slack_s(),
                           cwd=str(Path(__file__).resolve().parent), pass_fds=pass_fds)
        if wfd is not None:
            os.close(wfd); wfd = None
        out = p.stdout.decode("utf-8", "replace").strip().splitlines()
        if out:
            row = json.loads(out[-1])
        else:
            err = p.stderr[-200:].decode("utf-8", "replace")
            if p.returncode == -9:
                # SIGKILL with no stderr = the cgroup OOM killer (8 GB scope shared
                # with the KG worker + bevdirect-serve), never our timeout path
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


# === SECTION: declared layer for the product (ne_source=bevdirect) ===

NEC_DIR = AP_DIR / "ne_declared"                # <kg>.nec — NEC1 only (statistics), reused by retries ≤ NEC_KEEP_S
NEC_KEEP_S = 48 * 3600


class DeclaredDeficient(RuntimeError):
    """The bevdirect build of this parent would lose cells: at least one of
    the KG's own parcels is wider than a bevdirect cell + pad and has no
    complete copy (alpine Almen / forests — 75110: 13 parcels, 4 024 cells
    fewer than umfeld's ``lu``).  ``ne_cells`` (frozen, ne-cells-2 /
    bevdirect@v0.3.x) keeps one clipped piece per parcel, so the declared
    layer must come from umfeld's ``lu`` for this parent (via the director's
    relay); the cadastre for the pipeline is unaffected (pieces unioned)."""

    def __init__(self, kg: str, incomplete: int):
        super().__init__(f"{kg}: {incomplete} own parcel(s) clipped by the bevdirect cell grid — declared layer from lu")
        self.incomplete = incomplete


# Off by default since the per-cell request pattern: 75110 (13 clipped own
# parcels) builds 168 243 cells vs lu's 168 225 — parity.  Set
# VCT_LU_ON_CLIPPED=1 to send parents with clipped own parcels to lu again.
LU_ON_CLIPPED = os.environ.get("VCT_LU_ON_CLIPPED", "0") == "1"


def _hybrid_check(kg: str, row: dict | None) -> None:
    if not LU_ON_CLIPPED:
        return
    cad = (row or {}).get("cad") or {}
    inc = int(cad.get("incomplete_own") or 0)
    if inc > 0:
        raise DeclaredDeficient(kg, inc)


def _report_policy(kg: str, is_block: bool, bev_mode: bool) -> list[str]:
    """Helper flags for the /contrib report of *kg* under the change protocol
    (umfeld §5: a chunk's new statistics are *confirmed* by a second observer at
    once, or by the same observer on a later pass ≥ 20 h).

    * THIS peer posted the parent < 20 h ago (``self_reported``, local ring —
      the v2-upgrade path builds the layer twice per unit: parent
      ``v2_fetch`` + the subprocess's cadastre step 2–4 min later, and the
      director's ``vct_fleet`` view lags behind) → ``--skip-report``: umfeld
      would count it as the same pass, confirm nothing and just take the
      chunk list again (the Oct-8/9 double posts of 51022 / 06227 / 67310);
    * nobody of ours reported the parent (30 d, our bevdirect tag) → the
      full report preceded by a header probe (step 0 costs ~300 B; a 202
      ``need:"chunks"`` there is the expected first-pass answer);
    * someone of ours reported it (same observer ≥ 20 h ago or another
      peer — the header key is token + source class + algo + digest +
      epoch, observer is not in it) and it is not yet confirmed by a second
      sighting → ``--header-first`` with ``--prev-digest``: ``unchanged:true``
      is a few hundred bytes AND counts as the second sighting of every
      chunk (``chunks_confirmed``); a different digest gets 202 need:chunks
      → the full report, whose matching ``chunks_ap`` rows confirm, the
      rest is asked for in step 2.  In bev mode the block fetches the
      viewport anyway (it needs the cadastre), so this costs only the probe;
    * ≥ 2 observers already → ``--skip-report`` (nothing left to confirm);
      so does a non-bev peer when another observer already reported (the
      viewport fetch would be for the report alone).
    """
    mine = self_reported(kg)
    if mine is not None:
        age_min = (time.time() - float(mine.get("ts") or 0)) / 60
        log.info("vct_pilot: %s posted by this peer %.0f min ago (http=%s digest=%s) — same pass (<20 h), report skipped",
                 kg, age_min, mine.get("report_http"), mine.get("digest"))
        return ["--skip-report", f"self-reported {age_min:.0f} min ago (same pass <20h, digest {mine.get('digest')})"]
    prev = already_reported(kg)
    if prev is None:
        return ["--header-first"]
    same = prev.get("observer") == _observer()
    n_obs = int(prev.get("n_obs") or 1)
    if not prev.get("prov"):
        # umfeld §5a late attachment: every KG baselined before 2026-10-15
        # holds chunk versions WITHOUT a tile hash; only a chunk-bearing
        # report (step 1 with chunks_tiles) attaches it — a header probe
        # (unchanged:true) never does.  One full report per parent, then the
        # fleet map carries prov=true and the cheap probe path returns.
        log.info("vct_pilot: %s reported by %s without tile provenance — full report once (late tile-hash attachment)",
                 kg, prev.get("observer"))
        return ["--full-once"]
    if not same and (n_obs >= 2 or not bev_mode):
        return ["--skip-report", f"fleet-reported by {prev.get('observer')} at {prev.get('received_at')}"
                                 f" ({n_obs} observer{'s' if n_obs != 1 else ''})"]
    if not same:
        log.info("vct_pilot: %s reported by %s at %s — second observer %s probes/reports too (change confirmation)",
                 kg, prev.get("observer"), prev.get("received_at"), _observer())
    out = ["--header-first"]
    if prev.get("digest"):
        out += ["--prev-digest", str(prev["digest"])]
    return out


def build_declared(kg: str, bbox: tuple, cadastre: dict | None = None, is_block: bool = False) -> bytes:
    """NEC1 bytes for *kg* from the public BEV vector tiles — the SAME run as
    the pilot (one viewport grid, cells in memory, digests reported to
    umfeld-at ``/contrib``), synchronously.  The section (statistics only) is
    kept on disk for ``NEC_KEEP_S`` so a retried KG (or a second block on
    this peer) reuses it.

    *cadastre* (bev mode, ``austria_processor.fetch_cadastre_data_bevdirect``):
    a dict the helper fills with ``cad`` = the KG's parcels + the bbox's
    footprints, over a pipe — the one viewport grid feeds both the NE build
    and the pipeline.  When it is requested the helper always runs (the
    server answers warm cells from RAM, so a reuse costs no BEV traffic);
    an existing fresh NEC1 is kept (digest logged if the rebuild differs).

    Raises ``DeclaredDeficient`` when the build would lose cells (see the
    class) — the cadastre is still delivered — and ``RuntimeError`` on any
    other failure; ``ne_h3.fetch_declared`` falls back to umfeld's ``lu``
    through the director."""
    kg = str(kg).zfill(5)
    fp = NEC_DIR / f"{kg}.nec"
    fresh = False
    try:
        fresh = fp.exists() and time.time() - fp.stat().st_mtime < NEC_KEEP_S and fp.stat().st_size > 64
    except Exception:
        fresh = False
    if fresh and cadastre is None:
        log.info("vct_pilot: declared layer %s reused from %s (%.0f min old)", kg, fp,
                 (time.time() - fp.stat().st_mtime) / 60)
        _hybrid_check(kg, recent_for(kg, NEC_KEEP_S))
        return fp.read_bytes()
    if fresh:
        # cadastre requested + fresh NEC1 kept (a sibling block / retry on
        # this peer): fetch the (warm) cells for the pipeline only — no
        # rebuild (10+ min / GBs on a 1.5 M-cell parent), no report.
        log.info("vct_pilot: declared layer %s reused from %s (%.0f min old) — helper streams the cadastre only", kg, fp,
                 (time.time() - fp.stat().st_mtime) / 60)
        row = _run_helper(kg, bbox, None, ["--cadastre-only"], cadastre=cadastre)
        _log_row(row)
        if not row.get("ok"):
            raise RuntimeError(f"vct cadastre fetch failed: {row.get('error') or 'helper failed'}")
        _hybrid_check(kg, recent_for(kg, NEC_KEEP_S))
        return fp.read_bytes()
    extra = ["--out", str(fp)] + _report_policy(kg, is_block, bev_mode=True)
    t_run = time.time()
    row = _run_helper(kg, bbox, None, extra, cadastre=cadastre)
    # Helper killed by TIMEOUT_S *after* it wrote the NEC1 (huge parents —
    # Wildalpen 67112, 666 k cells: the build lands at ~1400 s and the
    # compare + /contrib report + change chunks overrun the budget).  The
    # declared layer on disk is complete (tmp → rename), only the report is
    # lost: use it instead of failing the attempt (which cost every block
    # a 20 s sleep + a second helper, and logged a FAILED row per block).
    if (not row.get("nec_out") and str(row.get("error") or "").startswith("helper timeout")):
        try:
            if fp.exists() and fp.stat().st_mtime >= t_run - 1 and fp.stat().st_size > 64:
                row.update(nec_out=str(fp), declared=True, report_lost="helper timeout after NEC1 written")
                log.warning("vct_pilot: %s helper timed out after writing the NEC1 (%.0f s) — "
                            "declared layer kept, /contrib report lost (re-reported by the next observer)",
                            kg, time.time() - t_run)
        except Exception:
            pass
    _log_row(row)
    # the NEC1 is written before the /contrib report — a 429 on the report
    # must not cost the product its declared layer
    if not row.get("nec_out") or not fp.exists():
        raise RuntimeError(f"vct declared build failed: {row.get('error') or 'no NEC1 written'}")
    _hybrid_check(kg, row)
    return fp.read_bytes()


def run_async(kg: str, bbox: tuple, umfeld_nec1: bytes | None, *, on_done=None,
              is_block: bool = False) -> threading.Thread | None:
    """Processor hook: launch the helper subprocess in a thread (non-daemon so
    the KG subprocess waits ≤ timeout for the row; the row is written by the
    helper, so a dead parent loses nothing).  ``None`` when disabled / deduped."""
    if not enabled():
        return None
    kg = str(kg).zfill(5)
    prev = recent_for(kg)
    if prev is not None:
        log.info("vct_pilot: %s already run %.0f min ago on this peer (ok=%s) — reusing", kg,
                 (time.time() - float(prev.get("ts") or 0)) / 60, prev.get("ok"))
        return None
    prev = already_reported(kg)
    if is_block and prev is not None and prev.get("observer") != _observer():
        # pilot-only mode: the viewport is fetched for the report alone —
        # another observer already holds it; the ≥2-observer confirmation
        # is left to bev-mode peers (which fetch the viewport anyway)
        log.info("vct_pilot: %s already reported fleet-wide by %s at %s (%s chunks) — block skips the parent viewport",
                 kg, prev.get("observer"), prev.get("received_at"), prev.get("chunks"))
        record({"ts": round(time.time(), 1), "kg": kg, "observer": _observer(), "ok": True, "skipped": "fleet-reported",
                "by": prev.get("observer"), "requests": 0, "tiles": 0, "tile_bytes": 0, "net_bytes_est": 0, "wall_s": 0})
        return None
    extra = ["--header-first"]
    if prev is not None and prev.get("digest"):
        extra += ["--prev-digest", str(prev["digest"])]
    if prev is not None and not prev.get("prov"):
        extra = ["--full-once"]          # late tile-hash attachment (see _report_policy)
    mine = self_reported(kg)
    if mine is not None:
        extra = ["--skip-report", f"self-reported {(time.time() - float(mine.get('ts') or 0)) / 60:.0f} min ago (same pass <20h)"]

    def _go():
        row = _run_helper(kg, bbox, umfeld_nec1, extra)
        _log_row(row)
        if on_done:
            try:
                on_done(row)
            except Exception:
                pass

    t = threading.Thread(target=_go, name=f"vct-pilot-{kg}", daemon=False)
    t.start()
    return t


def _log_row(row: dict) -> None:
    kg = row.get("kg")
    if row.get("ok"):
        c = row.get("cmp") or {}
        rp = row.get("report") or {}
        log.info("vct_pilot: %s %s cells=%s requests=%s tiles=%s tile_bytes=%.1fMB net~%.1fMB fetch=%ss build=%ss"
                 " | vs umfeld: common=%s only_ours=%s only_umfeld=%s dom_agree=%s%% within_tol=%s%%"
                 " | report: http=%s baseline=%s compared=%s same=%s changed=%s unknown=%s%s",
                 kg, row.get("source"), row.get("cells"), row.get("requests"), row.get("tiles"),
                 (row.get("tile_bytes") or 0) / 1e6, (row.get("net_bytes_est") or 0) / 1e6, row.get("fetch_s"),
                 row.get("build_s"), c.get("common"), c.get("only_ours"), c.get("only_umfeld"), c.get("dom_agree_pct"),
                 c.get("within_tol_pct"), row.get("report_http"), rp.get("baseline"), rp.get("compared"),
                 rp.get("chunks_same"), rp.get("chunks_changed"), rp.get("chunks_unknown_to_us"),
                 (" change_suspect=" + str(rp.get("change_suspect"))) if rp.get("change_suspect") else "",
                 )
        if row.get("tiles_qc"):
            tq = row["tiles_qc"]
            log.info("vct_pilot: %s qc: tiles %s/%s hash=%s chunks_tiles=%s qc_same_input=%s%s", kg, tq.get("fetched"),
                     tq.get("expected"), tq.get("hash"), row.get("chunks_tiles"), rp.get("qc_same_input"),
                     " FULL-ONCE(tile-hash attachment)" if row.get("full_once") else "")
        ch = row.get("chg")
        if ch and (ch.get("want") or row.get("header_probe")):
            log.info("vct_pilot: %s change: header=%s want=%s posted=%s stored=%s seen=%s rejected=%s deltas=%s bytes=%.2fMB parts=%s ok=%s%s",
                     kg, row.get("header_probe"), ch.get("want"), ch.get("posted"), ch.get("stored"), ch.get("seen"),
                     ch.get("rejected"), ch.get("deltas"), (ch.get("bytes") or 0) / 1e6, ch.get("parts"), ch.get("ok"),
                     (" error=" + str(ch.get("error"))) if ch.get("error") else "")
    else:
        log.warning("vct_pilot: %s FAILED after %ss: %s", kg, row.get("wall_s"), row.get("error"))


# === SECTION: peer status summary (shipped in the status push, ~250 B) ===

def compact(row: dict) -> dict:
    """~220 B view of a row for the status push / director table."""
    c = row.get("cmp") or {}
    rp = row.get("report") or {}
    return {"kg": row.get("kg"), "ok": bool(row.get("ok")), "ts": int(float(row.get("ts") or 0)),
            "src": row.get("source"), "cells": row.get("cells"), "req": row.get("requests"),
            "tiles": row.get("tiles"), "tile_mb": round(float(row.get("tile_bytes") or 0) / 1e6, 1),
            "net_mb": round(float(row.get("net_bytes_est") or 0) / 1e6, 1), "wall_s": row.get("wall_s"),
            "lru_mb": row.get("lru_mb"), "lru_max_mb": row.get("lru_max_mb"),
            "rss_mb": row.get("rss_peak_mb"), "tiled": row.get("tiled"), "cad_only": row.get("cadastre_only"),
            "common": c.get("common"), "umf": c.get("umfeld"), "dom": c.get("dom_agree_pct"), "tol": c.get("within_tol_pct"),
            "http": row.get("report_http"), "base": rp.get("baseline"), "same": rp.get("chunks_same"),
            "chg": rp.get("chunks_changed"), "unk": rp.get("chunks_unknown_to_us"), "digest": row.get("digest"),
            "suspect": rp.get("change_suspect"), "cov_lossy": (rp.get("coverage") or {}).get("lossy"),
            "bbox": row.get("input_bbox"), "partial": row.get("partial_viewport"),
            "obs": row.get("observer"), "decl": bool(row.get("declared")) or None,
            "err": (row.get("error") or "")[:80] or None, "skipped": row.get("skipped"), "by": row.get("by"),
            "cad": ({k: (row.get("cad") or {}).get(k) for k in ("parcels_own", "incomplete_own", "footprints_bbox")}
                    if row.get("cad") else None),
            "skip_rep": (row.get("skipped_report") or "")[:40] or None,
            # change protocol (0.2.0): ap version, header probe, step-2 counts
            "ap": row.get("ap"), "hdr": row.get("header_probe"),
            "confirmed": (row.get("report") or {}).get("chunks_confirmed"),
            "chg_want": (row.get("chg") or {}).get("want"), "chg_stored": (row.get("chg") or {}).get("stored"),
            "chg_rej": (row.get("chg") or {}).get("rejected"), "chg_deltas": (row.get("chg") or {}).get("deltas"),
            "chg_mb": round(float((row.get("chg") or {}).get("bytes") or 0) / 1e6, 2),
            "chg_ok": (row.get("chg") or {}).get("ok") if row.get("chg") else None,
            # QC (umfeld §5a, vtcseamless-py 0.3.0): tile provenance of the
            # build, whether the report carried chunks (= could attach tile
            # hashes), server verdicts that are NOT change
            "tq": ([int((row.get("tiles_qc") or {}).get("fetched") or 0), int((row.get("tiles_qc") or {}).get("expected") or 0)]
                   if row.get("tiles_qc") else None),
            "thash": (row.get("tiles_qc") or {}).get("hash"),
            "full": bool(row.get("report_http")) and not (row.get("header_probe") == 200 and not row.get("header_need")),
            "tinc": True if row.get("tiles_incomplete") else None,
            "qc_same": rp.get("qc_same_input") or None,
            "d_pipe": (row.get("chg") or {}).get("deltas_pipeline") or None,
            "d_jit": (row.get("chg") or {}).get("deltas_jitter") or None,
            "prov_missing": (row.get("prov_missing") or "")[:40] or None}


def summary(recent_n: int = 6) -> dict:
    rs = rows()
    now = time.time()
    d1 = [r for r in rs if now - float(r.get("ts") or 0) < 86400 and not r.get("skipped")]
    ok = [r for r in d1 if r.get("ok")]
    out = {"enabled": enabled(), "n": len(rs), "n_24h": len(d1), "ok_24h": len(ok),
           "fail_24h": len(d1) - len(ok),
           "skipped_24h": len([r for r in rs if now - float(r.get("ts") or 0) < 86400 and r.get("skipped")]),
           "net_mb_24h": round(sum(float(r.get("net_bytes_est") or 0) for r in d1) / 1e6, 1),
           "tile_mb_24h": round(sum(float(r.get("tile_bytes") or 0) for r in d1) / 1e6, 1),
           "requests_24h": sum(int(r.get("requests") or 0) for r in d1),
           "net_mb_total": round(sum(float(r.get("net_bytes_est") or 0) for r in rs) / 1e6, 1),
           # change protocol telemetry (24 h): reports that carried chunks_ap,
           # chunks umfeld asked for / we stored, upload bytes, header probes
           "ap_24h": len([r for r in ok if r.get("ap")]),
           "chg_want_24h": sum(int((r.get("chg") or {}).get("want") or 0) for r in ok),
           "chg_stored_24h": sum(int((r.get("chg") or {}).get("stored") or 0) for r in ok),
           "chg_rejected_24h": sum(int((r.get("chg") or {}).get("rejected") or 0) for r in ok),
           "chg_deltas_24h": sum(int((r.get("chg") or {}).get("deltas") or 0) for r in ok),
           "chg_fail_24h": len([r for r in ok if r.get("chg") and r["chg"].get("ok") is False]),
           "chg_mb_24h": round(sum(float((r.get("chg") or {}).get("bytes") or 0) for r in ok) / 1e6, 2),
           "hdr_unchanged_24h": len([r for r in ok if r.get("header_probe") == 200 and not r.get("header_need")]),
           "hdr_need_24h": len([r for r in ok if r.get("header_probe") and r.get("header_need")]),
           "hdr_anomaly_24h": len([r for r in ok if r.get("header_anomaly")]),
           "self_dedupe_24h": len([r for r in rs if now - float(r.get("ts") or 0) < 86400
                                   and str(r.get("skipped_report") or "").startswith("self-reported")]),
           "chunks_confirmed_24h": sum(int((r.get("report") or {}).get("chunks_confirmed") or 0) for r in ok),
           # QC (umfeld §5a): provenance coverage + the three not-change verdicts
           "prov_24h": len([r for r in ok if r.get("tiles_qc")]),
           "prov_missing_24h": len([r for r in ok if r.get("prov_missing")]),
           "tiles_incomplete_24h": len([r for r in d1 if r.get("tiles_incomplete")]),
           "qc_same_input_24h": sum(int((r.get("report") or {}).get("qc_same_input") or 0) for r in ok),
           "qc_same_kgs_24h": [{"kg": r.get("kg"), "n": int((r.get("report") or {}).get("qc_same_input") or 0),
                                "bbox": r.get("input_bbox")} for r in ok if int((r.get("report") or {}).get("qc_same_input") or 0)][-8:],
           "deltas_pipeline_24h": sum(int((r.get("chg") or {}).get("deltas_pipeline") or 0) for r in ok),
           "deltas_jitter_24h": sum(int((r.get("chg") or {}).get("deltas_jitter") or 0) for r in ok),
           "full_once_24h": len([r for r in ok if r.get("full_once")]),
           "recent": [compact(r) for r in rs[-recent_n:]]}
    try:
        import ne_h3
        bd = ne_h3.bevdirect_ready(probe_server=False)
        out["tooling"] = bool(bd.get("binary") and bd.get("package"))
        out["ne_source"] = ne_h3.ne_source()
        out["declared_24h"] = len([r for r in d1 if r.get("declared")])
        out["cadastre_24h"] = len([r for r in d1 if r.get("cad")])
        out["lu_fallback_24h"] = len([r for r in d1 if int(((r.get("cad") or {}).get("incomplete_own")) or 0) > 0])
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
    r.add_argument("--stdin-nec1", action="store_true", help="umfeld NEC1 bytes on stdin (optional, for the comparison)")
    r.add_argument("--umfeld", action="store_true", help="fetch umfeld's declared layer for the comparison")
    r.add_argument("--no-record", action="store_true")
    r.add_argument("--out", help="write the NEC1 section here (statistics only) — the product's declared layer")
    r.add_argument("--cadastre-fd", type=int, help="bev mode: stream the KG's parcels + footprints (gzip JSON) into this inherited pipe fd")
    r.add_argument("--skip-report", help="do not POST the /contrib report (reason recorded in the row)")
    r.add_argument("--header-first", action="store_true",
                   help="change protocol step 0: header-only report first (unchanged:true short-circuits the chunk table)")
    r.add_argument("--prev-digest", help="digest umfeld last saw from us for this KG (header probe telemetry: a 202 for the same digest is an anomaly)")
    r.add_argument("--full-once", action="store_true",
                   help="chunk-bearing report without the header probe (umfeld §5a late attachment of tile hashes)")
    r.add_argument("--cadastre-only", action="store_true",
                   help="bev mode with a fresh kept NEC1: stream the cadastre, skip the cell build and the report")
    sub.add_parser("status")
    sub.add_parser("rows")
    a = ap.parse_args(argv)
    if a.cmd == "run":
        logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
        nec = None
        if a.stdin_nec1:
            nec = sys.stdin.buffer.read() or None
        elif a.umfeld:
            import ne_h3
            try:
                sec = ne_h3.fetch_declared_umfeld(a.kg)
                nec = ne_h3.pack_section(ne_h3.MAGIC_LU, sec.header, sec.raw)
            except Exception as e:  # noqa: BLE001
                print(f"umfeld declared layer unavailable: {e}", file=sys.stderr)
                nec = None
        row = run(a.kg, tuple(float(v) for v in a.bbox.split(",")), nec, a.observer, out_nec=a.out,
                  cadastre_fd=a.cadastre_fd, skip_report=a.skip_report, header_first=a.header_first and not a.full_once,
                  prev_digest=a.prev_digest, full_once=a.full_once, cadastre_only=a.cadastre_only)
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
