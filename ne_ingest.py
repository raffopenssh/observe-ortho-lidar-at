"""ne_ingest — primary-side ingest of the fleet's ``<code>_ne.h3b`` products.

Runs as a daemon thread inside ``app.py`` (primary only, every ``TICK_S``,
single-flight across the two gunicorn workers).  Per tick:

1. ``pending()`` — committed manifest ``<code>_ne_h3b`` entries whose
   ``uploaded_at`` is newer than what ``data/ne/index.json`` has.
2. Pull up to the **inbound budget** (``inbound_budget`` channel ``ne``,
   ``ne_ingest_mb_per_h`` knob, default :data:`MB_PER_H_DEFAULT`):
   **peer first** — the uploading peer keeps the file for ``NE_KEEP_DAYS``
   (``product_origin`` says who; ``GET <peer>/api/v1/processing/product``)
   — Zenodo as fallback (one shared token ≈ 5000 req/h is the fleet-wide
   bottleneck, so the peer path burns none of it).  md5 against the
   manifest checksum, ``ne_h3.verify_h3b`` structural check, then written
   to ``data/ne/<code>.h3b`` (unsplit parents) or ``data/ne/blocks/
   <block>.h3b``.
3. **Split parents**: every block of a split KG carries the parent's full
   cell set (unseen cells = nodata, ``partial_cells: true``).  Once ≥1
   block is on disk the blocks are merged per cell (the block that observed
   the cell; overlap → larger land-use mass) into ``data/ne/<parent>.h3b``
   (``merged_from`` header).  When the parent is complete (coverage oracle
   + every live block's h3b ingested) the merged NEO1 is POSTed to umfeld.
4. **umfeld confirmation**: umfeld's ``/ne/manifest`` (cached by
   ``ne_rollout.refresh_ne_manifest``) carries ``obs.digest`` per KG; for
   every parent on disk the stored digest must equal our NEO1 digest —
   missing / mismatching → re-POST from the h3b (``post_attempts`` capped,
   exponential backoff).  Peers POST NEO1 for unsplit KGs themselves; this
   is the safety net ("just in case" the POST failed / umfeld lost it).

State: ``data/ne/index.json`` = ``{"files": {code: {uploaded_at, size, md5,
src, peer, at, kind}}, "parents": {parent: {blocks, digest, complete,
posted_digest, posted_at, post_attempts, post_next}}, "stats": {...}}``.
Readers of the corpus: ``ne_h3.read_h3b_header`` / ``read_h3b_chunk`` (see
docs/ne-primary-api.md).
"""
from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
import zlib
from pathlib import Path

import numpy as np

log = logging.getLogger(__name__)

DATA_DIR = Path("data/austria_processor")
MANIFEST_PATH = DATA_DIR / "zenodo_manifest.json"
NE_DIR = Path("data/ne")
BLOCK_DIR = NE_DIR / "blocks"
INDEX_FILE = NE_DIR / "index.json"
TICK_S = 300
MB_PER_H_DEFAULT = 15.0            # primary inbound budget for h3b pulls (metered VM)
MAX_PER_TICK = 40
PEER_TIMEOUT = 60
ZENODO_TIMEOUT = 120
MAX_FAILS = 4
POST_MAX_ATTEMPTS = 6
POST_BACKOFF_S = 1800
POST_GIB_PER_DAY = 2.0             # umfeld /contrib token limit is 4 GiB/day; a full resync (~2.4 GB) spreads over ≥1 day
POST_MIN_GAP_S = 0.6               # umfeld: ≤2 POST/s
CHANNEL = "ne"

_lock = threading.Lock()
_last_tick: dict = {}


# === SECTION: index ===

def _load_index() -> dict:
    try:
        d = json.loads(INDEX_FILE.read_text())
        if isinstance(d, dict):
            d.setdefault("files", {}); d.setdefault("parents", {}); d.setdefault("stats", {})
            d.setdefault("failed", {})
            return d
    except Exception:
        pass
    return {"files": {}, "parents": {}, "stats": {}, "failed": {}}


def _save_index(d: dict) -> None:
    NE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = INDEX_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(d, indent=1, sort_keys=True, default=str))
    tmp.replace(INDEX_FILE)


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _knob(key: str, default):
    try:
        import peer_director as _pd
        return _pd.knob(None, key, default)
    except Exception:
        return default


def load_manifest_entries(path: Path = MANIFEST_PATH) -> dict:
    """Shared, read-only view (mtime-cached via :mod:`json_cache`)."""
    try:
        import json_cache
        raw = json_cache.load(path)
        if not isinstance(raw, dict):
            return {}
        return raw.get("entries", raw) or {}
    except Exception:
        return {}


def load_index_ro() -> dict:
    """Read-only, mtime-cached view of the index for status/dashboard
    renders (``_load_index`` returns a fresh mutable copy for the
    ingest tick, which writes it back)."""
    try:
        import json_cache
        d = json_cache.load(INDEX_FILE)
        if isinstance(d, dict) and "files" in d:
            return d
    except Exception:
        pass
    return {"files": {}, "parents": {}, "stats": {}, "failed": {}}


def _committed(e) -> bool:
    return isinstance(e, dict) and int(e.get("size") or 0) > 0 and bool(e.get("uploaded_at")) \
        and "error" not in str(e.get("status") or "") and str(e.get("status") or "") != "removed"


def is_block(code: str) -> bool:
    return "-" in code


def parent_of(code: str) -> str:
    return code.split("-", 1)[0]


def path_for(code: str) -> Path:
    return (BLOCK_DIR / f"{code}.h3b") if is_block(code) else (NE_DIR / f"{code}.h3b")


def pending(entries: dict | None = None, idx: dict | None = None) -> list[tuple[str, dict]]:
    """``[(code, entry)]`` oldest upload first."""
    entries = entries if entries is not None else load_manifest_entries()
    idx = idx if idx is not None else _load_index()
    files, failed = idx["files"], idx.get("failed") or {}
    out = []
    for k, e in entries.items():
        if not k.endswith("_ne_h3b") or not _committed(e):
            continue
        code = k[:-7]
        have = files.get(code) or {}
        if have.get("uploaded_at") == e.get("uploaded_at") and path_for(code).exists():
            continue
        f = failed.get(code) or {}
        if f.get("uploaded_at") == e.get("uploaded_at") and int(f.get("n") or 0) >= MAX_FAILS:
            continue
        out.append((code, e))
    out.sort(key=lambda ce: str(ce[1].get("uploaded_at") or ""))
    return out


# === SECTION: pull (peer first, Zenodo fallback) ===

def _peer_url(peer_id: str) -> str | None:
    try:
        import peer_director as _pd
        cfg = _pd.load_peers_config()
        p = _pd.get_peer_by_id(cfg, peer_id)
        return (p or {}).get("url") or None
    except Exception:
        return None


def _admin_headers() -> dict:
    try:
        tok = Path("data/admin_token").read_text().strip()
        return {"X-Admin-Token": tok} if tok else {}
    except Exception:
        return {}


def _check_blob(blob: bytes, e: dict) -> None:
    want = int(e.get("size") or 0)
    if want and len(blob) != want:
        raise RuntimeError(f"size {len(blob)} != manifest {want}")
    ck = str(e.get("checksum") or "")
    if ck.startswith("md5:") and hashlib.md5(blob).hexdigest() != ck[4:]:
        raise RuntimeError("md5 mismatch")


def pull(code: str, e: dict) -> tuple[bytes, str, str | None]:
    """→ ``(blob, src, peer)``; ``src`` = ``peer`` | ``zenodo``."""
    import requests
    import product_origin as _po
    key = f"{code}_ne_h3b"
    peer = _po.peer_for(key, e.get("uploaded_at"))
    if peer:
        url = _peer_url(peer)
        if url:
            try:
                r = requests.get(f"{url.rstrip('/')}/api/v1/processing/product", params={"key": key},
                                 headers=_admin_headers(), timeout=PEER_TIMEOUT)
                if r.status_code == 200:
                    _check_blob(r.content, e)
                    return r.content, "peer", peer
                log.info("ne_ingest %s: peer %s has no copy (HTTP %s) — Zenodo fallback", code, peer, r.status_code)
            except Exception as ex:  # noqa: BLE001
                log.info("ne_ingest %s: peer %s pull failed (%s) — Zenodo fallback", code, peer, str(ex)[:120])
    import v2_ingest as _v2i
    link, hdr = _v2i._link(e)
    if not link:
        raise RuntimeError("manifest entry has no download link")
    r = requests.get(link, headers=hdr, timeout=ZENODO_TIMEOUT)
    if r.status_code in (403, 404, 410):
        raise FileNotFoundError(f"HTTP {r.status_code}")
    r.raise_for_status()
    _check_blob(r.content, e)
    return r.content, "zenodo", peer


def ingest_one(code: str, e: dict, idx: dict) -> dict:
    import ne_h3
    t0 = time.time()
    try:
        blob, src, peer = pull(code, e)
    except Exception as ex:  # noqa: BLE001
        f = idx["failed"].get(code) or {}
        if f.get("uploaded_at") != e.get("uploaded_at"):
            f = {"n": 0, "uploaded_at": e.get("uploaded_at")}
        f["n"] = int(f.get("n") or 0) + 1
        f["reason"] = str(ex)[:200]; f["ts"] = _now()
        idx["failed"][code] = f
        return {"code": code, "ok": False, "reason": f"pull: {ex}"}
    import inbound_budget
    inbound_budget.add(CHANNEL, len(blob))
    p = path_for(code)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".h3b.tmp")
    tmp.write_bytes(blob)
    try:
        rep = ne_h3.verify_h3b(tmp)
    except Exception as ex:  # noqa: BLE001
        rep = {"ok": False, "errors": [str(ex)]}
    if not rep.get("ok"):
        tmp.unlink(missing_ok=True)
        f = idx["failed"].get(code) or {"n": 0, "uploaded_at": e.get("uploaded_at")}
        f["n"] = int(f.get("n") or 0) + 1
        f["reason"] = "verify: " + "; ".join(rep.get("errors") or [])[:200]; f["ts"] = _now()
        idx["failed"][code] = f
        log.warning("ne_ingest %s: h3b verify FAILED: %s", code, f["reason"])
        return {"code": code, "ok": False, "reason": f["reason"]}
    tmp.replace(p)
    hdr = rep["header"]
    if not is_block(code):
        _index_cells(p)
    idx["files"][code] = {"uploaded_at": e.get("uploaded_at"), "size": len(blob),
                          "md5": hashlib.md5(blob).hexdigest(), "src": src, "peer": peer,
                          "at": _now(), "kind": "block" if is_block(code) else "parent",
                          "cells_n": hdr.get("cells_n"), "neo1_digest": (hdr.get("neo1") or {}).get("digest"),
                          "nec1_digest": (hdr.get("nec1") or {}).get("digest"),
                          "nec1_source": (hdr.get("nec1") or {}).get("source"),
                          "product_version": hdr.get("product_version")}
    idx["failed"].pop(code, None)
    st = idx["stats"]
    st[f"pulled_{src}"] = int(st.get(f"pulled_{src}") or 0) + 1
    st["bytes"] = int(st.get("bytes") or 0) + len(blob)
    if is_block(code):
        par = idx["parents"].setdefault(parent_of(code), {})
        par["dirty"] = True
    log.info("ne_ingest: %s %s %.2f MB from %s%s (%.1fs)", code, "block" if is_block(code) else "parent",
             len(blob) / 1e6, src, f" {peer}" if peer and src == "peer" else "", time.time() - t0)
    return {"code": code, "ok": True, "size": len(blob), "src": src}


# === SECTION: split-parent merge ===

def read_rows(path: Path) -> dict:
    """Whole h3b → ``{hdr, lu, obs, trees, bld, k_raw, tree_n, bld_n}`` (numpy/bytes)."""
    import base64
    import ne_h3
    hdr = ne_h3.read_h3b_header(path)
    lu = bytearray(); obs = bytearray(); trees = []; bld = []
    tree_n = []; bld_n = []
    rb = hdr.get("record_bytes") or {}
    tb, bb = int(rb.get("net1") or 4), int(rb.get("neb1") or 8)
    for ch in hdr["chunks"]:
        part = ne_h3.read_h3b_chunk(path, hdr, ch)
        lu += part["lu"]; obs += part["obs"]
        n = ch["cells"]
        ob = np.frombuffer(part["obs"], np.uint8).reshape(n, ne_h3.OBS_REC.size)
        tn = ob[:, 22].astype(np.int64); bn = ob[:, 18].astype(np.int64)
        tree_n.append(tn); bld_n.append(bn)
        # split the per-chunk tree / bld rows per cell
        to = np.concatenate([[0], np.cumsum(tn)]); bo = np.concatenate([[0], np.cumsum(bn)])
        tr, bl = part["trees"], part["bld"]
        for i in range(n):
            trees.append(tr[to[i] * tb:to[i + 1] * tb]); bld.append(bl[bo[i] * bb:bo[i + 1] * bb])
    k_raw = zlib.decompress(base64.b64decode(hdr["k_records"]["zlib_b64"]))
    return {"hdr": hdr, "lu": bytes(lu), "obs": bytes(obs), "trees": trees, "bld": bld, "k_raw": k_raw,
            "tree_n": np.concatenate(tree_n) if tree_n else np.zeros(0, np.int64),
            "bld_n": np.concatenate(bld_n) if bld_n else np.zeros(0, np.int64)}


def row_observed(obs: np.ndarray) -> np.ndarray:
    """Per NEO1 row: did the block see this cell?  Unseen rows are all-nodata
    (lu 0, h_mean 255, elev 65535); any of the three present → observed."""
    lu_mass = obs[:, 8:17].astype(np.int32).sum(axis=1)
    h_mean = obs[:, 19]
    elev = obs[:, 30:32].copy().view("<u2").reshape(-1)
    return (lu_mass > 0) | (h_mean != 255) | (elev != 65535)


def summary_from_rows(declared, obs_raw: bytes, trees_n: int, bld_n: int, epoch: str) -> dict:
    import ne_h3
    n = int(declared.header["cells_n"])
    lu_tab = declared.lu_table()
    ob = np.frombuffer(obs_raw, np.uint8).reshape(n, ne_h3.OBS_REC.size)
    observed = row_observed(ob)
    cids = lu_tab["cell_id"]
    area = ne_h3.cell_area_m2(cids)
    share = lu_tab["kg_share"].astype(np.float64) / 255.0
    w = area * share / 10_000.0
    decl = lu_tab["lu"].astype(np.float64) / 255.0
    obsl = ob[:, 8:17].astype(np.float64) / 255.0
    verdict = ob[:, 29]
    elev = ob[:, 30:32].copy().view("<u2").reshape(n).astype(np.float64)
    ev = elev[elev != 65535]
    G = ne_h3.GROUPS; V = ne_h3.VERDICTS
    return {
        "algo": {"declared": declared.header.get("algo"), "observed": ne_h3.ALGO_OBS,
                 "trees": ne_h3.ALGO_TREES, "buildings": ne_h3.ALGO_BLD},
        "record_bytes": {"nec1": ne_h3.LU_REC.size, "neo1": ne_h3.OBS_REC.size, "net1": 4, "neb1": 8},
        "terrain": {"cells_with_terrain": int((elev != 65535).sum()),
                    "elev_min_m": round(float(ev.min()), 1) if ev.size else None,
                    "elev_max_m": round(float(ev.max()), 1) if ev.size else None},
        "declared": {"digest": declared.digest, "epoch": declared.header.get("epoch"),
                     "source": declared.header.get("source"), "cells_n": n,
                     "kcells_n": int(declared.header.get("kcells_n", 0))},
        "observed": {"digest": ne_h3.digest_of(obs_raw), "epoch": epoch, "source": ne_h3.OBS_SOURCE,
                     "cells_observed": int(observed.sum()), "trees_n": int(trees_n), "manmade_n": int(bld_n)},
        "h3_res": ne_h3.H3_RES, "chunk_res": ne_h3.CHUNK_RES, "groups": list(G),
        "declared_ha": {g: round(float((decl[:, i] * w).sum()), 2) for i, g in enumerate(G)},
        "observed_ha": {g: round(float((obsl[:, i] * w).sum()), 2) for i, g in enumerate(G)},
        "verdict_ha": {V[v]: round(float(w[verdict == v].sum()), 2) for v in range(len(V))},
        "verdict_n": {V[v]: int((verdict == v).sum()) for v in range(len(V))},
        "merged": True,
    }


def _read_compact(path: Path, hdr: dict | None = None, *, want_lu: bool = True, want_rows: bool = True) -> dict:
    """Whole h3b in column form — ``obs`` as an (n, OBS_REC) uint8 matrix,
    ``trees`` / ``bld`` as the *raw* concatenated NET1 / NEB1 bytes (never a
    per-cell list of 1.5 M ``bytes`` objects: that is what blew the primary
    past its 4 GB cgroup on 80110 × 20 blocks, 2026-10-18).  ``want_lu`` /
    ``want_rows`` skip layers the caller does not need."""
    import base64
    import ne_h3
    hdr = hdr or ne_h3.read_h3b_header(path)
    lu = bytearray(); obs = bytearray(); trees = bytearray(); bld = bytearray()
    for ch in hdr["chunks"]:
        part = ne_h3.read_h3b_chunk(path, hdr, ch)
        obs += part["obs"]
        if want_lu:
            lu += part["lu"]
        if want_rows:
            trees += part["trees"]; bld += part["bld"]
    n = int(hdr["cells_n"])
    obs_m = np.frombuffer(bytes(obs), np.uint8).reshape(n, ne_h3.OBS_REC.size)
    k_raw = zlib.decompress(base64.b64decode(hdr["k_records"]["zlib_b64"])) if want_lu else b""
    return {"lu": bytes(lu), "obs": obs_m, "trees": bytes(trees), "bld": bytes(bld), "k_raw": k_raw,
            "tree_n": obs_m[:, 22].astype(np.int64), "bld_n": obs_m[:, 18].astype(np.int64)}


def _lu_ids(lu: bytes, n: int) -> np.ndarray:
    import ne_h3
    return np.frombuffer(lu, np.uint8).reshape(n, ne_h3.LU_REC.size)[:, :8].copy().view("<u8").reshape(-1)


def _scatter_rows(out: np.ndarray, out_off: np.ndarray, raw: bytes, src_cnt: np.ndarray,
                  sel: np.ndarray, src_row: np.ndarray, rec: int) -> None:
    """Copy the variable-length record runs of block rows ``src_row[sel]``
    (``raw`` = the block's concatenated records, ``src_cnt`` its per-row
    counts) into ``out`` at record offsets ``out_off[sel]``.  One fancy
    index at record granularity, no per-cell Python loop."""
    if not sel.any():
        return
    j = src_row[sel]
    c = src_cnt[j]
    total = int(c.sum())
    if total == 0:
        return
    src_off = np.concatenate([[0], np.cumsum(src_cnt)]).astype(np.int64)
    before = np.concatenate([[0], np.cumsum(c)])[:-1]
    ar = np.arange(total, dtype=np.int64)
    src_idx = np.repeat(src_off[j] - before, c) + ar
    dst_idx = np.repeat(out_off[sel] - before, c) + ar
    out[dst_idx] = np.frombuffer(raw, np.uint8).reshape(-1, rec)[src_idx]


def merge_blocks(parent: str, block_paths: list[Path], out_path: Path, *, complete: bool) -> dict:
    """Merge the blocks' NEO1/NET1/NEB1 per cell → ``out_path``.  Returns the
    header.  All blocks must share the parent's NEC1 (same digest/cell set).

    Two streaming passes, one block resident at a time: pass 1 picks the
    winning block per cell from NEO1 only (merged matrix + pick vector stay
    resident), pass 2 re-reads each block's NET1/NEB1 raws and scatters the
    picked rows straight into the output record arrays.  A 1.5 M-cell parent
    with 20+ blocks merges in a few hundred MB instead of ~4 GB."""
    import ne_h3
    paths = sorted(block_paths)
    if not paths:
        raise ValueError("no blocks")
    # slim per-block header facts (the full header carries ~30k chunk dicts)
    facts = []
    for p in paths:
        h = ne_h3.read_h3b_header(p)
        facts.append({"nec1": dict(h["nec1"] or {}), "cells_n": int(h["cells_n"]),
                      "epoch": str((h.get("neo1") or {}).get("epoch") or ""),
                      "pv": str(h.get("product_version") or ""), "record_bytes": dict(h.get("record_bytes") or {})})
        del h
    # Declared layer: all blocks normally share the parent's NEC1.  Since bev
    # mode builds it per block (bevdirect viewport on each peer) they can
    # disagree — a BEV tile update between two blocks' runs, or one block on
    # umfeld's lu (DeclaredDeficient / relay fallback) and another on its
    # own build.  Fallback: pick the declared layer with the MOST cells
    # (lu-declared blocks first — the frozen index layer is the reference)
    # and re-key every other block's rows by cell id onto it; observations
    # of cells absent from the chosen set are dropped and counted
    # (``nec1_disagree`` in the merged header, loud on /process.txt).
    def _is_bev(f):
        return str(f["nec1"].get("source") or "").startswith("bevdirect@")
    bi0 = min(range(len(facts)), key=lambda i: (_is_bev(facts[i]), -facts[i]["cells_n"], i))
    base = _read_compact(paths[bi0], want_rows=False)
    n = facts[bi0]["cells_n"]
    nec_digest = facts[bi0]["nec1"]["digest"]
    base_ids = _lu_ids(base["lu"], n)
    base_id_bytes = np.frombuffer(base["lu"], np.uint8).reshape(n, ne_h3.LU_REC.size)[:, :8].copy()
    rb = facts[bi0]["record_bytes"]
    tb, bb = int(rb.get("net1") or 4), int(rb.get("neb1") or 8)
    disagree = []
    merged_obs = None
    pick = np.full(n, -1, np.int64)
    best = np.full(n, -1, np.int64)
    src_row_all = np.arange(n, dtype=np.int64)
    tree_n = np.zeros(n, np.int64); bld_n = np.zeros(n, np.int64)
    rekey = {}        # bi → src_row (base index → block row, -1 absent) for disagreeing blocks

    # pass 1 — NEO1 only: pick the best-observed block per cell
    for bi, (p, f) in enumerate(zip(paths, facts)):
        r = base if bi == bi0 else _read_compact(p, want_lu=f["nec1"].get("digest") != nec_digest or f["cells_n"] != n, want_rows=False)
        if f["nec1"].get("digest") == nec_digest and f["cells_n"] == n:
            obs = r["obs"]; src_row = None
            tn_b, bn_b = r["tree_n"], r["bld_n"]
        else:
            m = f["cells_n"]
            ids = _lu_ids(r["lu"], m)
            # base ids are sorted (verify_h3b invariant) → searchsorted lookup
            pos = np.searchsorted(base_ids, ids)
            pos[pos >= n] = 0
            keep = base_ids[pos] == ids
            pos = np.where(keep, pos, -1)
            seen_src = row_observed(r["obs"])
            dropped = int((seen_src & ~keep).sum())
            # unseen rows: the block's own nodata pattern (every block carries
            # unseen cells), else a synthesized one (lu 0, h 255, elev 65535)
            if (~seen_src).any():
                tmpl = r["obs"][~seen_src][0].copy()
            else:
                tmpl = np.zeros(ne_h3.OBS_REC.size, np.uint8); tmpl[19] = 255; tmpl[30:32] = 255
            obs = np.tile(tmpl, (n, 1))
            obs[pos[keep]] = r["obs"][keep]
            obs[:, :8] = base_id_bytes
            src_row = np.full(n, -1, np.int64)
            src_row[pos[keep]] = np.nonzero(keep)[0]
            tn_b = np.zeros(n, np.int64); bn_b = np.zeros(n, np.int64)
            tn_b[pos[keep]] = r["tree_n"][keep]; bn_b[pos[keep]] = r["bld_n"][keep]
            rekey[bi] = src_row
            disagree.append({"block": p.stem, "nec1": f["nec1"].get("digest"), "cells_n": m,
                             "observed_dropped": dropped, "rekeyed": int(keep.sum())})
            log.warning("ne_ingest: %s: block %s declared layer %s (%s cells, %s) ≠ chosen %s (%s cells, %s) — re-keyed by cell id, %d observed cells dropped",
                        parent, p.stem, f["nec1"].get("digest"), m, f["nec1"].get("source"),
                        nec_digest, n, facts[bi0]["nec1"].get("source"), dropped)
        if merged_obs is None:
            merged_obs = obs.copy()      # nodata rows as the first block wrote them
        seen = row_observed(obs)
        mass = obs[:, 8:17].astype(np.int32).sum(axis=1)
        better = seen & (mass > best)
        pick[better] = bi; best[better] = mass[better]
        merged_obs[better] = obs[better]
        tree_n[better] = tn_b[better]; bld_n[better] = bn_b[better]
        if src_row is not None:
            src_row_all[better] = src_row[better]
        else:
            src_row_all[better] = np.nonzero(better)[0]
        del obs, r, tn_b, bn_b

    # pass 2 — NET1/NEB1: scatter the picked rows into the output arrays
    tree_off = np.concatenate([[0], np.cumsum(tree_n)]).astype(np.int64)
    bld_off = np.concatenate([[0], np.cumsum(bld_n)]).astype(np.int64)
    trees_out = np.zeros((int(tree_off[-1]), tb), np.uint8)
    bld_out = np.zeros((int(bld_off[-1]), bb), np.uint8)
    for bi, p in enumerate(paths):
        sel = pick == bi
        if not sel.any():
            continue
        r = _read_compact(p, want_lu=False)
        _scatter_rows(trees_out, tree_off[:-1], r["trees"], r["tree_n"], sel, src_row_all, tb)
        _scatter_rows(bld_out, bld_off[:-1], r["bld"], r["bld_n"], sel, src_row_all, bb)
        del r
    trees = trees_out.tobytes(); bld = bld_out.tobytes()
    del trees_out, bld_out
    # the merged rows' counts must agree with the NEO1 columns (verify_h3b)
    merged_obs[:, 22] = np.minimum(tree_n, 255).astype(np.uint8)
    merged_obs[:, 18] = np.minimum(bld_n, 255).astype(np.uint8)
    neo_raw = merged_obs.tobytes()
    del merged_obs
    declared = ne_h3.Section(ne_h3.MAGIC_LU, dict(facts[bi0]["nec1"], kg=parent, cells_n=n,
                                                  digest=nec_digest), base["lu"] + base["k_raw"])
    epochs = sorted({f["epoch"] for f in facts})
    epoch = epochs[-1] if epochs else time.strftime("%Y-%m")
    summary = summary_from_rows(declared, neo_raw, len(trees) // tb, len(bld) // bb, epoch)
    obs = {"cell_ids": declared.cell_ids(), "neo1_raw": neo_raw, "net1_raw": trees, "neb1_raw": bld,
           "tree_n": np.minimum(tree_n, 255).astype(np.uint8), "bld_n": np.minimum(bld_n, 255).astype(np.uint8),
           "summary": summary}
    pv = sorted({f["pv"] for f in facts})[-1]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    hdr = ne_h3.write_h3b(out_path, declared, obs, parent, pv,
                          extra_header={"merged_from": [p.stem for p in paths], "partial_cells": not complete,
                                        "parent_kg": parent, "block": None, "path": "ne_ingest.merge",
                                        "nec1_disagree": disagree or None})
    chk = ne_h3.verify_h3b(out_path, expected_declared_digest=nec_digest)
    if not chk["ok"]:
        out_path.unlink(missing_ok=True)
        raise RuntimeError("merged h3b self-check failed: " + "; ".join(chk["errors"]))
    _index_cells(out_path)
    return hdr


def _index_cells(path: Path) -> None:
    """Register a parent product in ``data/ne/cells.sqlite`` (public cells API
    chunk index, ``cells_api.index_file``).  Best-effort: a failure here never
    blocks the ingest; ``python3 ne_ingest.py --rebuild`` rescans everything."""
    try:
        import cells_api
        cells_api.index_file(path)
    except Exception as ex:  # noqa: BLE001
        log.warning("ne_ingest: cells index %s failed: %s", path.name, ex)


def _live_blocks(parent: str, entries: dict) -> list[str]:
    """Block codes of *parent* with a committed ``_json_v2`` (the live split layout)."""
    out = []
    pre = parent + "-"
    for k, e in entries.items():
        if k.startswith(pre) and k.endswith("_json_v2") and _committed(e):
            out.append(k[:-8])
    return sorted(out)


def _coverage_complete(parent: str, entries: dict) -> bool:
    try:
        from app import _kg_coverage_complete as _cc
        return bool(_cc(parent, entries)[0])
    except Exception:
        return False


def merge_dirty_parents(idx: dict, entries: dict, max_n: int = 10) -> list[dict]:
    out = []
    for parent, st in list(idx["parents"].items()):
        if not st.get("dirty") or len(out) >= max_n:
            continue
        live = _live_blocks(parent, entries)
        have = [b for b in live if (idx["files"].get(b) and path_for(b).exists())]
        if not have:
            # older layout on disk only (blocks no longer live) — use whatever is on disk
            have = [c for c in idx["files"] if is_block(c) and parent_of(c) == parent and path_for(c).exists()]
        if not have:
            st["dirty"] = False
            continue
        complete = bool(live) and set(have) >= set(live) and _coverage_complete(parent, entries)
        try:
            hdr = merge_blocks(parent, [path_for(b) for b in have], path_for(parent), complete=complete)
        except Exception as ex:  # noqa: BLE001
            log.warning("ne_ingest: merge %s failed: %s", parent, ex)
            st["merge_error"] = str(ex)[:200]; st["dirty"] = False
            continue
        st.update(blocks=have, live_blocks=live, complete=complete, digest=hdr["neo1"]["digest"],
                  merged_at=_now(), dirty=False, cells_n=hdr["cells_n"],
                  nec1_source=(hdr.get("nec1") or {}).get("source"),
                  nec1_digest=(hdr.get("nec1") or {}).get("digest"),
                  nec1_disagree=(len(hdr.get("nec1_disagree") or []) or None),
                  cells_observed=(hdr.get("summary") or {}).get("observed", {}).get("cells_observed"))
        st.pop("merge_error", None)
        # The merged file overwrote any earlier unsplit upload of the parent:
        # keep the ``files`` row's declared-layer fields in step, otherwise
        # ``_declared_of`` keeps reporting the old NEC1 (e.g. bevdirect@…)
        # and confirm_umfeld posts NEC1+NEO1 for a file whose NEC1 is the
        # frozen index layer → umfeld 400 → 24 h ramp HOLD (45324, 2026-10-15).
        fp = idx["files"].get(parent)
        if fp:
            fp.update(nec1_source=st["nec1_source"], nec1_digest=st["nec1_digest"],
                      neo1_digest=st["digest"], cells_n=hdr["cells_n"], merged_at=st["merged_at"])
        idx["stats"]["merges"] = int(idx["stats"].get("merges") or 0) + 1
        log.info("ne_ingest: merged %s from %d block(s) (%s) → %s cells, %s observed, digest %s",
                 parent, len(have), "complete" if complete else "partial", hdr["cells_n"],
                 st["cells_observed"], st["digest"])
        out.append({"parent": parent, "blocks": len(have), "complete": complete})
    return out


# === SECTION: umfeld confirmation / (re)post ===

def _disk_row(parent: str, idx: dict) -> tuple[dict | None, str]:
    """``(index row, "merge"|"file")`` describing the ``.h3b`` actually on
    disk for *parent*.  A block merge (``parents[parent]``) normally wins over
    the pre-merge upload row — but a whole-parent product ingested AFTER the
    last merge (``files[parent].at`` newer, different NEO1 digest) overwrote
    the merged file: then the ``files`` row is the truth.  Preferring the
    stale merge row posted a bevdirect-cell NEO1 as a bare (lu) NEO1 →
    umfeld 409 "cell set ≠ our NEC1 lu cell set" every 30 min (03212, 01806,
    2026-10-26)."""
    f = idx["files"].get(parent)
    st = idx["parents"].get(parent)
    if st and st.get("digest"):
        if f and f.get("at") and str(f["at"]) > str(st.get("merged_at") or "") \
                and f.get("neo1_digest") and f["neo1_digest"] != st["digest"]:
            return f, "file"
        return st, "merge"
    return f, "file"


_gf_budget = [10 ** 9]          # fixed-digest computations left in this confirm pass (full corpus read ≈ 0.3 s/file)
GF_COMPUTE_PER_TICK = 400


def _our_digest(parent: str, idx: dict, green_fix: bool = True) -> str | None:
    """Digest of the NEO1 rows we would POST for *parent*: the green-fixed
    rows (``ne_green_fix``, read-time override — the file keeps its original
    digest) unless *green_fix* is off.  Fixed digests are cached in the index
    under ``files[parent]["green_fix"]`` keyed on (FIX_ID, file digest)."""
    tgt, kind = _disk_row(parent, idx)
    if kind == "merge":
        if not tgt.get("complete"):
            return None                                   # partial merges are never posted
        raw = tgt["digest"]
    else:
        raw = (tgt or {}).get("neo1_digest")
    if not raw or not green_fix or not _knob("ne_green_fix", True):
        return raw
    gf = (tgt or {}).get("green_fix") or {}
    import ne_green_fix
    if gf.get("id") == ne_green_fix.FIX_ID and gf.get("of") == raw and gf.get("digest"):
        return gf["digest"]
    if _gf_budget[0] <= 0:
        return None                                       # computed on a later tick (budget per confirm pass)
    _gf_budget[0] -= 1
    try:
        import ne_h3
        d, n = ne_h3.neo1_fixed_digest(path_for(parent))
    except Exception as ex:  # noqa: BLE001
        log.warning("ne_ingest: green-fix digest %s failed: %s", parent, ex)
        return raw
    if tgt is not None:
        tgt["green_fix"] = {"id": ne_green_fix.FIX_ID, "of": raw, "digest": d, "cells": int(n)}
    return d


def _declared_of(parent: str, idx: dict) -> tuple[str | None, str | None]:
    """(nec1_source, nec1_digest) of the parent's h3b on disk (index first,
    header read once as fallback for pre-Oct-2026 index rows)."""
    f = idx["files"].get(parent) or {}
    st = idx["parents"].get(parent) or {}
    row, kind = _disk_row(parent, idx)
    if kind == "merge" and st.get("nec1_source"):
        return st["nec1_source"], st.get("nec1_digest")   # merged file on disk wins over the pre-merge upload row
    if f.get("nec1_source"):
        return f["nec1_source"], f.get("nec1_digest")
    if st.get("nec1_source"):
        return st["nec1_source"], st.get("nec1_digest")
    try:
        import ne_h3
        nh = ne_h3.read_h3b_header(path_for(parent)).get("nec1") or {}
        tgt = f if f else st
        tgt["nec1_source"] = nh.get("source"); tgt["nec1_digest"] = nh.get("digest")
        return nh.get("source"), nh.get("digest")
    except Exception:
        return None, None


def confirm_umfeld(idx: dict, max_posts: int = 5) -> dict:
    """Compare umfeld's stored obs digest with ours for every parent on disk;
    (re)POST on miss / mismatch.  Returns counters.

    bevdirect-declared products (``nec1.source`` = ``bevdirect@…``) are
    confirmed on THREE digests — ``obs.digest == ours`` AND
    ``obs.declared_digest == bev.digest == our NEC1 digest`` — and re-posted
    as a NEC1+NEO1 container (``nec1_section_from_h3b``); lu-declared ones
    stay on the bare NEO1 path.  ``bev_*`` counters feed the VTC ramp gate."""
    import ne_h3
    import ne_rollout
    mf = ne_rollout.refresh_ne_manifest() or {}
    kgs = mf.get("kgs") or {}
    if not kgs:
        return {"skipped": "no umfeld manifest"}
    out = {"checked": 0, "confirmed": 0, "missing": 0, "mismatch": 0, "posted": 0, "post_failed": 0,
           "pending_cells": 0, "bev_checked": 0, "bev_confirmed": 0, "bev_declared_mismatch": 0,
           "bev_posted": 0, "bev_post_failed": 0, "needs_bevdirect": 0, "day_cap_hold": 0}
    now = time.time()
    posts = 0
    conf = idx.setdefault("umfeld", {})
    max_posts = int(_knob("ne_post_max_per_tick", max_posts))
    day = time.strftime("%Y-%m-%d", time.gmtime())
    pd = idx["stats"].get("post_day") or {}
    if pd.get("day") != day:
        pd = {"day": day, "bytes": 0, "posts": 0}
    idx["stats"]["post_day"] = pd
    day_cap = float(_knob("ne_post_gib_per_day", POST_GIB_PER_DAY)) * 2 ** 30
    out["day_bytes"] = int(pd["bytes"])
    last_post = 0.0
    _gf_budget[0] = int(_knob("ne_green_fix_compute_per_tick", GF_COMPUTE_PER_TICK))
    for parent in sorted({c for c in idx["files"] if not is_block(c)} | set(idx["parents"])):
        want = _our_digest(parent, idx)
        if not want:
            continue
        out["checked"] += 1
        rec = kgs.get(parent) or {}
        if not isinstance(rec, dict):
            rec = {}
        obs = rec.get("obs") or {}
        have = obs.get("digest")
        c = conf.setdefault(parent, {})
        src, nec_digest = _declared_of(parent, idx)
        bev = bool(src and str(src).startswith("bevdirect@"))
        decl_ok = True
        if bev:
            out["bev_checked"] += 1
            bev_digest = (rec.get("bev") or {}).get("digest")
            decl_ok = bool(nec_digest) and obs.get("declared_digest") == nec_digest and bev_digest == nec_digest
            c["bev"] = {"nec1": nec_digest, "umfeld_bev": bev_digest, "obs_declared": obs.get("declared_digest")}
        if have == want and decl_ok:
            if c.get("confirmed_digest") != want:
                c.update(confirmed_digest=want, confirmed_at=_now(), post_attempts=0)
                c.pop("post_next", None)
            out["confirmed"] += 1
            if bev:
                out["bev_confirmed"] += 1
            continue
        if have == want and not decl_ok:
            out["bev_declared_mismatch"] += 1
        else:
            out["missing" if not have else "mismatch"] += 1
        if not bev and rec.get("status") not in (None, "built"):
            out["pending_cells"] += 1
            continue     # umfeld has not built the KG's declared layer yet (bev posts carry their own)
        if want != c.get("posted_digest") and c.get("posted_digest"):
            # our export changed (new product / post-processing pass) → fresh retry ladder
            c.update(post_attempts=0); c.pop("post_next", None); c.pop("needs_bevdirect", None)
        if c.get("needs_bevdirect"):
            out["needs_bevdirect"] += 1
            continue     # bare NEO1 can never match umfeld's lu cell set — needs a bevdirect NEC1 (see below)
        if posts >= max_posts or float(c.get("post_next") or 0) > now \
                or int(c.get("post_attempts") or 0) >= POST_MAX_ATTEMPTS:
            continue
        if pd["bytes"] >= day_cap:
            out["day_cap_hold"] += 1
            continue     # umfeld per-token daily volume — resumes at 00:00 UTC
        try:
            sec = ne_h3.neo1_section_from_h3b(path_for(parent), green_fix=bool(_knob("ne_green_fix", True)))
            nec = ne_h3.nec1_section_from_h3b(path_for(parent)) if bev else None
            gap = POST_MIN_GAP_S - (time.time() - last_post)
            if gap > 0:
                time.sleep(gap)
            last_post = time.time()
            r = ne_h3.post_observed(parent, sec, nec1_section=nec)
            posts += 1
            pd["bytes"] += len(sec) + (len(nec) if nec else 0)
            pd["posts"] += 1
            c.update(post_attempts=int(c.get("post_attempts") or 0) + 1, posted_digest=want,
                     posted_at=_now(), post_result=str(r)[:160])
            c.pop("post_error", None)
            c["post_next"] = now + 600          # re-check after umfeld rescans (30 s) + slack
            out["posted"] += 1
            idx["stats"]["posts"] = int(idx["stats"].get("posts") or 0) + 1
            if bev:
                out["bev_posted"] += 1
                c["lu_diff"] = r.get("lu_diff") if isinstance(r, dict) else None
            log.info("ne_ingest: NEO1 %s %s at umfeld (ours %s, theirs %s) — posted%s: %s", parent,
                     "missing" if not have else ("declared-mismatch" if have == want else "mismatch"), want, have,
                     " as NEC1+NEO1" if bev else "", str(r)[:120])
        except Exception as ex:  # noqa: BLE001
            n = int(c.get("post_attempts") or 0) + 1
            c.update(post_attempts=n, post_error=str(ex)[:200], post_next=now + POST_BACKOFF_S * (2 ** min(n, 4)))
            out["post_failed"] += 1
            if not bev and " 409" in str(ex) and "cell set" in str(ex):
                # our (lu-declared) cell set ≠ umfeld's lu and umfeld has no bev
                # layer for this KG: retrying the bare NEO1 is pointless — the
                # KG needs a bevdirect NEC1 in front of the obs (reprocess on
                # a bev-mode peer).  Park it until our digest changes.
                c["needs_bevdirect"] = _now()
                out["needs_bevdirect"] += 1
                log.warning("ne_ingest: NEO1 %s parked: bare NEO1 cell set ≠ umfeld lu and no bev layer — "
                            "needs a bevdirect NEC1+NEO1 product", parent)
            if bev:
                out["bev_post_failed"] += 1
                idx["stats"]["bev_post_fail_at"] = now
                idx["stats"]["bev_post_fail_msg"] = f"{parent}: {str(ex)[:160]}"
            log.warning("ne_ingest: NEO1 post %s%s failed (attempt %d): %s", parent,
                        " (NEC1+NEO1)" if bev else "", n, str(ex)[:160])
    idx["stats"]["umfeld_last"] = dict(out, at=_now())
    return out


# === SECTION: tick / status ===

def run_tick(max_n: int = MAX_PER_TICK) -> dict:
    if not _lock.acquire(blocking=False):
        return {"skipped": "busy"}
    t0 = time.time()
    out = {"pulled": 0, "failed": 0, "budget_hold": 0, "pending": 0, "merges": 0}
    try:
        import inbound_budget
        idx = _load_index()
        entries = load_manifest_entries()
        todo = pending(entries, idx)
        out["pending"] = len(todo)
        mb_h = float(_knob("ne_ingest_mb_per_h", MB_PER_H_DEFAULT))
        enabled = bool(_knob("ne_ingest_enabled", True))
        if enabled:
            for code, e in todo[:max_n]:
                if not inbound_budget.allow(CHANNEL, mb_h, int(e.get("size") or 0)):
                    out["budget_hold"] = len(todo) - out["pulled"] - out["failed"]
                    break
                try:
                    r = ingest_one(code, e, idx)
                except Exception as ex:  # noqa: BLE001
                    log.warning("ne_ingest %s crashed: %s", code, ex)
                    r = {"ok": False}
                out["pulled" if r.get("ok") else "failed"] += 1
            _save_index(idx)
        try:
            out["merges"] = len(merge_dirty_parents(idx, entries))
        except Exception as ex:  # noqa: BLE001
            log.warning("ne_ingest merge pass: %s", ex)
        try:
            # Sliver sweep (sliver_sweep.py): refresh umfeld's bbox_changes,
            # plan units for complete parents, merge received sweep h3bs
            # into the parent files → confirm_umfeld below sees the new
            # digest and re-posts.
            import sliver_sweep as _sls
            out["sweep"] = _sls.tick(idx, entries)
        except Exception as ex:  # noqa: BLE001
            log.warning("ne_ingest sweep pass: %s", ex)
        try:
            out["umfeld"] = confirm_umfeld(idx)
        except Exception as ex:  # noqa: BLE001
            log.warning("ne_ingest umfeld confirm: %s", ex)
        _save_index(idx)
        out["pending"] = max(0, len(todo) - out["pulled"] - out["failed"])
    finally:
        out["dt"] = round(time.time() - t0, 1); out["ts"] = _now()
        _last_tick.clear(); _last_tick.update(out)
        _lock.release()
    if out["pulled"] or out["failed"] or out["merges"] or (out.get("umfeld") or {}).get("posted"):
        log.info("ne_ingest tick: %s", out)
    return out


def status() -> dict:
    import inbound_budget
    idx = load_index_ro()
    entries = load_manifest_entries()
    files = idx["files"]
    parents = [c for c in files if not is_block(c)]
    blocks = [c for c in files if is_block(c)]
    todo = pending(entries, idx)
    corpus_bytes = sum(int(f.get("size") or 0) for f in files.values())
    pend_bytes = sum(int(e.get("size") or 0) for _, e in todo)
    merged = idx["parents"]
    um = (idx.get("stats") or {}).get("umfeld_last") or {}
    return {
        "parents": len(parents), "blocks": len(blocks), "corpus_mb": round(corpus_bytes / 1e6, 1),
        "merged_parents": len([p for p in merged.values() if p.get("digest")]),
        "merged_complete": len([p for p in merged.values() if p.get("complete")]),
        "pending": len(todo), "pending_mb": round(pend_bytes / 1e6, 1),
        "failed": len(idx.get("failed") or {}),
        "used_mb_h": round(inbound_budget.used_mb(CHANNEL), 1),
        "budget_mb_h": float(_knob("ne_ingest_mb_per_h", MB_PER_H_DEFAULT)),
        "enabled": bool(_knob("ne_ingest_enabled", True)),
        "pulled_peer": int((idx.get("stats") or {}).get("pulled_peer") or 0),
        "pulled_zenodo": int((idx.get("stats") or {}).get("pulled_zenodo") or 0),
        "posts": int((idx.get("stats") or {}).get("posts") or 0),
        "umfeld": {k: um.get(k) for k in ("checked", "confirmed", "missing", "mismatch", "posted",
                                           "post_failed", "pending_cells", "at", "bev_checked", "bev_confirmed",
                                           "bev_declared_mismatch", "bev_posted", "bev_post_failed",
                                           "needs_bevdirect", "day_cap_hold", "day_bytes")},
        "post_day": (idx.get("stats") or {}).get("post_day"),
        "green_fix": _green_fix_status(idx),
        "bev_post_fail_at": (idx.get("stats") or {}).get("bev_post_fail_at"),
        "bev_post_fail_msg": (idx.get("stats") or {}).get("bev_post_fail_msg"),
        "last_tick": dict(_last_tick),
    }


def _green_fix_status(idx: dict) -> dict:
    """Read-time green-ground override (``ne_green_fix``): how many parents
    have a fixed digest computed, how many of those umfeld confirmed."""
    import ne_green_fix
    conf = idx.get("umfeld") or {}
    n = done = cells = 0
    for parent in {c for c in idx["files"] if not is_block(c)} | set(idx["parents"]):
        tgt, _ = _disk_row(parent, idx)
        gf = (tgt or {}).get("green_fix") or {}
        if gf.get("id") != ne_green_fix.FIX_ID:
            continue
        n += 1
        cells += int(gf.get("cells") or 0)
        if (conf.get(parent) or {}).get("confirmed_digest") == gf.get("digest"):
            done += 1
    return {"id": ne_green_fix.FIX_ID, "enabled": bool(_knob("ne_green_fix", True)),
            "computed": n, "confirmed": done, "cells_touched": cells}


def text_line(v: dict | None = None) -> str:
    v = v if v is not None else status()
    um = v.get("umfeld") or {}
    s = (f"ne_ingest: corpus={v['parents']}p+{v['blocks']}b {v['corpus_mb']}MB merged={v['merged_parents']}"
         f"({v['merged_complete']} complete) pending={v['pending']} ({v['pending_mb']}MB)"
         f" budget={v['used_mb_h']}/{v['budget_mb_h']}MB/h src peer={v['pulled_peer']} zenodo={v['pulled_zenodo']}")
    if not v.get("enabled"):
        s += " DISABLED"
    if v.get("failed"):
        s += f" failed={v['failed']}"
    if um.get("checked") is not None:
        s += (f"\n    umfeld_confirm: ok={um.get('confirmed')}/{um.get('checked')} missing={um.get('missing')}"
              f" mismatch={um.get('mismatch')} reposted={v.get('posts')} unbuilt={um.get('pending_cells')}")
        if um.get("bev_checked"):
            s += (f" · bev(NEC1+NEO1): ok={um.get('bev_confirmed')}/{um.get('bev_checked')}"
                  f" declared_mismatch={um.get('bev_declared_mismatch')} post_failed={um.get('bev_post_failed')}")
        gf = v.get("green_fix") or {}
        if gf:
            s += (f"\n    green_fix({gf.get('id')}{'' if gf.get('enabled') else ' OFF'}): umfeld_confirmed="
                  f"{gf.get('confirmed')}/{gf.get('computed')} parents, {gf.get('cells_touched')} cells bau/alpen→gruen"
                  f" (read-time override, files untouched; ne_green_fix.py)")
        pdy = v.get("post_day") or {}
        s += (f"\n    resync(by digest): today {int(pdy.get('posts') or 0)} POSTs {(pdy.get('bytes') or 0) / 2 ** 20:.0f} MiB"
              f" (cap {float(_knob('ne_post_gib_per_day', POST_GIB_PER_DAY)):.1f} GiB/day, ≤2/s)")
        if um.get("day_cap_hold"):
            s += f" DAY-CAP-HOLD {um['day_cap_hold']} waiting"
        if um.get("needs_bevdirect"):
            s += f" · needs_bevdirect={um['needs_bevdirect']} (lu cell-set 409, parked — reprocess on a bev-mode peer)"
        fa = v.get("bev_post_fail_at")
        if fa and time.time() - float(fa) < 86400:
            s += f" LAST-FAIL {(time.time() - float(fa)) / 3600:.1f}h ago: {str(v.get('bev_post_fail_msg'))[:90]}"
    return s


def loop(should_run, tick_s: int = TICK_S) -> None:
    time.sleep(120)
    while True:
        try:
            if should_run():
                run_tick()
        except Exception as ex:  # noqa: BLE001
            log.warning("ne_ingest loop: %s", ex)
        time.sleep(tick_s)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if len(sys.argv) > 1 and sys.argv[1] == "status":
        print(text_line()); print(json.dumps(status(), indent=1, default=str))
    elif len(sys.argv) > 1 and sys.argv[1] == "--rebuild":
        import cells_api
        print(json.dumps(cells_api.rebuild_index())); print(json.dumps(cells_api.index_status(), indent=1))
    elif len(sys.argv) > 2 and sys.argv[1] == "merge":
        print(json.dumps(merge_blocks(sys.argv[2], [Path(p) for p in sys.argv[3:]],
                                      Path(f"/tmp/{sys.argv[2]}_merged.h3b"), complete=False), default=str)[:800])
    else:
        print(run_tick())
