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
   + every live block's h3b ingested) the merged NEO1 is POSTed to the NE server.
4. **NE-server confirmation**: the NE server's ``/ne/manifest`` (cached by
   ``ne_rollout.refresh_ne_manifest``) carries ``obs.digest`` per KG; for
   every parent on disk the stored digest must equal our NEO1 digest —
   missing / mismatching → re-POST from the h3b (``post_attempts`` capped,
   exponential backoff).  Peers POST NEO1 for unsplit KGs themselves; this
   is the safety net ("just in case" the POST failed / the NE server lost it).

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
import os
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
POST_GIB_PER_DAY = 2.0             # NE server /contrib token limit is 4 GiB/day; a full resync (~2.4 GB) spreads over ≥1 day
POST_MIN_GAP_S = 0.6               # NE server: ≤2 POST/s
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
    from kg_splitter import is_sweep_code
    files, failed = idx["files"], idx.get("failed") or {}
    out = []
    for k, e in entries.items():
        if not k.endswith("_ne_h3b") or not _committed(e):
            continue
        code = k[:-7]
        if is_sweep_code(code):
            # ``<parent>-sw<k>_ne_h3b`` is the sliver sweep's durability
            # archive: the authoritative copy arrives via
            # ``sliver_sweep.accept_result`` (data/ne/sweeps/) and is merged
            # by ``sliver_sweep.integrate`` — never a block of the layout.
            continue
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
    import config
    tok = config.operator_token()
    return {"X-Operator-Token": tok} if tok else {}


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
    from kg_splitter import is_sweep_code
    if is_sweep_code(code):
        return {"code": code, "ok": False, "reason": "sweep unit — not an ingest product"}
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
    # the NE server's lu (DeclaredDeficient / relay fallback) and another on its
    # own build.  Fallback: the merged declared layer is the UNION of every
    # block's cells — the reference block (lu-declared first — the frozen
    # index layer; then the one with the most cells) supplies the declared
    # row of every cell it holds, the other blocks add the cells it lacks
    # (with their own declared rows), and every block's observations are
    # re-keyed by cell id onto that union.  Nothing observed is ever dropped
    # (``nec1_union`` / ``nec1_disagree`` in the merged header).  Until
    # 2026-11-04 the chosen block's set was the output: a sliver-sweep unit
    # (lu-declared) merged into a bevdirect parent threw away every
    # bev-only observed cell (612 k cells over 1001 parents).
    def _rank(f):
        # reference preference: frozen lu (index-*) → bevdirect build → umfeld's
        # chunk-log view (``chunks@…`` — what ne_viewfill sweep units build on;
        # its cells join by union, it must not redefine the parent's layer)
        src = str(f["nec1"].get("source") or "")
        return 2 if src.startswith("chunks@") else 1 if src.startswith("bevdirect@") else 0
    bi0 = min(range(len(facts)), key=lambda i: (_rank(facts[i]), -facts[i]["cells_n"], i))
    base = _read_compact(paths[bi0], want_rows=False)
    n = facts[bi0]["cells_n"]
    nec_digest = facts[bi0]["nec1"]["digest"]
    base_ids = _lu_ids(base["lu"], n)
    union_added = []
    lu_rows = np.frombuffer(base["lu"], np.uint8).reshape(n, ne_h3.LU_REC.size)
    extra_ids = []; extra_rows = []
    for bi, (p, f) in enumerate(zip(paths, facts)):
        if bi == bi0 or (f["nec1"].get("digest") == nec_digest and f["cells_n"] == n):
            continue
        r = _read_compact(p, want_rows=False)
        m = f["cells_n"]
        ids = _lu_ids(r["lu"], m)
        new = ~np.isin(ids, base_ids)
        if extra_ids:
            new &= ~np.isin(ids, np.concatenate(extra_ids))
        if new.any():
            extra_ids.append(ids[new])
            extra_rows.append(np.frombuffer(r["lu"], np.uint8).reshape(m, ne_h3.LU_REC.size)[new].copy())
            union_added.append({"block": p.stem, "source": f["nec1"].get("source"), "cells_added": int(new.sum())})
        del r
    if extra_ids:
        all_ids = np.concatenate([base_ids] + extra_ids)
        all_rows = np.concatenate([lu_rows] + extra_rows)
        order = np.argsort(all_ids, kind="stable")
        base_ids = all_ids[order]
        lu_rows = np.ascontiguousarray(all_rows[order])
        n = int(base_ids.size)
        base["lu"] = lu_rows.tobytes()
        nec_digest = ne_h3.digest_of(base["lu"] + base["k_raw"])
        log.info("ne_ingest: %s: declared layers disagree — union of %d blocks = %d cells (+%d over %s)",
                 parent, len(paths), n, sum(u["cells_added"] for u in union_added), paths[bi0].stem)
        del all_ids, all_rows, order
    base_id_bytes = lu_rows[:, :8].copy()
    union_changed = bool(extra_ids)
    del lu_rows, extra_ids, extra_rows
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
        r = base if (bi == bi0 and not union_changed) else \
            _read_compact(p, want_lu=f["nec1"].get("digest") != nec_digest or f["cells_n"] != n, want_rows=False)
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
            (log.warning if dropped else log.info)(
                "ne_ingest: %s: block %s declared layer %s (%s cells, %s) ≠ merged %s (%s cells, union on %s) — re-keyed by cell id, %d observed cells dropped",
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
                                        "nec1_disagree": disagree or None,
                                        "nec1_union": union_added or None})
    chk = ne_h3.verify_h3b(out_path, expected_declared_digest=nec_digest)
    if not chk["ok"]:
        out_path.unlink(missing_ok=True)
        raise RuntimeError("merged h3b self-check failed: " + "; ".join(chk["errors"]))
    _index_cells(out_path)
    return hdr


def _index_cells(path: Path) -> None:
    """Register a parent product in ``data/ne/cells.sqlite`` (public cells API
    chunk index, ``cells_api.index_file``).  Best-effort: a failure here never
    blocks the ingest; ``python3 ne_ingest.py --rebuild`` rescans everything.
    Dot-prefixed temp outputs (e.g. ``.<kg>.sweep_merge.h3b`` from
    ``sliver_sweep``) are skipped — the caller indexes the final path after
    ``os.replace``."""
    if path.name.startswith("."):
        return
    try:
        import cells_api
        cells_api.index_file(path)
    except Exception as ex:  # noqa: BLE001
        log.warning("ne_ingest: cells index %s failed: %s", path.name, ex)


def _live_blocks(parent: str, entries: dict) -> list[str]:
    """Block codes of *parent* with a committed ``_json_v2`` (the live split layout)."""
    from kg_splitter import is_sweep_code
    out = []
    pre = parent + "-"
    for k, e in entries.items():
        if k.startswith(pre) and k.endswith("_json_v2") and _committed(e) and not is_sweep_code(k[:-8]):
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
        inputs = [path_for(b) for b in have]
        own = idx["files"].get(parent) or {}
        if own.get("kind") == "parent" and _committed(entries.get(f"{parent}_ne_h3b")) and path_for(parent).exists():
            # mixed parent: an own (unsplit) product is part of the live set
            # next to blocks — merging the blocks alone overwrote it (05003
            # lost its west half, 2026-11-04); the file on disk joins the
            # union (it is that product, or an earlier union that holds it)
            inputs.append(path_for(parent))
            st["own_in_merge"] = True
        try:
            tmp_out = path_for(parent).with_name(f".{parent}.dirty_merge.h3b")
            hdr = merge_blocks(parent, inputs, tmp_out, complete=complete)
            os.replace(tmp_out, path_for(parent))
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
        # and confirm_ne_server posts NEC1+NEO1 for a file whose NEC1 is the
        # frozen index layer → NE server 400 → 24 h ramp HOLD (45324, 2026-10-15).
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


# === SECTION: union recovery (sweep merges that dropped bev-only cells) ===

def recover_dropped(idx: dict, entries: dict) -> dict:
    """One-shot (per index) repair of parents whose merge dropped observed
    cells (``nec1_disagree[].observed_dropped`` > 0 in the h3b header — the
    pre-2026-11-04 rule kept only the reference block's cell set, so a
    lu-declared sliver-sweep unit merged into a bevdirect parent discarded
    every bev-only observed cell).  Split parents with their blocks on disk
    are marked ``dirty`` (``merge_dirty_parents`` → union merge; the sweep
    integrate then re-merges the units on digest drift); unsplit parents get
    their ``files`` row un-pinned so :func:`pending` re-pulls the original
    product (peer / Zenodo, inbound budget) — the drift re-merge follows.
    Progress lives in ``idx["stats"]["union_recovery"]``."""
    ur = idx["stats"].get("union_recovery")
    if ur is None:
        import ne_h3
        t0 = time.time()
        remerge, repull, dropped = [], [], 0
        for k, v in list(idx["parents"].items()):
            if not v.get("nec1_disagree"):
                continue
            try:
                h = ne_h3.read_h3b_header(path_for(k))
            except Exception:  # noqa: BLE001
                continue
            od = sum(int(x.get("observed_dropped") or 0) for x in (h.get("nec1_disagree") or []))
            if not od:
                continue
            dropped += od
            live = _live_blocks(k, entries)
            if live and all(path_for(b).exists() for b in live):
                v["dirty"] = True
                remerge.append(k)
            elif f"{k}_ne_h3b" in entries and idx["files"].get(k):
                idx["files"][k]["uploaded_at"] = None      # → pending() pulls the original again
                repull.append(k)
            else:
                log.warning("ne_ingest: union recovery: %s dropped %d cells but has neither blocks nor a parent product", k, od)
        ur = {"started_at": _now(), "remerge": len(remerge), "repull": len(repull), "cells_dropped": dropped,
              "remerge_codes": remerge, "repull_codes": repull, "scan_s": round(time.time() - t0, 1), "done": False}
        idx["stats"]["union_recovery"] = ur
        log.info("ne_ingest: union recovery: %d split parents to re-merge, %d unsplit to re-pull (%d observed cells dropped) in %.1fs",
                 len(remerge), len(repull), dropped, ur["scan_s"])
    if ur.get("done"):
        return ur
    st0 = str(ur.get("started_at") or "")
    ur["remerge_done"] = sum(1 for k in ur.get("remerge_codes") or []
                             if str((idx["parents"].get(k) or {}).get("merged_at") or "") > st0)
    ur["repull_done"] = sum(1 for k in ur.get("repull_codes") or []
                            if str((idx["files"].get(k) or {}).get("at") or "") > st0)
    if ur["remerge_done"] >= ur["remerge"] and ur["repull_done"] >= ur["repull"]:
        ur["done"] = True; ur["finished_at"] = _now()
        ur.pop("remerge_codes", None); ur.pop("repull_codes", None)
        log.info("ne_ingest: union recovery finished: %s", ur)
    return ur


# === SECTION: viewfill (ne_viewfill.py staged results) ===

VIEWFILL_STAGED = NE_DIR / "viewfill" / "staged"
VIEWFILL_PER_TICK = 200   # 2026-11-04: 40 left ~16 of 37 sweepers idle — staged results (≈25/min from the walk) must not queue behind the 5-min tick


def consume_viewfill(idx: dict, entries: dict, max_n: int = VIEWFILL_PER_TICK) -> dict:
    """Apply what ``ne_viewfill.py`` staged (runs inside ``run_tick`` under the
    ingest lock, on the live index): ``staged/<kg>.json`` (+ ``<kg>.h3b``).

    * staged h3b → replaces ``data/ne/<kg>.h3b`` **only if** the live file
      still has the digest the builder merged from (``base_digest``; a sweep
      integrate / fresh product in between wins and the entry is dropped —
      the builder re-walks the KG), cells index + index rows updated like
      ``sliver_sweep.integrate``;
    * ``plan_cells`` → ``sliver_sweep.plan_cells`` (sweep units on the view);
    * ``repost`` → ``NE server[kg].repost`` so ``confirm_NE server`` re-POSTs on the
      view even when our file digest did not change (borrowed rows only).
    Counters in ``idx.stats.viewfill``."""
    import ne_h3
    if not VIEWFILL_STAGED.exists():
        return {}
    files = sorted(VIEWFILL_STAGED.glob("?????.json"))[:max_n]
    if not files:
        return {}
    vf = idx["stats"].setdefault("viewfill", {"applied": 0, "swapped": 0, "rejected": 0, "planned_kgs": 0,
                                             "planned_cells": 0, "repost": 0, "errors": 0})
    out = {"applied": 0, "swapped": 0, "rejected": 0, "planned": 0, "repost": 0}
    for jf in files:
        kg = jf.stem
        h3b = VIEWFILL_STAGED / f"{kg}.h3b"
        try:
            rec = json.loads(jf.read_text())
        except Exception as ex:  # noqa: BLE001
            log.warning("ne_ingest viewfill: %s unreadable (%s) — dropped", jf.name, ex)
            jf.unlink(missing_ok=True); h3b.unlink(missing_ok=True)
            vf["errors"] += 1
            continue
        swapped = False
        try:
            if rec.get("staged_h3b") and h3b.exists():
                p = path_for(kg)
                live = None
                try:
                    live = str((ne_h3.read_h3b_header(p).get("neo1") or {}).get("digest") or "")
                except Exception:  # noqa: BLE001
                    pass
                if live and rec.get("base_digest") and live != rec["base_digest"]:
                    log.info("ne_ingest viewfill: %s live file moved on (%s ≠ staged base %s) — staged merge dropped",
                             kg, live, rec["base_digest"])
                    vf["rejected"] += 1; out["rejected"] += 1
                else:
                    rep = ne_h3.verify_h3b(h3b)
                    if not rep.get("ok"):
                        raise RuntimeError("verify: " + "; ".join(rep.get("errors") or [])[:160])
                    hdr = rep["header"]
                    os.replace(h3b, p)
                    _index_cells(p)
                    digest = (hdr.get("neo1") or {}).get("digest")
                    stp = idx["parents"].setdefault(kg, {})
                    stp.update(digest=digest, merged_at=_now(), dirty=False, cells_n=hdr.get("cells_n"),
                               nec1_source=(hdr.get("nec1") or {}).get("source"),
                               nec1_digest=(hdr.get("nec1") or {}).get("digest"),
                               nec1_disagree=(len(hdr.get("nec1_disagree") or []) or None),
                               cells_observed=(hdr.get("summary") or {}).get("observed", {}).get("cells_observed"),
                               viewfill_at=_now(), viewfill_from=[m.get("src") for m in rec.get("merged_from") or []])
                    stp.setdefault("complete", _coverage_complete(kg, entries))
                    stp.pop("merge_error", None); stp.pop("green_fix", None)
                    fp = idx["files"].get(kg)
                    if fp:
                        fp.update(nec1_source=stp["nec1_source"], nec1_digest=stp["nec1_digest"], neo1_digest=digest,
                                  cells_n=hdr.get("cells_n"), merged_at=stp["merged_at"])
                        fp.pop("green_fix", None)
                    swapped = True
                    vf["swapped"] += 1; out["swapped"] += 1
                    log.info("ne_ingest viewfill: %s ← staged union (%s observed of %s, from %s) digest %s", kg,
                             stp.get("cells_observed"), hdr.get("cells_n"),
                             ",".join(f"{m.get('src')}:{m.get('covers')}" for m in rec.get("merged_from") or []), digest)
            cells = rec.get("plan_cells") or []
            if cells:
                import sliver_sweep as _sls
                r = _sls.plan_cells(kg, np.asarray(cells, dtype=np.uint64), source="viewfill")
                if r.get("planned"):
                    vf["planned_kgs"] += 1; vf["planned_cells"] += int(r.get("cells") or 0) - int(r.get("deferred_cells") or 0)
                    out["planned"] += 1
                else:
                    log.info("ne_ingest viewfill: %s sweep plan not queued: %s", kg, r)
            if rec.get("repost") or swapped:
                c = idx.setdefault("NE server", {}).setdefault(kg, {})
                c["repost"] = "viewfill"
                c.update(post_attempts=0); c.pop("post_next", None)
                vf["repost"] += 1; out["repost"] += 1
            vf["applied"] += 1; out["applied"] += 1
            vf["last"] = {"kg": kg, "at": _now(), "swapped": swapped, "blank": rec.get("blank"),
                          "borrowable": rec.get("borrowable"), "residual": rec.get("residual_n")}
        except Exception as ex:  # noqa: BLE001
            log.warning("ne_ingest viewfill: %s failed: %s", kg, ex)
            vf["errors"] += 1
        finally:
            jf.unlink(missing_ok=True); h3b.unlink(missing_ok=True)
    return out


# === SECTION: NE-server confirmation / (re)post ===

def _disk_row(parent: str, idx: dict) -> tuple[dict | None, str]:
    """``(index row, "merge"|"file")`` describing the ``.h3b`` actually on
    disk for *parent*.  A block merge (``parents[parent]``) normally wins over
    the pre-merge upload row — but a whole-parent product ingested AFTER the
    last merge (``files[parent].at`` newer, different NEO1 digest) overwrote
    the merged file: then the ``files`` row is the truth.  Preferring the
    stale merge row posted a bevdirect-cell NEO1 as a bare (lu) NEO1 →
    NE server 409 "cell set ≠ our NEC1 lu cell set" every 30 min (03212, 01806,
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


def _view_of(rec: dict) -> tuple[str | None, str | None]:
    """NE-server manifest ``declared{source,state}`` → ``(source, state)``;
    source ``chunks@<state>`` = the KG has a chunk-log view (has_view)."""
    d = rec.get("declared") or {}
    return d.get("source"), d.get("state")


def _has_view(rec: dict) -> bool:
    return str(_view_of(rec)[0] or "").startswith("chunks@")


def _post_bare_on_view(parent: str, want: str, c: dict, green_fix: bool) -> tuple[dict, bytes, dict]:
    """Bare NEO1 re-keyed onto the NE server's ``/declared`` view (ne-change.md §4b):
    fetch → rebuild → POST; a 409 that carries ``view{}`` means the view moved
    between GET and POST → refetch once and rebuild.  Returns
    ``(server answer, posted section, rekey stats)``.  Raises on failure."""
    import ne_h3
    last = None
    for attempt in range(2):
        view = ne_h3.fetch_declared_view(parent)
        if view is None:
            raise ne_h3.NEError(f"NE server {parent}/declared: 404 (KG holds nothing)")
        sec, st = ne_h3.neo1_section_on_view(path_for(parent), view, green_fix=green_fix)
        try:
            return ne_h3.post_observed(parent, sec), sec, st
        except ne_h3.NEError as ex:
            last = ex
            if " 409" in str(ex) and '"view"' in str(ex) and attempt == 0:
                log.info("ne_ingest: NEO1 %s 409 on view %s — view moved, refetching", parent, st.get("state"))
                continue
            raise
    raise last  # pragma: no cover


CONFIRM_RULE = "view-2026-11-04"   # bump → every parent's retry ladder is reset once
OBS_MISSING_REFRESH_S = 3600       # the NE server recomputes obs_complete every 3 h (``obs_complete.every_s``)
OBS_MISSING_MAX_POSTS = 3          # per (view_source, missing_n) situation — guards a post/relist loop


def refresh_obs_missing(idx: dict, force: bool = False) -> dict:
    """``GET /contrib/api/v1/ne/revisit?kind=obs_missing&all=1`` (ETag-conditional,
    at most hourly) → ``idx["stats"]["obs_missing"]`` = ``{fetched_at, etag,
    pass_at, kgs_n, cells_n, kgs: {kg: {missing_n, extra_n, view_source}}}``.
    the NE server lists KGs whose observed layer lacks cells of today's declared
    view (the view moved after our NEO1 was validated: re-baseline, bevdirect
    edge fix, a neighbour's edge cells).  Never raises; the cached snapshot
    is returned on any failure."""
    cur = idx["stats"].get("obs_missing") or {}
    now = time.time()
    if not force and now - float(cur.get("fetched_ts") or 0) < OBS_MISSING_REFRESH_S:
        return cur
    try:
        import requests
        import ne_h3
        if not ne_h3.ne_server_enabled():
            return cur
        h = dict(ne_h3._ne_server_headers())
        if cur.get("etag"):
            h["If-None-Match"] = cur["etag"]
        r = requests.get(f"{ne_h3._ne_server_contrib_base()}/revisit", params={"kind": "obs_missing", "all": 1},
                         headers=h, timeout=120)
        if r.status_code == 304:
            cur["fetched_ts"] = now
            idx["stats"]["obs_missing"] = cur
            return cur
        r.raise_for_status()
        d = r.json()
        oc = d.get("obs_complete") or {}
        kgs = {}
        for x in d.get("obs_missing") or []:
            kg = str(x.get("kg") or "").zfill(5)
            if kg:
                kgs[kg] = {"missing_n": int(x.get("missing_n") or 0), "extra_n": int(x.get("extra_n") or 0),
                           "view_source": x.get("view_source")}
        cur = {"fetched_at": _now(), "fetched_ts": now, "etag": r.headers.get("ETag"),
               "pass_at": oc.get("pass_at") or d.get("generated_at"), "kgs_n": len(kgs),
               "cells_n": int(oc.get("cells_obs_missing") or sum(v["missing_n"] for v in kgs.values())),
               "kgs_no_obs": oc.get("kgs_no_obs"), "kgs": kgs}
        idx["stats"]["obs_missing"] = cur
        log.info("ne_ingest: the NE server obs_missing: %d KGs / %d cells (pass %s)", len(kgs), cur["cells_n"], cur["pass_at"])
    except Exception as ex:  # noqa: BLE001
        cur["fetched_ts"] = now
        cur["error"] = str(ex)[:160]
        idx["stats"]["obs_missing"] = cur
        log.warning("ne_ingest: the NE server revisit?kind=obs_missing failed: %s", ex)
    return cur


def confirm_ne_server(idx: dict, max_posts: int = 5) -> dict:
    """Compare the NE server's stored obs digest with ours for every parent on disk;
    (re)POST on miss / mismatch.  Returns counters.

    * bevdirect-declared products (``nec1.source`` = ``bevdirect@…``) are
      confirmed on THREE digests — ``obs.digest == ours`` AND
      ``obs.declared_digest == bev.digest == our NEC1 digest`` — and re-posted
      as a NEC1+NEO1 container (``nec1_section_from_h3b``).
    * lu-declared products post a **bare NEO1 built on the NE server's ``/declared``
      view** (since 2026-10-27, ne-change.md §4b): our rows re-keyed onto the
      view's cell set (extra cells dropped, unobserved view cells = unseen
      rows).  Confirmed when the NE server's ``obs.declared_source`` is
      ``chunks@…`` and its digest equals the rows we posted for the current
      file digest (``conf[p].view``).  An obs still validated against
      ``index-2026-03`` while the KG has a view (= the NE server's
      ``revisit.obs_on_index``) is re-posted first; KGs without a view keep
      the old lu-set path.  The view itself is fetched only right before a
      POST (~0.2 MB, never stored) — primary bandwidth stays per-post.
    ``bev_*`` counters feed the VTC ramp gate."""
    import ne_h3
    import ne_rollout
    mf = ne_rollout.refresh_ne_manifest() or {}
    kgs = mf.get("kgs") or {}
    if not kgs:
        return {"skipped": "no NE-server manifest"}
    out = {"checked": 0, "confirmed": 0, "missing": 0, "mismatch": 0, "posted": 0, "post_failed": 0,
           "pending_cells": 0, "bev_checked": 0, "bev_confirmed": 0, "bev_declared_mismatch": 0,
           "bev_posted": 0, "bev_post_failed": 0, "needs_bevdirect": 0, "day_cap_hold": 0,
           "obs_on_index": 0, "on_view": 0, "view_posted": 0, "view_dropped": 0, "view_filled": 0, "view_borrowed": 0,
           "no_view": 0, "obs_missing_due": 0, "obs_missing_held": 0, "obs_missing_listed": 0, "recovery_hold": 0}
    now = time.time()
    conf = idx.setdefault("ne_server", {})
    max_posts = int(_knob("ne_post_max_per_tick", max_posts))
    day = time.strftime("%Y-%m-%d", time.gmtime())
    pd = idx["stats"].get("post_day") or {}
    if pd.get("day") != day:
        pd = {"day": day, "bytes": 0, "posts": 0}
    idx["stats"]["post_day"] = pd
    day_cap = float(_knob("ne_post_gib_per_day", POST_GIB_PER_DAY)) * 2 ** 30
    out["day_bytes"] = int(pd["bytes"])
    _gf_budget[0] = int(_knob("ne_green_fix_compute_per_tick", GF_COMPUTE_PER_TICK))
    green_fix = bool(_knob("ne_green_fix", True))
    if mf.get("kgs_obs_on_index") is not None:
        vp = idx["stats"].setdefault("view_pass", {})
        vp.setdefault("obs_on_index_before", int(mf["kgs_obs_on_index"]))
        vp.setdefault("started_at", _now())
        vp["obs_on_index_now"] = int(mf["kgs_obs_on_index"])
        vp["manifest_at"] = mf.get("generated_at")
    # --- phase 1: classify every parent; collect post candidates with a priority
    om = refresh_obs_missing(idx)
    om_kgs = om.get("kgs") or {}
    om_pass = str(om.get("pass_at") or "")
    out["obs_missing_listed"] = len(om_kgs)
    if idx["stats"].get("confirm_rule") != CONFIRM_RULE:
        # rule change (2026-11-04: bev parents confirmed on the view too, struck-out
        # ladders were the old NEC1-digest rule looping) → fresh retry budget once
        for c0 in conf.values():
            if c0.get("post_attempts"):
                c0.update(post_attempts=0); c0.pop("post_next", None)
        idx["stats"]["confirm_rule"] = CONFIRM_RULE
    ur = idx["stats"].get("union_recovery") or {}
    recovering = set() if ur.get("done") else (set(ur.get("remerge_codes") or []) | set(ur.get("repull_codes") or []))
    st0 = str(ur.get("started_at") or "")
    cands: list[tuple[tuple, str, dict]] = []
    for parent in sorted({c for c in idx["files"] if not is_block(c)} | set(idx["parents"])):
        want = _our_digest(parent, idx, green_fix=green_fix)
        if not want:
            continue
        out["checked"] += 1
        if parent in recovering and not (str((idx["parents"].get(parent) or {}).get("merged_at") or "") > st0
                                         or str((idx["files"].get(parent) or {}).get("at") or "") > st0):
            out["recovery_hold"] += 1
            continue     # file still lacks the dropped bev cells — posting it would just bake nodata at the NE server
        rec = kgs.get(parent) or {}
        if not isinstance(rec, dict):
            rec = {}
        obs = rec.get("obs") or {}
        have = obs.get("digest")
        c = conf.setdefault(parent, {})
        src, nec_digest = _declared_of(parent, idx)
        bev = bool(src and str(src).startswith("bevdirect@"))
        has_view = _has_view(rec)
        on_index = bool(have) and str(obs.get("declared_source") or "").startswith("index-")
        # a bare NEO1 posted on the NE server's /declared view: confirmed when the NE server
        # holds exactly the rows we built on it for our current file digest
        v = c.get("view") or {}
        view_ok = bool(have) and have == v.get("obs_digest") and v.get("of") == want \
            and not str(obs.get("declared_source") or "").startswith("index-")
        decl_ok = True
        ok = False
        if bev:
            out["bev_checked"] += 1
            bev_digest = (rec.get("bev") or {}).get("digest")
            decl_ok = bool(nec_digest) and obs.get("declared_digest") == nec_digest and bev_digest == nec_digest
            c["bev"] = {"nec1": nec_digest, "ne_server_bev": bev_digest, "obs_declared": obs.get("declared_digest")}
            ok = (have == want and decl_ok) or view_ok
        elif has_view:
            ok = view_ok and str(obs.get("declared_source") or "").startswith("chunks@")
            if ok:
                out["on_view"] += 1
            elif on_index:
                out["obs_on_index"] += 1
        else:
            out["no_view"] += 1
            ok = have == want or view_ok
        # the NE server's obs_complete pass: the view holds cells our obs lacks
        omx = om_kgs.get(parent)
        om_due = False
        if ok and omx:
            key = f"{omx.get('view_source')}:{omx.get('missing_n')}"
            oc = c.get("obs_missing") or {}
            if oc.get("key") != key:
                om_due = True
            elif str(oc.get("posted_at") or "") < om_pass and int(oc.get("attempts") or 0) < OBS_MISSING_MAX_POSTS:
                om_due = True
            if om_due:
                out["obs_missing_due"] += 1
            else:
                out["obs_missing_held"] += 1
        # ne_viewfill staged a re-post (borrowed rows for blank view cells /
        # staged union): forces one post on the view even when umfeld holds
        # our current digest — the rows it holds are the old blank ones.
        vf_due = bool(c.get("repost"))
        if vf_due:
            om_due = True
            out["viewfill_due"] = int(out.get("viewfill_due") or 0) + 1
        # ne_viewfill staged a re-post (borrowed rows for blank view cells /
        # staged union): forces one post on the view even when the NE server holds
        # our current digest — the rows it holds are the old blank ones.
        vf_due = bool(c.get("repost"))
        if vf_due:
            om_due = True
            out["viewfill_due"] = int(out.get("viewfill_due") or 0) + 1
        if ok and not om_due:
            if c.get("confirmed_digest") != want:
                c.update(confirmed_digest=want, confirmed_at=_now(), post_attempts=0)
                c.pop("post_next", None)
            out["confirmed"] += 1
            if bev:
                out["bev_confirmed"] += 1
            continue
        if om_due:
            pass
        elif bev and have == want and not decl_ok:
            out["bev_declared_mismatch"] += 1
        else:
            out["missing" if not have else "mismatch"] += 1
        if not bev and rec.get("status") not in (None, "built"):
            out["pending_cells"] += 1
            continue     # the NE server has not built the KG's declared layer yet (bev posts carry their own)
        if want != c.get("posted_digest") and c.get("posted_digest"):
            # our export changed (new product / post-processing pass) → fresh retry ladder
            c.update(post_attempts=0); c.pop("post_next", None); c.pop("needs_bevdirect", None)
        if c.get("needs_bevdirect") and has_view and not c.get("needs_bevdirect_view"):
            # parked on the lu cell set — the view path can take it now (one fresh try)
            c.pop("needs_bevdirect", None); c.update(post_attempts=0); c.pop("post_next", None)
        if c.get("needs_bevdirect"):
            out["needs_bevdirect"] += 1
            continue     # bare NEO1 can never match the NE server's lu cell set — needs a bevdirect NEC1 (see below)
        if float(c.get("post_next") or 0) > now or int(c.get("post_attempts") or 0) >= POST_MAX_ATTEMPTS:
            continue
        # Which body: a bevdirect parent whose NEC1 the NE server does not hold yet
        # (bev digest ≠ ours) posts NEC1+NEO1 so the NE server gets the declared
        # layer; everything else posts a bare NEO1 re-keyed onto the NE server's
        # /declared view (the set the NE server validates against — bev parents
        # included: the NE server re-keys even a NEC1+NEO1 post onto its view and
        # answers with the view's declared_digest, which is why the old
        # "obs.declared_digest == our NEC1" rule looped 1004 parents).
        bev_body = bev and (rec.get("bev") or {}).get("digest") != nec_digest and not c.get("bev_body_refused")
        on_view = not bev_body and (has_view or bool(rec.get("declared")) or bev or bool(omx))
        # the NE server's order: obs_on_index (has_view first) → obs_missing → missing → mismatch;
        # a freshly swept parent (sliver_sweep integrated after the last
        # confirmation) jumps the mismatch backlog — the strip cells are
        # the whole point of the sweep (docs/sliver-sweep.md).
        swept_at = str((idx["parents"].get(parent) or {}).get("swept_at") or "")
        sweep_fresh = bool(swept_at) and swept_at > str(c.get("confirmed_at") or "")
        prio = (-1 if (sweep_fresh or vf_due) else 0 if (on_index and has_view) else 1 if om_due else 2 if not have else 3,
                -int((omx or {}).get("missing_n") or 0), 0 if has_view else 1, parent)
        cands.append((prio, parent, {"want": want, "have": have, "bev": bev, "bev_body": bev_body, "on_view": on_view,
                                     "has_view": has_view, "on_index": on_index, "c": c,
                                     "om": (dict(omx, key=f"{omx.get('view_source')}:{omx.get('missing_n')}") if om_due else None)}))
    # --- phase 2: POST in priority order within the tick / day budgets
    cands.sort(key=lambda t: t[0])
    posts = 0
    last_post = 0.0
    for _prio, parent, k in cands:
        if posts >= max_posts:
            break
        if pd["bytes"] >= day_cap:
            out["day_cap_hold"] += 1
            continue     # NE-server per-token daily volume — resumes at 00:00 UTC
        want, have, bev, has_view, c = k["want"], k["have"], k["bev"], k["has_view"], k["c"]
        bev_body, on_view, omx = k["bev_body"], k["on_view"], k["om"]
        why = ("obs-missing" if omx else "missing" if not have else
               "on-index" if k["on_index"] and has_view else
               "declared-mismatch" if have == want else "mismatch")
        try:
            gap = POST_MIN_GAP_S - (time.time() - last_post)
            if gap > 0:
                time.sleep(gap)
            last_post = time.time()
            st = None
            if bev_body:
                sec = ne_h3.neo1_section_from_h3b(path_for(parent), green_fix=green_fix)
                nec = ne_h3.nec1_section_from_h3b(path_for(parent))
                r = ne_h3.post_observed(parent, sec, nec1_section=nec)
                nbytes = len(sec) + len(nec)
            elif on_view:
                r, sec, st = _post_bare_on_view(parent, want, c, green_fix)
                nbytes = len(sec)
                c["view"] = {"of": want, "obs_digest": st["digest"], "state": st["state"], "view_digest": st["view_digest"],
                             "cells_n": st["cells_n"], "dropped": st["dropped"], "filled": st["filled"],
                             "borrowed": st.get("borrowed", 0), "hidden": st.get("hidden"), "at": _now()}
                c.pop("needs_bevdirect_view", None)
                out["view_posted"] += 1
                out["view_dropped"] += int(st["dropped"]); out["view_filled"] += int(st["filled"])
                out["view_borrowed"] += int(st.get("borrowed") or 0)
                vp = idx["stats"].setdefault("view_pass", {})
                vp["posted"] = int(vp.get("posted") or 0) + 1
                vp["cells_filled"] = int(vp.get("cells_filled") or 0) + int(st["filled"])
                vp["cells_borrowed"] = int(vp.get("cells_borrowed") or 0) + int(st.get("borrowed") or 0)
                vp["cells_dropped"] = int(vp.get("cells_dropped") or 0) + int(st["dropped"])
                if k["on_index"]:
                    vp["reposted_on_index"] = int(vp.get("reposted_on_index") or 0) + 1
                if omx:
                    vp["reposted_obs_missing"] = int(vp.get("reposted_obs_missing") or 0) + 1
                vp["last_at"] = _now()
                if isinstance(r, dict) and r.get("declared_layer") == "lu":
                    log.warning("ne_ingest: NEO1 %s answered declared_layer=lu on the view path — pipeline bug? %s",
                                parent, str(r)[:200])
            else:
                sec = ne_h3.neo1_section_from_h3b(path_for(parent), green_fix=green_fix)
                r = ne_h3.post_observed(parent, sec)
                nbytes = len(sec)
            posts += 1
            pd["bytes"] += nbytes
            pd["posts"] += 1
            c.update(post_attempts=int(c.get("post_attempts") or 0) + 1, posted_digest=want,
                     posted_at=_now(), post_result=str(r)[:160])
            c.pop("post_error", None)
            c["post_next"] = now + 600          # re-check after the NE server rescans (30 s) + slack
            if omx:
                oc = c.get("obs_missing") or {}
                c["obs_missing"] = {"key": omx["key"], "missing_n": omx.get("missing_n"), "posted_at": _now(),
                                    "pass_at": om_pass, "borrowed": int((st or {}).get("borrowed") or 0),
                                    "unseen": int((st or {}).get("filled") or 0),
                                    "attempts": (int(oc.get("attempts") or 0) + 1) if oc.get("key") == omx["key"] else 1}
                c.update(post_attempts=0)       # an obs_missing re-post is not a retry of a failed confirm
            out["posted"] += 1
            idx["stats"]["posts"] = int(idx["stats"].get("posts") or 0) + 1
            if bev_body:
                out["bev_posted"] += 1
                c["lu_diff"] = r.get("lu_diff") if isinstance(r, dict) else None
            log.info("ne_ingest: NEO1 %s %s at the NE server (ours %s, theirs %s) — posted%s: %s", parent, why, want, have,
                     " as NEC1+NEO1" if bev_body else (f" on view {st['state']} (cells={st['cells_n']} dropped={st['dropped']}"
                                                       f" borrowed={st.get('borrowed', 0)} unseen={st['filled']} hidden={st.get('hidden')})" if st else ""),
                     str(r)[:120])
        except Exception as ex:  # noqa: BLE001
            n = int(c.get("post_attempts") or 0) + 1
            c.update(post_attempts=n, post_error=str(ex)[:200], post_next=now + POST_BACKOFF_S * (2 ** min(n, 4)))
            out["post_failed"] += 1
            if bev_body and " 400" in str(ex):
                # the NE server refuses our NEC1 (e.g. a union layer after a block/sweep merge) — the
                # view path carries the observations without it
                c["bev_body_refused"] = _now()
            if not bev_body and " 409" in str(ex) and ("cell set" in str(ex) or has_view):
                # our (lu-declared) cell set ≠ the NE server's lu and the NE server has no bev
                # layer for this KG: retrying the bare NEO1 is pointless — the
                # KG needs a bevdirect NEC1 in front of the obs (reprocess on
                # a bev-mode peer).  Park it until our digest changes.
                c["needs_bevdirect"] = _now()
                if has_view:
                    c["needs_bevdirect_view"] = _now()   # even the view path 409s — do not retry on this digest
                out["needs_bevdirect"] += 1
                log.warning("ne_ingest: NEO1 %s parked: bare NEO1 cell set ≠ NE server %s and no bev layer — "
                            "needs a bevdirect NEC1+NEO1 product", parent, "view" if has_view else "lu")
            if bev_body:
                out["bev_post_failed"] += 1
                idx["stats"]["bev_post_fail_at"] = now
                idx["stats"]["bev_post_fail_msg"] = f"{parent}: {str(ex)[:160]}"
            log.warning("ne_ingest: NEO1 post %s%s failed (attempt %d): %s", parent,
                        " (NEC1+NEO1)" if bev_body else (" (on view)" if on_view else ""), n, str(ex)[:160])
    out["candidates"] = len(cands)
    idx["stats"]["ne_server_last"] = dict(out, at=_now())
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
        try:
            # retired old-layout blocks (stale_blocks.apply journal) — must
            # run under this lock, on the live idx, before pending()/merge
            import stale_blocks as _sb
            if _sb.consume_ne_journal(idx, entries):
                _save_index(idx)
            # drop failure records whose product is gone (status=removed /
            # re-uploaded): they can never be retried and only inflate the
            # ``failed=`` counter on /process.txt
            stale = [c for c, f in (idx.get("failed") or {}).items()
                     if not _committed(entries.get(c + "_ne_h3b") or {})
                     or (entries[c + "_ne_h3b"].get("uploaded_at") != f.get("uploaded_at"))]
            if stale:
                for c in stale:
                    idx["failed"].pop(c, None)
                log.info("ne_ingest: pruned %d stale failure record(s): %s", len(stale), ",".join(sorted(stale))[:200])
                _save_index(idx)
        except Exception as ex:  # noqa: BLE001
            log.warning("ne_ingest stale-block journal: %s", ex)
        try:
            r = consume_viewfill(idx, entries)
            if r:
                out["viewfill"] = r
                _save_index(idx)
        except Exception as ex:  # noqa: BLE001
            log.warning("ne_ingest viewfill: %s", ex)
        try:
            out["union_recovery"] = {k: v for k, v in recover_dropped(idx, entries).items() if not k.endswith("_codes")}
        except Exception as ex:  # noqa: BLE001
            log.warning("ne_ingest union recovery: %s", ex)
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
            # Sliver sweep (sliver_sweep.py): refresh the NE server's bbox_changes,
            # plan units for complete parents, merge received sweep h3bs
            # into the parent files → confirm_ne_server below sees the new
            # digest and re-posts.
            import sliver_sweep as _sls
            out["sweep"] = _sls.tick(idx, entries)
        except Exception as ex:  # noqa: BLE001
            log.warning("ne_ingest sweep pass: %s", ex)
        try:
            out["ne_server"] = confirm_ne_server(idx)
        except Exception as ex:  # noqa: BLE001
            log.warning("ne_ingest NE server confirm: %s", ex)
        _save_index(idx)
        out["pending"] = max(0, len(todo) - out["pulled"] - out["failed"])
    finally:
        out["dt"] = round(time.time() - t0, 1); out["ts"] = _now()
        _last_tick.clear(); _last_tick.update(out)
        _lock.release()
    if out["pulled"] or out["failed"] or out["merges"] or (out.get("ne_server") or {}).get("posted"):
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
    um = (idx.get("stats") or {}).get("ne_server_last") or {}
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
        "ne_server": {k: um.get(k) for k in ("checked", "confirmed", "missing", "mismatch", "posted",
                                           "post_failed", "pending_cells", "at", "bev_checked", "bev_confirmed",
                                           "bev_declared_mismatch", "bev_posted", "bev_post_failed",
                                           "needs_bevdirect", "day_cap_hold", "day_bytes", "obs_on_index", "on_view",
                                           "view_posted", "view_dropped", "view_filled", "view_borrowed", "no_view",
                                           "obs_missing_due", "obs_missing_held", "obs_missing_listed", "recovery_hold", "candidates",
                                           "viewfill_due")},
        "view_pass": (idx.get("stats") or {}).get("view_pass"),
        "obs_missing": {k: v for k, v in ((idx.get("stats") or {}).get("obs_missing") or {}).items() if k != "kgs"},
        "union_recovery": (idx.get("stats") or {}).get("union_recovery"),
        "viewfill": {k: v for k, v in ((idx.get("stats") or {}).get("viewfill") or {}).items()},
        "post_day": (idx.get("stats") or {}).get("post_day"),
        "green_fix": _green_fix_status(idx),
        "bev_post_fail_at": (idx.get("stats") or {}).get("bev_post_fail_at"),
        "bev_post_fail_msg": (idx.get("stats") or {}).get("bev_post_fail_msg"),
        "last_tick": dict(_last_tick),
    }


def _green_fix_status(idx: dict) -> dict:
    """Read-time green-ground override (``ne_green_fix``): how many parents
    have a fixed digest computed, how many of those NE-server confirmed."""
    import ne_green_fix
    conf = idx.get("ne_server") or {}
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


def green_fix_lines(limit: int = 20) -> list[str]:
    """``/process.txt?greenfix=1`` — the read-time bau/alpen→gruen override
    (``ne_green_fix``) end to end: how many parents have a fixed digest,
    how many NE-server confirmed, the re-POST backlog by reason (queued /
    posted-awaiting-manifest / struck-out / parked / day-cap), today's
    budget, and the *limit* most recently confirmed + oldest unconfirmed
    parents.  Everything comes from the ingest index (no disk walk)."""
    import ne_green_fix
    idx = load_index_ro()
    conf = idx.get("ne_server") or {}
    st = idx.get("stats") or {}
    now = time.time()
    rows_conf, rows_wait = [], []
    by = {"confirmed": 0, "posted": 0, "queued": 0, "struck_out": 0, "over_cap": 0, "parked": 0, "no_fix": 0}
    cells = 0
    parents = {c for c in idx["files"] if not is_block(c)} | set(idx["parents"])
    for parent in parents:
        tgt, _ = _disk_row(parent, idx)
        gf = (tgt or {}).get("green_fix") or {}
        if gf.get("id") != ne_green_fix.FIX_ID:
            by["no_fix"] += 1
            continue
        cells += int(gf.get("cells") or 0)
        c = conf.get(parent) or {}
        want = gf.get("digest")
        if c.get("confirmed_digest") == want:
            by["confirmed"] += 1
            rows_conf.append((str(c.get("confirmed_at") or ""), parent, int(gf.get("cells") or 0)))
        else:
            if c.get("posted_digest") == want:
                why = "posted"
            elif c.get("needs_bevdirect"):
                why = "parked"
            elif int(c.get("post_attempts") or 0) >= POST_MAX_ATTEMPTS:
                why = "over_cap" if "over-cap" in str(c.get("post_error") or "") or "> 16 MB" in str(c.get("post_error") or "") else "struck_out"
            else:
                why = "queued"
            by[why] += 1
            rows_wait.append((str(c.get("confirmed_at") or ""), parent, int(gf.get("cells") or 0), why,
                              int(c.get("post_attempts") or 0), float(c.get("post_next") or 0)))
    pdy = st.get("post_day") or {}
    cap = float(_knob("ne_post_gib_per_day", POST_GIB_PER_DAY))
    out = [f"green_fix detail ({ne_green_fix.FIX_ID}{'' if _knob('ne_green_fix', True) else ' OFF'}, ndvi≥{ne_green_fix.NDVI_MIN} & not road/bare → "
           f"{'+'.join(ne_green_fix.MOVE_GROUPS)}→{ne_green_fix.TARGET_GROUP}; read-time only, files untouched):",
           f"    parents fixed={len(parents) - by['no_fix']}/{len(parents)} cells_moved={cells:,} · NE-server confirmed={by['confirmed']} "
           f"posted(awaiting manifest)={by['posted']} queued={by['queued']} struck_out={by['struck_out']} over_cap(body>NE server /obs cap, ne_h3.OBS_BODY_CAP)={by['over_cap']} parked(needs_bevdirect)={by['parked']} "
           f"no_fix_digest_yet={by['no_fix']} (computed {_knob('ne_green_fix_compute_per_tick', GF_COMPUTE_PER_TICK)}/tick)",
           f"    budget: today {int(pdy.get('posts') or 0)} POSTs {(pdy.get('bytes') or 0) / 2 ** 20:.0f}/{cap * 1024:.0f} MiB · "
           f"max {POST_MAX_ATTEMPTS} attempts/digest · confirmed = NE-server manifest obs.digest == our green-fixed digest"]
    rows_conf.sort(reverse=True)
    if rows_conf:
        out.append("    recently confirmed: " + " ".join(f"{p}({n}c {at[11:16]}Z)" for at, p, n in rows_conf[:limit]))
    rows_wait.sort(key=lambda r: (0 if r[3] in ('struck_out', 'over_cap') else 1 if r[3] == 'parked' else 2, r[0], r[1]))
    for at, p, n, why, att, nxt in rows_wait[:limit]:
        extra = f" next in {max(0, int(nxt - now)) // 60}m" if nxt > now else ""
        out.append(f"    {p:<8} {why:<10} cells={n:<7} attempts={att}{extra} last_confirmed={at[:16] or '-'}")
    if len(rows_wait) > limit:
        out.append(f"    … +{len(rows_wait) - limit} more unconfirmed (?greenfix=N)")
    return out


def text_line(v: dict | None = None) -> str:
    v = v if v is not None else status()
    um = v.get("ne_server") or {}
    s = (f"ne_ingest: corpus={v['parents']}p+{v['blocks']}b {v['corpus_mb']}MB merged={v['merged_parents']}"
         f"({v['merged_complete']} complete) pending={v['pending']} ({v['pending_mb']}MB)"
         f" budget={v['used_mb_h']}/{v['budget_mb_h']}MB/h src peer={v['pulled_peer']} zenodo={v['pulled_zenodo']}")
    if not v.get("enabled"):
        s += " DISABLED"
    if v.get("failed"):
        s += f" failed={v['failed']}"
    if um.get("checked") is not None:
        s += (f"\n    ne_server_confirm: ok={um.get('confirmed')}/{um.get('checked')} missing={um.get('missing')}"
              f" mismatch={um.get('mismatch')} reposted={v.get('posts')} unbuilt={um.get('pending_cells')}")
        if um.get("bev_checked"):
            s += (f" · bev(NEC1+NEO1): ok={um.get('bev_confirmed')}/{um.get('bev_checked')}"
                  f" declared_mismatch={um.get('bev_declared_mismatch')} post_failed={um.get('bev_post_failed')}")
        gf = v.get("green_fix") or {}
        if gf:
            s += (f"\n    green_fix({gf.get('id')}{'' if gf.get('enabled') else ' OFF'}): ne_server_confirmed="
                  f"{gf.get('confirmed')}/{gf.get('computed')} parents, {gf.get('cells_touched')} cells bau/alpen→gruen"
                  f" (read-time override, files untouched; ne_green_fix.py)")
        pdy = v.get("post_day") or {}
        s += (f"\n    resync(by digest): today {int(pdy.get('posts') or 0)} POSTs {(pdy.get('bytes') or 0) / 2 ** 20:.0f} MiB"
              f" (cap {float(_knob('ne_post_gib_per_day', POST_GIB_PER_DAY)):.1f} GiB/day, ≤2/s)")
        if um.get("day_cap_hold"):
            s += f" DAY-CAP-HOLD {um['day_cap_hold']} waiting"
        vp = v.get("view_pass") or {}
        if vp or um.get("obs_on_index") is not None:
            s += (f"\n    view_pass(ne-change §4b): NE server kgs_obs_on_index {vp.get('obs_on_index_before')}→{vp.get('obs_on_index_now')}"
                  f" · ours: on_view_ok={um.get('on_view')} obs_on_index={um.get('obs_on_index')} no_view={um.get('no_view')}"
                  f" · posted_on_view={vp.get('posted') or 0} (of which on-index {vp.get('reposted_on_index') or 0},"
                  f" obs-missing {vp.get('reposted_obs_missing') or 0})"
                  f" cells borrowed={vp.get('cells_borrowed') or 0} unseen={vp.get('cells_filled') or 0}"
                  f" dropped={vp.get('cells_dropped') or 0}"
                  f" since {vp.get('started_at')} last {vp.get('last_at')} · ?q=on+view for rows")
        omx = v.get("obs_missing") or {}
        if omx.get("kgs_n") is not None or um.get("obs_missing_listed"):
            s += (f"\n    obs_missing(NE-server revisit): listed={omx.get('kgs_n')} KGs / {omx.get('cells_n')} cells"
                  f" (pass {omx.get('pass_at')}, no_obs={omx.get('kgs_no_obs')}) · ours: due={um.get('obs_missing_due')}"
                  f" held={um.get('obs_missing_held')} — view cells a neighbour observed are borrowed from the corpus,"
                  f" the rest post as unseen (sweep territory)")
            if omx.get("error"):
                s += f" LAST-ERR {omx['error'][:80]}"
        ur = v.get("union_recovery") or {}
        if ur and not ur.get("done"):
            s += (f"\n    union_recovery: {ur.get('remerge_done', 0)}/{ur.get('remerge', 0)} split parents re-merged,"
                  f" {ur.get('repull_done', 0)}/{ur.get('repull', 0)} unsplit re-pulled, {um.get('recovery_hold')} posts held (sweep merge dropped"
                  f" {ur.get('cells_dropped')} observed bev cells; union merge since 2026-11-04)")
        if um.get("needs_bevdirect"):
            s += f" · needs_bevdirect={um['needs_bevdirect']} (lu cell-set 409, parked — reprocess on a bev-mode peer)"
        try:
            import ne_viewfill as _nvf
            vf = v.get("viewfill") or {}
            s += ("\n    " + _nvf.text_line()
                  + f" · tick: applied={vf.get('applied', 0)} swapped={vf.get('swapped', 0)} rejected={vf.get('rejected', 0)}"
                  f" sweep={vf.get('planned_cells', 0)}c/{vf.get('planned_kgs', 0)}kg repost={vf.get('repost', 0)}"
                  f" errors={vf.get('errors', 0)} due={um.get('viewfill_due', 0)}"
                  " (ne_viewfill.py: every cell of the NE server's /declared view observed — corpus borrow → own products"
                  " union → sweep units on the view; docs/ne-viewfill.md)")
        except Exception as _ex:  # noqa: BLE001
            s += f"\n    viewfill: {_ex}"
        try:
            import ne_viewfill as _nvf
            vf = v.get("viewfill") or {}
            s += ("\n    " + _nvf.text_line()
                  + f" · tick: applied={vf.get('applied', 0)} swapped={vf.get('swapped', 0)} rejected={vf.get('rejected', 0)}"
                  f" sweep={vf.get('planned_cells', 0)}c/{vf.get('planned_kgs', 0)}kg repost={vf.get('repost', 0)}"
                  f" errors={vf.get('errors', 0)} due={um.get('viewfill_due', 0)}"
                  " (ne_viewfill.py: every cell of umfeld's /declared view observed — corpus borrow → own products"
                  " union → sweep units on the view; docs/ne-viewfill.md)")
        except Exception as _ex:  # noqa: BLE001
            s += f"\n    viewfill: {_ex}"
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
