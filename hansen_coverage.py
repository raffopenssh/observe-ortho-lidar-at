"""Hansen 0.5° tile coverage audit for the Zenodo tile cache.

Answers one question for ``/process.txt``: *which 0.5° Hansen tiles that
some KG needs are NOT on Zenodo, and for how long?*

Motivation (2026-10-18): GCS egress to the UMD Hansen bucket collapsed
fleet-wide; a handful of Tyrol tiles stayed uncached for hours while the
repair plan kept re-asking for them. Nothing on the dashboard said which
tiles were missing, so the gap was only visible by grepping warnings.
This module makes persistent gaps a first-class, cheap line:

    hansen_cover: zenodo=59/68 cells missing=10 (oldest 3d2h) HANSEN-GAPS
        10.0/46.5(kgs=41,3d2h) 12.0/46.5(kgs=37,3d2h) ...

*Needed* cells = every 0.5° grid cell intersected by a KG bbox in the
search index (that is exactly what ``HansenTileCache`` requests when the
KG is processed). *Have* cells = cells fully contained by some
``hansen_*.npz`` entry of a live ``hansen_*.zip`` in ``cache_manifest.json``
(ZIP indices are the on-disk ``zenodo_zip_index`` cache — only ZIPs whose
index is not cached yet cost a HEAD + central-directory read).

State lives in ``data/austria_processor/hansen_coverage.json`` and is
refreshed at most every ``TTL_S``; per-cell ``first_missing`` timestamps
persist across refreshes so the line can say how long a gap has existed.
A cell missing for more than ``GAP_FLAG_H`` hours raises the
``HANSEN-GAPS`` health flag.

CLI: ``python3 hansen_coverage.py [--refresh]``.
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

log = logging.getLogger(__name__)

STEP = 0.5
DATA_DIR = Path(os.environ.get("SRTM_DATA_DIR", "data"))
STATE_PATH = DATA_DIR / "austria_processor" / "hansen_coverage.json"
MANIFEST_PATH = DATA_DIR / "austria_processor" / "cache_manifest.json"
INDEX_DB = DATA_DIR / "search_index.db"
TTL_S = float(os.environ.get("HANSEN_COVERAGE_TTL_S", str(60 * 60)))
GAP_FLAG_H = float(os.environ.get("HANSEN_COVERAGE_GAP_FLAG_H", "24"))

_NPZ_RE = re.compile(r"hansen_([\d.]+)_([\d.]+)_([\d.]+)_([\d.]+)\.npz$")

Cell = Tuple[float, float]  # (lon_w, lat_s)


def _key(c: Cell) -> str:
    return f"{c[0]:.1f}/{c[1]:.1f}"


def needed_cells(db_path: Path = INDEX_DB) -> Dict[str, Dict[str, int]]:
    """cell key → {kgs: KGs whose bbox intersects the cell, todo: of
    those, not yet processed}. ``todo > 0`` means a frontier will request
    the tile organically; ``todo == 0`` means only repair/re-upgrade
    ever asks for it."""
    out: Dict[str, Dict[str, int]] = {}
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        rows = con.execute(
            "SELECT min_lon, min_lat, max_lon, max_lat, COALESCE(processed, 0) "
            "FROM kg WHERE min_lon IS NOT NULL").fetchall()
    finally:
        con.close()
    for w, s, e, n, done in rows:
        x = math.floor(w / STEP) * STEP
        while x < e:
            y = math.floor(s / STEP) * STEP
            while y < n:
                d = out.setdefault(_key((x, y)), {"kgs": 0, "todo": 0})
                d["kgs"] += 1
                if not done:
                    d["todo"] += 1
                y += STEP
            x += STEP
    return out


def local_cells(tiles_dir: Path = DATA_DIR / "austria_processor" / "hansen_tiles") -> Set[str]:
    """0.5° cells for which THIS host holds a Hansen tile on disk
    (``hansen_*.meta.json`` sidecars). On the primary these are tiles
    that could be flushed to Zenodo with ``ZenodoCache.upload_all()``."""
    out: Set[str] = set()
    try:
        for f in tiles_dir.glob("hansen_*.meta.json"):
            try:
                d = json.loads(f.read_text())
                if not f.with_suffix("").with_suffix(".npz").exists():
                    continue
                out.add(_key((float(d["w"]), float(d["s"]))))
            except Exception:
                continue
    except Exception:
        pass
    return out


def have_cells(allow_remote: bool = True) -> Tuple[Set[str], int]:
    """Cells covered by a live hansen_*.zip on Zenodo → (keys, n_zips).

    A cell counts as covered when some NPZ entry's bbox contains it
    (legacy 1.0° tiles cover four 0.5° cells).
    """
    try:
        cmf = json.loads(MANIFEST_PATH.read_text())
    except Exception:
        return set(), 0
    zips = [k for k, v in (cmf.get("files") or {}).items()
            if k.startswith("hansen") and k.endswith(".zip")
            and int((v or {}).get("size") or 0) > 0]
    if not zips:
        return set(), 0
    import zenodo_cache
    zc = zenodo_cache.ZenodoCache()
    have: Set[str] = set()
    n_ok = 0
    for name in zips:
        try:
            idx = zc._get_zip_index(name)
            if idx is None:
                continue
            if not allow_remote and not idx._index_path.exists():
                continue
            entries = idx.list_entries()
        except Exception as e:
            log.debug("hansen_coverage: index %s: %s", name, e)
            continue
        n_ok += 1
        for ent in entries:
            m = _NPZ_RE.search(ent)
            if not m:
                continue
            s, w, n, e = (float(g) for g in m.groups())
            x = math.floor(w / STEP + 1e-6) * STEP
            while x + STEP <= e + 1e-6:
                y = math.floor(s / STEP + 1e-6) * STEP
                while y + STEP <= n + 1e-6:
                    have.add(_key((x, y)))
                    y += STEP
                x += STEP
    return have, n_ok


def _load_state() -> dict:
    try:
        return json.loads(STATE_PATH.read_text())
    except Exception:
        return {}


def refresh(allow_remote: bool = True) -> dict:
    """Recompute coverage and persist. Keeps ``first_missing`` per cell."""
    prev = _load_state()
    prev_missing = (prev.get("missing") or {})
    need = needed_cells()
    have, n_zips = have_cells(allow_remote=allow_remote)
    local = local_cells()
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    missing = {}
    for k, d in need.items():
        if k in have:
            continue
        missing[k] = {
            "kgs": d["kgs"],
            "todo": d["todo"],
            "local": k in local,
            "first_missing": (prev_missing.get(k) or {}).get("first_missing") or now_iso,
        }
    st = {
        "at": time.time(),
        "at_iso": now_iso,
        "needed": len(need),
        "have": len([k for k in need if k in have]),
        "zips": n_zips,
        "missing": missing,
        "healed": sorted(k for k in prev_missing if k not in missing),
    }
    try:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(st, indent=1, sort_keys=True))
        os.replace(tmp, STATE_PATH)
    except Exception as e:
        log.debug("hansen_coverage: persist: %s", e)
    return st


def summary(max_age_s: float = TTL_S, allow_remote: bool = True) -> dict:
    """Cached coverage state (refreshes when older than ``max_age_s``)."""
    st = _load_state()
    if not st or time.time() - float(st.get("at") or 0) > max_age_s:
        try:
            st = refresh(allow_remote=allow_remote)
        except Exception as e:
            log.debug("hansen_coverage: refresh failed: %s", e)
            if not st:
                raise
    return st


def _age_h(iso: str) -> float:
    try:
        return (datetime.now(timezone.utc) - datetime.fromisoformat(iso)).total_seconds() / 3600
    except Exception:
        return 0.0


def _fmt_age(h: float) -> str:
    if h < 1:
        return f"{h * 60:.0f}m"
    if h < 48:
        return f"{h:.0f}h"
    d, r = divmod(h, 24)
    return f"{d:.0f}d{r:.0f}h"


def render_lines(st: dict, max_cells: int = 12) -> Tuple[List[str], Optional[str]]:
    """(/process.txt lines, health flag or None).

    Cells are sorted by KG demand; the flag fires when any cell has been
    missing longer than ``GAP_FLAG_H``.
    """
    missing = st.get("missing") or {}
    aged = {k: _age_h(v.get("first_missing", "")) for k, v in missing.items()}
    oldest = max(aged.values(), default=0.0)
    flag = None
    if missing and oldest >= GAP_FLAG_H:
        flag = f"HANSEN-GAPS({len(missing)} cells, oldest {_fmt_age(oldest)})"
    head = (f"hansen_cover: zenodo={st.get('have', 0)}/{st.get('needed', 0)} cells "
            f"zips={st.get('zips', 0)} missing={len(missing)}"
            + (f" (oldest {_fmt_age(oldest)})" if missing else "")
            + (" HANSEN-GAPS" if flag else "")
            + f" · as_of={st.get('at_iso', '?')[11:16]}Z"
            + (f" healed={len(st['healed'])}" if st.get("healed") else ""))
    lines = [head]
    if missing:
        rows = sorted(missing.items(), key=lambda kv: (-kv[1].get("kgs", 0), kv[0]))
        cells = " ".join(
            f"{k}(kgs={v.get('kgs', 0)}/todo={v.get('todo', 0)},{_fmt_age(aged[k])}"
            + (",on-primary-disk" if v.get("local") else "") + ")"
            for k, v in rows[:max_cells])
        more = f" … +{len(rows) - max_cells}" if len(rows) > max_cells else ""
        lines.append(f"    missing lon/lat(SW corner, 0.5°): {cells}{more}")
        n_local = sum(1 for v in missing.values() if v.get("local"))
        lines.append("    todo>0 → a frontier fetches it when it reaches those KGs (UMD, 2h .fail negative cache per peer); "
                     "todo=0 → only repair/re-upgrade asks. "
                     + (f"{n_local} tile(s) sit on the primary's disk: `python3 hansen_coverage.py --flush` uploads them. "
                        if n_local else "")
                     + "docs/zenodo-cache.md → Hansen gaps")
    return lines, flag


def probe_peers(missing: Set[str], timeout: float = 20.0, workers: int = 16) -> Dict[str, List[str]]:
    """Ask every registered peer which of the *missing* cells it holds.

    Uses each peer's ``/api/v1/processing/tiles`` (ZIP indices + local
    sidecars); anything there that Zenodo lacks must be local. One GET
    per peer — CLI only, never from the dashboard render.
    """
    import concurrent.futures as cf
    import requests
    tok = ""
    try:
        tok = (DATA_DIR / "admin_token").read_text().strip()
    except Exception:
        pass
    peers: List[Tuple[str, str]] = []
    try:
        pj = json.loads((DATA_DIR / "austria_processor" / "peers.json").read_text())
        items = pj.get("peers", pj)
        items = items.values() if isinstance(items, dict) else items
        for p in items:
            if p.get("url") and p.get("id") != "primary":
                peers.append((p["id"], p["url"].rstrip("/")))
    except Exception as e:
        log.warning("hansen_coverage: peers.json: %s", e)

    def _probe(p):
        pid, url = p
        try:
            r = requests.get(url + "/api/v1/processing/tiles",
                             headers={"X-Admin-Token": tok}, timeout=timeout)
            cells = {_key((float(t["w"]), float(t["s"])))
                     for t in r.json().get("hansen", [])
                     if abs(float(t["n"]) - float(t["s"]) - STEP) < 1e-6}
            return pid, cells & missing
        except Exception:
            return pid, None

    out: Dict[str, List[str]] = {k: [] for k in missing}
    with cf.ThreadPoolExecutor(workers) as ex:
        for pid, cells in ex.map(_probe, peers):
            if cells is None:
                out.setdefault("_unreachable", []).append(pid)
                continue
            for k in cells:
                out[k].append(pid)
    return out


def flush_local(dry_run: bool = False) -> dict:
    """Upload this host's local Hansen tiles to the Zenodo cache deposit
    (hansen only — the Copernicus tile store is untouched). Takes the
    fleet Zenodo lease per ZIP like the processor's flush does."""
    from contextlib import contextmanager
    import zenodo_cache
    from zenodo_lock import zenodo_upload_lock

    @contextmanager
    def _per_zip(zip_name):
        with zenodo_upload_lock(purpose=f"hansen_coverage_flush:{zip_name}") as l:
            yield l

    zc = zenodo_cache.ZenodoCache()
    st = zc.upload_all(dry_run=dry_run, per_zip_lock=_per_zip, only_products={"hansen"})
    if not dry_run:
        try:
            refresh()
        except Exception:
            pass
    return st


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--refresh", action="store_true", help="recompute now (ignore TTL)")
    ap.add_argument("--peers", action="store_true",
                    help="ask every peer which missing cells it holds locally")
    ap.add_argument("--flush", action="store_true",
                    help="upload THIS host's local Hansen tiles to Zenodo (hansen only)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO if a.flush else logging.WARNING,
                        format="%(levelname)s %(name)s: %(message)s")
    if a.flush:
        print(json.dumps(flush_local(dry_run=a.dry_run), indent=1, default=str))
    s = refresh() if (a.refresh or a.flush) else summary()
    for ln in render_lines(s, max_cells=100)[0]:
        print(ln)
    if a.peers:
        res = probe_peers(set(s.get("missing") or {}))
        unreach = res.pop("_unreachable", [])
        print("peers holding missing cells locally:")
        for k in sorted(res):
            print(f"    {k}: {' '.join(sorted(res[k])) or '-'}")
        if unreach:
            print(f"    (unreachable: {' '.join(unreach)})")
