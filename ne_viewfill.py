"""ne_viewfill — close every unobserved cell of the NE server's declared *view*.

the NE server's ``revisit?kind=obs_missing`` lists KGs whose observed layer holds
*blank* rows for cells of today's ``GET /contrib/api/v1/ne/{kg}/declared``
view (NEO1 padding the view re-key wrote for cells the pipeline never
ran on, or cells our merged file lost).  This builder walks those KGs —
largest gap first — and, per KG, makes every view cell observed from the
cheapest source that has it:

1. **corpus** — a neighbour product already observed the cell (NE server moved
   boundary cells between KGs): nothing to build, the view re-post borrows
   the row (``ne_h3.neo1_section_on_view``, which since 2026-11-04 also
   borrows for cells our file holds unseen);
2. **our own products** — the parent's own ``<kg>_ne_h3b`` (an unsplit run
   whose file a block merge overwrote: 05003 lost its west half, 2026-11-04),
   live / old-layout blocks and sweep units on disk that observe the cell →
   union merge (``ne_ingest.merge_blocks``) into a **staged** file;
3. **the pipeline** — cells unobserved anywhere are queued as sweep units
   (``sliver_sweep.plan_cells``: clustered, padded bboxes; built by the
   cache-only sweepers exactly like the bbox-revision slivers).

Runs as its own process (``ne-viewfill.service`` / hourly timer, nice 15,
memory-capped, time budget) so gunicorn / the director tick never carry
the view fetches or the merges.  It never touches ``data/ne/index.json``
or a live parent file: results go to ``data/ne/viewfill/staged/<kg>.json``
(+ ``<kg>.h3b``) and the ingest tick applies them under its own lock
(``ne_ingest.consume_viewfill``: file swap, cells index, index rows, sweep
plan, forced re-post).  Progress: ``viewfill:`` sub-line of ``ne_ingest:``
in ``/process.txt``; ``python3 ne_viewfill.py status``.

CLI::

    python3 ne_viewfill.py run [--budget-s N] [--max N] [--all] [--kgs 83103 42007]
    python3 ne_viewfill.py plan 83103          # dry run for one KG, nothing staged
    python3 ne_viewfill.py status
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path

import numpy as np

log = logging.getLogger("ne_viewfill")

_HERE = Path(__file__).resolve().parent
NE_DIR = _HERE / "data" / "ne"
VF_DIR = NE_DIR / "viewfill"
STAGED_DIR = VF_DIR / "staged"
PULLED_DIR = VF_DIR / "pulled"
STATE_FILE = VF_DIR / "state.json"
LOCK_FILE = VF_DIR / "run.lock"
CHANNEL = "viewfill"                 # inbound_budget channel (views + product pulls)
MB_PER_H_DEFAULT = 5000.0            # knob viewfill_mb_per_h — 5 GB/h: the walk, not the primary link, is the limit for the 2026-11 pass
RECHECK_S = 7 * 86400                # a KG whose view ETag + file digest are unchanged is not re-walked sooner
REPLAN_S = 7 * 86400                 # the same residual cell set is not queued to the sweepers again sooner
MIN_RESIDUAL_FOR_SWEEP = 1


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _knob(key: str, default):
    try:
        import peer_director as _pd
        return _pd.knob(None, key, default)
    except Exception:
        return default


# === SECTION: state ===

def load_state() -> dict:
    try:
        d = json.loads(STATE_FILE.read_text())
        if isinstance(d, dict):
            d.setdefault("kgs", {}); d.setdefault("stats", {}); d.setdefault("runs", [])
            return d
    except Exception:
        pass
    return {"kgs": {}, "stats": {}, "runs": []}


def save_state(d: dict) -> None:
    VF_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(d, indent=0, sort_keys=True, default=str))
    tmp.replace(STATE_FILE)


# === SECTION: candidates ===

def ne_server_obs_missing() -> dict:
    """``{kg: {blank_n, missing_n, view_source}}`` from the NE server's revisit list
    (empty between its 3-hourly passes — never the only source)."""
    try:
        import requests
        import ne_h3
        h = {"User-Agent": "observe-ortho-lidar-at/2.4 ne_viewfill"}
        h.update(ne_h3._ne_server_headers())
        r = requests.get(f"{ne_h3._ne_server_contrib_base()}/revisit", params={"kind": "obs_missing", "all": 1},
                         headers=h, timeout=180)
        r.raise_for_status()
        out = {}
        for x in r.json().get("obs_missing") or []:
            kg = str(x.get("kg") or "").zfill(5)
            b, m = int(x.get("blank_n") or 0), int(x.get("missing_n") or 0)
            if kg and (b or m):
                out[kg] = {"blank_n": b, "missing_n": m, "view_source": x.get("view_source")}
        return out
    except Exception as ex:  # noqa: BLE001
        log.warning("ne_viewfill: NE server revisit failed: %s", ex)
        return {}


def local_unseen(kg: str) -> tuple[int, int]:
    """``(unseen rows, cells)`` of the parent file on disk (NEO1 only)."""
    import ne_ingest
    r = ne_ingest._read_compact(ne_ingest.path_for(kg), want_lu=False, want_rows=False)
    return int((~ne_ingest.row_observed(r["obs"])).sum()), int(r["obs"].shape[0])


def mixed_parents(entries: dict, idx: dict) -> list[str]:
    """Parents with BOTH a committed own ``_ne_h3b`` and live blocks — the
    block merge used to drop the own product (``merge_dirty_parents`` now
    keeps it, but the files merged before 2026-11-04 lost it)."""
    import ne_ingest
    out = []
    for k, e in entries.items():
        if not k.endswith("_ne_h3b") or not ne_ingest._committed(e):
            continue
        code = k[:-7]
        if ne_ingest.is_block(code) or "-" in code:
            continue
        if ne_ingest._live_blocks(code, entries):
            out.append(code)
    return sorted(out)


def candidates(all_kgs: bool = False, st: dict | None = None) -> list[tuple[str, int, str]]:
    """``[(kg, estimated gap, why)]`` largest first.  Sources: the NE server's
    obs_missing list, parents whose file holds unseen rows (local scan,
    cached in the state for a day), the mixed parents; ``all_kgs`` adds
    every complete parent on disk (gap 0 → walked last)."""
    import ne_ingest
    entries = ne_ingest.load_manifest_entries()
    idx = ne_ingest.load_index_ro()
    st = st if st is not None else load_state()
    est: dict[str, tuple[int, str]] = {}
    for kg, v in ne_server_obs_missing().items():
        est[kg] = (v["blank_n"] + v["missing_n"], "NE server")
    scan = st["stats"].get("local_scan") or {}
    if time.time() - float(scan.get("ts") or 0) > 86400:
        t0 = time.time()
        un = {}
        for p in sorted(NE_DIR.glob("?????.h3b")):
            kg = p.stem
            if not ne_ingest._coverage_complete(kg, entries):
                continue
            try:
                u, n = local_unseen(kg)
            except Exception as ex:  # noqa: BLE001
                log.info("ne_viewfill: scan %s: %s", kg, ex)
                continue
            if u:
                un[kg] = u
        scan = {"ts": time.time(), "at": _now(), "unseen": un, "kgs": len(un), "cells": int(sum(un.values())),
                "dt": round(time.time() - t0, 1)}
        st["stats"]["local_scan"] = scan
        save_state(st)
        log.info("ne_viewfill: local scan: %d complete parents with %d unseen rows (%.0fs)", len(un), scan["cells"], scan["dt"])
    for kg, u in (scan.get("unseen") or {}).items():
        if u > est.get(kg, (0, ""))[0]:
            est[kg] = (int(u), "local")
    for kg in mixed_parents(entries, idx):
        if kg not in est:
            est[kg] = (1, "mixed")
    if all_kgs:
        for p in NE_DIR.glob("?????.h3b"):
            est.setdefault(p.stem, (0, "all"))
    now = time.time()
    out = []
    for kg, (g, why) in est.items():
        k = st["kgs"].get(kg) or {}
        if k.get("ts") and now - float(k["ts"]) < RECHECK_S and why in ("all", "local", "mixed") and not k.get("residual_n"):
            continue      # walked recently and nothing left we could do locally
        if not ne_ingest.path_for(kg).exists():
            continue
        out.append((kg, int(g), why))
    out.sort(key=lambda t: (-t[1], t[0]))
    return out


# === SECTION: per-KG work ===

def _ids_observed(path: Path) -> tuple[np.ndarray, np.ndarray, str]:
    """``(all ids, observed ids, neo1 digest)`` of an h3b."""
    import ne_ingest
    import ne_h3
    hdr = ne_h3.read_h3b_header(path)
    r = ne_ingest._read_compact(path, hdr, want_rows=False)
    n = int(r["obs"].shape[0])
    ids = ne_ingest._lu_ids(r["lu"], n)
    seen = ne_ingest.row_observed(r["obs"])
    return ids, ids[seen], str((hdr.get("neo1") or {}).get("digest") or "")


def _view_ids(view) -> np.ndarray:
    import ne_h3
    vn = int(view.header["cells_n"])
    return np.frombuffer(view.raw[:vn * ne_h3.LU_REC.size], np.uint8).reshape(vn, ne_h3.LU_REC.size)[:, :8] \
        .copy().view("<u8").reshape(vn)


def _pull_product(code: str, e: dict, dest: Path) -> bool:
    import ne_ingest
    import ne_h3
    import inbound_budget
    if dest.exists():
        return True
    mb_h = float(_knob("viewfill_mb_per_h", MB_PER_H_DEFAULT))
    if not inbound_budget.allow(CHANNEL, mb_h, int(e.get("size") or 0)):
        raise BudgetExhausted(f"{code}: {e.get('size')} B over the {mb_h:.0f} MB/h viewfill budget")
    blob, src, _peer = ne_ingest.pull(code, e)
    inbound_budget.add(CHANNEL, len(blob))
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".tmp")
    tmp.write_bytes(blob)
    rep = ne_h3.verify_h3b(tmp)
    if not rep.get("ok"):
        tmp.unlink(missing_ok=True)
        log.warning("ne_viewfill: %s from %s failed verify: %s", code, src, "; ".join(rep.get("errors") or [])[:160])
        return False
    tmp.replace(dest)
    log.info("ne_viewfill: pulled %s (%.2f MB, %s)", code, len(blob) / 1e6, src)
    return True


class BudgetExhausted(RuntimeError):
    pass


def _sources_for(kg: str, gap: np.ndarray, entries: dict, idx: dict, file_md5: str | None) -> list[tuple[Path, int]]:
    """Product files (other than the live parent file) that observe cells of
    *gap*: ``[(path, cells covered)]``.  Own product is pulled when the file
    on disk is not that product any more (merge output / different md5)."""
    import ne_ingest
    from kg_splitter import is_sweep_code
    cands: list[Path] = []
    e = entries.get(f"{kg}_ne_h3b")
    if ne_ingest._committed(e):
        ck = str(e.get("checksum") or "")
        try:
            import hashlib
            disk_md5 = hashlib.md5(ne_ingest.path_for(kg).read_bytes()).hexdigest()
        except Exception:  # noqa: BLE001
            disk_md5 = file_md5
        own_on_disk = bool(disk_md5) and ck == f"md5:{disk_md5}"
        if not own_on_disk:
            dest = PULLED_DIR / f"{kg}.own.h3b"
            if _pull_product(kg, e, dest):
                cands.append(dest)
    pre = kg + "-"
    for p in sorted(ne_ingest.BLOCK_DIR.glob(f"{kg}-*.h3b")):
        if p.name.startswith(pre) and not is_sweep_code(p.stem):
            cands.append(p)
    sweeps = NE_DIR / "sweeps"
    for p in sorted(sweeps.glob(f"{kg}-sw*.h3b")):
        cands.append(p)
    out = []
    for p in cands:
        try:
            _ids, seen, _d = _ids_observed(p)
        except Exception as ex:  # noqa: BLE001
            log.info("ne_viewfill: %s: source %s unreadable: %s", kg, p.name, ex)
            continue
        cov = int(np.isin(gap, seen).sum())
        if cov:
            out.append((p, cov))
    return out


def process(kg: str, entries: dict | None = None, idx: dict | None = None, *, stage: bool = True,
            st: dict | None = None) -> dict:
    """Walk one KG (see module doc).  Writes ``staged/<kg>.json`` (+ ``.h3b``)
    unless *stage* is False (dry run).  Returns the summary row."""
    import ne_ingest
    import ne_h3
    import inbound_budget
    kg = str(kg).zfill(5)
    entries = entries if entries is not None else ne_ingest.load_manifest_entries()
    idx = idx if idx is not None else ne_ingest.load_index_ro()
    t0 = time.time()
    p = ne_ingest.path_for(kg)
    row = {"kg": kg, "ts": time.time(), "at": _now()}
    if not p.exists():
        row.update(skip="no file"); return row
    if not ne_ingest._coverage_complete(kg, entries):
        row.update(skip="incomplete"); return row
    prev = (st or {}).get("kgs", {}).get(kg) or {}
    view = ne_h3.fetch_declared_view(kg)
    if view is None:
        row.update(skip="no view"); return row
    inbound_budget.add(CHANNEL, len(view.raw) + 2048)
    vids = np.unique(_view_ids(view))
    row.update(view_n=int(vids.size), view_digest=view.header.get("digest"), view_source=view.header.get("source"),
               view_etag=view.header.get("_etag"))
    ids, seen, digest = _ids_observed(p)
    row.update(file_n=int(ids.size), file_observed=int(seen.size), base_digest=digest)
    # Nothing moved since the last walk (same view ETag, same file digest, no
    # error) → the previous result still stands; re-walking only re-staged a
    # duplicate repost every hour for the ~6 800 NE-server-listed parents.
    if (prev.get("ts") and time.time() - float(prev["ts"]) < RECHECK_S and not prev.get("error")
            and prev.get("view_etag") and prev.get("view_etag") == row.get("view_etag")
            and prev.get("base_digest") == digest):
        row.update(skip="unchanged", blank=prev.get("blank"), residual_n=prev.get("residual_n"),
                   residual_digest=prev.get("residual_digest"), planned_ts=prev.get("planned_ts"))
        return row
    gap = np.setdiff1d(vids, seen, assume_unique=True)
    row["blank"] = int(gap.size)                       # what NE server counts as blank + missing today
    if not gap.size:
        row.update(residual_n=0, dt=round(time.time() - t0, 1))
        return row
    # 1. corpus (other products) — borrowed at post time
    bids, _rows = ne_h3.corpus_obs_rows(gap, exclude_kg=kg)
    borrowable = np.intersect1d(gap, bids, assume_unique=True) if len(bids) else np.zeros(0, np.uint64)
    row["borrowable"] = int(borrowable.size)
    gap2 = np.setdiff1d(gap, borrowable, assume_unique=True)
    # 2. our own products → staged union merge
    staged_h3b = None
    merged_from = []
    if gap2.size:
        file_md5 = (idx["files"].get(kg) or {}).get("md5")
        try:
            srcs = _sources_for(kg, gap2, entries, idx, file_md5)
        except BudgetExhausted:
            raise
        if srcs:
            merged_from = [{"src": sp.name, "covers": c} for sp, c in srcs]
            if stage:
                STAGED_DIR.mkdir(parents=True, exist_ok=True)
                tmp = STAGED_DIR / f".{kg}.viewfill.h3b"
                hdr = ne_ingest.merge_blocks(kg, [p] + [sp for sp, _c in srcs], tmp,
                                             complete=ne_ingest._coverage_complete(kg, entries))
                rep = ne_h3.verify_h3b(tmp)
                if not rep.get("ok"):
                    tmp.unlink(missing_ok=True)
                    raise RuntimeError("staged merge failed verify: " + "; ".join(rep.get("errors") or [])[:200])
                staged_h3b = STAGED_DIR / f"{kg}.h3b"
                os.replace(tmp, staged_h3b)
                _ids2, seen2, digest2 = _ids_observed(staged_h3b)
                row.update(staged_digest=digest2, staged_observed=int(seen2.size), staged_n=int(_ids2.size))
                gap2 = np.setdiff1d(gap2, seen2, assume_unique=True)
            else:
                cov = np.zeros(0, np.uint64)
                for sp, _c in srcs:
                    cov = np.union1d(cov, np.intersect1d(gap2, _ids_observed(sp)[1], assume_unique=True))
                gap2 = np.setdiff1d(gap2, cov, assume_unique=True)
    row["merged_from"] = merged_from
    row["residual_n"] = int(gap2.size)
    # 3. the pipeline — sweep units over what nobody observed
    plan = bool(gap2.size >= MIN_RESIDUAL_FOR_SWEEP)
    if plan and prev.get("residual_digest") and prev.get("planned_ts"):
        rd = ne_h3.digest_of(gap2.tobytes())
        if rd == prev["residual_digest"] and time.time() - float(prev["planned_ts"]) < REPLAN_S:
            plan = False
            row["residual_held"] = True
    row["residual_digest"] = ne_h3.digest_of(gap2.tobytes()) if gap2.size else None
    if stage:
        STAGED_DIR.mkdir(parents=True, exist_ok=True)
        rec = {k: v for k, v in row.items()}
        rec["staged_h3b"] = staged_h3b.name if staged_h3b else None
        rec["plan_cells"] = [int(x) for x in gap2.tolist()] if plan else []
        rec["repost"] = bool(staged_h3b) or bool(borrowable.size) or (not plan and not gap2.size and prev.get("blank"))
        tmpj = STAGED_DIR / f".{kg}.json.tmp"
        tmpj.write_text(json.dumps(rec, default=str))
        os.replace(tmpj, STAGED_DIR / f"{kg}.json")
        if plan:
            row["planned_ts"] = time.time()
    row["dt"] = round(time.time() - t0, 1)
    log.info("ne_viewfill: %s view=%d file=%d/%d blank=%d borrowable=%d merged=%s residual=%d%s (%.1fs)",
             kg, vids.size, seen.size, ids.size, gap.size, borrowable.size,
             ",".join(f"{m['src']}:{m['covers']}" for m in merged_from) or "-", gap2.size,
             " → sweep" if plan else "", row["dt"])
    return row


# === SECTION: run ===

def run(budget_s: float = 3000, max_kgs: int | None = None, kgs: list[str] | None = None,
        all_kgs: bool = False, stage: bool = True) -> dict:
    import fcntl
    import ne_ingest
    VF_DIR.mkdir(parents=True, exist_ok=True)
    fh = open(LOCK_FILE, "w")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return {"skipped": "another run holds the lock"}
    t0 = time.time()
    st = load_state()
    entries = ne_ingest.load_manifest_entries()
    idx = ne_ingest.load_index_ro()
    if kgs:
        todo = [(str(k).zfill(5), 0, "cli") for k in kgs]
    else:
        todo = candidates(all_kgs=all_kgs, st=st)
    out = {"candidates": len(todo), "walked": 0, "staged": 0, "planned_kgs": 0, "planned_cells": 0,
           "blank": 0, "borrowable": 0, "residual": 0, "skipped": 0, "errors": 0, "budget_stop": False}
    try:
        for kg, g, why in todo:
            if time.time() - t0 > budget_s or (max_kgs and out["walked"] >= max_kgs):
                out["budget_stop"] = True
                break
            if (STAGED_DIR / f"{kg}.json").exists():
                out["skipped"] += 1
                continue       # staged, waiting for the ingest tick
            try:
                row = process(kg, entries, idx, stage=stage, st=st)
            except BudgetExhausted as ex:
                log.warning("ne_viewfill: %s", ex)
                out["budget_stop"] = True
                break
            except Exception as ex:  # noqa: BLE001
                log.warning("ne_viewfill: %s failed: %s", kg, ex)
                out["errors"] += 1
                st["kgs"][kg] = {"ts": time.time(), "at": _now(), "error": str(ex)[:200], "why": why}
                continue
            row["why"] = why
            row["est"] = g
            if row.get("skip"):
                out["skipped"] += 1
            else:
                out["walked"] += 1
                out["blank"] += int(row.get("blank") or 0)
                out["borrowable"] += int(row.get("borrowable") or 0)
                out["residual"] += int(row.get("residual_n") or 0)
                if row.get("staged_digest"):
                    out["staged"] += 1
                if row.get("planned_ts"):
                    out["planned_kgs"] += 1
                    out["planned_cells"] += int(row.get("residual_n") or 0)
            st["kgs"][kg] = {k: v for k, v in row.items() if k not in ("merged_from",)} | \
                            {"merged_from": ",".join(f"{m['src']}:{m['covers']}" for m in (row.get("merged_from") or []))}
            if out["walked"] % 20 == 0:
                save_state(st)
    finally:
        out["dt"] = round(time.time() - t0, 1)
        out["at"] = _now()
        st["runs"] = (st.get("runs") or [])[-20:] + [out]
        s = st["stats"]
        for k in ("walked", "staged", "planned_kgs", "planned_cells", "blank", "borrowable", "residual", "errors"):
            s[k] = int(s.get(k) or 0) + int(out[k])
        s["last_run"] = out
        save_state(st)
        fcntl.flock(fh, fcntl.LOCK_UN); fh.close()
    log.info("ne_viewfill run: %s", out)
    return out


def status() -> dict:
    st = load_state()
    staged = sorted(p.stem for p in STAGED_DIR.glob("?????.json")) if STAGED_DIR.exists() else []
    s = dict(st.get("stats") or {})
    s.pop("local_scan", None)
    ls = (st.get("stats") or {}).get("local_scan") or {}
    return {"stats": s, "staged_waiting": len(staged), "kgs_walked": len(st.get("kgs") or {}),
            "local_scan": {k: ls.get(k) for k in ("at", "kgs", "cells")},
            "last_run": (st.get("stats") or {}).get("last_run")}


def text_line() -> str:
    try:
        s = status()
        st, lr = s["stats"], s.get("last_run") or {}
        # walk ETA: parents with an h3b not yet walked (within RECHECK_S) ÷ KGs per hourly run
        total = sum(1 for _ in NE_DIR.glob("?????.h3b"))
        # parents never walked are simply not candidates (no gap known) — the
        # walk is "done" when the last run got through its candidate list
        left = max(0, int(lr.get("candidates") or 0) - int(lr.get("walked") or 0) - int(lr.get("skipped") or 0)) \
            if lr.get("budget_stop") else 0
        per_run = int(lr.get("walked") or 0)
        runs = -(-left // per_run) if (left and per_run) else 0
        eta = f"~{runs}h" if left else "done"
        return (f"viewfill: kgs={s.get('kgs_walked', 0)}/{total} left={left} eta={eta}"
                f" | walked={st.get('walked', 0)} blank={st.get('blank', 0)} borrowable={st.get('borrowable', 0)}"
                f" staged={st.get('staged', 0)} sweep={st.get('planned_cells', 0)}c/{st.get('planned_kgs', 0)}kg"
                f" residual={st.get('residual', 0)} errors={st.get('errors', 0)} waiting={s['staged_waiting']}"
                f" last_run={lr.get('at', '-')} ({lr.get('walked', 0)} walked, {lr.get('candidates', 0)} cand"
                f"{', budget stop' if lr.get('budget_stop') else ''})"
                f" scan={s['local_scan'].get('kgs')}kg/{s['local_scan'].get('cells')}c@{s['local_scan'].get('at', '-')}")
    except Exception as ex:  # noqa: BLE001
        return f"viewfill: status failed: {ex}"


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run", "plan", "status"])
    ap.add_argument("kgs", nargs="*")
    ap.add_argument("--budget-s", type=float, default=3000)
    ap.add_argument("--max", type=int, default=None)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()
    if a.quiet:
        logging.getLogger().setLevel(logging.WARNING)
    if a.cmd == "status":
        print(text_line()); print(json.dumps(status(), indent=1, default=str))
    elif a.cmd == "plan":
        for kg in a.kgs:
            print(json.dumps(process(kg, stage=False), indent=1, default=str))
    else:
        print(json.dumps(run(a.budget_s, a.max, a.kgs or None, a.all), indent=1, default=str))
