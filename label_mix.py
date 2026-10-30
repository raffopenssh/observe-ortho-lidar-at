"""label_mix — detect + auto-requeue KGs whose products were stitched with
colliding label ids (restored tile checkpoints, fixed Oct 2026).

The bug: ``austria_processor`` renumbered the *objects* of a tile restored
from a checkpoint but left the tile's *label raster* with the ids of the
original run.  The stitchers resolve pixels through one global obj_map, so
pixels of tile A took the type / polygon of an unrelated object of tile B:
flat pasture rendered as ``roof`` (umfeld's glyph showed 27 ha of it on
Kohlschwarz 63330), polygons glued across the KG, NE cells with
``geb`` at ``h_max = 0``.  Fix: ``_remap_restored_tile_ids`` (restore path),
``v2_verify.segments_label_consistency`` (pre-upload gate, fatal) and the
v2.4 fast-path decline (label-mixed sources take the full path).

This module owns the *already uploaded* corpus:

* ``audit_h3b(path)`` — the NE cells carry the signature cheaply: cells
  dominated by ``geb`` with ``h_max ≤ 1 m`` and no structure, or by ``wald``
  with ``h_max ≤ 1 m`` and no tree.  Clean KGs measure ≈ 0 on both (p99 of
  3 804 files: 0.17 % / 0 %); affected ones 6–93 %.
* ``sweep()`` — director tick (incremental over new / changed ``data/ne/*.h3b``,
  cached per file mtime+size): detect → auto-requeue (plain force requeue,
  all products invalidated → the peer re-runs the whole tile loop with the
  fixed restore path) → settle ``done`` when a fresh ``_json`` postdates the
  requeue.  Bounded by ``MAX_REQUEUE_PER_TICK`` / ``MAX_OUTSTANDING`` so the
  retry queue is never flooded.
* ``summary()`` / ``status_line()`` — ``label_mix:`` line for ``/process.txt``
  and the chip in ``process.html`` (``/api/v1/director/status`` → ``label_mix``).

State: ``data/austria_processor/label_mix.json``
``{"files": {code: {"sig": "mtime:size", "geb": n, "geb_flat": n, "wald": n,
"wald_flat": n}}, "codes": {code: {"state": detected|requeued|done|fp|stuck, ...}}}``.
``fp`` = heuristic false positive: a full rerun passed the fatal
``segments_label_consistency`` gate and reproduced the same flat-cell counts
(deterministic low patch merged into a forest/roof object) — never rerun;
``python3 label_mix.py dismiss <code>…`` sets it by hand.

CLI::

    python3 label_mix.py            # status line + JSON summary
    python3 label_mix.py scan       # full rescan of data/ne (≈ 75 s), no requeue
    python3 label_mix.py sweep      # one director sweep now (scan + requeue + settle)
    python3 label_mix.py reset 63330  # requeued/done → detected (re-requeue next sweep)
    python3 label_mix.py dismiss 11151  # → fp (heuristic false positive, never rerun)
"""
from __future__ import annotations

import glob
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

log = logging.getLogger("label_mix")

DATA_DIR = Path("data/austria_processor")
NE_DIR = Path("data/ne")
STATE_FILE = DATA_DIR / "label_mix.json"
_LOCK = threading.Lock()

#: detection thresholds — fraction of dominated cells that are flat+empty
THRESH_FRAC = 0.02
#: … and at least this many such cells (tiny KGs / boundary noise)
MIN_CELLS = 20
MIN_CELLS_TOTAL = 500
MAX_REQUEUE_PER_TICK = 6
MAX_OUTSTANDING = 40
DONE_TTL_S = 14 * 86400
#: a rerun whose flat-cell counts land within this relative tolerance of
#: the pre-rerun counts is *deterministic* → the signal is a genuine flat
#: patch inside a merged forest/roof object (the product passed the fatal
#: v2_verify gate), not a restore-path id collision → ``fp``, never rerun.
FP_REL_TOL = 0.15


def _flat_counts(r: dict) -> tuple[int, int]:
    return int(r.get("geb_flat", 0) or 0), int(r.get("wald_flat", 0) or 0)


def _deterministic(prev: dict | None, cur: dict) -> bool:
    if not prev:
        return False
    for a, b in zip(_flat_counts(prev), _flat_counts(cur)):
        if abs(a - b) > FP_REL_TOL * max(a, b, 1):
            return False
    return True


def audit_h3b(path: str | Path) -> dict:
    """Per-KG signature counts from one ``.h3b`` (reads only the NEO1 rows)."""
    import ne_h3
    hdr = ne_h3.read_h3b_header(path)
    rows = [ne_h3.read_h3b_chunk(path, hdr, ch)["obs"] for ch in hdr["chunks"]]
    raw = b"".join(rows)
    rb = ne_h3.OBS_REC.size
    if not raw or len(raw) % rb:
        return {"n": 0, "geb": 0, "geb_flat": 0, "wald": 0, "wald_flat": 0}
    a = np.frombuffer(raw, dtype=np.uint8).reshape(-1, rb)
    lu = a[:, 8:17]
    bld_n, h_max, tree_n = a[:, 18], a[:, 21], a[:, 22]
    flat = (h_max != 255) & (h_max <= 2)            # 0.5 m units → ≤ 1 m
    geb = lu[:, 0] > 127                              # > 0.5 share
    wald = lu[:, 4] > 153                             # > 0.6 share
    return {"n": int(len(a)),
            "geb": int(geb.sum()), "geb_flat": int((geb & flat & (bld_n == 0)).sum()),
            "wald": int(wald.sum()), "wald_flat": int((wald & flat & (tree_n == 0)).sum())}


def is_affected(r: dict) -> tuple[bool, str]:
    if not r or r.get("n", 0) < MIN_CELLS_TOTAL:
        return False, ""
    why = []
    for g in ("geb", "wald"):
        tot, flat = r.get(g, 0), r.get(f"{g}_flat", 0)
        if tot and flat >= MIN_CELLS and flat / tot >= THRESH_FRAC:
            why.append(f"{g}_flat {flat}/{tot} ({100 * flat / tot:.0f}%)")
    return bool(why), ", ".join(why)


def _load() -> dict:
    try:
        d = json.loads(STATE_FILE.read_text())
        if isinstance(d, dict):
            d.setdefault("files", {}); d.setdefault("codes", {})
            return d
    except Exception:
        pass
    return {"files": {}, "codes": {}}


def _save(d: dict) -> None:
    d["t"] = time.time()
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(d, indent=1))
    tmp.replace(STATE_FILE)


def _sig(p: str) -> str:
    st = os.stat(p)
    return f"{int(st.st_mtime)}:{st.st_size}"


def scan(*, full: bool = False, budget_s: float | None = None) -> dict:
    """Audit new/changed h3b files (all when *full*).  Returns
    ``{"scanned": n, "new_detected": [codes]}``.  Never requeues."""
    t0 = time.time()
    with _LOCK:
        d = _load()
    files, codes = d["files"], d["codes"]
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    scanned, new_det, new_fp = 0, [], []
    present = set()
    for p in sorted(glob.glob(str(NE_DIR / "*.h3b"))):
        code = os.path.basename(p)[:-4]
        present.add(code)
        try:
            sig = _sig(p)
        except OSError:
            continue
        if not full and files.get(code, {}).get("sig") == sig:
            continue
        if budget_s is not None and time.time() - t0 > budget_s:
            break
        try:
            r = audit_h3b(p)
        except Exception as e:  # noqa: BLE001
            log.debug("label_mix audit %s: %s", code, e)
            continue
        scanned += 1
        r["sig"] = sig
        files[code] = r
        bad, why = is_affected(r)
        c = codes.get(code)
        if bad:
            if c is not None and c.get("state") == "done" and _deterministic(c.get("counts"), r):
                # the fresh full rerun passed v2_verify.segments_label_consistency
                # (fatal gate) and reproduced the same flat-cell counts → the
                # signal is deterministic, i.e. a real low patch merged into a
                # forest/roof object, not a colliding-id stitch.  Park as a
                # heuristic false positive; never rerun again.
                c.update(state="fp", why=why, fp_at=now_iso,
                         note=f"deterministic after rerun (prev {c.get('why')}); gate passed")
                new_fp.append(code)
            elif c is None or c.get("state") in ("done", None):
                # a *new* file that still shows the signature → (re)detect;
                # for a ``done`` entry that means the fresh run is bad too.
                codes[code] = {"state": "detected", "why": why, "detected_at": now_iso,
                               "attempts": int((c or {}).get("attempts", 0)),
                               "counts": {"geb_flat": r["geb_flat"], "wald_flat": r["wald_flat"]}}
                new_det.append(code)
            elif c.get("state") == "fp":
                c["why"] = why
            else:
                c["why"] = why
        elif c is not None and c.get("state") == "fp":
            codes.pop(code, None)      # a later rerun cleaned it after all
        elif c is not None and c.get("state") == "requeued":
            # fresh clean h3b landed → settled by sweep() against _json ts;
            # nothing to do here
            pass
    for code in [k for k in files if k not in present]:
        files.pop(code, None)
    with _LOCK:
        _save(d)
    return {"scanned": scanned, "new_detected": new_det, "new_fp": new_fp,
            "elapsed_s": round(time.time() - t0, 1)}


def _manifest_entries() -> dict:
    try:
        raw = json.loads((DATA_DIR / "zenodo_manifest.json").read_text())
        return raw.get("entries", raw) or {}
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# split parents (Oct 2026)
#
# ``data/ne/<parent>.h3b`` is the *merged* corpus file, so the audit fires on
# the parent code — but a split parent (``61047`` → ``61047-east`` …) has no
# ``61047_json``; its products are block-keyed.  A force requeue of the
# *parent* is a no-op there: ``processing_queue_add`` preserves complete
# sibling blocks (Aug 2026 guard), the coverage oracle still reads complete,
# and the dashboard GET prune drops the code from ``retry_queue.json`` within
# seconds.  21 of 23 "requeued" entries sat like that for 3.5 days.  So:
# audit every block's own ``_ne_h3b`` (1–3 MB each, private bucket) and
# requeue only the affected *block* codes — deleting a block's ``_json``
# punches a hole in the parent bbox, which is what makes the requeue stick.
# ---------------------------------------------------------------------------

H3B_TMP = Path("/tmp/label_mix_h3b")
MAX_BLOCK_FETCH_PER_TICK = 12
#: a ``requeued`` entry whose codes left the queue without their ``_json``
#: ever being invalidated was reverted by the prune → back to ``detected``
LOST_AFTER_S = 2 * 3600


def _live(v) -> bool:
    return isinstance(v, dict) and "error" not in str(v.get("status") or "") \
        and str(v.get("status") or "") != "removed"


def _blocks_of(code: str, ent: dict) -> list[str]:
    """Committed block codes of a split parent (``<code>-<dir>[-n]_json``);
    ``[]`` when the parent is unsplit (has its own ``_json``) or unknown."""
    pre = code + "-"
    # NB: a parent may carry its own ``_json`` *and* block ``_json``s (old-split
    # residue, the zen_stall ``orphan`` class — 14201/14235 on 2026-10-23).
    # Blocks win: they are what a requeue can actually invalidate; a
    # parent-level POST there only deletes the stale parent pointer and
    # leaves the code stuck in the queue for a pointless full rerun.
    return sorted({k[:-5] for k, v in ent.items()
                   if k.startswith(pre) and k.endswith("_json") and _live(v)})


def _fetch_block_h3b(block: str, ent: dict) -> Path | None:
    e = ent.get(f"{block}_ne_h3b")
    if not _live(e) or not e.get("bucket_url") or not e.get("filename"):
        return None
    H3B_TMP.mkdir(parents=True, exist_ok=True)
    dest = H3B_TMP / f"{block}_ne.h3b"
    try:
        if dest.exists() and dest.stat().st_size == int(e.get("size") or -1):
            return dest
    except OSError:
        pass
    try:
        import requests
        import zenodo_client
        url = f"{e['bucket_url'].rstrip('/')}/{e['filename']}"
        r = requests.get(url, headers={"Authorization": f"Bearer {zenodo_client.DEFAULT_TOKEN}"},
                         stream=True, timeout=(10, 120))
        r.raise_for_status()
        tmp = dest.with_suffix(".part")
        with open(tmp, "wb") as fh:
            for chunk in r.iter_content(1 << 18):
                fh.write(chunk)
        tmp.replace(dest)
        return dest
    except Exception as ex:  # noqa: BLE001
        log.info("label_mix: block h3b fetch %s failed: %s", block, ex)
        return None


def _audit_blocks(code: str, e: dict, blocks: list[str], ent: dict, budget: list) -> tuple[list, list]:
    """Audit each block's own h3b (cached per block ``uploaded_at``).
    Returns ``(affected_blocks, not_yet_auditable_blocks)``; ``budget`` is a
    one-element mutable fetch counter shared across the sweep."""
    cache = e.setdefault("block_audit", {})
    affected, pending = [], []
    e["no_h3b"] = []
    for b in blocks:
        h = ent.get(f"{b}_ne_h3b")
        stamp = str((h or {}).get("uploaded_at") or "") if _live(h) else ""
        c = cache.get(b)
        if c and c.get("stamp") == stamp and stamp:
            (affected if c.get("bad") else []).append(b)
            continue
        if not stamp:
            pending.append(b)          # pre-2.4 block, no NE cells yet
            e["no_h3b"].append(b)
            continue
        if budget[0] >= MAX_BLOCK_FETCH_PER_TICK:
            pending.append(b)
            continue
        budget[0] += 1
        p = _fetch_block_h3b(b, ent)
        if p is None:
            pending.append(b)
            continue
        try:
            r = audit_h3b(p)
        except Exception as ex:  # noqa: BLE001
            log.info("label_mix: block audit %s: %s", b, ex)
            pending.append(b)
            continue
        bad, why = is_affected(r)
        cache[b] = {"stamp": stamp, "bad": bad, "why": why, "n": r.get("n", 0)}
        if bad:
            affected.append(b)
        try:
            p.unlink()
        except OSError:
            pass
    return affected, pending


def _fresh_json(target: str, ent: dict, since: str) -> bool:
    j = ent.get(f"{target}_json")
    return _live(j) and str(j.get("uploaded_at") or "") > str(since or "")


def _requeue_lost(targets: list[str], requeued_at: str, ent: dict, queue: set, now_ts: float) -> bool:
    """``requeued`` but every target left the queue while its ``_json`` is
    still the *pre*-requeue one → the force requeue never invalidated
    anything (the GET prune reverted it).  Judged after ``LOST_AFTER_S``."""
    try:
        age = now_ts - datetime.fromisoformat(requeued_at).timestamp()
    except Exception:
        return False
    if age < LOST_AFTER_S:
        return False
    for t in targets:
        if t in queue:
            return False
        j = ent.get(f"{t}_json")
        if not _live(j):
            sub = _blocks_of(t, ent)
            if not sub:
                return False             # invalidated → run genuinely pending / in flight
            # parent-level requeue of a split parent: judge on its blocks
            if not _requeue_lost(sub, requeued_at, ent, queue, now_ts):
                return False
            continue
        if str(j.get("uploaded_at") or "") > str(requeued_at or ""):
            return False
    return True


def _retry_queue() -> list:
    try:
        q = json.loads((DATA_DIR / "retry_queue.json").read_text())
        return q if isinstance(q, list) else []
    except Exception:
        return []


LOG_RING = Path("data/combined_log_24h.jsonl")
_DECLINE_TOKEN = "label-mixed"


def _harvest_declines(d: dict) -> list:
    """Peers that decline the v2.4 fast path on a label-mixed source
    (``v2up: <code> fast declined → full path next: label-mixed …``) have
    no clean h3b for us to audit, and the full-GPKG upgrade they fall back
    to leaves the full GPKG's own ``segments`` / ``segment_type`` corrupt.
    Pick those codes out of the merged 24 h log ring (no new traffic) and
    mark them detected → full rerun."""
    if not LOG_RING.exists():
        return []
    new = []
    try:
        pos = int(d.get("log_pos") or 0)
        size = LOG_RING.stat().st_size
        if pos > size:
            pos = 0                       # ring was pruned/rewritten
        with open(LOG_RING, "rb") as fh:
            fh.seek(pos)
            for raw in fh:
                if _DECLINE_TOKEN.encode() not in raw or b"fast declined" not in raw:
                    continue
                try:
                    e = json.loads(raw)
                except Exception:
                    continue
                code = str(e.get("kg") or "").strip()
                if not code:
                    continue
                c = d["codes"].get(code)
                if c is not None and c.get("state") == "done" \
                        and str(e.get("ts") or "") <= str(c.get("done_at") or ""):
                    continue              # stale line re-read after a ring rewrite
                if c is None or c.get("state") == "done":
                    d["codes"][code] = {"state": "detected", "why": "fast-path decline: label-mixed light GPKG",
                                        "detected_at": str(e.get("ts") or ""), "attempts": 0,
                                        "source": "fast_decline"}
                    new.append(code)
            d["log_pos"] = fh.tell()
    except Exception as e:  # noqa: BLE001
        log.debug("label_mix harvest: %s", e)
    return new


def sweep(*, requeue: bool = True, scan_budget_s: float = 20.0) -> dict:
    """One director sweep: incremental scan, settle, auto-requeue."""
    res = scan(budget_s=scan_budget_s)
    with _LOCK:
        d = _load()
    codes = d["codes"]
    res["harvested"] = _harvest_declines(d)
    ent = _manifest_entries()
    queue = set(_retry_queue())
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    events = [(f"label_mix: {code} signature unchanged after a full rerun that passed the label-consistency "
               f"gate — deterministic low patch inside a merged object, not an id collision (fp, no further rerun)",
               code, "info") for code in res.get("new_fp") or []]
    # --- settle requeued → done; expire old done ---
    now_ts = time.time()
    for code, e in list(codes.items()):
        st = e.get("state")
        if st == "requeued":
            targets = e.get("blocks") or [code]
            # a parent-level requeue that nevertheless ran (blocks were
            # incomplete anyway) settles on the blocks' _json
            fresh_t = targets if e.get("blocks") else (_blocks_of(code, ent) or [code])
            if all(_fresh_json(t, ent, e.get("requeued_at")) for t in fresh_t):
                e["state"] = "done"; e["done_at"] = now_iso
                events.append((f"label_mix: {code} fresh products committed — label-mix repair complete", code, "info"))
                continue
            # prune-reverted requeue (parent-level POST on a split parent, or
            # the tombstone trap): nothing invalidated, nothing queued → retry
            # with block resolution, without burning an attempt.
            if _requeue_lost(targets, e.get("requeued_at") or "", ent, queue, now_ts):
                e["state"] = "detected"
                e["attempts"] = max(0, int(e.get("attempts", 0)) - 1)
                e["note"] = f"requeue of {','.join(targets)} reverted by the queue prune at {now_iso}; re-resolving"
                e.pop("blocks", None)
                events.append((f"label_mix: {code} requeue was pruned (products never invalidated) — "
                               f"re-resolving as blocks", code, "warning"))
        elif st in ("done", "fp"):
            try:
                if time.time() - datetime.fromisoformat(e.get("done_at") or e["fp_at"]).timestamp() > DONE_TTL_S:
                    codes.pop(code, None)
            except Exception:
                pass
    # --- requeue detected ---
    outstanding = sum(1 for e in codes.values() if e.get("state") == "requeued")
    n_rq = 0
    fetch_budget = [0]
    if requeue:
        for code, e in sorted(codes.items()):
            if e.get("state") != "detected":
                continue
            if n_rq >= MAX_REQUEUE_PER_TICK or outstanding >= MAX_OUTSTANDING:
                break
            if int(e.get("attempts", 0)) >= 2:
                e["state"] = "stuck"
                events.append((f"label_mix: {code} still label-mixed after 2 reruns — parked (stuck)", code, "warning"))
                continue
            # split parent → resolve to the affected block(s)
            blocks = _blocks_of(code, ent)
            targets = [code]
            if blocks:
                affected, pending = _audit_blocks(code, e, blocks, ent, fetch_budget)
                if affected:
                    targets = affected
                    e["blocks"] = affected
                    e["note"] = (f"split parent: {len(affected)}/{len(blocks)} block(s) label-mixed"
                                 + (f", {len(pending)} not auditable yet" if pending else ""))
                elif pending:
                    no_h3b = e.get("no_h3b") or []
                    if len(no_h3b) == len(pending):
                        e["note"] = (f"split parent: {len(pending)}/{len(blocks)} block(s) have no NE cells yet "
                                     f"(pre-2.4) — waiting for the v2.4 upgrade / fast-path decline")
                    else:
                        e["note"] = (f"split parent: block audit in progress ({len(pending)}/{len(blocks)} "
                                     f"left, {len(no_h3b)} without NE cells)")
                    continue
                else:
                    # every block audits clean on its own h3b → the parent
                    # signature is a merge artefact, not an id collision
                    e.update(state="fp", fp_at=now_iso,
                             note=f"split parent: all {len(blocks)} blocks audit clean (merge artefact)")
                    if code in queue and _cancel_requeue(code):
                        e["note"] += "; pending parent-level requeue cancelled"
                    events.append((f"label_mix: {code} split parent — every block's own NE cells are clean; "
                                   f"parent signature is a merge artefact (fp)", code, "info"))
                    continue
            to_post = [t for t in targets if t not in queue]
            ok = all(_post_requeue(t) for t in to_post)
            if ok:
                e["state"] = "requeued"; e["requeued_at"] = now_iso
                e["attempts"] = int(e.get("attempts", 0)) + 1
                if not to_post:
                    e["note"] = "already queued"
                n_rq += 1; outstanding += 1
                what = f"block(s) {','.join(targets)}" if blocks else "products"
                events.append((f"label_mix: {code} {what} stitched with colliding label ids ({e.get('why')}) "
                               f"— requeued for a full rerun (attempt {e['attempts']})", code, "warning"))
            else:
                e["note"] = f"requeue POST failed {now_iso}"
    with _LOCK:
        _save(d)
    for msg, code, level in events:
        try:
            import app as _app
            _app.director_event(msg, peer="director", kg=code, level=level)
        except Exception:
            log.info(msg)
    res.update({"requeued": n_rq, "events": len(events)})
    return res


def _post_requeue(code: str) -> bool:
    try:
        import requests
        import config
        headers = config.operator_headers()
        r = requests.post(config.director_url() + "/api/v1/processing/queue",
                          json={"kgs": [code], "position": 0, "skip_processed": False},
                          timeout=30, headers=headers)
        return r.ok
    except Exception as ex:  # noqa: BLE001
        log.warning("label_mix %s: requeue POST failed: %s", code, ex)
        return False


def _cancel_requeue(code: str) -> bool:
    """Undo an earlier parent-level force requeue that turned out to be a
    merge artefact: drop the ``<code>_requeue`` tombstone (else the prune
    keeps the code) and remove it from ``retry_queue.json``."""
    try:
        import requests
        import config
        headers = config.operator_headers()
        requests.post(config.director_url() + "/api/v1/admin/drop_tombstones",
                      json={"keys": [f"{code}_requeue"]}, timeout=30, headers=headers)
        r = requests.delete(config.director_url() + "/api/v1/processing/queue",
                            params={"kg": code}, timeout=30, headers=headers)
        return r.status_code in (200, 404)
    except Exception as ex:  # noqa: BLE001
        log.warning("label_mix %s: cancel requeue failed: %s", code, ex)
        return False


def summary() -> dict:
    d = _load()
    codes = d.get("codes") or {}
    by = {}
    for e in codes.values():
        by[e.get("state", "?")] = by.get(e.get("state", "?"), 0) + 1
    rows = sorted(codes.items(), key=lambda ce: (ce[1].get("state") != "requeued", ce[1].get("detected_at", "")))
    return {"counts": by, "total": len(codes), "files": len(d.get("files") or {}),
            "t": d.get("t"),
            "rows": [{"code": c, "state": e.get("state"), "why": e.get("why"),
                      "attempts": e.get("attempts", 0), "requeued_at": e.get("requeued_at"),
                      "blocks": e.get("blocks") or []}
                     for c, e in rows if e.get("state") not in ("done", "fp")][:40]}


def status_line(max_rows: int = 12) -> str | None:
    s = summary()
    if not s["total"]:
        return None
    cnt = s["counts"]
    line = ("label_mix: " + " ".join(f"{k}={cnt.get(k, 0)}" for k in ("detected", "requeued", "done", "fp", "stuck") if cnt.get(k))
            + f" of {s['files']} h3b scanned (restored-checkpoint label-id collision — full rerun each; "
              "gate: v2_verify.segments_label_consistency)")
    rows = s["rows"]
    if rows:
        line += " · " + " ".join(f"{r['code']}@{r['state']}" + (f"[a{r['attempts']}]" if r.get("attempts") else "")
                                 + (f"→{','.join(b.split('-', 1)[1] for b in r['blocks'])}" if r.get("blocks") else "")
                                 for r in rows[:max_rows])
        if len(rows) > max_rows:
            line += f" … +{len(rows) - max_rows}"
    line += " · debug: ?q=label_mix"
    return line


def dismiss(codes: list, note: str = "operator: deterministic / gate passed") -> int:
    """Mark codes as heuristic false positives (``fp``) — never rerun."""
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with _LOCK:
        d = _load()
        n = 0
        for c in codes:
            e = d["codes"].setdefault(c, {})
            e.update(state="fp", fp_at=now_iso, note=note); n += 1
        _save(d)
    return n


def reset(codes: list) -> int:
    with _LOCK:
        d = _load()
        n = 0
        for c in codes:
            e = d["codes"].get(c)
            if e:
                e["state"] = "detected"; e["attempts"] = 0; n += 1
                e.pop("blocks", None); e.pop("note", None)
        _save(d)
    return n


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    if cmd == "scan":
        print(json.dumps(scan(full=True), indent=1))
    elif cmd == "sweep":
        print(json.dumps(sweep(requeue="--no-requeue" not in sys.argv), indent=1))
    elif cmd == "reset":
        print("reset", reset(sys.argv[2:]))
    elif cmd == "dismiss":
        print("dismiss", dismiss(sys.argv[2:]))
    print(status_line() or "label_mix: nothing detected")
    if cmd == "status":
        print(json.dumps(summary(), indent=1))
