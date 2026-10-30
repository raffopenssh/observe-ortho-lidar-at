"""stale_blocks — retire old-split-layout residue that overlaps the live layout.

Problem (2026-10-30): ``kg_splitter`` re-splits a parent when its strike
count changes, so a block code processed under an *earlier* layout
(``23005-west``, ``84010-south``, ``31131-south`` …) keeps its committed
products while the parent is already fully covered by the current blocks.
Consequences:

* ``ne_ingest._live_blocks`` merges every block with a ``_json_v2`` into
  the parent ``.h3b`` → stale (sometimes label-mixed) cells win over the
  fresh ones (``label_mix: 23005@requeued→west``).
* ``kg_v2_store.codes_for_parent`` returns the stale block too → trees v3
  / grid25 / product segments double-count the overlap.
* a force-requeue of the stale code (label_mix, operator) can never run:
  the code is not in ``maybe_split_kg(parent)`` any more, so the peer's
  priority block skips it and it sits in ``retry_queue.json`` forever
  (``23005-west 15105-east 65309-south 31131-north 84010-center 05220-west``).

``scan()`` finds parents whose **current** layout covers the bbox on its
own while extra codes still carry live products.  ``apply()`` retires
those codes: Zenodo depositions deleted (manifest record kept as
``status=removed`` so the removal propagates through the timestamp merge,
pushed via ``/api/v1/manifest/push``), ``kg_v2_store`` row dropped, NE
block file + index row dropped and the parent marked dirty (journalled
in ``data/ne/stale_removals.json`` and consumed inside the ne_ingest tick
so we never race its in-memory index), retry-queue entry + ``_requeue``
tombstone dropped, label_mix entry settled, search-index row refreshed.

Parents whose current layout does NOT cover the bbox without the extra
codes (``drift`` — typically an unsplit parent product whose layout has
since split) are left alone: the old product is the valid coverage.

    python3 stale_blocks.py scan              # report only
    python3 stale_blocks.py apply [--limit N] [code ...]
    python3 stale_blocks.py retire [--dry-run] code ...   # orphan residue (no _json)
"""
from __future__ import annotations

import json
import logging
import re
import sys
import time
from pathlib import Path

log = logging.getLogger("stale_blocks")

DATA_DIR = Path("data/austria_processor")
MANIFEST = DATA_DIR / "zenodo_manifest.json"
NE_JOURNAL = Path("data/ne/stale_removals.json")
PRODUCT_SUFFIXES = ("json", "full_gpkg", "light_gpkg", "light_gpkg_v2", "json_v2", "ne_h3b")
_KEY_RE = re.compile(r"^(\d{5}(?:-[a-z][-a-z0-9]*)?)_(" + "|".join(PRODUCT_SUFFIXES) + r")$")
import config as _config
PRIMARY = _config.APP_URL


def _live(e) -> bool:
    return (isinstance(e, dict) and int(e.get("size") or 0) > 0
            and str(e.get("status") or "") != "removed"
            and "error" not in str(e.get("status") or ""))


def _entries() -> dict:
    raw = json.loads(MANIFEST.read_text()) if MANIFEST.exists() else {}
    return raw.get("entries", raw) or {}


def scan(entries: dict | None = None) -> dict:
    """→ ``{"stale": [{parent, codes, layout}], "drift": [...]}``."""
    import austria_processor as ap
    import kg_splitter
    import app as _app
    entries = entries if entries is not None else _entries()
    codes: dict[str, set] = {}
    for k, e in entries.items():
        m = _KEY_RE.match(k)
        if not m or not _live(e) or kg_splitter.is_sweep_code(m.group(1)):
            continue
        codes.setdefault(m.group(1), set()).add(m.group(2))
    kgs = {k["kg_code"]: k for k in ap.get_all_kgs(state_filter=None)}
    by_parent: dict[str, list] = {}
    for c in codes:
        by_parent.setdefault(c.split("-")[0], []).append(c)
    stale, drift = [], []
    for p, cs in sorted(by_parent.items()):
        if p not in kgs:
            continue
        try:
            layout = sorted(b["kg_code"] for b in kg_splitter.maybe_split_kg(kgs[p]))
        except Exception as ex:  # noqa: BLE001
            log.debug("layout %s: %s", p, ex)
            continue
        extra = sorted(c for c in cs if c not in layout)
        if not extra:
            continue

        def _cov(drop: set) -> bool:
            sans = {k: v for k, v in entries.items()
                    if not any(k.startswith(c + "_") for c in drop)}
            try:
                return bool(_app._kg_coverage_complete(p, sans)[0])
            except Exception:  # noqa: BLE001
                return False

        # Greedy: an extra code is stale when the parent stays covered
        # without it (older uploads tried first).  Evaluating all extras
        # at once missed e.g. 40008 — unsplit July v1 product + 4 finished
        # Oct blocks of a 4-block layout that has since drifted to 8
        # blocks: dropping everything uncovers the parent, dropping only
        # the unsplit product does not (2026-10-30).
        def _ts(c):
            return min((str((entries.get(f"{c}_{sfx}") or {}).get("uploaded_at") or "~")
                        for sfx in codes[c]), default="~")
        dropped: set = set()
        for c in sorted(extra, key=_ts):
            if _cov(dropped | {c}):
                dropped.add(c)
        kept = [c for c in extra if c not in dropped]
        if dropped:
            stale.append({"parent": p, "codes": sorted(dropped), "layout": layout,
                          "products": {c: sorted(codes[c]) for c in sorted(dropped)}})
        if kept:
            drift.append({"parent": p, "codes": kept, "layout": layout,
                          "products": {c: sorted(codes[c]) for c in kept}})
    return {"stale": stale, "drift": drift}


def _running_parents() -> set:
    try:
        import peer_director as pd
        cfg = pd.load_peers_config()
        run = pd._in_progress_kgs(cfg)
        return {str(c).split("-")[0] for c in run}
    except Exception as ex:  # noqa: BLE001
        log.warning("in-progress probe failed (%s) — refusing to apply", ex)
        return None


def _post(path: str, body: dict, admin: bool = True) -> dict:
    import requests
    import config
    hdr = {}
    if admin and config.operator_token():
        hdr["X-Operator-Token"] = config.operator_token()
    r = requests.post(PRIMARY + path, json=body, headers=hdr, timeout=30)
    try:
        return {"status": r.status_code, **(r.json() if r.content else {})}
    except Exception:  # noqa: BLE001
        return {"status": r.status_code, "text": r.text[:200]}


def _journal_ne(codes: list[str]) -> None:
    NE_JOURNAL.parent.mkdir(parents=True, exist_ok=True)
    try:
        cur = json.loads(NE_JOURNAL.read_text()) if NE_JOURNAL.exists() else []
    except Exception:  # noqa: BLE001
        cur = []
    cur = list(dict.fromkeys(list(cur) + list(codes)))
    tmp = NE_JOURNAL.with_suffix(".tmp")
    tmp.write_text(json.dumps(cur))
    tmp.replace(NE_JOURNAL)


def consume_ne_journal(idx: dict, entries: dict) -> list[str]:
    """Called inside ``ne_ingest.run_tick`` (holds the ingest lock, owns
    *idx*).  Drops the retired block files / rows, marks parents dirty; a
    parent whose on-disk file was produced by a merge that included the
    stale block and that has no live blocks left is re-pulled from its own
    ``<parent>_ne_h3b`` (row + file dropped → ``pending()`` picks it up)."""
    try:
        codes = json.loads(NE_JOURNAL.read_text()) if NE_JOURNAL.exists() else []
    except Exception:  # noqa: BLE001
        codes = []
    if not codes:
        return []
    import ne_ingest
    done = []
    for code in codes:
        parent = code.split("-")[0]
        if ne_ingest.is_block(code):
            try:
                ne_ingest.path_for(code).unlink(missing_ok=True)
            except Exception:  # noqa: BLE001
                pass
        # (a stale *parent-level* code keeps its file until the merge from
        # the live blocks overwrites it — no gap for the public cells API)
        idx["files"].pop(code, None)
        idx.get("failed", {}).pop(code, None)
        st = idx["parents"].setdefault(parent, {})
        st["dirty"] = True
        st["note"] = f"stale block {code} retired {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}"
        live = ne_ingest._live_blocks(parent, entries)
        frow = idx["files"].get(parent)
        if not live and frow and frow.get("merged_at") and _live(entries.get(f"{parent}_ne_h3b")):
            # unsplit layout: the merge overwrote the parent's own upload
            idx["files"].pop(parent, None)
            try:
                ne_ingest.path_for(parent).unlink(missing_ok=True)
            except Exception:  # noqa: BLE001
                pass
            st["dirty"] = False
            log.info("stale_blocks: %s re-pull scheduled (merged file superseded by own upload)", parent)
        done.append(code)
    try:
        NE_JOURNAL.unlink(missing_ok=True)
    except Exception:  # noqa: BLE001
        pass
    log.info("stale_blocks: NE journal consumed: %s", ",".join(done))
    return done


def reconcile_pushes(codes: list[str], manifest=None, rounds: int = 4) -> list[str]:
    """Re-push ``status=removed`` records for *codes* whose live manifest
    entry is still live (lost to the peer-sync RMW race).  The Zenodo files
    are already gone, so only the record is re-sent (fresh ``uploaded_at``
    so it wins the timestamp merge)."""
    from zenodo_client import Entry
    from datetime import datetime, timezone
    repushed = []
    for _ in range(rounds):
        time.sleep(2.0)
        live = _entries()
        todo = [k for c in codes for s in PRODUCT_SUFFIXES
                for k in (f"{c}_{s}",) if _live(live.get(k))]
        if not todo:
            break
        for key in todo:
            cur = live[key]
            now = datetime.now(timezone.utc).isoformat()
            rec = Entry.from_dict(dict(cur, size=0, status="removed", removed_at=now,
                                       uploaded_at=now, prev_uploaded_at=cur.get("prev_uploaded_at") or cur.get("uploaded_at")))
            if manifest is not None:
                manifest.set(key, rec)
            r = _post("/api/v1/manifest/push", {"key": key, "entry": rec.to_dict()}, admin=False)
            log.info("stale_blocks: re-push %s -> %s", key, r.get("status"))
            repushed.append(key)
    if manifest is not None:
        try:
            manifest.save()
        except Exception:  # noqa: BLE001
            pass
    return repushed


def _retire_one(code: str, entries: dict, manifest, client, queue: list, reason: str) -> list[str]:
    """Retire every live product of *code*: Zenodo deposition deleted,
    manifest record kept as ``status=removed`` + pushed, v2 store row,
    retry-queue entry, tombstones and NE block journalled.  Shared by
    ``apply`` (stale layouts) and ``retire`` (orphan residue)."""
    import kg_v2_store
    from zenodo_lock import zenodo_upload_lock
    keys = [f"{code}_{s}" for s in PRODUCT_SUFFIXES if _live(entries.get(f"{code}_{s}"))]
    with zenodo_upload_lock(purpose="stale_blocks", kg=code):
        for key in keys:
            rec = client.remove_deposition_keep_record(key, manifest, reason=reason)
            r = _post("/api/v1/manifest/push", {"key": key, "entry": rec.to_dict()}, admin=False)
            if r.get("status") not in (200, 409):
                raise RuntimeError(f"manifest push {key}: {r}")
    kg_v2_store.delete(code)
    if code in queue:
        queue[:] = [q for q in queue if q != code]
        (DATA_DIR / "retry_queue.json").write_text(json.dumps(queue))
    # Drop only the tombstones we superseded (a fresh ``removed`` record
    # outranks them) plus the ``_requeue`` marker.  A product key that is
    # NOT in the manifest any more (deleted earlier by a force requeue /
    # v2_regen, Zenodo file long gone) must keep its tombstone: dropping
    # it lets the peer-sync re-import the stale record from any peer that
    # still holds it — 48210-east/-west ``_json`` (May 2026, Zenodo 404)
    # came back that way on 2026-10-30 and dragged the parent's index
    # ``product_version`` to v1.
    _post("/api/v1/admin/drop_tombstones", {"keys": keys + [f"{code}_requeue"]})
    _journal_ne([code])
    return keys


def retire(codes: list[str], dry_run: bool = False) -> dict:
    """Retire *orphan residue*: codes that carry live products but no
    ``_json`` completion marker and are not part of a stale layout
    (``zen_stall`` ``orphan`` rows — e.g. ``32002-south-1`` full+light from
    a July run whose layout drifted, or an unsplit ``12114`` full+light
    (v1) superseded by its blocks).  Refuses codes with a live ``_json``
    (that is a complete product, use ``apply``) and codes whose parent is
    running on a peer.  After this the parent is queued by the operator
    (``POST /processing/queue skip_processed:false``) for a clean v2.4
    rerun of the uncovered blocks."""
    import shutil
    from zenodo_client import Client, Manifest
    import austria_processor as ap
    import kg_splitter
    entries = _entries()
    out = {"todo": [], "done": [], "errors": {}, "refused": {}}
    running = _running_parents()
    if running is None:
        return {"error": "peer probe failed"}
    for c in codes:
        parent = kg_splitter.parent_kg_code(c) if kg_splitter.is_block_code(c) else c
        live = [s for s in PRODUCT_SUFFIXES if _live(entries.get(f"{c}_{s}"))]
        if not live:
            out["refused"][c] = "no live products"
        elif "json" in live:
            out["refused"][c] = "has live _json (complete) — not residue"
        elif parent in running:
            out["refused"][c] = f"parent {parent} running on a peer"
        else:
            out["todo"].append(c)
    if dry_run or not out["todo"]:
        return out
    try:
        queue = json.loads((DATA_DIR / "retry_queue.json").read_text())
    except Exception:  # noqa: BLE001
        queue = []
    tmp_mf = Path("/tmp/stale_blocks_manifest.json")
    shutil.copy(MANIFEST, tmp_mf)
    manifest = Manifest(str(tmp_mf))
    client = Client(token=ap._zenodo_token())
    for code in out["todo"]:
        try:
            keys = _retire_one(code, entries, manifest, client, queue, reason="orphan residue")
            out["done"].append({"code": code, "removed": keys})
            log.info("stale_blocks: retired orphan %s (%s)", code, ",".join(keys))
        except Exception as ex:  # noqa: BLE001
            out["errors"][code] = str(ex)[:300]
            log.warning("stale_blocks: retire %s failed: %s", code, ex)
    out["repushed"] = reconcile_pushes([x["code"] for x in out["done"]], manifest)
    try:
        import app as _app
        for x in out["done"]:
            _app.director_event("info", f"stale_blocks: retired orphan {x['code']} ({len(x['removed'])} products) — "
                                "partial residue without _json, parent to be re-queued", x["code"])
    except Exception:  # noqa: BLE001
        pass
    return out


def apply(codes: list[str] | None = None, limit: int = 50, dry_run: bool = False) -> dict:
    import shutil
    import kg_v2_store
    from zenodo_client import Client, Manifest
    from zenodo_lock import zenodo_upload_lock
    import austria_processor as ap
    entries = _entries()
    rep = scan(entries)
    plan = {}
    for row in rep["stale"]:
        for c in row["codes"]:
            plan[c] = row["parent"]
    if codes:
        bad = [c for c in codes if c not in plan]
        if bad:
            return {"error": f"not stale (refusing): {bad}"}
        plan = {c: plan[c] for c in codes}
    running = _running_parents()
    if running is None:
        return {"error": "peer probe failed"}
    try:
        queue = json.loads((DATA_DIR / "retry_queue.json").read_text())
    except Exception:  # noqa: BLE001
        queue = []
    todo = [c for c in plan if plan[c] not in running][:limit]
    skipped = [c for c in plan if plan[c] in running]
    out = {"planned": len(plan), "todo": todo, "skipped_running": skipped, "done": [], "errors": {}}
    if dry_run or not todo:
        return out
    # Work on a private manifest copy; the live file is owned by srv and
    # updated through /manifest/push (timestamp-aware merge).
    tmp_mf = Path("/tmp/stale_blocks_manifest.json")
    shutil.copy(MANIFEST, tmp_mf)
    manifest = Manifest(str(tmp_mf))
    client = Client(token=ap._zenodo_token())
    touched_parents = set()
    for code in todo:
        parent = plan[code]
        try:
            keys = _retire_one(code, entries, manifest, client, queue, reason="stale split layout")
            touched_parents.add(parent)
            out["done"].append({"code": code, "removed": keys})
            log.info("stale_blocks: retired %s (%s)", code, ",".join(keys))
        except Exception as ex:  # noqa: BLE001
            out["errors"][code] = str(ex)[:300]
            log.warning("stale_blocks: %s failed: %s", code, ex)
    # The live manifest is also rewritten by the peer-sync merge (read-
    # modify-write, no lock across gunicorn workers), so a push can be lost
    # in that window.  Re-read and re-push whatever did not stick.
    out["repushed"] = reconcile_pushes([x["code"] for x in out["done"]], manifest)
    # label_mix: a requeue that only targeted retired blocks is settled
    try:
        import label_mix
        d = label_mix._load()
        chg = False
        for p in touched_parents:
            e = d["codes"].get(p)
            if e and e.get("state") == "requeued":
                tg = set(e.get("blocks") or [p])
                if tg <= {x["code"] for x in out["done"]}:
                    e["state"] = "done"
                    e["note"] = (e.get("note") or "") + f"; stale block(s) {','.join(sorted(tg))} retired (stale_blocks)"
                    chg = True
        if chg:
            label_mix._save(d)
    except Exception as ex:  # noqa: BLE001
        log.warning("stale_blocks: label_mix settle: %s", ex)
    try:
        import search_index as si
        ent2 = _entries()
        for p in touched_parents:
            si.get_index().update_kg(p, manifest=ent2)
    except Exception as ex:  # noqa: BLE001
        log.warning("stale_blocks: index refresh: %s", ex)
    try:
        import app as _app
        for x in out["done"]:
            _app.director_event("info", f"stale_blocks: retired {x['code']} ({len(x['removed'])} products) — "
                                "old split layout, parent covered by current blocks", x["code"])
    except Exception:  # noqa: BLE001
        pass
    return out


_STATUS = {"t": 0.0, "v": None, "busy": False}


def status(ttl: float = 900.0) -> dict:
    """``stale_blocks:`` line data for /process.txt.  The scan takes ~15 s
    (layout + coverage oracle for every drifted parent), so it runs in a
    background thread and the caller gets the last result (or
    ``computing`` on the first call)."""
    import threading
    now = time.time()
    if _STATUS["v"] is None or now - _STATUS["t"] >= ttl:
        if not _STATUS["busy"]:
            _STATUS["busy"] = True

            def _run():
                try:
                    rep = scan()
                    v = {"stale_parents": len(rep["stale"]),
                         "stale_codes": sum(len(r["codes"]) for r in rep["stale"]),
                         "drift_parents": len(rep["drift"]),
                         "sample": [f"{r['parent']}:" + ",".join(x.split('-', 1)[1] if '-' in x else x
                                                                  for x in r['codes'])
                                    for r in rep["stale"][:6]]}
                except Exception as ex:  # noqa: BLE001
                    v = {"error": str(ex)[:120]}
                _STATUS.update(t=time.time(), v=v, busy=False)

            threading.Thread(target=_run, name="stale_blocks_scan", daemon=True).start()
    return _STATUS["v"] or {"error": "computing"}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    for _n in ("app", "kg_splitter", "search_index", "zenodo_client"):
        logging.getLogger(_n).setLevel(logging.WARNING)
    args = sys.argv[1:]
    cmd = args[0] if args else "scan"
    if cmd == "scan":
        rep = scan()
        print(f"STALE parents={len(rep['stale'])} codes={sum(len(r['codes']) for r in rep['stale'])}")
        for r in rep["stale"]:
            print(f"  {r['parent']}: {' '.join(r['codes'])}   layout={'unsplit' if r['layout'] == [r['parent']] else len(r['layout'])}")
        print(f"DRIFT parents={len(rep['drift'])} (left alone)")
    elif cmd == "reconcile":
        # re-push removed records for codes whose Zenodo files are gone
        # but whose manifest entry reverted (peer-sync RMW race)
        codes = [a for a in args[1:] if re.match(r"^\d{5}(-[a-z][-a-z0-9]*)?$", a)]
        print(json.dumps(reconcile_pushes(codes), indent=1))
    elif cmd == "retire":
        dry = "--dry-run" in args
        codes = [a for a in args[1:] if re.match(r"^\d{5}(-[a-z][-a-z0-9]*)?$", a)]
        print(json.dumps(retire(codes, dry_run=dry), indent=1))
    elif cmd == "apply":
        limit = 50
        dry = "--dry-run" in args
        if "--limit" in args:
            limit = int(args[args.index("--limit") + 1])
        codes = [a for a in args[1:] if re.match(r"^\d{5}(-[a-z][-a-z0-9]*)?$", a)]
        print(json.dumps(apply(codes or None, limit=limit, dry_run=dry), indent=1))
    else:
        print(__doc__)
