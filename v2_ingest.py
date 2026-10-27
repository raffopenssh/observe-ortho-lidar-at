"""v2_ingest — primary-side ingest of fleet ``_json_v2`` products.

Runs as a daemon thread inside ``app.py`` (primary only, every
``TICK_S``).  Per tick:

1. ``pending()`` — manifest ``<code>_json_v2`` entries (committed: size>0,
   uploaded_at) whose code is NOT in ``kg_v2_store`` and has < ``MAX_FAILS``
   failed ingest attempts (``meta.ingest_failed``).
2. For up to ``max_n`` codes: download blob → ``v2_verify.verify_for_ingest``
   (v1 doc from ``kg_docs`` = file or older store copy; flat index row as
   fallback) → ``kg_v2_store.put`` FIRST (the search-index selector reads
   the store) → ``search_index.update_kg`` → delete ``json/<code>.json`` +
   ``add_freed`` → ``quality_flags.scan_doc``.
3. ``kg_log_harvest.harvest_live()`` + ``harvest_archive_step(1)``.

Failures are recorded in ``meta`` key ``ingest_failed`` as
``{code: {"n": int, "reason": str, "ts": str}}``; a *newer* upload of the
same code (different ``uploaded_at``) resets the strike.

Test against a scratch store: ``kg_v2_store.STORE_PATH = Path('/tmp/x.db')``
and call ``ingest_one(code, entries, delete_v1=False, update_index=False)``.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path

import kg_docs
from kg_splitter import is_sweep_code as _is_sweep
import kg_v2_store as store
import v2_verify

log = logging.getLogger(__name__)

MANIFEST_PATH = Path("data/austria_processor/zenodo_manifest.json")
TICK_S = 300
MAX_PER_TICK = 20
MAX_FAILS = 3
DOWNLOAD_TIMEOUT = 120

_lock = threading.Lock()
_last_tick: dict = {}


def load_manifest_entries(path: Path = MANIFEST_PATH) -> dict:
    """Shared, read-only view (mtime-cached via :mod:`json_cache`)."""
    try:
        import json_cache
        md = json_cache.load(path)
    except Exception:  # noqa: BLE001
        return {}
    if not isinstance(md, dict):
        return {}
    return md.get("entries", md) or {}


def _vkey(v) -> tuple:
    try:
        return tuple(int(x) for x in str(v).lstrip("v").split("."))
    except Exception:  # noqa: BLE001
        return (0,)


def _committed(e) -> bool:
    return isinstance(e, dict) and int(e.get("size") or 0) > 0 and bool(e.get("uploaded_at"))


def failed() -> dict:
    try:
        return json.loads(store.meta_get("ingest_failed", "{}") or "{}")
    except Exception:  # noqa: BLE001
        return {}


def _record_failure(code: str, reason: str, uploaded_at: str) -> None:
    f = failed()
    cur = f.get(code) or {}
    same = cur.get("uploaded_at") == uploaded_at
    n = int(cur.get("n", 0)) + 1 if same else 1
    f[code] = {"n": n, "reason": reason[:300], "uploaded_at": uploaded_at,
               "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    if same:  # keep the gone-candidate trail (sweep_gone) across retries
        for k in ("gone", "gone_depo", "prev"):
            if k in cur:
                f[code][k] = cur[k]
    elif cur.get("uploaded_at"):
        # remember the previous upload's verdict so a re-run that
        # reproduces it bit-identically is recognised (_deterministic_repeat)
        f[code]["prev"] = {"reason": cur.get("reason"), "uploaded_at": cur.get("uploaded_at")}
    store.meta_set("ingest_failed", json.dumps(f, separators=(",", ":")))


# Improvement-vs-v1 gates whose fatal verdict is waived when a *different*
# upload of the same code reproduced the identical fatal set + figures
# (deterministic → not the transient upstream hole the gate guards).
_DETERMINISTIC_WAIVABLE = ("_ge_v1", "_vs_v1", "_le_v1")


def _deterministic_repeat(code: str, rep, doc: dict, uploaded_at: str) -> bool:
    """True when the recorded strike for *code* came from an EARLIER upload
    (different ``uploaded_at``) with the exact same fatal errors, all of
    them v1-comparison gates, and the doc reports no upstream-failed
    tiles. The identical figures across two independent runs prove the
    gap is in the source data, not a lost read."""
    errs = list(rep.get("errors") or [])
    if not errs or not all(e.split(":", 1)[0].endswith(_DETERMINISTIC_WAIVABLE) for e in errs):
        return False
    try:
        if int((doc.get("data_quality") or {}).get("n_upstream_failed_tiles") or 0) > 0:
            return False
    except Exception:  # noqa: BLE001
        return False
    prev = failed().get(code) or {}
    if prev.get("uploaded_at") == uploaded_at:      # retry of this upload → look one back
        prev = prev.get("prev") or {}
    if not prev or not prev.get("uploaded_at") or prev.get("uploaded_at") == uploaded_at:
        return False
    reason = str(prev.get("reason") or "")
    if " | " not in reason:
        return False
    fatal = reason.split(" | ", 1)[1].split(" | warn:", 1)[0].split(" | hint:", 1)[0]
    return fatal == "; ".join(errs[:4])


# Oct 2026, 80110-southeast-2 (Sölden, South-Tyrol border): BEV initially
# served a cross-border strip (tile 5 = 2.26 M valid px in the v1 baseline)
# and has since clipped its ALS rasters cleanly to the Austrian territory
# (sovereignty).  Every fresh run now reads tile 5 as all-NoData, flags it
# ``outside_austria`` / ``no_valid_pixels`` and strikes out on
# ``lidar_tiles_ge_v1`` / ``segmented_area_ge_v1`` forever.  The gate
# exists to catch *transient* upstream holes; a tile the pipeline itself
# read as empty (not an ``upstream_fail``) is deterministic and can be
# confirmed from here with one windowed BEV read per tile (NoData COG
# blocks → a few KB).  Same family as 66319 (deterministic repeat).
BEV_EMPTY_MIN_VALID_PX = 100     # same threshold as the processor's tile skip


def _bev_empty_confirmed(code: str, rep, doc: dict, v1_doc: dict | None) -> str | None:
    """Return a short evidence string when every fatal error is a v1-comparison
    gate and every DTM tile v1 had but v2 lacks is (a) flagged empty by the
    pipeline (``outside_austria``/``no_valid_pixels``, not ``upstream_fail``)
    and (b) all-NoData on BEV right now (default dataset, direct read).
    None otherwise.  Result is remembered in the strike record so BEV is
    probed at most once per upload."""
    errs = list(rep.get("errors") or [])
    if not errs or not all(e.split(":", 1)[0].endswith(_DETERMINISTIC_WAIVABLE) for e in errs):
        return None
    gates = {e.split(":", 1)[0] for e in errs}
    if not gates & {"lidar_tiles_ge_v1", "segmented_area_ge_v1"}:
        return None
    dq = doc.get("data_quality") or {}
    try:
        if int(dq.get("n_upstream_failed_tiles") or 0) > 0:
            return None
    except Exception:  # noqa: BLE001
        return None
    t2 = {int(t.get("tile_index") or 0): t for t in (dq.get("tiles") or []) if isinstance(t, dict)}
    t1 = {int(t.get("tile_index") or 0): t
          for t in (((v1_doc or {}).get("data_quality") or {}).get("tiles") or []) if isinstance(t, dict)}
    if not t2 or not t1 or len(t1) != len(t2):
        return None
    missing = []
    for i, t in t2.items():
        o = t1.get(i)
        if o is None:
            return None
        if (o.get("bbox_wgs") or None) != (t.get("bbox_wgs") or None):
            return None                    # different grid → not comparable
        if o.get("dtm") and not t.get("dtm"):
            if t.get("upstream_fail") or not (t.get("outside_austria") or t.get("no_valid_pixels")):
                return None
            missing.append(t)
    if not missing:
        return None
    rec = failed().get(code) or {}
    cached = rec.get("bev_probe")
    if isinstance(cached, dict) and cached.get("uploaded_at") == rec.get("uploaded_at"):
        return cached.get("evidence") if cached.get("ok") else None
    evidence = []
    ok = True
    try:
        import raster_io
        import tile_index as ti
        from pyproj import Transformer
        from shapely.geometry import box
        tr = Transformer.from_crs(4326, 3035, always_xy=True)
        for t in missing:
            w, s, e, n = [float(x) for x in t["bbox_wgs"]]
            x0, y0 = tr.transform(w, s)
            x1, y1 = tr.transform(e, n)
            try:
                d = raster_io.read_dtm_dsm(box(x0, y0, x1, y1), ti.DEFAULT_DATASET)
                valid = int(d["mask"].sum())
            except ValueError as ex:          # 'No tiles cover bbox' → outside coverage
                if "No tiles cover" not in str(ex):
                    raise
                valid = 0
            evidence.append(f"tile {t.get('tile_index')}: {valid} valid px")
            if valid >= BEV_EMPTY_MIN_VALID_PX:
                ok = False
    except Exception as ex:  # noqa: BLE001  — BEV outage: don't decide, retry next tick
        log.warning("v2_ingest: %s BEV empty-tile probe failed: %s", code, ex)
        return None
    ev = "BEV " + ti.DEFAULT_DATASET + ": " + "; ".join(evidence)
    f = failed()
    if code in f:
        f[code]["bev_probe"] = {"uploaded_at": rec.get("uploaded_at"), "ok": ok, "evidence": ev}
        store.meta_set("ingest_failed", json.dumps(f, separators=(",", ":")))
    return ev if ok else None


def _clear_failure(code: str) -> None:
    f = failed()
    if code in f:
        f.pop(code)
        store.meta_set("ingest_failed", json.dumps(f, separators=(",", ":")))


def pending(entries: dict | None = None) -> list[str]:
    """Codes with a committed ``_json_v2`` not yet in the store (oldest
    upload first), excluding codes with >= MAX_FAILS strikes on the same
    upload."""
    entries = entries if entries is not None else load_manifest_entries()
    have = store.all_codes()
    fl = failed()
    out = []
    for k, e in entries.items():
        if not k.endswith("_json_v2") or not _committed(e):
            continue
        code = k[:-len("_json_v2")]
        if code in have:
            # re-ingest only if the manifest upload is newer than the stored one
            m = store.get_meta(code) or {}
            if (e.get("uploaded_at") or "") <= (m.get("uploaded_at") or ""):
                continue
        st = fl.get(code)
        if st and st.get("uploaded_at") == e.get("uploaded_at") and int(st.get("n", 0)) >= MAX_FAILS:
            continue
        out.append((e.get("uploaded_at") or "", code))
    out.sort()
    return [c for _, c in out]


def _link(entry: dict) -> tuple[str, dict]:
    from zenodo_client import DEFAULT_TOKEN
    hdr = {}
    link = entry.get("link", "") or ""
    if not link and entry.get("bucket_url") and entry.get("filename"):
        link = f"{entry['bucket_url']}/{entry['filename']}"
        hdr = {"Authorization": f"Bearer {DEFAULT_TOKEN}"}
    if not link and entry.get("depo_id") and entry.get("filename"):
        link = (f"https://zenodo.org/api/records/{entry['depo_id']}/draft/files/"
                f"{entry['filename']}/content?access_token={DEFAULT_TOKEN}")
    return link, hdr


MB_PER_H_DEFAULT = 25.0      # primary inbound budget for json_v2 pulls (metered VM; knob v2_ingest_mb_per_h)
CHANNEL = "v2json"


def _peer_pull(entry: dict) -> bytes | None:
    """v2.4: try the uploading peer's local copy first (``product_origin`` →
    ``GET <peer>/api/v1/processing/product?key=<code>_json_v2``) so the
    shared Zenodo token is not spent on ingest.  None → use Zenodo."""
    try:
        import requests
        import product_origin as _po
        import peer_director as _pd
        key = entry.get("key") or ""
        if not key:
            return None
        peer = _po.peer_for(key, entry.get("uploaded_at"))
        if not peer:
            return None
        p = _pd.get_peer_by_id(_pd.load_peers_config(), peer)
        url = (p or {}).get("url")
        if not url:
            return None
        hdr = {}
        try:
            tok = Path("data/admin_token").read_text().strip()
            if tok:
                hdr["X-Admin-Token"] = tok
        except Exception:
            pass
        r = requests.get(f"{url.rstrip('/')}/api/v1/processing/product", params={"key": key},
                         headers=hdr, timeout=30)
        if r.status_code != 200:
            return None
        blob = r.content
        want = int(entry.get("size") or 0)
        if want and len(blob) != want:
            return None
        ck = entry.get("checksum") or ""
        if ck.startswith("md5:"):
            import hashlib
            if hashlib.md5(blob).hexdigest() != ck[4:]:
                return None
        _stats["peer"] = _stats.get("peer", 0) + 1
        return blob
    except Exception:
        return None


_stats: dict = {}


def download(entry: dict) -> bytes:
    import requests
    import inbound_budget
    blob = _peer_pull(entry)
    if blob is not None:
        inbound_budget.add(CHANNEL, len(blob))
        return blob
    link, hdr = _link(entry)
    if not link:
        raise RuntimeError("manifest entry has no download link")
    r = requests.get(link, headers=hdr, timeout=DOWNLOAD_TIMEOUT)
    r.raise_for_status()
    blob = r.content
    inbound_budget.add(CHANNEL, len(blob))
    _stats["zenodo"] = _stats.get("zenodo", 0) + 1
    want = int(entry.get("size") or 0)
    if want and len(blob) != want:
        raise RuntimeError(f"size mismatch: got {len(blob)} want {want}")
    ck = entry.get("checksum") or ""
    if ck.startswith("md5:"):
        import hashlib
        got = hashlib.md5(blob).hexdigest()
        if got != ck[4:]:
            raise RuntimeError(f"md5 mismatch: {got} != {ck[4:]}")
    return blob


GONE_CONFIRMATIONS = 2          # deposition probes that must say "gone"
GONE_CONFIRM_GAP_S = 1800       # ... at least this far apart (transient 404s)
GONE_MAX_DROPS_PER_TICK = 3


def _probe_deposit(depo_id) -> str | None:
    """'gone' | 'present' | None (inconclusive: network / 5xx / 429 / auth).

    Only a positive 404/410 on the *deposition* resource counts as gone.
    A 200 counts as present even if the file list lacks our filename —
    PUT-then-DELETE replaces (zenodo_client._replace_in_bucket) legitimately
    rename files, so a file-level judgement here could drop a live product."""
    if not depo_id:
        return None
    try:
        import requests
        from zenodo_client import DEFAULT_TOKEN
        r = requests.get(f"https://zenodo.org/api/deposit/depositions/{depo_id}",
                         headers={"Authorization": f"Bearer {DEFAULT_TOKEN}"}, timeout=30)
        if r.status_code in (404, 410):
            return "gone"
        if r.status_code == 200:
            return "present"
    except Exception as ex:  # noqa: BLE001
        log.debug("v2_ingest: deposit probe %s: %s", depo_id, ex)
    return None


def _zenodo_control_ok() -> bool:
    """Zenodo API + our token answer a trivial request with 200 AND the
    fleet Zenodo circuit is clear. Guards the sweep against an outage /
    revoked token turning every probe into a 4xx."""
    # The fleet circuit trips on peer PUT failures (write timeouts), which
    # says nothing about whether a deposition GET 404 is trustworthy.
    # Oct 2026: it sat in "lossy" (tripped, but 15 GB/h still landing) for
    # days, so this gate skipped every sweep and 72008/72120/72194 stayed
    # at n=3 forever despite their deposits being verifiably gone.  Only a
    # *truly* degraded circuit (nothing landing — same evidence rule as the
    # ZENODO-DEGRADED banner) blocks the sweep; the control probe below is
    # the real guard against outage / revoked token.
    tripped = False
    try:
        from zenodo_cache import zenodo_degraded
        tripped = bool(zenodo_degraded())
    except Exception:  # noqa: BLE001
        pass
    if not tripped:
        try:
            import peer_director as _pd
            tripped = bool(((_pd.load_director_state() or {}).get("zenodo_circuit") or {}).get("degraded"))
        except Exception:  # noqa: BLE001
            pass
    if tripped:
        landing = False
        try:
            import app as _app
            zu = _app._zenodo_upload_health()
            landing = zu.get("ok_files", 0) >= 5 and zu.get("ok_bytes", 0) >= 1e9
        except Exception:  # noqa: BLE001
            landing = False
        if not landing:
            return False
    try:
        import requests
        from zenodo_client import DEFAULT_TOKEN
        r = requests.get("https://zenodo.org/api/deposit/depositions", params={"size": 1},
                         headers={"Authorization": f"Bearer {DEFAULT_TOKEN}"}, timeout=30)
        return r.status_code == 200
    except Exception:  # noqa: BLE001
        return False


def _note_gone_candidate(code: str, entry: dict) -> None:
    """Mark *code*'s failure record as a gone-candidate (file 404). The
    sweep does the actual deposition probes, paced and confirmed."""
    f = failed()
    cur = f.get(code) or {}
    if cur.get("uploaded_at") != (entry.get("uploaded_at") or ""):
        return  # _record_failure will write a fresh record for this upload
    cur.setdefault("gone", [])
    cur["gone_depo"] = entry.get("depo_id")
    f[code] = cur
    store.meta_set("ingest_failed", json.dumps(f, separators=(",", ":")))


def _drop_manifest_key(key: str, reason: str) -> bool:
    """Remove *key* from the primary's zenodo_manifest.json with a
    tombstone (via ``app`` so the peer-sync merge honours it)."""
    try:
        import app as _app
        return bool(_app._manifest_drop_keys([key], reason=reason))
    except Exception as ex:  # noqa: BLE001
        log.warning("v2_ingest: drop manifest key %s failed: %s", key, ex)
        return False


def sweep_gone(entries: dict | None = None) -> dict:
    """Confirm-and-drop dangling ``_json_v2`` manifest entries.

    A failure record becomes a candidate when the file download 404s
    (``_note_gone_candidate``). Each sweep re-probes the *deposition* at
    most once per GONE_CONFIRM_GAP_S; only after GONE_CONFIRMATIONS probes
    all said "gone" (>= GAP apart) is the manifest key dropped. A single
    "present" verdict clears the candidate (transient 404). The sweep is
    skipped entirely when the control probe fails or the Zenodo circuit is
    degraded, and drops are capped per tick. The manifest entry must still
    carry the same uploaded_at/depo_id we observed — a re-upload in the
    meantime means the record is stale and is simply cleared.

    Sep 2026: 72008/72120/72194 parent-level _json_v2 whose deposits had
    been deleted after the KGs were re-split into blocks sat at n=3
    forever, inflating verify_fail."""
    entries = entries if entries is not None else load_manifest_entries()
    f = failed()
    cands = {c: r for c, r in f.items() if isinstance(r, dict) and "gone" in r}
    out = {"candidates": len(cands), "probed": 0, "dropped": [], "cleared": []}
    if not cands:
        return out
    now = time.time()
    if not _zenodo_control_ok():
        out["skipped"] = "zenodo control probe failed / circuit degraded"
        return out
    changed = False
    for code, rec in cands.items():
        key = f"{code}_json_v2"
        e = entries.get(key)
        if (not isinstance(e, dict) or e.get("uploaded_at") != rec.get("uploaded_at")
                or e.get("depo_id") != rec.get("gone_depo")):
            f.pop(code, None); changed = True     # entry gone/replaced — stale record
            out["cleared"].append(code)
            continue
        gone = [float(t) for t in rec.get("gone") or []]
        if gone and now - gone[-1] < GONE_CONFIRM_GAP_S:
            continue
        verdict = _probe_deposit(e.get("depo_id"))
        out["probed"] += 1
        if verdict == "present":
            rec.pop("gone", None); rec.pop("gone_depo", None); changed = True
            log.info("v2_ingest: %s deposit %s present — file 404 was transient", code, e.get("depo_id"))
            continue
        if verdict != "gone":
            continue
        gone.append(now); rec["gone"] = gone; changed = True
        if len(gone) < GONE_CONFIRMATIONS or gone[-1] - gone[0] < GONE_CONFIRM_GAP_S:
            log.info("v2_ingest: %s deposit %s gone (%d/%d confirmations)", code,
                     e.get("depo_id"), len(gone), GONE_CONFIRMATIONS)
            continue
        if len(out["dropped"]) >= GONE_MAX_DROPS_PER_TICK:
            continue
        if _drop_manifest_key(key, f"v2_ingest: deposit {e.get('depo_id')} gone, "
                                   f"{len(gone)} probes over {(gone[-1] - gone[0]) / 60:.0f} min"):
            f.pop(code, None)
            out["dropped"].append(code)
            log.warning("v2_ingest: %s _json_v2 deposit %s confirmed gone — manifest entry dropped",
                        code, e.get("depo_id"))
            try:
                import app as _app
                _app.director_event(
                    f"v2_ingest: {code} _json_v2 deposit {e.get('depo_id')} confirmed gone "
                    f"({len(gone)} probes over {(gone[-1] - gone[0]) / 60:.0f} min) — dangling "
                    f"manifest entry dropped", peer="director", kg=code, level="warning")
            except Exception:  # noqa: BLE001
                pass
    if changed:
        store.meta_set("ingest_failed", json.dumps(f, separators=(",", ":")))
    return out


def _v1_row(code: str) -> dict | None:
    try:
        import search_index as si
        from kg_splitter import parent_kg_code
        if parent_kg_code(code) != code:
            # The index row is the *parent* aggregate (sum over sibling
            # blocks).  Comparing a single block's parcel/segment count
            # against it is meaningless — 45306-south struck out on
            # `parcels_vs_v1_row: v2=1906 v1=975` (the row then held only
            # the -southwest sibling).  Blocks have no v1 baseline.
            return None
        r = si.get_index()._conn().execute(
            "SELECT parcel_count, n_segments FROM kg WHERE kg_code=?",
            (parent_kg_code(code),)).fetchone()
        return {"n_parcels": r[0], "n_segments": r[1]} if r else None
    except Exception:  # noqa: BLE001
        return None


def ingest_one(code: str, entries: dict, *, delete_v1: bool = True,
               update_index: bool = True, scan_flags: bool = True,
               json_dir: Path | None = None) -> dict:
    """Ingest one code.  Returns ``{code, ok, reason|size, freed}``."""
    e = entries.get(f"{code}_json_v2")
    if not _committed(e):
        return {"code": code, "ok": False, "reason": "no committed _json_v2 entry"}
    up_at = e.get("uploaded_at") or ""
    t0 = time.time()
    try:
        blob = download(e)
    except Exception as ex:  # noqa: BLE001
        _record_failure(code, f"download: {ex}", up_at)
        if "404" in str(ex):
            # Possibly a dangling entry (deposit deleted on Zenodo). Never
            # act on one observation: sweep_gone() confirms with two
            # deposition probes >= GONE_CONFIRM_GAP_S apart before dropping.
            _note_gone_candidate(code, e)
        return {"code": code, "ok": False, "reason": f"download: {ex}"}
    # v1 baseline: the file (or an older store copy); flat index row fallback
    v1_doc = kg_docs.load(code, json_dir)
    rep, doc = v2_verify.verify_for_ingest(code, blob, v1_doc, entries,
                                           v1_row=None if v1_doc else _v1_row(code))
    if not rep["ok"] or doc is None:
        if doc is not None and _deterministic_repeat(code, rep, doc, up_at):
            # Oct 2026, 66319 Laafeld: two independent full runs on two
            # peers 10 h apart reproduced the identical
            # ``segmented_area_ge_v1: v2=24460693 v1=25738086`` figure —
            # BEV no longer serves the cross-border strip the June v1 run
            # had, not an outage. A gate that only exists to catch
            # *transient* upstream holes must not strike a reproducible
            # result forever (each re-run reset n and re-dispatched).
            log.warning("v2_ingest: %s verify FAILED but reproduced bit-identical on a re-upload "
                        "— accepting as deterministic (%s)", code, rep.summary())
            try:
                import app as _app
                _app.director_event("warning", f"v2_ingest: {code} accepted on deterministic repeat "
                                    f"of {'; '.join(rep['errors'][:2])[:160]}", code)
            except Exception:  # noqa: BLE001
                pass
        elif doc is not None and (_ev := _bev_empty_confirmed(code, rep, doc, v1_doc)):
            log.warning("v2_ingest: %s verify FAILED on v1-comparison gates only; the missing DTM "
                        "tiles are all-NoData on BEV (clipped to AT since the v1 run), accepting (%s)", code, _ev)
            try:
                import app as _app
                _app.director_event("warning", f"v2_ingest: {code} accepted — DTM tiles v1 had are now "
                                    f"clipped/empty on BEV ({_ev[:120]})", code)
            except Exception:  # noqa: BLE001
                pass
        else:
            _record_failure(code, rep.summary(), up_at)
            log.warning("v2_ingest: %s verify FAILED: %s", code, rep.summary())
            return {"code": code, "ok": False, "reason": rep.summary()}
    # 1) store FIRST — the index selector reads the store
    info = store.put(code, blob, uploaded_at=up_at, doc=doc, source="zenodo")
    if _is_sweep(code):
        # Sliver-sweep add-on (docs/sliver-sweep.md → Products): it only
        # needs to exist in the store (bbox + grid25 for trees v3 /
        # heightfield / product segments via codes_for_parent).  No index
        # row (the parent aggregate would double-count the pad strip), no
        # repair registry row, no quality-flag scan.
        _clear_failure(code)
        log.info("v2_ingest: %s sweep add-on stored (%d B gz, %s, %.1fs)",
                 code, info["size"], rep.summary().split(" |")[0], time.time() - t0)
        return {"code": code, "ok": True, "size": info["size"], "freed": 0,
                "warnings": rep["warnings"], "sweep": True}
    try:  # repairable-layer registry (docs/product-repair.md)
        import product_repair as _pr
        _pr.observe(code, doc, up_at, str(e.get("version") or ""))
    except Exception as ex:  # noqa: BLE001
        log.debug("v2_ingest: %s product_repair observe: %s", code, ex)
    # 2) index row
    if update_index:
        try:
            import search_index as si
            si.get_index().update_kg(code, manifest=entries)
        except Exception as ex:  # noqa: BLE001
            log.warning("v2_ingest: %s update_kg: %s", code, ex)
    # 3) drop the v1 file, account the saving
    freed = 0
    if delete_v1:
        p = kg_docs.file_path(code, json_dir)
        try:
            if p.exists():
                freed = p.stat().st_size
                p.unlink()
                store.add_freed(code, freed)
        except Exception as ex:  # noqa: BLE001
            log.warning("v2_ingest: %s delete v1 file: %s", code, ex)
    # 4) quality flags from the in-memory doc
    if scan_flags:
        try:
            import quality_flags
            quality_flags.scan_doc(doc, code)
        except Exception as ex:  # noqa: BLE001
            log.warning("v2_ingest: %s scan_doc: %s", code, ex)
    _clear_failure(code)
    log.info("v2_ingest: %s ingested (%d B gz, v1 freed %d B, %s, %.1fs)",
             code, info["size"], freed, rep.summary().split(" |")[0], time.time() - t0)
    return {"code": code, "ok": True, "size": info["size"], "freed": freed,
            "warnings": rep["warnings"]}


def run_tick(max_n: int = MAX_PER_TICK, *, harvest: bool = True) -> dict:
    """One ingest pass.  Safe to call from any thread; single-flight."""
    if not _lock.acquire(blocking=False):
        return {"skipped": "busy"}
    t0 = time.time()
    out = {"ingested": 0, "failed": 0, "pending": 0, "codes": [], "freed": 0}
    try:
        entries = load_manifest_entries()
        todo = pending(entries)
        out["pending"] = len(todo)
        try:
            import inbound_budget
            import peer_director as _pd
            _mb_h = float(_pd.knob(None, "v2_ingest_mb_per_h", MB_PER_H_DEFAULT))
        except Exception:
            inbound_budget = None
            _mb_h = MB_PER_H_DEFAULT
        for code in todo[:max_n]:
            # Primary inbound budget (metered VM): stop the tick when the
            # trailing hour would exceed ``v2_ingest_mb_per_h``.  The
            # backlog simply drains over the following ticks.
            if inbound_budget is not None:
                _sz = int((entries.get(f"{code}_json_v2") or {}).get("size") or 0)
                if not inbound_budget.allow(CHANNEL, _mb_h, _sz):
                    out["budget_hold"] = len(todo) - out["ingested"] - out["failed"]
                    break
            try:
                r = ingest_one(code, entries)
            except Exception as ex:  # noqa: BLE001
                log.warning("v2_ingest: %s crashed: %s", code, ex)
                _record_failure(code, f"crash: {ex}",
                                (entries.get(f"{code}_json_v2") or {}).get("uploaded_at") or "")
                r = {"ok": False}
            if r.get("ok"):
                out["ingested"] += 1
                out["freed"] += int(r.get("freed") or 0)
                out["codes"].append(code)
            else:
                out["failed"] += 1
        out["pending"] = max(0, len(todo) - out["ingested"] - out["failed"])
        try:
            sg = sweep_gone(entries)
            if sg.get("probed") or sg.get("dropped") or sg.get("cleared"):
                out["gone_sweep"] = sg
        except Exception as ex:  # noqa: BLE001
            log.warning("v2_ingest: gone sweep: %s", ex)
        if harvest:
            try:
                import kg_log_harvest
                out["log_live"] = kg_log_harvest.harvest_live()
                out["log_archive"] = kg_log_harvest.harvest_archive_step(max_days=1)
            except Exception as ex:  # noqa: BLE001
                log.warning("v2_ingest: log harvest: %s", ex)
    finally:
        out["dt"] = round(time.time() - t0, 1)
        out["ts"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        _last_tick.clear()
        _last_tick.update(out)
        _lock.release()
    if out["ingested"] or out["failed"]:
        log.info("v2_ingest tick: %s", {k: v for k, v in out.items() if k != "codes"})
    return out


def last_tick() -> dict:
    return dict(_last_tick)


def _json_dir_stats(json_dir: Path = kg_docs.JSON_DIR) -> tuple[int, int]:
    n = b = 0
    try:
        for p in Path(json_dir).glob("*.json"):
            try:
                b += p.stat().st_size
                n += 1
            except OSError:
                pass
    except Exception:  # noqa: BLE001
        pass
    return n, b


_stats_cache: dict = {"t": 0.0, "v": None}


def status(total_kgs: int = None, ttl: float = 60.0) -> dict:
    """Structured block for ``/api/v1/director/status.v2`` and the
    ``v2:`` line in ``/process.txt``.  TTL-cached (manifest + json dir
    scans are not free)."""
    if not total_kgs:
        from kg_universe import expected_total
        total_kgs = expected_total()
    now = time.time()
    if _stats_cache["v"] is not None and now - _stats_cache["t"] < ttl:
        return _stats_cache["v"]
    entries = load_manifest_entries()
    day_ago = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now - 86400))
    try:
        from v21_products import MANIFEST_VERSION as _cur
    except Exception:  # noqa: BLE001
        _cur = "v2.3"
    # Source of truth: the search index (one row per PARENT KG, version =
    # min across blocks).  A parent counts as upgraded only at the CURRENT
    # product version; 2.1 parents awaiting re-upgrade are ``stale``.
    # The 24h count / rate only see current-version uploads, so a parent
    # that went v2 → v2.1 → v2.2 is counted once, not three times.
    ps = None
    try:
        import search_index as _si
        ps = _si.get_index().progress_summary(entries, total_kgs=total_kgs,
                                              current_version=_cur)["v2"]
    except Exception as ex:  # noqa: BLE001
        log.debug("v2 status: index unavailable (%s), manifest fallback", ex)
    fresh_v2 = 0
    codes_by_version: dict = {}
    for k, e in entries.items():
        if not _committed(e):
            continue
        if k.endswith("_json_v2"):
            v = str(e.get("version") or "v2")
            codes_by_version[v] = codes_by_version.get(v, 0) + 1
        elif k.endswith("_json") and (e.get("version") == "v2"):
            fresh_v2 += 1
    if ps is None:
        # Manifest fallback (parents folded; version = min across blocks).
        par: dict = {}
        up_ts: dict = {}
        for k, e in entries.items():
            if not _committed(e) or not k.endswith("_json_v2"):
                continue
            code = k[:-8]
            if _is_sweep(code):
                continue            # add-on, not a block of the layout
            parent = code.split("-", 1)[0]
            v = str(e.get("version") or "v2")
            if parent not in par or _vkey(v) < _vkey(par[parent]):
                par[parent] = v
            ts = e.get("uploaded_at") or ""
            up_ts[parent] = max(up_ts.get(parent, ""), ts)
        by_version: dict = {}
        for v in par.values():
            by_version[v] = by_version.get(v, 0) + 1
        cur_parents = [p for p, v in par.items() if v == _cur]
        up_24h = sum(1 for p in cur_parents if up_ts.get(p, "") > day_ago)
        rate_h = up_24h / 24.0
        remaining = max(0, total_kgs - len(cur_parents))
        ps = {
            "current_version": _cur, "by_version": by_version,
            "upgraded_current": len(cur_parents), "upgraded_any": len(par),
            "stale_versions": {v: n for v, n in by_version.items() if v != _cur},
            "stale_total": sum(n for v, n in by_version.items() if v != _cur),
            "upgraded_24h": up_24h, "rate_per_h": round(rate_h, 2),
            "eta_days": round(remaining / rate_h / 24.0, 1) if rate_h > 0 else None,
            "remaining": remaining, "source": "manifest",
        }
    else:
        ps["source"] = "index"
    upgraded = ps["upgraded_current"]
    up_24h = ps["upgraded_24h"]
    rate_h = ps["rate_per_h"]
    eta_d = ps.get("eta_days")
    first_up = min((e.get("uploaded_at") or "" for k, e in entries.items()
                    if k.endswith("_json_v2") and _committed(e)), default="")
    v21_codes = sum(n for v, n in codes_by_version.items() if v in ("v2.1", "v2.2"))
    st = store.stats()
    ls = store.log_stats()
    fl = failed()
    n_files, b_files = _json_dir_stats()
    try:
        import kg_log_harvest
        done = set((store.meta_get("log_harvest_archive_done", "") or "").split(",")) - {""}
        arch_remaining = sum(1 for p in kg_log_harvest.ARCHIVE_DIR.glob("*.jsonl.gz")
                             if p.name[:10] not in done) if kg_log_harvest.ARCHIVE_DIR.is_dir() else 0
    except Exception:  # noqa: BLE001
        arch_remaining = None
    try:
        pend = len(pending(entries))
    except Exception:  # noqa: BLE001
        pend = None
    v = {
        "upgraded_parents": upgraded, "total": total_kgs,
        "upgraded_pct": round(100.0 * upgraded / max(total_kgs, 1), 2),
        "upgraded_24h": up_24h, "rate_per_h": round(rate_h, 2), "eta_days": eta_d,
        "first_upload": first_up or None,
        "current_version": ps["current_version"],
        "by_version": ps["by_version"],            # parents, min version across blocks
        "upgraded_any": ps["upgraded_any"],        # parents with any _json_v2
        "stale_versions": ps["stale_versions"],    # upgraded but below current
        "stale_total": ps["stale_total"],
        "remaining": ps["remaining"],
        "source": ps.get("source"),
        "codes_by_version": codes_by_version,      # product codes (blocks), manifest
        "fresh_v2": fresh_v2, "v21_codes": v21_codes,
        "verify_fail": len(fl), "verify_fail_codes": sorted(fl)[:20],
        "pending": pend,
        "store": st, "kg_log": ls, "archive_days_remaining": arch_remaining,
        "json_files": n_files, "json_bytes": b_files,
        "last_tick": last_tick(),
    }
    _stats_cache.update(t=now, v=v)
    return v


def _fmt_b(n) -> str:
    n = float(n or 0)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or u == "TB":
            return f"{n:.0f}{u}" if u in ("B", "KB") else f"{n:.1f}{u}"
        n /= 1024
    return f"{n:.1f}TB"


def text_line(v: dict | None = None) -> str:
    """One-line summary for ``/process.txt``."""
    v = v or status()
    st = v["store"]
    n = st.get("codes") or 0
    avg = (st.get("v1_bytes_freed") or 0) / n if n else 0
    eta = f"{v['eta_days']}d" if v.get("eta_days") is not None else "?"
    ls = v["kg_log"]
    parts = [
        f"v2: at_{v.get('current_version', 'v2.2')}={v['upgraded_parents']}/{v['total']} ({v['upgraded_pct']}%) "
        f"+{v['upgraded_24h']}/24h @{v['rate_per_h']}/h eta={eta} "
        f"parents_by_version[{' '.join(f'{k}={n}' for k, n in (v.get('by_version') or {}).items())}] "
        f"stale={v.get('stale_total', 0)} any_v2={v.get('upgraded_any', 0)}",
        f"fresh_v2={v['fresh_v2']}",
        f"verify_fail={v['verify_fail']}" + ("" if v["pending"] is None else f" pending={v['pending']}"),
        f"store={n}codes/{_fmt_b(st.get('db_bytes'))} freed={_fmt_b(st.get('v1_bytes_freed'))} "
        f"avg/kg={_fmt_b(avg)}"
        + (" store_pv[" + ' '.join(f'{k}={n}' for k, n in sorted((st.get('product_versions') or {}).items()))
           + f"] grid25={_fmt_b(st.get('grid25_bytes'))}" if n else ""),
        f"json_files={v['json_files']} disk={_fmt_b(v['json_bytes'])}",
        f"kg_log={ls.get('codes', 0)}codes/{ls.get('rows', 0)}rows/{_fmt_b(ls.get('bytes'))}"
        + (f" archive_remaining={v['archive_days_remaining']}d"
           if v.get("archive_days_remaining") is not None else ""),
    ]
    return " · ".join(parts)


def loop(should_run, tick_s: int = TICK_S) -> None:
    """Daemon-thread body.  ``should_run()`` is polled every tick (primary
    check lives in app.py so this module stays import-safe)."""
    time.sleep(90)   # let srv settle after (re)start
    while True:
        try:
            if should_run():
                run_tick()
        except Exception as ex:  # noqa: BLE001
            log.warning("v2_ingest loop: %s", ex)
        time.sleep(tick_s)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if len(sys.argv) > 1 and sys.argv[1] == "status":
        print(text_line())
        print(json.dumps(status(), indent=1, default=str))
    elif len(sys.argv) > 1:
        ents = load_manifest_entries()
        for c in sys.argv[1:]:
            print(ingest_one(c, ents, delete_v1=False, update_index=False, scan_flags=False))
    else:
        print(run_tick())
