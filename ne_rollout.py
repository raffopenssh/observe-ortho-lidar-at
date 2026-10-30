"""ne_rollout — primary-side bookkeeping for the v2.4 legacy-section clean-up + NE cells.

Two questions the operator asks every hour of the wave, answered from the
Zenodo manifest (+ the NE server's NE manifest, cached):

* ``legacy:``  how many Zenodo files may still carry legacy sections, how many
  legacy depositions were removed (records kept, ``status=removed``) and how
  many of those removals the audit has already confirmed gone on Zenodo.
* ``ne:``      NE coverage — NE-server built / 7 850, our ``_ne_h3b`` products,
  NEO1 posted, the active ``ne_source``, h3b bytes on the primary disk.

Rules (mirror ``austria_processor.legacy_pending`` / ``_mark_property_free``):
  a product record is CLEAN iff ``status in (cf, stripped)`` or
  ``status == removed`` or it is a ``_full_gpkg`` (never had legacy sections) or an
  ``_ne_h3b``.  Everything else written before this code (every v1 ``_json``,
  v1 ``_light_gpkg``, v2.x ``_json_v2`` / ``_light_gpkg_v2``) is SUSPECT.

Also the **removed-record audit**: for records with ``status=removed`` we ask
the Zenodo deposition API once (HEAD/GET → 404/410 = gone) and persist the
verdict in ``data/austria_processor/legacy_audit.json``; a 200 is loud
(``REMOVED-BUT-PRESENT``) and the director re-issues the delete.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("ne_rollout")

DATA_DIR = Path("data/austria_processor")
MANIFEST_PATH = DATA_DIR / "zenodo_manifest.json"
AUDIT_FILE = DATA_DIR / "legacy_audit.json"
NE_MANIFEST_CACHE = DATA_DIR / "ne_server_manifest.json"
NE_SOURCE_FILE = DATA_DIR / "ne_source"
NE_DIR = Path("data/ne")
JSON_DIR = DATA_DIR / "json"
LOCAL_STRIP_STATE = DATA_DIR / "legacy_local_strip.json"

CLEAN_STATUS = ("cf", "stripped", "removed")
SUSPECT_SUFFIXES = ("_json", "_light_gpkg", "_json_v2", "_light_gpkg_v2")

_lock = threading.Lock()
_cache: dict = {"at": 0.0, "mtime": None, "summary": None}
SUMMARY_TTL_S = 60


# === SECTION: manifest audit ===

def _suffix(key: str) -> str:
    for suf in ("_light_gpkg_v2", "_json_v2", "_ne_h3b", "_full_gpkg", "_light_gpkg", "_json", "_error"):
        if key.endswith(suf):
            return suf
    return ""


def _committed(e: dict) -> bool:
    return isinstance(e, dict) and int(e.get("size") or 0) > 0 and "error" not in str(e.get("status") or "")


def audit_manifest(entries: dict | None = None) -> dict:
    """One pass over the manifest → counts + the suspect / removed code lists."""
    if entries is None:
        try:
            raw = json.loads(MANIFEST_PATH.read_text())
            entries = raw.get("entries", raw)
        except Exception:
            entries = {}
    try:
        from v21_products import MANIFEST_VERSION as CUR
    except Exception:  # noqa: BLE001
        CUR = "v2.4"
    suspect = {s: 0 for s in SUSPECT_SUFFIXES}
    clean = {s: 0 for s in SUSPECT_SUFFIXES}
    removed = 0
    stripped = 0
    cf = 0
    from kg_splitter import is_sweep_code as _is_sweep
    h3b = 0
    h3b_sweep = 0
    h3b_bytes = 0
    suspect_codes: set[str] = set()
    removed_keys: list[str] = []
    codes_cur_pair: set[str] = set()
    codes_with_json: set[str] = set()
    for k, e in entries.items():
        if not isinstance(e, dict):
            continue
        suf = _suffix(k)
        if not suf or suf == "_error":
            continue
        code = k[: -len(suf)]
        st = str(e.get("status") or "")
        if st == "removed":
            removed += 1
            removed_keys.append(k)
            continue
        if not _committed(e):
            continue
        if suf == "_json":
            codes_with_json.add(code)
        if suf == "_ne_h3b":
            if _is_sweep(code):
                h3b_sweep += 1          # sliver-sweep archive copies — not parent coverage
            else:
                h3b += 1
                h3b_bytes += int(e.get("size") or 0)
            continue
        if suf == "_full_gpkg":
            continue
        if st == "stripped":
            stripped += 1
        if st == "cf":
            cf += 1
        if st in CLEAN_STATUS:
            clean[suf] += 1
        else:
            suspect[suf] += 1
            suspect_codes.add(code)
        if suf == "_json_v2" and str(e.get("version") or "") == CUR and st == "cf":
            lg2 = entries.get(f"{code}_light_gpkg_v2")
            if _committed(lg2) and str(lg2.get("status") or "") == "cf":
                codes_cur_pair.add(code)
    n_suspect_files = sum(suspect.values())
    return {
        "current_version": CUR,
        "suspect_files": n_suspect_files,
        "suspect_by_kind": suspect,
        "clean_by_kind": clean,
        "suspect_codes": len(suspect_codes),
        "codes_with_json": len(codes_with_json),
        "codes_v24_clean_pair": len(codes_cur_pair),
        "removed_records": removed,
        "removed_keys": removed_keys,
        "stripped": stripped,
        "cf": cf,
        "h3b": h3b,
        "h3b_sweep": h3b_sweep,
        "h3b_gb": round(h3b_bytes / 1e9, 2),
        "_suspect_code_set": suspect_codes,
    }


# === SECTION: removed-record audit (Zenodo deposition really gone?) ===

# === SECTION: local JSON mirror of the Zenodo strip ===

def strip_local_json(entries: dict | None = None, max_files: int = 200) -> dict:
    """Mirror the fleet's v1 ``_json`` strip onto the primary's local copies.

    Peers strip + re-PUT the Zenodo ``_json`` in place (``status=stripped``)
    or replace it with a 2.4 run (``status=cf``); the primary keeps a local
    copy under ``data/austria_processor/json/<code>.json`` for the coverage
    oracle / search-index ingest, which nobody rewrites.  Once the manifest
    says the Zenodo copy is clean we drop the legacy sections from the
    local file too (same ``strip_legacy_sections``), so the primary never
    holds legacy data past the point the fleet has retired it.  Bounded
    per call; ``legacy_local_strip.json`` records what was done.
    Returns {stripped, remaining, checked}."""
    if entries is None:
        try:
            raw = json.loads(MANIFEST_PATH.read_text())
            entries = raw.get("entries", raw)
        except Exception:
            entries = {}
    try:
        from austria_processor import strip_legacy_sections, LEGACY_JSON_SECTIONS
    except Exception as e:  # noqa: BLE001
        log.debug("strip_local_json: import failed: %s", e)
        return {"stripped": 0, "remaining": -1, "checked": 0}
    try:
        st = json.loads(LOCAL_STRIP_STATE.read_text())
    except Exception:
        st = {}
    done_codes = set(st.get("codes") or [])
    stripped = remaining = checked = 0
    for p in sorted(JSON_DIR.glob("*.json")):
        code = p.stem
        if code in done_codes:
            continue
        e = entries.get(f"{code}_json")
        if not isinstance(e, dict) or str(e.get("status") or "") not in ("cf", "stripped"):
            remaining += 1
            continue
        if checked >= max_files:
            remaining += 1
            continue
        checked += 1
        try:
            with open(p) as fh:
                doc = json.load(fh)
            if any(k in doc for k in LEGACY_JSON_SECTIONS):
                out = strip_legacy_sections(doc)
                tmp = p.with_suffix(".tmp")
                with open(tmp, "w") as fh:
                    json.dump(out, fh, separators=(",", ":"), default=str)
                os.replace(tmp, p)
                stripped += 1
            done_codes.add(code)
        except Exception as e:  # noqa: BLE001
            log.warning("strip_local_json %s: %s", code, e)
    try:
        tmp = LOCAL_STRIP_STATE.with_suffix(".tmp")
        tmp.write_text(json.dumps({"codes": sorted(done_codes), "at": _now(),
                                   "last": {"stripped": stripped, "remaining": remaining}}))
        tmp.replace(LOCAL_STRIP_STATE)
    except Exception:
        pass
    return {"stripped": stripped, "remaining": remaining, "checked": checked}


def local_strip_counts() -> dict:
    try:
        st = json.loads(LOCAL_STRIP_STATE.read_text())
        return {"local_clean": len(st.get("codes") or []), "local_suspect": int((st.get("last") or {}).get("remaining", -1))}
    except Exception:
        return {"local_clean": 0, "local_suspect": -1}


def load_audit() -> dict:
    try:
        d = json.loads(AUDIT_FILE.read_text())
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def save_audit(d: dict) -> None:
    tmp = AUDIT_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(d, indent=0, sort_keys=True))
    tmp.replace(AUDIT_FILE)


def audit_removed(max_checks: int = 20, entries: dict | None = None, timeout: float = 20.0) -> dict:
    """Verify up to *max_checks* not-yet-audited ``status=removed`` records
    against the deposition API.  Returns {checked, gone, present, pending}.
    Cheap: one small GET per record, ≤ 20 per director tick."""
    import requests
    from zenodo_client import DEFAULT_TOKEN
    if entries is None:
        try:
            raw = json.loads(MANIFEST_PATH.read_text())
            entries = raw.get("entries", raw)
        except Exception:
            return {"error": "manifest unreadable"}
    aud = load_audit()
    todo = [k for k, e in entries.items() if isinstance(e, dict) and e.get("status") == "removed"
            and (k not in aud or aud[k].get("verdict") == "present")]
    todo.sort(key=lambda k: aud.get(k, {}).get("checked_at", ""))
    checked = gone = present = 0
    for k in todo[:max_checks]:
        e = entries[k]
        depo = int(e.get("depo_id") or 0)
        if not depo:
            aud[k] = {"verdict": "gone", "checked_at": _now(), "note": "no depo_id"}
            gone += 1
            continue
        try:
            r = requests.get(f"https://zenodo.org/api/deposit/depositions/{depo}",
                             headers={"Authorization": f"Bearer {DEFAULT_TOKEN}"}, timeout=timeout)
        except Exception as ex:  # noqa: BLE001
            log.debug("legacy audit %s: %s", k, ex)
            break
        checked += 1
        if r.status_code in (404, 410):
            aud[k] = {"verdict": "gone", "checked_at": _now(), "depo_id": depo}
            gone += 1
        elif r.status_code == 200:
            # A record the owner deleted on the Zenodo website keeps an
            # owner-only draft residue in the deposit API (submitted=true,
            # files listed, DELETE → 500) while the public record answers
            # 410.  That is gone for every reader; don't re-delete forever
            # (12362: published April v1 trio, deleted by the owner).
            try:
                if (r.json() or {}).get("submitted"):
                    pr = requests.get(f"https://zenodo.org/api/records/{depo}", timeout=timeout)
                    if pr.status_code == 410:
                        aud[k] = {"verdict": "gone", "checked_at": _now(), "depo_id": depo,
                                  "note": "public record 410 (deleted), owner draft residue"}
                        gone += 1
                        continue
            except Exception:  # noqa: BLE001
                pass
            aud[k] = {"verdict": "present", "checked_at": _now(), "depo_id": depo}
            present += 1
            log.warning("legacy audit: %s depo %d marked removed but still PRESENT on Zenodo", k, depo)
        elif r.status_code in (401, 403, 429) or r.status_code >= 500:
            break           # don't burn the tick on a Zenodo hiccup
        else:
            aud[k] = {"verdict": f"http_{r.status_code}", "checked_at": _now(), "depo_id": depo}
    save_audit(aud)
    pending = sum(1 for k, e in entries.items() if isinstance(e, dict) and e.get("status") == "removed"
                  and k not in aud)
    return {"checked": checked, "gone": gone, "present": present, "pending": pending}


def audit_counts(entries: dict | None = None) -> dict:
    aud = load_audit()
    if entries is None:
        try:
            raw = json.loads(MANIFEST_PATH.read_text())
            entries = raw.get("entries", raw)
        except Exception:
            entries = {}
    keys = [k for k, e in entries.items() if isinstance(e, dict) and e.get("status") == "removed"]
    gone = sum(1 for k in keys if aud.get(k, {}).get("verdict") == "gone")
    present = [k for k in keys if aud.get(k, {}).get("verdict") == "present"]
    return {"removed": len(keys), "verified_gone": gone, "present": present,
            "unverified": len(keys) - gone - len(present)}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# === SECTION: NE-server manifest (cached) ===

NE_MANIFEST_FULL_EVERY_S = 6 * 3600   # deltas omit queue/frozen → refetch full body this often


def _merge_ne_delta(base: dict, delta: dict) -> dict:
    """Merge a ``delta:true`` answer into *base* (by KG code; boundary entries
    repeat → idempotent).  Counters + generated_at come from the delta;
    queue/frozen are kept from *base*."""
    out = dict(base)
    kgs = dict(base.get("kgs") or {})
    for code, ent in (delta.get("kgs") or {}).items():
        if ent is None:
            kgs.pop(code, None)
        else:
            kgs[code] = ent
    out["kgs"] = kgs
    for k in ("built", "kgs_total", "obs_posted", "kgs_obs_on_bev", "kgs_obs_on_index",
              "generated_at", "obs_public"):
        if k in delta:
            out[k] = delta[k]
    out["delta"] = False  # our merged copy is a full view
    return out


def refresh_ne_manifest(max_age_s: int = 600, full: bool = False) -> dict | None:
    """GET the NE server's /ne/manifest at most every *max_age_s*, persisted to
    ``ne_server_manifest.json`` so both gunicorn workers and the HA shadow
    read the same view.  Returns the manifest dict.

    Polling is delta-based: every call after the first sends
    ``since=<generated_at of the previous answer>`` (+ ETag) and merges the
    changed KGs into the stored copy.  ``delta:false`` (cursor >7 d old or
    unknown) replaces the copy.  A full body (which alone carries
    ``queue``/``frozen``) is refetched every ``NE_MANIFEST_FULL_EVERY_S`` or
    on *full=True*."""
    cached = None
    try:
        if NE_MANIFEST_CACHE.exists():
            cached = json.loads(NE_MANIFEST_CACHE.read_text())
            if not full and time.time() - NE_MANIFEST_CACHE.stat().st_mtime < max_age_s:
                return cached.get("manifest")
    except Exception:
        cached = None
    cached = cached or {}
    prev = cached.get("manifest") or {}
    since = prev.get("generated_at") if (prev.get("kgs") and not prev.get("delta")) else None
    full_age = time.time() - float(cached.get("full_at_ts") or 0)
    want_full = full or not since or full_age > NE_MANIFEST_FULL_EVERY_S
    try:
        import ne_h3
        if want_full:
            mf, etag = ne_h3.fetch_manifest(etag=cached.get("etag") if not since else None)
            delta_used = False
        else:
            try:
                mf, etag = ne_h3.fetch_manifest(etag=cached.get("delta_etag"), since=since)
                delta_used = True
            except Exception as e:  # noqa: BLE001
                # 400 (bad cursor) or any delta-path failure → fall back to full
                log.info("ne manifest delta(since=%s) failed: %s — fetching full", since, e)
                mf, etag = ne_h3.fetch_manifest()
                delta_used = False
        rec = {"etag": cached.get("etag"), "delta_etag": cached.get("delta_etag"),
               "full_at_ts": cached.get("full_at_ts"), "fetched_at": _now()}
        if mf is None:                       # 304 — nothing changed; touch mtime
            mf = prev
        elif delta_used and mf.get("delta"):
            rec["delta_etag"] = etag
            mf = _merge_ne_delta(prev, mf)
        else:                                # full body (requested, or delta:false fallback)
            rec["etag"] = etag
            rec["full_at_ts"] = time.time()
            mf = dict(mf)
            mf["delta"] = False
        rec["manifest"] = mf
        if mf:
            tmp = NE_MANIFEST_CACHE.with_suffix(".tmp")
            tmp.write_text(json.dumps(rec))
            tmp.replace(NE_MANIFEST_CACHE)
        return mf
    except Exception as e:  # noqa: BLE001
        log.debug("ne manifest refresh: %s", e)
        return prev or None


def ne_built_codes(mf: dict | None = None) -> set[str]:
    mf = mf if mf is not None else refresh_ne_manifest()
    if not mf:
        return set()
    return {k for k, v in (mf.get("kgs") or {}).items()
            if isinstance(v, dict) and (v.get("lu") or {}) and v.get("status", "built") == "built"}


def ne_source() -> str:
    """Active declared-layer source — mirrors ``ne_h3.ne_source``."""
    try:
        import ne_h3
        return ne_h3.ne_source()
    except Exception:
        return "bevdirect"


# === SECTION: summary + process.txt lines ===

def summary(force: bool = False) -> dict:
    """TTL-cached (60 s) combined view for /process.txt and director/status."""
    with _lock:
        try:
            mt = MANIFEST_PATH.stat().st_mtime
        except Exception:
            mt = None
        if not force and _cache["summary"] is not None and time.time() - _cache["at"] < SUMMARY_TTL_S \
                and _cache["mtime"] == mt:
            return _cache["summary"]
        try:
            raw = json.loads(MANIFEST_PATH.read_text())
            entries = raw.get("entries", raw)
        except Exception:
            entries = {}
        a = audit_manifest(entries)
        a.pop("_suspect_code_set", None)
        a.pop("removed_keys", None)
        a["removed_audit"] = audit_counts(entries)
        a["local_json"] = local_strip_counts()
        mf = None
        try:
            if NE_MANIFEST_CACHE.exists():
                mf = (json.loads(NE_MANIFEST_CACHE.read_text()) or {}).get("manifest")
        except Exception:
            mf = None
        ne = {"source": ne_source(), "ne_server_built": None, "ne_server_total": None, "ne_server_queue": None,
              "obs_posted": None, "frozen": None}
        if mf:
            ne.update(ne_server_built=mf.get("built"), ne_server_total=mf.get("kgs_total"),
                      obs_posted=mf.get("obs_posted"), kgs_obs_on_bev=mf.get("kgs_obs_on_bev"),
                      ne_server_queue={k: (mf.get("queue") or {}).get(k) for k in ("done", "total", "avg_seconds", "finished", "current")},
                      frozen={k: (mf.get("frozen") or {}).get(k) for k in ("algo", "lu_epoch", "lu_source", "bev_source", "until")})
        # on-disk h3b corpus (primary fast path)
        n_disk = 0
        b_disk = 0
        try:
            for p in NE_DIR.glob("*.h3b"):
                n_disk += 1
                b_disk += p.stat().st_size
        except Exception:
            pass
        ne.update(h3b_zenodo=a["h3b"], h3b_zenodo_gb=a["h3b_gb"], h3b_sweep=a.get("h3b_sweep", 0), h3b_disk=n_disk,
                  h3b_disk_gb=round(b_disk / 1e9, 2))
        # bevdirect tooling readiness across the fleet (peers ship
        # ``status.ne_tooling`` on full pushes; cross-worker cache on disk).
        try:
            import peer_director as _pd
            rdy = tot = 0
            tags: dict = {}
            for fp in _pd._PEER_PUSH_DIR.glob("*.json"):
                if time.time() - fp.stat().st_mtime > 1800:
                    continue
                st = ((json.loads(fp.read_text()) or {}).get("status") or {})
                nt = st.get("ne_tooling")
                if not isinstance(nt, dict):
                    continue
                tot += 1
                if nt.get("binary") and nt.get("package"):
                    rdy += 1
                    tags[nt.get("tag")] = tags.get(nt.get("tag"), 0) + 1
            ne.update(bevdirect_ready=rdy, bevdirect_reporting=tot, bevdirect_tags=tags)
        except Exception:
            pass
        out = {"legacy": a, "ne": ne, "generated_at": _now()}
        _cache.update(at=time.time(), mtime=mt, summary=out)
        return out


def process_txt_lines() -> list[str]:
    s = summary()
    a, ne = s["legacy"], s["ne"]
    sb = a["suspect_by_kind"]
    ra = a["removed_audit"]
    flag = ""
    if ra["present"]:
        flag = f" REMOVED-BUT-PRESENT={len(ra['present'])}"
    l1 = (f"legacy:   suspect_files={a['suspect_files']} "
          f"[json={sb['_json']} light={sb['_light_gpkg']} json_v2={sb['_json_v2']} light_v2={sb['_light_gpkg_v2']}] "
          f"codes={a['suspect_codes']}/{a['codes_with_json']} · clean: cf={a['cf']} stripped={a['stripped']} "
          f"removed={a['removed_records']} (gone={ra['verified_gone']} unverified={ra['unverified']}){flag} · "
          f"v2.4-clean pairs={a['codes_v24_clean_pair']} · local_json: suspect={a['local_json']['local_suspect']} clean={a['local_json']['local_clean']}")
    q = ne.get("ne_server_queue") or {}
    l2 = (f"ne:       source={ne['source']} ne_server_built={ne.get('ne_server_built')}/{ne.get('ne_server_total')} "
          f"(queue {q.get('done')}/{q.get('total')} @{q.get('avg_seconds')}s{' done' if q.get('finished') else ''}) "
          f"obs_posted={ne.get('obs_posted')}{(' obs_on_bev=' + str(ne.get('kgs_obs_on_bev'))) if ne.get('kgs_obs_on_bev') is not None else ''}"
          f" · h3b: zenodo={ne['h3b_zenodo']} ({ne['h3b_zenodo_gb']} GB)"
          + (f" +sweep={ne['h3b_sweep']}" if ne.get('h3b_sweep') else "") + " "
          f"disk={ne['h3b_disk']} ({ne['h3b_disk_gb']} GB)")
    if ne.get("bevdirect_reporting") is not None:
        l2 += f" · bevdirect_ready={ne.get('bevdirect_ready')}/{ne.get('bevdirect_reporting')}"
        if ne.get("bevdirect_tags"):
            l2 += " [" + " ".join(f"{k}={v}" for k, v in sorted(ne["bevdirect_tags"].items(), key=str)) + "]"
    fz = ne.get("frozen") or {}
    if fz.get("algo"):
        l2 += f" · frozen {fz.get('algo')}/{fz.get('lu_source')}@{fz.get('lu_epoch')} until {fz.get('until')}"
    return [l1, l2]


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) > 1 and sys.argv[1] == "audit":
        print(json.dumps(audit_removed(int(sys.argv[2]) if len(sys.argv) > 2 else 20), indent=1))
    elif len(sys.argv) > 1 and sys.argv[1] == "refresh":
        mf = refresh_ne_manifest(0)
        print("built", (mf or {}).get("built"), "of", (mf or {}).get("kgs_total"))
    else:
        for ln in process_txt_lines():
            print(ln)
