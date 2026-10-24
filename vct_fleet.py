"""vct_fleet — director / primary side of the VTC pilot (docs/vct-pilot.md).

Peers run ``vct_pilot.py`` and ship a ~250 B ``status.vct_pilot`` summary
(+ the last 6 compact rows) in every full status push.  This module

* **enrols** pilot peers (``vct_pilot_peers`` knob; ``vct_pilot_auto`` lets
  the director pick that many *frontier* peers whose bevdirect tooling is
  ready — one enrolment per tick, sticky, logged as a director event),
* **stages** the tooling on pilot peers ahead of the fleet pacing and flips
  the sidecar on a *running* pilot peer through ``/api/v1/admin/vct_pilot``
  (no restart — the processor reads it at the next KG's NE step),
* **confirms contributions** at umfeld-at: for every reported pilot KG,
  ``GET /contrib/api/v1/ne/{kg}/report`` must list a report with our observer
  label and digest (≤5 checks / tick, once per KG, cached in
  ``data/austria_processor/vct_fleet.json``),
* **checks the NE round-trip** of each pilot KG: umfeld's manifest
  ``obs.digest`` == the NEO1 digest the primary holds (``ne_ingest`` index),
  and for split parents whether the blocks were pulled, merged complete and
  the merged NEO1 confirmed — the "does the primary assemble split KG
  contributions" check,
* renders the ``vct_pilot:`` block of ``/process.txt`` (fleet bandwidth
  through vtcseamless, agreement vs umfeld, report outcomes, per-KG rows).

No cadastre data passes through here: digests, counts and bytes only.
"""
from __future__ import annotations

import json
import re
import logging
import threading
import time
from pathlib import Path

log = logging.getLogger("vct_fleet")

DATA_DIR = Path("data/austria_processor")
STATE_FILE = DATA_DIR / "vct_fleet.json"
TICK_S = 300
CONFIRM_PER_TICK = 5
CONFIRM_RETRY_S = 1800
FLIP_MIN_INTERVAL_S = 600
STAGE_MIN_INTERVAL_S = 900
ROWS_KEEP = 60
# Ramp health gate: never enrol another peer while the pilot's own 24 h
# window shows failures (needs RAMP_MIN_SAMPLE rows to judge).
RAMP_MIN_OK_RATIO = 0.90
RAMP_MIN_SAMPLE = 3

_lock = threading.Lock()
_last = {"tick": 0.0, "flip": {}, "stage": {}}


# === SECTION: state ===

def _load() -> dict:
    try:
        d = json.loads(STATE_FILE.read_text())
        if isinstance(d, dict):
            d.setdefault("kgs", {})
            return d
    except Exception:
        pass
    return {"kgs": {}, "events": []}


def _save(d: dict) -> None:
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(d, separators=(",", ":"), sort_keys=True))
        tmp.replace(STATE_FILE)
    except Exception as e:  # noqa: BLE001
        log.warning("vct_fleet save: %s", e)


# === SECTION: cumulative /contrib tally (survives ROWS_KEEP pruning) ===
#
# ``state.kgs`` only keeps the newest ROWS_KEEP rows, so "how much have we
# contributed back to umfeld-at" needs its own counter.  ``state.contrib``
# is bumped as rows arrive (``_collect_rows``) and when umfeld confirms a
# report (``_confirm_contrib``); per-UTC-day buckets give the 24h/7d rates.
#   reports   — /contrib POSTs that returned HTTP 200 (digest accepted)
#   confirmed — of those, listed by umfeld under our observer + digest
#   failed    — pilot ran OK but the report did not get a 200
#   codes     — distinct KG codes reported (re-reports don't inflate)
#   cells     — H3 cells covered by accepted reports
TALLY_DAYS_KEEP = 30


def _tally(st: dict) -> dict:
    t = st.get("contrib")
    if not isinstance(t, dict):
        # first run on this state: seed from whatever rows are still held
        t = {"reports": 0, "confirmed": 0, "failed": 0, "cells": 0, "codes": [], "by_day": {},
             "first_ts": None, "last_ts": None}
        st["contrib"] = t
        for r in sorted(st.get("kgs", {}).values(), key=lambda r: int(r.get("ts") or 0)):
            _tally_report(st, r)
            if r.get("contrib") is True:
                _tally_confirm(st, r)
    return t


def _day(ts) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(float(ts or time.time())))


def _bucket(t: dict, ts) -> list:
    bd = t.setdefault("by_day", {})
    b = bd.setdefault(_day(ts), [0, 0, 0])
    if len(bd) > TALLY_DAYS_KEEP:
        for d in sorted(bd)[: len(bd) - TALLY_DAYS_KEEP]:
            bd.pop(d, None)
    return b


def _tally_report(st: dict, r: dict) -> None:
    """A new pilot row landed (new kg, or newer ts/digest for a known kg)."""
    if not r.get("ok"):
        return
    if r.get("http") is None and not r.get("err"):
        # no /contrib attempt: ``skipped`` (fleet-reported / self-dedupe) or a
        # bev-mode block row — neither a report nor a failure
        return
    t = st.get("contrib") if isinstance(st.get("contrib"), dict) else _tally(st)
    b = _bucket(t, r.get("ts"))
    if r.get("http") == 200:
        t["reports"] += 1
        t["cells"] += int(r.get("cells") or 0)
        b[0] += 1
        kg = str(r.get("kg") or "")
        if kg and kg not in t["codes"]:
            t["codes"].append(kg)
        ts = int(r.get("ts") or time.time())
        t["first_ts"] = ts if not t.get("first_ts") else min(t["first_ts"], ts)
        t["last_ts"] = ts if not t.get("last_ts") else max(t["last_ts"], ts)
    else:
        t["failed"] += 1
        b[2] += 1


def _tally_confirm(st: dict, r: dict) -> None:
    t = st.get("contrib") if isinstance(st.get("contrib"), dict) else _tally(st)
    t["confirmed"] += 1
    _bucket(t, r.get("ts"))[1] += 1


def contrib_summary(st: dict | None = None) -> dict:
    st = st if st is not None else _load()
    t = _tally(st)
    bd = t.get("by_day") or {}
    today, yday = _day(time.time()), _day(time.time() - 86400)
    d24 = [0, 0, 0]
    # 24h ≈ today + yesterday's bucket (day granularity; good enough for a rate)
    for d in (today, yday):
        for i, x in enumerate(bd.get(d, [0, 0, 0])):
            d24[i] += x
    last7 = sorted(bd)[-7:]
    d7 = [sum(bd[d][i] for d in last7) for i in range(3)]
    return {"reports": t["reports"], "confirmed": t["confirmed"], "failed": t["failed"],
            "kgs": len(t.get("codes") or []), "cells": t["cells"],
            "reports_48h": d24[0], "confirmed_48h": d24[1], "failed_48h": d24[2],
            "reports_7d": d7[0], "confirmed_7d": d7[1], "failed_7d": d7[2], "days_7d": len(last7),
            "first_ts": t.get("first_ts"), "last_ts": t.get("last_ts")}


def _cfg() -> tuple[list[str], int]:
    """(pilot peers ∪ bev peers, auto).  bev mode implies the pilot."""
    import peer_director as pd
    peers = pd.knob(None, "vct_pilot_peers", []) or []
    auto = int(pd.knob(None, "vct_pilot_auto", 0) or 0)
    return sorted({str(p) for p in peers} | set(_bev_peers())), auto


def _bev_peers() -> list[str]:
    """Peers whose declared NE layer is built locally (``ne_source=bevdirect``
    per peer; NEO1 posted as NEC1+NEO1).  docs/vct-pilot.md → bev mode."""
    import peer_director as pd
    try:
        return sorted({str(p) for p in (pd.knob(None, "vct_bev_peers", []) or [])})
    except Exception:
        return []


def _ramp_per_day() -> int:
    import peer_director as pd
    try:
        return max(0, int(pd.knob(None, "vct_ramp_per_day", 0) or 0))
    except Exception:
        return 0


def _pushes() -> dict[str, dict]:
    """peer_id → fresh pushed status (cross-worker disk mirror)."""
    import peer_director as pd
    out = {}
    try:
        for fp in pd._PEER_PUSH_DIR.glob("*.json"):
            try:
                if time.time() - fp.stat().st_mtime > 1800:
                    continue
                d = json.loads(fp.read_text())
                st = (d or {}).get("status") or {}
                if st:
                    out[fp.stem] = st
            except Exception:
                continue
    except Exception:
        pass
    return out


def _peer_url(peer_id: str) -> str | None:
    import peer_director as pd
    for p in pd.load_peers_config().get("peers", []):
        if p.get("id") == peer_id and p.get("url"):
            return p["url"].rstrip("/")
    return None


def _event(msg: str, peer: str = "director", kg: str = "", level: str = "info") -> None:
    try:
        import app as _app
        _app.director_event(msg, peer=peer, kg=kg, level=level)
    except Exception:
        log.info("%s %s", peer, msg)


# === SECTION: director tick ===

def director_tick(director) -> None:
    now = time.time()
    if now - _last["tick"] < TICK_S:
        return
    if not _lock.acquire(blocking=False):
        return
    _last["tick"] = now
    try:
        pushes = _pushes()
        peers, auto = _cfg()
        peers = _auto_enrol(director, peers, auto, pushes)
        st = _load()
        bev = _ramp_enrol(director, peers, _bev_peers(), pushes, st)
        peers = sorted(set(peers) | set(bev))
        _sync_peers(peers, bev, pushes)
        _tally(st)  # seed the cumulative /contrib tally before new rows land
        _collect_rows(st, pushes)
        _confirm_contrib(st)
        _scrape_umfeld_qc(st)
        _save(st)
    finally:
        _lock.release()


def _tooling_ready(st: dict) -> bool:
    nt = st.get("ne_tooling") or {}
    return bool(nt.get("binary") and nt.get("package"))


def _bev_code(st: dict) -> bool:
    """Peer runs code that knows the NEC1+NEO1 path (its push carries
    ``vct_pilot.ne_source``).  Older code would take ``ne_source=bevdirect``
    down the legacy grid-cell builder + bare NEO1 → 409s, so neither the
    ramp nor the start payload may hand bev mode to such a peer."""
    return (st.get("vct_pilot") or {}).get("ne_source") is not None


def peer_bev_ready(peer_id: str) -> bool:
    """For ``peer_director.start_peer_processor``: bev mode only for a peer
    whose last push shows new code + tooling."""
    st = _pushes().get(peer_id) or {}
    return _bev_code(st) and _tooling_ready(st)


def _frontier_ids(director) -> list[str]:
    try:
        plan = (director.state.get("frontier_strip_plan") or {}) if director is not None else {}
        return sorted(str(k) for k in plan.keys())
    except Exception:
        return []


def _auto_enrol(director, peers: list[str], auto: int, pushes: dict) -> list[str]:
    """Fill ``vct_pilot_peers`` up to ``vct_pilot_auto`` with running peers —
    frontiers first (fresh KGs exercise the NE step), then every other
    processing peer (cache-only peers run the ``ne_declared`` step on their
    KGs too; since 2026-10-08 the whole fleet reports, ``vct_pilot_auto`` ≥
    fleet size).  One enrolment per tick; ready tooling first, otherwise
    stage tooling on one candidate and wait."""
    if auto <= len(peers):
        return peers
    import peer_director as pd
    frontier = _frontier_ids(director)
    cands = [p for p, s in pushes.items() if p not in peers and p != "primary" and not p.startswith("test")
             and s.get("state") in ("running", "processing")]
    cands.sort(key=lambda p: (p not in frontier, p))
    if not cands:
        return peers
    ready = [p for p in cands if _tooling_ready(pushes[p])]
    if ready:
        # frontier first, then lowest steal — the pilot adds ~1-3 min CPU per KG
        def _steal(p):
            try:
                return float(((pushes[p].get("system") or {}).get("cpu_steal")) or 0)
            except Exception:
                return 0.0
        pick = sorted(ready, key=lambda p: (p not in frontier, _steal(p)))[0]
        new = sorted(set(peers) | {pick})
        try:
            pd.set_director_config({"vct_pilot_peers": new})
        except Exception as e:  # noqa: BLE001
            log.warning("vct_fleet enrol %s: %s", pick, e)
            return peers
        _event(f"vct_pilot: enrolled {pick} ({'frontier' if pick in frontier else 'cache'}, tooling ready; auto={auto}, pilot={len(new)})", peer=pick)
        return new
    # nobody ready: stage tooling on one frontier (own pacing, independent of
    # the fleet-wide 10-min stager in app._maybe_stage_ne_tooling)
    for p in cands:
        if time.time() - _last["stage"].get(p, 0) < STAGE_MIN_INTERVAL_S:
            continue
        url = _peer_url(p)
        if not url:
            continue
        _last["stage"][p] = time.time()

        def _go(url=url, p=p):
            try:
                import requests
                r = requests.post(url + "/api/v1/admin/ne_tooling", headers=pd._admin_headers(), timeout=(3, 10))
                log.info("vct_fleet: staging tooling on frontier %s -> HTTP %s", p, r.status_code)
            except Exception as e:  # noqa: BLE001
                log.debug("vct_fleet stage %s: %s", p, e)
        threading.Thread(target=_go, daemon=True, name=f"vct-stage-{p}").start()
        break
    return peers


# === SECTION: daily ramp into bev mode (docs/vct-pilot.md → Ramp) ===

def _ramp_candidates(director, bev: list[str], pilot: list[str], pushes: dict) -> list[str]:
    """Peers that could switch to bev mode now: fresh push, bevdirect tooling
    ready (pin), not primary, not already in bev.  Order: proven pilot peers
    (≥1 ok row in 24 h) first, then frontier peers (fresh KGs exercise the
    NE step), then lowest CPU steal."""
    frontier = set(_frontier_ids(director))

    def _steal(p):
        try:
            return float(((pushes[p].get("system") or {}).get("cpu_steal")) or 0)
        except Exception:
            return 0.0

    def _proven(p):
        vp = pushes[p].get("vct_pilot") or {}
        return p in pilot and int(vp.get("ok_24h") or 0) > 0
    cands = [p for p, s in pushes.items()
             if p not in bev and p != "primary" and not p.startswith("test")
             and _tooling_ready(s) and _bev_code(s)]
    return sorted(cands, key=lambda p: (not _proven(p), p not in frontier, _steal(p), p))


def _ramp_health(peers: list[str], pushes: dict) -> str | None:
    """Reason to HOLD the ramp, else None.  Two signals:
    (1) the enrolled peers' own 24 h pilot windows (the ``kgs_24h ok=`` chip)
    — a failing viewport/build on the peers already switched;
    (2) the primary's NEC1+NEO1 round-trip (``ne_ingest.confirm_umfeld``):
    any container post that umfeld refused (400/409) in the last 24 h, or
    confirmed bev products below RAMP_MIN_OK_RATIO once ≥RAMP_MIN_SAMPLE
    are checked."""
    n = ok = 0
    for pid in peers:
        vp = (pushes.get(pid) or {}).get("vct_pilot") or {}
        n += int(vp.get("n_24h") or 0)
        ok += int(vp.get("ok_24h") or 0)
    if n >= RAMP_MIN_SAMPLE and ok < RAMP_MIN_OK_RATIO * n:
        return f"pilot ok={ok}/{n} in 24h < {RAMP_MIN_OK_RATIO:.0%}"
    try:
        import ne_ingest
        v = ne_ingest.status()
        fa = v.get("bev_post_fail_at")
        msg = str(v.get("bev_post_fail_msg") or "")
        # Only a *refusal* (400/409 = umfeld rejected our container) is a
        # ramp signal.  5xx / connection errors are umfeld restarting —
        # ne_ingest retries those on its own backoff ladder.
        refused = bool(re.search(r"\b4(00|09|22)\b", msg))
        if fa and refused and time.time() - float(fa) < 86400:
            # no elapsed time in the text: the hold string is compared tick
            # to tick and must stay stable (else one warning per tick)
            at = time.strftime("%m-%d %H:%M", time.gmtime(float(fa)))
            return f"umfeld refused a NEC1+NEO1 post at {at}Z ({msg[:60]})"
        um = v.get("umfeld") or {}
        bc, bo = int(um.get("bev_checked") or 0), int(um.get("bev_confirmed") or 0)
        if bc >= RAMP_MIN_SAMPLE and bo < RAMP_MIN_OK_RATIO * bc:
            return f"bev products confirmed {bo}/{bc} < {RAMP_MIN_OK_RATIO:.0%}"
    except Exception as e:  # noqa: BLE001
        log.debug("vct_fleet ramp health (ne_ingest): %s", e)
    return None


def _ramp_enrol(director, pilot: list[str], bev: list[str], pushes: dict, st: dict) -> list[str]:
    """``vct_ramp_per_day`` > 0: move ONE eligible peer into ``vct_bev_peers``
    every 24h/N, sticky, until no candidate is left.  Holds (no enrolment,
    reason on the dashboard) while the pilot / round-trip is unhealthy.
    State in ``vct_fleet.json["ramp"]`` so it survives srv restarts / HA
    handover.  Returns the (possibly grown) bev list."""
    per_day = _ramp_per_day()
    ramp = st.setdefault("ramp", {})
    if per_day <= 0:
        if ramp.get("active"):
            ramp.update(active=False, hold=None)
        return bev
    import peer_director as pd
    now = time.time()
    interval = 86400.0 / per_day
    ramp.update(active=True, per_day=per_day, interval_s=int(interval))
    cands = _ramp_candidates(director, bev, pilot, pushes)
    ramp["candidates"] = len(cands)
    fleet = {p for p in pushes if p != "primary" and not p.startswith("test")}
    if fleet and fleet <= set(bev) and not cands:
        # ramp complete: nothing left to enrol, so the health gate has
        # nothing to hold — don't keep warning about umfeld hiccups
        if ramp.get("hold"):
            ramp["hold"] = None
        if not ramp.get("complete_at"):
            ramp["complete_at"] = now
            _event(f"vct_pilot: ramp complete — every peer is in bev mode ({len(bev)})")
        return bev
    hold = _ramp_health(sorted(set(pilot) | set(bev)), pushes)
    if hold != ramp.get("hold"):
        if hold:
            _event(f"vct_pilot: ramp HOLD — {hold} (bev={len(bev)})", level="warning")
        elif ramp.get("hold"):
            _event(f"vct_pilot: ramp resumed (bev={len(bev)})")
        ramp["hold"] = hold
    if hold:
        return bev
    last = float(ramp.get("last_at") or 0)
    if not last:
        if bev:
            # bev peers already exist (operator seeded the first one): the
            # next one joins a full interval from now
            ramp["last_at"] = now
            return bev
        # nothing in bev mode yet: switch the first peer right away
    elif now - last < interval:
        return bev
    if not cands:
        if fleet and fleet <= set(bev):
            if not ramp.get("complete_at"):
                ramp["complete_at"] = now
                _event(f"vct_pilot: ramp complete — every peer is in bev mode ({len(bev)})")
        else:
            # peers exist that are not eligible yet (old code / tooling
            # missing) — wait, the next tick re-evaluates
            ramp["waiting"] = f"{len(fleet - set(bev))} peer(s) not eligible (code/tooling)"
        return bev
    ramp.pop("complete_at", None)
    ramp.pop("waiting", None)
    pick = cands[0]
    new = sorted(set(bev) | {pick})
    try:
        pd.set_director_config({"vct_bev_peers": new})
    except Exception as e:  # noqa: BLE001
        log.warning("vct_fleet ramp enrol %s: %s", pick, e)
        return bev
    ramp["last_at"] = now
    ramp.setdefault("enrolled", []).append([int(now), pick])
    ramp["enrolled"] = ramp["enrolled"][-120:]
    _event(f"vct_pilot: ramp → bev mode on {pick} ({len(new)} in bev, {len(cands) - 1} eligible left; "
           f"next in {interval / 3600:.0f}h)", peer=pick)
    return new


def ramp_status(bev: list[str], pushes: dict, st: dict | None = None) -> dict:
    st = st if st is not None else _load()
    ramp = dict(st.get("ramp") or {})
    per_day = _ramp_per_day()
    out = {"per_day": per_day, "active": per_day > 0, "bev": len(bev),
           "fleet": len([p for p in pushes if p != "primary" and not p.startswith("test")]),
           "hold": ramp.get("hold"), "candidates": ramp.get("candidates"), "waiting": ramp.get("waiting"),
           "complete": bool(ramp.get("complete_at")), "history": ramp.get("enrolled") or []}
    if per_day > 0:
        last = float(ramp.get("last_at") or 0)
        out["next_in_s"] = max(0, int(last + 86400.0 / per_day - time.time())) if last else None
    return out


def _sync_peers(peers: list[str], bev: list[str], pushes: dict) -> None:
    """Make every peer's sidecars agree with the lists, live: pilot sidecar
    (``vct_pilot``) for ``peers``; ``ne_source`` bevdirect|umfeld for
    ``bev`` (only on peers whose push reports ``vct_pilot.ne_source`` — i.e.
    new enough code to take the flag)."""
    import peer_director as pd
    for pid, st in pushes.items():
        if pid == "primary":
            continue
        want = pid in peers
        vp = st.get("vct_pilot") or {}
        have = bool(vp.get("enabled"))
        want_src = "bevdirect" if pid in bev else "umfeld"
        have_src = vp.get("ne_source")          # None on old code → never flip the source
        src_due = have_src is not None and have_src != want_src
        if src_due and want_src == "umfeld":
            # NEVER downgrade: umfeld's /k cadastre routes are 410 since the
            # Kataster purge, so an umfeld peer only produces cadastre-less
            # KGs.  An empty bev list here is a config glitch (the 2026-10-15
            # shadow takeover without director_config.json), not an intent.
            if not _last.get("_no_downgrade_warned"):
                _last["_no_downgrade_warned"] = True
                _event(f"vct_pilot: refusing to flip {pid} (and others) back to ne_source=umfeld — "
                       f"bev list has {len(bev)} peers; umfeld cadastre is retired", level="warning")
            src_due = False
        if want == have and not src_due:
            continue
        if (want or want_src == "bevdirect") and not _tooling_ready(st):
            # stage first; the flip waits for the next push that says ready
            if time.time() - _last["stage"].get(pid, 0) >= STAGE_MIN_INTERVAL_S:
                _last["stage"][pid] = time.time()
                url = _peer_url(pid)
                if url:
                    def _stage(url=url, pid=pid):
                        try:
                            import requests
                            r = requests.post(url + "/api/v1/admin/ne_tooling", headers=pd._admin_headers(), timeout=(3, 10))
                            log.info("vct_fleet: staging tooling on pilot %s -> HTTP %s", pid, r.status_code)
                        except Exception as e:  # noqa: BLE001
                            log.debug("vct_fleet stage %s: %s", pid, e)
                    threading.Thread(target=_stage, daemon=True, name=f"vct-stage-{pid}").start()
            continue
        if time.time() - _last["flip"].get(pid, 0) < FLIP_MIN_INTERVAL_S:
            continue
        url = _peer_url(pid)
        if not url:
            continue
        _last["flip"][pid] = time.time()
        body = {"on": want}
        if src_due:
            body["ne_source"] = want_src

        def _flip(url=url, pid=pid, want=want, body=body):
            try:
                import requests
                r = requests.post(url + "/api/v1/admin/vct_pilot", json=body, headers=pd._admin_headers(),
                                  timeout=(3, 15))
                j = r.json() if r.ok else {}
                if r.ok and j.get("changed"):
                    _event(f"vct_pilot: {'ON' if want else 'OFF'} on {pid} (live sidecar, no restart)", peer=pid)
                if r.ok and j.get("ne_source_changed"):
                    _event(f"vct_pilot: ne_source={body.get('ne_source')} on {pid} (live sidecar; "
                           f"next KG builds its declared layer {'locally' if body.get('ne_source') == 'bevdirect' else 'from umfeld lu'})",
                           peer=pid)
                if r.ok and j.get("refused"):
                    log.info("vct_fleet: %s refused: %s", pid, j.get("refused"))
                elif r.status_code == 404:
                    log.info("vct_fleet: %s still on old code (no admin/vct_pilot)", pid)
                elif not r.ok:
                    log.info("vct_fleet: flip %s -> HTTP %s", pid, r.status_code)
            except Exception as e:  # noqa: BLE001
                log.debug("vct_fleet flip %s: %s", pid, e)
        threading.Thread(target=_flip, daemon=True, name=f"vct-flip-{pid}").start()


REPORTED_KEEP_S = 30 * 86400


def _note_reported(st: dict, r: dict, pid: str) -> None:
    """30-day map of parents reported to umfeld /contrib (HTTP 200, a
    digest, not a skipped row) → ``GET /api/v1/director/vct_reported``."""
    if not (r.get("ok") and r.get("digest") and int(r.get("http") or 0) == 200) or r.get("skipped") or r.get("skip_rep"):
        return
    if r.get("chg_ok") is False:
        # the report landed but umfeld's wanted chunk rows did not — not a
        # complete contribution; a sibling block / next pass sends them again
        return
    kg = str(r.get("kg") or "")
    rep = st.setdefault("reported", {})
    cur = rep.get(kg)
    ts = int(r.get("ts") or 0)
    # distinct observers of the same source → umfeld's change confirmation
    # ("a second observer confirms at once"); vct_pilot._report_policy stops
    # re-reporting a parent once two observers hold it
    observers = set((cur or {}).get("observers") or ([cur["observer"]] if cur and cur.get("observer") else []))
    if cur and _src_class(cur.get("source")) != _src_class(r.get("src")):
        observers = set()
    if r.get("obs"):
        observers.add(r.get("obs"))
    # umfeld §5a: tile hashes travel only with a chunk-bearing report; once
    # one landed for this parent the policy goes back to the header probe
    prov = bool((cur or {}).get("prov")) or bool(r.get("tq") and r.get("full"))
    if cur and int(cur.get("ts") or 0) >= ts:
        cur["observers"] = sorted(observers)[:8]
        cur["n_obs"] = len(observers)
        cur["prov"] = prov
        return
    rep[kg] = {"ts": ts, "peer": pid, "observer": r.get("obs"), "digest": r.get("digest"), "source": r.get("src"),
               "observers": sorted(observers)[:8], "n_obs": len(observers), "prov": prov}
    now = time.time()
    for k in [k for k, v in rep.items() if now - int(v.get("ts") or 0) > REPORTED_KEEP_S]:
        rep.pop(k, None)


def _src_class(src) -> str:
    """umfeld keys reports by source CLASS: every ``bevdirect@*`` tag is one
    class (the step-0 key, ``bev_equivalent_tags``), so a binary bump must
    not make the fleet forget who reported a parent."""
    s = str(src or "")
    return "bevdirect" if s.startswith("bevdirect@") else s


def reported(kg: str, source: str, max_age_s: float = REPORTED_KEEP_S) -> dict | None:
    kg = str(kg).zfill(5)
    try:
        st = _load()
        v = (st.get("reported") or {}).get(kg)
        if v and _src_class(v.get("source")) == _src_class(source) and time.time() - int(v.get("ts") or 0) < max_age_s:
            out = dict(v)
            out["received_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(v.get("ts") or 0)))
            return out
    except Exception:
        pass
    return None


UMFELD_QC_EVERY_S = 1800    # public /api/v1/ne/stats, ~1 KB gzipped — national change.qc


def _scrape_umfeld_qc(st: dict) -> None:
    """umfeld's national ``change.qc`` (§5a: cells_jitter / cells_pipeline /
    cluster_share / versions_with_tile_hash) + confirmed/candidate cell
    counts → ``state.umfeld_qc`` (the ``qc(umfeld)`` line on /process.txt).
    Public tier, tokenless, one GET per 30 min."""
    cur = st.get("umfeld_qc") or {}
    if time.time() - float(cur.get("ts") or 0) < UMFELD_QC_EVERY_S:
        return
    try:
        import requests
        import umfeld
        r = requests.get(f"https://{umfeld.HOST}/api/v1/ne/stats", timeout=(3, 20),
                         headers={"User-Agent": "srtm-lidar-at/2.4 vct_fleet", "Accept-Encoding": "gzip"})
        r.raise_for_status()
        ch = (r.json() or {}).get("change") or {}
        qc = ch.get("qc") or {}
        st["umfeld_qc"] = {"ts": time.time(), "since": ch.get("since"),
                           "confirmed_cells": (ch.get("confirmed") or {}).get("cells"),
                           "candidate_cells": (ch.get("candidate") or {}).get("cells"),
                           "cells_changed": ch.get("cells_changed"),
                           **{k: qc.get(k) for k in ("cells_pipeline", "cells_jitter", "cells_clustered", "cells_isolated",
                                                     "cluster_share", "versions_with_tile_hash", "versions")}}
    except Exception as e:  # noqa: BLE001
        st["umfeld_qc"] = dict(cur, ts=time.time(), error=str(e)[:100])


def _collect_rows(st: dict, pushes: dict) -> None:
    """Fold the peers' recent rows into ``state.kgs`` (keyed by kg; newest wins)."""
    kgs = st.setdefault("kgs", {})
    for pid, s in pushes.items():
        vp = s.get("vct_pilot") or {}
        for r in vp.get("recent") or []:
            kg = str(r.get("kg") or "")
            if not kg:
                continue
            _note_reported(st, r, pid)
            cur = kgs.get(kg)
            # newest row wins fleet-wide.  The old rule only skipped stale rows
            # from the *same* peer, so a KG seen by two peers (split blocks,
            # fleet-reported skips) flip-flopped every tick and bumped the
            # tally each time (Oct 8-9: ~20k "reports"/day for ~270 real KGs).
            if cur and int(cur.get("ts") or 0) >= int(r.get("ts") or 0):
                continue
            rec = dict(r, peer=pid)
            if cur and cur.get("digest") == rec.get("digest"):
                for k in ("contrib", "contrib_at", "contrib_err", "contrib_next"):
                    if k in cur:
                        rec[k] = cur[k]
            # a report with no digest (skipped / block row) must not drop a
            # confirmed real report's state either
            elif cur and not rec.get("digest") and cur.get("digest"):
                for k in ("digest", "http", "contrib", "contrib_at", "contrib_err", "contrib_next"):
                    if k in cur:
                        rec[k] = cur[k]
            else:
                _tally_report(st, rec)
            kgs[kg] = rec
    if len(kgs) > ROWS_KEEP:
        for kg in sorted(kgs, key=lambda k: int(kgs[k].get("ts") or 0))[:len(kgs) - ROWS_KEEP]:
            kgs.pop(kg, None)


def _confirm_contrib(st: dict) -> None:
    """Did umfeld store our report?  ``GET /contrib/api/v1/ne/{kg}/report`` lists
    received reports; ours must appear with our observer + digest."""
    import requests
    import umfeld
    n = 0
    now = time.time()
    for kg, r in sorted(st["kgs"].items(), key=lambda kv: -int(kv[1].get("ts") or 0)):
        if not r.get("ok") or r.get("contrib") is True or not r.get("digest"):
            continue
        if float(r.get("contrib_next") or 0) > now or n >= CONFIRM_PER_TICK:
            continue
        n += 1
        try:
            resp = requests.get(f"{umfeld.NE_CONTRIB_BASE}/{kg}/report", headers=umfeld.headers(), timeout=30)
            resp.raise_for_status()
            j = resp.json() or {}
            found = None
            for rep in j.get("reports") or []:
                body = rep.get("report") or {}
                if body.get("digest") == r.get("digest") and body.get("observer") == r.get("obs"):
                    found = rep
                    break
            if found:
                r.update(contrib=True, contrib_at=found.get("received_at"), contrib_token=found.get("token"))
                r.pop("contrib_err", None); r.pop("contrib_next", None)
                _tally_confirm(st, r)
            else:
                r.update(contrib=False, contrib_err=f"not listed ({j.get('count')} reports)", contrib_next=now + CONFIRM_RETRY_S)
        except Exception as e:  # noqa: BLE001
            r.update(contrib=False, contrib_err=str(e)[:100], contrib_next=now + CONFIRM_RETRY_S)


# === SECTION: NE round-trip + split assembly per pilot KG (read-only) ===

def _ne_view(kg: str) -> dict:
    """obs round-trip + split state for a parent from ne_ingest's index, the
    product manifest and umfeld's cached manifest.  Pure reads."""
    out: dict = {}
    try:
        import ne_ingest as nei
        idx = nei.load_index_ro()
        entries = nei.load_manifest_entries()
        live = nei._live_blocks(kg, entries)
        f = idx.get("files", {}).get(kg) or {}
        p = idx.get("parents", {}).get(kg) or {}
        um = idx.get("umfeld", {}).get(kg) or {}
        if live:
            have = [b for b in live if idx.get("files", {}).get(b)]
            out["split"] = {"blocks": len(live), "ingested": len(have), "merged": bool(p.get("digest")),
                            "complete": bool(p.get("complete")), "cells_observed": p.get("cells_observed"),
                            "cells_n": p.get("cells_n"), "merge_error": p.get("merge_error")}
            ours = p.get("digest") if p.get("complete") else None
        else:
            out["split"] = None
            ours = f.get("neo1_digest")
            if not ours:
                out["h3b"] = "uploaded" if nei._committed(entries.get(f"{kg}_ne_h3b")) else "missing"
        out["ours"] = ours
        out["confirmed"] = bool(ours) and um.get("confirmed_digest") == ours
        if um.get("post_error"):
            out["post_error"] = um["post_error"][:60]
        if um.get("bev"):
            b = um["bev"]
            out["bev"] = {"ok": bool(b.get("nec1")) and b.get("nec1") == b.get("umfeld_bev") == b.get("obs_declared"),
                          "lu_diff": um.get("lu_diff")}
    except Exception as e:  # noqa: BLE001
        out["error"] = str(e)[:80]
    try:
        import ne_rollout
        mf = (__import__("json_cache").load(ne_rollout.NE_MANIFEST_CACHE) or {}).get("manifest") or {}
        rec = (mf.get("kgs") or {}).get(kg) or {}
        out["umfeld_obs"] = ((rec.get("obs") or {}).get("digest")) if isinstance(rec, dict) else None
        out["umfeld_lu_cells"] = ((rec.get("lu") or {}).get("cells_n")) if isinstance(rec, dict) else None
        if out.get("ours") and out["umfeld_obs"] == out["ours"]:
            out["confirmed"] = True
    except Exception:
        pass
    return out


def split_audit() -> dict:
    """Fleet-wide check that the primary assembles split parents correctly:
    counts over ne_ingest's merged parents + the ones whose merged NEO1 cell
    set disagrees with umfeld's lu cell count.  That equality only holds
    when the merged NEC1 *is* umfeld's lu (blocks carry the parent's full
    cell set) → ``cells_mismatch`` (loud).  A ``bevdirect@`` NEC1 is built
    per peer from BEV tiles and differs from the frozen index layer by
    design (documented source noise, e.g. 75110: 168 243 vs 168 225) →
    counted quietly as ``bev_lu_delta`` (docs/vct-pilot.md)."""
    out = {"parents": 0, "complete": 0, "partial": 0, "confirmed": 0, "cells_mismatch": [], "merge_errors": 0,
           "awaiting_blocks": 0, "nec1_disagree": 0, "bev_lu_delta": 0}
    try:
        import ne_ingest as nei
        import ne_rollout
        idx = nei._load_index()
        try:
            mf = (__import__("json_cache").load(ne_rollout.NE_MANIFEST_CACHE) or {}).get("manifest") or {}
        except Exception:
            mf = {}
        kgs = mf.get("kgs") or {}
        um = idx.get("umfeld", {})
        for parent, p in idx.get("parents", {}).items():
            out["parents"] += 1
            if p.get("merge_error"):
                out["merge_errors"] += 1
            if p.get("nec1_disagree"):
                out["nec1_disagree"] += 1
            if not p.get("digest"):
                out["awaiting_blocks"] += 1
                continue
            if p.get("complete"):
                out["complete"] += 1
                if (um.get(parent) or {}).get("confirmed_digest") == p.get("digest"):
                    out["confirmed"] += 1
                lu = ((kgs.get(parent) or {}).get("lu") or {}) if isinstance(kgs.get(parent), dict) else {}
                if lu.get("cells_n") and p.get("cells_n") and int(lu["cells_n"]) != int(p["cells_n"]):
                    if str(p.get("nec1_source") or "").startswith("bevdirect@"):
                        out["bev_lu_delta"] += 1
                    else:
                        out["cells_mismatch"].append(f"{parent}:{p['cells_n']}!={lu['cells_n']}")
            else:
                out["partial"] += 1
    except Exception as e:  # noqa: BLE001
        out["error"] = str(e)[:80]
    return out


# === SECTION: status + /process.txt ===

def status() -> dict:
    peers, auto = _cfg()
    bev = _bev_peers()
    pushes = _pushes()
    st = _load()
    per_peer = {}
    for pid in sorted(set(peers) | {p for p, s in pushes.items()
                                     if (s.get("vct_pilot") or {}).get("enabled")
                                     or (s.get("vct_pilot") or {}).get("ne_source") == "bevdirect"}):
        s = pushes.get(pid) or {}
        vp = s.get("vct_pilot") or {}
        per_peer[pid] = {"in_pilot": pid in peers, "enabled": bool(vp.get("enabled")), "tooling": _tooling_ready(s),
                         "in_bev": pid in bev, "ne_source": vp.get("ne_source"),
                         "declared_24h": vp.get("declared_24h", 0),
                         "state": s.get("state"), "n_24h": vp.get("n_24h", 0), "ok_24h": vp.get("ok_24h", 0),
                         "fail_24h": vp.get("fail_24h", 0), "net_mb_24h": vp.get("net_mb_24h", 0.0),
                         "tile_mb_24h": vp.get("tile_mb_24h", 0.0), "requests_24h": vp.get("requests_24h", 0),
                         "net_mb_total": vp.get("net_mb_total", 0.0), "pushed": bool(s),
                         # change protocol (vtcseamless-py 0.2.0)
                         "ap_24h": vp.get("ap_24h", 0), "chg_want_24h": vp.get("chg_want_24h", 0),
                         "chg_stored_24h": vp.get("chg_stored_24h", 0), "chg_rejected_24h": vp.get("chg_rejected_24h", 0),
                         "chg_deltas_24h": vp.get("chg_deltas_24h", 0), "chg_fail_24h": vp.get("chg_fail_24h", 0),
                         "chg_mb_24h": vp.get("chg_mb_24h", 0.0), "hdr_unchanged_24h": vp.get("hdr_unchanged_24h", 0),
                         "chunks_confirmed_24h": vp.get("chunks_confirmed_24h", 0),
                         "hdr_need_24h": vp.get("hdr_need_24h", 0), "hdr_anomaly_24h": vp.get("hdr_anomaly_24h", 0),
                         "self_dedupe_24h": vp.get("self_dedupe_24h", 0),
                         # QC (umfeld §5a / vtcseamless-py 0.3.0)
                         "prov_24h": vp.get("prov_24h", 0), "prov_missing_24h": vp.get("prov_missing_24h", 0),
                         "tiles_incomplete_24h": vp.get("tiles_incomplete_24h", 0),
                         "qc_same_input_24h": vp.get("qc_same_input_24h", 0),
                         "qc_same_kgs_24h": vp.get("qc_same_kgs_24h") or [],
                         "deltas_pipeline_24h": vp.get("deltas_pipeline_24h", 0),
                         "deltas_jitter_24h": vp.get("deltas_jitter_24h", 0),
                         "full_once_24h": vp.get("full_once_24h", 0)}
    kgs = []
    for kg, r in sorted(st.get("kgs", {}).items(), key=lambda kv: -int(kv[1].get("ts") or 0)):
        kgs.append(dict(r, ne=_ne_view(kg)))
    try:
        import ne_ingest
        _um = (ne_ingest.status() or {}).get("umfeld") or {}
        bev_rt = {k: _um.get(k) for k in ("bev_checked", "bev_confirmed", "bev_declared_mismatch", "bev_post_failed")}
    except Exception:
        bev_rt = {}
    return {"peers": per_peer, "auto": auto, "configured": peers, "bev_peers": bev, "kgs": kgs,
            "contrib": contrib_summary(st), "split_audit": split_audit(), "ramp": ramp_status(bev, pushes, st), "bev_roundtrip": bev_rt,
            "fleet": {"net_mb_24h": round(sum(v["net_mb_24h"] or 0 for v in per_peer.values()), 1),
                      "tile_mb_24h": round(sum(v["tile_mb_24h"] or 0 for v in per_peer.values()), 1),
                      "requests_24h": sum(v["requests_24h"] or 0 for v in per_peer.values()),
                      "net_mb_total": round(sum(v["net_mb_total"] or 0 for v in per_peer.values()), 1),
                      "kgs_24h": sum(v["n_24h"] or 0 for v in per_peer.values()),
                      "ok_24h": sum(v["ok_24h"] or 0 for v in per_peer.values()),
                      "declared_24h": sum(v["declared_24h"] or 0 for v in per_peer.values()),
                      "ap_24h": sum(v["ap_24h"] or 0 for v in per_peer.values()),
                      "chg_want_24h": sum(v["chg_want_24h"] or 0 for v in per_peer.values()),
                      "chg_stored_24h": sum(v["chg_stored_24h"] or 0 for v in per_peer.values()),
                      "chg_rejected_24h": sum(v["chg_rejected_24h"] or 0 for v in per_peer.values()),
                      "chg_deltas_24h": sum(v["chg_deltas_24h"] or 0 for v in per_peer.values()),
                      "chg_fail_24h": sum(v["chg_fail_24h"] or 0 for v in per_peer.values()),
                      "chg_mb_24h": round(sum(v["chg_mb_24h"] or 0 for v in per_peer.values()), 2),
                      "hdr_unchanged_24h": sum(v["hdr_unchanged_24h"] or 0 for v in per_peer.values()),
                      "chunks_confirmed_24h": sum(v.get("chunks_confirmed_24h") or 0 for v in per_peer.values()),
                      "hdr_need_24h": sum(v.get("hdr_need_24h") or 0 for v in per_peer.values()),
                      "hdr_anomaly_24h": sum(v.get("hdr_anomaly_24h") or 0 for v in per_peer.values()),
                      "self_dedupe_24h": sum(v.get("self_dedupe_24h") or 0 for v in per_peer.values()),
                      "confirmed_2obs": sum(1 for v in (st.get("reported") or {}).values() if int(v.get("n_obs") or 0) >= 2),
                      "reported_30d": len(st.get("reported") or {}),
                      "reported_prov": sum(1 for v in (st.get("reported") or {}).values() if v.get("prov")),
                      **{k: sum(int(v.get(k) or 0) for v in per_peer.values())
                         for k in ("prov_24h", "prov_missing_24h", "tiles_incomplete_24h", "qc_same_input_24h",
                                   "deltas_pipeline_24h", "deltas_jitter_24h", "full_once_24h")},
                      "qc_same_kgs_24h": [dict(q, peer=pid) for pid, v in per_peer.items()
                                          for q in (v.get("qc_same_kgs_24h") or [])][:12]},
            "umfeld_qc": st.get("umfeld_qc") or {}}


def _chg_tag(r: dict) -> str:
    """Row tail for the change protocol: '' for pre-0.2.0 rows / nothing asked."""
    if not r.get("ap") or not (r.get("chg_want") or r.get("hdr")):
        return ""
    conf = f" confirmed={r['confirmed']}" if r.get("confirmed") else ""
    if r.get("hdr") == 200 and not r.get("chg_want"):
        return " chg:hdr-unchanged" + conf
    t = f" chg:want={r.get('chg_want')} stored={r.get('chg_stored')}" + conf
    if r.get("chg_deltas"):
        t += f" deltas={r.get('chg_deltas')}"
    if r.get("chg_ok") is False:
        t += " FAIL"
    if r.get("d_pipe") or r.get("d_jit"):
        t += f" (pipeline={r.get('d_pipe') or 0} jitter={r.get('d_jit') or 0} not-change)"
    return t


def _qc_kgs(qk: list) -> str:
    """``kg@peer:n@W,S,E,N`` for the qc_same_input KGs (what umfeld asks to see)."""
    parts = []
    for q in qk[:4]:
        t = f"{q.get('kg')}@{q.get('peer')}:{q.get('n')}"
        if q.get("bbox"):
            t += "@" + ",".join(f"{float(b):.3f}" for b in q["bbox"])
        parts.append(t)
    return " ".join(parts)


def _hm(ts) -> str:
    try:
        return time.strftime("%m-%d %H:%M", time.gmtime(float(ts)))
    except Exception:
        return "?"


def process_txt_lines(max_rows: int = 8) -> list[str]:
    v = status()
    pp = v["peers"]
    if not pp and not v["kgs"]:
        if v["auto"]:
            return [f"vct_pilot: (no peer enrolled yet; auto={v['auto']} — waiting for a frontier with bevdirect tooling)"]
        return ["vct_pilot: off (POST /api/v1/director/config {\"vct_pilot_peers\":[\"atNN\"]} or {\"vct_pilot_auto\":1}; docs/vct-pilot.md)"]
    def _ptag(d):
        # bev = declared layer built locally (product switched); bev? = in
        # the list, sidecar not acked yet; on = telemetry-only pilot
        if d.get('in_bev'):
            return 'bev' if d.get('ne_source') == 'bevdirect' else ('tooling?' if not d['tooling'] else 'bev?')
        if d.get('ne_source') == 'bevdirect':
            return 'bev,not-configured'
        return ('on' if d['enabled'] else ('tooling?' if not d['tooling'] else 'pending'))
    if len(pp) <= 6:
        ptxt = " ".join(
            f"{pid}({_ptag(d)}"
            f"{'' if d['in_pilot'] else ',not-configured'}{',' + str(d['state']) if d['state'] not in ('running', 'processing') else ''})"
            for pid, d in pp.items())
    else:
        # fleet-scale: histogram + only the peers that need attention
        hist: dict[str, int] = {}
        for d in pp.values():
            hist[_ptag(d)] = hist.get(_ptag(d), 0) + 1
        odd = [f"{pid}({_ptag(d)}{'' if d['in_pilot'] else ',not-configured'})"
               for pid, d in pp.items() if _ptag(d) not in ('on', 'bev') or not d['in_pilot']]
        ptxt = f"{len(pp)}[" + " ".join(f"{k}={n}" for k, n in sorted(hist.items())) + "]"
        if odd:
            ptxt += " " + " ".join(odd[:6]) + (f" +{len(odd) - 6}" if len(odd) > 6 else "")
    rp = v.get("ramp") or {}
    if rp.get("active"):
        if rp.get("hold"):
            rtxt = f" ramp={rp['per_day']}/d HOLD({rp['hold']}) bev={rp['bev']}/{rp['fleet']}"
        elif rp.get("complete"):
            rtxt = f" ramp={rp['per_day']}/d complete bev={rp['bev']}/{rp['fleet']}"
        else:
            nx = rp.get("next_in_s")
            rtxt = (f" ramp={rp['per_day']}/d bev={rp['bev']}/{rp['fleet']} eligible={rp.get('candidates')} "
                    f"next={'?' if nx is None else (f'{nx / 3600:.1f}h' if nx >= 3600 else f'{nx // 60}m')}"
                    f"{' waiting(' + rp['waiting'] + ')' if rp.get('waiting') else ''}")
    elif rp.get("bev"):
        rtxt = f" bev={rp['bev']}/{rp['fleet']} (ramp off)"
    else:
        rtxt = ""
    brt = v.get("bev_roundtrip") or {}
    if brt.get("bev_checked"):
        rtxt += (f" bev_obs={brt.get('bev_confirmed')}/{brt.get('bev_checked')}"
                 f"{' DECL-MISMATCH=' + str(brt.get('bev_declared_mismatch')) if brt.get('bev_declared_mismatch') else ''}"
                 f"{' POST-FAILED=' + str(brt.get('bev_post_failed')) if brt.get('bev_post_failed') else ''}")
    f = v["fleet"]
    sa = v["split_audit"]
    kgs = v["kgs"]
    contrib_ok = sum(1 for r in kgs if r.get("contrib") is True)
    contrib_n = sum(1 for r in kgs if r.get("ok"))
    obs_ok = sum(1 for r in kgs if (r.get("ne") or {}).get("confirmed"))
    _d1 = time.time() - 86400
    suspect_n = sum(1 for r in kgs if r.get("suspect") and int(r.get("ts") or 0) > _d1)
    partial_n = sum(1 for r in kgs if r.get("partial") and int(r.get("ts") or 0) > _d1)
    l1 = (f"vct_pilot: peers={ptxt} auto={v['auto']}{rtxt} · kgs_24h={f['kgs_24h']} ok={f['ok_24h']}"
          f"{' declared=' + str(f['declared_24h']) if f.get('declared_24h') else ''} "
          f"· bw(vtcseamless): net~{f['net_mb_24h']}MB/24h tiles={f['tile_mb_24h']}MB req={f['requests_24h']} "
          f"total~{f['net_mb_total']}MB · contrib_confirmed={contrib_ok}/{contrib_n} obs_confirmed={obs_ok}/{len(kgs)}"
          f"{' change_suspect_24h=' + str(suspect_n) if suspect_n else ''}"
          f"{' partial_viewport_24h=' + str(partial_n) if partial_n else ''}")
    l2 = (f"    split_assembly(fleet): parents={sa['parents']} complete={sa['complete']} confirmed={sa['confirmed']} "
          f"partial={sa['partial']} awaiting_blocks={sa['awaiting_blocks']} merge_errors={sa['merge_errors']}")
    if sa.get("nec1_disagree"):
        l2 += f" nec1_disagree={sa['nec1_disagree']}"
    if sa.get("bev_lu_delta"):
        l2 += f" bev_lu_delta={sa['bev_lu_delta']}"
    if sa.get("cells_mismatch"):
        l2 += f" CELLS-MISMATCH={len(sa['cells_mismatch'])} [{' '.join(sa['cells_mismatch'][:4])}]"
    c = v.get("contrib") or {}
    lc = (f"    contrib(umfeld-at /contrib): kgs={c.get('kgs', 0)} reports={c.get('reports', 0)} "
          f"confirmed={c.get('confirmed', 0)} failed={c.get('failed', 0)} cells={c.get('cells', 0):,} "
          f"· 48h={c.get('reports_48h', 0)}/{c.get('confirmed_48h', 0)}ok/{c.get('failed_48h', 0)}fail "
          f"· 7d={c.get('reports_7d', 0)} (@{(c.get('reports_7d', 0) / max(1, c.get('days_7d') or 1)):.1f}/d)"
          f"{' first=' + _hm(c['first_ts']) if c.get('first_ts') else ''}"
          f"{' last=' + _hm(c['last_ts']) if c.get('last_ts') else ''}")
    # change protocol line (vtcseamless-py 0.2.0 / ne-cells-2 chunks_ap)
    lch = (f"    change(ne-change ap-1): reports_ap_24h={f.get('ap_24h', 0)}/{f['ok_24h']} "
           f"chunks want={f.get('chg_want_24h', 0)} stored={f.get('chg_stored_24h', 0)} rejected={f.get('chg_rejected_24h', 0)} "
           f"deltas={f.get('chg_deltas_24h', 0)} up~{f.get('chg_mb_24h', 0.0)}MB/24h"
           f"{' STEP2-FAIL=' + str(f['chg_fail_24h']) if f.get('chg_fail_24h') else ''} "
           f"· hdr unchanged={f.get('hdr_unchanged_24h', 0)} need={f.get('hdr_need_24h', 0)}"
           f"{' HDR-ANOMALY=' + str(f['hdr_anomaly_24h']) if f.get('hdr_anomaly_24h') else ''}"
           f" self_dedupe={f.get('self_dedupe_24h', 0)} confirmed_chunks={f.get('chunks_confirmed_24h', 0)} "
           f"· reported_30d={f.get('reported_30d', 0)} "
           f"2obs={f.get('confirmed_2obs', 0)}")
    # QC line (umfeld ne-change.md §5a): fleet-side verdicts that are NOT
    # change + umfeld's national change.qc
    uq = v.get("umfeld_qc") or {}
    qk = f.get("qc_same_kgs_24h") or []
    lqc = (f"    qc(§5a): prov={f.get('prov_24h', 0)}/{f['ok_24h']} full_once={f.get('full_once_24h', 0)} "
           f"reported_prov={f.get('reported_prov', 0)}/{f.get('reported_30d', 0)}"
           f"{' PROV-MISSING=' + str(f['prov_missing_24h']) if f.get('prov_missing_24h') else ''}"
           f" · tiles_incomplete={f.get('tiles_incomplete_24h', 0)}"
           f"{' QC-SAME-INPUT=' + str(f['qc_same_input_24h']) + ' [' + _qc_kgs(qk) + ']' if f.get('qc_same_input_24h') else ''}"
           f" · deltas pipeline={f.get('deltas_pipeline_24h', 0)} jitter={f.get('deltas_jitter_24h', 0)}"
           f" · umfeld(/ne/stats{', ' + _hm(uq.get('ts')) if uq.get('ts') else ''}): "
           + (f"error={uq.get('error')}" if uq.get("error") and uq.get("cells_jitter") is None else
              f"confirmed={uq.get('confirmed_cells')} candidate={uq.get('candidate_cells')} "
              f"jitter={uq.get('cells_jitter')} pipeline={uq.get('cells_pipeline')} "
              f"cluster_share={round(float(uq.get('cluster_share') or 0), 3)} "
              f"tile_hash={uq.get('versions_with_tile_hash')}/{uq.get('versions')}"))
    out = [l1, lc, lch, lqc, l2]
    for r in kgs[:max_rows]:
        ne = r.get("ne") or {}
        if r.get("ok"):
            cmp_ = (f"umfeld common={r.get('common')}/{r.get('umf')} dom={r.get('dom')}% tol={r.get('tol')}%"
                    if r.get("common") is not None else "umfeld n/a")
            rep = (f"report {r.get('http')} base={r.get('base')} same={r.get('same')} "
                   f"chg={'SUSPECT:' + str(r.get('suspect')) if r.get('suspect') else r.get('chg')} unk={r.get('unk')}"
                   f" {'✓contrib' if r.get('contrib') is True else ('✗' + str(r.get('contrib_err') or 'unchecked'))}"
                   f"{' partial-viewport(no report)' if r.get('partial') else ''}"
                   f"{(' tiles=' + str(r['tq'][0]) + '/' + str(r['tq'][1])) if r.get('tq') else ''}"
                   f"{' QC-SAME-INPUT=' + str(r['qc_same']) if r.get('qc_same') else ''}"
                   f"{_chg_tag(r)}")
            core = (f"{r['kg']} {r.get('peer')} {_hm(r.get('ts'))} {r.get('src')}{' DECLARED' if r.get('decl') else ''}"
                    f"{' CAD-ONLY(nec reused)' if r.get('cad_only') else ''} cells={r.get('cells')} req={r.get('req')} "
                    f"net~{r.get('net_mb')}MB{(' lru=' + str(r.get('lru_mb')) + '/' + str(r.get('lru_max_mb')) + 'MB') if r.get('lru_mb') is not None else ''}"
                    f"{(' rss=' + str(r.get('rss_mb')) + 'MB') if r.get('rss_mb') else ''}{(' tiled=' + str(r.get('tiled'))) if r.get('tiled') else ''}"
                    f" {r.get('wall_s')}s | {cmp_} | {rep}")
        else:
            core = f"{r['kg']} {r.get('peer')} {_hm(r.get('ts'))} FAILED {r.get('wall_s')}s: {r.get('err')}"
        sp = ne.get("split")
        if sp:
            tail = (f"split {sp['ingested']}/{sp['blocks']} blocks "
                    f"{'merged-complete' if sp['complete'] else ('merged-partial' if sp['merged'] else 'not-merged')}"
                    f"{' ERR:' + sp['merge_error'][:40] if sp.get('merge_error') else ''}")
        elif sp is None and ne.get("h3b"):
            tail = f"unsplit h3b={ne['h3b']}"
        else:
            tail = "unsplit"
        tail += f" obs={'✓confirmed' if ne.get('confirmed') else ('posted?' if ne.get('ours') else 'no-neo1-yet')}"
        if ne.get("bev"):
            b = ne["bev"]
            tail += f" on=bev{'✓' if b.get('ok') else '✗'}"
            if b.get("lu_diff"):
                ld = b["lu_diff"]
                tail += f" lu_diff(-{ld.get('missing_n')}/+{ld.get('extra_n')})"
        if ne.get("post_error"):
            tail += f" post_err={ne['post_error']}"
        out.append(f"    {core} | {tail}")
    if len(kgs) > max_rows:
        out.append(f"    … +{len(kgs) - max_rows} older pilot KGs (director/status.vct_pilot)")
    return out


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) > 1 and sys.argv[1] == "status":
        print(json.dumps(status(), indent=1, default=str))
    else:
        for ln in process_txt_lines():
            print(ln)
