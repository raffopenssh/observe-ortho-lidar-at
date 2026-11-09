"""Peer Director — Orchestrates processing across one or more VMs.

Each VM has 100 GB/month bandwidth (resets on the 17th). The director:
1. Tracks bandwidth usage per peer via vnstat
2. Chooses which peer should be active based on remaining budget
3. Starts/stops processors on peers via their REST API
4. Proxies status/logs from the active peer to the dashboard
5. Operates sequentially — only one peer processes at a time
   (Copernicus credentials are shared, so parallelism would cause 402s)

Peer config: data/austria_processor/peers.json
State: data/austria_processor/director_state.json
"""
from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

import config

log = logging.getLogger(__name__)


def _cell_key_pretty(k: str) -> str:
    """'47.5/48/12/13' → '47.5-48/12-13' (the process.txt cell notation)."""
    try:
        s, n, w, e = k.split('/')
        return '%s-%s/%s-%s' % (s, n, w, e)
    except Exception:
        return k


def _emit_director_event(msg: str, *, peer: str = '', kg: str = '',
                         level: str = 'info') -> None:
    """Lazy bridge into ``app.director_event`` so orchestration events
    (credential / cache-cell plan changes, peer updates) show up in the
    dashboard's 24h merged log alongside peer-sourced lines. Safe to
    call from any thread; never raises.
    """
    try:
        from app import director_event as _de
        _de(msg, peer=peer, kg=kg, level=level)
    except Exception:
        pass

# Git commit hash (read once at import)
try:
    _LOCAL_GIT_COMMIT = subprocess.check_output(
        ['git', 'rev-parse', '--short', 'HEAD'],
        cwd=str(Path(__file__).parent), stderr=subprocess.DEVNULL
    ).decode().strip()
except Exception:
    _LOCAL_GIT_COMMIT = 'unknown'

# Cache of {peer_commit: bool} — True iff peer_commit is a descendant of
# (or equal to) _LOCAL_GIT_COMMIT, i.e. the peer is at or *ahead* of the
# director's frozen-at-import target. We must not flag such peers as
# "stale / needs_manual_update": pulling on them would *downgrade* them.
# Happens routinely when the director was started before a fresh git
# push to origin/main and peers already auto-pulled past us.
_PEER_COMMIT_AHEAD_CACHE: dict[str, bool] = {}

def _peer_commit_contains(base: str, peer_commit: str) -> bool:
    """True iff *peer_commit* has *base* in its history (or equals it).
    Cached per (base, peer); ``git merge-base --is-ancestor BASE PEER``.
    False on any git error (peer commit unknown locally, etc.)."""
    if not peer_commit or not base or base == 'unknown':
        return False
    # peer-reported value → only a hex object name may reach the git argv
    if not re.fullmatch(r'[0-9a-fA-F]{4,64}', str(peer_commit)):
        return False
    if peer_commit == base or peer_commit.startswith(base) or base.startswith(peer_commit):
        return True
    key = f'{base}:{peer_commit}'
    cached = _PEER_COMMIT_AHEAD_CACHE.get(key)
    if cached is not None:
        return cached
    try:
        rc = subprocess.call(
            ['git', 'merge-base', '--is-ancestor', base, peer_commit],
            cwd=str(Path(__file__).parent),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        result = (rc == 0)
    except Exception:
        result = False
    _PEER_COMMIT_AHEAD_CACHE[key] = result
    return result


def _peer_commit_is_ahead_or_equal(peer_commit: str) -> bool:
    """True iff *peer_commit* contains _LOCAL_GIT_COMMIT in its history."""
    return _peer_commit_contains(_LOCAL_GIT_COMMIT, peer_commit)


# First commit whose app.py / austria_processor.py understand the sliver
# sweep (``PUT /processing/sweep_spec`` + ``sliver_sweep.peer_units``).
# Peers below it answer 405 to the spec push and would never report a
# unit — ``_orchestrate_cache_only`` only offers sweep work to peers at
# or past this commit (docs/sliver-sweep.md).
SWEEP_MIN_COMMIT = 'f778efe'  # fast path + add-on products + h3b archive (older code yields slow pre-products units)


def _peer_sweep_capable(ps: dict | None) -> bool:
    commit = ((ps or {}).get('git_commit') or '').strip()[:7]
    return bool(commit) and _peer_commit_contains(SWEEP_MIN_COMMIT, commit)

DATA_DIR = Path('data/austria_processor')
PEERS_CONFIG = DATA_DIR / 'peers.json'
DIRECTOR_STATE = DATA_DIR / 'director_state.json'



def _admin_headers() -> dict:
    """Headers for outbound peer control calls: ``X-Operator-Token`` from
    ``config.operator_token()`` (``SRTM_OPERATOR_TOKEN``, same value on every
    VM).  Empty in single-VM mode (loopback needs no token)."""
    tok = config.operator_token()
    return {'X-Operator-Token': tok} if tok else {}


def _env_peer_id(url: str) -> str:
    """Stable roster id for an ``SRTM_PEERS`` URL (host label)."""
    from urllib.parse import urlparse
    host = (urlparse(url).hostname or url).strip().lower()
    return host.split('.')[0] or host


def _env_peer_rows() -> list:
    return [{'id': _env_peer_id(u), 'url': u, 'enabled': True}
            for u in config.peers()]


# Bandwidth budget per peer per billing cycle (bytes)
BANDWIDTH_BUDGET_GB = 95  # conservative — leave 5 GB headroom out of 100
BANDWIDTH_BUDGET_BYTES = BANDWIDTH_BUDGET_GB * (1024 ** 3)
BANDWIDTH_RENEW_DAY = 17  # day of month when exe.dev bandwidth resets


def _peer_budget_bytes(peer: dict, cfg: dict) -> int:
    """Per-peer bandwidth budget in bytes.

    Resolution order: ``peer.budget_gb`` (override) >
    ``cfg.budget_gb`` (global) > ``BANDWIDTH_BUDGET_GB`` (default).
    Used to let a few canary peers run past the conservative global
    cap so we can probe what exe.dev's real billing-cycle limits are
    without disturbing the rest of the fleet.
    """
    try:
        per = peer.get('budget_gb') if isinstance(peer, dict) else None
        if per is not None:
            per = float(per)
            if per > 0:
                return int(per * (1024 ** 3))
    except (TypeError, ValueError):
        pass
    g = cfg.get('budget_gb', BANDWIDTH_BUDGET_GB) if isinstance(cfg, dict) else BANDWIDTH_BUDGET_GB
    return int(g * (1024 ** 3))


def _peer_bw_depleted(peer: dict, bw: dict, cfg: dict,
                     headroom_gb: float = 2.0) -> bool:
    """Evidence-based bandwidth depletion check.

    Canary-by-default philosophy (see _enforce_peer_bandwidth_walls):
    we do NOT know exe.dev's real per-account limits or billing-cycle
    anchors. The nominal ``budget_gb`` (95 / 200 / 250) is a guess —
    the only authoritative wall is ``observed_cap_gb`` set by a quality-
    grade canary slowdown.

    A peer is considered depleted iff:
      * it has an ``observed_cap_gb`` (a real, evidence-based wall), AND
      * its current cycle ``used_gb`` is within ``headroom_gb`` of that
        cap (i.e. ``cap - used < headroom``).

    Peers without an ``observed_cap_gb`` are NEVER considered depleted
    by this check, no matter how far past the nominal budget they run.
    The canary slowdown detector + ``_enforce_peer_bandwidth_walls``
    will park them when their real wall actually hits.

    NB: previously this gate compared used_bytes to ``_peer_budget_bytes``
    directly. That meant every peer past ~250 GB of nominal budget got
    disqualified from all eligibility lists — including peers whose
    billing cycle had just renewed but whose vnstat reading still
    spanned the old cycle (the peer-side ``get_local_bandwidth`` keys
    off the GLOBAL ``BANDWIDTH_RENEW_DAY=17``, not the peer's effective
    renew_day). The new gate is consistent with canary-by-default.
    """
    cap = peer.get('observed_cap_gb') if isinstance(peer, dict) else None
    if not isinstance(cap, (int, float)) or cap <= 0:
        return False
    used = (bw or {}).get('used_bytes', 0) or 0
    used_gb = used / (1024 ** 3)
    return used_gb >= float(cap) - float(headroom_gb)

# Number of enabled peers to keep idle as reserve (never started by
# the director).  Operational headroom for ad-hoc work, RF training,
# and bandwidth/credential burst capacity.
#
# Now that spinning up new peers is cheap and unlimited, the default
# reserve is 0 — use every enabled peer.  The capacity factor + Zenodo
# upload mutex + per-peer cooldowns are sufficient to keep us tender
# towards upstream servers.  Set ``min_reserve_peers`` in peers.json if
# you really want some peers idle (e.g. during ad-hoc development).
MIN_RESERVE_PEERS = 0
# How many cache-only peers may run concurrently with the frontier peer.
# Each cache-only peer only does BEV reads + CPU + Zenodo upload — no
# Copernicus credentials — so we can run as many as the whitelist and
# the reserve policy permit. The Zenodo upload mutex serialises writes
# so peers wait their turn rather than fighting. 64 is a soft ceiling
# well above the current fleet size; it just prevents the orchestrator
# from spinning up an absurd number of peers if min_reserve is misset.
MAX_CACHE_ONLY_PEERS = 64

# === v2 upgrade backfill (segv2 rollout) ===
# When the cache-ready whitelist runs low, idle cache-only peers are fed
# *upgrade* units: KGs that are v1-complete on Zenodo (``_json`` +
# ``_full_gpkg``) and have no ``_json_v2`` yet. The peer re-classifies
# them from their own full GPKG (no BEV / openEO traffic, no credential)
# and uploads ``_light_gpkg_v2`` + ``_json_v2``; the primary ingests the
# blob (``v2_ingest``). Only the processor decides per code (needs
# ``--v2-upgrade`` → start payload ``v2_upgrade:true``).
V2_UPGRADE_FILL_BELOW_READY = 40   # inject when cache_ready_kgs < this
MAX_V2_UPGRADE_PEERS = 24          # ≈ concurrent full-GPKG downloads (codes injected/tick)
V2_UPGRADE_ZEN_WARN_MAX = 2.0      # fleet Zenodo warns/min above which we inject nothing
V2_UPGRADE_CAND_TTL_S = 120        # was 300: at fast-path cadence a 5 min stale list re-dispatches done codes
#: v2.4 fast path (``v24_fast``): a KG with a committed v2.3+ pair is
#: relabelled from its light GPKG in minutes.  ONE shared Zenodo token
#: (~5000 req/h fleet-wide) and ≈16 API calls per fast KG ⇒ ~300 KG/h
#: ceiling before 429s; at ~5 min/unit that is ~24 concurrent fast units.
#: Explicit governor on top of the Zenodo-warn brake and the cache-only
#: ramp; live-editable via ``POST /api/v1/director/config``
#: ``{"v24_fast_max_peers": N}``.
V24_FAST_MAX_PEERS = 24
V2_COMPLETE_FRESH_TTL_S = 60       # fresh-manifest post-filter in _v2_upgrade_fill

# === SECTION: operator knobs (director_config.json) ===
# Small cross-worker file for live-tunable scalars.  peers.json top-level
# keys are NOT safe for this: ``_merge_peers_with_disk`` lets the loop
# worker's in-memory snapshot win, so a knob POSTed on the other gunicorn
# worker is clobbered within a tick.  Resolution order in ``knob()``:
# director_config.json > peers.json top-level > default.
DIRECTOR_CONFIG_FILE = DATA_DIR / 'director_config.json'
DIRECTOR_KNOBS = {
    'v24_fast_max_peers': int, 'max_v2_upgrade_peers': int, 'max_cache_only_peers': int,
    'v2_upgrade': bool, 'ne_source': str, 'v2_ingest_mb_per_h': float,
    'ne_ingest_mb_per_h': float, 'ne_ingest_enabled': bool, 'v2_upgrade_fill_below_ready': int,
    'ne_require_built': bool, 'v24_first': bool,
    # NE /obs re-post pacing (ne_ingest): posts per 5-min tick, GiB/day
    'ne_post_max_per_tick': int, 'ne_post_gib_per_day': float, 'ne_green_fix': bool,
    # VTC pilot (docs/vct-pilot.md): explicit peer ids + how many frontier
    # peers the director may auto-enrol (0 = manual only).
    'vct_pilot_peers': list, 'vct_pilot_auto': int,
    # Product switch (docs/vct-pilot.md → bev mode): peers whose declared
    # NE layer is built locally from the BEV vector tiles (per-peer
    # ``ne_source=bevdirect``).
    'vct_bev_peers': list,
    # Sliver sweep (docs/sliver-sweep.md): on/off, how many cache-only
    # workers sweep, parents per worker, bbox_changes min_delta.
    'sliver_sweep': bool, 'sweep_peers': int, 'sweep_per_peer': int, 'sweep_min_delta': float,
    'sweep_peers_auto': bool, 'sweep_peers_max': int,
    # declared-layer ``lu`` fetches per scan tick (~1.2 MB each, primary metered):
    # the only throughput limit of the sweep once the sweepers idle.
    'sweep_lu_per_tick': int,
    # Gradual ramp (docs/vct-pilot.md → Ramp): every 24h/N the director
    # moves ONE more eligible peer into vct_bev_peers (health-gated,
    # sticky) until the whole fleet builds its declared layer locally.
    # 0 = off.
    'vct_ramp_per_day': int,
    # Zenodo PUT throughput watchdog (zenodo_client.ProgressFileWrapper):
    # peer floor = max(abs_kbps, frac × fleet median PUT rate). Published
    # to peers in the peer_status ack (``fleet_put_rate()``).
    'zenodo_put_floor_abs_kbps': float, 'zenodo_put_floor_frac': float,
}
_knob_cache: dict = {'at': 0.0, 'd': {}}


def director_config() -> dict:
    now = time.time()
    if now - _knob_cache['at'] < 10:
        return _knob_cache['d']
    d = {}
    try:
        if DIRECTOR_CONFIG_FILE.exists():
            raw = json.loads(DIRECTOR_CONFIG_FILE.read_text())
            d = raw if isinstance(raw, dict) else {}
    except Exception:
        d = {}
    _knob_cache.update(at=now, d=d)
    return d


def set_director_config(updates: dict) -> dict:
    """Validate + persist knob updates (``None`` deletes a key)."""
    cur = dict(director_config())
    for k, v in (updates or {}).items():
        if k == '_force':
            continue
        if k not in DIRECTOR_KNOBS:
            raise ValueError(f'unknown knob {k!r}; allowed: {sorted(DIRECTOR_KNOBS)}')
        if v is None:
            cur.pop(k, None)
            continue
        typ = DIRECTOR_KNOBS[k]
        if typ is bool:
            v = bool(v) if not isinstance(v, str) else v.strip().lower() in ('1', 'true', 'yes', 'on')
        elif typ is str:
            v = str(v).strip()
            if k == 'ne_source' and v != 'bevdirect':
                raise ValueError('ne_source must be bevdirect')
            if k == 'ne_source' and v == 'bevdirect' and not updates.get('_force'):
                # Fleet-wide flip is not the supported path: the per-peer
                # list ``vct_bev_peers`` (+ ``vct_ramp_per_day`` for one
                # peer/day) switches peers one at a time, health-gated.
                # docs/vct-pilot.md → bev mode
                raise ValueError('fleet-wide ne_source=bevdirect is refused; use vct_bev_peers / vct_ramp_per_day '
                                 '(docs/vct-pilot.md) or pass _force')
        elif typ is list:
            if isinstance(v, str):
                v = [x.strip() for x in v.split(',') if x.strip()]
            v = sorted({str(x).strip() for x in (v or []) if str(x).strip()})
        else:
            v = typ(v)
        cur[k] = v
    tmp = DIRECTOR_CONFIG_FILE.with_suffix('.tmp')
    tmp.write_text(json.dumps(cur, indent=1, sort_keys=True))
    tmp.replace(DIRECTOR_CONFIG_FILE)
    _knob_cache.update(at=0.0)
    return cur


def knob(cfg: dict | None, key: str, default):
    d = director_config()
    if key in d:
        return d[key]
    if isinstance(cfg, dict) and key in cfg:
        return cfg[key]
    return default
#: 2.3: bring the existing v2 product line (2.0/2.1/2.2 KGs) to the current
#: version BEFORE starting on never-upgraded v1 KGs (clean slate — every
#: served v2 JSON gets robust top-N ranking).  Flip back to False once the
#: `v2:` line shows parents_by_version has no v2/v2.1/v2.2 left.
V2_REUPGRADE_FIRST = True

# === Frontier cell pre-warm (Fix #2) ===
# A frontier only fetches the handful of 0.1° tiles its KG needs, so the
# shared Zenodo cache never fills densely enough for a cache-only peer to
# find a *whole* KG ready. We let each frontier proactively densify the
# Zenodo cache around its own cell after every successful KG. The trigger
# is demand-side (too few cache-ready KGs); a Copernicus-health gate is the
# supply-side brake. The flag rides the existing cache_manifest.json sync
# to peers (no restart, no new endpoint).
#
# Enable warming only while the cache-only fleet is starved:
PREWARM_CACHE_READY_LOW = 100     # enable when cache_ready_kgs < this
PREWARM_CACHE_READY_HIGH = 150    # disable again above this (hysteresis)
# Supply-side gate: only warm when Copernicus is healthy.
PREWARM_COP_EMA_MIN = 0.8         # require sub_factor_ema['copernicus'] >=
# Bounded extra fetches per completed frontier KG, scaled by Copernicus
# health between MIN and MAX (a 0.1° cell = 4 Copernicus products + Hansen).
PREWARM_CELLS_PER_KG_MAX = 8
PREWARM_CELLS_PER_KG_MIN = 2
# product_repair (docs/product-repair.md): director sweep cadence + caps on
# the cell-fill plan shipped via prewarm.repair_cells / repair_codes.
PRODUCT_REPAIR_SWEEP_S = 15 * 60
# Frontier *planning* grid (docs/peer-director.md → Frontier planning
# cells). Decoupled from the v1 Zenodo ZIP grid (zenodo_cache.STRIP_HEIGHT
# × STRIP_WIDTH = 1°×2°) since tile store v2: Copernicus tiles are
# idempotent per-tile PUTs into 0.5°×1° shards, so two frontiers in the
# same ZIP cell can no longer lose each other's uploads (Hansen, still on
# the v1 ZIP path, is serialised by the per-ZIP fleet lock). The finer
# grid lifts the frontier cap from ~9 (work cells) to ~35 so credentials /
# peers / the capacity factor become the binding constraint. Set
# FRONTIER_CELL_LAT=1.0 FRONTIER_CELL_LON=2.0 to restore the old 1:1 grid.
FRONTIER_CELL_LAT = float(os.environ.get('FRONTIER_CELL_LAT', '0.5'))
FRONTIER_CELL_LON = float(os.environ.get('FRONTIER_CELL_LON', '1.0'))
#: hard ceiling on concurrent frontiers now that cells no longer bind
#: (~4× the Sep 2026 openEO load at 20; capacity factor + ramp still
#: scale within it). Raise once openEO / credit burn is observed stable.
FRONTIER_MAX_PARALLEL = int(os.environ.get('FRONTIER_MAX_PARALLEL', '20'))
# End-game cell sharing: when the number of planning cells with pending
# work drops below the credential capacity (the last few KGs of a run all
# sit in one or two cells), several frontiers may share ONE cell — at most
# ``FRONTIER_ENDGAME_PER_CELL`` per cell and never more than that cell's
# pending item count (parents + split blocks). Peers already de-duplicate
# via the cross-peer claim check + the director's dup-KG reconciler, and
# tile store v2 writes are idempotent per tile, so sharing a cell is safe.
# 8 (was 4, 2026-11-08): the last cell of the run held 8 straggler blocks
# (81201 ×2, 81206 ×1, 87104 ×5) while 4 ran and ~45 peers idled — the
# per-cell cap is only a ceiling, the Σ min(cap, pending items) bound
# still keeps frontiers ≤ real work.
FRONTIER_ENDGAME_PER_CELL = int(os.environ.get('FRONTIER_ENDGAME_PER_CELL', '8'))
PRODUCT_REPAIR_MAX_CELLS = 40
PRODUCT_REPAIR_MAX_CODES = 300
# Repair cells stay published while hourly openEO read-timeouts are
# at most this many OR fewer than successful months (see _product_repair_plan).
PRODUCT_REPAIR_OPENEO_MAX_TIMEOUTS = 3

# --- Small-fleet efficiency knobs (2026-08-22) ------------------------
# Shadow self-park is skipped while the fleet is small: dedicating a
# whole worker to a standby role costs ~1/N of fleet throughput, which
# is fine at N=50 but not at N=8. The shadow keeps processing; on
# promotion it aborts its in-flight KG (tile checkpoints + the chkpt
# registry make that loss cheap). Above the threshold we return to the
# conservative park-the-shadow behaviour.
SHADOW_PARK_MIN_FLEET = 12   # park shadow only when enabled workers >= this
# Cache-only mix-in: when cache-ready KGs accumulate, reserve a slice
# of the fleet for cache-only work instead of letting the parallel-
# frontier orchestrator absorb every peer. Only binds when peer count
# (not credential capacity) is the frontier ceiling — i.e. exactly the
# small-fleet regime; at scale creds bind first and the reservation is
# a no-op, so this stays scale-ready.
CACHE_MIX_MIN_READY = 20     # reserve only when cache_ready_kgs >= this
CACHE_MIX_FRAC = 0.25        # fraction of enabled peers reserved
CACHE_MIX_MAX_RESERVE = 8    # cap so big fleets still favour frontiers

# Per-peer circuit breaker. Each peer has a (failure_count, open_until)
# tuple. After CB_OPEN_THRESHOLD consecutive failures across *any*
# director-outbound HTTP call, the peer is skipped for CB_OPEN_SECONDS
# (exponential up to CB_OPEN_MAX_SECONDS) on every subsequent
# `_peer_request`. A single success resets the counter. Distinct from
# the bandwidth-specific backoff above (which only suppresses bandwidth
# polls), this stops the director from spending its whole tick waiting
# for a wedged peer's gunicorn to time out.
CB_OPEN_THRESHOLD = 3
CB_OPEN_BASE_SECONDS = 60        # 1 min on first trip
CB_OPEN_MAX_SECONDS = 1800       # cap at 30 min
_PEER_CB: dict[str, dict] = {}   # peer_id -> {fails, open_until, trips}
_PEER_CB_LOCK = threading.Lock()
# Reverse map: peer_url -> peer_id. Updated by the director loop each
# tick from peers.json so module-level helpers (get_peer_bandwidth,
# get_peer_status) can attribute failures to the right peer without
# threading peer_id through every call site.
_PEER_URL_TO_ID: dict[str, str] = {}
# Process-local throttle for revalidate_all_credentials. Each gunicorn
# worker has its own copy; the primary check is still the persisted
# state['_creds_revalidated_at'], but this avoids hitting OIDC twice
# in the same worker within a tick.
#
# Single-flight semantics: only one thread per process may run the
# OIDC probe at a time. Other threads see the throttle window as
# closed and use cached ``last_status`` instead. This is what stops
# the dashboard-induced OIDC storm: with 4 threads × 2 workers each
# racing past a stale timestamp, we'd otherwise issue 8 OIDC requests
# × 8 credentials = 64 outbound requests per tick.
_LAST_CREDS_REVALIDATED_AT: float = 0.0
_REVALIDATE_LOCK = threading.Lock()
# Hard floor on revalidation cadence. Even the director loop won't
# probe more often than this. The dashboard hot path uses cached
# ``last_status`` from ``copernicus.list_credentials()``; that cache
# is updated by ``_save_credentials_to_disk()`` after each probe.
# 30 min (was 10): with ~107 creds a sweep is ~430 requests / 1.5 MB
# (openid-configuration + token + openeo root + oidc providers per
# cred) — ~9 MB/h of primary egress at 10 min. Exhaustion / 402
# transitions still surface live via peers' usage telemetry.
_REVALIDATE_INTERVAL_S = 1800

# === Zenodo tile-cache manifest reconcile + Zenodo-degraded circuit ===
# Hourly reconcile of cache_manifest.json against the live deposit
# listing (zenodo_cache.reconcile_manifest). Runs in a daemon thread
# off the director loop (ZIP probes can take minutes when Zenodo is
# slow); TTL-throttled via state['_cache_reconcile_at'] like
# _refresh_credentials_if_due. Also fired immediately when the
# Zenodo-degraded circuit clears.
CACHE_RECONCILE_INTERVAL_S = 3600
_CACHE_RECONCILE_LOCK = threading.Lock()
# Fleet-wide Zenodo-degraded circuit: trips when >= N distinct peers log
# a Zenodo PUT failure / 5xx / timeout inside the window; clears after
# CLEAR_S without a new failure. Published to peers via
# cache_manifest.json['zenodo_circuit'] (rides the 5-min sync like the
# prewarm directive). While set, peers skip cache-cell REPLACEMENT
# uploads and never tombstone (zenodo_cache.zenodo_degraded()).
ZENODO_DEGRADED_MIN_PEERS = 3
ZENODO_DEGRADED_WINDOW_S = 15 * 60
ZENODO_DEGRADED_CLEAR_S = 30 * 60
ZENODO_DEGRADED_SCAN_INTERVAL_S = 60
# Hard kill switch: only the director loop is permitted to call
# ``revalidate_all_credentials()``. The dashboard path
# (``get_status`` → ``_valid_credentials``) reads cached statuses
# only. This is the architectural fix for the OIDC storm: the hot
# request path can never trigger network probes, regardless of
# any cache miss or worker swap.
_REVALIDATE_OWNER_THREAD: int | None = None


def _peer_id_for_url(peer_url: str) -> str:
    if not peer_url:
        return ''
    return _PEER_URL_TO_ID.get(peer_url.rstrip('/'), '')


class _CircuitOpenError(Exception):
    """Raised when a peer's breaker is open and we skip the call."""


class _DormantSkipFanout(Exception):
    """Control-flow sentinel: fleet is dormant — skip the capacity
    fanout + EMA bookkeeping but keep the pushed-status snapshot."""


def _cb_should_skip(peer_id: str) -> bool:
    if not peer_id:
        return False
    with _PEER_CB_LOCK:
        ent = _PEER_CB.get(peer_id)
        if not ent:
            return False
        return time.time() < ent.get('open_until', 0.0)


def _cb_record_success(peer_id: str) -> None:
    if not peer_id:
        return
    with _PEER_CB_LOCK:
        if peer_id in _PEER_CB:
            _PEER_CB[peer_id]['fails'] = 0
            _PEER_CB[peer_id]['open_until'] = 0.0


def _cb_record_failure(peer_id: str, exc: Exception | None = None) -> None:
    if not peer_id:
        return
    with _PEER_CB_LOCK:
        ent = _PEER_CB.setdefault(
            peer_id, {'fails': 0, 'open_until': 0.0, 'trips': 0})
        ent['fails'] = int(ent.get('fails', 0)) + 1
        if ent['fails'] >= CB_OPEN_THRESHOLD:
            trips = int(ent.get('trips', 0)) + 1
            ent['trips'] = trips
            cooldown = min(
                CB_OPEN_MAX_SECONDS,
                CB_OPEN_BASE_SECONDS * (2 ** min(trips - 1, 8)))
            ent['open_until'] = time.time() + cooldown
            ent['fails'] = 0
            log.warning(
                'Peer %s circuit breaker OPEN for %ds (trip #%d, last err=%s)',
                peer_id, int(cooldown), trips, str(exc)[:120] if exc else 'n/a')


def _cb_state_for(peer_id: str) -> dict:
    """Return a serialisable snapshot of one peer's breaker state."""
    if not peer_id:
        return {}
    with _PEER_CB_LOCK:
        ent = _PEER_CB.get(peer_id)
        if not ent:
            return {'open': False, 'fails': 0, 'trips': 0}
        now = time.time()
        open_until = float(ent.get('open_until', 0.0))
        return {
            'open': now < open_until,
            'fails': int(ent.get('fails', 0)),
            'trips': int(ent.get('trips', 0)),
            'cooldown_remaining_s': max(0, int(open_until - now)),
        }


def _peer_request(method: str, url: str, *,
                  peer_id: str = '',
                  timeout=None,
                  raise_circuit: bool = False,
                  **kwargs):
    """requests.request wrapper with per-peer circuit breaker.

    * Refuses (raises ``_CircuitOpenError`` / returns None) when the
      peer's breaker is open.
    * On any transport exception or 5xx response, records a failure.
    * On 2xx/3xx/4xx (i.e. peer responded), records a success.

    Callers that already have peer-id context should pass ``peer_id``.
    Returns the ``Response`` on success or None if the call was
    short-circuited (and ``raise_circuit`` is False).
    """
    if peer_id and _cb_should_skip(peer_id):
        if raise_circuit:
            raise _CircuitOpenError(peer_id)
        return None
    if timeout is None:
        timeout = PEER_TIMEOUT_PROBE  # resolved at call time
    kwargs.setdefault('headers', {})
    if 'X-Operator-Token' not in kwargs['headers']:
        kwargs['headers'].update(_admin_headers())
    try:
        r = requests.request(method, url, timeout=timeout, **kwargs)
    except Exception as e:
        _cb_record_failure(peer_id, e)
        raise
    if r.status_code >= 500:
        _cb_record_failure(peer_id, RuntimeError(f'HTTP {r.status_code}'))
    else:
        _cb_record_success(peer_id)
    return r


# Per-peer dedup for graceful cache-only stop signals. The processor's
# signal handler sets _shutdown_requested=True but only exits *after*
# the current KG finishes — sometimes 1–2h later. Without dedup, every
# director tick re-sends SIGTERM (we observed 63 in 36h on one peer).
# Map peer_id → unix ts of last graceful stop emitted by this director.
_LAST_GRACEFUL_STOP_TS: dict[str, float] = {}

# How often the director checks state (seconds)
DIRECTOR_POLL_INTERVAL = 30
# Grace period after stopping a peer before starting another (seconds)
SWITCH_COOLDOWN = 10
# HTTP timeout for peer API calls.  Tuple = (connect_timeout, read_timeout).
# A 3 s connect catches DNS / TLS-handshake hangs without waiting forever.
# Read timeout is short for cheap probes (bandwidth/status) and longer for
# operations that legitimately take time (start/stop, cache_manifest sync).
PEER_TIMEOUT = (3, 8)
PEER_TIMEOUT_PROBE = (3, 8)         # bandwidth, status, log
PEER_TIMEOUT_CONTROL = (5, 25)      # start/stop/cache-manifest/queue PUT
# Bandwidth poll concurrency — keep small to avoid ephemeral-port exhaustion
# but enough that the loop finishes well within DIRECTOR_POLL_INTERVAL even
# when most peers are wedged.
BANDWIDTH_POLL_CONCURRENCY = 10
# After this many consecutive bandwidth-poll failures, back off polling
# the peer for BANDWIDTH_BACKOFF_SECONDS so a single dead peer can't drag
# out the loop on every tick.
BANDWIDTH_BACKOFF_THRESHOLD = 3
BANDWIDTH_BACKOFF_SECONDS = 900     # 15 min — be tender with flaky peers
# Number of consecutive unreachable polls before failover (avoids killing
# peers during heavy GPKG builds that briefly starve gunicorn).
UNREACHABLE_FAILOVER_THRESHOLD = 3
# --- Fleet-dormant hold mode ---------------------------------------
# When EVERY remote enabled peer is unreachable continuously for this
# long (e.g. exe.dev powered off the whole fleet), the director enters
# a dormant hold: it stops all orchestration (activation churn, shadow
# election, stale-update retries, OIDC probes, canary/steal/BEV checks)
# and instead runs a light reachability probe sweep every
# FLEET_DORMANT_PROBE_INTERVAL_S. The first peer that becomes reachable
# again (either via its own status push heartbeat or a successful probe)
# wakes the director instantly back into normal operation. The primary
# keeps serving the interactive app / dashboard untouched throughout.
FLEET_DORMANT_AFTER_S = 15 * 60
FLEET_DORMANT_PROBE_INTERVAL_S = 300
# Run-complete hold (Nov 2026 endgame): when the whole run is done — no
# pending KG/block anywhere, empty retry queue, no open sliver-sweep /
# viewfill / v2-dispatch work, nothing repairable, no peer inside a KG —
# the director stops orchestrating instead of restart-looping idle
# frontiers into quarantine. Like the dormant hold it skips the per-tick
# status fanout + bandwidth polls (pushed heartbeats only) and
# ``app._sync_peer_data`` skips its 5-min fanout. Wake is automatic the
# moment any work appears again (queue POST, umfeld change → sweep unit,
# viewfill plan, repair cells cached) — re-checked every
# RUN_COMPLETE_RECHECK_S from local state files only.
RUN_COMPLETE_AFTER_S = int(os.environ.get('RUN_COMPLETE_AFTER_S', str(10 * 60)))
RUN_COMPLETE_RECHECK_S = int(os.environ.get('RUN_COMPLETE_RECHECK_S', '300'))
# How long to keep a peer out of rotation after a local Zenodo network
# failure (the same peer hitting the same network problem on retry would
# loop forever).  Cleared automatically when not_before passes.
# 60 min base — Zenodo's targeted rate-limits are slow to clear and we'd
# rather a peer sit out longer than thrash in/out.
ZENODO_NETWORK_COOLDOWN_MIN = 60
# Escalating cooldown for peers with a 'hold tendency' (repeat offenders).
# Each cooldown applied within HOLD_TENDENCY_WINDOW_HOURS is counted; the next
# cooldown is multiplied by 2**(count-1), capped at HOLD_TENDENCY_MAX_MIN.
# Short window so we react quickly to targeted rate-limits: a peer that
# trips twice within ~3 h is almost certainly being throttled by name/IP
# and should sit out aggressively rather than thrash in/out every 30 min.
# Aggressive escalation factor: each repeat multiplies the cooldown by
# HOLD_TENDENCY_FACTOR (cubic-ish growth instead of doubling).
# Wider window so we count repeats over a longer baseline; with cheap
# peers we can afford to keep an offender out for the rest of the day.
HOLD_TENDENCY_WINDOW_HOURS = 6
HOLD_TENDENCY_FACTOR = 3
HOLD_TENDENCY_MAX_MIN = 24 * 60   # 24 h ceiling

# A reserved KG is held for a cooled-down peer so it can resume from its
# own tile checkpoints. But the held KG must never be deadlocked behind a
# multi-day park (manual long cooldown, role park to 2027, billing-cycle
# park to next renewal). Once the holder's cooldown exceeds this many
# hours, ``_clear_completed_reservations`` releases the KG so any peer can
# pick it up (checkpoints survive on the cross-peer Zenodo chkpt registry,
# so the resume is still cheap). 36h comfortably exceeds the longest
# legitimate short-cooldown (24h HOLD_TENDENCY ceiling) so a normal
# escalated Zenodo cooldown is never prematurely released.
RESERVATION_MAX_HOLD_HOURS = 36

# --- Server-friendliness throttle ----------------------------------------
#
# When BEV / Zenodo / Copernicus servers start emitting warnings (HTTP 0
# range-read drops, 429s, 503s, openEO 402s) we voluntarily reduce the
# number of concurrent peers so we don't hammer them. The capacity factor
# is recomputed on every director tick from the fleet-wide warning rate.
#
# We never go below ``THROTTLE_MIN_FACTOR`` (so a single noisy peer can't
# drop the fleet to zero) and we never run more than 100% of the
# configured max. We also overlay a slow sinusoidal drift (period
# ~2 hours) so the activity pattern looks organic rather than
# bang‑on‑max all the time. Phase per peer comes from a stable hash so
# different VMs take their breaks at different times.
THROTTLE_MIN_FACTOR = 0.20
THROTTLE_MAX_FACTOR = 1.00
# warnings/min (per kind) at which the factor reaches its minimum.
# A few stray retries are normal; sustained > ~6/min is real pressure.
# Tightened down 2026-04-30: with a 20+ peer fleet a small per-peer
# warning rate aggregates into real fleet-wide pressure on upstream
# servers. Lower saturation rates ⇒ throttle bites earlier, peers back
# off sooner. We'd rather lose ~30 % of cache-only slots for an hour
# than get the whole token banned.
# Re-tuned 2026-04-23 for the ~50 peer fleet. Saturation is computed
# against the fleet-wide *max* per-peer rate, but ambient noise (one
# log line every few minutes per peer) aggregates faster as the fleet
# grows. Values below assume ~50 peers; scale up roughly proportional
# to len(peers)/50 if the fleet keeps growing.
THROTTLE_SATURATION_RATE = {
    'bev': 5.0,
    'zenodo': 1.5,        # Zenodo rate-limits aggressively
    'copernicus': 0.4,    # 402s should be near-zero in steady state
}
# Dead-zone: fleet-max rates below this fraction of saturation are
# treated as zero. Keeps ambient noise (a stale-manifest 404 every
# couple of minutes from one peer) from pulling capacity off 100%.
THROTTLE_DEAD_ZONE_FRAC = 0.10
# EMA smoothing factor for the capacity decision (per tick, ~30 s).
# Smaller = slower to react / recover. 0.25 gives a half‑life of ~3 ticks.
# Smaller alpha → slower reaction up *and* down. Recovery is slow on
# purpose: once we've upset Zenodo/BEV, easing back gradually is far
# safer than snapping back to full throttle the moment warnings stop.
THROTTLE_EMA_ALPHA = 0.30
# Slow per-peer noise EMA. Updated every director tick from the peer's
# current 5-min warning rate. Used by choose_active_peer / orchestrators
# so a peer that has been noisy in the last few hours stays penalised
# even after the 5/10-min sliding windows have rolled to zero. With a
# 30 s tick cadence, alpha=0.006 gives ~58 min half-life — a peer that
# upset Zenodo at 06:00 still scores noisy at 09:00.
PEER_NOISE_LONG_EMA_ALPHA = 0.006
# Per-peer CPU-steal EWMA (tick cadence ~30 s). alpha=0.033 gives a
# ~15 min half-life — slow enough to ride out brief hypervisor blips,
# fast enough that a peer that lands on a quiet host after a park is
# eligible for frontier work again within ~30 min. Stored on
# ``state['peer_steal_ema']`` so it survives director restart and is
# visible to all gunicorn workers via director_state.json.
PEER_STEAL_EMA_ALPHA = 0.033
# Rolling-steal auto-park: when a peer's steal EMA sits at or above
# this for STEAL_PARK_PERSIST_S, park it for STEAL_PARK_COOLDOWN_S.
# exe.dev doesn't re-place VMs on idle, but a 30 min idle window often
# coincides with a noisy neighbour finishing its workload, and either
# way we stop burning Copernicus credits / KG wall-time on a peer that
# is effectively running at ~15-25 %% of a core.
STEAL_PARK_THRESHOLD_PCT = 70.0
STEAL_PARK_RECOVERY_PCT = 50.0       # streak reset hysteresis
STEAL_PARK_PERSIST_S = 10 * 60
STEAL_PARK_COOLDOWN_S = 30 * 60
# --- Stuck-KG watchdog --------------------------------------------------
# A peer whose processor wedges *inside* a KG (e.g. a BEV re-read that
# never gives up, or a hung SSL upload) holds a running slot forever
# while producing nothing. We've seen peers frozen on the same KG+step
# for 72 h (at21/at92/at94, Jun 2026). The tile-checkpoint + chkpt
# registry machinery means a hard restart is nearly free — the next
# peer resumes the finished tiles.
#
# CRITICAL: some KGs legitimately take a long time. The worst legit
# *opaque* window (a single step emitting NO director-visible progress
# field — gpkg_full stitch / upload chunk) observed over 7 days is ~5 h.
# copernicus/upload steps move step_detail_ts every few minutes, so
# their multi-hour totals don't count as "frozen". We therefore key the
# watchdog off a FINGERPRINT (kg+step+tile+detail_ts+completed) that
# advances during any healthy work, and only kill when that fingerprint
# has been frozen for STUCK_KG_FROZEN_S — set to ~2.4x the worst legit
# opaque window so a slow-but-progressing KG is never murdered.
STUCK_KG_FROZEN_S = 12 * 3600        # no progress fingerprint change → kill
STUCK_KG_MIN_STATUS_FRESH_S = 15 * 60  # require a recent (non-stale) status
# --- Duplicate-KG reconciler -------------------------------------------
# The dispatch-side guards (cross-peer claim registry + _in_progress_kgs
# filter + block-level whitelist guard) stop two peers being *handed* the
# same KG within a director tick. They cannot undo a duplicate that arose
# OUTSIDE that window — e.g. two peers self-picked the same parent code
# from their own retry queues minutes/hours apart (well past the 150s
# _RECENT_DISPATCH_TTL), or a peer started a KG before another finished
# uploading it. Those run to completion side-by-side, burning a second
# peer's CPU + bandwidth on identical work (observed: 92111 on at26+at112
# 37 min apart; 49008 on at109+at115 ~2.4h apart; 01803 on at27+at114).
# This reconciler keeps the furthest-along peer and hard-stops the
# loser(s) so they rejoin rotation onto fresh work — the dispatch guards
# then keep them off the winner's KG. Modeled on the stuck-KG watchdog:
# fresh-status-only, scheduled-peer-safe, rate-limited, and it requires
# the duplicate to persist across DUP_KG_PERSIST_S so we never fight a
# brand-new dispatch the claim registry is still resolving.
DUP_KG_PERSIST_S = 120.0             # dup set must hold this long before we act
DUP_KG_MIN_AGE_S = 300.0             # both peers must be >5 min into the KG
DUP_KG_STOPS_PER_TICK = 2            # cap loser hard-stops per tick (anti-herd)
# Hard tier for frontier scheduling: peers at or above this rolling
# steal % are demoted to the high-steal tier when picking active /
# parallel frontier peers. A low-steal peer always outranks a
# high-steal one when available; the continuous noise + capacity
# penalty still orders peers *within* a tier.
FRONTIER_HIGH_STEAL_BIAS_PCT = 50.0
# --- Frontier BEV-health bias ------------------------------------------
# Pools whose recent BEV warn rate is markedly higher than the fleet
# median get demoted in frontier selection so scarce Copernicus credits
# don't land on a peer that's about to burn most of its KG wall-time
# retrying BEV. Soft bias: a continuous penalty added to the sort key
# (same shape as the steal capacity penalty). Hard tier: pools >= the
# absolute floor (4 wpm, same as the park trigger) are demoted to a
# worse tier so low-pool-pressure peers always outrank them.
FRONTIER_POOL_BEV_HARD_WPM = 4.0    # >= this avg_wpm => tier-demoted
FRONTIER_POOL_BEV_BIAS_FLOOR = 1.0  # avg_wpm below this contributes 0
# --- BEV-outage detection (escalating fleet pause) ---------------------
#
# The smooth EMA throttle eases throughput when BEV is grumpy, but a
# real outage (HTTP 0 / 502 / connection-reset everywhere) needs a
# step response: stop hammering BEV, let it heal, then probe.
#
# We trigger when fleet warns/min for the 'bev' kind sustains above
# BEV_OUTAGE_TRIGGER_WPM for BEV_OUTAGE_TRIGGER_PERSIST_S, AND we have
# at least BEV_OUTAGE_MIN_PEERS distinct peers contributing to that
# rate. The pause cooldown escalates 1h → 4h → 12h → 24h when a fresh
# trigger fires within BEV_OUTAGE_RESET_S of the previous unpause;
# otherwise we reset to level 1.
#
# While the pause is active, cache-only peers are not started and the
# active frontier is asked to graceful-stop at the current KG boundary
# so we don't keep retrying BEV during the cooldown. Frontier peers
# already mid-finalization (zenodo upload steps) are left alone.
BEV_OUTAGE_TRIGGER_WPM = 12.0       # fleet warns/min (1m window aggregate)
BEV_OUTAGE_RECOVERY_WPM = 2.0       # below this for PERSIST_S → clear
BEV_OUTAGE_TRIGGER_PERSIST_S = 4 * 60   # sustained pressure before pausing
BEV_OUTAGE_RECOVERY_PERSIST_S = 5 * 60  # quiet streak required before unpause
BEV_OUTAGE_MIN_PEERS = 4            # must affect this many distinct peers
BEV_OUTAGE_LEVELS_S = (60 * 60, 4 * 3600, 12 * 3600, 24 * 3600)
BEV_OUTAGE_RESET_S = 48 * 3600      # ≥ this since last unpause → level 1
# --- Parked-pool canary probes -----------------------------------------
# Pools parked via `bev_pool_park` may be stuck on a transient route
# flap (20–60 min) that clears long before the 1h/4h/12h/24h cooldown
# expires. We poll one peer per parked pool every BEV_POOL_PROBE_INTERVAL_S
# (default 5 min) by calling its local /api/v1/bev_probe endpoint (single
# HEAD-range to bev_proxy._BEV_TEST_URL with TIFF magic check). On a
# successful probe (206 + TIFF magic) we clear `not_before` on every peer
# in that /24 early and drop the pool's escalation entry. Probe cost:
# ~5 s wall worst-case per parked pool; runs in parallel.
BEV_POOL_PROBE_INTERVAL_S = 5 * 60      # min interval between probes per pool
BEV_POOL_PROBE_REQ_TIMEOUT_S = 12       # outbound HTTP timeout (peer probe ≤ 8 s)
# --- IP-pool blocked-egress detection ----------------------------------
#
# exe.dev VMs share egress NAT pools (typically a small set of /24s).
# When one pool gets shaped or banned (BEV/Zenodo/Copernicus return 0
# / 5xx for that source IP range), peers in that pool will all see
# the same elevated warn rate while peers in clean pools see normal
# rates. We aggregate per-peer 5m BEV warn rates by /24 (peers report
# their outbound IP in host_profile) and park peers in any pool whose
# rate is ≥ IP_POOL_BIAS x the fleet median AND ≥ IP_POOL_MIN_WPM
# absolute. The parked peers get a 30 min cooldown so the offending
# pool has time to heal (or exe.dev to re-roll the NAT mapping).
IP_POOL_MIN_PEERS = 2               # need at least N peers in pool to count
IP_POOL_BIAS = 3.0                  # pool rate vs fleet median multiplier
IP_POOL_MIN_WPM = 4.0               # absolute floor (one-pool fleet still works)
IP_POOL_PARK_COOLDOWN_S = 30 * 60   # 30 min off then re-eligible
# --- Outage localization (pool-targeted vs fleet-wide) -----------------
#
# When a BEV outage triggers, we'd rather park just the affected /24
# than stop the whole fleet. We classify the outage as 'pool' (one
# /24 dominates) only when pool attribution is *reliable*: enough
# peers have reported their outbound IP (else most of the fleet is in
# the '?' bucket and we can't tell). Tunables:
OUTAGE_POOL_KNOWN_FRAC = 0.60     # ≥ 60% of peers must have a known /24
OUTAGE_POOL_DOMINANCE = 0.70      # smallest pool-prefix must ≥ 70% of warns
OUTAGE_POOL_AVG_MULT = 2.0        # every prefix pool: avg ≥ 2x other-pool median
OUTAGE_POOL_MAX_SET = 3           # bound prefix size; >3 pools → fleet outage
# History rings (persisted via director_state.json → survives restart + HA):
BEV_PAUSE_HISTORY_MAX = 64        # last N pause events (~24h worth)
POOL_HISTORY_MAX = 1440           # 24 h at one sample/min
POOL_HISTORY_SAMPLE_INTERVAL_S = 60
# Sinusoidal drift: period and amplitude (fraction of total range).
THROTTLE_DRIFT_PERIOD_S = 2 * 3600    # 2 hours
# Sinusoidal drift amplitude. Reduced 2026-04-23 from 0.10 to 0.04 —
# the previous ±10 % was loud enough to flip the dashboard color
# between green/yellow/red on its own when the EMA sat near a
# threshold, even with no upstream pressure. Cosmetic, not operational.
THROTTLE_DRIFT_AMPLITUDE = 0.04        # ±4% wobble around the EMA value

# Ramp limiter: how many *new* peers we start per director tick. Without
# this, after a restart we can fire up 19 peers in ~2 seconds, each of
# which immediately POSTs cache-manifest sync + priority-queue PUT to
# the primary AND starts hammering BEV/Zenodo. A gentler ramp (1-3 per
# tick depending on capacity factor) lets each new peer warm its caches
# and the upstreams notice gradually. Empirically this is the difference
# between Zenodo accepting all uploads and Zenodo emitting blanket 429s
# on the whole cluster for 30 minutes.
RAMP_MIN_STARTS_PER_TICK = 1
RAMP_MAX_STARTS_PER_TICK = 3

# Strip-hog release: minimum interval between graceful stops sent to a
# running frontier that holds more than its fair share of strips while
# credentialed candidates are starving (empty strip plans). Graceful =
# KG-boundary stop, so the in-flight KG always completes + uploads.
FRONTIER_HOG_RELEASE_COOLDOWN_S = 3600

# Minimum interval between processor restarts on the same active frontier
# peer when the cred/strip plan + exclude set haven't changed. The
# director sees ``state in ('idle','stopped')`` between KGs (the
# subprocess exits cleanly per-KG) and would otherwise fire a fresh
# start every 30s tick. Empirically we see ~8 restarts/hour without
# this guard. Plan changes always restart immediately regardless.
FRONTIER_RESTART_COOLDOWN_S = 180

# Canary bandwidth history & auto-park.
#
# As of 2026-05 every enabled peer is treated as a canary: exe.dev's
# real per-account billing-cycle limits are unknown (each VM was created
# in a different account at a different date) so we don't trust the
# global 95 GB / 17th-of-month assumption. Instead we sample every
# peer's cumulative ``used_bytes`` on every bandwidth poll and, if the
# observed throughput collapses (exe.dev started shaping), park that
# peer with ``not_before = now + cooldown`` and persist
# ``observed_cap_gb = used_gb-at-park`` in peers.json so we learn each
# peer's wall organically. Slowdown park applies to ALL peers; the
# stricter noise-based park (warning-rate spike) stays canary-only —
# generic noisy peers are already handled by load-shifting in the
# scheduler and we don't want to remove them from rotation just for
# being upstream-grumpy.
#
# A peer with a per-peer ``budget_gb`` override is still called a canary
# because its budget is intentionally raised above the conservative
# global cap to probe further; the dashboard surfaces this distinctly.
#
# Sampling cadence is whatever ``BANDWIDTH_POLL_INTERVAL`` is doing
# (every ~minute on the active poll loop, less for backed-off peers).
# Ring is bounded so the state file can't blow up.
CANARY_HISTORY_MAX = 240          # ~4h at 1 sample/min
# Per-peer billing-cycle telemetry. Cumulative used_bytes only drops
# at a billing-cycle renewal, so we don't need a full ring — a single
# previous-sample is enough to detect each drop in flight, and we just
# append the drop event (rare: <=1/month per peer). Total persisted
# footprint is ~ (90 peers × ~30 B last + few events × ~50 B) ≈ a few
# KB across the whole fleet — vs ~2 MB for a naive 30-day-of-samples
# ring. Matches the design discipline of fleet_proxy_history (commit
# 27aa243): persist only the shape the dashboard / learner needs.
BW_HISTORY_INTERVAL_S = 3600       # min spacing between drop-checks
BW_RENEWAL_DROP_FRAC = 0.5         # drop fraction to count as a renewal
BW_RENEWAL_MIN_DROP_GB = 20.0      # absolute drop floor (anti-noise)
BW_RENEWAL_EVENTS_MAX = 6          # keep last N renewals/peer (~6 months)
CANARY_BASELINE_MIN_SAMPLES = 6   # need a baseline before we can compare
CANARY_BASELINE_WINDOW_S = 1800   # 30-min trailing baseline
CANARY_RECENT_WINDOW_S = 600      # 10-min recent throughput
CANARY_SLOWDOWN_RATIO = 0.30      # park if recent < 30% of baseline
CANARY_NOISE_PARK_THRESHOLD = 1.5 # park if noise_score >= this
CANARY_PARK_COOLDOWN_S = 6 * 3600 # 6 h not_before (quality-grade parks)
# Soft parks (defensive, not network-grade or not persistent) are
# almost always upstream blips (Zenodo upload stall, openEO 502,
# transient internet hiccup) rather than exe.dev shaping. A 6 h
# cooldown is wasteful here — the blip resolves in minutes and we
# burn hours of peer capacity. Use a shorter cooldown so soft-parked
# peers cycle back into rotation; if the underlying issue is real
# they’ll soft-park again on the next tick (and eventually graduate
# to quality-grade once persistence + network-grade both fire).
CANARY_PARK_COOLDOWN_SOFT_S = 60 * 60   # 1 h
# Canary slowdown is ignored while the peer's CPU is this busy (the
# recent-window stall is compute, not shaping).  Pushed telemetry:
# system.perf.cpu_total_ewma (10-min EWMA) / system.cpu_total.
CANARY_COMPUTE_BOUND_CPU_PCT = 30.0
CANARY_COMPUTE_BOUND_STEPS = ('terrain', 'segment', 'vectorise', 'gpkg_full',
                              'gpkg_light', 'json', 'v2_verify', 'anchor')


def _canary_peer_compute_bound(p: dict) -> bool:
    """True when the peer's last pushed status shows a busy CPU or a
    CPU-bound pipeline step — see the canary check for why that makes
    a throughput collapse uninteresting.  Stale/missing status → False."""
    try:
        cached = _PEER_STATUS_CACHE.get(p.get('url') or '')
        if not cached or (time.time() - cached[0]) > 900:
            return False
        st = cached[1] or {}
        if str(st.get('state') or '') not in ('running', 'processing'):
            return False
        sysd = st.get('system') or {}
        perf = sysd.get('perf') or {}
        cpu = perf.get('cpu_total_ewma')
        if cpu is None:
            cpu = sysd.get('cpu_total')
        if isinstance(cpu, (int, float)) and float(cpu) >= CANARY_COMPUTE_BOUND_CPU_PCT:
            return True
        step = str((st.get('current_kg') or {}).get('step') or st.get('current_kg_step') or '').lower()
        return any(step.startswith(x) for x in CANARY_COMPUTE_BOUND_STEPS)
    except Exception:
        return False
# Don't trip the slowdown check until the canary has actually moved
# enough bytes for the average to be meaningful (otherwise a peer that
# happens to be idle between KGs reads as 'shaped').
CANARY_MIN_BYTES_FOR_PARK = 500 * 1024 * 1024  # 500 MB in baseline window
# Tightened thresholds to avoid false positives from peers that are
# Zenodo-upload-bound (their cumulative byte counter advances slowly
# even when the network is healthy).
#
# Park only when the peer was actually moving NETWORK-GRADE bytes
# before the collapse — baseline >= CANARY_BASELINE_NETWORK_MBPS —
# AND it has truly stalled — recent <= CANARY_RECENT_PARKED_MBPS.
# This keeps the watchdog active for real shaping events while not
# tripping when a peer happens to be uploading a 4 GB GPKG to Zenodo.
#
# Even tighter: only persist ``observed_cap_gb`` when the trigger
# meets these thresholds (so we don't pollute the fleet wall estimate
# with park events caused by upload-bound idleness).
CANARY_BASELINE_NETWORK_MBPS = 5.0   # was implicit, now explicit (BEV reads sustain >>5 MB/s)
CANARY_RECENT_PARKED_MBPS = 0.5      # really stalled, not just 'a bit slow'
# Fleet wall estimate is only published once we've gathered this many
# *quality* observations across distinct peers — so the dashboard
# doesn't claim a confident wall from 3 noisy datapoints.
FLEET_WALL_MIN_QUALITY_OBS = 5
# Time persistence: a peer's slowdown must be continuously observed for
# this many seconds before we count the park as a quality observation.
# 15 min eats roughly through any general internet hiccup or BEV/Zenodo
# blip without delaying real shaping detection meaningfully.
CANARY_QUALITY_PERSIST_S = 15 * 60
# A peer's slowdown streak resets when its ratio recovers ABOVE this
# value (sticky hysteresis around the park threshold of 0.30).
CANARY_SLOWDOWN_RECOVERY_RATIO = 0.60
# Cross-peer correlation: if a fraction ≥ this of peers with usable
# canary samples are currently in slowdown, treat as a fleet-wide
# upstream event — NO peer parked during this window earns a quality
# observation. Soft-park (defensive removal) still fires.
FLEET_CONCURRENT_SLOWDOWN_FRAC = 0.30

# Warmup hold for fresh peers. A brand-new peer has no tile cache and
# zero history; throwing it straight at frontier work means it starts
# by hammering Copernicus/BEV. We let it sit eligible-but-unused for a
# few minutes so the first cache-only or cache-manifest pre-sync can
# warm its local caches before it's asked to do real work.
#
# 'first_seen' is recorded in peers.json the first time the director
# observes a peer. Cache-only is allowed (it's the gentlest workload),
# but frontier promotion is held off for this many seconds.
WARMUP_HOLD_SECONDS = 5 * 60  # 5 minutes

# Cache-manifest sync backoff. After this many consecutive PUT failures
# we skip the peer's manifest sync for SYNC_BACKOFF_SECONDS, mirroring
# bandwidth backoff. Without this, a peer with a flaky network triggers
# a full sync on every start, every tick, blowing latency budgets.
SYNC_BACKOFF_THRESHOLD = 3
SYNC_BACKOFF_SECONDS = 600        # 10 min


def _capacity_history_entry(e: tuple) -> dict:
    """Serialise a capacity_history ring tuple to a compact dict.

    Schema versions (carried in ``v``):
      v=1: legacy (added stl/cpu)
      v=2: + cumulative failed-KG and partial-KG counters (``fk``/``pk``).
           Cumulative (not delta) so a rate-per-hour can be derived over
           any sub-window of the 2 h ring; also makes HA handover &
           gunicorn-worker swaps consistent (cumulative is monotonic and
           survives a re-load from disk + the on-disk failed/partial JSON).

    Supports legacy 7-tuples for in-memory back-compat.
    """
    if len(e) == 7:
        t, f, b, z, c, s, cf = e
        fk = pk = None
    else:
        t, f, b, z, c, s, cf, fk, pk = e
    out: dict = {'t': t, 'f': f, 'bev': b, 'zen': z, 'cop': c}
    if s is not None and cf is not None:
        out['stl'] = s
        out['cpu'] = cf
        # v is bumped to 2 once we also have fk/pk; otherwise stay at 1
        # so the legacy 7-field loader keeps working on older state.
        if fk is not None or pk is not None:
            out['v'] = 2
            if fk is not None:
                out['fk'] = int(fk)
            if pk is not None:
                out['pk'] = int(pk)
        else:
            out['v'] = 1
    return out


def _default_peers_config() -> dict:
    """Default peers.json: this instance plus the ``SRTM_PEERS`` roster."""
    return {
        'budget_gb': BANDWIDTH_BUDGET_GB,
        'renew_day': BANDWIDTH_RENEW_DAY,
        'peers': [
            {
                'id': 'primary',
                'url': None,  # null = this instance (local)
                'enabled': True,
            },
            *_env_peer_rows(),
        ],
    }


def _merge_env_peers(cfg: dict) -> bool:
    """Add ``SRTM_PEERS`` URLs missing from peers.json (never removes rows
    — disabling/removing a peer is an operator action).  Returns True when
    the config changed."""
    rows = cfg.setdefault('peers', [])
    known = {(r.get('url') or '').rstrip('/') for r in rows}
    changed = False
    for r in _env_peer_rows():
        if r['url'] not in known:
            rows.append(r)
            changed = True
    return changed


# === SECTION: operator intent journal (cross-worker) ===
# gunicorn runs 2 worker processes; only one runs the director loop. The
# loop holds ``self.cfg`` in memory for the whole tick and re-saves it
# mid-tick (first_seen stamping, canary parks, role changes). Because
# ``_merge_peers_with_disk`` lets the in-memory row win per-peer, an
# operator action served by the OTHER worker (release-hold / clear-
# cooldown popping ``reserved_kg`` / ``not_before`` and writing disk) is
# silently resurrected by the loop's stale snapshot, then re-read on the
# next reload — so the clear never sticks. The dashboard "release hold"
# button appeared to do nothing for exactly this reason.
#
# Fix: operator mutations append an idempotent *intent* here in addition
# to writing peers.json immediately. The director loop drains intents at
# the top of every tick (after it reloads disk into ``self.cfg``) and
# re-applies them to its own in-memory cfg, so a stale snapshot can no
# longer resurrect a cleared field. Intents are re-applied every tick
# until they expire (default 5 min) — long enough to outlast any single
# tick's mid-flight save, after which the loop's reload already sees the
# clean disk value. Tiny JSON, append+prune, fcntl-locked.
PEER_INTENTS = DATA_DIR / 'peer_intents.json'
PEER_INTENTS_LOCK = PEER_INTENTS.with_suffix('.json.lock')
PEER_INTENT_TTL_S = 300


def record_peer_intent(peer_id: str,
                       set_fields: dict | None = None,
                       clear_fields: list | None = None,
                       ttl_s: int = PEER_INTENT_TTL_S) -> None:
    """Append an operator intent for ``peer_id``.

    ``set_fields`` maps peer-row keys to values to force; ``clear_fields``
    lists keys to remove. Re-applied by the director loop every tick until
    ``ttl_s`` elapses. Safe to call from any gunicorn worker.
    """
    if not peer_id or (not set_fields and not clear_fields):
        return
    import fcntl as _fcntl
    rec = {
        'id': f'{peer_id}-{time.time():.6f}',
        'peer_id': peer_id,
        'set': dict(set_fields or {}),
        'clear': list(clear_fields or []),
        'created': time.time(),
        'expires': time.time() + max(30, int(ttl_s)),
    }
    PEER_INTENTS.parent.mkdir(parents=True, exist_ok=True)
    with open(PEER_INTENTS_LOCK, 'a+') as _lf:
        try:
            _fcntl.flock(_lf.fileno(), _fcntl.LOCK_EX)
        except OSError:
            pass
        try:
            cur = []
            try:
                if PEER_INTENTS.exists():
                    cur = json.loads(PEER_INTENTS.read_text()) or []
            except Exception:
                cur = []
            now = time.time()
            cur = [i for i in cur if isinstance(i, dict)
                   and float(i.get('expires', 0)) > now]
            cur.append(rec)
            tmp = PEER_INTENTS.with_suffix('.json.tmp')
            tmp.write_text(json.dumps(cur, indent=2))
            tmp.replace(PEER_INTENTS)
        finally:
            try:
                _fcntl.flock(_lf.fileno(), _fcntl.LOCK_UN)
            except Exception:
                pass


def apply_peer_intents(cfg: dict) -> bool:
    """Apply all live operator intents to ``cfg`` in place.

    Returns True if any peer row changed. Drops expired intents from
    disk. Intents are NOT removed on apply (kept until expiry) so a
    mid-tick resurrection by a stale director snapshot is re-corrected
    on the following tick.
    """
    import fcntl as _fcntl
    if not PEER_INTENTS.exists():
        return False
    changed = False
    with open(PEER_INTENTS_LOCK, 'a+') as _lf:
        try:
            _fcntl.flock(_lf.fileno(), _fcntl.LOCK_EX)
        except OSError:
            pass
        try:
            try:
                intents = json.loads(PEER_INTENTS.read_text()) or []
            except Exception:
                intents = []
            now = time.time()
            live = [i for i in intents if isinstance(i, dict)
                    and float(i.get('expires', 0)) > now]
            if len(live) != len(intents):
                try:
                    tmp = PEER_INTENTS.with_suffix('.json.tmp')
                    tmp.write_text(json.dumps(live, indent=2))
                    tmp.replace(PEER_INTENTS)
                except Exception:
                    pass
            by_id = {p.get('id'): p for p in (cfg.get('peers') or [])
                     if isinstance(p, dict)}
            for it in live:
                p = by_id.get(it.get('peer_id'))
                if not p:
                    continue
                for k, v in (it.get('set') or {}).items():
                    if p.get(k) != v:
                        p[k] = v
                        changed = True
                for k in (it.get('clear') or []):
                    if k in p:
                        p.pop(k, None)
                        changed = True
        finally:
            try:
                _fcntl.flock(_lf.fileno(), _fcntl.LOCK_UN)
            except Exception:
                pass
    return changed


def load_peers_config() -> dict:
    """Load or create peers config (``SRTM_PEERS`` rows are merged in)."""
    if PEERS_CONFIG.exists():
        try:
            cfg = json.loads(PEERS_CONFIG.read_text())
            if _merge_env_peers(cfg):
                PEERS_CONFIG.write_text(json.dumps(cfg, indent=2))
            return cfg
        except Exception:
            pass
    cfg = _default_peers_config()
    PEERS_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    PEERS_CONFIG.write_text(json.dumps(cfg, indent=2))
    return cfg

# Sidecar fcntl lockfile for peers.json. We hold an exclusive flock
# here for the duration of every save_peers_config() call, so that
# concurrent writers (e.g. a gunicorn worker handling /peers/add and
# the director loop adding ``first_seen`` or a canary park) cannot
# race and silently clobber each other's roster changes. See
# ``save_peers_config`` for the merge-with-disk semantics that build
# on top of this lock.
PEERS_CONFIG_LOCK = PEERS_CONFIG.with_suffix(PEERS_CONFIG.suffix + '.lock')

# How long a removal tombstone keeps a peer id out of the roster.
# Long enough that every stale in-memory snapshot (gunicorn workers,
# director loop, HA shadow snapshot pushes) has been refreshed many
# times over; short enough that ids can eventually be recycled.
PEER_TOMBSTONE_TTL_S = 30 * 86400


def _peer_tombstones(cfg: dict | None) -> dict:
    """Return the ``{peer_id: removed_at_epoch}`` tombstone map."""
    tomb = (cfg or {}).get('removed_peers')
    if not isinstance(tomb, dict):
        return {}
    out = {}
    for pid, ts in tomb.items():
        try:
            out[str(pid)] = float(ts)
        except (TypeError, ValueError):
            continue
    return out


def _merge_peers_with_disk(mem_cfg: dict,
                           disk_cfg: dict | None,
                           removed_ids: set | None = None,
                           readded_ids: set | None = None) -> dict:
    """Reconcile ``mem_cfg`` with the freshly-read ``disk_cfg``.

    Goal: prevent two concurrent writers from silently dropping each
    other's peer-roster changes. Specifically the historical bug where
    ``/api/v1/director/peers/add`` (gunicorn worker) appended ``atNN``
    to its load-time snapshot, while the director loop (different
    worker) was simultaneously stamping ``first_seen`` on its own
    snapshot — whichever finished last clobbered the other.

    Rules:
    * Top-level fields (budget_gb, renew_day, frontier_plan, …) come
      from ``mem_cfg`` — the caller is the latest thinker.
    * Peer list is the union by ``id``. For an id present in both,
      ``mem_cfg`` wins (the caller is mutating that peer's fields).
      For a disk-only id (added by another worker since we loaded),
      the disk row is preserved. For a mem-only id (we just added
      it), it's appended.
    * ``removed_ids`` is the explicit channel for deletions — those
      ids are filtered out of the merged roster regardless of which
      side they appear on. ``remove_peer`` uses this so a removal
      can't be "undone" by a disk-only entry.
    * Deletions are **durable**: every removed id is recorded in a
      ``removed_peers`` tombstone map (id → removal epoch, TTL
      ``PEER_TOMBSTONE_TTL_S``) that is itself merged from both sides
      and re-applied on every save. Without this, a *later* save from
      a writer whose in-memory ``cfg`` predates the removal would
      resurrect the peer via the union rule above — which is exactly
      what happened on 2026-08-04 when 92 shut-down peers were deleted
      one-by-one and the next director tick brought them all back.
      To genuinely re-add a peer, ``director_add_peer`` drops its
      tombstone explicitly.
    * Order: disk order first (stable for operators reading the file),
      mem-only peers appended at the end.
    """
    if not isinstance(disk_cfg, dict):
        disk_cfg = {}
    now = time.time()
    # Merged, TTL-pruned tombstones from both sides + this call's deletions.
    tomb = _peer_tombstones(disk_cfg)
    for pid, ts in _peer_tombstones(mem_cfg).items():
        tomb[pid] = max(ts, tomb.get(pid, 0.0))
    for pid in (removed_ids or ()):
        tomb[str(pid)] = now
    for pid in (readded_ids or ()):
        tomb.pop(str(pid), None)
    tomb = {pid: ts for pid, ts in tomb.items()
            if now - ts < PEER_TOMBSTONE_TTL_S}
    removed = set(removed_ids or ()) | set(tomb)
    out = dict(mem_cfg)
    if tomb:
        out['removed_peers'] = tomb
    else:
        out.pop('removed_peers', None)
    mem_peers = list(mem_cfg.get('peers') or [])
    disk_peers = list(disk_cfg.get('peers') or [])
    mem_by_id = {p.get('id'): p for p in mem_peers if isinstance(p, dict) and p.get('id')}
    disk_by_id = {p.get('id'): p for p in disk_peers if isinstance(p, dict) and p.get('id')}
    seen: set = set()
    merged: list = []
    for pid, dp in disk_by_id.items():
        if pid in removed:
            seen.add(pid)
            continue
        if pid in mem_by_id:
            merged.append(mem_by_id[pid])
        else:
            merged.append(dp)
        seen.add(pid)
    for mp in mem_peers:
        pid = mp.get('id') if isinstance(mp, dict) else None
        if not pid or pid in seen or pid in removed:
            continue
        merged.append(mp)
        seen.add(pid)
    # A tombstone must never outrank an explicit re-add: if the caller
    # kept the peer in mem_cfg *and* dropped its tombstone (see
    # director_add_peer), the loop above already skipped it — so this
    # is only a safety net for tombstones that no longer apply.
    out['peers'] = merged
    return out


def save_peers_config(cfg: dict, removed_ids: set | None = None,
                      readded_ids: set | None = None):
    """Atomically persist ``cfg`` to ``peers.json`` under fcntl lock.

    Concurrent writers (other gunicorn workers, director loop ticks,
    HA shadow snapshots) all funnel through the same lockfile, so a
    given save is serialised against every other save. Within the
    locked region we re-read the on-disk cfg and ``_merge_peers_with_disk``
    it with the caller's ``cfg`` so disk-only peers (added by some
    other worker since this caller loaded) are preserved.

    Deletions: pass ``removed_ids={'atNN', ...}`` — the merge will
    filter those ids out of the union regardless of which side they
    were on. A plain ``save_peers_config(cfg)`` will NOT delete a peer
    that's also on disk; this prevents stale snapshots from quietly
    dropping peers. Deletions are also persisted as durable
    ``removed_peers`` tombstones (TTL ``PEER_TOMBSTONE_TTL_S``) so a
    later save from a writer holding a pre-removal snapshot can't
    resurrect the peer. Pass ``readded_ids={'atNN'}`` to clear a
    tombstone when genuinely re-adding a peer.
    """
    PEERS_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    import fcntl as _fcntl
    import tempfile as _tempfile
    # Open lockfile (create if missing). Keep open for the whole
    # critical section; flock is per-fd and released on close.
    with open(PEERS_CONFIG_LOCK, 'a+') as _lf:
        try:
            _fcntl.flock(_lf.fileno(), _fcntl.LOCK_EX)
        except OSError:
            # Filesystem doesn't support flock — fall back to a
            # best-effort unlocked write. Better than crashing.
            disk = None
            try:
                if PEERS_CONFIG.exists():
                    disk = json.loads(PEERS_CONFIG.read_text())
            except Exception:
                disk = None
            merged = _merge_peers_with_disk(cfg, disk, removed_ids,
                                            readded_ids)
            PEERS_CONFIG.write_text(json.dumps(merged, indent=2, default=str))
            return
        try:
            disk = None
            try:
                if PEERS_CONFIG.exists():
                    disk = json.loads(PEERS_CONFIG.read_text())
            except Exception:
                disk = None
            merged = _merge_peers_with_disk(cfg, disk, removed_ids,
                                            readded_ids)
            fd, tmp = _tempfile.mkstemp(
                dir=str(PEERS_CONFIG.parent), suffix='.tmp')
            try:
                with os.fdopen(fd, 'w') as f:
                    json.dump(merged, f, indent=2, default=str)
                os.replace(tmp, PEERS_CONFIG)
            except BaseException:
                try:
                    os.unlink(tmp)
                except Exception:
                    pass
                raise
        finally:
            try:
                _fcntl.flock(_lf.fileno(), _fcntl.LOCK_UN)
            except Exception:
                pass


def load_director_state_ro() -> dict:
    """Read-only, mtime-cached view of ``director_state.json`` for hot
    request paths (``/director/heartbeat`` is hit ~50×/min by the fleet
    and used to re-parse the 5 MB file every time).  Never mutate."""
    try:
        import json_cache
        d = json_cache.load(DIRECTOR_STATE)
        if isinstance(d, dict):
            return d
    except Exception:
        pass
    return load_director_state()


def load_director_state() -> dict:
    """Load persisted director state (fresh mutable copy)."""
    if DIRECTOR_STATE.exists():
        try:
            return json.loads(DIRECTOR_STATE.read_text())
        except Exception:
            pass
    return {
        'active_peer': None,
        'last_switch': None,
        'peer_bandwidth': {},  # peer_id -> {used_bytes, cycle_start, last_check}
        'mode': 'auto',  # auto | manual | paused
    }


# Per-peer-keyed maps inside director_state.json. Removing a peer must
# drop its rows or director_state.json grows without bound (it hit 5 MB
# / 125 peer keys before the Aug-2026 fleet shutdown, most of it dead).
# Lists (capacity_history, ip_pools_history, ...) are fleet-wide rings
# and intentionally NOT in this set.
_PEER_KEYED_STATE_MAPS = (
    'peer_bandwidth', '_bandwidth_backoff', '_bandwidth_misses',
    '_peer_caps', 'peer_last_live_ts', 'peer_warning_rates',
    'peer_noise_long_ema', 'peer_steal_ema', 'peer_steal_streak',
    'canary_history', 'canary_slowdown_streaks', 'bw_cycle',
    'stuck_kg_track', 'dup_kg_track', 'frontier_hog_release',
    '_frontier_restart_log', '_cred_bootstrap_at', 'peer_update_state',
    'unreachable_count', 'parallel_unreachable_count',
    'frontier_cred_plan', 'frontier_strip_plan', 'cache_only_assigned',
    'graceful_stop_sent', 'bev_pool_escalation',
)

# Fleet-membership lists in director_state.json (values are peer ids).
_PEER_KEYED_STATE_LISTS = (
    'cache_only_active', 'parallel_frontiers_active',
)


def _gc_peer_state(state: dict, dead_ids: set) -> int:
    """Drop every per-peer row for ``dead_ids`` from ``state``.

    Returns the number of entries removed. Purely a size/hygiene
    optimisation — nothing depends on these rows existing, and every
    consumer already tolerates a missing peer key (the maps are
    lazily repopulated on the next sample for live peers).
    """
    if not dead_ids:
        return 0
    dropped = 0
    for key in _PEER_KEYED_STATE_MAPS:
        m = state.get(key)
        if not isinstance(m, dict):
            continue
        for pid in dead_ids:
            if m.pop(pid, None) is not None:
                dropped += 1
    for key in _PEER_KEYED_STATE_LISTS:
        lst = state.get(key)
        if not isinstance(lst, list):
            continue
        kept = [x for x in lst if x not in dead_ids]
        if len(kept) != len(lst):
            dropped += len(lst) - len(kept)
            state[key] = kept
    for key in ('active_peer', 'shadow_peer'):
        if state.get(key) in dead_ids:
            state[key] = None
    return dropped


# Operator-set director mode (auto | manual | paused) lives in its own
# sidecar and is authoritative over whatever `mode` a director_state.json
# snapshot carries. Reason (2026-09-13 incident): /director/stop ran in
# gunicorn worker A and wrote mode=paused via A's stale state copy; the
# director loop in worker B only re-reads mode at tick START, and any of
# its ~16 mid-tick save_director_state() calls wrote its in-memory
# mode='auto' straight back — the pause silently undid itself and the
# fleet restarted within minutes. Every save now re-stamps mode from
# this file, and every mode gate reads it directly.
DIRECTOR_MODE_FILE = DATA_DIR / 'director_mode.json'
_VALID_MODES = ('auto', 'manual', 'paused')


def read_director_mode(default: str = 'auto') -> str:
    try:
        m = (json.loads(DIRECTOR_MODE_FILE.read_text()) or {}).get('mode')
        if m in _VALID_MODES:
            return m
    except Exception:
        pass
    return default


def write_director_mode(mode: str):
    if mode not in _VALID_MODES:
        raise ValueError(f'bad mode {mode!r}')
    DIRECTOR_MODE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = DIRECTOR_MODE_FILE.with_suffix('.json.tmp')
    tmp.write_text(json.dumps({
        'mode': mode,
        'set_at': datetime.now(timezone.utc).isoformat(),
    }))
    os.replace(tmp, DIRECTOR_MODE_FILE)


def save_director_state(state: dict):
    DIRECTOR_STATE.parent.mkdir(parents=True, exist_ok=True)
    import tempfile
    # Authoritative operator intent wins over any worker's snapshot.
    if DIRECTOR_MODE_FILE.exists():
        state['mode'] = read_director_mode(state.get('mode', 'auto'))
    fd, tmp = tempfile.mkstemp(dir=DIRECTOR_STATE.parent, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(state, f, indent=2, default=str)
        os.replace(tmp, DIRECTOR_STATE)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _peer_renewal_events(peer_id: str, state: dict) -> list:
    """Return the per-peer renewal event log.

    Each entry is [ts, prev_gb, new_gb] appended in-flight by
    ``_sample_canary_history`` when cumulative used_bytes drops. Tiny
    on disk (≤ BW_RENEWAL_EVENTS_MAX per peer).
    """
    rec = ((state or {}).get('bw_cycle') or {}).get(peer_id) or {}
    return rec.get('events') or []


def _learned_renew_day(peer_id: str, state: dict) -> int | None:
    """Most recent renewal day-of-month observed for this peer.

    Reads the event log written by ``_sample_canary_history``. Returns
    None if no renewal has been observed yet (peer too young, no drop
    visible).
    """
    events = _peer_renewal_events(peer_id, state)
    if not events:
        return None
    ts_after = int(events[-1][0])
    return datetime.fromtimestamp(ts_after, tz=timezone.utc).day


def _peer_renew_day(peer: dict, cfg: dict) -> int:
    """Effective renew day for a peer.

    Operator override > observed BW drop > stored renew_day >
    day-of-month of first_seen > global cfg renew_day. Day is clamped
    to 1..28 so it always resolves on every month.
    """
    rd = peer.get('renew_day_override') or peer.get('renew_day_learned')
    if not rd:
        rd = peer.get('renew_day')
    if not rd:
        fs = peer.get('first_seen')
        if isinstance(fs, str) and len(fs) >= 10:
            try:
                rd = int(fs[8:10])
            except (ValueError, TypeError):
                rd = None
    if not rd:
        rd = cfg.get('renew_day', BANDWIDTH_RENEW_DAY)
    try:
        rd = max(1, min(28, int(rd)))
    except (TypeError, ValueError):
        rd = BANDWIDTH_RENEW_DAY
    return rd


def _peer_next_renew(peer: dict, cfg: dict,
                    *, now: datetime | None = None) -> datetime:
    """Return the next billing-cycle renewal datetime for a peer (UTC).

    Uses the peer's effective renew_day. If today's renew day hasn't
    passed yet this month, returns this month's anchor at 00:00 UTC;
    otherwise next month's. Day is clamped to 1..28.
    """
    rd = _peer_renew_day(peer, cfg)
    n = now or datetime.now(timezone.utc)
    candidate = n.replace(day=rd, hour=0, minute=0, second=0, microsecond=0)
    if candidate <= n:
        m = n.month + 1
        y = n.year + (1 if m > 12 else 0)
        m = ((m - 1) % 12) + 1
        candidate = candidate.replace(year=y, month=m)
    return candidate


# Bandwidth thresholds for the active frontier peer.
#
# When the active peer is below LOW_WATER_GRACEFUL_GB remaining and it
# is currently mid-KG, we send a *graceful* stop (finish-then-exit) so
# the in-flight KG gets uploaded instead of being killed and retried.
# When it drops below HARD_DEPLETED_GB we treat it as depleted and
# rotate even mid-KG (credential safety > 1 KG of work). Park the peer
# until its next renewal so the director's normal scheduler skips it
# without us having to track per-peer budget state separately.
BANDWIDTH_LOW_WATER_GB = 4.0
BANDWIDTH_HARD_DEPLETED_GB = 1.0


def get_billing_cycle_start(renew_day: int | None = None) -> datetime:
    """Return the start of the current billing cycle for a given anchor day.

    If ``renew_day`` is None we fall back to the global
    ``BANDWIDTH_RENEW_DAY`` (17). When called from ``get_local_bandwidth``
    on a peer that has an effective renew_day in ``peers.json``, that
    value is passed in so the vnstat sum cuts on the *peer's* cycle,
    not on the global anchor — critical once peers' learned days start
    diverging from 17 (a peer first seen on day 3 of a future month
    will have ``renew_day_learned=3``, and its vnstat used_bytes must
    reset there, not wait 14 days for the global day 17).
    """
    now = datetime.now(timezone.utc)
    day = int(renew_day) if renew_day else BANDWIDTH_RENEW_DAY
    day = max(1, min(28, day))
    if now.day >= day:
        return now.replace(day=day, hour=0, minute=0, second=0, microsecond=0)
    # Before the anchor day — cycle started last month.
    if now.month == 1:
        return now.replace(year=now.year - 1, month=12, day=day,
                           hour=0, minute=0, second=0, microsecond=0)
    return now.replace(month=now.month - 1, day=day,
                       hour=0, minute=0, second=0, microsecond=0)




def _local_peer_renew_day() -> int | None:
    """Resolve this VM's effective renew_day from local peers.json.

    Returns None when the peer has no anchor of its own — the caller
    then falls back to ``BANDWIDTH_RENEW_DAY``. Cheap (single JSON read,
    ~few KB) and tolerant: any failure returns None so vnstat math
    continues using the global anchor exactly as before.
    """
    sid = config.self_id()
    try:
        cfg = load_peers_config()
    except Exception:
        return None
    me = get_peer_by_id(cfg, sid)
    if not me:
        return None
    # Prefer evidence (override > learned > stored). Skip the
    # first_seen day-of-month fallback here — it's a guess, and we'd
    # rather use the global anchor than promote a guess to a per-peer
    # cycle that's then invisible to the operator.
    rd = (me.get('renew_day_override')
          or me.get('renew_day_learned')
          or me.get('renew_day'))
    try:
        return int(rd) if rd else None
    except (TypeError, ValueError):
        return None


def get_local_bandwidth(renew_day: int | None = None) -> dict:
    """Get bandwidth usage from local vnstat.

    ``renew_day`` (1..28) anchors the billing cycle. Pass the peer's
    effective renew_day when known so used_bytes resets on the right
    day. When omitted, falls back to ``_local_peer_renew_day()`` (own
    peers.json entry) and finally the global ``BANDWIDTH_RENEW_DAY``.
    """
    if renew_day is None:
        renew_day = _local_peer_renew_day()
    try:
        result = subprocess.run(
            ['vnstat', '--json', 'm'],
            capture_output=True, text=True, timeout=5
        )
        data = json.loads(result.stdout)
        iface = data['interfaces'][0]
        now = datetime.now(timezone.utc)
        cycle_start = get_billing_cycle_start(renew_day)

        # Sum traffic from cycle_start to now
        total_rx = 0
        total_tx = 0
        for m in iface.get('traffic', {}).get('month', []):
            dt = m['date']
            month_date = datetime(dt['year'], dt['month'], 1, tzinfo=timezone.utc)
            # Include months that overlap with the billing cycle
            if month_date.year == cycle_start.year and month_date.month == cycle_start.month:
                total_rx += m['rx']
                total_tx += m['tx']
            elif month_date.year == now.year and month_date.month == now.month:
                if now.month != cycle_start.month:  # avoid double-counting
                    total_rx += m['rx']
                    total_tx += m['tx']

        total = total_rx + total_tx
        return {
            'used_bytes': total,
            'used_gb': round(total / (1024 ** 3), 2),
            'rx_bytes': total_rx,
            'tx_bytes': total_tx,
            'budget_gb': BANDWIDTH_BUDGET_GB,
            'remaining_gb': round(max(0, BANDWIDTH_BUDGET_BYTES - total) / (1024 ** 3), 2),
            'pct_used': round(100 * total / BANDWIDTH_BUDGET_BYTES, 1),
            'cycle_start': cycle_start.isoformat(),
            'cycle_renew_day': cycle_start.day,
            'checked_at': now.isoformat(),
        }
    except Exception as e:
        log.warning('Failed to read local bandwidth: %s', e)
        return {'error': str(e), 'used_bytes': 0, 'used_gb': 0,
                'remaining_gb': BANDWIDTH_BUDGET_GB, 'pct_used': 0}


def get_peer_bandwidth(peer_url: str, peer_id: str = '') -> dict:
    """Get bandwidth from a remote peer.  Short timeout — must not block the loop."""
    pid = peer_id or _peer_id_for_url(peer_url)
    # Prefer pushed bandwidth if fresh — saves an HTTP round-trip.
    pushed = get_pushed_bandwidth(pid)
    if pushed is not None:
        out = dict(pushed)
        out['_pushed'] = True
        return out
    try:
        r = _peer_request(
            'GET',
            peer_url.rstrip('/') + '/api/v1/bandwidth',
            peer_id=pid,
            timeout=PEER_TIMEOUT_PROBE,
        )
        if r is None:
            return {'error': 'circuit_open', 'used_bytes': 0, 'used_gb': 0,
                    'remaining_gb': BANDWIDTH_BUDGET_GB, 'pct_used': 0,
                    'estimated': True, '_circuit_open': True}
        r.raise_for_status()
        return r.json()
    except Exception as e:
        # Don't log here — the caller (_update_bandwidth) decides whether to
        # warn based on the consecutive-failure count.  Avoids log spam when
        # a peer is wedged.
        return {'error': str(e), 'used_bytes': 0, 'used_gb': 0,
                'remaining_gb': BANDWIDTH_BUDGET_GB, 'pct_used': 0,
                'estimated': True}


# Last-known peer status cache. Busy peers (e.g. running heavy GPKG
# builds) sometimes take >8s to respond on /processing/status, which
# blows past PEER_TIMEOUT_PROBE and makes the dashboard report them as
# 'unreachable, completed=0' even though they're actively working. We
# stash the last successful response per peer_url and surface it (with
# state='busy') when a fresh poll times out, as long as it's recent.
_PEER_STATUS_CACHE: dict[str, tuple[float, dict]] = {}
_PEER_STATUS_CACHE_TTL = 300.0   # 5 min — covers a slow GPKG step.
_PEER_CAPS_TTL_S = 1800.0        # /info + /credentials cap probe; also keyed on peer commit
# Status-push cache: peers POST /api/v1/director/peer_status every
# PEER_PUSH_INTERVAL seconds. The director loop prefers this over
# pulling /processing/status; polls only when push is stale or
# unavailable. Slashes outbound director traffic ~50x at fleet scale.
PEER_PUSH_INTERVAL = 30          # seconds (peers run a ticker)
PEER_PUSH_FRESH_S = 100          # consider push fresh for this long (cache-only peers push every 60 s)
_PEER_PUSH: dict[str, dict] = {}  # peer_id -> {ts, status, bandwidth}
_RECENT_LOG_RING = 200  # matches austria_processor's recent_log cap


def pushed_recent_log_wm(peer_id: str) -> str | None:
    """Newest recent_log ts we hold for *peer_id* (from the freshest
    push on either worker), or None. Returned in the peer_status ack so
    the peer can ship only newer lines next time. None → peer sends the
    full ring (director restart / failover self-heals)."""
    try:
        # No freshness gate here (idle peers push full status only every
        # 240 s, so get_pushed_status() would call them stale).
        with _PEER_PUSH_LOCK:
            ent = _PEER_PUSH.get(peer_id)
        try:
            fp = _PEER_PUSH_DIR / f'{peer_id}.json'
            if fp.stat().st_mtime > float((ent or {}).get('ts') or 0.0) + 0.5:
                d = json.loads(fp.read_text())
                if isinstance(d, dict) and d.get('ts'):
                    ent = d
        except Exception:
            pass
        rl = ((ent or {}).get('status') or {}).get('recent_log') or []
        mx = max((e.get('ts') or '' for e in rl if isinstance(e, dict)), default='')
        return mx or None
    except Exception:
        return None
_PEER_PUSH_DIR = Path('/tmp/srtm_peer_push')   # cross-worker mirror
_PEER_PUSH_LOCK = threading.Lock()


# Status-fields that are bandwidth-shipped only once per process
# lifetime (constants until the peer restarts). The director merges
# them from the previous push so heartbeats don't lose them.
_STICKY_SYSTEM_FIELDS = ('host',)


# --- v2 upgrade strikes, fleet-wide union -------------------------------
# Peers ship their local ``v2_upgrade_failed.json`` as ``status.v2_strikes``
# ({code: n}) on every full push. The director folds them into one file so
# ``_compute_v2_upgrade_candidates`` can skip codes whose strikes *summed
# across peers* reached V2_UPGRADE_MAX_STRIKES (a fresh-KG verify FAIL on
# the frontier + one upgrade FAIL on a cache peer = struck out). File, not
# memory: peer pushes land on either gunicorn worker; the director loop
# runs in one of them. flock'd read-merge-write, only when a peer's map
# actually changed (a few writes/day fleet-wide).
V2_FLEET_STRIKES_FILE = DATA_DIR / 'v2_strikes_fleet.json'
V2_UPGRADE_MAX_STRIKES = 2      # mirror austria_processor.V2_UPGRADE_MAX_STRIKES


def _v2_fleet_strikes_merge(peer_id: str, strikes: dict) -> None:
    import fcntl as _fcntl
    try:
        V2_FLEET_STRIKES_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(V2_FLEET_STRIKES_FILE.with_suffix('.lock'), 'w') as lk:
            _fcntl.flock(lk, _fcntl.LOCK_EX)
            try:
                d = json.loads(V2_FLEET_STRIKES_FILE.read_text()) if V2_FLEET_STRIKES_FILE.exists() else {}
                if not isinstance(d, dict):
                    d = {}
            except Exception:
                d = {}
            now = time.time()
            # drop this peer's stale entries, then write the fresh map
            for code in list(d.keys()):
                pe = d[code]
                if isinstance(pe, dict) and peer_id in pe and str(code) not in strikes:
                    pe.pop(peer_id, None)
                    if not pe:
                        d.pop(code, None)
            for code, n in strikes.items():
                d.setdefault(str(code), {})[peer_id] = {'n': int(n), 'ts': now}
            tmp = V2_FLEET_STRIKES_FILE.with_suffix('.tmp')
            tmp.write_text(json.dumps(d, separators=(',', ':')))
            tmp.replace(V2_FLEET_STRIKES_FILE)
    except Exception as e:
        log.debug('v2 fleet strikes merge (%s): %s', peer_id, e)


def v2_fleet_strikes() -> dict:
    """{code: {peer_id: {n, ts}}} — the union of every peer's strikes."""
    try:
        if V2_FLEET_STRIKES_FILE.exists():
            d = json.loads(V2_FLEET_STRIKES_FILE.read_text())
            return d if isinstance(d, dict) else {}
    except Exception:
        pass
    return {}


# === SECTION: v2_regen — auto-requeue KGs whose v1 full GPKG is stably gone ===
# A v2 upgrade needs the v1 ``_full_gpkg``. When a peer finds it *gone*
# (deposition alive, file missing → 404) it strikes the code out and
# reports it as ``status.v2_gone``. The director collects those here,
# verifies the absence itself against the deposition API — never while
# the fleet Zenodo-degraded circuit is tripped, and twice ≥30 min apart —
# then pushes the code through the canonical ``POST /processing/queue``
# with ``keep_products`` so the v1 ``_json`` / ``_light_gpkg`` stay
# referenced until the fresh v2.2 run replaces them in place. State is a
# small disk file (cross-worker, like v2_priority.json); every transition
# is a ``v2regen:`` director_event; ``/process.txt`` has a ``v2_regen:``
# line; structured twin at director/status.v2.regen.
V2_REGEN_FILE = DATA_DIR / 'v2_regen.json'
V2_REGEN_CHECK_INTERVAL_S = 300
V2_REGEN_CONFIRM_GAP_S = 1800
V2_REGEN_CONFIRMATIONS = 2
V2_REGEN_MAX_REQUEUE_PER_TICK = 3
V2_REGEN_MAX_OUTSTANDING = 20
V2_REGEN_DONE_TTL_S = 7 * 86400

# === SECTION: v2 orphan sweep (parent-level _json_v2 residue) ===
# Sep 2026: before the upload-chain ordering fix, a fresh run whose
# _full_gpkg / _light_gpkg_v2 upload timed out still landed the tiny
# ``<code>_json_v2`` before the KG was re-queued. The re-run then went
# through the splitter and produced complete *block* products, leaving a
# parent-level ``_json_v2`` with no sibling ``_json`` / ``_full_gpkg`` /
# ``_light_gpkg_v2``. Nothing can heal it (no full GPKG to upgrade from,
# coverage oracle already complete via the blocks) and it poisons the
# telemetry: v2_ingest strikes out on it (``verify_fail``) and the
# parents_by_version histogram reports it as a stale v2.x parent.
# The sweep removes such entries — Zenodo deposition first, manifest +
# tombstone second — only after verifying against the deposition API
# that the depo holds exactly that one ``_v2.json.gz`` file.
V2_ORPHAN_CHECK_INTERVAL_S = 1800
V2_ORPHAN_MIN_AGE_S = 24 * 3600
V2_ORPHAN_MAX_PER_TICK = 3
# json_heal: rebuild missing v1 ``_json`` completion markers from ``_json_v2``
# (json_heal.py). State file so both gunicorn workers render the same line.
JSON_HEAL_INTERVAL_S = 600
JSON_HEAL_STATE = DATA_DIR / 'json_heal_state.json'

# === SECTION: empty-draft sweep ===
# Drafts on Zenodo with NO title, NO files, NOT referenced by any manifest
# (products / cache / shards / chkpt) and older than EMPTY_DRAFT_MIN_AGE_S.
# Historic cause: zenodo_client created the draft with `{}` metadata and
# only set title + file afterwards, so a failed bucket PUT (lossy circuit)
# left an untitled, empty draft behind (54 of them on 2026-10-08). The
# client now attaches metadata at creation + discards on failure; this
# sweep retires the backlog and acts as the safety net. Conservative on
# purpose: positive evidence only (HTTP 200 + empty title + zero files),
# capped per tick, hourly, director-loop worker only.
EMPTY_DRAFT_CHECK_INTERVAL_S = 3600
EMPTY_DRAFT_MIN_AGE_S = 6 * 3600
EMPTY_DRAFT_MAX_PER_TICK = 10
EMPTY_DRAFT_LIST_PAGES = 10
EMPTY_DRAFT_MAX_PAGE = 120   # rotating window end (~12k most recent drafts)

# === SECTION: lost-upload reconciler (drafts WITH a product file, no manifest) ===
# Oct 2026 (07218-north): the subprocess streamed ``07218-north_full.gpkg``
# (623 MB) to depo 23452088, the bucket confirmed it, ``Manifest.set`` ran —
# and the record was gone by the time the parent reloaded the manifest
# (lost in a concurrent manifest rewrite on the peer). Nothing could
# recover from that: the code's ``_json`` is load-bearing (zen_stall
# ``orphan*``), so no sweep ever requeues it, and the full GPKG sat on
# Zenodo unreferenced. The reconciler rides the empty-draft listing: a
# titled product draft whose id is in NO manifest, holding exactly one
# file whose name maps to a product key the manifest lacks, ≥6 h old,
# code not queued / running → adopt it into the manifest (bucket, md5,
# size, Zenodo's own timestamp). ``status=cf`` only when the code's own
# ``_json`` is ``cf`` (same 2.4 run), else the legacy audit decides.
# Legacy ``_light.gpkg`` and duplicates of a committed key are left alone
# (listed for the operator in the director event). Capped per tick.
LOST_UPLOAD_MAX_PER_TICK = 10
LOST_UPLOAD_FILE_KEYS = (            # filename suffix -> manifest key suffix
    ('_full.gpkg', '_full_gpkg'),
    ('_light_v2.gpkg', '_light_gpkg_v2'),
    ('_v2.json.gz', '_json_v2'),
    ('_ne.h3b', '_ne_h3b'),
    ('.json', '_json'),
)


def _v2_regen_orphan(code: str, ent: dict) -> bool:
    """True when *code*'s products are old-split residue: the parent KG's
    bbox is covered by committed ``_json`` products WITHOUT any of this
    code's own entries (``app._kg_coverage_complete``).  Such a code can
    never be regenerated usefully — ``GET /processing/queue`` prunes it as
    complete on the next poll (the requeue-revert loop of 72321 / 03123 /
    48210-east+west, Oct 2026: requeued → pruned → "reverted" → requeued …
    with a wasted ``v2up: … gone from Zenodo`` dispatch each round).
    Block codes: the parent is covered by a newer unsplit run or a newer
    layout.  Parent codes: the blocks cover it.  Conservative: any error →
    False (not an orphan)."""
    try:
        import app as _app
        code = str(code)
        parent = code.split('-', 1)[0]
        ent2 = {k: v for k, v in ent.items() if not k.startswith(code + '_')}
        if code == parent and not any(k.startswith(parent + '-') and k.endswith('_json') for k in ent2):
            return False      # unsplit parent with nothing else covering it
        ok, _ts = _app._kg_coverage_complete(parent, ent2)
        return bool(ok)
    except Exception:
        return False


def _v2_regen_drop_orphan_queue(code: str) -> None:
    """Remove an orphan code from retry_queue.json + its ``_requeue``
    tombstone so the prune/requeue loop ends (best-effort)."""
    try:
        qpath = DATA_DIR / 'retry_queue.json'
        q = json.loads(qpath.read_text()) if qpath.exists() else []
        if code in q:
            q = [c for c in q if c != code]
            qpath.write_text(json.dumps(q))
    except Exception:
        pass
    try:
        import app as _app
        _app._drop_tombstone_keys([f'{code}_requeue'])
    except Exception:
        pass


def _v2_regen_load() -> dict:
    try:
        d = json.loads(V2_REGEN_FILE.read_text()) if V2_REGEN_FILE.exists() else {}
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _v2_regen_save(d: dict) -> None:
    try:
        tmp = V2_REGEN_FILE.with_suffix('.tmp')
        tmp.write_text(json.dumps(d, indent=1, sort_keys=True))
        tmp.replace(V2_REGEN_FILE)
    except Exception as e:
        log.warning('v2_regen save: %s', e)


_V2_REGEN_LOCK = threading.Lock()


def v2_regen_add_candidates(peer_id: str, gone: dict) -> None:
    """Record ``{code: ts}`` reported by *peer_id* as regen candidates."""
    import re as _re
    with _V2_REGEN_LOCK:
        d = _v2_regen_load()
        changed = False
        now = datetime.now(timezone.utc).isoformat(timespec='seconds')
        for code, ts in list(gone.items())[:100]:
            code = str(code)
            if not _re.match(r'^\d+(-[a-z][-a-z0-9]*)?$', code):
                continue
            # value is a ts string (pre-2026-09-22 peers) or {ts, key}
            key = f'{code}_full_gpkg'
            if isinstance(ts, dict):
                k = str(ts.get('key') or '')
                if k in (f'{code}_full_gpkg', f'{code}_json'):
                    key = k
            e = d.get(code)
            if e is None:
                d[code] = {'state': 'candidate', 'key': key,
                           'first_seen': now, 'peers': [peer_id], 'checks': []}
                changed = True
            elif e.get('state') in ('candidate', 'confirming') and peer_id not in (e.get('peers') or []):
                e.setdefault('peers', []).append(peer_id)
                changed = True
        if changed:
            _v2_regen_save(d)


def v2_regen_summary() -> dict:
    d = _v2_regen_load()
    by = {}
    for e in d.values():
        by[e.get('state', '?')] = by.get(e.get('state', '?'), 0) + 1
    rows = sorted(((c, e) for c, e in d.items() if e.get('state') != 'done'),
                  key=lambda ce: ce[1].get('first_seen', ''))
    return {'counts': by, 'total': len(d),
            'rows': [{'code': c, 'state': e.get('state'),
                      'checks': len(e.get('checks') or []),
                      'peers': len(e.get('peers') or []),
                      'requeued_at': e.get('requeued_at'),
                      'note': e.get('note')} for c, e in rows[:20]]}


def v2_fleet_struck_out(threshold: int = V2_UPGRADE_MAX_STRIKES) -> set:
    """Codes whose strikes summed over peers reached *threshold*."""
    out = set()
    for code, pe in v2_fleet_strikes().items():
        if isinstance(pe, dict) and sum(int((e or {}).get('n', 0)) for e in pe.values()) >= threshold:
            out.add(str(code))
    return out


def record_peer_push(peer_id: str, status: dict,
                     bandwidth: dict | None = None) -> None:
    """Record a peer-pushed status payload. Called from the HTTP handler.

    Merges 'sticky' fields (currently ``system.host``) from the
    previous push so idle heartbeats — which intentionally omit
    constants to save bandwidth — don't erase them. Without this the
    fleet-profile view would lose cpu_model / sys_vendor as soon as a
    peer went idle.
    """
    if not peer_id:
        return
    status = status or {}
    with _PEER_PUSH_LOCK:
        prev = _PEER_PUSH.get(peer_id) or {}
    # The freshest previous push may live on the sibling worker: consult
    # the disk mirror (mtime-gated) so a recent_log *delta* is merged
    # onto the right base regardless of which worker accepted it.
    try:
        fp = _PEER_PUSH_DIR / f'{peer_id}.json'
        if fp.stat().st_mtime > float(prev.get('ts') or 0.0) + 0.5:
            d = json.loads(fp.read_text())
            if isinstance(d, dict) and float(d.get('ts') or 0) > float(prev.get('ts') or 0):
                prev = d
    except Exception:
        pass
    with _PEER_PUSH_LOCK:
        prev_status = prev.get('status') or {}
        prev_sys = prev_status.get('system') or {}
        new_sys = status.get('system') or {}
        # recent_log delta (Sep 2026 bandwidth pass): the peer only ships
        # log lines newer than the watermark we acked on its previous
        # push (``recent_log_delta`` = that watermark). Rebuild the full
        # ring here so every consumer (combined log, proxy/status,
        # dashboard) sees exactly what a full push would have carried.
        _delta_wm = status.get('recent_log_delta')
        if _delta_wm and isinstance(status.get('recent_log'), list):
            _prev_rl = prev_status.get('recent_log') or []
            _seen = set()
            _merged_rl = []
            for e in list(_prev_rl) + list(status['recent_log']):
                if not isinstance(e, dict):
                    continue
                k = (e.get('ts') or '', e.get('msg') or '', e.get('level') or '')
                if k in _seen:
                    continue
                _seen.add(k)
                _merged_rl.append(e)
            _merged_rl.sort(key=lambda e: e.get('ts') or '')
            status = dict(status)
            status['recent_log'] = _merged_rl[-_RECENT_LOG_RING:]
            status.pop('recent_log_delta', None)
        # Pull sticky fields forward when the new payload lacks them.
        merged_sys = dict(new_sys)
        for k in _STICKY_SYSTEM_FIELDS:
            if k not in merged_sys and k in prev_sys:
                merged_sys[k] = prev_sys[k]
        if merged_sys:
            status = dict(status)
            status['system'] = merged_sys
        _ent = {
            'ts': time.time(),
            'status': status,
            'bandwidth': bandwidth,
        }
        _PEER_PUSH[peer_id] = _ent
        _new_strikes = status.get('v2_strikes')
        _old_strikes = prev_status.get('v2_strikes')
    # Cross-worker share: a push lands on whichever gunicorn worker
    # accepted the connection, but the director loop / all_status /
    # get_peer_status run in either. Without this the loop worker saw
    # only ~half the pushes, judged them stale (>75 s) and fell back to
    # polling /processing/status (~30 GETs/min fleet-wide). ~2-5 KB
    # atomic write per push; read side is mtime-gated.
    try:
        _PEER_PUSH_DIR.mkdir(parents=True, exist_ok=True)
        _tmp = _PEER_PUSH_DIR / f'.{peer_id}.tmp'
        _tmp.write_text(json.dumps(_ent, default=str))
        os.replace(_tmp, _PEER_PUSH_DIR / f'{peer_id}.json')
    except Exception:
        pass
    if isinstance(_new_strikes, dict) and _new_strikes != _old_strikes:
        _v2_fleet_strikes_merge(peer_id, _new_strikes)
    note_put_progress(peer_id, status, now=_ent['ts'])
    _gone = status.get('v2_gone')
    if isinstance(_gone, dict) and _gone and _gone != prev_status.get('v2_gone'):
        v2_regen_add_candidates(peer_id, _gone)


# --- fleet Zenodo PUT rate (throughput-watchdog floor) -------------------
# Every uploading peer already pushes ``current_kg.upload_progress``
# ({file, sent, total}); the director turns consecutive pushes into a
# per-upload span rate, takes the median over the uploads active in the
# last 5 min, smooths it over a 15 min history and hands the result back
# in the peer_status *ack* (~30 B — no new poll, no new push).  Cross-
# worker via a small flock'd file next to the push mirror.  Consumed by
# ``zenodo_client.fleet_put_floor()`` (peer-local mirror written by
# ``app._peer_status_push_loop``; the primary mirrors it itself here).
_FLEET_PUT_SAMPLES = _PEER_PUSH_DIR / '_put_rates.json'
_FLEET_PUT_LOCK = _PEER_PUSH_DIR / '_put_rates.lock'
_FLEET_PUT_LOCAL = DATA_DIR / 'zenodo_fleet_put.json'
FLEET_PUT_ACTIVE_S = 300        # a sample counts while its last push is this fresh
FLEET_PUT_MIN_SPAN_S = 60       # ≥ 2 pushes apart before a rate is trusted
FLEET_PUT_HIST_S = 15 * 60      # rolling median of per-tick medians
ZENODO_PUT_FLOOR_ABS_KBPS = 20.0
ZENODO_PUT_FLOOR_FRAC = 0.25
_fleet_put_cache: dict = {'at': 0.0, 'v': {}}


def _fleet_put_rw(mutate):
    """flock'd read-modify-write of the samples file; returns the dict."""
    import fcntl
    _PEER_PUSH_DIR.mkdir(parents=True, exist_ok=True)
    with open(_FLEET_PUT_LOCK, 'a+') as lf:
        fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
        try:
            try:
                d = json.loads(_FLEET_PUT_SAMPLES.read_text())
                if not isinstance(d, dict):
                    d = {}
            except Exception:
                d = {}
            if mutate(d):
                tmp = _FLEET_PUT_SAMPLES.with_suffix('.tmp')
                tmp.write_text(json.dumps(d))
                os.replace(tmp, _FLEET_PUT_SAMPLES)
            return d
        finally:
            fcntl.flock(lf.fileno(), fcntl.LOCK_UN)


def note_put_progress(peer_id: str, status: dict, now: float | None = None) -> None:
    """Fold one pushed ``upload_progress`` into the per-peer span sample."""
    try:
        up = ((status or {}).get('current_kg') or {}).get('upload_progress')
        if not isinstance(up, dict):
            return
        sent, total, fname = int(up.get('sent') or 0), int(up.get('total') or 0), str(up.get('file') or '')
        if total <= 0 or not fname:
            return
        now = now if now is not None else time.time()

        def _m(d):
            e = (d.get('peers') or {}).get(peer_id)
            if (isinstance(e, dict) and e.get('file') == fname and e.get('total') == total
                    and sent >= int(e.get('s') or 0)):
                e['t'], e['s'] = now, sent
            else:
                e = {'file': fname, 'total': total, 't0': now, 's0': sent, 't': now, 's': sent}
            d.setdefault('peers', {})[peer_id] = e
            return True
        _fleet_put_rw(_m)
    except Exception:
        log.debug('note_put_progress failed', exc_info=True)


def fleet_put_rate(ttl: float = 30.0) -> dict:
    """``{ts, fleet_med_bps|None, n, floor_bps, abs_min_bps, frac, raw_med_bps}``
    — cached *ttl* s; mirrors itself to ``zenodo_fleet_put.json`` for the
    primary's own zenodo_client."""
    import statistics
    now = time.time()
    if now - _fleet_put_cache['at'] < ttl and _fleet_put_cache['v']:
        return _fleet_put_cache['v']
    cfg = director_config()
    abs_kbps = float(cfg.get('zenodo_put_floor_abs_kbps', ZENODO_PUT_FLOOR_ABS_KBPS) or 0)
    frac = float(cfg.get('zenodo_put_floor_frac', ZENODO_PUT_FLOOR_FRAC) or 0)
    out = {'raw': None, 'n': 0, 'hist_med': None}

    def _m(d):
        rates = []
        for pid, e in list((d.get('peers') or {}).items()):
            try:
                if now - float(e['t']) > FLEET_PUT_ACTIVE_S:
                    continue
                span = float(e['t']) - float(e['t0'])
                gain = int(e['s']) - int(e['s0'])
                if span >= FLEET_PUT_MIN_SPAN_S and gain > 0:
                    rates.append(gain / span)
            except Exception:
                continue
        # prune dead samples (> 1 h) so the file stays tiny
        d['peers'] = {k: v for k, v in (d.get('peers') or {}).items()
                      if now - float((v or {}).get('t') or 0) <= 3600}
        hist = [h for h in (d.get('hist') or []) if now - float(h[0]) <= FLEET_PUT_HIST_S]
        if rates:
            out['raw'] = statistics.median(rates)
            out['n'] = len(rates)
            hist.append([now, out['raw']])
        d['hist'] = hist[-60:]
        if hist:
            out['hist_med'] = statistics.median(h[1] for h in hist)
        return True
    try:
        _fleet_put_rw(_m)
    except Exception:
        log.debug('fleet_put_rate failed', exc_info=True)
    med = out['hist_med']
    abs_bps = abs_kbps * 1024.0
    floor = max(abs_bps, frac * med) if med else abs_bps
    v = {'ts': now, 'fleet_med_bps': (round(med) if med else None), 'n': out['n'],
         'raw_med_bps': (round(out['raw']) if out['raw'] else None),
         'floor_bps': round(floor), 'abs_min_bps': round(abs_bps), 'frac': frac}
    _fleet_put_cache.update(at=now, v=v)
    try:
        tmp = _FLEET_PUT_LOCAL.with_suffix('.tmp')
        tmp.write_text(json.dumps(v))
        os.replace(tmp, _FLEET_PUT_LOCAL)
    except Exception:
        pass
    return v


def fleet_put_ack() -> dict:
    """Slim ack payload for peers: ``{med, n, floor, abs, frac}`` (bytes/s)."""
    v = fleet_put_rate()
    return {'med': v.get('fleet_med_bps'), 'n': v.get('n'), 'floor': v.get('floor_bps'),
            'abs': v.get('abs_min_bps'), 'frac': v.get('frac')}


def get_pushed_status(peer_id: str) -> dict | None:
    """Return fresh pushed status for a peer, or None if stale/missing.

    Consults the on-disk copy written by the sibling gunicorn worker
    when our in-memory entry is missing or older than the file."""
    if not peer_id:
        return None
    with _PEER_PUSH_LOCK:
        ent = _PEER_PUSH.get(peer_id)
        ts = ent['ts'] if ent else 0.0
    if (time.time() - ts) > 20.0:
        try:
            fp = _PEER_PUSH_DIR / f'{peer_id}.json'
            st = fp.stat()
            if st.st_mtime > ts + 0.5:
                d = json.loads(fp.read_text())
                if isinstance(d, dict) and d.get('ts'):
                    with _PEER_PUSH_LOCK:
                        cur = _PEER_PUSH.get(peer_id)
                        if not cur or float(d['ts']) > cur['ts']:
                            _PEER_PUSH[peer_id] = d
                            ent = d
        except Exception:
            pass
    if not ent:
        return None
    if (time.time() - ent['ts']) > PEER_PUSH_FRESH_S:
        return None
    return ent


def get_pushed_bandwidth(peer_id: str) -> dict | None:
    ent = get_pushed_status(peer_id)
    if ent is None:
        return None
    return ent.get('bandwidth')


def get_peer_status(peer_url: str | None, peer_id: str = '') -> dict:
    """Get processing status from a peer. None = local."""
    if peer_url is None:
        # Local — read progress.json directly
        pf = DATA_DIR / 'progress.json'
        if pf.exists():
            try:
                d = json.loads(pf.read_text())
                # Check if processor is actually alive
                try:
                    subprocess.check_output(
                        ['pgrep', '-f', 'austria_processor.py'],
                        text=True, timeout=2
                    )
                except Exception:
                    if d.get('state') in ('running', 'processing'):
                        d['state'] = 'stopped'
                # Always reflect the live srv's commit, not whatever
                # value an old processor left in progress.json.
                d['git_commit'] = _LOCAL_GIT_COMMIT
                return d
            except Exception:
                pass
        return {'state': 'idle', 'git_commit': _LOCAL_GIT_COMMIT}
    pid = peer_id or _peer_id_for_url(peer_url)
    # Prefer pushed status if fresh — avoids polling the peer entirely.
    pushed = get_pushed_status(pid)
    if pushed is not None:
        age = time.time() - pushed['ts']
        # Short-circuit: a push <60 s old is the fastest possible
        # answer and there's no value in re-deriving it. Skip even
        # the pull-cache write so concurrent callers from get_status()
        # / capacity-tick fanouts don't all serialise on the dict
        # under GIL contention. Anything older than 60 s falls
        # through to the slower path that also refreshes the pull
        # cache as a fallback for circuit-open scenarios.
        if age < 60.0:
            d = dict(pushed.get('status') or {})
            d['_pushed'] = True
            d['_push_age_s'] = round(age, 1)
            d['_push_fresh'] = True
            return d
        d = dict(pushed.get('status') or {})
        d['_pushed'] = True
        d['_push_age_s'] = round(age, 1)
        # Still cache for fallback / proxy reads.
        _PEER_STATUS_CACHE[peer_url] = (time.time(), d)
        return d
    try:
        r = _peer_request(
            'GET',
            peer_url.rstrip('/') + '/api/v1/processing/status',
            peer_id=pid,
            timeout=PEER_TIMEOUT_PROBE,
        )
        if r is None:
            # Circuit open — use cached value if available.
            cached = _PEER_STATUS_CACHE.get(peer_url)
            if cached and (time.time() - cached[0]) < _PEER_STATUS_CACHE_TTL:
                d = dict(cached[1])
                d['_stale'] = True
                d['_circuit_open'] = True
                d['_stale_age_s'] = round(time.time() - cached[0], 1)
                return d
            return {'state': 'unreachable', 'error': 'circuit_open',
                    '_circuit_open': True}
        r.raise_for_status()
        d = r.json()
        _PEER_STATUS_CACHE[peer_url] = (time.time(), d)
        return d
    except Exception as e:
        # Fall back to last-known good status if recent enough — a slow
        # peer that's actually doing work shouldn't show as unreachable.
        cached = _PEER_STATUS_CACHE.get(peer_url)
        if cached and (time.time() - cached[0]) < _PEER_STATUS_CACHE_TTL:
            d = dict(cached[1])
            d['_stale'] = True
            d['_stale_age_s'] = round(time.time() - cached[0], 1)
            d['_stale_error'] = str(e)[:120]
            # Don't override the original 'state' — the peer is most
            # likely still doing whatever it was doing. The dashboard
            # surfaces _stale so it's clear data is cached.
            return d
        return {'state': 'unreachable', 'error': str(e)}


def _parent_geometrically_done(code: str, entries: dict | None) -> bool | None:
    """Coverage-oracle verdict for a *parent* KG: ``True`` when its committed
    ``_json`` products (plain and/or blocks of ANY historical layout) cover
    the parent bbox, ``False`` when there is a real hole, ``None`` when the
    oracle is unavailable.

    Why not the search-index ``processed`` flag: the index flips a parent
    to ``processed=1`` as soon as ONE block JSON exists
    (``search_index._enrich_kg_from_blocks``), so a half-done split parent
    (67204: 2 of 6 blocks, 2026-11-07 — 74 such parents / 138 cached
    blocks) looked "done" to the cache-only whitelist and sat on the
    frontiers alone at ~3.6 KG/h while 40 cache-only peers idled. And why
    not plain block-set membership: layouts drift with strikes (91123 was
    finished as 6 blocks, today's layout wants 8 doubled ones), so a
    set check calls a geometrically complete parent pending and the
    frontier pinned to its cell restart-loops into quarantine."""
    try:
        from app import _kg_coverage_complete as _cc
        if entries is None:
            return None
        return bool(_cc(code, entries)[0])
    except Exception:
        return None


def _manifest_entries_ro() -> dict | None:
    try:
        mf_path = DATA_DIR / 'zenodo_manifest.json'
        if not mf_path.exists():
            return None
        import json_cache
        raw = json_cache.load(mf_path) or {}
        return raw.get('entries', raw) or {}
    except Exception:
        return None


def get_peer_log(peer_url: str | None, lines: int = 50) -> list[str]:
    """Get recent log lines from a peer."""
    if peer_url is None:
        log_file = DATA_DIR / 'logs' / 'processor.log'
        if log_file.exists():
            try:
                result = subprocess.run(
                    ['tail', f'-{max(1, min(int(lines), 5000))}', str(log_file)],
                    capture_output=True, text=True, timeout=5
                )
                return result.stdout.splitlines()
            except Exception:
                pass
        return []
    try:
        r = requests.get(
            peer_url.rstrip('/') + '/api/v1/processing/log',
            params={'lines': lines},
            timeout=PEER_TIMEOUT_PROBE,
        headers=_admin_headers())
        r.raise_for_status()
        d = r.json()
        return d.get('lines', d.get('log', []))
    except Exception:
        return []


# Per-peer cache-manifest sync backoff state. Module-global so we
# don't have to thread the director instance through every call site.
# Maps peer_url -> {fails: int, suppress_until: float epoch}.
_SYNC_BACKOFF: dict[str, dict] = {}
# /api/v1/credentials per-peer fetch cache: url -> (ts, creds list)
_CRED_FETCH_CACHE: dict[str, tuple] = {}
_CRED_FETCH_TTL_RUNNING_S = 600
_CRED_FETCH_TTL_IDLE_S = 1800


def _put_json_gz(url: str, obj, timeout, headers: dict | None = None):
    """PUT ``obj`` as gzipped JSON (cluster JSON shrinks 5-10×). Falls
    back to plain JSON once on 400/415/500 so a peer that predates the
    gzip request hook (app._inflate_gzip_request_body) still works."""
    import gzip as _gz
    raw = json.dumps(obj).encode('utf-8')
    h = dict(_admin_headers())
    if headers:
        h.update(headers)
    h['Content-Type'] = 'application/json'
    h['Content-Encoding'] = 'gzip'
    r = requests.put(url, data=_gz.compress(raw, compresslevel=6),
                     headers=h, timeout=timeout)
    if r.status_code in (400, 415, 500):
        h.pop('Content-Encoding', None)
        r = requests.put(url, data=raw, headers=h, timeout=timeout)
    return r


def _sync_kg_strikes_to_peer(peer_url: str) -> None:
    """Push local kg_strikes.json (adaptive-split counters) to a peer.

    Best-effort, max(local, remote) per KG. Without it, peers would only
    see strikes they observed themselves, missing strikes accumulated on
    other peers (e.g. the same KG bounced between three frontiers).
    """
    p = DATA_DIR / 'kg_strikes.json'
    if not p.exists():
        return
    try:
        local = json.loads(p.read_text())
        if not local:
            return
        r = _put_json_gz(
            peer_url.rstrip('/') + '/api/v1/processing/kg_strikes',
            local, timeout=PEER_TIMEOUT_CONTROL)
        if r.ok:
            log.info('KG strikes sync to %s: %d updated', peer_url,
                     r.json().get('updated', 0))
        else:
            log.debug('KG strikes sync to %s: HTTP %d', peer_url, r.status_code)
    except Exception as e:
        log.debug('KG strikes sync to %s: %s', peer_url, e)


def _sync_cache_manifest_to_peer(peer_url: str) -> None:
    """Push local Zenodo tile-cache manifest to a remote peer.

    Ensures the peer shares the same Zenodo cache deposit, so it can
    read cached Copernicus/Hansen tiles instead of re-fetching them.

    Backs off after ``SYNC_BACKOFF_THRESHOLD`` consecutive failures so a
    peer with a flaky network doesn't burn the budget on every tick.
    Honours ``Retry-After`` if the peer responds with one.
    """
    manifest_path = DATA_DIR / 'cache_manifest.json'
    if not manifest_path.exists():
        return
    bo = _SYNC_BACKOFF.get(peer_url) or {}
    if bo.get('suppress_until', 0) > time.time():
        log.debug('Cache manifest sync to %s suppressed (%.0fs left)',
                  peer_url, bo['suppress_until'] - time.time())
        return
    try:
        local_cm = json.loads(manifest_path.read_text())
        if not local_cm.get('depo_id'):
            return
        # Piggyback the tombstone DROP journal (~330 B gz). This is the only
        # primary→peer path for negative tombstones: peers don't peer-sync
        # from the primary, so without it a sweep never converges and peers
        # re-run finished KGs (63330, Sep 2026). Not persisted into the
        # peer's cache_manifest.json — see the PUT handler in app.py.
        try:
            _dp = DATA_DIR / 'manifest_tombstone_drops.json'
            if _dp.exists():
                _dj = json.loads(_dp.read_text())
                if isinstance(_dj, dict) and _dj:
                    local_cm = dict(local_cm)
                    local_cm['tombstone_drops'] = {
                        k: str(v) for k, v in _dj.items() if isinstance(v, str)}
        except Exception:
            pass
        r = _put_json_gz(
            peer_url.rstrip('/') + '/api/v1/processing/cache_manifest',
            local_cm, timeout=PEER_TIMEOUT_CONTROL)
        if r.ok:
            result = r.json()
            log.info('Cache manifest sync to %s: %d entries updated',
                     peer_url, result.get('updated', 0))
            _SYNC_BACKOFF[peer_url] = {'fails': 0, 'suppress_until': 0}
        else:
            # Honour Retry-After if present (Zenodo proxy etc).
            ra = r.headers.get('Retry-After')
            ra_secs = 0.0
            if ra:
                try:
                    ra_secs = float(ra)
                except ValueError:
                    pass
            fails = int(bo.get('fails', 0)) + 1
            entry = {'fails': fails, 'suppress_until': 0.0}
            if fails >= SYNC_BACKOFF_THRESHOLD or ra_secs > 0:
                entry['suppress_until'] = (
                    time.time() + max(SYNC_BACKOFF_SECONDS, ra_secs))
                log.warning(
                    'Cache manifest sync to %s: HTTP %d (fail %d) — '
                    'suppressing for %.0fs%s',
                    peer_url, r.status_code, fails,
                    entry['suppress_until'] - time.time(),
                    f' (Retry-After {ra_secs:.0f}s)' if ra_secs else '',
                )
            else:
                log.warning('Cache manifest sync to %s: HTTP %d (fail %d)',
                            peer_url, r.status_code, fails)
            _SYNC_BACKOFF[peer_url] = entry
    except Exception as e:
        fails = int(bo.get('fails', 0)) + 1
        entry = {'fails': fails, 'suppress_until': 0.0}
        if fails >= SYNC_BACKOFF_THRESHOLD:
            entry['suppress_until'] = time.time() + SYNC_BACKOFF_SECONDS
            log.warning(
                'Cache manifest sync to %s failed (%s, fail %d) — '
                'suppressing for %ds',
                peer_url, e, fails, SYNC_BACKOFF_SECONDS)
        else:
            log.warning('Cache manifest sync to %s failed: %s', peer_url, e)
        _SYNC_BACKOFF[peer_url] = entry


# === SECTION: Cross-peer dispatch claim registry ===
# Closes the race where the same KG is handed to two peers within one
# director tick: the frontier queue sync (``sync_queue_to_peer``) and the
# cache-only whitelist push (``_push_queue_to_peer``) both run from the
# director loop, but a peer's ``current_kg`` isn't visible in its status
# snapshot for a few seconds after it's started — so the status-based
# ``in_progress`` gate can't see a KG that was dispatched moments earlier.
# We record every (code -> peer, ts) dispatch and refuse to re-hand a code
# to a *different* peer inside the TTL window. Same-peer re-syncs (the
# common every-tick case) are never blocked and refresh the claim.
# Lives in the director-loop worker (single lock holder), so the dict is
# effectively process-local and consistent for the dispatch paths.
_RECENT_DISPATCH: dict = {}        # code -> (ts, peer_url)
_RECENT_DISPATCH_TTL = 150.0       # seconds; covers status-visibility lag
_DISPATCH_LOCK = threading.Lock()


def _claim_dispatch(codes: list, peer_url: str) -> list:
    """Return the subset of *codes* not recently claimed by another peer,
    recording the survivors as claimed by *peer_url*.

    A code claimed by a *different* peer within ``_RECENT_DISPATCH_TTL`` is
    dropped (the other peer already got it this tick). After the TTL the
    claim lapses so a genuinely-reassigned KG (dead peer) can move.
    """
    if not codes:
        return codes
    now = time.time()
    out: list = []
    with _DISPATCH_LOCK:
        # Prune expired claims so the dict can't grow unbounded.
        for _c in [k for k, (t, _) in _RECENT_DISPATCH.items()
                   if now - t > _RECENT_DISPATCH_TTL]:
            _RECENT_DISPATCH.pop(_c, None)
        for c in codes:
            claim = _RECENT_DISPATCH.get(c)
            if (claim and claim[1] != peer_url
                    and (now - claim[0]) <= _RECENT_DISPATCH_TTL):
                continue  # another peer was handed this KG moments ago
            out.append(c)
        for c in out:
            _RECENT_DISPATCH[c] = (now, peer_url)
    return out


def _requeue_interrupted_kg(code: str, *, reason: str = '') -> bool:
    """Push an interrupted parent KG onto the director-side retry queue.

    Load-bearing for the stuck-KG watchdog (and any other path that
    hard-stops a peer mid-KG without a surviving peer holding the same
    code). A SIGKILL'd processor only re-adds its in-flight KG to its
    *own* local pending pool via ``IN_PROGRESS_FILE`` crash-recovery —
    but the director then re-admits the peer onto a *different*
    dispatch, so the original KG is silently orphaned (it lives in
    ``kg_list.json`` but is referenced by nothing: not the retry queue,
    not failed/deferred sets, no manifest entry). This bites big,
    disk-bound parents (e.g. 50017/Hörgersteig, Jun 2026) that wedge on
    ``gpkg_full`` on peer after peer and never get auto-split because
    each interruption lands on a different peer's local strike file.

    Goes through the canonical ``POST /api/v1/processing/queue``
    endpoint on the local loopback (``config.APP_URL``) rather than
    writing ``retry_queue.json`` directly. This matters because the
    primary runs gunicorn with two workers: a direct file write from
    the director-loop worker would race the OTHER worker's queue
    GET-prune / POST handlers (last-writer-wins truncation) and would
    bypass the endpoint's completed-code filtering. The endpoint
    serialises queue mutations and is the single source of truth the
    dashboard, the GET-prune oracle, and the peer dispatch all share.

    Pushes the *parent* code (strips any ``-block`` suffix) at the
    front (``position: 0``) with ``skip_processed: True`` — so a KG
    that genuinely completed on some other peer between the freeze and
    now is dropped instead of needlessly reprocessed. The interrupted
    parent we care about is coverage-incomplete (no manifest products),
    so it survives the filter and the next eligible peer picks it up
    via ``sync_queue_to_peer``. Returns True on a successful enqueue.
    """
    import re as _re
    m = _re.match(r'^(\d+)(?:-[a-z][-a-z0-9]*)?$', str(code))
    parent = m.group(1) if m else str(code)
    try:
        r = requests.post(
            config.APP_URL + '/api/v1/processing/queue',
            json={'kgs': [parent], 'position': 0, 'skip_processed': True},
            timeout=PEER_TIMEOUT_CONTROL, headers=_admin_headers())
        if not r.ok:
            log.warning('requeue interrupted KG %s: HTTP %s',
                        parent, r.status_code)
            return False
        body = r.json() if r.content else {}
    except Exception as e:
        log.warning('requeue interrupted KG %s failed: %s', parent, e)
        return False
    log.info('requeued interrupted KG %s to retry_queue front%s (%s)',
             parent, (f' ({reason})' if reason else ''),
             body.get('status', 'ok'))
    return True


def sync_queue_to_peer(peer_url: str, exclude: set | None = None) -> dict:
    """Push the local priority queue to a remote peer.

    Reads the local retry_queue.json and POSTs it to the peer's
    /api/v1/processing/queue endpoint (position=0 = front of queue).
    Skips if queue is empty.

    `exclude` is a set of KG codes that must NOT be sent to this peer
    (typically KGs reserved for cooled-down peers so they can resume
    them from tile checkpoints when their cooldown lifts).
    """
    queue_path = DATA_DIR / 'retry_queue.json'
    try:
        queue = json.loads(queue_path.read_text()) if queue_path.exists() else []
    except Exception:
        queue = []
    if exclude:
        queue = [c for c in queue if c not in exclude]
    # Claim-gate: drop codes another peer was handed within the last TTL
    # window (the status-based in_progress gate can't see a just-dispatched
    # KG until the peer's current_kg becomes visible). Prevents two peers
    # racing the same KG — e.g. at100 (cache-only whitelist) + at102
    # (frontier queue) both starting 40318 15s apart.
    queue = _claim_dispatch(queue, peer_url)
    _drop = sorted(exclude) if exclude else []
    if not queue:
        if _drop:
            # Nothing to push, but the peer's *local* retry queue may
            # still hold an excluded code from an earlier sync — purge
            # it, or the peer runs it anyway (34009-east, 2026-11-07).
            try:
                requests.post(
                    peer_url.rstrip('/') + '/api/v1/processing/queue',
                    json={'kgs': [], 'drop': _drop},
                    timeout=PEER_TIMEOUT_CONTROL, headers=_admin_headers())
            except Exception:
                pass
        return {'status': 'empty_queue', 'synced': 0}

    # Identify tombstoned codes (force-requeued KGs that are locally
    # "completed" but the operator wants re-processed). The peer's
    # /processing/queue endpoint filters out already-completed KGs when
    # skip_processed=True — so tombstoned codes would be silently dropped.
    # Push them in a second request with skip_processed=False so the peer
    # accepts them at the front of its queue.
    tombstoned = set()
    try:
        import re as _re
        tpath = DATA_DIR / 'manifest_tombstones.json'
        # Negative tombstones (app.py drop journal): a dropped tombstone
        # must not be pushed to peers with skip_processed=False, or the
        # peer re-runs finished work (Soelden/80110, Aug 2026).
        _drops: dict = {}
        try:
            _dp = DATA_DIR / 'manifest_tombstone_drops.json'
            if _dp.exists():
                _dj = json.loads(_dp.read_text())
                if isinstance(_dj, dict):
                    _drops = {k: str(v) for k, v in _dj.items()
                              if isinstance(v, str)}
        except Exception:
            _drops = {}
        if tpath.exists():
            tdata = json.loads(tpath.read_text())
            if isinstance(tdata, dict):
                tdata = {k: v for k, v in tdata.items()
                         if not (k in _drops and not str(v) > _drops[k])}
                for key in tdata:
                    m = _re.match(r'^(\d+(?:-[a-z][-a-z0-9]*)?)_', key)
                    if m:
                        tombstoned.add(m.group(1))
    except Exception:
        tombstoned = set()

    tombstoned_in_queue = [c for c in queue if c in tombstoned]
    normal_queue = [c for c in queue if c not in tombstoned]

    try:
        # Push normal codes first (they go to position 0).
        result = {'status': 'empty_queue'}
        if normal_queue:
            r = requests.post(
                peer_url.rstrip('/') + '/api/v1/processing/queue',
                json={'kgs': normal_queue, 'position': 0, 'skip_processed': True,
                      'drop': _drop},
                timeout=PEER_TIMEOUT_CONTROL,
            headers=_admin_headers())
            result = r.json()
        elif _drop:
            try:
                requests.post(
                    peer_url.rstrip('/') + '/api/v1/processing/queue',
                    json={'kgs': [], 'drop': _drop},
                    timeout=PEER_TIMEOUT_CONTROL, headers=_admin_headers())
            except Exception:
                pass
        # Then push tombstoned codes at position 0 — ends up in front,
        # ahead of the normal codes, preserving the original ordering.
        if tombstoned_in_queue:
            # keep_products + gone_keys: the peer invalidates exactly the
            # product keys the primary tombstoned — nothing more. For a
            # v2_regen requeue that is only ``<code>_full_gpkg``; the v1
            # ``_json`` / ``_light_gpkg`` stay referenced fleet-wide until
            # the fresh run replaces them (a peer-side ``_json`` tombstone
            # would otherwise merge back into the primary).
            _gone_keys = []
            try:
                _tq = set(tombstoned_in_queue)
                for key in (tdata if isinstance(tdata, dict) else {}):
                    m = _re.match(r'^(\d+(?:-[a-z][-a-z0-9]*)?)_', key)
                    if m and (m.group(1) in _tq or m.group(1).split('-')[0] in _tq) \
                            and not key.endswith('_requeue'):
                        _gone_keys.append(key)
            except Exception:
                _gone_keys = []
            r2 = requests.post(
                peer_url.rstrip('/') + '/api/v1/processing/queue',
                json={'kgs': tombstoned_in_queue, 'position': 0,
                      'skip_processed': False,
                      'keep_products': True, 'gone_keys': _gone_keys},
                timeout=PEER_TIMEOUT_CONTROL,
            headers=_admin_headers())
            result = r2.json()
        log.info('Queue sync to %s: pushed %d KGs (%d tombstoned) — %s%s',
                 peer_url, len(queue), len(tombstoned_in_queue),
                 result.get('status', '?'),
                 (' (excluded ' + ','.join(sorted(exclude)) + ')') if exclude else '')
        return result
    except Exception as e:
        log.warning('Queue sync to %s failed: %s', peer_url, e)
        return {'error': str(e)}


def _push_sweep_spec(peer_url: str, specs: dict) -> dict:
    """Director → peer: replace the peer's sliver-sweep unit specs
    (``sliver_sweep.spec_for`` per parent).  Tiny JSON."""
    r = requests.put(f"{peer_url.rstrip('/')}/api/v1/processing/sweep_spec",
                     json={'specs': specs}, headers=_admin_headers(), timeout=20)
    r.raise_for_status()
    return r.json()


def _push_queue_to_peer(peer_url: str, codes: list) -> dict:
    """Push an explicit list of KG codes as the peer's priority queue.

    Used when sending a *cache-only* peer a whitelist of fully-cached KGs.
    Replaces the peer's priority queue with the given codes.
    """
    if not codes:
        return {'status': 'empty_whitelist', 'synced': 0}
    # Claim-gate against the cross-peer dispatch registry so a code already
    # handed to another peer this tick (via the frontier queue or another
    # cache-only slice) isn't re-dispatched here. Same-peer re-syncs refresh
    # the claim and are never blocked.
    codes = _claim_dispatch(list(codes), peer_url)
    if not codes:
        return {'status': 'empty_whitelist', 'synced': 0}
    try:
        # Replace the queue (PUT) so previous frontier work doesn't leak in.
        r = requests.put(
            peer_url.rstrip('/') + '/api/v1/processing/queue',
            json={'queue': codes},
            timeout=PEER_TIMEOUT_CONTROL,
        headers=_admin_headers())
        if r.ok:
            log.info('Whitelist queue PUT to %s: %d KGs', peer_url, len(codes))
            return r.json()
        # Fallback: POST at front
        r2 = requests.post(
            peer_url.rstrip('/') + '/api/v1/processing/queue',
            json={'kgs': codes, 'position': 0, 'skip_processed': True},
            timeout=PEER_TIMEOUT_CONTROL,
        headers=_admin_headers())
        log.info('Whitelist queue POST to %s: %d KGs (PUT was %d)',
                 peer_url, len(codes), r.status_code)
        return r2.json() if r2.ok else {'error': f'http {r2.status_code}'}
    except Exception as e:
        log.warning('Whitelist queue push to %s failed: %s', peer_url, e)
        return {'error': str(e)}


def _in_progress_kgs(cfg: dict, exclude_peer_id: str | None = None) -> set:
    """Return the set of KG codes currently being processed by *other* peers.

    Probes each peer's `/api/v1/processing/status` and pulls `current_kg.code`.
    Used to exclude an active KG from the priority queue we push to a peer
    we're about to (re)start, so two peers don't race the same KG.

    Without this, the retry-queue sync happily re-issues a KG to a fresh
    frontier even though another peer is mid-tile on it. Tile checkpoints
    are per-peer, so the duplicate work is wasted.
    """
    import re as _re
    from concurrent.futures import ThreadPoolExecutor, as_completed
    targets = [p for p in cfg.get('peers', [])
               if p.get('url') and (not exclude_peer_id
                                    or p.get('id') != exclude_peer_id)]
    out: set = set()
    if not targets:
        return out
    with ThreadPoolExecutor(max_workers=min(16, len(targets))) as pool:
        futs = {pool.submit(get_peer_status, p['url']): p for p in targets}
        # PEER_TIMEOUT_PROBE is a (connect, read) tuple; cap the wall-clock
        # wait at read*2 + a small buffer so a single hung peer doesn't
        # stall the director loop.
        _read_to = (PEER_TIMEOUT_PROBE[1]
                    if isinstance(PEER_TIMEOUT_PROBE, tuple)
                    else PEER_TIMEOUT_PROBE)
        for f in as_completed(futs, timeout=_read_to * 2 + 2):
            try:
                ps = f.result()
            except Exception:
                continue
            if ps.get('state') not in ('running', 'processing'):
                continue
            ck = ps.get('current_kg') or {}
            code = ck.get('code') if isinstance(ck, dict) else None
            if code:
                out.add(str(code))
                # Also block the parent KG code so a sibling block of an
                # already-being-split KG isn't issued to another peer.
                m = _re.match(r'^(\d+)(?:-[a-z][-a-z0-9]*)?$', str(code))
                if m:
                    out.add(m.group(1))
    return out


def _excluded_kgs(cfg: dict, exclude_peer_id: str | None = None) -> set:
    """Union of reservations and in-progress KGs across other peers.

    The single source of truth for "don't issue these KGs to *exclude_peer_id*".
    Combines:
      * `_reserved_kgs` — KGs held for cooled-down peers (resume from
        tile checkpoints once their not_before expires).
      * `_in_progress_kgs` — KGs another peer is currently processing.
    """
    return (_reserved_kgs(cfg, exclude_peer_id=exclude_peer_id)
            | _in_progress_kgs(cfg, exclude_peer_id=exclude_peer_id))


def _reserved_kgs(cfg: dict, exclude_peer_id: str | None = None) -> set:
    """Collect KGs reserved by other peers (not `exclude_peer_id`).

    Reservations persist past `not_before` so that the holding peer can
    actually pick the KG back up once cooldown lifts. Stale reservations
    are pruned separately by `_clear_completed_reservations` once the KG
    appears in the local `_get_completed_kgs()` set.

    Split-KG awareness: a reservation on a block code (``60336-northwest``)
    also blocks the parent code (``60336``), and a reservation on a
    parent blocks the parent itself (block siblings are still OK to run
    in parallel — adding parent → all-blocks would over-block legitimate
    parallel block work). The asymmetric expansion is correct because
    a peer that holds a *block* still leaves other blocks free for
    parallel processing.
    """
    import re as _re
    out = set()
    for p in cfg.get('peers', []):
        if exclude_peer_id and p.get('id') == exclude_peer_id:
            continue
        kg = p.get('reserved_kg')
        if not kg:
            continue
        s = str(kg)
        out.add(s)
        # Block parent code too: an old (pre-splitter) peer would
        # otherwise treat ``60336`` as free while another peer holds
        # ``60336-northwest`` mid-tile.
        m = _re.match(r'^(\d+)-[a-z][-a-z0-9]*$', s)
        if m:
            out.add(m.group(1))
    return out


_TOOLING_DEFER_COUNT: dict = {}   # peer_id -> consecutive 'tooling installing' start deferrals


def start_peer_processor(peer_url: str | None, exclude_kgs: set | None = None,
                         *, cache_only: bool = False,
                         queue_whitelist: list | None = None,
                         cred_indices: list | None = None,
                         lat_strips: list | None = None,
                         v2_upgrade: bool = False) -> dict:
    """Start the processor on a peer.

    For remote peers, syncs the local priority queue first so the peer
    processes the same KGs in priority order. `exclude_kgs` lets callers
    suppress KGs reserved for other (cooled-down) peers.

    If *cache_only* is True, the peer is started with the ``--cache-only``
    flag so it refuses any Copernicus/Hansen API call.  Use *queue_whitelist*
    to send only KGs known to be fully cached.

    Hard gate: refuses while the operator has the director paused
    (sidecar ``director_mode.json``), whatever orchestrator called us.
    """
    if read_director_mode() == 'paused':
        return {'error': 'director paused', 'skipped': True}
    payload = {}
    if cache_only:
        payload['cache_only'] = True
    if v2_upgrade:
        payload['v2_upgrade'] = True
    _ne_src = knob(None, 'ne_source', None)
    if _ne_src == 'bevdirect':
        payload['ne_source'] = _ne_src
    # VTC pilot flag + per-peer bev mode (docs/vct-pilot.md).  Always sent
    # (True/False) so a peer leaving the pilot clears its sidecar at the
    # next start; a peer in ``vct_bev_peers`` gets ``ne_source=bevdirect``
    # (its declared layer is built locally from the BEV vector tiles).  A
    # peer whose tooling is still installing refuses the start with 503
    # (processing/start gate) and we retry next tick.
    try:
        _pid = _peer_id_for_url(peer_url)
        _bev = _pid in set(knob(None, 'vct_bev_peers', []) or [])
        payload['vct_pilot'] = _bev or _pid in set(knob(None, 'vct_pilot_peers', []) or [])
        if _bev:
            payload['ne_source'] = 'bevdirect'
    except Exception:
        pass
    if cred_indices is not None:
        payload['cred_indices'] = list(cred_indices)
    if lat_strips is not None:
        payload['lat_strips'] = list(lat_strips)
    if peer_url is None:
        # Local start — use the local API so _processor_process is tracked
        try:
            r = requests.post(config.APP_URL + '/api/v1/processing/start',
                              json=payload, timeout=PEER_TIMEOUT_CONTROL, headers=_admin_headers())
            return r.json() if r.ok else {'error': f'local start: {r.status_code}'}
        except Exception as e:
            return {'error': str(e)}
    # Remote peer — sync cache manifest + KG strikes + priority queue before starting
    _sync_cache_manifest_to_peer(peer_url)
    _sync_kg_strikes_to_peer(peer_url)
    if queue_whitelist is not None:
        queue_result = _push_queue_to_peer(peer_url, list(queue_whitelist))
    else:
        queue_result = sync_queue_to_peer(peer_url, exclude=exclude_kgs)
    # Try API start; if it transiently 500s (often due to a race in
    # processor state — e.g. an externally-detected processor that
    # didn't fully exit), retry once after a short sleep.
    last_err: str | None = None
    last_status: int | None = None
    for attempt in (1, 2):
        try:
            r = requests.post(
                peer_url.rstrip('/') + '/api/v1/processing/start',
                json=payload,
                timeout=PEER_TIMEOUT_CONTROL,
            headers=_admin_headers())
            if r.ok:
                result = r.json()
                result['queue_sync'] = queue_result
                _TOOLING_DEFER_COUNT.pop(_peer_id_for_url(peer_url), None)
                if attempt > 1:
                    result['retry'] = attempt
                return result
            if r.status_code == 409:
                # Already running. If body says "external", the previous
                # subprocess hasn't fully exited yet — surface as a
                # transient error so the caller retries on the next tick
                # (don't drop the error key, callers gate on it).
                try:
                    body = r.json()
                except Exception:
                    body = {'error': r.text[:200]}
                err = (body.get('error') or '').lower()
                if 'external' in err:
                    last_err = body.get('error') or 'external processor'
                    last_status = 409
                    if attempt < 2:
                        time.sleep(3.0)
                        continue
                    return {'error': f'api_start_failed: {last_err}',
                            'queue_sync': queue_result,
                            'method': 'no_fallback_constrained'}
                # Tracked-state 409 (we already started it) — fine.
                body['queue_sync'] = queue_result
                body['already_running'] = True
                body.pop('error', None)
                return body
            last_status = r.status_code
            last_err = (r.text or '')[:200]
            if r.status_code == 503 and 'bevdirect tooling' in last_err:
                # peer gate: re-pinned tooling still installing — not a
                # fault, the caller retries next tick.  Surface once.
                try:
                    _pid = _peer_id_for_url(peer_url)
                    _n = _TOOLING_DEFER_COUNT.get(_pid, 0) + 1
                    _TOOLING_DEFER_COUNT[_pid] = _n
                    _nt = (r.json().get('ne_tooling') or {})
                    # info for the first ~10 min; after that the install is
                    # evidently NOT converging (at205, Oct 2026: 8 h of
                    # deferrals hidden at info level) — escalate so it
                    # shows in /process.txt?warn=1 with the stale reason.
                    _lvl = 'warning' if _n >= 20 else 'info'
                    _emit_director_event('start deferred: bevdirect tooling installing ('
                                         + str(_nt.get('binary_version')) + ')'
                                         + (f' — {_n} consecutive deferrals, package_stale='
                                            f'{_nt.get("package_stale")!s}; srv restart on the peer retries pip'
                                            if _n >= 20 else ''),
                                         peer=_pid, level=_lvl)
                except Exception:
                    pass
                return {'error': 'api_start_failed: bevdirect tooling installing',
                        'queue_sync': queue_result, 'method': 'tooling_wait', 'retry': True}
        except Exception as e:
            last_err = str(e)
        if attempt < 2:
            time.sleep(1.5)
    # The systemd fallback restarts ``austria_processor.service`` which
    # (a) is disabled on peers and (b) has no awareness of cache_only,
    # cred_indices, lat_strips, or queue_whitelist. Falling back here
    # silently bypasses the director's per-peer assignments — e.g. a
    # cache-only start turns into a full frontier start, then the
    # director kills it as 'non-active running frontier' and retries
    # forever. Only fall back when the call had no per-peer contract.
    is_constrained = bool(cache_only or cred_indices or lat_strips
                            or queue_whitelist is not None)
    if is_constrained:
        log.warning(
            'API start on %s failed (status=%s err=%s); skipping systemd '
            'fallback because contract requires cache_only=%s '
            'cred_indices=%s lat_strips=%s queue_whitelist=%s',
            peer_url, last_status, last_err,
            cache_only, cred_indices, lat_strips,
            None if queue_whitelist is None else len(queue_whitelist))
        return {'error': f'api_start_failed: {last_status or last_err}',
                'queue_sync': queue_result,
                'method': 'no_fallback_constrained'}
    log.warning('API start on %s returned %s, trying systemd fallback',
                peer_url, last_status if last_status else last_err)
    # Fallback: ask the peer to restart the processor via systemd
    try:
        r2 = requests.post(
            peer_url.rstrip('/') + '/api/v1/admin/restart_processor',
            json={},
            timeout=30,
        headers=_admin_headers())
        result = r2.json() if r2.ok else {'error': f'systemd fallback: {r2.status_code}'}
        result['queue_sync'] = queue_result
        result['method'] = 'systemd_fallback'
        return result
    except Exception as e2:
        return {'error': str(e2), 'queue_sync': queue_result, 'method': 'both_failed'}


def stop_peer_processor(peer_url: str | None, graceful: bool = False) -> dict:
    """Stop the processor on a peer.

    If ``graceful`` is True, asks the peer to exit cleanly after the
    current KG instead of SIGTERM'ing mid-KG. Used for cache-only peers
    where mid-KG kills waste pure CPU work without any credential-safety
    benefit.
    """
    url = peer_url if peer_url else config.APP_URL
    try:
        params = {'graceful': '1'} if graceful else None
        r = requests.post(
            url.rstrip('/') + '/api/v1/processing/stop',
            params=params,
            timeout=30,  # stop can take a moment
            headers=_admin_headers(),
        )
        if r.ok:
            return r.json()
        # If 404 (not running), that's fine
        if r.status_code == 404:
            return {'status': 'already_stopped'}
        return {'error': f'stop returned {r.status_code}'}
    except Exception as e:
        return {'error': str(e)}


def safely_stop_peer(peer_url: str | None, peer_id: str = '?',
                     max_wait: int = 60) -> dict:
    """Stop a peer's processor and verify it actually stopped.

    CRITICAL for credential safety: returns success only after we've
    confirmed via /processing/status that the peer is no longer running.
    On failure, the caller MUST NOT start another peer (would cause
    parallel processing and 402 rate-limit errors on shared credentials).

    Returns: {'status': 'stopped'}, {'status': 'already_stopped'},
             or {'error': '...', 'last_state': '...'}.
    """
    # 1. Send stop request
    stop_result = stop_peer_processor(peer_url)
    if 'error' in stop_result and stop_result.get('status') != 'already_stopped':
        log.warning('Peer %s stop API failed: %s', peer_id, stop_result['error'])
        # Don't give up yet — status check below may show it's actually stopped

    # 2. Poll status until processor is stopped (or timeout)
    deadline = time.time() + max_wait
    last_state = 'unknown'
    while time.time() < deadline:
        status = get_peer_status(peer_url)
        last_state = status.get('state', 'unknown')
        if last_state in ('idle', 'stopped'):
            return {'status': 'stopped', 'verified': True}
        if last_state == 'unreachable':
            # Peer's gunicorn is down/overloaded; we can't verify. Wait a bit.
            time.sleep(3)
            continue
        # Still running — give it time, retry stop after a few seconds
        time.sleep(2)
        if time.time() < deadline - 5:
            stop_peer_processor(peer_url)  # retry stop in case first one was lost
            time.sleep(3)

    # Timeout — could not verify peer is stopped
    log.error('Peer %s did NOT stop within %ds (last state: %s) — '
              'refusing to activate another peer (credential safety)',
              peer_id, max_wait, last_state)
    return {'error': f'stop_not_verified after {max_wait}s',
            'last_state': last_state}


def _peer_is_scheduled(peer: dict) -> bool:
    """Check if a peer has a not_before date that hasn't passed yet."""
    not_before = peer.get('not_before')
    if not not_before:
        return False
    try:
        nb = datetime.fromisoformat(not_before)
        if nb.tzinfo is None:
            nb = nb.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) < nb
    except (ValueError, TypeError):
        return False


def _clear_completed_reservations(cfg: dict, state: dict | None = None) -> bool:
    """Drop reserved_kg for peers whose held KG is already completed,
    or whose holder is no longer eligible to claim the reservation.

    A reservation otherwise persists past `not_before` so the holding
    peer can pick its held KG back up after the cooldown lifts. But if
    the holder's `not_before` has elapsed AND the holder remains
    bandwidth-exhausted (cannot run the KG), the reservation deadlocks
    the KG — ``_reserved_kgs`` excludes other peers. Release it so a
    parallel frontier picks it up.
    Returns True if the config was modified.
    """
    completed = set()
    try:
        # Imported lazily — app.py imports peer_director at startup.
        from app import _get_completed_kgs
        completed = _get_completed_kgs()
    except Exception:
        pass
    bw_map = (state or {}).get('peer_bandwidth', {}) if state else {}
    changed = False
    for p in cfg.get('peers', []):
        kg = p.get('reserved_kg')
        if not kg:
            continue
        if str(kg) in completed:
            p.pop('reserved_kg', None)
            changed = True
            continue
        # Multi-day hold? A KG must never be deadlocked behind a holder
        # that won't be eligible for a long time (manual long park, role
        # park to 2027, billing-cycle park to next renewal). The holder's
        # tile checkpoints survive on Zenodo's chkpt registry, so any peer
        # that later picks up the KG resumes cheaply — there's no reason to
        # pin it. Release as soon as the cooldown exceeds the long-hold
        # threshold so the operator never has to hand-release a held KG.
        if _peer_is_scheduled(p):
            nb = p.get('not_before')
            if nb:
                try:
                    nb_dt = datetime.fromisoformat(str(nb))
                    if nb_dt.tzinfo is None:
                        nb_dt = nb_dt.replace(tzinfo=timezone.utc)
                    hold_h = (nb_dt - datetime.now(timezone.utc)).total_seconds() / 3600.0
                    if hold_h >= RESERVATION_MAX_HOLD_HOURS:
                        log.warning('Releasing held KG %s from %s '
                                    '(multi-day hold: %.0fh until not_before %s)',
                                    kg, p['id'], hold_h, nb)
                        p.pop('reserved_kg', None)
                        changed = True
                        continue
                except Exception:
                    pass
            # Short cooldown — keep the reservation so the holder resumes.
            continue
        # not_before elapsed (or absent). If holder is bandwidth-
        # exhausted, it can never claim — release so others can.
        if state is not None:
            bw = bw_map.get(p['id'], {})
            if _peer_bw_depleted(p, bw, cfg):
                log.warning('Releasing held KG %s from %s '
                            '(cooldown elapsed but bandwidth exhausted)',
                            kg, p['id'])
                p.pop('reserved_kg', None)
                changed = True
    return changed


def _ready_reservation_holder(cfg: dict, state: dict | None = None) -> str | None:
    """Return peer_id of an enabled, non-scheduled peer that holds a
    reserved KG ready to be resumed. Returns None if no such peer.

    A holder with insufficient bandwidth (<2 GB remaining) cannot
    actually run the held KG, so it is ignored. Otherwise the
    director would pre-empt the active peer in a loop without ever
    being able to activate the holder (choose_active_peer also
    bandwidth-gates), starving parallel frontiers.
    """
    bw_map = (state or {}).get('peer_bandwidth', {}) if state else {}
    for p in cfg.get('peers', []):
        if not p.get('enabled', True):
            continue
        if not p.get('reserved_kg'):
            continue
        if _peer_is_scheduled(p):
            continue
        if state is not None:
            bw = bw_map.get(p['id'], {})
            if _peer_bw_depleted(p, bw, cfg):
                continue
        return p['id']
    return None


def _peer_age_seconds(peer: dict) -> float:
    """Seconds since the peer was first seen by the director.

    Falls back to a large value if first_seen is missing (legacy entries)
    so the warmup hold doesn't block long-lived peers retroactively.
    """
    fs = peer.get('first_seen')
    if not fs:
        return float('inf')
    try:
        from datetime import datetime as _dt
        t = _dt.fromisoformat(fs)
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - t).total_seconds()
    except Exception:
        return float('inf')


def _peer_in_warmup(peer: dict) -> bool:
    """True if peer is too fresh to be promoted to frontier work.

    During warmup we still allow cache-only assignments (which are the
    gentlest workload) so the peer's tile cache fills naturally.
    """
    return _peer_age_seconds(peer) < WARMUP_HOLD_SECONDS


# --- KG complexity weights (used by complexity-weighted LPT partition
# in _orchestrate_cache_only). Cached at module level so we don't
# re-read kg_list.json + re-derive tile counts on every director tick.
_KG_WEIGHT_CACHE: dict[str, float] = {}
_KG_WEIGHT_CACHE_BUILT_AT: float = 0.0
_KG_WEIGHT_CACHE_TTL = 3600.0   # 1 h — KG geometry is static, this is
                                # just defensive against config edits.


def _kg_weights(codes: list) -> dict:
    """Return {kg_code: weight} for the requested codes.

    Weight ≈ expected processing cost relative to a tiny KG. Built
    from tile count (the dominant cost driver — each tile runs
    Felzenszwalb + RAG + RF inference) plus a small legacy term for the
    vector-feature count (vectorise). Block codes inherit their
    parent's geometry; ``maybe_split_kg`` is invoked lazily so this
    works correctly for both parent and block codes.

    Unknown codes get weight=1.0 (one tile's worth) so the partition
    falls back gracefully if kg_list.json is missing entries.
    """
    global _KG_WEIGHT_CACHE_BUILT_AT
    now = time.time()
    if (now - _KG_WEIGHT_CACHE_BUILT_AT) > _KG_WEIGHT_CACHE_TTL:
        _KG_WEIGHT_CACHE.clear()
        _KG_WEIGHT_CACHE_BUILT_AT = now
    out: dict[str, float] = {}
    missing: list[str] = []
    for c in codes:
        w = _KG_WEIGHT_CACHE.get(c)
        if w is not None:
            out[c] = w
        else:
            missing.append(c)
    if not missing:
        return out
    # Lazy import — avoid pulling kg_splitter on module load.
    try:
        from kg_splitter import (maybe_split_kg, is_block_code,
                                   parent_kg_code, _compute_n_tiles)
    except Exception:
        # No splitter available — fall back to unit weights.
        for c in missing:
            _KG_WEIGHT_CACHE[c] = 1.0
            out[c] = 1.0
        return out
    kg_by_code = {}
    try:
        kg_list_path = DATA_DIR / 'kg_list.json'
        if kg_list_path.exists():
            for kg in json.loads(kg_list_path.read_text()):
                cc = kg.get('kg_code')
                if cc:
                    kg_by_code[cc] = kg
    except Exception:
        kg_by_code = {}
    for c in missing:
        try:
            parent = parent_kg_code(c) if is_block_code(c) else c
            kg = kg_by_code.get(parent)
            if not kg:
                _KG_WEIGHT_CACHE[c] = 1.0
                out[c] = 1.0
                continue
            if is_block_code(c):
                blocks = maybe_split_kg(kg)
                # Find this block by code; if mismatch, average the
                # parent uniformly.
                tgt = next((b for b in blocks
                            if b.get('kg_code') == c), None)
                if tgt and 'bbox' in tgt:
                    bb = tgt['bbox']
                    n_tiles = _compute_n_tiles(
                        bb['min_lon'], bb['min_lat'],
                        bb['max_lon'], bb['max_lat'])
                    # Per-feature cost — parent's count divided
                    # across blocks (cheap & good enough).
                    feats = kg.get('feature_count', 0) / max(
                        len(blocks), 1)
                else:
                    bb = kg.get('bbox') or {}
                    n_tiles = _compute_n_tiles(
                        bb.get('min_lon', 0), bb.get('min_lat', 0),
                        bb.get('max_lon', 0), bb.get('max_lat', 0)
                    ) / max(len(blocks) if blocks else 1, 1)
                    feats = kg.get('feature_count', 0) / max(
                        len(blocks) if blocks else 1, 1)
            else:
                bb = kg.get('bbox') or {}
                n_tiles = _compute_n_tiles(
                    bb.get('min_lon', 0), bb.get('min_lat', 0),
                    bb.get('max_lon', 0), bb.get('max_lat', 0))
                feats = kg.get('feature_count', 0)
            # Weight: tile count dominates; tiny feature-count nudge.
            w = max(1.0, float(n_tiles) + 0.0005 * float(feats))
        except Exception:
            w = 1.0
        _KG_WEIGHT_CACHE[c] = w
        out[c] = w
    return out


def _peer_cpu_steal(peer_id: str) -> float | None:
    """Return EWMA CPU steal %% for *peer_id*, or None if unknown.

    Reads the freshest value we have: peer-pushed status
    (``system.perf.cpu_steal_ewma`` > ``system.cpu_steal``). Returns
    None when the peer hasn't pushed telemetry yet (e.g. old build,
    first 30 s after start) so callers can treat it as "no signal"
    rather than "0 %% steal".
    """
    pushed = get_pushed_status(peer_id)
    if not pushed:
        return None
    sysd = pushed.get('status', {}).get('system') or {}
    perf = sysd.get('perf') or {}
    v = perf.get('cpu_steal_ewma')
    if v is None:
        v = sysd.get('cpu_steal')
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _peer_cpu_steal_ema(peer_id: str, state: dict) -> float | None:
    """Return the rolling director-side steal EMA for *peer_id*.

    Updated in the director tick from the per-peer status stream
    (``state['peer_steal_ema']``). Falls back to the instantaneous
    ``_peer_cpu_steal`` so we don't return None just because the
    director hasn't ticked since the peer reported in.
    """
    try:
        v = ((state.get('peer_steal_ema') or {}) or {}).get(peer_id)
        if isinstance(v, (int, float)):
            return float(v)
    except Exception:
        pass
    return _peer_cpu_steal(peer_id)


def _peer_cpu_capacity(peer_id: str, default: float = 1.0) -> float:
    """Effective CPU capacity for *peer_id* in [0.10, 1.0].

    capacity = max(0.10, 1 - steal_ewma / 100). Floor at 0.10 so a
    pathological 95 %% steal peer still gets *some* work assigned (LPT
    becomes degenerate at exactly 0). Returns ``default`` when the
    peer hasn't pushed telemetry yet.
    """
    s = _peer_cpu_steal(peer_id)
    if s is None:
        return default
    return max(0.10, 1.0 - s / 100.0)


def _fleet_steal_median(peer_ids: list) -> float | None:
    """Median CPU steal across *peer_ids*; None when nobody reported."""
    vals = [v for v in (_peer_cpu_steal(pid) for pid in peer_ids)
            if v is not None]
    if not vals:
        return None
    vals.sort()
    n = len(vals)
    return vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2


def _peer_noise_score(peer_id: str, state: dict) -> float:
    """Return a [0, +inf) noise score for a peer.

    Sums (rate_5m / saturation) across BEV / Zenodo / Copernicus from
    the peer's last reported ``warning_rates`` (cached on the director
    state under ``peer_warning_rates``). Higher = noisier upstreams
    *seen by this peer*. Used to bias scheduling toward quiet peers so
    fresh peers without warnings carry more load while peers that just
    upset Zenodo cool off naturally.

    Score 0.0 means "silent" — either truly quiet or no data yet (a
    fresh peer has no warning history, which is what we want: it's
    preferred for the next slot).
    """
    wr = ((state.get('peer_warning_rates') or {}).get(peer_id) or {})
    long_ema = ((state.get('peer_noise_long_ema') or {}).get(peer_id) or {})
    if not wr and not long_ema:
        return 0.0
    score = 0.0
    for kind, sat in THROTTLE_SATURATION_RATE.items():
        if sat <= 0:
            continue
        r_now = float(((wr.get(kind) or {}).get('5m')) or 0.0)
        r_long = float(long_ema.get(kind) or 0.0)
        # Take the max of "current pressure" and "recent-history pressure"
        # so a peer that just upset Zenodo five minutes ago still scores
        # noisy even though its 5-min rate already rolled back to zero.
        score += max(r_now, r_long) / sat
    return score


def _peer_noise_score_live(peer_id: str, state: dict) -> float:
    """Live-only noise score for *park* decisions.

    Unlike :func:`_peer_noise_score` (which max-es with the slow long
    EMA and is meant for scheduling bias), this uses only the current
    5-min warning rates, and deliberately excludes ``copernicus``:
    Copernicus 402/errors follow the *credential*, not the peer —
    parking the peer never fixes a degraded cred (the rotator does),
    it just knocks a healthy VM out of rotation.

    Jul 2026 incident: the noise-park gate used the EMA-backed score,
    which barely decays during a 1 h park. Canary-override peers
    (at26..at48) cycled park→unpark→park for 6+ h with live rates of
    0.0, collapsing the parallel-frontier set and triggering
    plan-drift restart storms on the survivors.
    """
    wr = ((state.get('peer_warning_rates') or {}).get(peer_id) or {})
    if not wr:
        return 0.0
    score = 0.0
    for kind in ('bev', 'zenodo'):
        sat = THROTTLE_SATURATION_RATE.get(kind) or 0.0
        if sat <= 0:
            continue
        r_now = float(((wr.get(kind) or {}).get('5m')) or 0.0)
        score += r_now / sat
    return score


def _peer_pool_bev_wpm(peer_id: str, cfg: dict, state: dict) -> float:
    """Return the peer's egress /24 BEV avg_wpm (0.0 if unknown).

    Consumed by frontier selection: pools that are visibly upset with
    BEV right now should not be assigned scarce Copernicus credits.
    Uses the per-tick ``ip_pools`` snapshot already maintained by
    ``_check_ip_pool_block`` so we don't recompute on the hot path.
    """
    peer = get_peer_by_id(cfg, peer_id)
    if not peer:
        return 0.0
    host = ((state.get('peer_statuses') or {}).get(peer_id) or {})
    sys = (host.get('system') or {}) if isinstance(host, dict) else {}
    pool_key = ((sys.get('host') or {}).get('outbound_24')) or ''
    if not pool_key:
        return 0.0
    pools = state.get('ip_pools') or {}
    pinfo = pools.get(pool_key) or {}
    return float(pinfo.get('avg_wpm') or 0.0)


def _peer_pool_bev_score(peer_id: str, cfg: dict, state: dict
                         ) -> tuple[int, float]:
    """Frontier-sort contribution for the peer's egress pool BEV health.

    Returns ``(hard_tier, soft_penalty)`` where ``hard_tier`` is 1 for
    pools at/above ``FRONTIER_POOL_BEV_HARD_WPM`` (demoted), and
    ``soft_penalty`` is a continuous penalty (avg_wpm - floor),
    clamped to >= 0, in the same units as ``_peer_noise_score``.
    """
    wpm = _peer_pool_bev_wpm(peer_id, cfg, state)
    if wpm <= 0:
        return (0, 0.0)
    hard = 1 if wpm >= FRONTIER_POOL_BEV_HARD_WPM else 0
    soft = max(0.0, wpm - FRONTIER_POOL_BEV_BIAS_FLOOR)
    return (hard, soft)


def choose_active_peer(cfg: dict, state: dict) -> str | None:
    """Pick the best peer to run the processor on.

    Reservation holders win first — if an enabled, non-scheduled peer
    still holds a reserved KG, give it priority so it can resume the
    held KG from its tile checkpoints. Otherwise pick the peer with the
    most remaining bandwidth.
    Returns None if all candidates have <2 GB remaining.
    """
    holder = _ready_reservation_holder(cfg, state)
    if holder:
        return holder

    # Score-based pick: prefer quiet peers (low warning fingerprint),
    # break ties by remaining bandwidth. Fresh peers have score 0 and
    # a full budget, so they naturally win until they earn warnings.
    candidates: list[tuple[float, int, str]] = []
    for peer in cfg.get('peers', []):
        if not peer.get('enabled', True):
            continue
        if _peer_is_scheduled(peer):
            continue
        pinned = (peer.get('pinned_role') or '').strip().lower()
        if pinned in ('idle', 'off', 'pause', 'paused', 'parked'):
            continue  # user-pinned idle
        if pinned in ('cache_only', 'cache-only', 'cacheonly'):
            continue  # user-pinned cache_only — not eligible as primary frontier
        if _peer_in_warmup(peer):
            continue  # fresh peer — hold off frontier promotion
        pid = peer['id']
        bw = state.get('peer_bandwidth', {}).get(pid, {})
        used = bw.get('used_bytes', 0)
        # Canary-by-default: only disqualify when we have measured
        # evidence (observed_cap_gb) that the peer has hit its wall.
        if _peer_bw_depleted(peer, bw, cfg):
            continue
        # For ranking, expose remaining vs cap-or-nominal so peers with
        # known caps still sort below those without (more headroom).
        cap_g = peer.get('observed_cap_gb')
        if isinstance(cap_g, (int, float)) and cap_g > 0:
            remaining = int(float(cap_g) * (1024 ** 3) - used)
        else:
            remaining = _peer_budget_bytes(peer, cfg) - used
        noise = _peer_noise_score(pid, state)
        # CPU-steal penalty + hard tier. Frontier work burns scarce
        # Copernicus credentials, so we never want to land it on a
        # heavily-contended host when a quieter peer is available.
        # The rolling EMA is more stable than the instantaneous
        # ``_peer_cpu_steal`` (which jitters with every tick) and
        # better reflects whether the peer's host is structurally
        # over-subscribed vs briefly noisy.
        s_ema = _peer_cpu_steal_ema(pid, state)
        if s_ema is None:
            s_ema = _peer_cpu_steal(pid) or 0.0
        cap = max(0.10, 1.0 - float(s_ema) / 100.0)
        steal_pen = (1.0 / cap) - 1.0    # 0.0 at 100%% capacity
        # Hard tier: peers >= FRONTIER_HIGH_STEAL_BIAS_PCT are demoted.
        # Low-steal peers (Platinum 8259CL pool in May 2026) outrank
        # any high-steal peer (AMD EPYC pool at ~60 %% steal), even if
        # the high-steal peer is otherwise quieter or has more bw.
        steal_tier = 1 if float(s_ema) >= FRONTIER_HIGH_STEAL_BIAS_PCT else 0
        # Pool BEV health: avoid landing fresh frontier work on a peer
        # whose /24 is currently visibly upset with BEV (would burn most
        # of its KG wall-time retrying).
        pool_tier, pool_pen = _peer_pool_bev_score(pid, cfg, state)
        # Sort key: combined hard tier first (any hard signal demotes),
        # then low noise+steal+pool, then high remaining bandwidth.
        candidates.append((
            steal_tier + pool_tier,
            noise + steal_pen + pool_pen,
            -remaining, pid,
        ))

    if not candidates:
        return None
    candidates.sort()
    return candidates[0][3]


def get_peer_by_id(cfg: dict, peer_id: str) -> dict | None:
    for p in cfg.get('peers', []):
        if p['id'] == peer_id:
            return p
    return None


class PeerDirector:
    """Background thread that orchestrates peer processing."""

    def __init__(self):
        self.cfg = load_peers_config()
        self.state = load_director_state()
        # Seed URL→id reverse map for circuit-breaker attribution.
        global _PEER_URL_TO_ID
        _PEER_URL_TO_ID = {
            (p.get('url') or '').rstrip('/'): p.get('id') or ''
            for p in (self.cfg.get('peers') or [])
            if p.get('url') and p.get('id')
        }
        self._lock = threading.Lock()
        self._running = False
        self._thread = None
        self._lock_fd = None
        # Dedicated oidc-reval daemon thread, owned by THIS Director
        # instance. Started in start(), stopped in stop() so it cleanly
        # joins on step_down / HA handover (singleton swap). Lifecycle
        # is intentionally tied to the instance, NOT a module global,
        # so a demoted director never keeps probing OIDC in the
        # background after relinquishing the role.
        self._oidc_thread: threading.Thread | None = None
        self._oidc_stop = threading.Event()
        # EMA capacity factor (0..1). Persisted to director_state.json so
        # restart of the director (or a gunicorn worker swap) doesn't
        # erase recent fleet-wide warning history. We also restore a
        # per-peer slow noise EMA below so a peer that misbehaved is
        # remembered for hours, not just for the 10-min sliding window.
        self._capacity_ema: float = float(
            self.state.get('capacity_ema_persisted',
                           THROTTLE_MAX_FACTOR) or THROTTLE_MAX_FACTOR)
        # Per-kind smoothed sub-factor EMAs (bev / zenodo / copernicus).
        # Used by ``_effective_creds_per_frontier`` so the per=1↔per=2
        # decision doesn't flap on every transient cop blip — only the
        # smoothed signal crosses the hysteresis thresholds.
        _persisted_sub_ema = self.state.get('sub_factor_ema') or {}
        self._sub_factor_ema: dict[str, float] = {
            k: float(_persisted_sub_ema.get(k, THROTTLE_MAX_FACTOR)
                      or THROTTLE_MAX_FACTOR)
            for k in ('bev', 'zenodo', 'copernicus')
        }
        self._capacity_components: dict = {}
        # Sliding history of (ts, factor, bev, zenodo, copernicus) tuples.
        # Sized for ~2h of 30s ticks (240 entries). Survives ticks but not
        # restarts; that's fine for a UI sparkline.
        from collections import deque as _dq
        self._capacity_history = _dq(maxlen=240)
        # Sliding history of (ts, frontiers, cache_only, cache_ready)
        # tuples for the Progress card sparkline. Same retention
        # window (~2h) and persistence as capacity_history so the
        # chart survives restarts and is visible to all gunicorn
        # workers.
        self._peer_history = _dq(maxlen=240)
        # Fleet BEV-proxy-pool history. Sized for ~24h at 1 sample
        # every 10 min (144 entries). Tighter cadence isn't useful —
        # peer pools refresh every 30 min and the dashboard only needs
        # "is the fleet seeing fewer proxies than usual" visibility.
        # Each entry: {t, n_reporting, prx_total, prx_min, prx_med,
        # prx_max, no_prx, stale}. Persisted via director_state.json.
        self._proxy_history = _dq(maxlen=144)
        self._proxy_history_last_ts = 0.0
        try:
            for entry in (self.state.get('fleet_proxy_history') or []):
                self._proxy_history.append(entry)
        except Exception:
            pass
        # Restore persisted history on startup (and let non-director
        # gunicorn workers see the same data via load_director_state).
        try:
            for entry in (self.state.get('capacity_history') or []):
                # stl/cpu are None for legacy entries (pre-2026-05-19,
                # OR transitional records that 50c6e8b persisted with
                # 0.0/1.0 placeholders before this load-path fix). We
                # discriminate by a per-entry 'v' marker: v>=1 means the
                # writer knew about stl/cpu. Without it we drop the
                # fields so /process.txt min/med/max isn't contaminated.
                _v = entry.get('v') or 0
                _stl = entry.get('stl') if _v >= 1 else None
                _cpu = entry.get('cpu') if _v >= 1 else None
                # v>=2: cumulative failed/partial KG counters. We persist
                # cumulative counts (rather than per-tick deltas) so the
                # rate-per-hour can be computed by the renderer over any
                # sub-window of the ring and so HA handover / gunicorn
                # restarts don't lose history-aware accounting.
                _fk = entry.get('fk') if _v >= 2 else None
                _pk = entry.get('pk') if _v >= 2 else None
                self._capacity_history.append((
                    int(entry.get('t') or 0),
                    float(entry.get('f') or 0.0),
                    float(entry.get('bev') or 0.0),
                    float(entry.get('zen') or 0.0),
                    float(entry.get('cop') or 0.0),
                    float(_stl) if isinstance(_stl, (int, float)) else None,
                    float(_cpu) if isinstance(_cpu, (int, float)) else None,
                    int(_fk) if isinstance(_fk, (int, float)) else None,
                    int(_pk) if isinstance(_pk, (int, float)) else None,
                ))
        except Exception:
            pass
        try:
            for entry in (self.state.get('peer_history') or []):
                self._peer_history.append((
                    int(entry.get('t') or 0),
                    int(entry.get('fr') or 0),
                    int(entry.get('co') or 0),
                    int(entry.get('cr') or 0),
                ))
        except Exception:
            pass

    # --- Server-friendliness throttle ------------------------------------
    # When the fleet has at least this many reporting peers, switch from
    # ``max`` aggregation to the high-percentile aggregation below.
    # Below this count a single hot peer must still drive the decision;
    # above it, one peer in a stale-cred retry storm shouldn't drag the
    # whole fleet down (we have ~59 peers in production).
    _PERCENTILE_MIN_PEERS = 10
    # 90th percentile across reporting peers — i.e. ignore the worst
    # ~10% of outliers. With 59 peers that means up to 5 hot peers can
    # be misbehaving without throttling the rest.
    _PERCENTILE = 0.90

    def _fleet_warning_rates(self, statuses: dict) -> dict:
        """Aggregate per-peer ``warning_rates`` into fleet-wide signals.

        Small fleets (< _PERCENTILE_MIN_PEERS reporting): use ``max`` per
        kind — a single peer seeing 6 BEV warnings/min is already a
        strong signal that the upstream is pushing back.

        Large fleets: use the 90th percentile per kind. With 59 peers a
        couple of misconfigured / stale-cred peers shouldn't drag the
        capacity factor for the other 57. Persistent saturation across
        many peers still wins because the 90th percentile follows the
        body of the distribution.

        ``auth`` is reported but doesn't feed throttle (see comment on
        ``THROTTLE_SATURATION_RATE`` — 401 / invalid_client means a
        peer's local cred file is stale, not that the upstream is
        unhealthy).
        """
        # Per-kind collected rates from live peers (one entry per peer).
        per_kind: dict[str, list[float]] = {
            'bev': [], 'zenodo': [], 'copernicus': [], 'auth': [],
        }
        # Per-kind: which peer pushed the highest rate (for tooltips).
        top_peer: dict[str, tuple[float, str]] = {
            k: (0.0, '') for k in per_kind
        }
        peer_count = 0
        # Track per-peer 'last seen running' so we can give unreachable
        # peers a grace period before excluding them. A peer doing a
        # heavy GPKG upload may stop responding to /processing/status
        # for several minutes — we don't want to drop its warning
        # signal in that case. Only after UNREACHABLE_GRACE seconds do
        # we treat the peer's stale warning window as no-longer-current.
        UNREACHABLE_GRACE = 30 * 60   # 30 minutes
        now = time.time()
        last_live = self.state.setdefault('peer_last_live_ts', {})
        for pid, ps in (statuses or {}).items():
            wr = (ps or {}).get('warning_rates') or {}
            state = (ps or {}).get('state')
            is_live = state in ('running', 'processing', 'idle')
            if is_live:
                last_live[pid] = int(now)
            if not wr:
                continue
            # Stopped is a clean exit (SIGTERM during update / manual
            # stop) — we know the peer isn't doing work, so its stale
            # warning window is safely droppable immediately.
            #
            # 'complete' is the post-run terminal state when a peer
            # finishes its assigned KGs. Same logic applies: the
            # processor isn't running, no new warnings are landing,
            # whatever's in warning_rates is a frozen snapshot of the
            # last KG. Including it pinned the fleet capacity_factor at
            # ~0.2 even when nothing live was emitting warnings.
            if state in ('stopped', 'complete'):
                continue
            # Unreachable peers may be busy uploading or briefly
            # network-flaky. Honour their last warning rates until
            # they've been silent for UNREACHABLE_GRACE.
            if state == 'unreachable':
                last = last_live.get(pid, 0)
                if last and (now - last) > UNREACHABLE_GRACE:
                    continue
                # else: include as if live (peer may be uploading)
            peer_count += 1
            for kind in per_kind:
                # 5-min window is the sweet spot: long enough to ignore
                # one-off retries, short enough to react within ~5 min.
                rate = float(((wr.get(kind) or {}).get('5m')) or 0.0)
                # Always record the rate (even 0) so percentile
                # computation reflects the true fleet shape.
                per_kind[kind].append(rate)
                if rate > top_peer[kind][0]:
                    top_peer[kind] = (rate, str(pid))
        # Aggregate per kind. ``auth`` always uses max because it's
        # diagnostic only (not fed into the throttle).
        agg: dict[str, float] = {}
        use_pctl = peer_count >= self._PERCENTILE_MIN_PEERS
        for kind, rates in per_kind.items():
            if not rates:
                agg[kind] = 0.0
                continue
            if kind == 'auth':
                agg[kind] = max(rates)
            elif not use_pctl:
                # Small fleet: ``max`` — but once ≥3 peers report, ask
                # for corroboration (2nd-highest). At the tail of the
                # run (2026-11-06: 6 running peers, 45 idle) a single
                # peer's two NDVI 500s in 5 min (0.4/min = copernicus
                # saturation) pinned the fleet factor at 0.2 → frontier
                # cap 9→2, cache-only cap 60→14 — fewer peers running →
                # still ``max`` → stuck. One peer can be unlucky; two
                # peers seeing the same upstream push back is a signal.
                xs = sorted(rates)
                agg[kind] = xs[-2] if len(xs) >= 3 else xs[-1]
            else:
                # 90th percentile via nearest-rank.
                xs = sorted(rates)
                idx = max(0, min(len(xs) - 1,
                                  int(round(self._PERCENTILE *
                                            (len(xs) - 1)))))
                agg[kind] = xs[idx]
        agg['_peers_reporting'] = peer_count
        agg['_aggregation'] = 'p90' if use_pctl else 'max'
        # Surface the loudest peer per kind so operators can see which
        # peer is dragging the signal up (especially useful when p90
        # filtered it out and the throttle didn't engage).
        agg['_top_peer'] = {
            k: {'rate': round(v[0], 3), 'pid': v[1]}
            for k, v in top_peer.items() if v[0] > 0
        }
        # Track which peers contributed and which didn't so we can
        # surface gaps in /api/v1/director/status. Useful when we
        # expect 50 peers and only 45 report — the missing 5 are
        # silent because they're scheduled, unreachable past grace,
        # or stopped.
        all_pids = sorted((statuses or {}).keys())
        reporting_pids = sorted(
            pid for pid, ps in (statuses or {}).items()
            if (ps or {}).get('warning_rates')
            and (ps or {}).get('state') not in ('stopped', 'complete'))
        agg['_peers_reporting_ids'] = reporting_pids
        agg['_peers_silent_ids'] = [
            pid for pid in all_pids if pid not in set(reporting_pids)]
        return agg

    def _capacity_factor(self, statuses: dict) -> float:
        """Combined capacity factor in [THROTTLE_MIN_FACTOR, 1.0].

        Per-kind sub-factors decay linearly from 1.0 (no warnings) down to
        THROTTLE_MIN_FACTOR at the saturation rate. The minimum sub-factor
        wins (i.e. whichever upstream is angriest dominates). An EMA
        smooths it across ticks; a slow sinusoidal drift adds a natural
        wobble so we don't sit pinned at the cap.
        """
        rates = self._fleet_warning_rates(statuses)
        # Fleet CPU-steal median across *running* peers — same population
        # the cache-only ramp brake uses (see _max_cache_only_peers).
        # Snapshot it here so /api/v1/director/status carries it on
        # every tick (cheap; we already have ``statuses``) and the
        # sparkline can plot the rolling history.
        try:
            _steal_vals = []
            for _pid, _ps in (statuses or {}).items():
                _st = (_ps or {}).get('state')
                if _st not in ('running', 'processing'):
                    continue
                _sys = (_ps or {}).get('system') or {}
                _perf = _sys.get('perf') or {}
                _v = _perf.get('cpu_steal_ewma')
                if _v is None:
                    _v = _sys.get('cpu_steal')
                if isinstance(_v, (int, float)):
                    _steal_vals.append(float(_v))
            _steal_vals.sort()
            if _steal_vals:
                _n = len(_steal_vals)
                _steal_med = (_steal_vals[_n // 2] if _n % 2
                               else (_steal_vals[_n // 2 - 1]
                                     + _steal_vals[_n // 2]) / 2)
            else:
                _steal_med = 0.0
        except Exception:
            _steal_vals = []
            _steal_med = 0.0
        # cpu_factor curve mirrors the damping in _max_cache_only_peers:
        # gentle ramp-brake that floors at 0.55. Surfaces *why* the
        # director may be running fewer cache-only peers than the
        # warning-rate ceiling alone would allow.
        if _steal_med >= 40.0:
            _cpu_factor = max(0.55, 1.0 - (_steal_med - 30.0) / 200.0)
        else:
            _cpu_factor = 1.0
        sub = {}
        for kind, sat in THROTTLE_SATURATION_RATE.items():
            r = float(rates.get(kind, 0.0))
            if sat <= 0:
                sub[kind] = 1.0
                continue
            # Dead-zone: tiny ambient rates read as zero. Without this,
            # a single peer logging one warning every ~3 min sits at
            # ~0.3/min fleet-max and drags capacity off 100% forever.
            dead = THROTTLE_DEAD_ZONE_FRAC * sat
            if r <= dead:
                sub[kind] = THROTTLE_MAX_FACTOR
                continue
            # 1.0 at r=dead, MIN_FACTOR at r>=sat, linear in between.
            frac = max(0.0, min(1.0, (r - dead) / (sat - dead)))
            sub[kind] = THROTTLE_MAX_FACTOR - frac * (
                THROTTLE_MAX_FACTOR - THROTTLE_MIN_FACTOR)
        raw = min(sub.values()) if sub else THROTTLE_MAX_FACTOR
        # EMA smoothing.
        a = THROTTLE_EMA_ALPHA
        self._capacity_ema = a * raw + (1.0 - a) * self._capacity_ema
        # Per-kind EMAs (same alpha) — feed adaptive decisions that
        # depend only on one upstream (e.g. cred-per-frontier).
        for _k in ('bev', 'zenodo', 'copernicus'):
            _v = float(sub.get(_k, THROTTLE_MAX_FACTOR))
            self._sub_factor_ema[_k] = (
                a * _v + (1.0 - a) * self._sub_factor_ema.get(
                    _k, THROTTLE_MAX_FACTOR))
        # Slow sinusoidal drift to mimic an organic activity pattern.
        # Phase derived from local hostname so different primaries (and
        # the future-self of the same primary post‑restart) stay roughly
        # in sync but not identical.
        import math, socket
        phase = (abs(hash(socket.gethostname())) % 1000) / 1000.0
        t = time.time() / THROTTLE_DRIFT_PERIOD_S + phase
        drift = THROTTLE_DRIFT_AMPLITUDE * math.sin(2 * math.pi * t)
        final = max(THROTTLE_MIN_FACTOR,
                    min(THROTTLE_MAX_FACTOR, self._capacity_ema + drift))
        self._capacity_components = {
            'rates': {k: rates.get(k, 0.0)
                       for k in ('bev', 'zenodo', 'copernicus', 'auth')},
            'steal_median': round(float(_steal_med), 1),
            'steal_n': len(_steal_vals),
            'cpu_factor': round(float(_cpu_factor), 3),
            'sub_factors': {k: round(v, 3) for k, v in sub.items()},
            'sub_factor_emas': {k: round(float(v), 3)
                                 for k, v in self._sub_factor_ema.items()},
            'raw': round(raw, 3),
            'ema': round(self._capacity_ema, 3),
            'drift': round(drift, 3),
            'factor': round(final, 3),
            'peers_reporting': rates.get('_peers_reporting', 0),
            'peers_reporting_ids': rates.get('_peers_reporting_ids') or [],
            'peers_silent_ids': rates.get('_peers_silent_ids') or [],
            # Aggregation strategy (max for small fleets, p90 for >=10
            # reporting peers) and per-kind loudest peer. Both surface
            # in /api/v1/director/status so the dashboard can show
            # "Cop signal driven by peer at43 (1.8/min); fleet p90 0.0".
            'aggregation': rates.get('_aggregation', 'max'),
            'top_peer': rates.get('_top_peer') or {},
        }
        # Cumulative failed/partial KG counters. We read the on-disk
        # registries (already replicated to shadow via SNAPSHOT_FILES,
        # so the counter is durable across HA handover) and persist the
        # cumulative value into each history sample. The renderer (and
        # /process.txt) derive the rate as a (newest-oldest)/window
        # delta over any sub-window of the 2h ring.
        _fk_n: int | None = None
        _pk_n: int | None = None
        try:
            _fp = DATA_DIR / 'failed_kgs.json'
            if _fp.exists():
                _d = json.loads(_fp.read_text() or '[]')
                if isinstance(_d, list):
                    _fk_n = len(_d)
                elif isinstance(_d, dict):
                    _fk_n = len(_d)
        except Exception:
            pass
        try:
            _pp = DATA_DIR / 'partial_kgs.json'
            if _pp.exists():
                _d = json.loads(_pp.read_text() or '{}')
                if isinstance(_d, dict):
                    _pk_n = len(_d)
                elif isinstance(_d, list):
                    _pk_n = len(_d)
        except Exception:
            pass
        # Append to ring buffer. Compact tuple (no dict) to keep the
        # JSON payload small even when serialised in get_status().
        self._capacity_history.append((
            int(time.time()),
            round(final, 3),
            round(float(rates.get('bev', 0.0)), 3),
            round(float(rates.get('zenodo', 0.0)), 3),
            round(float(rates.get('copernicus', 0.0)), 3),
            round(float(_steal_med), 1),
            round(float(_cpu_factor), 3),
            _fk_n,
            _pk_n,
        ))
        return final

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        # Use a file lock to ensure only one director loop runs across
        # all gunicorn workers. The lock is non-blocking; if another worker
        # already holds it, this worker skips the director loop.
        import fcntl
        lock_path = DATA_DIR / 'director.lock'
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._lock_fd = open(lock_path, 'w')
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, IOError):
            log.info('PeerDirector lock held by another worker — skipping')
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        # Freshen director_state.json mtime immediately so heartbeats
        # return 200 right after a srv restart. Without this, the file
        # carries its pre-restart mtime (often 5+ minutes old), the
        # heartbeat liveness check fails, peer watchdogs trip a takeover,
        # and the cluster cascades. The loop will refresh the contents
        # within ~30s; this is just a marker that says 'a director loop
        # is here'.
        try:
            DIRECTOR_STATE.parent.mkdir(parents=True, exist_ok=True)
            DIRECTOR_STATE.touch(exist_ok=True)
        except Exception:
            pass
        # Spin up the dedicated oidc-reval daemon. Must come after the
        # file-lock acquisition so we never have a non-director thread
        # probing OIDC. Stopped in self.stop() on step_down/handover.
        self._oidc_stop.clear()
        self._oidc_thread = threading.Thread(
            target=self._oidc_reval_loop, name='oidc-reval', daemon=True)
        self._oidc_thread.start()
        log.info('PeerDirector started (lock acquired)')
        # Smoke test: a director with zero Copernicus credentials cannot
        # run frontiers. The 2026-05-08 incident silently disarmed the
        # whole fleet because builtin creds were removed and the
        # cred-fanout helper had a NameError. Surfacing this loud at
        # startup makes the failure mode obvious in journalctl.
        try:
            import copernicus as _cop_smoke
            n_creds = len(_cop_smoke.list_credentials() or [])
            if n_creds == 0:
                log.warning(
                    'director starting with 0 Copernicus credentials — '
                    'Sentinel-based steps will fail. Add via POST '
                    '/api/v1/credentials, COPERNICUS_CREDENTIALS_FILE or '
                    'COPERNICUS_BOOTSTRAP_CREDS (see .env.example).')
            else:
                log.info('Director credential pool: %d entries', n_creds)
        except Exception as _e:
            log.warning('credential smoke test failed: %s', _e)

    def stop(self, *, join_timeout: float = 8.0):
        """Stop the director loop, join the thread, and release the file lock.

        Critical for in-process handover: ``_do_takeover`` calls
        ``old.stop()`` and immediately tries ``new.start()``. If we don't
        join + release here, the new instance's ``fcntl.LOCK_NB`` fails
        (same-process), the takeover silently aborts, and we end up with
        ``IS_DIRECTOR_FLAG`` set on disk but no loop running. The
        heartbeat then returns 200 forever while the cluster is
        actually unmanaged.
        """
        self._running = False
        # Stop the oidc-reval daemon first so it can't fire a long
        # OIDC sweep right after we release the director lock. join
        # under the same budget as the loop thread.
        try:
            self._oidc_stop.set()
        except Exception:
            pass
        oidc_thr = self._oidc_thread
        if (oidc_thr and oidc_thr.is_alive()
                and oidc_thr is not threading.current_thread()):
            try:
                oidc_thr.join(timeout=max(1.0, join_timeout))
            except Exception as e:
                log.warning('PeerDirector.stop: oidc-reval join failed: %s', e)
        self._oidc_thread = None
        thr = self._thread
        if thr and thr.is_alive() and thr is not threading.current_thread():
            try:
                thr.join(timeout=join_timeout)
            except Exception as e:
                log.warning('PeerDirector.stop: thread join failed: %s', e)
        self._thread = None
        # Explicitly release the file lock so the next instance can
        # acquire it in this same process.
        fd = getattr(self, '_lock_fd', None)
        if fd is not None:
            try:
                import fcntl as _fcntl
                _fcntl.flock(fd, _fcntl.LOCK_UN)
            except Exception:
                pass
            try:
                fd.close()
            except Exception:
                pass
            self._lock_fd = None

    # Short-lived cache for get_status() output. Dashboard polls
    # /director/status (15s) AND /director/proxy/status (5s, also calls
    # get_status). Each fanout hits all 60 peers (status + credentials)
    # with PEER_TIMEOUT_PROBE up to (3,8) — easily 5–15s wall-clock.
    # Without coalescing, every gunicorn thread (8 total) is permanently
    # blocked inside as_completed(), wedging the entire web app.
    # 4s TTL means at most one fanout per dashboard cycle; single-flight
    # lock means concurrent callers share the same in-flight result
    # instead of each launching their own 60-peer fanout.
    # Fresh window: callers within this many seconds of the last
    # successful compute get the cached value with zero work. The
    # 60-peer fanout costs ~3–6s on a warm system; setting TTL below
    # that guarantees most callers pay full price (every dashboard
    # poll, every process.txt curl from an agent). 30s is short
    # enough that stale director state is never user-visible (the
    # dashboard refreshes faster than a peer can change role) but
    # long enough that all five dashboard pollers (process.html
    # poll/5s, fetchPrioQueue/15s, fetchDirectorStatus/15s,
    # fetchAllPeerStatuses/8s, fetchLog24h/30s) hit cache during a
    # single page-load wave.
    # Cross-worker disk cache: gunicorn runs 2 workers. Without a
    # shared cache each worker has its own in-memory copy, and a
    # client doing one request every TTL/2 seconds still pays cold
    # cost on every alternating worker. The disk file is a tiny
    # JSON payload written atomically; readers tolerate parse
    # errors silently.
    _STATUS_CACHE_FILE = Path('/tmp/srtm_director_status_cache.json')
    _STATUS_CACHE_TTL = 30.0
    # Stale window: if a recompute is in flight, callers that arrive
    # with a cached value at most this old are served the stale copy
    # immediately instead of blocking. Keeps /process.txt and
    # /process.html responsive even during fanout. We trigger a
    # background refresh when the cache crosses TTL but is still
    # within STALE, so the next caller sees fresh data without any
    # request having had to wait.
    _STATUS_CACHE_STALE = 180.0

    def _is_director_signature(self) -> tuple[bool, int]:
        """Cheap fingerprint of the ``is_director`` flag file.

        Returned tuple is (exists, mtime_ns). Used by ``get_status`` to
        detect a director flip that happened in *another* gunicorn
        worker (or via direct disk write — handover, HA promotion,
        manual recovery) and invalidate this worker's status cache.

        Without this guard, a 2-worker gunicorn that flipped role in
        only one worker would have the other worker keep returning a
        cached status reflecting the old role for up to
        ``_STATUS_CACHE_STALE`` (3 min). On a primary that just got
        demoted (e.g. bandwidth-walled, manual park, takeover by a
        peer) that stale worker would also keep running director
        side-effects driven from cached state — including the 60-peer
        fanout that burns external bandwidth. Cheap stat() per call is
        well worth it.
        """
        return (config.is_director(), 0)

    def get_status(self) -> dict:
        """Full director status for the dashboard.

        TTL-cached + single-flight + stale-while-revalidate:
        - within ``_STATUS_CACHE_TTL``: serve cache, no work
        - between TTL and STALE: serve cache, kick a background
          refresh (if none in flight)
        - older than STALE or no cache: block on a recompute
          (single-flight: concurrent callers share the same in-flight
          result instead of each launching their own 60-peer fanout)
        """
        import time as _t
        # Lazy-init cache attrs (instance is constructed before this method
        # may be called from multiple threads).
        if not hasattr(self, '_status_cache_lock'):
            self._status_cache_lock = threading.Lock()
            self._status_cache_inflight = threading.Lock()
            self._status_cache_value: dict | None = None
            self._status_cache_ts: float = 0.0
        now = _t.time()
        # Snapshot the director-flag fingerprint *before* we read the
        # cache. If it differs from the fingerprint stamped on the
        # cache, drop both the in-memory and on-disk copies and force
        # a recompute. This catches the cross-worker drift case where
        # one gunicorn worker flipped role (writing/removing
        # ``is_director``) but the other worker's cache still reflects
        # the old role. See ``_is_director_signature`` for rationale.
        flag_sig = self._is_director_signature()
        with self._status_cache_lock:
            cached = self._status_cache_value
            cached_sig = getattr(self, '_status_cache_flag_sig', None)
            if cached is not None and cached_sig != flag_sig:
                log.info(
                    'director-flag flip detected (cache_sig=%s '
                    'disk_sig=%s) — invalidating get_status cache',
                    cached_sig, flag_sig,
                )
                self._status_cache_value = None
                self._status_cache_ts = 0.0
                cached = None
                # Also nuke the cross-worker disk cache so the *other*
                # worker doesn't immediately re-promote a stale value
                # we just rejected.
                try:
                    self._STATUS_CACHE_FILE.unlink()
                except FileNotFoundError:
                    pass
                except Exception as _e:  # noqa: BLE001
                    log.debug('status cache disk unlink failed: %s', _e)
            age = now - self._status_cache_ts if cached is not None else None
        # Cross-worker promotion: if our in-memory copy is stale (or
        # missing) but the disk file is fresher, adopt it. This is
        # what lets the second gunicorn worker piggy-back on a
        # recompute the first worker just paid for.
        if cached is None or (age is not None and age >= self._STATUS_CACHE_TTL):
            disk = self._status_cache_load_disk()
            if disk is not None:
                disk_ts = float(disk.get('_cached_at', 0) or 0)
                disk_sig = (
                    bool(disk.get('_flag_exists', False)),
                    int(disk.get('_flag_mtime_ns', 0) or 0),
                )
                disk_age = now - disk_ts
                # Reject the disk cache too if it was written by a
                # worker that saw a different director-flag state.
                # Prevents a stale cross-worker promotion from
                # resurrecting the cache we just invalidated.
                if disk_sig != flag_sig:
                    log.debug(
                        'disk status cache flag mismatch '
                        '(disk_sig=%s live_sig=%s) — ignoring',
                        disk_sig, flag_sig,
                    )
                elif disk_age >= 0 and (age is None or disk_age < age):
                    with self._status_cache_lock:
                        self._status_cache_value = disk
                        self._status_cache_ts = disk_ts
                        self._status_cache_flag_sig = disk_sig
                    cached = disk
                    age = disk_age
        if cached is not None and age is not None and age < self._STATUS_CACHE_TTL:
            return cached
        # Stale-while-revalidate: serve aged cache + spawn a refresh.
        if cached is not None and age is not None and age < self._STATUS_CACHE_STALE:
            if self._status_cache_inflight.acquire(blocking=False):
                def _bg_refresh():
                    try:
                        # Re-snapshot the flag fingerprint at the
                        # moment of compute, not at the time the
                        # request came in.
                        sig = self._is_director_signature()
                        result = self._compute_status()
                        with self._status_cache_lock:
                            self._status_cache_value = result
                            self._status_cache_ts = _t.time()
                            self._status_cache_flag_sig = sig
                        self._status_cache_save_disk(result, sig)
                    except Exception as _e:  # noqa: BLE001
                        log.warning('get_status background refresh: %s', _e)
                    finally:
                        try:
                            self._status_cache_inflight.release()
                        except RuntimeError:
                            pass
                threading.Thread(
                    target=_bg_refresh,
                    name='dir-status-bg',
                    daemon=True,
                ).start()
            # Else: another thread is already refreshing — fine.
            return cached
        # No usable cache: block on compute under single-flight.
        acquired = self._status_cache_inflight.acquire(timeout=20)
        if not acquired:
            with self._status_cache_lock:
                if self._status_cache_value is not None:
                    return self._status_cache_value
            # No prior cache and we couldn't get the lock — fall through
            # and compute (rare, only on first request).
            self._status_cache_inflight.acquire()
        try:
            # Re-check after acquiring lock — another thread may have
            # just populated the cache.
            with self._status_cache_lock:
                if (self._status_cache_value is not None and
                        (_t.time() - self._status_cache_ts) < self._STATUS_CACHE_TTL):
                    return self._status_cache_value
            sig = self._is_director_signature()
            result = self._compute_status()
            with self._status_cache_lock:
                self._status_cache_value = result
                self._status_cache_ts = _t.time()
                self._status_cache_flag_sig = sig
            self._status_cache_save_disk(result, sig)
            return result
        finally:
            try:
                self._status_cache_inflight.release()
            except RuntimeError:
                pass

    def _status_cache_load_disk(self) -> dict | None:
        try:
            if self._STATUS_CACHE_FILE.exists():
                with self._STATUS_CACHE_FILE.open() as f:
                    return json.load(f)
        except Exception:
            return None
        return None

    def _status_cache_save_disk(
        self, payload: dict, flag_sig: tuple[bool, int] | None = None,
    ) -> None:
        try:
            d = dict(payload)
            d['_cached_at'] = time.time()
            if flag_sig is None:
                flag_sig = self._is_director_signature()
            d['_flag_exists'] = bool(flag_sig[0])
            d['_flag_mtime_ns'] = int(flag_sig[1])
            tmp = self._STATUS_CACHE_FILE.with_suffix('.tmp')
            tmp.write_text(json.dumps(d, default=str))
            tmp.replace(self._STATUS_CACHE_FILE)
        except Exception as _e:  # noqa: BLE001
            log.debug('status cache disk save failed: %s', _e)

    def _compute_status(self) -> dict:
        """Full director status for the dashboard."""
        # Always re-read config from disk so all gunicorn workers see new peers
        try:
            disk_cfg = load_peers_config()
            with self._lock:
                self.cfg = disk_cfg
        except Exception:
            pass
        # Also pull director_state from disk so workers that don't run
        # the director loop (e.g. the worker handling this request) see
        # fields written by the worker that does — peer_update_state,
        # capacity_factor, etc.
        try:
            disk_state = load_director_state()
        except Exception:
            disk_state = {}
        with self._lock:
            cfg = self.cfg.copy()
            state = self.state.copy()
        # Merge disk values for fields the director writes; keep our
        # in-memory values otherwise so per-worker scratch state isn't
        # clobbered.
        for _k in ('peer_update_state', 'capacity_factor',
                   'capacity_components', 'capacity_history',
                   'peer_history', 'fleet_proxy_history',
                   'canary_history', 'bw_cycle',
                   'canary_slowdown_streaks', 'canary_fleet_slowdown',
                   'capacity_ema_persisted', 'sub_factor_ema',
                   '_target_frontier_count',
                   'peer_warning_rates', 'peer_noise_long_ema',
                   'peer_steal_ema', 'peer_steal_streak',
                   'peer_last_live_ts', 'peer_meta',
                   'parallel_frontiers_active', 'frontier_cred_plan',
                   'frontier_strip_plan', 'cache_only_active',
                   'parallel_unreachable_count',
                   'zenodo_circuit', 'cache_reconcile',
                   '_cache_reconcile_at',
                   '_creds_revalidated_at',
                   '_cache_ready_cache',
                   'active_peer', 'mode', 'last_switch',
                   'fleet_dormant', 'fleet_last_reachable_ts',
                   'run_complete',
                   'bev_pause', 'bev_pause_history',
                   'bev_pool_escalation', 'bev_pool_probes',
                   'ip_pools', 'ip_pools_history',
                   # Bandwidth map: only the director-loop worker
                   # updates this in memory. Without pulling it from
                   # disk, the other gunicorn worker's _bw_map is
                   # empty/stale, so cache_only_eligible diverges
                   # between workers and the dashboard flickers
                   # (e.g. '16/28 cache' one tick, different the
                   # next). _fleet_bw_summary indirectly suffers too
                   # (effective_budget/remaining_gb decorated onto
                   # peers_status come from this map).
                   'peer_bandwidth',
                   '_bandwidth_backoff', '_bandwidth_misses'):
            if _k in disk_state:
                state[_k] = disk_state[_k]
        # Capability cache: adopt the sibling worker's fresher probes so
        # only one worker ever fans out /info + /credentials.
        try:
            _dc = disk_state.get('_peer_caps') or {}
            if isinstance(_dc, dict) and _dc:
                with self._lock:
                    _mc = self.state.setdefault('_peer_caps', {})
                    for _pid, _e in _dc.items():
                        if isinstance(_e, dict) and float(_e.get('at') or 0) > \
                                float((_mc.get(_pid) or {}).get('at') or 0):
                            _mc[_pid] = dict(_e)
        except Exception:
            pass
        # Sync the per-process EMA caches from disk too. Workers that
        # don't run the director loop never update these in memory, so
        # _max_parallel_frontiers / _target_frontier_count would compute
        # against the THROTTLE_MAX_FACTOR defaults — producing a higher
        # frontier target than the director-running worker. Two workers
        # serving alternating /api/v1/director/status requests then make
        # the dashboard flicker between e.g. 4 and 8 max frontiers.
        try:
            _disk_ema = disk_state.get('capacity_ema_persisted')
            if _disk_ema is not None:
                self._capacity_ema = float(_disk_ema)
            _disk_sub = disk_state.get('sub_factor_ema') or {}
            for _kk in ('bev', 'zenodo', 'copernicus'):
                if _kk in _disk_sub:
                    self._sub_factor_ema[_kk] = float(_disk_sub[_kk])
        except Exception:
            pass

        # Poll all peer statuses in parallel — a single wedged peer
        # must never wedge the dashboard request.
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FTimeout
        statuses: dict[str, dict] = {}
        peers_list = list(cfg.get('peers', []))

        def _fallback_from_cache(reason: str) -> None:
            """Populate ``statuses`` from the per-URL cache (or mark
            as unreachable). Used when the thread pool is unusable.
            """
            import time as _t
            for p in peers_list:
                cached = _PEER_STATUS_CACHE.get(p.get('url') or '')
                if cached and (_t.time() - cached[0]) < _PEER_STATUS_CACHE_TTL:
                    d = dict(cached[1])
                    d['_stale'] = True
                    d['_stale_age_s'] = round(_t.time() - cached[0], 1)
                    d['_stale_error'] = reason
                    statuses[p['id']] = d
                else:
                    statuses[p['id']] = {'state': 'unreachable',
                                         'error': reason}

        # Guard against ``RuntimeError: cannot schedule new futures
        # after interpreter shutdown`` — happens when a gunicorn worker
        # has hit EMFILE (Too many open files) and is mid-shutdown but
        # Flask is still trying to dispatch /director/status. The
        # exception used to fire on every poll for the dying request,
        # spamming the log with thousands of identical tracebacks
        # (which previously masqueraded as a recursion error in the
        # dashboard log).
        try:
            with ThreadPoolExecutor(
                max_workers=BANDWIDTH_POLL_CONCURRENCY,
                thread_name_prefix='dir-st',
            ) as ex:
                futs = {ex.submit(get_peer_status, p.get('url')): p
                        for p in peers_list}
                import time as _t
                deadline = _t.time() + 15
                for fut, p in list(futs.items()):
                    try:
                        statuses[p['id']] = fut.result(
                            timeout=max(0.1, deadline - _t.time())
                        )
                    except (FTimeout, Exception) as e:
                        # Worker-pool budget blew up before the per-call
                        # timeout fired — try the last-known cached
                        # status before declaring this peer unreachable.
                        cached = _PEER_STATUS_CACHE.get(p.get('url') or '')
                        if cached and (_t.time() - cached[0]) < _PEER_STATUS_CACHE_TTL:
                            d = dict(cached[1])
                            d['_stale'] = True
                            d['_stale_age_s'] = round(_t.time() - cached[0], 1)
                            d['_stale_error'] = str(e)[:120]
                            statuses[p['id']] = d
                        else:
                            statuses[p['id']] = {'state': 'unreachable',
                                                 'error': str(e)}
        except RuntimeError as _rte:
            log.warning('get_status: thread pool unavailable (%s) — '
                        'returning cached peer statuses', _rte)
            _fallback_from_cache('interpreter_shutdown')

        # Persistent per-peer metadata: remembers last-known git_commit,
        # region, last_kg_code/name/uploaded_at across peer reboots and
        # status timeouts. Mutated below when a peer reports fresh values.
        meta_cache = state.setdefault('peer_meta', {})
        peers_status = []
        for peer in peers_list:
            pid = peer['id']
            url = peer.get('url')
            ps = statuses.get(pid, {'state': 'unreachable'})
            # Refresh meta_cache from the live status (only when the peer
            # actually reported a value — never overwrite a known value
            # with an empty string from a stale/proxied response).
            try:
                _meta = meta_cache.setdefault(pid, {})
                for _k in ('git_commit', 'region', 'last_kg_code',
                           'last_kg_name', 'last_kg_uploaded_at',
                           'last_kg_seconds'):
                    _v = ps.get(_k)
                    if _v not in (None, '', 0):
                        _meta[_k] = _v
            except Exception:
                pass
            bw = dict(state.get('peer_bandwidth', {}).get(pid, {}))
            proc_status = ps.get('state', 'unknown')
            # Surface effective per-peer budget so dashboard / API
            # consumers see the canary override, not just the global.
            _eff_budget_gb = round(_peer_budget_bytes(peer, cfg) / (1024 ** 3), 2)
            bw['effective_budget_gb'] = _eff_budget_gb
            if peer.get('budget_gb') is not None:
                # Recompute remaining_gb against the per-peer budget so
                # the dashboard doesn't show "0.0 GB remaining" for a
                # canary that actually has headroom under its override.
                _used_bytes = bw.get('used_bytes', 0) or 0
                bw['remaining_gb'] = round(
                    max(0, int(_eff_budget_gb * (1024 ** 3)) - _used_bytes)
                    / (1024 ** 3), 2)
                bw['budget_gb'] = _eff_budget_gb

            # Per-peer renew_day: explicit override > day-of-month of
            # first_seen > global cfg renew_day. This is informational
            # only — vnstat cycle math still uses the global anchor (no
            # peer rollout required to change behaviour) — but it lets
            # the dashboard show realistic per-peer reset estimates.
            _renew_day = peer.get('renew_day')
            if not _renew_day:
                _fs = peer.get('first_seen')
                if isinstance(_fs, str) and len(_fs) >= 10:
                    try:
                        _renew_day = int(_fs[8:10])
                    except (ValueError, TypeError):
                        _renew_day = None
            if not _renew_day:
                _renew_day = cfg.get('renew_day', BANDWIDTH_RENEW_DAY)

            # Park-reason surfaced from the latest canary_note so the
            # dashboard can distinguish bev_pool_park (whole /24 stuck
            # on a flaky route) from steal_park / auto_park /
            # park_until_renewal / role_park. Used to render the
            # POOL-PARKED tag + cooldown chip on the peer strip.
            _park_reason = None
            _park_pool = None
            if peer.get('not_before'):
                _notes = peer.get('canary_notes') or []
                if _notes and isinstance(_notes[-1], dict):
                    _last = _notes[-1]
                    _park_reason = _last.get('event')
                    _park_pool = _last.get('pool')
            peers_status.append({
                'id': pid,
                'url': url,
                'enabled': peer.get('enabled', True),
                'not_before': peer.get('not_before'),
                'park_reason': _park_reason,
                'park_pool': _park_pool,
                'scheduled': _peer_is_scheduled(peer),
                'reserved_kg': peer.get('reserved_kg'),
                'renew_day': _renew_day,
                'observed_cap_gb': peer.get('observed_cap_gb'),
                'observed_cap_at': peer.get('observed_cap_at'),
                'first_seen': peer.get('first_seen'),
                'stale_status': bool(ps.get('_stale')),
                'stale_age_s': ps.get('_stale_age_s'),
                'zenodo_cooldown_history': peer.get('zenodo_cooldown_history') or [],
                # Warning fingerprint for load-shifting visibility.
                'warning_rates': ps.get('warning_rates') or {},
                'noise_score': round(_peer_noise_score(pid, state), 3),
                'circuit_breaker': _cb_state_for(pid),
                'canary': self._canary_summary(peer, state),
                'role': self._peer_role(peer),
                'cache_only_run': bool(ps.get('cache_only')),
                'is_active': pid == state.get('active_peer'),
                'processor_state': proc_status,
                'current_kg': (ps.get('current_kg') or {}).get('code'),
                'current_kg_name': (ps.get('current_kg') or {}).get('name'),
                'current_kg_started_at': (ps.get('current_kg') or {}).get('started_at'),
                'current_kg_step': (ps.get('current_kg') or {}).get('step'),
                'current_kg_tile': (ps.get('current_kg') or {}).get('current_tile'),
                'current_kg_n_tiles': (ps.get('current_kg') or {}).get('n_tiles'),
                # v2 rollout telemetry (progress.json fields written by
                # austria_processor main(); dashboard peer badges).
                'current_kg_v2_upgrade': bool((ps.get('current_kg') or {}).get('v2_upgrade')),
                'model_version': ps.get('model_version'),
                'v2_upgrade_mode': bool(ps.get('v2_upgrade')),
                'v2_upgraded': int(ps.get('v2_upgraded') or 0),
                'completed': ps.get('completed', 0),
                'bandwidth': bw,
                'online': proc_status != 'unreachable',
                'git_commit': (ps.get('git_commit')
                               or (meta_cache.get(pid) or {}).get('git_commit', '')),
                'region': (ps.get('region')
                           or (meta_cache.get(pid) or {}).get('region', '')),
                # Last completed KG (sticky across status timeouts).
                'last_kg_code': (ps.get('last_kg_code')
                                 or (meta_cache.get(pid) or {}).get('last_kg_code')),
                'last_kg_name': (ps.get('last_kg_name')
                                 or (meta_cache.get(pid) or {}).get('last_kg_name')),
                'last_kg_uploaded_at': (ps.get('last_kg_uploaded_at')
                                        or (meta_cache.get(pid) or {}).get('last_kg_uploaded_at')),
                'last_kg_seconds': (ps.get('last_kg_seconds')
                                    or (meta_cache.get(pid) or {}).get('last_kg_seconds')),
                # Authoritative cred_indices/lat_strips from the peer's own
                # progress.json — what the running processor subprocess
                # actually has in env. Used as fallback when the director's
                # frontier_cred_plan / frontier_strip_plan is stale/empty.
                '_reported_cred_indices': ps.get('cred_indices'),
                '_reported_lat_strips': ps.get('lat_strip_filter'),
                '_reported_cells': ps.get('cell_filter'),
                # Stale-update tracking (auto-retry status)
                'update_state': (state.get('peer_update_state') or {}).get(pid),
                # Host telemetry (CPU steal / iowait / busy ewma + perf
                # summary + static host_profile). Peers push these via
                # /api/v1/director/peer_status; without forwarding them
                # here the fleet_cpu line in /process.txt and the
                # steal=/iow= bw_extras chips silently render empty.
                # Trimmed to the fields actually consumed downstream so
                # /director/status doesn't bloat by ~3 KB/peer.
                'system': {
                    k: v for k, v in (ps.get('system') or {}).items()
                    if k in ('cpu_steal', 'cpu_iowait', 'cpu_total',
                             'cpu_user', 'cpu_system',
                             'perf', 'host',
                             'load_1m', 'cpu_pct', 'ram_pct',
                             'disk_free_gb')
                    and v is not None
                },
                # BEV proxy-pool slim summary pushed by the peer
                # (bev_proxy.summary()). Used by _fleet_proxy_summary
                # to render fleet_proxy: in /process.txt without a
                # per-peer /api/v1/info poll.
                'proxy_pool': ps.get('proxy_pool'),
            })

        cache_ready = state.get('_cache_ready_cache') or {}
        # Count only peers whose processor is actively running in cache-only
        # mode — ``cache_only_run`` reflects last-started mode and stays True
        # for stopped/complete peers too.
        cache_only_running = sum(
            1 for p in peers_status
            if p['cache_only_run']
            and p.get('processor_state') in ('running', 'processing')
        )
        # Theoretical max cache-only peers: enabled, online, not
        # scheduled-out (BW park / not_before), not the active frontier,
        # not authorised for parallel-frontier work, not reserved, and
        # with >2 GB headroom on their budget. Matches the gates in
        # ``_orchestrate_cache_only`` so the dashboard chip can show a
        # realistic denominator instead of the static hard cap.
        _parallel_authorised = set(
            state.get('parallel_frontiers_active') or [])
        _parallel_authorised |= set(
            (state.get('frontier_cred_plan') or {}).keys())
        _active_id = state.get('active_peer')
        _bw_map = state.get('peer_bandwidth') or {}
        cache_only_eligible = 0
        for _p in cfg.get('peers', []):
            _pid = _p.get('id')
            if not _pid or not _p.get('enabled', True):
                continue
            if _peer_is_scheduled(_p):
                continue
            if _pid == _active_id:
                continue
            if _pid in _parallel_authorised:
                continue
            if _p.get('reserved_kg'):
                continue
            _bw_p = _bw_map.get(_pid) or {}
            if _peer_bw_depleted(_p, _bw_p, cfg):
                continue
            # Online check via peers_status (which already polled).
            _row = next((r for r in peers_status if r['id'] == _pid), None)
            if _row is None or not _row.get('online', False):
                continue
            cache_only_eligible += 1

        # --- Credential pool & assignment plan ----------------------
        try:
            cred_pool = self._credential_pool()
        except Exception:
            cred_pool = []
        try:
            valid_creds = self._valid_credentials()
        except Exception:
            valid_creds = []
        per = self._effective_creds_per_frontier(cfg)
        max_par = self._max_parallel_frontiers(cfg)
        cred_plan = state.get('frontier_cred_plan') or {}
        strip_plan = state.get('frontier_strip_plan') or {}
        cached_strips = self._cached_lat_ranges() if hasattr(self, '_cached_lat_ranges') else []
        austria_strips = self._austria_lat_strips()
        austria_cells = self._austria_cells()
        # Annotate peer rows with their assignments — but only when
        # the peer is actually doing frontier work. A stale frontier_cred_plan
        # for a peer currently running cache-only would otherwise mislead
        # the dashboard into thinking those creds are locked.
        for row in peers_status:
            pid = row['id']
            proc_st = row.get('processor_state')
            cache_only_run = bool(row.get('cache_only_run'))
            is_running = proc_st in ('running', 'processing')
            holds_creds = is_running and not cache_only_run
            if holds_creds:
                # Prefer the peer's own reported cred_indices (ground truth
                # from its env) over the director's plan, which can be
                # stale across worker restarts or director state resets.
                reported = row.pop('_reported_cred_indices', None)
                if reported:
                    row['cred_indices'] = list(reported)
                elif pid in cred_plan:
                    row['cred_indices'] = cred_plan[pid]
                reported_strips = row.pop('_reported_lat_strips', None)
                reported_cells = row.pop('_reported_cells', None)
                if reported_cells:
                    row['cells'] = list(reported_cells)
                elif reported_strips:
                    row['lat_strips'] = list(reported_strips)
                elif pid in strip_plan:
                    # strip_plan now stores cell 4-tuples; report under
                    # whichever key matches the data shape so the
                    # dashboard can render it.
                    plan = strip_plan[pid]
                    if plan and len(plan[0]) == 4:
                        row['cells'] = plan
                    else:
                        row['lat_strips'] = plan
            else:
                row.pop('_reported_cred_indices', None)
                row.pop('_reported_lat_strips', None)
                row.pop('_reported_cells', None)
            row['pinned_role'] = (
                next((p.get('pinned_role') for p in cfg.get('peers', [])
                      if p['id'] == pid), None)
            )

        # Refresh capability cache for all peers (cheap when cached).
        for peer in peers_list:
            try:
                self._refresh_peer_caps(peer)
            except Exception:
                pass

        # --- Credential holders: which peer currently holds each cred ---
        # A peer 'holds' a credential if it's running frontier work using it.
        # All peers expose cred_subset_env now — they only hold the cred
        # indices in cred_plan. Cache-only peers hold no credentials.
        cred_holders: dict[int, list[str]] = {i: [] for i in range(len(cred_pool))}
        for row in peers_status:
            pid = row['id']
            proc_st = row.get('processor_state')
            # Only actively-running peers hold credentials. Paused peers
            # release their creds so other peers can use them.
            if proc_st not in ('running', 'processing'):
                continue
            if row.get('cache_only_run'):
                continue
            # Use the row's annotated cred_indices, which already prefers
            # the peer-reported ground truth over a possibly-stale plan.
            indices = row.get('cred_indices') or cred_plan.get(pid) or []
            for idx in indices:
                if 0 <= idx < len(cred_pool):
                    cred_holders[idx].append(pid)
        # Annotate cred_pool entries with holders
        for i, c in enumerate(cred_pool):
            c['holders'] = cred_holders.get(i, [])
        # Aggregate per-credential usage telemetry across all peers — the
        # processor runs on peers, so the director's local stats are 0.
        try:
            self._aggregate_credential_usage(cred_pool, peers_list)
        except Exception as e:
            log.debug('aggregate_credential_usage: %s', e)
        # Stash the *aggregated, health-scored* pool into state so the
        # frontier assignment path sees real per-cred health (incl. the
        # ``stalled`` clamp). Without this, ``_assign_cred_indices`` and
        # ``_valid_credentials`` fall back to the primary's LOCAL
        # ``copernicus.list_credentials()`` — but the primary never
        # processes, so every cred there scores a flat 1.0 with no stall
        # signal. Tied scores collapse the rank key ``(-score, idx)`` to
        # pure index order, so the low-index stalled creds (1, 3, 9, 10…)
        # were deterministically assigned first, churning peers onto dead
        # tokens. Only the director-loop worker (which computes this
        # aggregate) writes it; the value rides director_state.json so the
        # other gunicorn worker / HA shadow read the same scores.
        try:
            with self._lock:
                self.state['credentials'] = self._slim_cred_pool_for_state(cred_pool)
        except Exception:
            log.debug('stash aggregated credentials failed', exc_info=True)
        # Append a fleet-shape sample using the SAME numbers we return
        # to the dashboard so the sparkline's last point always matches
        # the card text. Throttled to avoid pile-on from concurrent
        # dashboard polls; the director tick still records its own
        # samples on the 30s cadence as a fallback.
        try:
            _now = int(time.time())
            _last_t = (
                self._peer_history[-1][0]
                if self._peer_history else 0
            )
            if _now - _last_t >= 5:
                _fr = len(state.get('parallel_frontiers_active') or [])
                _co = int(cache_only_running)
                _cr = len(cache_ready.get('codes') or [])
                self._peer_history.append((_now, _fr, _co, _cr))
                with self._lock:
                    self.state['peer_history'] = [
                        {'t': t, 'fr': fr, 'co': co, 'cr': cr}
                        for (t, fr, co, cr) in list(self._peer_history)
                    ]
        except Exception:
            log.debug('peer_history status snapshot failed', exc_info=True)
        return {
            'mode': state.get('mode', 'auto'),
            'active_peer': state.get('active_peer'),
            'last_switch': state.get('last_switch'),
            'budget_gb': cfg.get('budget_gb', BANDWIDTH_BUDGET_GB),
            'renew_day': cfg.get('renew_day', BANDWIDTH_RENEW_DAY),
            'min_reserve_peers': cfg.get('min_reserve_peers', MIN_RESERVE_PEERS),
            'max_cache_only_peers': cfg.get('max_cache_only_peers', MAX_CACHE_ONLY_PEERS),
            'cache_only_eligible': cache_only_eligible,
            'cache_only_running': cache_only_running,
            'cache_ready_kgs': len(cache_ready.get('codes') or []),
            'cache_ready_at': cache_ready.get('at'),
            'credentials': cred_pool,
            'valid_credentials': valid_creds,
            'min_creds_per_frontier': per,
            'min_creds_per_frontier_floor': max(1, int(cfg.get(
                'min_creds_per_frontier',
                self.MIN_CREDS_PER_FRONTIER_DEFAULT))),
            'adaptive_creds_per_frontier': bool(cfg.get(
                'adaptive_creds_per_frontier', True)),
            'max_parallel_frontiers': max_par,
            'cached_lat_strips': [[s, n] for s, n in cached_strips],
            'cached_cells': [list(c) for c in self._cached_cells()],
            'austria_lat_strips': [[s, n] for s, n in austria_strips],
            'austria_cells': [[s, n, w, e] for s, n, w, e in austria_cells],
            'frontier_cells': [list(c) for c in self._frontier_cells()],
            'barren_cells': [list(c) for c in sorted(self._barren_cells())],
            'barren_cells_detail': self.barren_cells_detail(),
            'parallel_frontiers_active': state.get('parallel_frontiers_active', []),
            'cache_miss_count': len(self._load_cache_misses()),
            'cycle_start': get_billing_cycle_start().isoformat(),
            'fleet_bw': self._fleet_bw_summary(peers_status),
            # 5-min TTL'd — cheap on hot status recomputes.
            'bw_learn': self._bw_learn_summary(state, cfg),
            'fleet_proxy': self._fleet_proxy_summary_with_history(peers_status),
            'v2': self._v2_status(),
            'fleet_proxy_history': (
                list(self._proxy_history)
                if getattr(self, '_lock_fd', None) is not None
                else (state.get('fleet_proxy_history') or [])
            ),
            'canary_fleet_slowdown': state.get('canary_fleet_slowdown'),
            'capacity_factor': state.get(
                'capacity_factor', self._capacity_ema),
            'capacity_ema': state.get(
                'capacity_ema_persisted', self._capacity_ema),
            'capacity_components': state.get(
                'capacity_components', self._capacity_components),
            # Use in-memory history if this worker runs the loop;
            # otherwise fall back to the persisted snapshot so workers
            # that never tick still serve a populated chart.
            # Prefer disk-backed history when *this* worker is not the
            # one running the director loop (its in-memory deque was
            # snapshotted at import time and never updates). The
            # loop-running worker persists on every tick, so disk is
            # at most ~30s behind live deque and identical at steady
            # state. Two-worker race ('with_v: 0' vs 'with_v: 1' on
            # alternating polls) is what made the /process.txt steal
            # stats look empty for ~30 min after a srv restart.
            'capacity_history': (
                [(_capacity_history_entry(_e))
                 for _e in list(self._capacity_history)]
                if getattr(self, '_lock_fd', None) is not None
                else (state.get('capacity_history') or [])
            ),
            'peer_history': (
                [{'t': t, 'fr': fr, 'co': co, 'cr': cr}
                 for (t, fr, co, cr) in list(self._peer_history)]
                if self._peer_history
                else (state.get('peer_history') or [])
            ),
            # Per-peer warning rates from the last director tick. Used
            # by the dashboard noise pill and by load-shifting logic.
            'peer_warning_rates': state.get('peer_warning_rates') or {},
            'peer_noise_long_ema': state.get('peer_noise_long_ema') or {},
            'peer_steal_ema': state.get('peer_steal_ema') or {},
            'peer_last_live_ts': state.get('peer_last_live_ts') or {},
            'peer_meta': state.get('peer_meta') or {},
            # BEV outage + IP-pool stats (surfaced in /process.txt).
            'fleet_dormant': state.get('fleet_dormant') or {},
            'run_complete': {k: v for k, v in (state.get('run_complete') or {}).items() if not k.startswith('_')},
            'zenodo_circuit': {k: v for k, v in (state.get('zenodo_circuit') or {}).items() if not k.startswith('_')},
            'cache_reconcile': state.get('cache_reconcile') or {},
            'bev_pause': state.get('bev_pause') or {},
            'bev_pause_history': state.get('bev_pause_history') or [],
            'bev_pool_escalation': state.get('bev_pool_escalation') or {},
            'bev_pool_probes': state.get('bev_pool_probes') or {},
            'ip_pools': state.get('ip_pools') or {},
            'ip_pools_history': state.get('ip_pools_history') or [],
            'shadow_peer': state.get('shadow_peer'),
            'shadow_url': state.get('shadow_url'),
            'shadow_last_push_ts': state.get('shadow_last_push_ts'),
            'shadow_last_push_ok': state.get('shadow_last_push_ok'),
            'shadow_last_push_kind': state.get('shadow_last_push_kind'),
            'shadow_last_push_bytes': state.get('shadow_last_push_bytes'),
            # HA hardening (edbf5b1): merged-log shadow delta + edge
            # reachability probe — rendered as ``log_delta=`` / ``edge:``
            # in /process.txt. Slim the probe ring to the summary fields.
            'shadow_log_delta_last': state.get('shadow_log_delta_last'),
            'edge_probe': {
                k: v for k, v in (state.get('edge_probe') or {}).items()
                if k in ('last', 'last_ts', 'unreachable_since',
                         'consecutive_fail', 'handed_over_ts')
            },
            'is_director_local': config.is_director(),
            'self_id': self._self_id_safe(),
            # Pointer to the current director (peer view): when this
            # VM is *not* the director, the dashboard renders a
            # "⇗ director: <id>" link to the director's process.html
            # so the user always knows where to find the live
            # orchestration view.
            'self_url': self._self_url_safe(),
            'director_url': self._director_url_safe(),
            'frontier_cred_plan': state.get('frontier_cred_plan') or {},
            'frontier_strip_plan': state.get('frontier_strip_plan') or {},
            'frontier_cell_plan': state.get('frontier_strip_plan') or {},
            'peers': peers_status,
        }

    def _self_id_safe(self) -> str:
        return config.self_id()

    def _self_url_safe(self) -> str | None:
        return config.env('SRTM_SELF_URL') or (config.APP_URL if config.is_director() else None)

    def _director_url_safe(self) -> str | None:
        """URL of the currently-authoritative director.

        On the director itself this returns its own URL (self_url).
        On a peer it returns ``config.PRIMARY_URL``. Used by the dashboard
        to render a link to the director's process.html so users
        opening a peer dashboard can navigate to the live
        orchestration view in one click.
        """
        if config.is_director():
            return self._self_url_safe()
        return config.PRIMARY_URL or None

    def set_mode(self, mode: str):
        write_director_mode(mode)  # sidecar first: authoritative
        with self._lock:
            self.state['mode'] = mode
            save_director_state(self.state)
        _emit_director_event(f'director mode → {mode}', peer='director',
                             level='warning' if mode == 'paused' else 'info')

    def set_active_peer(self, peer_id: str | None):
        """Manually set the active peer (for manual mode)."""
        with self._lock:
            self.state['active_peer'] = peer_id
            self.state['last_switch'] = datetime.now(timezone.utc).isoformat()
            save_director_state(self.state)

    def reload_config(self):
        with self._lock:
            self.cfg = load_peers_config()
        # Refresh the URL→id reverse map so module-level helpers can
        # attribute circuit-breaker events without threaded peer_id args.
        global _PEER_URL_TO_ID
        _PEER_URL_TO_ID = {
            (p.get('url') or '').rstrip('/'): p.get('id') or ''
            for p in (self.cfg.get('peers') or [])
            if p.get('url') and p.get('id')
        }

    def propagate_throttle(self, enabled: bool) -> dict:
        """Propagate throttle state to all remote peers."""
        results = {}
        with self._lock:
            cfg = self.cfg.copy()
        for peer in cfg.get('peers', []):
            url = peer.get('url')
            if url is None:
                continue
            try:
                # First check current state
                r = requests.get(url.rstrip('/') + '/api/v1/processing/throttle',
                                 timeout=PEER_TIMEOUT_PROBE,
                                 headers=_admin_headers())
                current = r.json().get('throttle', False) if r.ok else None
                if current == enabled:
                    results[peer['id']] = 'already_' + ('on' if enabled else 'off')
                    continue
                # Toggle to match desired state
                r2 = requests.post(url.rstrip('/') + '/api/v1/processing/throttle',
                                   timeout=PEER_TIMEOUT_CONTROL,
                                   headers=_admin_headers())
                if r2.ok:
                    results[peer['id']] = 'set_' + ('on' if enabled else 'off')
                else:
                    results[peer['id']] = f'error_{r2.status_code}'
            except Exception as e:
                results[peer['id']] = f'error: {e}'
        return results

    def remove_peer(self, peer_id: str) -> dict:
        """Remove a peer from config. Stops its processor first."""
        with self._lock:
            cfg = self.cfg
        peer = get_peer_by_id(cfg, peer_id)
        if not peer:
            return {'error': f'Peer {peer_id} not found'}
        if peer.get('url') is None:
            return {'error': 'Cannot remove the primary peer'}
        # Stop processor on the peer
        url = peer['url']
        try:
            stop_peer_processor(url)
        except Exception as e:
            log.warning('Failed to stop peer %s before removal: %s', peer_id, e)
        # Remove from config
        with self._lock:
            cfg['peers'] = [p for p in cfg.get('peers', []) if p['id'] != peer_id]
            # Pass removed_ids so the merge-with-disk in save_peers_config
            # can't resurrect this peer from a stale on-disk snapshot
            # written by a different worker between our load and save.
            save_peers_config(cfg, removed_ids={peer_id})
            # Clear from state
            self.state.get('peer_bandwidth', {}).pop(peer_id, None)
            if self.state.get('active_peer') == peer_id:
                self.state['active_peer'] = None
            _gc_peer_state(self.state, {peer_id})
            save_director_state(self.state)
            self.cfg = cfg
        # Remove from peer_urls.txt
        peer_urls_path = DATA_DIR / 'peer_urls.txt'
        if peer_urls_path.exists():
            try:
                urls = {u.strip() for u in peer_urls_path.read_text().splitlines() if u.strip()}
                urls.discard(url)
                peer_urls_path.write_text('\n'.join(sorted(urls)) + '\n' if urls else '')
            except Exception:
                pass
        log.info('Removed peer %s (%s)', peer_id, url)
        return {'status': 'removed', 'peer_id': peer_id}

    def _update_bandwidth(self):
        """Refresh bandwidth data for all peers in parallel.

        A single wedged peer must never delay the loop.  Each remote peer
        is polled in a worker thread with short tuple-timeouts (connect=3,
        read=8).  After BANDWIDTH_BACKOFF_THRESHOLD consecutive failures
        we skip polling that peer for BANDWIDTH_BACKOFF_SECONDS so the
        loop stays fast even when half the fleet is dead.
        """
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FTimeout
        import time as _t

        peers = list(self.cfg.get('peers', []))
        # Local first — cheap, in-process.
        for peer in peers:
            if peer.get('url') is None:
                bw = get_local_bandwidth()
                with self._lock:
                    self.state.setdefault('peer_bandwidth', {})[peer['id']] = bw

        remote = [p for p in peers if p.get('url')]
        if not remote:
            return

        with self._lock:
            backoff = self.state.setdefault('_bandwidth_backoff', {})
            misses = self.state.setdefault('_bandwidth_misses', {})
        now = _t.time()

        # Filter peers currently in backoff so the loop stays fast.
        to_poll = []
        for p in remote:
            pid = p['id']
            until = backoff.get(pid, 0)
            if until > now:
                continue
            to_poll.append(p)

        results: dict[str, dict] = {}
        if to_poll:
            poll_started = _t.time()
            try:
                with ThreadPoolExecutor(
                    max_workers=BANDWIDTH_POLL_CONCURRENCY,
                    thread_name_prefix='dir-bw'
                ) as ex:
                    futs = {ex.submit(get_peer_bandwidth, p['url']): p for p in to_poll}
                    # Hard wall-clock budget: even if a worker hangs we
                    # don't wait more than this for the whole batch.
                    deadline = poll_started + 15
                    for fut, p in list(futs.items()):
                        try:
                            results[p['id']] = fut.result(
                                timeout=max(0.1, deadline - _t.time())
                            )
                        except FTimeout:
                            results[p['id']] = {
                                'error': 'bandwidth poll exceeded 15 s budget',
                                'used_bytes': 0, 'used_gb': 0,
                                'remaining_gb': BANDWIDTH_BUDGET_GB,
                                'pct_used': 0, 'estimated': True,
                            }
                        except Exception as e:
                            results[p['id']] = {
                                'error': str(e),
                                'used_bytes': 0, 'used_gb': 0,
                                'remaining_gb': BANDWIDTH_BUDGET_GB,
                                'pct_used': 0, 'estimated': True,
                            }
            except Exception:
                log.exception('bandwidth pool error')
            log.debug('bandwidth poll: %d peers in %.1fs',
                     len(to_poll), _t.time() - poll_started)

        # Apply results + maintain backoff bookkeeping.
        with self._lock:
            self.state.setdefault('peer_bandwidth', {})
            for p in remote:
                pid = p['id']
                if pid in results:
                    bw = results[pid]
                    self.state['peer_bandwidth'][pid] = bw
                    if bw.get('error'):
                        misses[pid] = misses.get(pid, 0) + 1
                        if misses[pid] >= BANDWIDTH_BACKOFF_THRESHOLD:
                            backoff[pid] = now + BANDWIDTH_BACKOFF_SECONDS
                            log.warning(
                                'Backing off bandwidth poll for %s for %ds '
                                'after %d failures (last err: %s)',
                                pid, BANDWIDTH_BACKOFF_SECONDS,
                                misses[pid], str(bw.get('error'))[:80]
                            )
                    else:
                        misses.pop(pid, None)
                        backoff.pop(pid, None)
                # else: still in backoff, keep last known value
            self.state['_bandwidth_backoff'] = backoff
            self.state['_bandwidth_misses'] = misses
        # Sample canary throughput AFTER the bandwidth state has
        # settled. Cheap (one append per peer) and guarded so the
        # main loop is unaffected if it raises.
        try:
            self._sample_canary_history()
        except Exception:
            log.debug('canary sample failed', exc_info=True)
        try:
            # Hourly cadence — bw_history only grows once per hour, so
            # scanning more often is pure busywork. The throttle is
            # in-memory only; a restart re-runs immediately (which is
            # the right behaviour: catch up after downtime).
            _now = time.time()
            if (_now - getattr(self, '_renew_learn_last_ts', 0.0)
                    >= BW_HISTORY_INTERVAL_S):
                self._learn_renew_days()
                self._renew_learn_last_ts = _now
        except Exception:
            log.debug('renew-day learn failed', exc_info=True)

    def _learn_renew_days(self) -> None:
        """Persist the empirically-observed billing-cycle renew day
        for any peer that doesn't have an explicit override.

        Replaces the ``first_seen`` day-of-month heuristic with the
        actually-measured drop in cumulative used_bytes. Only updates
        when the observation differs from the currently-effective
        renew_day for that peer, and only when ``peer.renew_day`` is
        not explicitly set by the operator (which always wins).

        Self-heals the dashboard's ``next_renew_in_days`` chip and
        keeps eligibility / park-until-renewal math anchored on real
        per-peer cycles instead of a fleet-wide guess.
        """
        with self._lock:
            cfg = self.cfg
            state = self.state
        dirty = False
        for p in cfg.get('peers', []):
            pid = p.get('id')
            if not pid:
                continue
            # Operator override always wins.
            if p.get('renew_day_override'):
                continue
            learned = _learned_renew_day(pid, state)
            if learned is None:
                continue
            prev_learned = p.get('renew_day_learned')
            if prev_learned == learned:
                continue
            p['renew_day_learned'] = learned
            # Promote learned value into the live ``renew_day`` field
            # so all existing code paths (eligibility, _peer_next_renew,
            # process.txt rendering) pick it up without conditional
            # plumbing. ``renew_day_learned`` is the audit trail.
            p['renew_day'] = learned
            dirty = True
            try:
                self.director_event(
                    pid,
                    'learned billing renew_day=%d from observed BW drop' % learned,
                    level='info', kind='bw_learn',
                )
            except Exception:
                pass
        if dirty:
            try:
                save_peers_config(cfg)
            except Exception:
                log.warning('save peers.json after renew-day learn', exc_info=True)

    # ------------------------------------------------------------------
    # Canary bandwidth history
    # ------------------------------------------------------------------
    def _sample_canary_history(self) -> None:
        """Append a (ts, used_bytes, mode) sample for every enabled peer.

        ``mode`` is 1 when the peer is in frontier mode at sample time
        (active frontier or one of the parallel frontiers), 0 otherwise.
        Tagging samples lets the quality-gate restrict its baseline to
        ticks when the peer was network-bound (BEV reads sustain >>5
        MB/s); pure cache-only / Zenodo-upload baselines are <0.5 MB/s
        and can never satisfy the network-grade gate, so without
        tagging the quality observation never fires.

        Legacy 2-element samples are upgraded in-place on read.
        """
        with self._lock:
            cfg = self.cfg
            bw_map = self.state.get('peer_bandwidth') or {}
            hist = self.state.setdefault('canary_history', {})
            now = int(time.time())
            keep_ids = {p['id'] for p in cfg.get('peers', [])
                        if p.get('enabled', True)}
            for stale_pid in list(hist.keys()):
                if stale_pid not in keep_ids:
                    hist.pop(stale_pid, None)
            # Frontier-mode set: active + parallel frontiers as last
            # planned by the director loop. Cheap O(N).
            frontier_ids = set()
            ap = self.state.get('active_peer')
            if ap:
                frontier_ids.add(ap)
            frontier_ids |= set(
                self.state.get('parallel_frontiers_active') or [])
            frontier_ids |= set(
                (self.state.get('frontier_cred_plan') or {}).keys())
            for p in cfg.get('peers', []):
                if not p.get('enabled', True):
                    hist.pop(p['id'], None)
                    continue
                pid = p['id']
                bw = bw_map.get(pid) or {}
                used = bw.get('used_bytes')
                if used is None or bw.get('error'):
                    continue
                series = hist.setdefault(pid, [])
                mode = 1 if pid in frontier_ids else 0
                # Skip duplicate samples (peer cached, no fresh data).
                if series and series[-1][0] == now:
                    continue
                series.append([now, int(used), mode])
                # Trim to ring size.
                if len(series) > CANARY_HISTORY_MAX:
                    del series[: len(series) - CANARY_HISTORY_MAX]
            # Cycle-renewal detection (cheap, append-only).
            # We don't store a full 30-day used_bytes ring — just the
            # last sample + a tiny event log of detected drops. A
            # renewal is the only thing that makes cumulative
            # used_bytes decrease, so a one-step diff catches it.
            # See fleet_proxy_history (commit 27aa243) for the same
            # discipline: persist only the shape the dashboard / learner
            # actually needs.
            bw_cycle = self.state.setdefault('bw_cycle', {})
            for stale_pid in list(bw_cycle.keys()):
                if stale_pid not in keep_ids:
                    bw_cycle.pop(stale_pid, None)
            one_gb = 1024 ** 3
            for p in cfg.get('peers', []):
                if not p.get('enabled', True):
                    continue
                pid = p['id']
                bw = bw_map.get(pid) or {}
                used = bw.get('used_bytes')
                if used is None or bw.get('error'):
                    continue
                used = int(used)
                rec = bw_cycle.setdefault(pid, {'last': None, 'events': []})
                last = rec.get('last') or [0, 0]
                # Throttle: only check at most once per
                # BW_HISTORY_INTERVAL_S. The tick fires every ~minute,
                # so this collapses 60x of work.
                if last[0] and (now - last[0]) < BW_HISTORY_INTERVAL_S:
                    continue
                prev_ts, prev_used = last
                # Detect renewal drop relative to previous sample.
                if prev_used and used < prev_used:
                    drop = prev_used - used
                    if (drop / one_gb) >= BW_RENEWAL_MIN_DROP_GB and \
                       prev_used > 0 and (drop / prev_used) >= BW_RENEWAL_DROP_FRAC:
                        rec['events'].append([
                            now,
                            round(prev_used / one_gb, 1),
                            round(used / one_gb, 1),
                        ])
                        if len(rec['events']) > BW_RENEWAL_EVENTS_MAX:
                            del rec['events'][:len(rec['events']) - BW_RENEWAL_EVENTS_MAX]
                rec['last'] = [now, used]

    def _canary_throughput(self, pid: str, window_s: int,
                           *, frontier_only: bool = False) -> dict | None:
        """Return mean throughput (bytes/s) over the trailing window.

        Returns dict with ``samples``, ``span_s``, ``delta_bytes``,
        ``rate_bps``, or None if not enough data.

        ``frontier_only=True`` restricts the trailing window to samples
        captured while the peer was in frontier mode. Used by the
        quality gate so the fleet wall isn't polluted by Zenodo-bound
        cache-only baselines. Returns None if fewer than 2 such samples
        in the window.
        """
        series = (self.state.get('canary_history') or {}).get(pid) or []
        if len(series) < 2:
            return None
        cutoff = series[-1][0] - window_s
        # Find first sample >= cutoff.
        i = 0
        for i in range(len(series)):
            if series[i][0] >= cutoff:
                break
        window = series[i:]
        if frontier_only:
            # Sample is [ts, used, mode]; legacy 2-tuples treated as
            # cache-mode (mode=0) so legacy data can never falsely
            # satisfy the quality gate.
            window = [s for s in window
                      if len(s) >= 3 and s[2] == 1]
        if len(window) < 2:
            return None
        span = window[-1][0] - window[0][0]
        if span <= 0:
            return None
        delta = max(0, window[-1][1] - window[0][1])
        return {
            'samples': len(window),
            'span_s': span,
            'delta_bytes': delta,
            'rate_bps': delta / span,
        }

    def _canary_summary(self, peer: dict, state: dict) -> dict | None:
        """Compact summary for the status payload.

        Every enabled peer is sampled (canary-by-default). ``budget_gb``
        on the peer flags the *override* canaries probing past the
        global cap, surfaced to the dashboard as ``override:true``.
        Returns None only for peers with no samples yet so the
        dashboard can keep using ``peer.canary`` as both a presence
        check and a data source for the badge.
        """
        pid = peer['id']
        if not (state.get('canary_history') or {}).get(pid):
            return None
        series = (state.get('canary_history') or {}).get(pid) or []
        base = self._canary_throughput(pid, CANARY_BASELINE_WINDOW_S)
        recent = self._canary_throughput(pid, CANARY_RECENT_WINDOW_S)
        # Last ~30 sample points for sparkline rendering.
        spark = [
            {'t': int(s[0]), 'used_bytes': int(s[1])}
            for s in series[-60:]
        ]
        return {
            'budget_gb': peer.get('budget_gb'),
            'override': peer.get('budget_gb') is not None,
            'observed_cap_gb': peer.get('observed_cap_gb'),
            'samples': len(series),
            'baseline_mbps': round(base['rate_bps'] / 1e6, 3) if base else None,
            'recent_mbps': round(recent['rate_bps'] / 1e6, 3) if recent else None,
            'ratio': (round(recent['rate_bps'] / base['rate_bps'], 3)
                       if base and recent and base['rate_bps'] > 0 else None),
            'noise_score': round(_peer_noise_score(pid, state), 3),
            'park_thresholds': {
                'slowdown_ratio': CANARY_SLOWDOWN_RATIO,
                'noise_score': CANARY_NOISE_PARK_THRESHOLD,
                'cooldown_s': CANARY_PARK_COOLDOWN_S,
            },
            'history': spark,
            'notes': (peer.get('canary_notes') or [])[-5:],
        }

    # 5-min TTL on the aggregate. Underlying data only changes on a
    # detected renewal drop (rare) or an operator edit; the heavier
    # status cache (30 s + stale-while-revalidate) sits above this so
    # in practice the compute fires at most every few minutes per
    # worker. Costs O(peers) per recompute (tiny event lists).
    _BW_LEARN_CACHE_TTL = 300.0

    def _bw_learn_summary(self, state: dict, cfg: dict) -> dict:
        import time as _t
        cache = getattr(self, '_bw_learn_cache', None)
        if cache and (_t.time() - cache[0]) < self._BW_LEARN_CACHE_TTL:
            return cache[1]
        out = self._bw_learn_summary_compute(state, cfg)
        self._bw_learn_cache = (_t.time(), out)
        return out

    def _bw_learn_summary_compute(self, state: dict, cfg: dict) -> dict:
        """Aggregate view of empirically-learned renew days.

        Returns:
          * ``observed``: list of (peer_id, day, prev_gb, new_gb, ts)
            for each *most-recent* observed BW renewal across peers,
            newest first.
          * ``by_day``: histogram of effective renew_day across the
            fleet so the operator can see how peers cluster.
          * ``peers_with_history``: how many peers have any BW history
            samples (telemetry coverage).
          * ``peers_learned``: how many peers have a learned renew_day.
        """
        from collections import Counter
        bw_cycle = (state or {}).get('bw_cycle') or {}
        observed: list[dict] = []
        peers_with_history = 0
        for p in cfg.get('peers', []):
            pid = p.get('id')
            if not pid:
                continue
            rec = bw_cycle.get(pid) or {}
            if rec.get('last'):
                peers_with_history += 1
            events = rec.get('events') or []
            if not events:
                continue
            ts_after, prev_gb, new_gb = events[-1][0], events[-1][1], events[-1][2]
            observed.append({
                'peer_id': pid,
                'day': datetime.fromtimestamp(
                    int(ts_after), tz=timezone.utc).day,
                'prev_gb': round(float(prev_gb), 1),
                'new_gb': round(float(new_gb), 1),
                'ts': int(ts_after),
            })
        observed.sort(key=lambda e: -e['ts'])
        # Histogram of *effective* renew_day across the fleet — plus
        # the per-source breakdown so the operator can tell apart
        # "50 peers have a learned anchor" from "50 peers fell
        # through to the first_seen day-of-month fallback". Without
        # this split the dashboard looks identical whether the fleet
        # has real evidence or is guessing wildly.
        by_day = Counter()
        sources = Counter()  # override / learned / stored / first_seen / global
        learned = 0
        for p in cfg.get('peers', []):
            try:
                by_day[_peer_renew_day(p, cfg)] += 1
            except Exception:
                pass
            # Categorise the source that won inside _peer_renew_day.
            if p.get('renew_day_override'):
                src = 'override'
            elif p.get('renew_day_learned'):
                src = 'learned'
            elif p.get('renew_day'):
                src = 'stored'
            else:
                fs = p.get('first_seen')
                if isinstance(fs, str) and len(fs) >= 10:
                    try:
                        int(fs[8:10])
                        src = 'first_seen'
                    except (ValueError, TypeError):
                        src = 'global'
                else:
                    src = 'global'
            sources[src] += 1
            if p.get('renew_day_learned'):
                learned += 1
        return {
            'peers_with_history': peers_with_history,
            'peers_learned': learned,
            'observed': observed,
            'by_day': dict(sorted(by_day.items())),
            'sources': dict(sources),
        }

    @staticmethod
    def _fleet_bw_summary(peers_status: list[dict]) -> dict:
        """Aggregate bandwidth view that doesn't lie about the wall.

        Replaces the old "GB left of N\u00a0GB" aggregate (which assumed
        all peers share one cycle anchor and one budget). Reports:

          * ``used_gb``: actual bytes consumed across enabled peers
            this cycle (the only number we measure, not guess).
          * ``budget_gb``: nominal sum of effective per-peer budgets,
            for context only — NOT a reliable wall.
          * ``parked``: peers currently in not_before cooldown.
          * ``observed_cap_gb_min/median``: distilled from
            ``observed_cap_gb`` across peers that have hit the wall.
          * ``next_renew_in_days``: minimum days-to-renew across peers
            (per-peer renew_day) so the dashboard shows the soonest
            upcoming reset rather than averaging.
        """
        from datetime import datetime as _dt, timezone as _tz
        now = _dt.now(_tz.utc)
        used = 0.0
        budget = 0.0
        n_total = 0
        n_parked = 0
        n_with_cap = 0
        caps: list[float] = []
        soonest_days = None
        for p in peers_status:
            if not p.get('enabled'):
                continue
            n_total += 1
            bw = p.get('bandwidth') or {}
            if isinstance(bw.get('used_gb'), (int, float)):
                used += float(bw['used_gb'])
            eb = bw.get('effective_budget_gb') or bw.get('budget_gb')
            if isinstance(eb, (int, float)):
                budget += float(eb)
            if p.get('scheduled'):
                n_parked += 1
            cap = p.get('observed_cap_gb')
            cap_quality = bool(p.get('observed_cap_quality'))
            if isinstance(cap, (int, float)) and cap_quality:
                n_with_cap += 1
                caps.append(float(cap))
            rd = p.get('renew_day')
            if isinstance(rd, int) and 1 <= rd <= 28:
                # next occurrence of rd this/next month
                try:
                    nxt = now.replace(day=rd, hour=0, minute=0,
                                      second=0, microsecond=0)
                    if nxt <= now:
                        m = now.month + 1
                        y = now.year + (1 if m > 12 else 0)
                        m = ((m - 1) % 12) + 1
                        nxt = nxt.replace(year=y, month=m)
                    days = max(0, int((nxt - now).total_seconds() // 86400))
                    if soonest_days is None or days < soonest_days:
                        soonest_days = days
                except ValueError:
                    pass
        # Only publish a fleet wall once we have enough QUALITY
        # datapoints to mean something. Until then the dashboard sees
        # ``observed_cap_gb_count`` so it can show 'wall=? (gathering)'.
        cap_min = None
        cap_med = None
        if caps and len(caps) >= FLEET_WALL_MIN_QUALITY_OBS:
            cap_min = min(caps)
            scaps = sorted(caps)
            cap_med = scaps[len(scaps) // 2]
        return {
            'peers_enabled': n_total,
            'peers_parked': n_parked,
            'used_gb': round(used, 2),
            'budget_gb_nominal': round(budget, 2),
            'observed_cap_gb_count': n_with_cap,
            'observed_cap_gb_min': round(cap_min, 2) if cap_min is not None else None,
            'observed_cap_gb_median': round(cap_med, 2) if cap_med is not None else None,
            'next_renew_in_days': soonest_days,
        }

    def _v2_status(self) -> dict:
        """v2 rollout counters (upgraded/fresh/verify_fail/store/kg_log).
        Primary-local data — on a non-primary director the store is
        empty and only the manifest-derived counters are meaningful."""
        try:
            import v2_ingest
            out = dict(v2_ingest.status())
        except Exception as e:  # noqa: BLE001
            out = {'error': str(e)}
        try:
            cc = self.state.get('_v2_cand_cache') or {}
            out['dispatch'] = {
                'assigned': list(self.state.get('v2_upgrade_assigned') or []),
                'priority': self._load_v2_priority(),
                'candidates_total': cc.get('total', 0),
                'fast_total': cc.get('fast_total', 0),
                'ne_wait': cc.get('ne_wait', 0), 'ne_built': cc.get('ne_built', 0),
                'fill_below_ready': V2_UPGRADE_FILL_BELOW_READY,
                'v24_first': self._v24_first(None),
                'max_peers': int(knob(None, 'max_v2_upgrade_peers', MAX_V2_UPGRADE_PEERS)),
                'v24_fast': dict(self.state.get('v24_fast_gov') or {},
                                 max=int(knob(None, 'v24_fast_max_peers', V24_FAST_MAX_PEERS))),
            }
            _fs = v2_fleet_strikes()
            out['dispatch']['fleet_strikes'] = {
                'codes': len(_fs),
                'struck_out': len(v2_fleet_struck_out()),
                'peers': len({p for pe in _fs.values() if isinstance(pe, dict) for p in pe}),
            }
            out['regen'] = v2_regen_summary()
        except Exception:
            pass
        try:
            import label_mix as _lm
            out['label_mix'] = _lm.summary()
        except Exception:
            pass
        try:
            import ne_rollout as _ner
            out['ne_rollout'] = _ner.summary()
        except Exception as e:  # noqa: BLE001
            out['ne_rollout'] = {'error': str(e)}
        try:
            import ne_ingest as _nei
            out['ne_ingest'] = _nei.status()
        except Exception as e:  # noqa: BLE001
            out['ne_ingest'] = {'error': str(e)}
        return out

    def _fleet_proxy_summary_with_history(self, peers_status: list[dict]) -> dict:
        """Compute the current fleet_proxy summary AND maintain the
        in-memory + on-disk history ring.

        Only the gunicorn worker holding the director lock appends to
        the ring (mirrors the capacity_history pattern); the other
        worker reads the ring from disk via load_director_state
        through state['fleet_proxy_history']. Sample cadence is
        ~10 min (PROXY_HISTORY_SAMPLE_S), tracked via
        self._proxy_history_last_ts. Persistence happens implicitly:
        save_director_state copies state['fleet_proxy_history'] when
        the director loop saves on each tick (we set it below).
        """
        import time as _t
        summ = self._fleet_proxy_summary(peers_status)
        # Append to history only on the director-loop worker (lock held)
        # and at most once per ~10 min. get_status is called more often
        # than that (per-request via dashboards / proxy/* endpoints).
        if getattr(self, '_lock_fd', None) is not None:
            now = _t.time()
            PROXY_HISTORY_SAMPLE_S = 600  # 10 min
            if (now - getattr(self, '_proxy_history_last_ts', 0.0)
                    >= PROXY_HISTORY_SAMPLE_S) and summ.get('peers_reporting'):
                self._proxy_history_last_ts = now
                entry = {
                    't': int(now),
                    'n': int(summ.get('peers_reporting') or 0),
                    'tot': int(summ.get('proxies_total') or 0),
                    'mn': int(summ.get('proxies_min') or 0),
                    'md': int(summ.get('proxies_median') or 0),
                    'mx': int(summ.get('proxies_max') or 0),
                    'no': int(summ.get('peers_no_proxies') or 0),
                    'st': int(summ.get('peers_stale') or 0),
                }
                self._proxy_history.append(entry)
                # Mirror into state so save_director_state persists it
                # alongside the other history rings. Cross-worker
                # visibility relies on this snapshot — the other
                # gunicorn worker re-reads director_state.json and
                # picks up 'fleet_proxy_history'.
                try:
                    self.state['fleet_proxy_history'] = list(self._proxy_history)
                except Exception:
                    pass
        return summ

    @staticmethod
    def _fleet_proxy_summary(peers_status: list[dict]) -> dict:
        """Aggregate BEV-proxy-pool state across the fleet.

        Input is the ``peers_status`` list built in ``_compute_status``.
        Each row may carry a ``proxy_pool`` dict pushed by the peer
        (bev_proxy.summary()) — small fixed-size dict, ~200 B/peer.
        Output is rendered as a single ``fleet_proxy:`` line in
        /process.txt and surfaced under ``fleet_proxy`` in
        ``/api/v1/director/status`` for structured consumers.

        Only counts peers whose processor is currently running (proxy
        pool only matters for peers actively reading BEV); stopped /
        idle peers carry stale state we don't want skewing min/median.
        """
        import time as _t
        now = _t.time()
        pools: list[dict] = []
        peers_alive = 0
        peers_stale = 0
        STALE_REFRESH_S = 3 * 1800  # 3× REFRESH_INTERVAL = 90 min
        for p in peers_status:
            if p.get('processor_state') not in ('running', 'processing'):
                continue
            pp = p.get('proxy_pool')
            if not isinstance(pp, dict):
                continue
            peers_alive += 1
            lr = pp.get('last_refresh') or {}
            lr_ts = float(lr.get('ts') or 0.0)
            if lr_ts > 0 and (now - lr_ts) > STALE_REFRESH_S:
                peers_stale += 1
            pools.append(pp)
        if not pools:
            return {
                'peers_reporting': 0,
                'peers_stale': 0,
            }
        healthy = [int(p.get('healthy') or 0) for p in pools]
        proxies = [int(p.get('proxies') or 0) for p in pools]
        cooling = [int(p.get('cooling_down') or 0) for p in pools]
        phase2 = [int((p.get('last_refresh') or {}).get('phase2') or 0)
                  for p in pools]
        ages_min = []
        for p in pools:
            lr = p.get('last_refresh') or {}
            ts = float(lr.get('ts') or 0.0)
            if ts > 0:
                ages_min.append((now - ts) / 60.0)
        def _med(xs: list) -> float | None:
            if not xs:
                return None
            s = sorted(xs)
            return s[len(s) // 2]
        # Count peers running with zero proxies in pool (direct slots
        # only) — acceptable when BEV direct egress is healthy, but
        # worth surfacing.
        peers_no_proxies = sum(1 for n in proxies if n == 0)
        return {
            'peers_reporting': peers_alive,
            'peers_stale': peers_stale,
            'peers_no_proxies': peers_no_proxies,
            'proxies_total': sum(proxies),
            'proxies_min': min(proxies),
            'proxies_median': _med(proxies),
            'proxies_max': max(proxies),
            'healthy_total': sum(healthy),
            'cooling_total': sum(cooling),
            'last_phase2_median': _med(phase2),
            'last_refresh_age_min_median': (
                round(_med(ages_min), 1) if ages_min else None),
            'last_refresh_age_min_max': (
                round(max(ages_min), 1) if ages_min else None),
        }

    # --- Fleet-dormant hold mode -------------------------------------
    def _update_fleet_dormancy(self, statuses: dict) -> bool:
        """Track whole-fleet reachability; enter/exit dormant hold.

        Returns True while the fleet is dormant (every remote enabled
        peer unreachable for >= FLEET_DORMANT_AFTER_S). While dormant
        the director loop skips all orchestration and only runs the
        primary-park enforcer + a light probe sweep
        (``_fleet_dormant_probe``). Wake is instant on the first
        reachable peer — either via this tick's status snapshot (peer
        push heartbeats land here) or via a successful probe.

        Rationale: when exe.dev powers off the whole peer fleet, the
        director would otherwise churn forever — activation attempts
        against dead URLs, shadow-election warnings, stale-update
        retries, OIDC probes — filling logs with noise and wasting
        cycles on the dashboard host. Holding is the correct state;
        the interactive app on / keeps running untouched.
        """
        now = time.time()
        remote_ids = {p['id'] for p in self.cfg.get('peers', [])
                      if p.get('url') and p.get('enabled', True)}
        if not remote_ids:
            return False
        reachable = [pid for pid, ps in (statuses or {}).items()
                     if pid in remote_ids and isinstance(ps, dict)
                     and ps.get('state') != 'unreachable']
        dorm = self.state.get('fleet_dormant') or {}
        if reachable:
            with self._lock:
                self.state['fleet_last_reachable_ts'] = now
            if dorm.get('active'):
                self._wake_from_dormant(sorted(reachable)[:5])
            return False
        if dorm.get('active'):
            return True
        last = float(self.state.get('fleet_last_reachable_ts') or 0.0)
        if not last:
            # First observation — start the clock, don't go dormant yet.
            with self._lock:
                self.state['fleet_last_reachable_ts'] = now
            return False
        if (now - last) < FLEET_DORMANT_AFTER_S:
            return False
        since = datetime.now(timezone.utc).isoformat(timespec='seconds')
        with self._lock:
            self.state['fleet_dormant'] = {
                'active': True, 'since': since,
                'remote_peers': len(remote_ids),
            }
        log.warning('FLEET DORMANT: all %d remote peers unreachable for '
                    '%.0f min — director on hold, probing every %d min. '
                    'Wakes automatically when any peer returns.',
                    len(remote_ids), (now - last) / 60,
                    FLEET_DORMANT_PROBE_INTERVAL_S // 60)
        _emit_director_event(
            'fleet dormant: all %d remote peers unreachable for %.0fm — '
            'director on hold (ping mode, every %dm); wakes automatically '
            'when any peer returns' % (
                len(remote_ids), (now - last) / 60,
                FLEET_DORMANT_PROBE_INTERVAL_S // 60),
            level='warning')
        return True

    def _wake_from_dormant(self, woken_by: list[str]) -> None:
        """Exit dormant hold: clear breakers so the next tick fans out
        with fresh eyes, stamp wake metadata, log loudly."""
        dorm = self.state.get('fleet_dormant') or {}
        if not dorm.get('active'):
            return
        now_iso = datetime.now(timezone.utc).isoformat(timespec='seconds')
        with self._lock:
            self.state['fleet_dormant'] = {
                'active': False, 'since': dorm.get('since'),
                'woke_at': now_iso, 'woken_by': list(woken_by),
            }
            self.state['fleet_last_reachable_ts'] = time.time()
        # Reset all circuit breakers + bandwidth backoffs so the resumed
        # orchestration isn't blinded for up to 30 min by stale opens.
        with _PEER_CB_LOCK:
            _PEER_CB.clear()
        with self._lock:
            self.state['_bandwidth_backoff'] = {}
            self.state['_bandwidth_misses'] = {}
        log.warning('FLEET WAKE: peer(s) %s reachable again (dormant since '
                    '%s) — resuming orchestration',
                    ','.join(woken_by), dorm.get('since'))
        _emit_director_event(
            'fleet wake: peer(s) %s reachable again (dormant since %s) — '
            'resuming orchestration' % (','.join(woken_by),
                                        dorm.get('since')),
            level='warning')


    # --- Run-complete hold mode (whole run finished) -------------------
    def _run_complete_checks(self, statuses: dict | None) -> dict:
        """Is there any work left for the fleet?  Cheap, local-file based.
        Returns ``{"ok": bool, "open": {check: detail…}, "at": iso}`` —
        ``ok`` only when every check is clear."""
        open_: dict = {}
        try:
            self._frontier_cells()
            tot = getattr(self, '_frontier_pending_total', None)
            if tot is None:
                open_['kg_scan'] = 'no completion evidence'
            elif tot > 0:
                open_['kg_pending'] = tot
        except Exception as e:  # noqa: BLE001
            open_['kg_scan'] = str(e)[:80]
        try:
            rq = DATA_DIR / 'retry_queue.json'
            codes = json.loads(rq.read_text()) if rq.exists() else []
            if codes:
                open_['retry_queue'] = len(codes)
        except Exception:
            pass
        try:
            busy = [pid for pid, ps in (statuses or {}).items()
                    if isinstance(ps, dict) and ((ps.get('current_kg') or {}).get('code'))
                    and str(ps.get('state') or ps.get('processor_state') or '') in
                    ('running', 'processing', 'paused', 'pausing', 'paused_bw')]
            if busy:
                open_['peers_in_kg'] = sorted(busy)[:6]
        except Exception:
            pass
        try:
            import sliver_sweep as _sls
            if _sls.enabled():
                sw = _sls.status()
                by = sw.get('by_state') or {}
                n = sum(int(by.get(k) or 0) for k in ('pending', 'dispatched', 'received'))
                n += int(sw.get('units_open') or 0) + int(sw.get('hold_units') or 0) \
                    + int(sw.get('scan_backlog') or 0)
                if n:
                    open_['sweep'] = {k: by.get(k, 0) for k in ('pending', 'dispatched', 'received')} | {
                        'units_open': sw.get('units_open'), 'hold': sw.get('hold_units'),
                        'scan_backlog': sw.get('scan_backlog')}
        except Exception as e:  # noqa: BLE001
            open_['sweep'] = str(e)[:80]
        try:
            import ne_viewfill as _nvf
            lr = (_nvf.status().get('last_run') or {})
            if lr.get('budget_stop'):
                left = max(0, int(lr.get('candidates') or 0) - int(lr.get('walked') or 0)
                           - int(lr.get('skipped') or 0))
                if left:
                    open_['viewfill'] = left
        except Exception:
            pass
        try:
            dsp = ((self.state.get('v2') or {}).get('dispatch') or {})
            n = int(dsp.get('candidates_total') or 0) + int(dsp.get('fast_total') or 0) \
                + len(dsp.get('assigned') or [])
            if n:
                open_['v2_dispatch'] = n
        except Exception:
            pass
        try:
            import product_repair as _pr
            fx = int((_pr.summary(rows=0) or {}).get('fixable') or 0)
            if fx:
                open_['repair_fixable'] = fx
        except Exception:
            pass
        return {'ok': not open_, 'open': open_,
                'at': datetime.now(timezone.utc).isoformat(timespec='seconds')}

    def _update_run_complete(self, statuses: dict | None) -> bool:
        """Enter / keep / leave the run-complete hold.  Returns True while
        the director should skip orchestration (nothing left to do)."""
        now = time.time()
        rc = dict(self.state.get('run_complete') or {})
        mode = read_director_mode(self.state.get('mode', 'auto'))
        if mode != 'auto':
            return False
        if rc.get('active'):
            if now - float(rc.get('_checked_ts') or 0) < RUN_COMPLETE_RECHECK_S:
                return True
            chk = self._run_complete_checks(statuses)
            rc['_checked_ts'] = now
            rc['last_check'] = chk
            if chk['ok']:
                with self._lock:
                    self.state['run_complete'] = rc
                return True
            self._wake_from_run_complete(chk['open'])
            return False
        # not active: evaluate at the recheck cadence (the scan is cached
        # 5 min anyway) and require the condition to hold continuously.
        if now - float(rc.get('_checked_ts') or 0) < min(60, RUN_COMPLETE_RECHECK_S):
            return False
        chk = self._run_complete_checks(statuses)
        rc['_checked_ts'] = now
        rc['last_check'] = chk
        if not chk['ok']:
            rc.pop('clear_since_ts', None)
            with self._lock:
                self.state['run_complete'] = rc
            return False
        first = float(rc.get('clear_since_ts') or 0) or now
        rc['clear_since_ts'] = first
        if now - first < RUN_COMPLETE_AFTER_S:
            with self._lock:
                self.state['run_complete'] = rc
            return False
        since = datetime.now(timezone.utc).isoformat(timespec='seconds')
        rc.update(active=True, since=since, entries=int(rc.get('entries') or 0) + 1)
        rc.pop('woke_at', None); rc.pop('woken_by', None)
        with self._lock:
            self.state['run_complete'] = rc
            self.state['parallel_frontiers_active'] = []
        log.warning('RUN COMPLETE: no pending KG / queue / sweep / viewfill / v2 / '
                    'repair work and no peer inside a KG for %d min — director on '
                    'hold (heartbeats only, re-check every %d min). Wakes on any new work.',
                    RUN_COMPLETE_AFTER_S // 60, RUN_COMPLETE_RECHECK_S // 60)
        _emit_director_event(
            'run complete: nothing left to orchestrate (KGs, queue, sweep, viewfill, '
            'v2 dispatch, repairs all clear for %dm) — director on hold, peer fanout '
            'suspended; re-checks local state every %dm and wakes on new work'
            % (RUN_COMPLETE_AFTER_S // 60, RUN_COMPLETE_RECHECK_S // 60), level='warning')
        try:
            self._stop_idle_processors_for_hold(statuses)
        except Exception:
            log.exception('run-complete: stopping idle processors failed')
        return True

    def _stop_idle_processors_for_hold(self, statuses: dict | None) -> None:
        """Ask processors that are running but idle (no KG) to stop — a
        processor with an empty queue exits by itself, this only shortens
        the tail.  Never touches a peer inside a KG (the hold requires none)."""
        n = 0
        for p in self.cfg.get('peers', []):
            ps = (statuses or {}).get(p['id']) or {}
            if not p.get('url') or not isinstance(ps, dict):
                continue
            if str(ps.get('state') or '') not in ('running', 'processing'):
                continue
            if (ps.get('current_kg') or {}).get('code'):
                continue
            try:
                stop_peer_processor(p['url'])
                n += 1
            except Exception as e:  # noqa: BLE001
                log.debug('run-complete stop %s: %s', p['id'], e)
        if n:
            log.info('run-complete: asked %d idle processor(s) to stop', n)

    def _wake_from_run_complete(self, open_: dict) -> None:
        rc = dict(self.state.get('run_complete') or {})
        if not rc.get('active'):
            return
        now_iso = datetime.now(timezone.utc).isoformat(timespec='seconds')
        rc.update(active=False, woke_at=now_iso, woken_by=open_)
        rc.pop('clear_since_ts', None)
        with self._lock:
            self.state['run_complete'] = rc
        with _PEER_CB_LOCK:
            _PEER_CB.clear()
        with self._lock:
            self.state['_bandwidth_backoff'] = {}
            self.state['_bandwidth_misses'] = {}
        log.warning('RUN-COMPLETE WAKE: new work %s (on hold since %s) — resuming orchestration',
                    json.dumps(open_, default=str)[:200], rc.get('since'))
        _emit_director_event(
            'run complete → wake: new work %s (hold since %s) — resuming orchestration'
            % (json.dumps(open_, default=str)[:160], rc.get('since')), level='warning')

    def _fleet_dormant_probe(self) -> None:
        """Light reachability sweep while dormant. Every
        FLEET_DORMANT_PROBE_INTERVAL_S, probe up to 5 peers round-robin
        (bypassing circuit breakers) with short timeouts. Any success
        triggers an immediate wake. Peer push heartbeats provide the
        fast path anyway (a rebooting peer pushes to the director within
        ~30 s); this sweep is the belt-and-braces for peers whose push
        loop is broken or misconfigured.
        """
        now = time.time()
        last = float(self.state.get('_dormant_probe_ts') or 0.0)
        if (now - last) < FLEET_DORMANT_PROBE_INTERVAL_S:
            return
        with self._lock:
            self.state['_dormant_probe_ts'] = now
        remote = [p for p in self.cfg.get('peers', [])
                  if p.get('url') and p.get('enabled', True)]
        if not remote:
            return
        idx = int(self.state.get('_dormant_probe_idx') or 0)
        n = min(5, len(remote))
        woke: list[str] = []
        for i in range(n):
            p = remote[(idx + i) % len(remote)]
            try:
                r = requests.get(
                    p['url'].rstrip('/') + '/api/v1/processing/status',
                    timeout=(3, 8))
                if r.ok:
                    woke.append(p['id'])
                    _cb_record_success(p['id'])
            except Exception:
                pass
        with self._lock:
            self.state['_dormant_probe_idx'] = (idx + n) % len(remote)
        if woke:
            self._wake_from_dormant(woke)

    def _enforce_primary_park(self) -> None:
        """Belt-and-braces: keep the primary peer parked.

        The primary VM is the public-facing dashboard host (DNS, search
        index, share storage, Zenodo lock broker). It must NEVER carry
        frontier or cache-only processing load — even briefly during a
        rotation — because that would steal CPU/IO from request
        handling. Operationally we keep it pinned ``idle`` and
        ``not_before`` far in the future.

        This enforcer runs every director tick and:
          * sets ``pinned_role='idle'`` if missing
          * extends ``not_before`` to >= 30 days from now if shorter or
            absent
          * if the primary somehow ended up the active_peer (race),
            demotes it.

        Cheap: O(1), no I/O unless cfg actually changed.  No-op in
        single-VM mode (``config.peers()`` empty): the one instance IS the
        processor.
        """
        if not config.peers():
            return
        from datetime import datetime as _dt, timezone as _tz
        with self._lock:
            cfg = self.cfg
            primary = None
            for p in cfg.get('peers', []):
                if p.get('id') == 'primary':
                    primary = p
                    break
            if primary is None:
                return
            changed = False
            cur_role = (primary.get('pinned_role') or '').strip().lower()
            if cur_role not in ('idle', 'off', 'pause', 'paused', 'parked'):
                primary['pinned_role'] = 'idle'
                changed = True
            # Primary is intentionally pinned far into the future:
            # the public dashboard host must never carry processing
            # load. Floor at 2027-01-01 (well past any conceivable
            # exe.dev billing cycle) and extend if we ever drift
            # below.
            target = _dt(2027, 1, 1, tzinfo=_tz.utc)
            nb = primary.get('not_before')
            extend = False
            if not nb:
                extend = True
            else:
                try:
                    cur = _dt.fromisoformat(nb)
                    if cur.tzinfo is None:
                        cur = cur.replace(tzinfo=_tz.utc)
                    if cur < target:
                        extend = True
                except (TypeError, ValueError):
                    extend = True
            if extend:
                primary['not_before'] = target.isoformat()
                changed = True
            if self.state.get('active_peer') == 'primary':
                log.warning('Primary somehow set as active_peer — demoting')
                self.state['active_peer'] = None
        if changed:
            try:
                save_peers_config(self.cfg)
                log.info('Primary park enforced: pinned_role=%s not_before=%s',
                         primary.get('pinned_role'), primary.get('not_before'))
            except Exception:
                log.exception('Primary park enforce: save_peers_config failed')

    def _release_unverified_bw_parks(self) -> None:
        """Periodic cleanup: clear not_before from peers we parked on
        a budget guess (no observed_cap_gb) so they rejoin the rotation.

        Runs at most once per hour (state-throttled). Was one-shot,
        but the nominal-budget gate in _check_and_switch kept minting
        new unverified park-until-renewal stamps after the flag was
        set (Jul 2026: 19 peers stranded for weeks). That gate is now
        evidence-only too, so this sweep is belt-and-braces.
        Only releases parks tagged by ``_park_peer_until_renewal``
        (event=='park_until_renewal' in canary_notes) where the peer
        never earned an ``observed_cap_gb``. Hand-set ``not_before``
        values (e.g. primary 2027-01-01, manual cooldowns) are
        preserved by looking for the matching note.
        """
        from datetime import datetime as _dt, timezone as _tz
        with self._lock:
            last = self.state.get('_unverified_bw_parks_released')
            # Legacy value was boolean True (one-shot); treat as "long
            # ago" so upgraded directors sweep immediately.
            if isinstance(last, (int, float)) and \
                    (time.time() - last) < 3600:
                return
            cfg = self.cfg
            changed = False
            released: list[str] = []
            for p in cfg.get('peers', []):
                pid = p.get('id')
                if pid in ('primary',):
                    continue  # primary is enforced separately
                if not p.get('enabled', True):
                    continue
                if p.get('observed_cap_gb') is not None:
                    continue  # genuine cap evidence; keep park
                nb = p.get('not_before')
                if not nb:
                    continue
                # Did *we* set this via park-until-renewal recently?
                notes = p.get('canary_notes') or []
                ours = False
                for n in reversed(notes[-8:]):
                    if not isinstance(n, dict):
                        continue
                    if n.get('event') == 'park_until_renewal' and \
                            n.get('cooldown_until') == nb:
                        ours = True
                        break
                if not ours:
                    continue
                p.pop('not_before', None)
                notes.append({
                    'at': _dt.now(_tz.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'event': 'park_released',
                    'reason': 'no observed_cap_gb evidence; '
                              'canary-by-default policy',
                })
                if len(notes) > 32:
                    del notes[: len(notes) - 32]
                changed = True
                released.append(pid)
            if changed:
                try:
                    save_peers_config(self.cfg)
                except Exception:
                    log.exception('release_unverified_bw_parks: save failed')
            self.state['_unverified_bw_parks_released'] = time.time()
        if released:
            log.warning('Released unverified BW parks on %d peers '
                        '(no observed_cap_gb evidence): %s',
                        len(released), ','.join(released))
            try:
                _emit_director_event(
                    f'released {len(released)} unverified BW parks '
                    f'(canary-by-default): {",".join(released)}',
                    peer='director', level='warning')
            except Exception:
                pass

    # Short rolling cooldown for role-based parks (director, shadow).
    # Refreshed every director tick while the role is held, so the
    # peer naturally rejoins rotation within this window after a
    # handover. Long enough to survive HA flapping and the next tick.
    _ROLE_PARK_REFRESH_S = 2 * 3600

    def _enforce_director_self_park(self) -> None:
        """Keep current director + shadow out of the processing pool.

        Why park them:
          * Director pays a recurring fleet-fanout BW cost (status
            polls, log merge, credential probes). Stacking frontier
            or cache-only work on top is exactly how a director
            burns through bandwidth faster than peers.
          * Shadow must be ready to take over instantly; an in-flight
            KG would have to be aborted on promotion.

        Implementation: a short ROLLING cooldown (2 h, refreshed every
        tick) instead of a long +30 d stamp. As soon as a peer stops
        being director / shadow, its ``not_before`` naturally expires
        within 2 h with NO explicit release — fixes the bug where
        ex-directors (at55, at52, at66) remained parked for 30 d
        because the self-park ran only when the peer was still
        director.

        Primary is exempt (handled by ``_enforce_primary_park``
        with the 2027 floor).
        """
        from datetime import datetime as _dt, timedelta as _td, timezone as _tz
        if not config.is_director() or not config.peers():
            return
        me = config.self_id()
        shadow = (self.state.get('shadow_peer') or '').strip()
        target = _dt.now(_tz.utc) + _td(seconds=self._ROLE_PARK_REFRESH_S)
        targets = {}
        if me != 'primary':
            targets[me] = 'director'
        if shadow and shadow != 'primary' and shadow != me:
            # Small-fleet exception (2026-08-22): with only a handful
            # of workers, parking the shadow costs a full processing
            # slot for a role that is pure standby. Let it process;
            # the snapshot push is cheap and a promotion just aborts
            # its in-flight KG (checkpoints make that loss small).
            n_workers = sum(1 for p in (self.cfg.get('peers') or [])
                            if p.get('enabled', True)
                            and p.get('id') != 'primary')
            if n_workers >= SHADOW_PARK_MIN_FLEET:
                targets[shadow] = 'shadow'
        # Note: empty targets is fine and falls through to the cleanup
        # loop below — we still want to release ex-directors / ex-shadows
        # whose pinned_role was set by a previous tick but never cleared.
        with self._lock:
            cfg = self.cfg
            changed = False
            # First pass: clear stale role-parks on peers that are no
            # longer director/shadow. Bug fix: previously this method
            # only stamped roles; pinned_role='idle' accumulated forever
            # on every ex-shadow, leaving them permanently excluded
            # from rotation even after not_before rolled off.
            for p in cfg.get('peers', []):
                pid = p.get('id')
                if pid in targets or pid == 'primary':
                    continue
                notes = p.get('canary_notes') or []
                last_rp = next((n for n in reversed(notes[-12:])
                                if isinstance(n, dict)
                                and n.get('event') == 'role_park'), None)
                if not last_rp:
                    continue  # role wasn't set by us — leave it alone
                # Was this peer parked by us, but is no longer in the
                # active director/shadow set? Clear our stamp.
                cur_role = (p.get('pinned_role') or '').strip().lower()
                if cur_role != 'idle':
                    continue
                p['pinned_role'] = None
                # Only drop not_before if it matches a role_park stamp
                # (don't shrink canary/manual cooldowns).
                nb = p.get('not_before')
                if nb and last_rp.get('cooldown_until') == nb:
                    p.pop('not_before', None)
                notes.append({
                    'at': _dt.now(_tz.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'event': 'park_released',
                    'reason': 'role_park auto-clear: peer no longer '
                              f'{last_rp.get("role", "director/shadow")}',
                })
                if len(notes) > 32:
                    del notes[: len(notes) - 32]
                p['canary_notes'] = notes
                changed = True
            if not targets:
                # No-op fast path — the outer save block below handles
                # persistence if the cleanup pass made any changes.
                pass
            for p in cfg.get('peers', []):
                pid = p.get('id')
                if pid not in targets:
                    continue
                role_tag = targets[pid]
                cur_role = (p.get('pinned_role') or '').strip().lower()
                if cur_role not in ('idle', 'off', 'pause', 'paused', 'parked'):
                    p['pinned_role'] = 'idle'
                    changed = True
                nb = p.get('not_before')
                extend = False
                if not nb:
                    extend = True
                else:
                    try:
                        cur = _dt.fromisoformat(nb)
                        if cur.tzinfo is None:
                            cur = cur.replace(tzinfo=_tz.utc)
                        # Don't shrink a longer (canary / manual) park.
                        # Only refresh if our rolling target is later.
                        if cur < target:
                            extend = True
                    except (TypeError, ValueError):
                        extend = True
                if extend:
                    p['not_before'] = target.isoformat()
                    notes = p.setdefault('canary_notes', [])
                    notes.append({
                        'at': _dt.now(_tz.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'event': 'role_park',
                        'role': role_tag,
                        'cooldown_until': p['not_before'],
                    })
                    if len(notes) > 32:
                        del notes[: len(notes) - 32]
                    changed = True
            if self.state.get('active_peer') in targets:
                log.warning(
                    'Role-parked peer %s set as active_peer — demoting',
                    self.state.get('active_peer'),
                )
                self.state['active_peer'] = None
        if changed:
            try:
                save_peers_config(self.cfg)
            except Exception:
                log.exception('role-park: save_peers_config failed')
        # Stop any local processor running on the director itself.
        if me in targets:
            try:
                r = requests.get(
                    config.APP_URL + '/api/v1/processing/status',
                    timeout=5,
                )
                running = bool(r.ok and (r.json() or {}).get('running'))
            except Exception:
                running = False
            if running:
                log.warning(
                    'Director %s has local processor running — issuing '
                    'graceful stop', me,
                )
                try:
                    stop_peer_processor(None, graceful=True)
                    _emit_director_event(
                        f'director self-park: {me} stopping local '
                        'processor (director must not carry load)',
                        peer=me, level='warning',
                    )
                except Exception:
                    log.exception('director self-park: local stop failed')

    def _park_peer_until_renewal(self, peer: dict, next_renew,
                                 *, remaining_gb: float,
                                 mid_kg: bool, hard: bool) -> None:
        """Park a peer until its next billing-cycle renewal.

        ``hard``: depleted, rotate now — verified hard stop.
        ``mid_kg`` (and not hard): low-water graceful — ask the peer
        to finish the current KG then exit; park record is written
        immediately so the director's idle/stopped branch demotes
        cleanly when the KG completes.
        ``mid_kg=False`` and not hard: idle near low-water — park now
        (no work to finish).
        """
        from datetime import datetime as _dt, timezone as _tz
        pid = peer['id']
        peer_url = peer.get('url')
        # 1) write not_before + audit note under the cfg lock.
        with self._lock:
            live = get_peer_by_id(self.cfg, pid)
            if not live:
                return
            nb = next_renew.isoformat()
            existing = live.get('not_before')
            # Don't shrink an existing later cooldown.
            try:
                if existing:
                    cur = _dt.fromisoformat(existing)
                    if cur.tzinfo is None:
                        cur = cur.replace(tzinfo=_tz.utc)
                    if cur >= next_renew:
                        nb = existing
            except (TypeError, ValueError):
                pass
            live['not_before'] = nb
            notes = live.setdefault('canary_notes', [])
            notes.append({
                'at': _dt.now(_tz.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'event': 'park_until_renewal',
                'remaining_gb': round(float(remaining_gb), 2),
                'mid_kg': bool(mid_kg),
                'hard': bool(hard),
                'cooldown_until': nb,
            })
            if len(notes) > 32:
                del notes[: len(notes) - 32]
        try:
            save_peers_config(self.cfg)
        except Exception:
            log.exception('park_peer_until_renewal: save_peers_config failed')

        tag = 'depleted' if hard else ('low-water graceful' if mid_kg else 'low-water park')
        log.warning('Peer %s %s (%.2f GB remaining) — not_before=%s%s',
                    pid, tag, remaining_gb, next_renew.isoformat(),
                    ' (mid-KG; sending graceful stop)' if (mid_kg and not hard) else '')
        try:
            _emit_director_event(
                f'park-until-renewal {pid}: {tag}, '
                f'{remaining_gb:.2f} GB remaining, resumes {next_renew.isoformat()}',
                peer=pid, level='warning')
        except Exception:
            pass

        # 2) issue stop. Hard stop = verified; graceful = fire-and-keep-running.
        if peer_url is None:
            return
        try:
            if hard:
                res = safely_stop_peer(peer_url, pid)
                if 'error' in res:
                    log.error('park_peer_until_renewal %s: hard stop failed: %s',
                              pid, res['error'])
            else:
                stop_peer_processor(peer_url, graceful=True)
        except Exception:
            log.exception('park_peer_until_renewal %s: stop failed', pid)

    def _enforce_peer_bandwidth_walls(self) -> None:
        """Park-until-renewal for ANY peer that has hit *its own* wall.

        Canary-by-default philosophy: we do NOT know exe.dev's real
        per-account limits, billing-cycle anchors, or whether 200 GB
        is even the right number. Every peer probes its individual
        ceiling by running until throughput actually collapses. The
        canary slowdown detector then stamps ``observed_cap_gb`` on
        the peer — that's the *only* evidence-based wall.

        This sweep extends ``_park_peer_until_renewal`` to non-active
        peers, but ONLY when we have measured evidence:

          * ``observed_cap_gb`` set AND peer's used_gb ≥ cap → the
            peer has revisited its known wall → park.
          * remaining < HARD_DEPLETED_GB AND cap evidence → hard
            stop (depleted by its own observation).

        Peers without an ``observed_cap_gb`` are explicitly NOT
        parked, no matter how far past the 95 GB nominal budget they
        run. The 95 GB number is a guess; the canary slowdown gate
        is the real authority. This avoids stranding the fleet on
        an incorrect billing-cycle assumption.
        """
        with self._lock:
            cfg = self.cfg.copy()
            state_copy = self.state.copy()
        active_id = state_copy.get('active_peer')
        bw_map = state_copy.get('peer_bandwidth') or {}
        for p in list(cfg.get('peers', [])):
            if not p.get('enabled', True):
                continue
            pid = p['id']
            if pid == active_id:
                continue  # handled by _check_and_switch
            if _peer_is_scheduled(p):
                continue  # already parked
            bw = bw_map.get(pid) or {}
            used = bw.get('used_bytes')
            if used is None:
                continue
            used_gb = used / (1024 ** 3)
            # Evidence requirement: peer must have a quality
            # observed_cap_gb (from a network-grade slowdown) AND
            # its current used must be at/past that cap.
            cap = p.get('observed_cap_gb')
            if not isinstance(cap, (int, float)):
                continue
            if used_gb < float(cap):
                continue
            # Compute remaining vs *cap* (the observed wall), not the
            # nominal 95 GB budget.
            remaining = float(cap) - used_gb  # ≤ 0 by gate above
            # Determine mid-KG via cached peer status (no extra HTTP).
            proc_state_now = 'unknown'
            url = p.get('url')
            if url is None:
                # Local peer; read progress directly via get_peer_status.
                try:
                    proc_state_now = get_peer_status(None, pid).get(
                        'state', 'unknown')
                except Exception:
                    proc_state_now = 'unknown'
            else:
                cached = _PEER_STATUS_CACHE.get(url)
                if cached and (time.time() - cached[0]) < _PEER_STATUS_CACHE_TTL:
                    proc_state_now = cached[1].get('state', 'unknown')
                else:
                    pushed = get_pushed_status(pid)
                    if pushed is not None:
                        proc_state_now = (pushed.get('status') or {}
                                          ).get('state', 'unknown')
            mid_kg = proc_state_now in ('running', 'processing')
            hard = remaining < BANDWIDTH_HARD_DEPLETED_GB
            try:
                next_renew = _peer_next_renew(p, cfg)
            except Exception:
                log.exception('peer-bw-wall %s: next_renew failed', pid)
                continue
            try:
                self._park_peer_until_renewal(
                    p, next_renew, remaining_gb=remaining,
                    mid_kg=mid_kg, hard=hard,
                )
            except Exception:
                log.exception('peer-bw-wall %s: park failed', pid)

    def _check_canary_health(self) -> None:
        """Park peers whose throughput collapsed (whole fleet) or whose
        warning noise spiked (canary-override peers only).

        Slowdown park: applies to every enabled peer — the canonical
        signal for exe.dev shaping. We persist ``observed_cap_gb`` on
        the peer the moment we trip, so we learn each peer's wall.

        Noise park: applies only to peers with an explicit
        ``budget_gb`` override (intentional canaries). Generic peers
        get a noisy score for transient upstream issues that the
        load-shifter already handles — yanking them out of rotation
        for 6 h would be too aggressive.

        Sets ``not_before`` so the director's normal scheduling logic
        stops sending the peer work; existing graceful-stop /
        not_before paths in ``_check_and_switch`` handle the cleanup.
        """
        with self._lock:
            cfg = self.cfg
            state = self.state
            peers = list(cfg.get('peers', []))
            # Streaks: peer_id -> {since: epoch, last_ratio: float}
            streaks = state.setdefault('canary_slowdown_streaks', {})
        from datetime import datetime as _dt, timedelta as _td, timezone as _tz
        cfg_changed = False
        now_ts = time.time()

        # First pass: classify each peer's current state without acting,
        # update streaks, and tally fleet-wide concurrent slowdown so
        # the second pass can suppress quality flags during fleet-wide
        # upstream events (Zenodo / BEV outages).
        per_peer: list[dict] = []
        n_with_canary = 0
        n_in_slowdown = 0
        for p in peers:
            if not p.get('enabled', True):
                continue
            pid = p['id']
            base = self._canary_throughput(pid, CANARY_BASELINE_WINDOW_S)
            recent = self._canary_throughput(pid, CANARY_RECENT_WINDOW_S)
            usable = (base and recent
                      and base['samples'] >= CANARY_BASELINE_MIN_SAMPLES
                      and base['delta_bytes'] >= CANARY_MIN_BYTES_FOR_PARK
                      and base['rate_bps'] > 0)
            ratio = base_mbps = recent_mbps = None
            if usable:
                ratio = recent['rate_bps'] / base['rate_bps']
                base_mbps = base['rate_bps'] / 1e6
                recent_mbps = recent['rate_bps'] / 1e6
                n_with_canary += 1
                # Compute-bound ≠ shaped (Oct 2026): v2-upgrade and
                # frontier peers pull a 2–3 GB GPKG / the BEV tiles at KG
                # start (= the 30-min baseline) and then spend 1–2 h in
                # terrain/segment/vectorise with ~0 network.  The ratio
                # collapses to 0.00 while the CPU is pegged — at100 was
                # soft-parked 4× in 8 h, at117 lost a frontier KG + 1 h.
                # A shaped peer stalls with an idle CPU; a busy CPU means
                # the slowdown is the workload, not the network.
                if ratio < CANARY_SLOWDOWN_RATIO and _canary_peer_compute_bound(p):
                    streaks.pop(pid, None)
                    ratio = max(ratio, CANARY_SLOWDOWN_RECOVERY_RATIO)
                if ratio < CANARY_SLOWDOWN_RATIO:
                    n_in_slowdown += 1
                    # Start / continue streak.
                    s = streaks.setdefault(pid, {'since': now_ts})
                    s['last_ratio'] = ratio
                    s['last_ts'] = now_ts
                elif ratio >= CANARY_SLOWDOWN_RECOVERY_RATIO:
                    streaks.pop(pid, None)
            per_peer.append({
                'peer': p,
                'pid': pid,
                'usable': usable,
                'ratio': ratio,
                'base_mbps': base_mbps,
                'recent_mbps': recent_mbps,
            })
        # Trim streaks for peers no longer enabled.
        live_ids = {pp['pid'] for pp in per_peer}
        for stale_id in list(streaks.keys()):
            if stale_id not in live_ids:
                streaks.pop(stale_id, None)

        # Cross-peer correlation: are we in a fleet-wide upstream event?
        # If so, no peer parked this tick can earn a quality observation.
        fleet_wide = False
        if n_with_canary >= 4:
            frac = n_in_slowdown / n_with_canary
            if frac >= FLEET_CONCURRENT_SLOWDOWN_FRAC:
                fleet_wide = True
                log.info('Canary: fleet-wide slowdown detected '
                         '(%d/%d peers ≥ %.0f%%) — suppressing quality '
                         'observations this tick',
                         n_in_slowdown, n_with_canary,
                         FLEET_CONCURRENT_SLOWDOWN_FRAC * 100)
        # Stash for status payload.
        state['canary_fleet_slowdown'] = {
            'with_canary': n_with_canary,
            'in_slowdown': n_in_slowdown,
            'fleet_wide': fleet_wide,
            'ts': now_ts,
        }

        # Second pass: act.
        for entry in per_peer:
            p = entry['peer']
            pid = entry['pid']
            # Already parked? skip.
            if _peer_is_scheduled(p):
                continue
            reasons: list[str] = []
            slowdown_tripped = False
            quality_obs = False
            if entry['usable']:
                ratio = entry['ratio']
                base_mbps = entry['base_mbps']
                recent_mbps = entry['recent_mbps']
                if ratio < CANARY_SLOWDOWN_RATIO:
                    slowdown_tripped = True
                    streak = streaks.get(pid) or {}
                    streak_age = max(0.0, now_ts - float(streak.get('since') or now_ts))
                    # Network-grade gate must look at FRONTIER-mode
                    # samples only — cache-only baselines are
                    # Zenodo-bound (<0.5 MB/s) and can never satisfy
                    # the ≥5 MB/s threshold. Without this, the quality
                    # path is dead for the steady-state fleet workload
                    # mix and the fleet wall never converges.
                    fb = self._canary_throughput(
                        pid, CANARY_BASELINE_WINDOW_S, frontier_only=True)
                    fr = self._canary_throughput(
                        pid, CANARY_RECENT_WINDOW_S, frontier_only=True)
                    fbase_mbps = (fb['rate_bps'] / 1e6) if fb else None
                    frecent_mbps = (fr['rate_bps'] / 1e6) if fr else None
                    network_grade = (
                        fbase_mbps is not None
                        and frecent_mbps is not None
                        and fbase_mbps >= CANARY_BASELINE_NETWORK_MBPS
                        and frecent_mbps <= CANARY_RECENT_PARKED_MBPS
                    )
                    persistent = streak_age >= CANARY_QUALITY_PERSIST_S
                    quality_obs = (
                        network_grade and persistent and not fleet_wide
                    )
                    # --- Observation window (quality-path fix, Jul 2026).
                    # Parking at streak 0s made the 15-min persistence
                    # gate unsatisfiable: the peer was stopped before
                    # the streak could mature, so observed_cap_gb was
                    # NEVER learned fleet-wide (0 quality obs at 33 TB
                    # used) and every slowdown cost a 1 h idle cooldown
                    # — even for transient Zenodo blips that recover in
                    # minutes. New behaviour: let the peer keep running
                    # through the persistence window. If the slowdown
                    # is a blip, the streak resets (ratio ≥ 0.60) and
                    # we never lost a peer-hour. If it's real shaping,
                    # we park once with evidence (and, when network-
                    # grade, learn the wall). Worst case we "waste" 15
                    # min of degraded throughput — far cheaper than a
                    # guaranteed 1 h park per blip.
                    if not persistent:
                        # Log once per streak so process.txt shows the
                        # observation without per-tick spam.
                        s = streaks.get(pid)
                        if s is not None and not s.get('observe_logged'):
                            s['observe_logged'] = True
                            log.info(
                                'canary observe %s: throughput ratio=%.2f '
                                '(recent=%.2f vs baseline %.2f MB/s) — '
                                'watching for %ds before park',
                                pid, ratio, recent_mbps, base_mbps,
                                CANARY_QUALITY_PERSIST_S)
                    else:
                        qtag = ' [QUALITY]' if quality_obs else ' [soft'
                        if not quality_obs:
                            why = []
                            if not network_grade:
                                why.append('not network-grade')
                            if fleet_wide:
                                why.append('fleet-wide event')
                            qtag = ' [soft: ' + ', '.join(why) + ']'
                        reasons.append(
                            f'throughput collapsed: '
                            f'recent={recent_mbps:.2f} MB/s '
                            f'vs baseline {base_mbps:.2f} MB/s '
                            f'(ratio={ratio:.2f}, streak {int(streak_age)}s)'
                            + qtag
                        )
            # Noise-park: stricter, canary-override peers only.
            # LIVE rates only (bev+zenodo) — the EMA-backed
            # _peer_noise_score is for scheduling bias and barely
            # decays across a 1 h park, which produced ghost
            # park→unpark→park loops (Jul 2026). Copernicus noise is
            # excluded: it follows the credential, not the peer.
            is_override_canary = p.get('budget_gb') is not None
            if is_override_canary:
                noise = _peer_noise_score_live(pid, state)
                if noise >= CANARY_NOISE_PARK_THRESHOLD:
                    reasons.append(f'noise_score={noise:.2f} (live)')
            if not reasons:
                continue
            # Park: write not_before, persist, send graceful stop.
            # Quality-grade parks (real shaping observation) earn the
            # full 6 h cooldown; soft parks get the shorter window so
            # peers don't sit idle for hours after a transient blip.
            cooldown_s = (CANARY_PARK_COOLDOWN_S
                          if quality_obs
                          else CANARY_PARK_COOLDOWN_SOFT_S)
            cooldown = _dt.now(_tz.utc) + _td(seconds=cooldown_s)
            # Used-GB at park time — the observed wall for this peer.
            bw_now = (state.get('peer_bandwidth') or {}).get(pid) or {}
            used_gb_at_park = bw_now.get('used_gb')
            with self._lock:
                # Re-resolve under lock in case cfg was reloaded.
                live = get_peer_by_id(self.cfg, pid)
                if not live or not live.get('enabled', True):
                    continue
                if _peer_is_scheduled(live):
                    continue
                live['not_before'] = cooldown.isoformat()
                # Persist the observed shaping wall ONLY when the
                # trigger met the stricter network-grade gate. Soft
                # parks (low baseline, plausibly Zenodo-bound peers)
                # don't pollute the fleet wall estimate.
                if (quality_obs and slowdown_tripped
                        and isinstance(used_gb_at_park, (int, float))):
                    prev = live.get('observed_cap_gb')
                    if prev is None or used_gb_at_park < float(prev):
                        live['observed_cap_gb'] = round(float(used_gb_at_park), 2)
                    live['observed_cap_at'] = _dt.now(_tz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                    live['observed_cap_quality'] = True
                notes = live.setdefault('canary_notes', [])
                note: dict = {
                    'at': _dt.now(_tz.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'event': 'auto_park',
                    'reasons': reasons,
                    'cooldown_until': cooldown.isoformat(),
                }
                if slowdown_tripped and isinstance(used_gb_at_park, (int, float)):
                    note['used_gb_at_park'] = round(float(used_gb_at_park), 2)
                    note['quality'] = bool(quality_obs)
                notes.append(note)
                # Cap notes ring so peers.json doesn't grow unbounded.
                if len(notes) > 32:
                    del notes[: len(notes) - 32]
                cfg_changed = True
                peer_url = live.get('url')
            # Fresh observation window after the cooldown: without
            # this, the stale 'since' timestamp makes any post-resume
            # slowdown instantly 'persistent' (streak_age includes the
            # parked period).
            streaks.pop(pid, None)
            tag = 'canary park' if is_override_canary else 'auto-park'
            log.warning('%s %s: %s — not_before=%s',
                        tag, pid, '; '.join(reasons), cooldown.isoformat())
            try:
                _emit_director_event(
                    f'{tag} {pid}: {"; ".join(reasons)} (cooldown until {cooldown.isoformat()})',
                    peer=pid, level='warning')
            except Exception:
                pass
            try:
                if peer_url:
                    stop_peer_processor(peer_url, graceful=True)
            except Exception as e:
                log.warning('%s %s: graceful stop failed: %s', tag, pid, e)
        if cfg_changed:
            try:
                save_peers_config(self.cfg)
            except Exception:
                log.exception('canary park: save_peers_config failed')

    def _check_steal_health(self) -> None:
        """Auto-park peers whose rolling CPU-steal EMA stays >=
        :data:`STEAL_PARK_THRESHOLD_PCT` for :data:`STEAL_PARK_PERSIST_S`.

        Motivation: exe.dev's pools are bimodally contended (May 2026:
        AMD EPYC pool ~60 %% steal_med, Platinum 8259CL ~0 %%). A peer
        running at 80-95 %% steal burns Copernicus credits and KG
        wall-time for ~15-25 %% of a core. A short cooldown stops the
        bleeding and — since exe.dev can re-place idle VMs onto less
        contended hosts when they next ask for cycles — sometimes
        rescues the peer outright.

        Rules:
          * EMA threshold + recovery hysteresis (50 %%) prevents flap.
          * Streak must last ``STEAL_PARK_PERSIST_S`` (10 min) so a
            transient noisy-neighbour spike doesn't trip the park.
          * Cooldown is short (30 min) compared to the bandwidth-wall
            park (renewal day) — we want the peer back in rotation
            quickly if the host clears.
          * Skip peers that already have a future ``not_before`` set
            (primary park, bw-wall park, canary park), so we don't
            stomp on longer cooldowns.
          * Skip peers without a fleet ``peer_steal_ema`` sample yet.
        """
        from datetime import datetime as _dt, timedelta as _td, timezone as _tz
        with self._lock:
            cfg = self.cfg
            state = self.state
            peers = list(cfg.get('peers', []))
            streaks = state.setdefault('peer_steal_streak', {})
            stl_ema = dict(state.get('peer_steal_ema') or {})
        now_ts = time.time()
        cfg_changed = False
        live_ids: set[str] = set()
        for p in peers:
            if not p.get('enabled', True):
                continue
            pid = p['id']
            live_ids.add(pid)
            s = stl_ema.get(pid)
            if not isinstance(s, (int, float)):
                streaks.pop(pid, None)
                continue
            # Update streak.
            if s >= STEAL_PARK_THRESHOLD_PCT:
                st = streaks.setdefault(pid, {'since': now_ts})
                st['last_steal'] = round(float(s), 2)
                st['last_ts'] = now_ts
            elif s <= STEAL_PARK_RECOVERY_PCT:
                streaks.pop(pid, None)
                continue
            else:
                # In the hysteresis band: hold streak but don't extend.
                continue
            # Streak alive — long enough?
            streak_age = max(0.0, now_ts - float(
                (streaks.get(pid) or {}).get('since') or now_ts))
            if streak_age < STEAL_PARK_PERSIST_S:
                continue
            # Don't stomp on a longer cooldown already in effect.
            if _peer_is_scheduled(p):
                continue
            cooldown = _dt.now(_tz.utc) + _td(seconds=STEAL_PARK_COOLDOWN_S)
            with self._lock:
                live = get_peer_by_id(self.cfg, pid)
                if not live or not live.get('enabled', True):
                    continue
                if _peer_is_scheduled(live):
                    continue
                live['not_before'] = cooldown.isoformat()
                notes = live.setdefault('canary_notes', [])
                reason = (f'rolling steal {s:.0f}% >= '
                          f'{STEAL_PARK_THRESHOLD_PCT:.0f}% for '
                          f'{int(streak_age)}s')
                notes.append({
                    'at': _dt.now(_tz.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'event': 'steal_park',
                    'reasons': [reason],
                    'cooldown_until': cooldown.isoformat(),
                    'steal_ema': round(float(s), 2),
                })
                if len(notes) > 32:
                    del notes[: len(notes) - 32]
                cfg_changed = True
                peer_url = live.get('url')
            log.warning('steal-park %s: %s — not_before=%s',
                        pid, reason, cooldown.isoformat())
            try:
                _emit_director_event(
                    f'steal-park {pid}: {reason} (cooldown until '
                    f'{cooldown.isoformat()})',
                    peer=pid, level='warning')
            except Exception:
                pass
            try:
                if peer_url:
                    stop_peer_processor(peer_url, graceful=True)
            except Exception as e:
                log.warning('steal-park %s: graceful stop failed: %s',
                            pid, e)
            # Reset streak so the next admission cycle starts clean.
            streaks.pop(pid, None)
        # Trim streaks for peers no longer in the fleet.
        for stale_id in list(streaks.keys()):
            if stale_id not in live_ids:
                streaks.pop(stale_id, None)
        if cfg_changed:
            try:
                save_peers_config(self.cfg)
            except Exception:
                log.exception('steal-park: save_peers_config failed')

    # ------------------------------------------------------------------
    # Stuck-KG watchdog
    # ------------------------------------------------------------------
    @staticmethod
    def _kg_progress_fingerprint(ps: dict) -> str | None:
        """A string that changes whenever a peer makes *any* visible
        progress inside its current KG.

        Components (all director-visible via the pushed status):
          * current_kg.code      — new KG => fresh fingerprint
          * current_kg.step      — step transition (starting/lidar/.../upload)
          * current_kg.current_tile / n_tiles — per-tile loop advance
          * current_kg.step_detail_ts — within-step detail update
            (copernicus harmonics N/12, upload N%, ...), which is how
            the legitimately-multi-hour steps prove they're alive
          * completed            — a KG finished and a new one started

        Returns None when the peer is not processing a KG (idle/stopped/
        unreachable) — the watchdog skips those.
        """
        if not isinstance(ps, dict):
            return None
        state = (ps.get('state') or '').lower()
        if state not in ('running', 'processing'):
            return None
        ckg = ps.get('current_kg')
        if not isinstance(ckg, dict) or not ckg.get('code'):
            return None
        return '|'.join(str(x) for x in (
            ckg.get('code'),
            ckg.get('step'),
            ckg.get('current_tile'),
            ckg.get('n_tiles'),
            ckg.get('step_detail_ts'),
            ps.get('completed'),
        ))

    def _check_stuck_kg(self, statuses: dict) -> None:
        """Hard-restart peers whose processor has wedged inside a KG.

        A peer is "stuck" when its progress fingerprint
        (:meth:`_kg_progress_fingerprint`) has not changed for
        :data:`STUCK_KG_FROZEN_S`. We deliberately track a fingerprint
        rather than wall-clock KG elapsed because some KGs (frontier
        copernicus fetches, big gpkg uploads) legitimately run for
        hours — but they keep moving the fingerprint (tile index /
        step_detail_ts) while they do. A truly hung peer freezes every
        component at once.

        Guards against false positives:
          * Only fires on fresh, non-stale status (we must actually be
            seeing the peer's live state, not a cached fallback).
          * Resets the timer the instant the fingerprint changes.
          * Skips peers already scheduled-off (parked) — don't stomp a
            cooldown.

        Recovery is a *hard* stop (mid-KG SIGTERM→SIGKILL): the peer is
        frozen, so a graceful "exit after current KG" would never fire.
        Tile checkpoints + the cross-peer chkpt registry mean the work
        done so far isn't lost — the next peer to pick up the KG resumes
        the finished tiles. After the stop the scheduler re-admits the
        peer on a subsequent tick (no explicit park), and the KG goes
        back through the normal in-progress/requeue path.
        """
        now_ts = time.time()
        with self._lock:
            track = self.state.setdefault('stuck_kg_track', {})
            peers = list(self.cfg.get('peers', []))
            live_ids = {p['id'] for p in peers if p.get('enabled', True)}
        for p in peers:
            if not p.get('enabled', True):
                continue
            pid = p['id']
            ps = statuses.get(pid) or {}
            # Require a fresh, real status read. A stale/cached fallback
            # (the peer's gunicorn was briefly unreachable) must NOT be
            # mistaken for a frozen fingerprint — otherwise a flaky
            # network would get a busy peer killed.
            if ps.get('_stale') or (ps.get('state') == 'unreachable'):
                # Keep the existing timer but don't advance/evaluate it;
                # we simply have no fresh evidence this tick.
                continue
            fp = self._kg_progress_fingerprint(ps)
            if fp is None:
                track.pop(pid, None)
                continue
            ent = track.get(pid)
            if not ent or ent.get('fp') != fp:
                track[pid] = {'fp': fp, 'since': now_ts,
                              'kg': (ps.get('current_kg') or {}).get('code')}
                continue
            frozen_for = now_ts - float(ent.get('since') or now_ts)
            if frozen_for < STUCK_KG_FROZEN_S:
                continue
            # Don't stomp on a peer we've deliberately parked.
            if _peer_is_scheduled(p):
                continue
            kg_code = (ps.get('current_kg') or {}).get('code') or '?'
            step = (ps.get('current_kg') or {}).get('step') or '?'
            detail = (ps.get('current_kg') or {}).get('step_detail') or ''
            peer_url = p.get('url')
            msg = (f'stuck-kg {pid}: KG {kg_code} frozen on step "{step}" '
                   + (f'[{str(detail)[:60]}] ' if detail else '')
                   + f'for {frozen_for/3600:.1f}h (no progress fingerprint '
                   f'change) — hard restart')
            log.warning(msg)
            try:
                _emit_director_event(msg, peer=pid, kg=str(kg_code),
                                     level='warning')
            except Exception:
                pass
            try:
                # Hard stop: the processor is wedged, so graceful
                # (exit-after-KG) would never take effect.
                stop_peer_processor(peer_url, graceful=False)
            except Exception as e:
                log.warning('stuck-kg %s: hard stop failed: %s', pid, e)
            # Re-queue the interrupted KG on the director side. The
            # SIGKILL'd peer only re-adds it to its OWN local pending
            # pool (IN_PROGRESS_FILE crash-recovery); the director then
            # re-admits this peer onto a DIFFERENT dispatch, orphaning
            # the wedged KG entirely (lives in kg_list.json but nothing
            # references it — not the queue, not failed/deferred, no
            # manifest entry). Pushing it to the retry queue closes that
            # hole. The prune oracle keeps it only while coverage-
            # incomplete, so a since-finished KG can't loop.
            if kg_code and kg_code != '?':
                # Bump the adaptive-split strike counter on the DIRECTOR's
                # authoritative kg_strikes.json. The SIGKILL'd peer bumps
                # only its own local file (IN_PROGRESS_FILE crash
                # recovery) — but the KG bounces to a *different* peer
                # each time, so without this the strikes never accumulate
                # anywhere that matters and oversize KGs (91017 Schröcken,
                # 50017 Hörgersteig) are re-dispatched whole forever.
                # The director's file is max-merge synced to every peer
                # at dispatch time (_sync_kg_strikes_to_peer), so after
                # ADAPTIVE_SPLIT_THRESHOLD watchdog firings the next
                # dispatch expands the parent into blocks.
                try:
                    from kg_splitter import bump_strike, parent_kg_code
                    _strikes = bump_strike(parent_kg_code(str(kg_code)))
                    log.info('stuck-kg %s: strike %d recorded for KG %s',
                             pid, _strikes, kg_code)
                except Exception as e:
                    log.warning('stuck-kg %s: strike bump for %s failed: %s',
                                pid, kg_code, e)
                try:
                    _requeue_interrupted_kg(
                        kg_code, reason=f'stuck-kg {pid} hard restart')
                except Exception as e:
                    log.warning('stuck-kg %s: requeue %s failed: %s',
                                pid, kg_code, e)
            # Clear the timer so we don't immediately re-fire if the
            # status lingers before the stop takes hold.
            track.pop(pid, None)
        # Trim trackers for peers no longer in the fleet.
        for stale_id in list(track.keys()):
            if stale_id not in live_ids:
                track.pop(stale_id, None)

    # ------------------------------------------------------------------
    # Duplicate-KG reconciler
    # ------------------------------------------------------------------
    @staticmethod
    def _kg_progress_frac(ps: dict) -> float:
        """Return a 0..1 progress estimate for the peer's current KG.

        Mirrors ``_peerKgFrac`` in static/process.html exactly so the
        director's "who is furthest along" decision matches the number
        the dashboard shows in each peer's progress bar:
          * tile progress (``(current_tile-1)/n_tiles``) dominates when
            tile info is present — it's what every long step iterates;
          * otherwise fall back to the step's index in the canonical
            pipeline order.
        """
        ckg = ps.get('current_kg') if isinstance(ps, dict) else None
        if not isinstance(ckg, dict):
            return 0.0
        try:
            ct = int(ckg.get('current_tile') or 0)
            nt = int(ckg.get('n_tiles') or 0)
        except (TypeError, ValueError):
            ct = nt = 0
        if nt > 0 and ct > 0:
            return min(1.0, max(0.0, (ct - 1) / nt))
        # Canonical pipeline order — keep in sync with STEPS in
        # static/process.html.
        steps = ('lidar', 'terrain', 'ortho', 'copernicus',
                 'hansen', 'segment', 'vectorise', 'gpkg_full',
                 'gpkg_light', 'json', 'validate', 'upload', 'done')
        step = str(ckg.get('step') or '')
        import re as _re
        if _re.match(r'^tile_\d+$', step):
            step = 'segment'
        if step.startswith('upload'):
            step = 'upload'
        if step == 'json_summary':
            step = 'json'
        try:
            idx = steps.index(step)
        except ValueError:
            idx = 0
        return idx / max(1, len(steps) - 1)

    def _check_duplicate_kgs(self, statuses: dict) -> None:
        """Detect two+ peers running the *same* KG and stop the laggards.

        Dispatch guards (claim registry, ``_in_progress_kgs`` filter,
        block-level whitelist guard) prevent two peers being *handed*
        the same KG inside a tick. This handles the residue: duplicates
        that arise outside the 150s claim window — peers self-picking
        the same parent code from their own retry queues hours apart, or
        a peer starting a KG before another finished its upload. Those
        otherwise run side-by-side to completion, wasting a peer.

        Resolution: group running peers by *parent* KG code, then split
        each group into collision components: two codes collide only if
        they are identical or one is a prefix-at-``-`` of the other
        (block ``92117-west`` collides with parent ``92117``, and
        ``16111-southeast-1`` with ``16111-southeast``). **Sibling
        blocks do NOT collide** — ``45410-center`` and ``45410-west``
        cover disjoint areas and running them on two peers is exactly
        the parallelism the cache-only fleet exists for. (Sep 2026: the
        parent-only grouping hard-stopped 76 sibling blocks in 24 h,
        each losing up to 75 % of a block.) Within a component keep the
        peer with the highest progress fraction (tiebreak:
        longest-running, then id), and hard-stop the rest. Tile checkpoints + the cross-peer chkpt
        registry mean the loser's finished tiles aren't lost; the winner
        (or whoever next picks the KG) resumes them. After the stop the
        scheduler re-admits the loser on a later tick onto fresh work,
        and the dispatch guards keep it off the winner's KG.

        Anti-herd guards (mirrors the stuck-KG watchdog):
          * fresh-status only — never act on a stale/cached fallback;
          * both peers must be > ``DUP_KG_MIN_AGE_S`` into the KG (a
            just-started peer's current_kg may not yet be claim-visible);
          * the duplicate must persist for ``DUP_KG_PERSIST_S`` before we
            act (don't fight a dispatch the claim registry is resolving);
          * never stop a deliberately-parked peer;
          * cap stops at ``DUP_KG_STOPS_PER_TICK`` per tick.
        """
        import re as _re
        from datetime import datetime as _dt
        now_ts = time.time()

        def _parent(code: str) -> str:
            m = _re.match(r'^(\d+)(?:-[a-z][-a-z0-9]*)?$', str(code))
            return m.group(1) if m else str(code)

        def _started_ts(ps: dict) -> float:
            ckg = ps.get('current_kg') or {}
            s = ckg.get('started_at') if isinstance(ckg, dict) else None
            if not s:
                return now_ts
            try:
                return _dt.fromisoformat(str(s)).timestamp()
            except Exception:
                return now_ts

        with self._lock:
            peers = list(self.cfg.get('peers', []))
            track = self.state.setdefault('dup_kg_track', {})
            by_id = {p['id']: p for p in peers}

        # Group active, fresh-status peers by parent KG.
        groups: dict[str, list] = {}
        for p in peers:
            if not p.get('enabled', True):
                continue
            pid = p['id']
            ps = statuses.get(pid) or {}
            if ps.get('_stale') or ps.get('state') == 'unreachable':
                continue
            if ps.get('state') not in ('running', 'processing'):
                continue
            ckg = ps.get('current_kg') or {}
            code = ckg.get('code') if isinstance(ckg, dict) else None
            if not code:
                continue
            groups.setdefault(_parent(code), []).append((pid, ps))

        def _collides(a: str, b: str) -> bool:
            a, b = str(a), str(b)
            return a == b or a.startswith(b + '-') or b.startswith(a + '-')

        def _components(lst: list) -> list:
            """Split a parent group into connected components under
            ``_collides`` (union-find over ≤ a handful of peers)."""
            n = len(lst)
            root = list(range(n))

            def _find(i: int) -> int:
                while root[i] != i:
                    root[i] = root[root[i]]
                    i = root[i]
                return i
            codes = [((ps.get('current_kg') or {}).get('code') or '')
                     for _, ps in lst]
            for i in range(n):
                for j in range(i + 1, n):
                    if _collides(codes[i], codes[j]):
                        root[_find(i)] = _find(j)
            comps: dict = {}
            for i in range(n):
                comps.setdefault(_find(i), []).append(lst[i])
            return [c for c in comps.values() if len(c) > 1]

        # Keyed by "<parent>:<sorted member ids>" so sibling-block groups
        # that never collide don't keep a tracker alive.
        dup_groups: dict[str, list] = {}
        for par, lst in groups.items():
            if len(lst) < 2:
                continue
            for comp in _components(lst):
                key = f"{par}:{'+'.join(sorted(pid for pid, _ in comp))}"
                dup_groups[key] = comp
        # Prune trackers for groups no longer duplicated.
        for par in list(track.keys()):
            if par not in dup_groups:
                track.pop(par, None)

        stops_done = 0
        for par in sorted(dup_groups):
            lst = dup_groups[par]
            # All collide-members must be genuinely settled into the KG
            # before we trust the duplicate (avoids racing a dispatch the
            # claim registry is still resolving / a peer mid-startup).
            if any((now_ts - _started_ts(ps)) < DUP_KG_MIN_AGE_S
                   for _, ps in lst):
                track.pop(par, None)
                continue
            members = sorted(pid for pid, _ in lst)
            ent = track.get(par)
            if not ent or ent.get('members') != members:
                track[par] = {'members': members, 'since': now_ts}
                continue
            if (now_ts - float(ent.get('since') or now_ts)) < DUP_KG_PERSIST_S:
                continue
            # Rank: keep highest progress, then longest-running, then id.
            # Keep highest progress; tiebreak longest-running (smaller
            # started_ts), then lowest id — fully deterministic so both
            # gunicorn workers (and a post-handover director) would pick
            # the same winner from the same snapshot.
            ranked = sorted(
                lst,
                key=lambda t: (-self._kg_progress_frac(t[1]),
                               _started_ts(t[1]),
                               t[0]),
            )
            winner_id = ranked[0][0]
            for loser_id, lps in ranked[1:]:
                if stops_done >= DUP_KG_STOPS_PER_TICK:
                    break
                lp = by_id.get(loser_id)
                if not lp:
                    continue
                if _peer_is_scheduled(lp):
                    continue  # don't stomp a deliberate park
                lkg = ((lps.get('current_kg') or {}).get('code')
                       or par.split(':', 1)[0])
                wfrac = self._kg_progress_frac(
                    dict(ranked[0][1]))
                lfrac = self._kg_progress_frac(lps)
                msg = (f'dup-kg {loser_id}: KG {lkg} also running on '
                       f'{winner_id} (winner {wfrac*100:.0f}% vs '
                       f'{lfrac*100:.0f}%) — hard stop loser')
                log.warning(msg)
                try:
                    _emit_director_event(msg, peer=loser_id,
                                         kg=str(lkg), level='warning')
                except Exception:
                    pass
                try:
                    # Hard stop: a graceful exit-after-KG would let the
                    # loser finish the very work we're deduping. The
                    # winner keeps the KG; loser's tile checkpoints are
                    # preserved for resume.
                    stop_peer_processor(lp.get('url'), graceful=False)
                    stops_done += 1
                except Exception as e:
                    log.warning('dup-kg %s: hard stop failed: %s',
                                loser_id, e)
            # Clear so we re-evaluate from scratch next tick (the loser's
            # status will flip to stopped and the group dissolves).
            track.pop(par, None)

    # ------------------------------------------------------------------
    # BEV outage: escalating fleet pause
    # ------------------------------------------------------------------
    def _fleet_bev_warn_rate(self, statuses: dict) -> tuple[float, int]:
        """Return (sum_warns_per_min, n_peers_contributing) for BEV (1m window).

        Uses the per-peer ``warning_rates`` payload that peers push on
        every status tick. The director's existing EMA throttle uses the
        fleet *max* over 5m — we want a sharper signal here, so we sum
        the 1m rates across peers (an outage hits every peer at once,
        so the sum scales linearly with fleet size while a noisy
        single peer doesn't move the needle).
        """
        total = 0.0
        n = 0
        for pid, ps in (statuses or {}).items():
            if not isinstance(ps, dict):
                continue
            wr = ((ps.get('warning_rates') or {}).get('bev') or {})
            r = float(wr.get('1m') or 0.0)
            if r > 0:
                total += r
                n += 1
        return total, n

    def _classify_outage_multi_pool(
        self, statuses: dict,
    ) -> tuple[str, list[tuple[str, float, int]], float]:
        """Decide whether an outage is attributable to a small set of /24s.

        Returns ``(scope, pools, known_total_1m)`` where:
          * ``scope`` is ``'pools'`` if we can pin ≥ ``OUTAGE_POOL_DOMINANCE``
            of warns onto a small set (≤3) of /24s AND we have enough
            pool-coverage to trust the attribution. Otherwise ``'fleet'``.
          * ``pools`` is the list of ``(pool_key, sum_1m_wpm, n_peers)``
            tuples ordered by sum_1m descending. Empty when scope=='fleet'.
          * ``known_total_1m`` is the sum of 1m wpm across peers with a
            known outbound_24 (audit / event log).

        Heuristic vs the original single-pool classifier:
          * Still requires ≥ ``OUTAGE_POOL_KNOWN_FRAC`` of pressure to
            land in known pools (else the '?' bucket dominates and we
            can't distinguish a per-pool issue from a fleet outage).
          * Picks the smallest prefix of the pool ranking whose
            cumulative share ≥ ``OUTAGE_POOL_DOMINANCE`` (default 70%).
          * Bounded to ``OUTAGE_POOL_MAX_SET`` pools (default 3) — if we
            need >3 pools to reach dominance, the failure is fleet-wide.
          * Each pool in the prefix must have per-peer avg ≥
            ``OUTAGE_POOL_AVG_MULT`` (default 2.0) × the median per-peer
            avg over the *other* pools (so a single noisy peer in a
            clean pool doesn't taint the pool).

        Replaces ``_classify_outage_scope`` which only ever returned
        one pool; the 2026-05-28 incident showed two equally-bad pools
        (109.94.96.0/24 + 162.43.189.0/24) each ≈50% of warns — neither
        cleared the 70%% single-pool dominance bar, so the director
        falsely concluded fleet outage and paused everyone.
        """
        per_pool: dict[str, dict] = {}
        known_total_1m = 0.0
        unknown_total_1m = 0.0
        for pid, ps in (statuses or {}).items():
            if not isinstance(ps, dict):
                continue
            wr = ((ps.get('warning_rates') or {}).get('bev') or {})
            r = float(wr.get('1m') or 0.0)
            if r <= 0:
                continue
            sysd = ps.get('system') or {}
            host = sysd.get('host') or {}
            key = host.get('outbound_24') or '?'
            if key == '?':
                unknown_total_1m += r
                continue
            d = per_pool.setdefault(key, {'sum': 0.0, 'n': 0})
            d['sum'] += r
            d['n'] += 1
            known_total_1m += r
        denom = known_total_1m + unknown_total_1m
        if denom <= 0 or not per_pool:
            return 'fleet', [], known_total_1m
        known_frac = known_total_1m / denom
        if known_frac < OUTAGE_POOL_KNOWN_FRAC:
            return 'fleet', [], known_total_1m
        # Rank by summed 1m wpm.
        ranked = sorted(per_pool.items(), key=lambda kv: -kv[1]['sum'])
        # Find the smallest prefix reaching dominance.
        cum = 0.0
        prefix: list[tuple[str, dict]] = []
        for key, d in ranked:
            prefix.append((key, d))
            cum += d['sum']
            if cum / known_total_1m >= OUTAGE_POOL_DOMINANCE:
                break
        if cum / known_total_1m < OUTAGE_POOL_DOMINANCE:
            return 'fleet', [], known_total_1m
        if len(prefix) > OUTAGE_POOL_MAX_SET:
            return 'fleet', [], known_total_1m
        # Per-peer-rate sanity check is intentionally omitted in the
        # multi-pool path. The 2026-05-28 incident showed every peer in
        # both bad pools emitted ~1 wpm — the dominance is in *which*
        # peers are warning (almost everyone in pools A and B, almost
        # no one elsewhere), not in per-peer intensity. The combination
        # of share threshold + OUTAGE_POOL_MAX_SET bound already prevents
        # the trivial 'every pool a bit' case from being misclassified
        # as a pool issue.
        out: list[tuple[str, float, int]] = [
            (k, round(d['sum'], 2), d['n']) for k, d in prefix
        ]
        return 'pools', out, known_total_1m

    def _classify_outage_scope(
        self, statuses: dict, pools: dict, fleet_rate: float,
    ) -> tuple[str, str | None, float]:
        """Decide whether an in-progress BEV outage is pool-localized.

        Returns ``(scope, pool_key, attributable_rate)`` where:
          * ``scope`` is ``'pool'`` if one /24 dominates the warns AND
            we have enough pool coverage to trust the attribution.
            Otherwise ``'fleet'``.
          * ``pool_key`` is the offending /24 (only when scope=='pool').
          * ``attributable_rate`` is the sum of 1m BEV wpm coming from
            peers in the offending pool (for the event log / history).

        Heuristic: pool attribution requires
          * ≥ OUTAGE_POOL_KNOWN_FRAC of peers contributing warns have a
            known outbound_24 (else most pressure is in the '?' bucket
            and we can't tell which pool is to blame),
          * the top pool's *summed* 1m BEV wpm is ≥ OUTAGE_POOL_DOMINANCE
            of total known-pool warns,
          * AND ≥ OUTAGE_POOL_AVG_MULT × the second pool's per-peer
            avg (a single noisy peer in an otherwise-clean pool
            shouldn't tag the whole pool).
        """
        # Sum 1m BEV wpm per pool (matches the trigger metric).
        per_pool: dict[str, dict] = {}
        known_total_1m = 0.0
        unknown_total_1m = 0.0
        for pid, ps in (statuses or {}).items():
            if not isinstance(ps, dict):
                continue
            wr = ((ps.get('warning_rates') or {}).get('bev') or {})
            r = float(wr.get('1m') or 0.0)
            if r <= 0:
                continue
            sys = ps.get('system') or {}
            host = sys.get('host') or {}
            key = host.get('outbound_24') or '?'
            if key == '?':
                unknown_total_1m += r
                continue
            d = per_pool.setdefault(key, {'sum': 0.0, 'n': 0})
            d['sum'] += r
            d['n'] += 1
            known_total_1m += r
        denom = known_total_1m + unknown_total_1m
        if denom <= 0 or not per_pool:
            return 'fleet', None, fleet_rate
        known_frac = known_total_1m / denom
        if known_frac < OUTAGE_POOL_KNOWN_FRAC:
            return 'fleet', None, fleet_rate
        # Top-2 by summed 1m wpm.
        ranked = sorted(per_pool.items(), key=lambda kv: -kv[1]['sum'])
        top_key, top_d = ranked[0]
        top_share = top_d['sum'] / known_total_1m
        if top_share < OUTAGE_POOL_DOMINANCE:
            return 'fleet', None, fleet_rate
        if len(ranked) >= 2:
            top_avg = top_d['sum'] / max(1, top_d['n'])
            second = ranked[1][1]
            second_avg = second['sum'] / max(1, second['n'])
            if second_avg > 0 and top_avg < OUTAGE_POOL_AVG_MULT * second_avg:
                return 'fleet', None, fleet_rate
        return 'pool', top_key, round(top_d['sum'], 2)

    def _peers_in_pool(self, pool_key: str, statuses: dict) -> list[str]:
        out: list[str] = []
        for pid, ps in (statuses or {}).items():
            if not isinstance(ps, dict):
                continue
            host = (ps.get('system') or {}).get('host') or {}
            if (host.get('outbound_24') or '') == pool_key:
                out.append(pid)
        return out

    def _record_bev_pause_event(self, ev: dict) -> None:
        """Append a pause event to the persistent 24h ring."""
        with self._lock:
            hist = list(self.state.get('bev_pause_history') or [])
            hist.append(ev)
            if len(hist) > BEV_PAUSE_HISTORY_MAX:
                hist = hist[-BEV_PAUSE_HISTORY_MAX:]
            self.state['bev_pause_history'] = hist

    # === SECTION: Zenodo-degraded circuit + cache reconcile ===

    # Message classifier for the circuit: Zenodo *write-path* failures
    # only (PUT failed / upload failed / 5xx / timeouts). Deliberately
    # excludes read-side 404s on ZIP indices — those are manifest drift
    # (handled by the reconciler), not Zenodo being down.
    _ZEN_FAIL_RE = None

    @classmethod
    def _zen_fail_re(cls):
        if cls._ZEN_FAIL_RE is None:
            import re as _re
            cls._ZEN_FAIL_RE = _re.compile(
                r'(zenodo_client: PUT .* failed|'
                r'zenodo_cache: Upload of .* failed|'
                r'zenodo_cache: upload of .* failed|'
                r'Failed to upload .*zip|'
                r'Zenodo network|'
                r'Zenodo (GET|POST|PUT|DELETE) .* → 5\d\d|'
                r'(50[0-9]|429) (Server Error|Client Error|Bad Gateway|'
                r'Gateway Time-out|Service Unavailable)|'
                r'Gateway Time-out|Bad Gateway|'
                r'zenodo\.org.*(timed out|Max retries|SSLError|'
                r'Connection aborted))', _re.I)
        return cls._ZEN_FAIL_RE

    def _scan_zenodo_failures(self, window_s: int) -> dict:
        """Distinct peers with a Zenodo write failure in the last
        ``window_s`` seconds, from the tail of the merged 24h log ring
        (``data/combined_log_24h.jsonl``). Returns ``{peer: last_iso}``.
        Reads at most the last ~3 MB of the file."""
        p = Path('data/combined_log_24h.jsonl')
        out: dict = {}
        try:
            if not p.exists():
                return out
            size = p.stat().st_size
            with open(p, 'rb') as f:
                if size > 3_000_000:
                    f.seek(size - 3_000_000)
                    f.readline()
                raw = f.read()
        except Exception:
            return out
        cutoff = (datetime.now(timezone.utc)
                  - timedelta(seconds=window_s)).isoformat()
        rx = self._zen_fail_re()
        for line in raw.splitlines():
            try:
                e = json.loads(line)
            except Exception:
                continue
            ts = e.get('ts') or ''
            if ts < cutoff or e.get('level') not in ('warning', 'error'):
                continue
            msg = e.get('msg') or ''
            low = msg.lower()
            if 'zenodo' not in low and 'deposit' not in low:
                continue
            if 'zip index fetch failed' in low and '404' in low:
                continue
            if not rx.search(msg):
                continue
            pid = e.get('peer') or '?'
            if ts > out.get(pid, ''):
                out[pid] = ts
        return out

    def _publish_zenodo_circuit(self, zc: dict) -> None:
        """Write ``zenodo_circuit`` into cache_manifest.json so it rides
        the existing primary→peer sync (same transport as ``prewarm``).
        Idempotent on ``degraded``."""
        manifest_path = DATA_DIR / 'cache_manifest.json'
        try:
          from zenodo_cache import cache_manifest_write_lock as _cmwl
          with _cmwl():
            data = {}
            if manifest_path.exists():
                data = json.loads(manifest_path.read_text())
            prev = data.get('zenodo_circuit') or {}
            if bool(prev.get('degraded')) == bool(zc.get('degraded')) and \
                    prev.get('since') == zc.get('since'):
                return
            data['zenodo_circuit'] = {
                'degraded': bool(zc.get('degraded')),
                'since': zc.get('since'),
                'cleared_at': zc.get('cleared_at'),
                'updated_at': datetime.now(timezone.utc).isoformat(),
            }
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = str(manifest_path) + '.circuit.tmp'
            with open(tmp, 'w') as f:
                json.dump(data, f, indent=2, sort_keys=True)
            os.replace(tmp, manifest_path)
        except Exception as e:
            log.debug('zenodo_circuit publish failed: %s', e)

    # ------------------------------------------------------------------
    def _known_depo_ids(self) -> set:
        """Every deposition id any local manifest references (products,
        cache deposit, tile-store shards, chkpt registry). A draft whose
        id is in here is NEVER a sweep candidate, whatever Zenodo says."""
        ids: set = set()
        for fn in ('zenodo_manifest.json', 'cache_manifest.json',
                   'checkpoint_registry.json'):
            try:
                raw = json.loads((DATA_DIR / fn).read_text())
            except Exception:
                continue

            def _walk(o):
                if isinstance(o, dict):
                    for k, v in o.items():
                        if k in ('depo_id', 'deposit_id', 'deposition_id') and v:
                            try:
                                ids.add(int(v))
                            except Exception:
                                pass
                        else:
                            _walk(v)
                elif isinstance(o, list):
                    for v in o:
                        _walk(v)
            _walk(raw)
        return ids

    def _check_empty_drafts(self) -> None:
        """Delete untitled, file-less Zenodo drafts (see the ``empty-draft
        sweep`` SECTION). Gates: id not in any manifest, ``title`` empty,
        ``files`` empty, ``state`` unsubmitted, created ≥6 h ago, ≤10 per
        hourly tick, each candidate re-fetched individually right before
        DELETE. Not gated on the fleet Zenodo circuit (same reasoning as
        ``_check_v2_orphans``: evidence is positive, a failed DELETE is
        just retried next hour)."""
        now = time.time()
        if now - float(self.state.get('_empty_draft_checked_at') or 0) < EMPTY_DRAFT_CHECK_INTERVAL_S:
            return
        self.state['_empty_draft_checked_at'] = now
        try:
            import zenodo_client as _zc
            hdr = {'Authorization': f'Bearer {_zc.DEFAULT_TOKEN}'}
            # Product deposits are drafts too (~26k), so page through the
            # most recent EMPTY_DRAFT_LIST_PAGES×100 — orphans are always
            # born near the head of that list (one per final PUT failure).
            # The window rotates across ticks (pages 1-10, 11-20, … up to
            # EMPTY_DRAFT_MAX_PAGE, then back to 1) so the Oct-2026 backlog
            # (~420 drafts spread over ~10k product drafts) is reached
            # without hammering the list endpoint (429s) in one tick.
            start = int(self.state.get('_empty_draft_page_cursor') or 1)
            if start > EMPTY_DRAFT_MAX_PAGE:
                start = 1
            self.state['_empty_draft_page_cursor'] = start + EMPTY_DRAFT_LIST_PAGES
            drafts: list = []
            for page in range(start, start + EMPTY_DRAFT_LIST_PAGES):
                r = requests.get('https://zenodo.org/api/deposit/depositions',
                                 params={'status': 'draft', 'size': 100, 'page': page,
                                         'sort': 'mostrecent'},
                                 headers=hdr, timeout=60)
                if r.status_code != 200:
                    log.info('empty_drafts: list page %d HTTP %s — stopping', page, r.status_code)
                    break
                chunk = r.json()
                if not isinstance(chunk, list) or not chunk:
                    break
                drafts.extend(chunk)
                if len(chunk) < 100:
                    self.state['_empty_draft_page_cursor'] = 1  # end of list
                    break
        except Exception as ex:
            log.info('empty_drafts: list failed (%s) — using %d listed so far', ex, len(drafts))
        if not drafts:
            return
        known = self._known_depo_ids()
        if not known:
            log.warning('empty_drafts: no manifest ids loaded — refusing to sweep')
            return
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=EMPTY_DRAFT_MIN_AGE_S)
        try:
            self._adopt_lost_uploads(drafts, known, cutoff, hdr)
        except Exception:
            log.exception('lost-upload reconciler error')

        def _is_empty(d: dict) -> bool:
            if not isinstance(d, dict) or not d.get('id'):
                return False
            if d.get('title') or (d.get('metadata') or {}).get('title'):
                return False
            if d.get('files') or d.get('submitted') or d.get('state') not in (None, 'unsubmitted'):
                return False
            if (d.get('doi') or (d.get('metadata') or {}).get('doi')
                    or (d.get('metadata') or {}).get('prereserve_doi', {}).get('recid') not in (None, d.get('id'))):
                return False
            try:
                created = datetime.fromisoformat(d['created'].replace('Z', '+00:00'))
            except Exception:
                return False
            return created < cutoff

        cands = [d for d in drafts if _is_empty(d) and int(d['id']) not in known]
        total_empty = sum(1 for d in drafts if _is_empty(d))
        if not cands:
            if total_empty:
                log.info('empty_drafts: %d empty draft(s) seen, none eligible', total_empty)
            return
        deleted = 0
        for d in cands[:EMPTY_DRAFT_MAX_PER_TICK]:
            did = int(d['id'])
            try:
                # Re-fetch right before deleting: list views can be stale.
                # NB: Zenodo answers 404 ("persistent identifier does not
                # exist") for exactly these metadata-less drafts even
                # though they appear in the list and DELETE works — so a
                # 404 keeps the list-view verdict; 200 must still be empty;
                # anything else (429/5xx) is skipped.
                g = requests.get(f'https://zenodo.org/api/deposit/depositions/{did}',
                                 headers=hdr, timeout=30)
                if g.status_code == 200 and not _is_empty(g.json()):
                    log.info('empty_drafts: %d no longer empty/eligible — skipped', did)
                    continue
                if g.status_code not in (200, 404):
                    log.info('empty_drafts: %d re-fetch HTTP %s — skipped', did, g.status_code)
                    continue
                x = requests.delete(f'https://zenodo.org/api/deposit/depositions/{did}',
                                    headers=hdr, timeout=30)
                if x.status_code in (204, 404, 410):
                    deleted += 1
                else:
                    log.warning('empty_drafts: DELETE %d → HTTP %s %s', did, x.status_code, x.text[:120])
            except Exception as ex:
                log.warning('empty_drafts: %d: %s', did, ex)
        try:
            import app as _app
            _app.director_event(
                f'empty_drafts: deleted {deleted}/{len(cands)} untitled file-less draft(s) '
                f'older than {EMPTY_DRAFT_MIN_AGE_S // 3600}h ({total_empty} empty seen, '
                f'{len(known)} manifest ids guarded, pages {start}-{start + EMPTY_DRAFT_LIST_PAGES - 1})', peer='director',
                level='info' if deleted == len(cands) else 'warning')
        except Exception:
            pass

    # ------------------------------------------------------------------
    def _adopt_lost_uploads(self, drafts: list, known: set, cutoff, hdr: dict) -> None:
        """Adopt product files that landed on Zenodo but never made it into
        the manifest (see the ``lost-upload reconciler`` SECTION). Runs on
        the draft listing ``_check_empty_drafts`` already paid for."""
        import re as _re
        _title_rx = _re.compile(r'KG (\d+(?:-[a-z][-a-z0-9]*)?)\b')
        mf_path = DATA_DIR / 'zenodo_manifest.json'
        try:
            from zenodo_client import META_REV as _meta_rev
        except Exception:
            _meta_rev = 0
        try:
            raw = json.loads(mf_path.read_text())
            ent = raw.get('entries', raw) or {}
        except Exception as ex:
            log.info('lost_uploads: manifest unreadable (%s) — skipped', ex)
            return

        def _committed(e) -> bool:
            return (isinstance(e, dict) and int(e.get('size') or 0) > 0
                    and e.get('status') not in ('removed',)
                    and 'error' not in str(e.get('status') or ''))

        cands = []          # (draft, code, key, file)
        skipped = {'legacy_light': 0, 'dup_of_committed': 0, 'young': 0, 'odd': 0}
        for d in drafts:
            try:
                did = int(d.get('id') or 0)
            except Exception:
                continue
            if not did or did in known or d.get('submitted'):
                continue
            title = d.get('title') or (d.get('metadata') or {}).get('title') or ''
            files = d.get('files') or []
            if not title or len(files) != 1:
                continue
            m = _title_rx.search(title)
            if not m:
                continue
            code = m.group(1)
            f = files[0]
            fn = str(f.get('filename') or f.get('key') or '')
            if not fn.startswith(code) or int(f.get('filesize') or f.get('size') or 0) <= 0:
                skipped['odd'] += 1
                continue
            rest = fn[len(code):]
            if rest == '_light.gpkg':
                skipped['legacy_light'] += 1
                continue
            key = next((code + ks for fs, ks in LOST_UPLOAD_FILE_KEYS if rest == fs), None)
            if key is None:
                skipped['odd'] += 1
                continue
            try:
                created = datetime.fromisoformat(str(d['created']).replace('Z', '+00:00'))
            except Exception:
                continue
            if created >= cutoff:
                skipped['young'] += 1
                continue
            if _committed(ent.get(key)):
                skipped['dup_of_committed'] += 1
                continue
            cands.append((d, code, key, f))
        if not cands:
            if skipped['dup_of_committed']:
                log.info('lost_uploads: none adoptable (%d duplicate(s) of committed keys, '
                         '%d legacy light, %d young)', skipped['dup_of_committed'],
                         skipped['legacy_light'], skipped['young'])
            return
        # A re-run in flight writes its own (newer) record — never race it.
        try:
            qpath = DATA_DIR / 'retry_queue.json'
            queue = set(json.loads(qpath.read_text())) if qpath.exists() else set()
        except Exception:
            queue = set()
        try:
            running = _in_progress_kgs(self.cfg)
        except Exception:
            running = set()
        busy = queue | running
        cands = [c for c in cands if c[1] not in busy and c[1].split('-')[0] not in busy]
        adopted = []
        for d, code, key, f in cands[:LOST_UPLOAD_MAX_PER_TICK]:
            did = int(d['id'])
            try:
                # Re-fetch: list views are stale; the bucket is the truth.
                g = requests.get(f'https://zenodo.org/api/deposit/depositions/{did}',
                                 headers=hdr, timeout=30)
                if g.status_code != 200:
                    log.info('lost_uploads: %d re-fetch HTTP %s — skipped', did, g.status_code)
                    continue
                dd = g.json()
                files = dd.get('files') or []
                bucket = (dd.get('links') or {}).get('bucket') or ''
                if len(files) != 1 or not bucket:
                    continue
                ff = files[0]
                fn = str(ff.get('filename') or '')
                size = int(ff.get('filesize') or 0)
                md5 = str(ff.get('checksum') or '').replace('md5:', '')
                if fn != str(f.get('filename') or f.get('key')) or size <= 0 or not md5:
                    continue
                b = requests.get(bucket, headers=hdr, timeout=30)
                ts = ''
                if b.status_code == 200:
                    for c in (b.json().get('contents') or []):
                        if c.get('key') == fn:
                            if int(c.get('size') or 0) != size or c.get('delete_marker'):
                                size = 0
                            ts = str(c.get('updated') or c.get('created') or '')
                if size <= 0:
                    continue
                if not ts:
                    ts = str(dd.get('modified') or dd.get('created') or '')
                ej = ent.get(f'{code}_json')
                entry = {
                    'key': key, 'depo_id': did, 'bucket_url': bucket, 'filename': fn,
                    'size': size, 'checksum': f'md5:{md5}', 'uploaded_at': ts,
                    'version': str((dd.get('metadata') or {}).get('version')
                                   or (ej or {}).get('version') or ''),
                    'meta_rev': int(_meta_rev),
                    'adopted_at': datetime.now(timezone.utc).isoformat(),
                }
                if (not key.endswith('_full_gpkg') and isinstance(ej, dict)
                        and ej.get('status') == 'cf' and key != f'{code}_json'):
                    entry['status'] = 'cf'
                # Re-read + atomic write (the peer-sync thread rewrites this file too).
                raw = json.loads(mf_path.read_text())
                ent2 = raw.get('entries', raw)
                if _committed(ent2.get(key)):
                    continue
                ent2[key] = entry
                tmp = mf_path.with_suffix('.tmp.adopt')
                tmp.write_text(json.dumps({'entries': ent2}, indent=2, sort_keys=True))
                tmp.replace(mf_path)
                ent = ent2
                adopted.append(f'{key}@{did}')
                if key.endswith('_json'):
                    # local copy for the search index / coverage oracle
                    try:
                        jp = DATA_DIR / 'json' / f'{code}.json'
                        if not jp.exists():
                            r = requests.get(f'{bucket}/{fn}', headers=hdr, timeout=120)
                            if r.status_code == 200 and r.content[:1] == b'{':
                                jp.parent.mkdir(parents=True, exist_ok=True)
                                jp.write_bytes(r.content)
                    except Exception as ex:
                        log.info('lost_uploads: %s local json fetch: %s', code, ex)
            except Exception as ex:
                log.warning('lost_uploads: %d: %s', did, ex)
        if adopted or len(cands) > LOST_UPLOAD_MAX_PER_TICK:
            try:
                import app as _app
                _app.director_event(
                    f'lost_uploads: adopted {len(adopted)}/{len(cands)} unreferenced product '
                    f'file(s) into the manifest: {" ".join(adopted) or "-"}'
                    + (f' (+{len(cands) - LOST_UPLOAD_MAX_PER_TICK} next tick)'
                       if len(cands) > LOST_UPLOAD_MAX_PER_TICK else '')
                    + (f'; skipped dup={skipped["dup_of_committed"]} legacy_light={skipped["legacy_light"]}'
                       if skipped['dup_of_committed'] or skipped['legacy_light'] else ''),
                    peer='director', level='warning' if adopted else 'info')
            except Exception:
                pass

    # ------------------------------------------------------------------
    def _check_v2_orphans(self) -> None:
        """Remove dead parent-level ``_json_v2`` residue (see the
        ``v2 orphan sweep`` SECTION). Director-loop worker only, every
        V2_ORPHAN_CHECK_INTERVAL_S. Hard gates: Zenodo circuit clear,
        entry ≥24 h old, code not queued / not running on any peer,
        parent bbox covered without it, and the deposition on Zenodo
        contains exactly the one ``<code>_v2.json.gz`` file. Deliberately
        NOT gated on the fleet Zenodo circuit (see inline note)."""
        now = time.time()
        if now - float(self.state.get('_v2_orphan_checked_at') or 0) < V2_ORPHAN_CHECK_INTERVAL_S:
            return
        self.state['_v2_orphan_checked_at'] = now
        # No fleet-circuit gate here (unlike _check_v2_regen): the circuit
        # trips on *peer* large-body PUT failures, which can stay degraded
        # for many hours (Sep 23 2026: 01:54→11:30+, sweep never ran) while
        # the primary's small deposition GET/DELETE calls work fine. The
        # sweep's evidence is positive (HTTP 200 + exactly one file) so a
        # flaky Zenodo cannot yield a false candidate; a failed DELETE is
        # simply skipped and retried next interval.
        mf_path = DATA_DIR / 'zenodo_manifest.json'
        try:
            raw = json.loads(mf_path.read_text()) if mf_path.exists() else {}
            ent = raw.get('entries', raw) or {}
        except Exception:
            return
        try:
            import app as _app
            _cov = _app._kg_coverage_complete
        except Exception:
            return
        try:
            qpath = DATA_DIR / 'retry_queue.json'
            queue = set(json.loads(qpath.read_text())) if qpath.exists() else set()
        except Exception:
            queue = set()
        running: set = set()  # filled lazily (peer fanout) once a candidate exists

        def _com(e):
            return isinstance(e, dict) and int(e.get('size') or 0) > 0 and bool(e.get('uploaded_at'))

        cutoff = datetime.now(timezone.utc) - timedelta(seconds=V2_ORPHAN_MIN_AGE_S)
        cands = []
        for k, e in ent.items():
            if not k.endswith('_json_v2') or not _com(e):
                continue
            code = k[:-len('_json_v2')]
            if code in queue:
                continue
            if any(_com(ent.get(f'{code}_{sfx}')) for sfx in
                   ('json', 'full_gpkg', 'light_gpkg', 'light_gpkg_v2')):
                continue
            try:
                if datetime.fromisoformat(str(e['uploaded_at'])) > cutoff:
                    continue
            except Exception:
                continue
            parent = code.split('-')[0]
            try:
                sans = dict(ent); sans.pop(k, None)
                if not _cov(parent, sans)[0]:
                    continue
            except Exception:
                continue
            cands.append((code, k, e))
        if not cands:
            return
        try:
            running = _in_progress_kgs(self.cfg)
        except Exception:
            return
        cands = [c for c in cands if c[0] not in running and c[0].split('-')[0] not in running]
        if not cands:
            return
        import zenodo_client as _zc
        hdr = {'Authorization': f'Bearer {_zc.DEFAULT_TOKEN}'}
        done = 0
        for code, key, e in cands:
            if done >= V2_ORPHAN_MAX_PER_TICK:
                break
            depo = e.get('depo_id')
            if not depo or any(kk != key and isinstance(ee, dict) and ee.get('depo_id') == depo
                               for kk, ee in ent.items()):
                continue
            try:
                r = requests.get(f'https://zenodo.org/api/deposit/depositions/{depo}',
                                 headers=hdr, timeout=30)
            except Exception as ex:
                log.info('v2_orphan: %s depo %s probe failed (%s) — skip', code, depo, ex)
                continue
            if r.status_code == 404:
                files = []
            elif r.status_code != 200:
                log.info('v2_orphan: %s depo %s HTTP %s — skip', code, depo, r.status_code)
                continue
            else:
                d = r.json()
                files = [f.get('filename') for f in (d.get('files') or [])]
                if d.get('submitted') or files != [f'{code}_v2.json.gz']:
                    log.warning('v2_orphan: %s depo %s unexpected (submitted=%s files=%s) — NOT touching',
                                code, depo, d.get('submitted'), files)
                    continue
                try:
                    from zenodo_lock import zenodo_upload_lock
                    with zenodo_upload_lock(purpose='v2_orphan', max_wait_s=120):
                        rd = requests.delete(f'https://zenodo.org/api/deposit/depositions/{depo}',
                                             headers=hdr, timeout=60)
                except Exception as ex:
                    log.info('v2_orphan: %s depo %s delete failed (%s) — skip', code, depo, ex)
                    continue
                if rd.status_code not in (204, 404):
                    log.warning('v2_orphan: %s depo %s DELETE HTTP %s — skip', code, depo, rd.status_code)
                    continue
            try:
                rm = requests.delete(f'{config.APP_URL}/api/v1/processing/manifest/{key}', timeout=15)
                ok = rm.status_code in (200, 404)
            except Exception as ex:
                ok = False
                log.warning('v2_orphan: %s manifest delete failed: %s', key, ex)
            try:
                import v2_ingest as _vi
                _vi._clear_failure(code)
            except Exception:
                pass
            done += 1
            try:
                import app as _app
                _app.director_event(
                    f'v2orphan: removed parent-level {key} (depo {depo}, {int(e.get("size") or 0)} B, '
                    f'no _json/_full_gpkg/_light_gpkg_v2; blocks cover parent) manifest_ok={ok}',
                    peer='director', kg=code)
            except Exception:
                pass

    def _check_json_heal(self) -> None:
        """Rebuild missing ``_json`` completion markers from ``_json_v2``
        (see json_heal.py — Oct 2026 role-eviction purge bug). Director-loop
        worker only, every JSON_HEAL_INTERVAL_S, a few codes per tick; codes
        queued or running on a peer are left to that peer. Not gated on
        the fleet Zenodo circuit (same reasoning as _check_v2_orphans: the
        primary's small PUTs work while peers' multi-GB ones time out)."""
        now = time.time()
        if now - float(self.state.get('_json_heal_checked_at') or 0) < JSON_HEAL_INTERVAL_S:
            return
        self.state['_json_heal_checked_at'] = now
        import json_heal as _jh
        try:
            qpath = DATA_DIR / 'retry_queue.json'
            queued = set(json.loads(qpath.read_text())) if qpath.exists() else set()
        except Exception:
            queued = set()
        try:
            running = _in_progress_kgs(self.cfg)
        except Exception:
            running = set()

        def _ev(msg, kg='', level='info'):
            try:
                import app as _app
                _app.director_event(msg, peer='director', kg=kg, level=level)
            except Exception:
                pass
        res = _jh.sweep(running=running, queued=queued, event=_ev)
        res['t'] = now
        try:
            JSON_HEAL_STATE.write_text(json.dumps(res))
        except Exception:
            pass

    V24_AUDIT_INTERVAL_S = 300

    def _v24_audit_tick(self) -> None:
        """Every 5 min (docs/v2.4-rollout.md → Director step 5): refresh the
        NE manifest (candidate gate ``ne_require_built``), verify up
        to 20 ``status=removed`` legacy records against the deposition API
        and re-issue the delete for ``present`` verdicts.  Runs in a
        background thread so Zenodo latency never blocks the tick."""
        now = time.time()
        if now - float(self.state.get('_v24_audit_at') or 0) < self.V24_AUDIT_INTERVAL_S:
            return
        if getattr(self, '_v24_audit_thread', None) is not None and self._v24_audit_thread.is_alive():
            return
        self.state['_v24_audit_at'] = now

        def _run():
            import ne_rollout
            # label_mix (Oct 2026): incremental h3b audit for the
            # restored-checkpoint label-id collision + bounded auto-requeue
            # of affected codes for a full rerun (label_mix.py).
            try:
                import label_mix as _lm
                _lr = _lm.sweep()
                if _lr.get('new_detected') or _lr.get('requeued'):
                    log.info('label_mix sweep: %s', _lr)
            except Exception as e:  # noqa: BLE001
                log.warning('label_mix sweep failed: %s', e)
            try:
                # 10 min is plenty; each miss costs ~260 KB on the metered link.
                ne_rollout.refresh_ne_manifest(max_age_s=600)
            except Exception as e:  # noqa: BLE001
                log.debug('ne manifest refresh: %s', e)
            # Current metadata wording on every committed deposition
            # (title/description/keywords only; never publishes, never
            # touches uploaded_at) — zenodo_meta_refresh, ≤ PER_TICK/5 min.
            try:
                import zenodo_meta_refresh as _zmr
                mr = _zmr.run()
                if mr.get('ok') or mr.get('errors') or mr.get('stopped'):
                    log.info('meta refresh: %s', {k: mr.get(k) for k in ('ok', 'gone', 'published', 'errors', 'stopped', 'pending')})
            except Exception as e:  # noqa: BLE001
                log.debug('meta refresh: %s', e)
            # Primary's local v1 JSON copies follow the Zenodo strip
            # (local mirror).
            try:
                ls = ne_rollout.strip_local_json()
                if ls.get('stripped'):
                    log.info('legacy local json strip: %s', ls)
            except Exception as e:  # noqa: BLE001
                log.debug('legacy local strip: %s', e)
            try:
                res = ne_rollout.audit_removed(20)
            except Exception as e:  # noqa: BLE001
                log.debug('legacy audit: %s', e)
                return
            if res.get('present'):
                # Re-issue the delete: the record says removed but the
                # deposition still answers 200.
                try:
                    from zenodo_client import Client as _ZC, DEFAULT_TOKEN as _TK
                    aud = ne_rollout.load_audit()
                    raw = json.loads((DATA_DIR / 'zenodo_manifest.json').read_text())
                    ent = raw.get('entries', raw) or {}
                    n = 0
                    for k, a in aud.items():
                        if a.get('verdict') != 'present' or n >= 5:
                            continue
                        depo = int((ent.get(k) or {}).get('depo_id') or a.get('depo_id') or 0)
                        if not depo:
                            continue
                        r = requests.delete(f'https://zenodo.org/api/deposit/depositions/{depo}',
                                            headers={'Authorization': f'Bearer {_TK}'}, timeout=30)
                        n += 1
                        try:
                            from app import director_event as _ev
                            _ev(f'legacy audit: {k} depo {depo} REMOVED-BUT-PRESENT → delete HTTP {r.status_code}',
                                peer='director', kg=k.split('_')[0],
                                level='warning' if r.status_code >= 300 else 'info')
                        except Exception:
                            pass
                        if r.status_code in (204, 404, 410):
                            a['verdict'] = 'gone'; a['checked_at'] = ne_rollout._now()
                    ne_rollout.save_audit(aud)
                except Exception as e:  # noqa: BLE001
                    log.warning('legacy re-delete: %s', e)
            if res.get('checked'):
                log.info('v2.4 legacy audit: %s', res)

        self._v24_audit_thread = threading.Thread(target=_run, daemon=True, name='v24-audit')
        self._v24_audit_thread.start()

    def _check_v2_regen(self) -> None:
        """Verify + auto-requeue v2_regen candidates (see V2_REGEN_FILE
        section). Director-loop worker only, every V2_REGEN_CHECK_INTERVAL_S.
        Hard gates: fleet Zenodo circuit clear, control probe 200, two
        independent "gone" verdicts ≥30 min apart, ≤3 requeues/tick,
        ≤20 outstanding."""
        now = time.time()
        if now - float(self.state.get('_v2_regen_checked_at') or 0) < V2_REGEN_CHECK_INTERVAL_S:
            return
        self.state['_v2_regen_checked_at'] = now
        with _V2_REGEN_LOCK:
            d = _v2_regen_load()
        if not d:
            return
        now_iso = datetime.now(timezone.utc).isoformat(timespec='seconds')
        mf_path = DATA_DIR / 'zenodo_manifest.json'
        try:
            raw = json.loads(mf_path.read_text()) if mf_path.exists() else {}
            ent = raw.get('entries', raw) or {}
        except Exception:
            ent = {}
        changed = False
        try:
            qpath0 = DATA_DIR / 'retry_queue.json'
            queue0 = set(json.loads(qpath0.read_text())) if qpath0.exists() else set()
        except Exception:
            queue0 = set()
        try:
            _tp0 = DATA_DIR / 'manifest_tombstones.json'
            tomb0 = json.loads(_tp0.read_text()) if _tp0.exists() else {}
            if not isinstance(tomb0, dict):
                tomb0 = {}
        except Exception:
            tomb0 = {}
        # --- settle requeued / expire done (no Zenodo traffic) ---
        for code, e in list(d.items()):
            st = e.get('state')
            if st in ('requeued', 'candidate', 'confirming') and ent and _v2_regen_orphan(code, ent):
                e['state'] = 'dismissed'; e['done_at'] = now_iso
                e['note'] = 'orphan (old-split residue): parent covered without this code — never regenerated'
                changed = True
                _v2_regen_drop_orphan_queue(code)
                try:
                    import app as _app
                    _app.director_event(f'v2regen: {code} dismissed — old-split residue, parent bbox is covered '
                                        f'without it (was {st}); dropped from the queue',
                                        peer='director', kg=code)
                except Exception:
                    pass
                continue
            if st == 'requeued':
                j2 = ent.get(f'{code}_json_v2')
                from v21_products import v2_products_complete as _v2c
                if isinstance(j2, dict) and _v2c(ent, code) \
                        and str(j2.get('uploaded_at') or '') > str(e.get('requeued_at') or ''):
                    e['state'] = 'done'; e['done_at'] = now_iso; changed = True
                    try:
                        import app as _app
                        _app.director_event(f'v2regen: {code} fresh v2.2 products committed — regen complete',
                                            peer='director', kg=code)
                    except Exception:
                        pass
                    continue
                # Reverted requeue (Sep 2026): the code left the queue, its
                # ``_requeue`` tombstone is gone and the gone product key
                # is referenced again (peer-sync re-import) — nothing will
                # ever run it, yet it counts against V2_REGEN_MAX_OUTSTANDING.
                # Demote to ``confirming`` (checks kept) so it is re-verified
                # and re-requeued through the normal gates.
                key0 = e.get('key') or f'{code}_full_gpkg'
                g0 = ent.get(key0)
                try:
                    _age = now - datetime.fromisoformat(e.get('requeued_at') or now_iso).timestamp()
                except Exception:
                    _age = 0
                if (_age > 1800 and code not in queue0
                        and f'{code}_requeue' not in tomb0
                        and isinstance(g0, dict) and g0.get('depo_id')):
                    e['state'] = 'confirming'
                    e['note'] = f'requeue reverted (tombstones dropped) — re-confirming; was requeued {e.get("requeued_at")}'
                    e['reverted'] = int(e.get('reverted') or 0) + 1
                    changed = True
                    log.warning('v2_regen %s: requeue reverted — back to confirming', code)
            elif st in ('done', 'dismissed'):
                try:
                    ts = datetime.fromisoformat(e.get('done_at') or e.get('first_seen')).timestamp()
                except Exception:
                    ts = 0
                if now - ts > V2_REGEN_DONE_TTL_S:
                    d.pop(code, None); changed = True
        pending = [(c, e) for c, e in d.items() if e.get('state') in ('candidate', 'confirming')]
        if not pending:
            if changed:
                with _V2_REGEN_LOCK:
                    _v2_regen_save(d)
            return
        # --- gates ---
        try:
            from zenodo_cache import zenodo_degraded as _zdeg
            degraded = bool(_zdeg())
        except Exception:
            degraded = False
        if degraded or (self.state.get('zenodo_circuit') or {}).get('degraded'):
            # LOSSY ≠ DEGRADED (docs/copernicus-throttle.md → Correction
            # 2026-09-23): the circuit trips on peer PUT failures and in
            # Oct 2026 stayed tripped for days while ~20 GB/h kept landing.
            # Verification here is a deposition-API GET (200 + file list),
            # not a bucket fetch — trustworthy whenever uploads are landing.
            # Only hold when the hour is truly dry.
            _landing = False
            try:
                import app as _app0
                _zu = _app0._zenodo_upload_health()
                _landing = bool(_zu.get('ok_files', 0) >= 5 and _zu.get('ok_bytes', 0) >= 1e9)
            except Exception:
                _landing = False
            if not _landing:
                if changed:
                    with _V2_REGEN_LOCK:
                        _v2_regen_save(d)
                log.info('v2_regen: %d candidate(s) waiting — Zenodo circuit degraded, no verification', len(pending))
                return
            log.info('v2_regen: circuit tripped but uploads landing (lossy) — verifying %d candidate(s)', len(pending))
        try:
            import zenodo_client as _zc
            hdr = {'Authorization': f'Bearer {_zc.DEFAULT_TOKEN}'}
            r = requests.get('https://zenodo.org/api/deposit/depositions', params={'size': 1},
                             headers=hdr, timeout=30)
            if r.status_code != 200:
                log.info('v2_regen: control probe HTTP %s — skipping this tick', r.status_code)
                return
        except Exception as ex:
            log.info('v2_regen: control probe failed (%s) — skipping this tick', ex)
            return
        outstanding = sum(1 for e in d.values() if e.get('state') == 'requeued')
        requeued_now = 0
        try:
            qpath = DATA_DIR / 'retry_queue.json'
            queue = set(json.loads(qpath.read_text())) if qpath.exists() else set()
        except Exception:
            queue = set()
        for code, e in pending[:10]:
            key = e.get('key') or f'{code}_full_gpkg'
            g = ent.get(key)
            if not isinstance(g, dict) or not g.get('depo_id'):
                # Manifest already dropped it (operator requeue / prior regen).
                e['state'] = 'dismissed'; e['done_at'] = now_iso
                e['note'] = 'no manifest entry'; changed = True
                continue
            verdict = None
            try:
                r = requests.get(f"https://zenodo.org/api/deposit/depositions/{g['depo_id']}",
                                 headers=hdr, timeout=30)
                if r.status_code == 200:
                    files = r.json().get('files') or []
                    present = any(f.get('filename') == g.get('filename') for f in files)
                    verdict = 'present' if present else 'gone'
                elif r.status_code in (404, 410):
                    verdict = 'gone'
            except Exception as ex:
                log.debug('v2_regen %s: deposition probe %s', code, ex)
            if verdict is None:
                continue
            e.setdefault('checks', []).append({'ts': now_iso, 'v': verdict})
            e['checks'] = e['checks'][-6:]
            changed = True
            if verdict == 'present':
                e['state'] = 'dismissed'; e['done_at'] = now_iso
                e['note'] = 'file present on Zenodo (peer 404 was transient)'
                try:
                    import app as _app
                    _app.director_event(f'v2regen: {code} {key.split("_", 1)[1]} is present on Zenodo — '
                                        f'dismissed (transient 404 on {",".join(e.get("peers") or [])})',
                                        peer='director', kg=code, level='warning')
                except Exception:
                    pass
                continue
            gone = [c for c in e['checks'] if c.get('v') == 'gone']
            e['state'] = 'confirming'
            if len(gone) < V2_REGEN_CONFIRMATIONS:
                continue
            try:
                gap = (datetime.fromisoformat(gone[-1]['ts']) - datetime.fromisoformat(gone[0]['ts'])).total_seconds()
            except Exception:
                gap = 0
            if gap < V2_REGEN_CONFIRM_GAP_S:
                continue
            if code in queue:
                e['state'] = 'requeued'; e['requeued_at'] = now_iso
                e['note'] = 'already queued'; continue
            if requeued_now >= V2_REGEN_MAX_REQUEUE_PER_TICK or outstanding >= V2_REGEN_MAX_OUTSTANDING:
                continue
            try:
                r = requests.post(config.APP_URL + '/api/v1/processing/queue',
                                  json={'kgs': [code], 'position': 0, 'skip_processed': False,
                                        'keep_products': True, 'gone_keys': [key]},
                                  timeout=PEER_TIMEOUT_CONTROL, headers=_admin_headers())
                ok = r.ok
            except Exception as ex:
                ok = False
                log.warning('v2_regen %s: requeue POST failed: %s', code, ex)
            if ok:
                e['state'] = 'requeued'; e['requeued_at'] = now_iso
                requeued_now += 1; outstanding += 1
                try:
                    import app as _app
                    _app.director_event(
                        f'v2regen: {code} {key.split("_", 1)[1]} confirmed gone (depo {g.get("depo_id")}, '
                        f'{len(gone)} checks over {gap / 60:.0f} min) — requeued for a fresh v2.2 run; '
                        f'v1 json/light kept until replaced', peer='director', kg=code, level='warning')
                except Exception:
                    pass
        if changed:
            with _V2_REGEN_LOCK:
                _v2_regen_save(d)

    def _check_zenodo_degraded(self) -> None:
        """Trip / clear the fleet Zenodo-degraded circuit. Director-loop
        worker only; throttled to one log scan per
        ZENODO_DEGRADED_SCAN_INTERVAL_S. State persisted in
        director_state.json['zenodo_circuit'] (survives the gunicorn
        2-worker swap + HA handover via SNAPSHOT_FILES)."""
        now = time.time()
        zc = dict(self.state.get('zenodo_circuit') or {})
        if now - float(zc.get('_scanned_at') or 0) < ZENODO_DEGRADED_SCAN_INTERVAL_S:
            return
        fails = self._scan_zenodo_failures(ZENODO_DEGRADED_WINDOW_S)
        zc['_scanned_at'] = now
        zc['peers_15m'] = sorted(fails)
        zc['n_peers_15m'] = len(fails)
        last_fail_iso = max(fails.values()) if fails else zc.get('last_failure_at')
        if fails:
            zc['last_failure_at'] = last_fail_iso
            if zc.get('degraded'):
                # Cumulative scope since the trip (the 15-min window is a
                # *recency* signal, not the incident's blast radius).
                zc['peers_since_trip'] = sorted(
                    set(zc.get('peers_since_trip') or []) | set(fails))
                zc['n_peers_since_trip'] = len(zc['peers_since_trip'])
        now_iso = datetime.now(timezone.utc).isoformat(timespec='seconds')
        was = bool(zc.get('degraded'))
        if not was and len(fails) >= ZENODO_DEGRADED_MIN_PEERS:
            zc.update({'degraded': True, 'since': now_iso,
                       'trip_count': int(zc.get('trip_count') or 0) + 1,
                       'trip_peers': sorted(fails),
                       'peers_since_trip': sorted(fails),
                       'n_peers_since_trip': len(fails)})
            _emit_director_event(
                f'zenodo_circuit: TRIPPED — {len(fails)} peers logged '
                f'Zenodo write failures in {ZENODO_DEGRADED_WINDOW_S // 60} min '
                f'({", ".join(sorted(fails)[:8])}); peers defer cache-cell '
                f'replacement uploads + never tombstone until clear',
                peer='director', level='warning')
            log.warning('zenodo_circuit tripped: %s', sorted(fails))
        elif was:
            quiet_s = None
            try:
                lf = zc.get('last_failure_at')
                if lf:
                    quiet_s = (datetime.now(timezone.utc)
                               - datetime.fromisoformat(lf)).total_seconds()
            except Exception:
                quiet_s = None
            if quiet_s is None or quiet_s >= ZENODO_DEGRADED_CLEAR_S:
                zc.update({'degraded': False, 'cleared_at': now_iso})
                _emit_director_event(
                    f'zenodo_circuit: CLEARED after '
                    f'{int((quiet_s or 0) // 60)} min without Zenodo write '
                    f'failures (was degraded since {zc.get("since")}); '
                    f'running cache-manifest reconcile now',
                    peer='director')
                log.info('zenodo_circuit cleared; triggering reconcile')
                self.state['zenodo_circuit'] = zc
                self._publish_zenodo_circuit(zc)
                self._reconcile_cache_manifest_if_due(force=True)
                return
        self.state['zenodo_circuit'] = zc
        self._publish_zenodo_circuit(zc)

    def _reconcile_cache_manifest_if_due(self, force: bool = False) -> None:
        """Hourly (or forced) cache_manifest ↔ deposit reconcile in a
        daemon thread. Single-flight via _CACHE_RECONCILE_LOCK; TTL via
        state['_cache_reconcile_at']. Skipped while the circuit is
        degraded (the listing is exactly what's flaky then) unless
        forced. Emits a director_event when anything changed."""
        if not config.uploads_enabled():
            # Reconcile edits the deposit; nothing to do without a token.
            return
        now = time.time()
        last = float(self.state.get('_cache_reconcile_at') or 0)
        if not force and now - last < CACHE_RECONCILE_INTERVAL_S:
            return
        if not force and (self.state.get('zenodo_circuit') or {}).get('degraded') \
                and now - last < 3 * CACHE_RECONCILE_INTERVAL_S:
            # Skip while degraded — but not indefinitely: a long-running
            # degraded window (Sep 2026: 15 h) must not suspend the chkpt
            # GC, since deposit file-slot exhaustion (400 on every new
            # cell upload) is itself a cause of degraded readings.
            return
        if not _CACHE_RECONCILE_LOCK.acquire(blocking=False):
            return
        self.state['_cache_reconcile_at'] = now

        def _run():
            try:
                import zenodo_cache as _zc
                s = _zc.reconcile_manifest(dry_run=False, take_lock=True)
                slim = {k: s.get(k) for k in (
                    'at', 'tombstoned', 'restored', 'corrupt', 'unknown',
                    'unverified_cleared', 'drift_fixed', 'changed',
                    'chkpt_gc', 'chkpt_files', 'junk_deleted',
                    'deposit_files', 'took_s', 'error')}
                slim['forced'] = bool(force)
                self.state['cache_reconcile'] = slim
                if s.get('error'):
                    _emit_director_event(
                        f'cache_reconcile: failed — {s["error"]}',
                        peer='director', level='warning')
                elif s.get('changed'):
                    det = '; '.join(
                        f'{d["action"]} {d["file"]}'
                        for d in (s.get('details') or [])
                        if d.get('action') in ('tombstoned', 'restored', 'corrupt',
                                               'chkpt_gc', 'junk_deleted'))
                    _emit_director_event(
                        f'cache_reconcile: tombstoned={s["tombstoned"]} '
                        f'restored={s["restored"]} corrupt={s["corrupt"]} '
                        f'unknown={s["unknown"]} '
                        f'chkpt_gc={s.get("chkpt_gc", 0)} '
                        f'unverified_cleared={s["unverified_cleared"]} '
                        f'drift_fixed={s["drift_fixed"]} — {det[:600]}',
                        peer='director',
                        level='warning' if (s['tombstoned'] or s['corrupt'])
                        else 'info')
                else:
                    log.info('cache_reconcile: clean (deposit_files=%s)',
                             s.get('deposit_files'))
            except Exception as e:
                log.warning('cache_reconcile thread failed: %s', e)
                self.state['cache_reconcile'] = {
                    'at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
                    'error': str(e)[:200]}
            finally:
                _CACHE_RECONCILE_LOCK.release()

        threading.Thread(target=_run, daemon=True,
                         name='cache-reconcile').start()

    def _check_bev_outage(self, statuses: dict) -> None:
        """Detect a BEV outage and flip ``state['bev_pause']``.

        Scope-aware:
          * If one /24 attributable for >=70% of warns AND we know
            outbound_24 for >=60% of warning peers, park just that
            pool (level-escalated cooldown via not_before on each
            affected peer). Other pools keep working.
          * Else fleet-wide pause (cache-only + parallel orchestrators
            short-circuit while active).

        State shape persisted on the director:
            state['bev_pause'] = {
                'active': bool,
                'scope': 'fleet' | 'pool',
                'pool': str | None,
                'level': 1..4,
                'since': iso8601,
                'until': iso8601,
                'cooldown_s': int,
                'reason': str,
                'trigger_rate_wpm': float,
                'trigger_n_peers': int,
                'last_unpause_ts': float,
                'trigger_streak_since': float,
                'recovery_streak_since': float,
            }
        Per-pool independent escalation lives in
        ``state['bev_pool_escalation']``: ``{pool_key: {level, last_unpause_ts}}``
        so a flaky pool that bounces between healthy and shaped
        accumulates escalation independently of the fleet level.
        """
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
        now = time.time()
        with self._lock:
            bp = dict(self.state.get('bev_pause') or {})
            pool_esc = dict(self.state.get('bev_pool_escalation') or {})
        rate, n_peers = self._fleet_bev_warn_rate(statuses)
        active = bool(bp.get('active'))

        if active:
            # Are we ready to clear?
            until = bp.get('until')
            until_ts = 0.0
            try:
                if until:
                    until_ts = _dt.fromisoformat(until).timestamp()
            except Exception:
                until_ts = 0.0
            cooldown_done = (until_ts and now >= until_ts)
            if rate <= BEV_OUTAGE_RECOVERY_WPM:
                rec_since = float(bp.get('recovery_streak_since') or 0.0)
                if rec_since <= 0:
                    bp['recovery_streak_since'] = now
                    rec_since = now
                quiet_age = now - rec_since
            else:
                bp['recovery_streak_since'] = 0.0
                quiet_age = 0.0
            quiet_unpause = (quiet_age >= BEV_OUTAGE_RECOVERY_PERSIST_S)
            if cooldown_done or quiet_unpause:
                # Record this pause event before clearing.
                ended = _dt.now(_tz.utc).isoformat()
                ev = {
                    'scope': bp.get('scope') or 'fleet',
                    'pool': bp.get('pool'),
                    'level': bp.get('level'),
                    'since': bp.get('since'),
                    'ended': ended,
                    'cooldown_s': bp.get('cooldown_s'),
                    'trigger_rate_wpm': bp.get('trigger_rate_wpm'),
                    'trigger_n_peers': bp.get('trigger_n_peers'),
                    'end_reason': ('cooldown' if cooldown_done
                                   else 'recovered'),
                    'final_rate_wpm': round(rate, 2),
                }
                self._record_bev_pause_event(ev)
                # If this was a pool pause, update its escalation
                # ledger so a re-trigger within 48 h escalates.
                if bp.get('scope') == 'pool' and bp.get('pool'):
                    pkey = bp['pool']
                    pool_esc[pkey] = {
                        'level': bp.get('level') or 1,
                        'last_unpause_ts': now,
                    }
                bp['active'] = False
                bp['last_unpause_ts'] = now
                bp['recovery_streak_since'] = 0.0
                bp['trigger_streak_since'] = 0.0
                bp['reason'] = ('cooldown expired' if cooldown_done
                                else f'recovered (rate={rate:.1f} wpm for '
                                     f'{int(quiet_age)}s)')
                with self._lock:
                    self.state['bev_pause'] = bp
                    self.state['bev_pool_escalation'] = pool_esc
                log.warning('bev_pause UNPAUSE (%s%s): %s',
                            bp.get('scope') or 'fleet',
                            (' ' + bp.get('pool')) if bp.get('pool') else '',
                            bp['reason'])
                try:
                    _emit_director_event(
                        f'BEV outage cleared ({bp.get("scope") or "fleet"}'
                        f'{(" " + bp.get("pool")) if bp.get("pool") else ""}): '
                        f'{bp["reason"]}',
                        peer='director', level='warning')
                except Exception:
                    pass
            else:
                with self._lock:
                    self.state['bev_pause'] = bp
            return

        # Not active: check trigger.
        if rate >= BEV_OUTAGE_TRIGGER_WPM and n_peers >= BEV_OUTAGE_MIN_PEERS:
            since = float(bp.get('trigger_streak_since') or 0.0)
            if since <= 0:
                bp['trigger_streak_since'] = now
                since = now
            streak = now - since
            if streak >= BEV_OUTAGE_TRIGGER_PERSIST_S:
                # Multi-pool classification: park N bad pools instead of
                # the whole fleet when the failure attributes to ≤ K /24s.
                # See _classify_outage_multi_pool docstring + the
                # 2026-05-28 post-mortem for the rationale (two equally
                # bad pools at ~50% each previously fell through to
                # fleet-scope pause).
                scope, mp_pools, _known_total = (
                    self._classify_outage_multi_pool(statuses))
                # Decide cooldown level per scope. For multi-pool we
                # escalate the MAX level across the affected pools so a
                # repeat offender pool keeps climbing (this matches the
                # historical single-pool behaviour).
                if scope == 'pools':
                    prev_levels = [
                        int((pool_esc.get(k) or {}).get('level') or 0)
                        for k, _, _ in mp_pools
                    ]
                    last_ups = [
                        float((pool_esc.get(k) or {}).get('last_unpause_ts') or 0.0)
                        for k, _, _ in mp_pools
                    ]
                    prev_level = max(prev_levels) if prev_levels else 0
                    last_up = max(last_ups) if last_ups else 0.0
                else:
                    last_up = float(bp.get('last_unpause_ts') or 0.0)
                    prev_level = int(bp.get('level') or 0)
                if last_up > 0 and (now - last_up) <= BEV_OUTAGE_RESET_S:
                    level = min(prev_level + 1, len(BEV_OUTAGE_LEVELS_S))
                else:
                    level = 1
                cooldown_s = BEV_OUTAGE_LEVELS_S[level - 1]
                until_dt = _dt.now(_tz.utc) + _td(seconds=cooldown_s)
                cooldown_iso = until_dt.isoformat()
                if scope == 'pools':
                    pool_strs = ', '.join(
                        f'{k}({s:.1f}wpm,n={n})' for k, s, n in mp_pools)
                    reason = (
                        f'pools {pool_strs} '
                        f'(fleet total {rate:.1f}) for {int(streak)}s '
                        f'→ parking peers in those /24s only '
                        f'(level {level}/{len(BEV_OUTAGE_LEVELS_S)} → '
                        f'{cooldown_s//3600}h cooldown)'
                    )
                else:
                    reason = (
                        f'fleet BEV rate {rate:.1f} wpm over '
                        f'{n_peers} peers for {int(streak)}s '
                        f'(level {level}/{len(BEV_OUTAGE_LEVELS_S)} → '
                        f'{cooldown_s//3600}h cooldown)'
                    )
                if scope == 'pools':
                    # NOTE: we DO NOT set bev_pause.active = True. The
                    # global pause was the previous behaviour and meant
                    # 90 peers got parked for a problem with 17. Instead
                    # we record each parked-pool event in the history
                    # ring so the dashboard / /process.txt audit trail
                    # still shows the action, and we extend each pool's
                    # escalation ledger for the per-pool unpause path.
                    bp['trigger_streak_since'] = 0.0
                    with self._lock:
                        self.state['bev_pause'] = bp
                    log.warning('bev_pool_park: %s', reason)
                    try:
                        _emit_director_event(
                            f'BEV outage → per-pool parks: {reason}',
                            peer='director', level='error')
                    except Exception:
                        pass
                    try:
                        for pkey, psum, _pn in mp_pools:
                            self._record_bev_pause_event({
                                'scope': 'pool',
                                'pool': pkey,
                                'level': level,
                                'since': _dt.now(_tz.utc).isoformat(),
                                'ended': None,
                                'cooldown_s': cooldown_s,
                                'trigger_rate_wpm': psum,
                                'trigger_n_peers': len(
                                    self._peers_in_pool(pkey, statuses)),
                                'end_reason': None,
                                'final_rate_wpm': None,
                            })
                            pool_esc[pkey] = {
                                'level': level,
                                'last_unpause_ts': now,
                            }
                        with self._lock:
                            self.state['bev_pool_escalation'] = pool_esc
                    except Exception:
                        log.exception('bev_pool_park: history append failed')
                    try:
                        cfg_changed = False
                        for pkey, _, _ in mp_pools:
                            targets = self._peers_in_pool(pkey, statuses)
                            for pid in targets:
                                with self._lock:
                                    live = get_peer_by_id(self.cfg, pid)
                                    if not live or not live.get('enabled', True):
                                        continue
                                    cur_nb = live.get('not_before')
                                    try:
                                        if cur_nb and _dt.fromisoformat(
                                                cur_nb) > until_dt:
                                            continue
                                    except Exception:
                                        pass
                                    live['not_before'] = cooldown_iso
                                    notes = live.setdefault('canary_notes', [])
                                    notes.append({
                                        'at': _dt.now(_tz.utc).strftime(
                                            '%Y-%m-%dT%H:%M:%SZ'),
                                        'event': 'bev_pool_park',
                                        'reasons': [reason],
                                        'cooldown_until': cooldown_iso,
                                        'pool': pkey,
                                        'level': level,
                                    })
                                    if len(notes) > 32:
                                        del notes[: len(notes) - 32]
                                    cfg_changed = True
                                    peer_url = live.get('url')
                                try:
                                    ps = (statuses or {}).get(pid) or {}
                                    if peer_url and (ps.get('state') or '') in (
                                            'running', 'processing'):
                                        stop_peer_processor(peer_url,
                                                            graceful=True)
                                except Exception:
                                    pass
                        if cfg_changed:
                            try:
                                save_peers_config(self.cfg)
                            except Exception:
                                log.exception(
                                    'bev_pool_park: save_peers_config failed')
                    except Exception:
                        log.exception('bev_pool_park: park fan-out failed')
                    return
                # Fleet scope: original full-fleet pause behaviour.
                bp = {
                    'active': True,
                    'scope': 'fleet',
                    'pool': None,
                    'level': level,
                    'since': _dt.now(_tz.utc).isoformat(),
                    'until': cooldown_iso,
                    'cooldown_s': cooldown_s,
                    'reason': reason,
                    'trigger_rate_wpm': round(rate, 2),
                    'trigger_n_peers': n_peers,
                    'last_unpause_ts': float(bp.get('last_unpause_ts') or 0),
                    'trigger_streak_since': 0.0,
                    'recovery_streak_since': 0.0,
                }
                with self._lock:
                    self.state['bev_pause'] = bp
                log.warning('bev_pause PAUSE (fleet): %s', reason)
                try:
                    _emit_director_event(
                        f'BEV outage detected (fleet): {reason}',
                        peer='director', level='error')
                except Exception:
                    pass
                try:
                    active_id = self.state.get('active_peer')
                    # Fleet scope: graceful-stop every non-frontier
                    # cache-only peer. Active frontier may be
                    # mid-finalization (Zenodo) — doesn't touch BEV.
                    for p in list(self.cfg.get('peers', [])):
                        if not p.get('enabled', True):
                            continue
                        if p['id'] == active_id:
                            continue
                        if _peer_is_scheduled(p):
                            continue
                        try:
                            ps = (statuses or {}).get(p['id']) or {}
                            if (ps.get('state') or '') in (
                                    'running', 'processing'):
                                stop_peer_processor(p.get('url'),
                                                    graceful=True)
                        except Exception:
                            pass
                except Exception:
                    log.exception('bev_pause: stop fan-out failed')
            else:
                with self._lock:
                    self.state['bev_pause'] = bp
        else:
            # Conditions not met — reset the trigger streak so a flicker
            # below threshold doesn't accumulate.
            if bp.get('trigger_streak_since'):
                bp['trigger_streak_since'] = 0.0
                with self._lock:
                    self.state['bev_pause'] = bp

    def is_bev_paused(self) -> bool:
        with self._lock:
            bp = self.state.get('bev_pause') or {}
        return bool(bp.get('active'))

    def clear_bev_pause(self, reason: str = 'manual clear') -> dict:
        from datetime import datetime as _dt, timezone as _tz
        """Manually clear an active BEV pause (and persist to disk).

        Records the event in ``bev_pause_history`` with
        ``end_reason='manual'`` so the audit trail shows it was
        operator-driven, not cooldown / recovery. Resets escalation
        ledger for pool pauses too so the next trigger starts at L1.

        Returns ``{cleared: bool, was_active: bool, scope, pool,
        level, since}`` for the response.
        """
        now = time.time()
        with self._lock:
            bp = dict(self.state.get('bev_pause') or {})
            pool_esc = dict(self.state.get('bev_pool_escalation') or {})
            was_active = bool(bp.get('active'))
            snap = {
                'scope': bp.get('scope') or 'fleet',
                'pool': bp.get('pool'),
                'level': bp.get('level'),
                'since': bp.get('since'),
            }
        if not was_active:
            return {'cleared': False, 'was_active': False, **snap}
        ended = _dt.now(_tz.utc).isoformat()
        ev = {
            'scope': snap['scope'],
            'pool': snap['pool'],
            'level': snap['level'],
            'since': snap['since'],
            'ended': ended,
            'cooldown_s': bp.get('cooldown_s'),
            'trigger_rate_wpm': bp.get('trigger_rate_wpm'),
            'trigger_n_peers': bp.get('trigger_n_peers'),
            'end_reason': 'manual',
            'final_rate_wpm': None,
            'manual_reason': reason,
        }
        self._record_bev_pause_event(ev)
        # Drop pool escalation for the affected pool so we don't punish
        # the next legitimate trigger with the prior level.
        if snap['scope'] == 'pool' and snap['pool']:
            pool_esc.pop(snap['pool'], None)
        bp['active'] = False
        bp['last_unpause_ts'] = now
        bp['recovery_streak_since'] = 0.0
        bp['trigger_streak_since'] = 0.0
        bp['reason'] = f'manual clear: {reason}'
        with self._lock:
            self.state['bev_pause'] = bp
            self.state['bev_pool_escalation'] = pool_esc
            try:
                save_director_state(self.state)
            except Exception:
                log.exception('clear_bev_pause: save_director_state failed')
        log.warning('bev_pause MANUAL CLEAR (%s%s): %s',
                    snap['scope'],
                    (' ' + snap['pool']) if snap['pool'] else '',
                    reason)
        try:
            _emit_director_event(
                f'BEV pause manually cleared ({snap["scope"]}'
                f'{(" " + snap["pool"]) if snap["pool"] else ""} '
                f'L{snap["level"]}): {reason}',
                peer='director', level='warning')
        except Exception:
            pass
        # Bust the status cache so the dashboard reflects the clear
        # on the very next poll rather than after the TTL.
        try:
            with self._status_cache_lock:
                self._status_cache_value = None
                self._status_cache_ts = 0.0
        except Exception:
            pass
        return {'cleared': True, 'was_active': True, **snap}

    # ------------------------------------------------------------------
    # Parked-pool BEV canary probes
    # ------------------------------------------------------------------
    def _parked_pools_from_peers(self, statuses: dict) -> dict:
        """Discover currently-parked /24 pools via `bev_pool_park` notes.

        Returns ``{pool_key: {peers: [pid, ...], cooldown_until: iso}}``
        keyed by ``outbound_24``. A pool is "parked" iff at least one
        enabled peer in it carries a `not_before` in the future AND its
        most-recent `canary_notes` entry has `event == 'bev_pool_park'`
        AND that note's `pool` matches the peer's current outbound_24.
        We restrict to bev_pool_park parks so we don't probe peers that
        are merely parked for steal / canary slowdown / role-eviction.
        """
        import datetime as _datetime
        out: dict = {}
        with self._lock:
            peers = list(self.cfg.get('peers', []))
        now = _datetime.datetime.now(_datetime.timezone.utc)
        for p in peers:
            pid = p.get('id')
            if not pid or not p.get('enabled', True):
                continue
            nb = p.get('not_before')
            if not nb:
                continue
            try:
                nb_dt = _datetime.datetime.fromisoformat(nb)
                if nb_dt <= now:
                    continue
            except Exception:
                continue
            notes = p.get('canary_notes') or []
            if not notes:
                continue
            last = notes[-1] if isinstance(notes[-1], dict) else None
            if not last or last.get('event') != 'bev_pool_park':
                continue
            pool = last.get('pool')
            if not pool:
                continue
            # Sanity: peer's *current* outbound_24 should still match
            # the parked pool (else we're chasing a stale park).
            ps = (statuses or {}).get(pid) or {}
            cur24 = (((ps.get('system') or {}).get('host') or {})
                     .get('outbound_24') or '')
            if cur24 and cur24 != pool:
                continue
            ent = out.setdefault(pool, {'peers': [], 'cooldown_until': nb})
            ent['peers'].append(pid)
            # Keep the latest cooldown_until for visibility.
            try:
                if _datetime.datetime.fromisoformat(
                        ent['cooldown_until']) < nb_dt:
                    ent['cooldown_until'] = nb
            except Exception:
                pass
        return out

    def _canary_probe_parked_pools(self, statuses: dict) -> None:
        """Probe each parked /24 pool's reachability to BEV.

        For every pool currently parked by `bev_pool_park`:
          1. Skip if we probed it within ``BEV_POOL_PROBE_INTERVAL_S``.
          2. Pick one peer in the pool (prefers a peer that is
             reachable in the latest statuses snapshot).
          3. Call ``GET /api/v1/bev_probe`` on that peer.
          4. On success (ok=true): clear ``not_before`` on every peer
             in the /24, drop ``bev_pool_escalation[pool]``, append a
             ``bev_pool_unpark`` event to ``bev_pause_history`` for
             the audit trail, emit a director event.
          5. On failure: just record the probe time + reason; the
             cooldown stays in place.

        Probes run sequentially per tick (typically 0–3 parked pools,
        each probe ≤ ~8 s peer-side) which keeps the director tick
        bounded. State persisted under ``state['bev_pool_probes']``:
        ``{pool_key: {last_ts, ok, http_code, peer_id, latency_s,
        error}}``.
        """
        try:
            parked = self._parked_pools_from_peers(statuses or {})
        except Exception:
            log.exception('bev_pool_probe: discover parked pools failed')
            return
        if not parked:
            return
        now = time.time()
        with self._lock:
            probes = dict(self.state.get('bev_pool_probes') or {})
        for pool, info in parked.items():
            last = probes.get(pool) or {}
            last_ts = float(last.get('last_ts') or 0.0)
            if now - last_ts < BEV_POOL_PROBE_INTERVAL_S:
                continue
            # Pick a target peer: prefer one we have a recent status
            # for (it's at least answering admin polls); fall back to
            # any peer in the pool.
            candidates = list(info.get('peers') or [])
            if not candidates:
                continue
            reachable = [pid for pid in candidates
                         if isinstance((statuses or {}).get(pid), dict)
                         and (statuses[pid].get('state') or '')
                         not in ('', 'unreachable')]
            target = (reachable or candidates)[0]
            with self._lock:
                live = get_peer_by_id(self.cfg, target)
            url = (live or {}).get('url')
            if not url:
                # Primary is not parked under bev_pool_park (it's the
                # director host), but be defensive.
                probes[pool] = {
                    'last_ts': now, 'ok': False, 'error': 'no_url',
                    'peer_id': target,
                }
                continue
            probe_url = url.rstrip('/') + '/api/v1/bev_probe'
            try:
                import requests as _req
                r = _req.get(probe_url,
                             timeout=BEV_POOL_PROBE_REQ_TIMEOUT_S,
                             headers=_admin_headers())
                if r.status_code != 200:
                    probes[pool] = {
                        'last_ts': now, 'ok': False,
                        'http_code': r.status_code, 'peer_id': target,
                        'error': f'HTTP {r.status_code}',
                    }
                    continue
                pj = r.json() or {}
            except Exception as e:
                probes[pool] = {
                    'last_ts': now, 'ok': False, 'peer_id': target,
                    'error': f'request: {e}',
                }
                continue
            probe_ok = bool(pj.get('ok'))
            probes[pool] = {
                'last_ts': now,
                'ok': probe_ok,
                'http_code': pj.get('http_code'),
                'magic_ok': pj.get('magic_ok'),
                'latency_s': pj.get('latency_s'),
                'peer_id': target,
                'error': pj.get('error'),
            }
            if not probe_ok:
                log.info('bev_pool_probe: pool=%s peer=%s still bad '
                         '(code=%s magic=%s err=%s)',
                         pool, target, pj.get('http_code'),
                         pj.get('magic_ok'), pj.get('error'))
                continue
            # Probe succeeded → unpark every peer in the /24.
            self._unpark_pool_after_probe(pool, info, target, pj)
        with self._lock:
            # Prune entries for pools no longer parked so the dict
            # doesn't grow unbounded across days.
            still_parked = set(parked.keys())
            keep = {k: v for k, v in probes.items() if k in still_parked}
            self.state['bev_pool_probes'] = keep

    def _unpark_pool_after_probe(self, pool: str, info: dict,
                                 probed_peer: str, probe_resp: dict) -> None:
        """Clear ``not_before`` on every peer in ``pool`` after a
        successful canary probe.

        Only clears not_before stamps tagged by a ``bev_pool_park``
        canary_note (so we never accidentally release a primary stamp,
        a steal_park, a role_park, an auto_park, or a manual park).
        Drops the pool's escalation entry so the next legit trigger
        starts at level 1, appends a ``bev_pool_unpark`` event to
        ``bev_pause_history`` for the dashboard audit ring, emits a
        director event, busts the status cache.
        """
        from datetime import datetime as _dt, timezone as _tz2
        cleared_ids: list[str] = []
        with self._lock:
            pool_esc = dict(self.state.get('bev_pool_escalation') or {})
            cfg_changed = False
            for p in list(self.cfg.get('peers', [])):
                if (p.get('canary_notes') or []) == []:
                    continue
                last = p.get('canary_notes')[-1] if isinstance(
                    p.get('canary_notes')[-1], dict) else None
                if not last or last.get('event') != 'bev_pool_park':
                    continue
                if last.get('pool') != pool:
                    continue
                if not p.get('not_before'):
                    continue
                # Mark as released early.
                p['not_before'] = None
                cleared_ids.append(p.get('id'))
                notes = p.setdefault('canary_notes', [])
                notes.append({
                    'at': _dt.now(_tz2.utc).strftime(
                        '%Y-%m-%dT%H:%M:%SZ'),
                    'event': 'bev_pool_unpark',
                    'reasons': [
                        f'canary probe ok via {probed_peer} '
                        f'(code={probe_resp.get("http_code")} '
                        f'lat={probe_resp.get("latency_s")}s)'
                    ],
                    'pool': pool,
                })
                if len(notes) > 32:
                    del notes[: len(notes) - 32]
                cfg_changed = True
            popped = pool_esc.pop(pool, None)
            self.state['bev_pool_escalation'] = pool_esc
            if cfg_changed:
                try:
                    save_peers_config(self.cfg)
                except Exception:
                    log.exception('bev_pool_unpark: save_peers_config failed')
        # Audit event.
        try:
            self._record_bev_pause_event({
                'scope': 'pool',
                'pool': pool,
                'level': (popped or {}).get('level'),
                'since': None,
                'ended': _dt.now(_tz2.utc).isoformat(),
                'cooldown_s': None,
                'trigger_rate_wpm': None,
                'trigger_n_peers': len(cleared_ids),
                'end_reason': 'canary_probe',
                'final_rate_wpm': None,
                'probed_peer': probed_peer,
                'probe_latency_s': probe_resp.get('latency_s'),
            })
        except Exception:
            log.exception('bev_pool_unpark: history append failed')
        try:
            _emit_director_event(
                f'BEV pool {pool} cleared early by canary probe '
                f'(via {probed_peer}, {len(cleared_ids)} peers released)',
                peer='director', level='warning')
        except Exception:
            pass
        log.warning('bev_pool_unpark: pool=%s peers=%s via probe peer=%s',
                    pool, cleared_ids, probed_peer)
        # Bust the status cache so dashboard reflects the unpark
        # without waiting for TTL.
        try:
            with self._status_cache_lock:
                self._status_cache_value = None
                self._status_cache_ts = 0.0
        except Exception:
            pass

    # ------------------------------------------------------------------
    # IP-pool egress block detection
    # ------------------------------------------------------------------
    def _compute_ip_pools(self, statuses: dict) -> dict:
        """Aggregate per-peer 5m BEV warn rates + steal by outbound /24.

        Returns a dict keyed by ``outbound_24`` (e.g. ``'1.2.3.0/24'``)
        with ``{peers, n, avg_wpm, median_wpm, max_wpm, steal_med}``.
        Peers without a known outbound_ip land in pool ``'?'``.

        We carry steal here too so the IP-pool view in /process.txt
        gives one picture of "is this /24 shaped" *and* "is this /24
        on a contended hypervisor pool". Steal is taken from the
        fleet-wide ``peer_steal_ema`` so it matches the same source
        used by ``_check_steal_health`` and ``fleet_cpu``.
        """
        steal_ema = self.state.get('peer_steal_ema') or {}
        # Build peer→park_reason map so per-pool counters can attribute
        # parks to their underlying cause (bev_pool / steal / cap /
        # role / other). Single pass over cfg avoids O(N²) lookups.
        from datetime import datetime as _dt, timezone as _tz
        _now = _dt.now(_tz.utc)
        peer_park = {}  # pid -> reason key or None
        for _p in (self.cfg.get('peers') or []):
            _nb = _p.get('not_before')
            if not _nb:
                continue
            try:
                _nb_dt = _dt.fromisoformat(str(_nb).replace('Z', '+00:00'))
            except Exception:
                continue
            if _nb_dt <= _now:
                continue
            # Latest canary_note tells us why.
            _reason = 'other'
            _notes = _p.get('canary_notes') or []
            if _notes and isinstance(_notes[-1], dict):
                ev = (_notes[-1].get('event') or '').lower()
                if ev in ('bev_pool_park', 'ip_pool_park'):
                    _reason = 'bev_pool'
                elif ev == 'steal_park':
                    _reason = 'steal'
                elif ev in ('auto_park', 'park_until_renewal'):
                    _reason = 'cap'
                elif ev == 'role_park':
                    _reason = 'role'
                else:
                    _reason = ev or 'other'
            peer_park[_p['id']] = _reason
        pools: dict = {}
        for pid, ps in (statuses or {}).items():
            if not isinstance(ps, dict):
                continue
            sys = ps.get('system') or {}
            host = sys.get('host') or {}
            key = host.get('outbound_24') or '?'
            wr = ((ps.get('warning_rates') or {}).get('bev') or {})
            r = float(wr.get('5m') or 0.0)
            pool = pools.setdefault(key, {
                'peers': [], 'rates': [], 'sum_wpm': 0.0, 'steal': [],
                'parked': 0, 'parked_by_reason': {},
            })
            pool['peers'].append(pid)
            pool['rates'].append(r)
            pool['sum_wpm'] += r
            _s = steal_ema.get(pid)
            if isinstance(_s, (int, float)):
                pool['steal'].append(float(_s))
            _pr = peer_park.get(pid)
            if _pr:
                pool['parked'] += 1
                pool['parked_by_reason'][_pr] = (
                    pool['parked_by_reason'].get(_pr, 0) + 1)
        # Finalize stats.
        def _med(xs):
            n = len(xs)
            if not n:
                return 0.0
            s = sorted(xs)
            return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2
        out: dict = {}
        for key, pool in pools.items():
            n = len(pool['peers'])
            avg = (pool['sum_wpm'] / n) if n else 0.0
            mx = max(pool['rates']) if pool['rates'] else 0.0
            steal_max = max(pool['steal']) if pool['steal'] else None
            out[key] = {
                'peers': pool['peers'],
                'n': n,
                'avg_wpm': round(avg, 3),
                'median_wpm': round(_med(pool['rates']), 3),
                'max_wpm': round(mx, 3),
                'steal_med': (round(_med(pool['steal']), 1)
                              if pool['steal'] else None),
                'steal_max': (round(steal_max, 1)
                              if steal_max is not None else None),
                'steal_n': len(pool['steal']),
                'parked': pool['parked'],
                'parked_by_reason': pool['parked_by_reason'],
            }
        return out

    def _sample_ip_pool_history(self, pools: dict) -> None:
        """Append a 24h-ring sample of per-pool BEV rates + steal.

        Stored as state['ip_pools_history'] = list of
        ``{ts, pools: {key: {avg_wpm, n, steal_med}}}``.
        Throttled to once every POOL_HISTORY_SAMPLE_INTERVAL_S.
        """
        now = time.time()
        with self._lock:
            hist = list(self.state.get('ip_pools_history') or [])
            last_ts = float(hist[-1]['ts']) if hist else 0.0
            if (now - last_ts) < POOL_HISTORY_SAMPLE_INTERVAL_S:
                return
            slim = {}
            for k, v in (pools or {}).items():
                if k == '?':
                    continue
                slim[k] = {
                    'avg_wpm': v.get('avg_wpm'),
                    'max_wpm': v.get('max_wpm'),
                    'n': v.get('n'),
                    'steal_med': v.get('steal_med'),
                    'steal_max': v.get('steal_max'),
                    'parked': v.get('parked', 0),
                }
            hist.append({'ts': int(now), 'pools': slim})
            if len(hist) > POOL_HISTORY_MAX:
                hist = hist[-POOL_HISTORY_MAX:]
            self.state['ip_pools_history'] = hist

    def _check_ip_pool_block(self, statuses: dict) -> None:
        """Park peers in an IP pool that shows a disproportionate BEV warn rate."""
        from datetime import datetime as _dt, timedelta as _td, timezone as _tz
        pools = self._compute_ip_pools(statuses)
        with self._lock:
            self.state['ip_pools'] = pools
        # Persistent 24h ring — sampled here so we get one consistent
        # snapshot per director tick rather than racing two callers.
        try:
            self._sample_ip_pool_history(pools)
        except Exception:
            log.exception('ip_pools_history sample failed')
        # Drop the unknown bucket from the analysis — we can't park what
        # we can't identify, and the '?' pool mixes peers across actual
        # pools so its rate is meaningless.
        scored = {k: v for k, v in pools.items()
                  if k != '?' and v['n'] >= IP_POOL_MIN_PEERS}
        if len(scored) < 2:
            return  # need at least two pools to compare
        all_avgs = sorted(v['avg_wpm'] for v in scored.values())
        nn = len(all_avgs)
        fleet_med = (all_avgs[nn // 2] if nn % 2
                     else (all_avgs[nn // 2 - 1] + all_avgs[nn // 2]) / 2)
        offenders = []
        for key, v in scored.items():
            if v['avg_wpm'] < IP_POOL_MIN_WPM:
                continue
            if v['avg_wpm'] >= max(fleet_med * IP_POOL_BIAS, IP_POOL_MIN_WPM):
                offenders.append((key, v))
        if not offenders:
            return
        cfg_changed = False
        cooldown = _dt.now(_tz.utc) + _td(seconds=IP_POOL_PARK_COOLDOWN_S)
        for key, v in offenders:
            reason = (f'IP pool {key} BEV rate {v["avg_wpm"]:.1f}/min '
                      f'(fleet median {fleet_med:.1f}, n={v["n"]} peers)')
            log.warning('ip-pool-park: %s', reason)
            try:
                _emit_director_event(
                    f'IP pool block suspected: {reason} — parking '
                    f'{len(v["peers"])} peer(s) for '
                    f'{IP_POOL_PARK_COOLDOWN_S // 60} min',
                    peer='director', level='warning')
            except Exception:
                pass
            for pid in v['peers']:
                with self._lock:
                    live = get_peer_by_id(self.cfg, pid)
                    if not live or not live.get('enabled', True):
                        continue
                    if _peer_is_scheduled(live):
                        continue
                    live['not_before'] = cooldown.isoformat()
                    notes = live.setdefault('canary_notes', [])
                    notes.append({
                        'at': _dt.now(_tz.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'event': 'ip_pool_park',
                        'reasons': [reason],
                        'cooldown_until': cooldown.isoformat(),
                        'pool': key,
                    })
                    if len(notes) > 32:
                        del notes[: len(notes) - 32]
                    cfg_changed = True
                    peer_url = live.get('url')
                try:
                    if peer_url:
                        stop_peer_processor(peer_url, graceful=True)
                except Exception as e:
                    log.warning('ip-pool-park %s: graceful stop failed: %s',
                                pid, e)
        if cfg_changed:
            try:
                save_peers_config(self.cfg)
            except Exception:
                log.exception('ip-pool-park: save_peers_config failed')

    def _check_and_switch(self):
        """Check if we need to switch the active peer."""
        with self._lock:
            mode = read_director_mode(self.state.get('mode', 'auto'))
            self.state['mode'] = mode
            if mode == 'paused':
                return
            if mode == 'manual':
                # In manual mode, just ensure the designated peer is running
                active_id = self.state.get('active_peer')
                if active_id:
                    peer = get_peer_by_id(self.cfg, active_id)
                    if peer:
                        status = get_peer_status(peer.get('url'))
                        if status.get('state') in ('idle', 'stopped', 'unknown'):
                            excl = _excluded_kgs(self.cfg, exclude_peer_id=active_id)
                            log.info('Manual mode: starting processor on %s%s', active_id,
                                     (' — excluding ' + ','.join(sorted(excl))) if excl else '')
                            start_peer_processor(peer.get('url'), exclude_kgs=excl)
                return

            # Auto mode — check bandwidth and switch if needed
            active_id = self.state.get('active_peer')
            cfg = self.cfg.copy()
            state_copy = self.state.copy()


        # Pre-empt the active peer when a different peer's reservation
        # becomes ready (cooldown lifted, KG still pending). The held KG
        # has tile checkpoints on the holder, so it finishes much faster
        # there than starting fresh on the substitute. We only pre-empt
        # between KGs (never mid-KG) by waiting for idle/stopped state.
        if active_id:
            holder = _ready_reservation_holder(cfg, state_copy)
            if holder and holder != active_id:
                active_peer_cfg = get_peer_by_id(cfg, active_id)
                ps = get_peer_status(active_peer_cfg.get('url')) if active_peer_cfg else {}
                proc_state = ps.get('state', 'unknown')
                if proc_state in ('idle', 'stopped'):
                    log.info('Reservation ready on %s — pre-empting %s (between KGs)',
                             holder, active_id)
                    with self._lock:
                        # Clear graceful-stop flag before nilling active_id
                        self.state.get('graceful_stop_sent', {}).pop(active_id, None)
                        self.state['active_peer'] = None
                        active_id = None
                else:
                    # Mid-KG: send graceful stop once. The flag prevents
                    # us from re-sending it on every tick.
                    sent = self.state.get('graceful_stop_sent', {})
                    if sent.get(active_id) != holder:
                        log.info('Reservation ready on %s; sending graceful stop to %s (will exit after current KG)',
                                 holder, active_id)
                        try:
                            stop_peer_processor(active_peer_cfg.get('url'), graceful=True)
                            with self._lock:
                                sent[active_id] = holder
                                self.state['graceful_stop_sent'] = sent
                        except Exception as e:
                            log.warning('Graceful stop on %s failed: %s', active_id, e)
                    else:
                        log.info('Reservation ready on %s; %s still finishing current KG (graceful stop pending)',
                                 holder, active_id)

        # Check if active peer is scheduled (not_before in the future)
        if active_id:
            active_peer_cfg = get_peer_by_id(cfg, active_id)
            if active_peer_cfg and _peer_is_scheduled(active_peer_cfg):
                nb = active_peer_cfg.get('not_before', '?')
                log.info('Peer %s is scheduled not before %s, switching', active_id, nb)
                peer = get_peer_by_id(cfg, active_id)
                if peer:
                    res = safely_stop_peer(peer.get('url'), active_id)
                    if 'error' in res:
                        log.error('Cannot deactivate %s: %s — holding off switch',
                                  active_id, res['error'])
                        return  # leave active_peer as is; retry next tick
                with self._lock:
                    self.state['active_peer'] = None
                    active_id = None

        # Check if active peer is near / over its budget.
        #
        # Three regimes (mirror the canary-park flow so peers waiting
        # for renewal land back in rotation automatically):
        #
        #   remaining < HARD_DEPLETED_GB — rotate even mid-KG; park
        #     until next renewal so the scheduler skips the peer.
        #   remaining < LOW_WATER_GB & mid-KG — send graceful stop so
        #     the in-flight KG finishes and uploads, then the peer
        #     auto-parks on idle (handled in the idle/stopped branch
        #     further down via _peer_is_scheduled).
        #   remaining < LOW_WATER_GB & idle — park (no work to finish).
        #
        # Evidence-only invariant (see _peer_bw_depleted / AGENTS.md):
        # "remaining" is measured against the peer's *observed_cap_gb*
        # — the only evidence-based wall — NOT the nominal budget_gb
        # guess. Peers without an observed cap are never parked here;
        # the canary slowdown detector catches their real wall. The
        # previous nominal-budget gate parked every freshly-promoted
        # frontier whose vnstat cycle exceeded 95 GB, causing constant
        # frontier reassignment churn (Jul 2026 incident: 19 peers
        # parked weeks-long with "-478 GB remaining").
        if active_id:
            bw = state_copy.get('peer_bandwidth', {}).get(active_id, {})
            used = bw.get('used_bytes', 0)
            active_peer_cfg = get_peer_by_id(cfg, active_id) or {}
            cap_gb = active_peer_cfg.get('observed_cap_gb')
            if isinstance(cap_gb, (int, float)) and cap_gb > 0:
                remaining_gb = float(cap_gb) - used / (1024 ** 3)
            else:
                remaining_gb = float('inf')  # no evidence → no park

            if remaining_gb < BANDWIDTH_LOW_WATER_GB:
                peer = get_peer_by_id(cfg, active_id)
                if peer:
                    next_renew = _peer_next_renew(peer, cfg)
                    proc_state_now = (state_copy.get('peer_status_cache') or {}).get(active_id, {}).get('state')
                    if not proc_state_now:
                        try:
                            proc_state_now = get_peer_status(peer.get('url'), active_id).get('state', 'unknown')
                        except Exception:
                            proc_state_now = 'unknown'
                    mid_kg = proc_state_now == 'running'
                    hard = remaining_gb < BANDWIDTH_HARD_DEPLETED_GB
                    self._park_peer_until_renewal(
                        peer, next_renew, remaining_gb=remaining_gb,
                        mid_kg=mid_kg, hard=hard,
                    )
                    if hard or not mid_kg:
                        # Rotate now: stop verified, park written.
                        with self._lock:
                            if self.state.get('active_peer') == active_id:
                                self.state['active_peer'] = None
                            active_id = None
                    else:
                        # Mid-KG with low-water: graceful stop has
                        # been sent; let the peer keep ticking until
                        # it finishes the KG and reports idle. The
                        # not_before park is already written, so when
                        # the idle/stopped branch fires it'll see
                        # _peer_is_scheduled and demote cleanly.
                        return

        # Check if active peer's processor has stopped unexpectedly
        if active_id:
            peer = get_peer_by_id(cfg, active_id)
            if peer:
                status = get_peer_status(peer.get('url'))
                proc_state = status.get('state', 'unknown')

                # --- Zenodo pause detection ----------------------------
                # paused_zenodo with global=true (rate_limit/auth on the
                # token) blocks every peer using the same token, so we
                # leave the peer paused (probing itself) and don't switch.
                # Non-global (network) is a peer-local issue; cool that
                # peer down for a bit and switch to another peer.
                zinfo = status.get('zenodo_pause') or {}
                # Only act when the parent loop has truly entered the
                # paused_zenodo state (writes that state to progress.json).
                # Mid-KG the subprocess may have written the pause file
                # but the parent is still building light GPKG / JSON; we
                # don't want to abort that work.
                if proc_state == 'paused_zenodo':
                    is_global = bool(zinfo.get('global'))
                    reason = zinfo.get('reason', 'network')
                    if is_global:
                        log.info('Active peer %s: Zenodo paused globally (reason=%s) \u2014 holding all peers',
                                 active_id, reason)
                        # Don't switch \u2014 every peer would hit the same
                        # token-wide issue.  The peer probes Zenodo itself.
                        return
                    # Hold-tendency escalation: peers that repeatedly trip
                    # the Zenodo network cooldown are likely on a flaky
                    # route. Track recent cooldown timestamps and double
                    # the duration on each repeat within the window so we
                    # don't burn the whole day cycling them in/out.
                    now = datetime.now(timezone.utc)
                    cutoff = now - timedelta(hours=HOLD_TENDENCY_WINDOW_HOURS)
                    history = []
                    for p in self.cfg.get('peers', []):
                        if p['id'] == active_id:
                            raw = p.get('zenodo_cooldown_history') or []
                            for ts in raw:
                                try:
                                    t = datetime.fromisoformat(ts)
                                    if t.tzinfo is None:
                                        t = t.replace(tzinfo=timezone.utc)
                                except Exception:
                                    continue
                                if t >= cutoff:
                                    history.append(t)
                            break
                    repeat_count = len(history) + 1  # this incident
                    cd_min = min(
                        ZENODO_NETWORK_COOLDOWN_MIN * (HOLD_TENDENCY_FACTOR ** (repeat_count - 1)),
                        HOLD_TENDENCY_MAX_MIN,
                    )
                    # Honour Retry-After if Zenodo provided one. We use
                    # the larger of our escalated cooldown vs the server's
                    # ask, capped at the hold-tendency ceiling. This is
                    # the cooperative thing to do: the server told us
                    # exactly when to come back, so don't come back
                    # earlier just because our own escalation said so.
                    try:
                        ra_secs = float(zinfo.get('retry_after', 0) or 0)
                    except Exception:
                        ra_secs = 0.0
                    if ra_secs > 0:
                        cd_min = min(
                            max(cd_min, ra_secs / 60.0),
                            HOLD_TENDENCY_MAX_MIN,
                        )
                    if repeat_count > 1:
                        log.warning('Active peer %s: Zenodo network failure '
                                    '(repeat #%d in %dh, retry_after=%.0fs) \u2014 cooling down '
                                    '%.1f min (escalated) and switching',
                                    active_id, repeat_count,
                                    HOLD_TENDENCY_WINDOW_HOURS, ra_secs, cd_min)
                    else:
                        log.warning('Active peer %s: Zenodo network failure (retry_after=%.0fs) \u2014 cooling down %.1f min and switching',
                                    active_id, ra_secs, cd_min)
                    # Apply not_before cooldown so choose_active_peer skips this peer.
                    # Also reserve the in-progress KG so substitute peers skip
                    # it — the cooled peer keeps its tile checkpoints and
                    # will finish quickly when the cooldown lifts.
                    cd_until = now + timedelta(minutes=cd_min)
                    cur_kg = (status.get('current_kg') or {}).get('code')
                    # On escalation (repeat #2+) release the held KG so
                    # another peer can pick it up — the offender clearly
                    # can't reach Zenodo and shouldn't block progress.
                    # First trip: keep the reservation so the peer can
                    # resume from its own tile checkpoints when cooldown
                    # lifts.
                    release_hold = repeat_count > 1
                    for p in self.cfg.get('peers', []):
                        if p['id'] == active_id:
                            p['not_before'] = cd_until.isoformat()
                            if release_hold:
                                released = p.pop('reserved_kg', None)
                                if released or cur_kg:
                                    log.warning('Releasing held KG %s from %s (escalated cooldown)',
                                                released or cur_kg, active_id)
                            elif cur_kg:
                                p['reserved_kg'] = str(cur_kg)
                            # Record this incident; prune older than window.
                            history.append(now)
                            p['zenodo_cooldown_history'] = [
                                t.isoformat() for t in history
                            ]
                            break
                    save_peers_config(self.cfg)
                    # Stop processor on this peer (still uploading retries)
                    res = safely_stop_peer(peer.get('url'), active_id)
                    if 'error' in res:
                        log.error('Cannot stop %s during Zenodo failover: %s',
                                  active_id, res['error'])
                        return
                    with self._lock:
                        self.state['active_peer'] = None
                        active_id = None
                # ------------------------------------------------------
                # 'complete' is the processor's clean terminal state when
                # it exhausts its pending list (every other idle gate in
                # this file already treats it as idle). It was missing
                # here, so an active frontier that ran out of work — or
                # exited via the disk-pressure pause path — sat as the
                # designated active peer with its credentials held and
                # was never restarted (at123: 'complete' since 2026-09-14,
                # 5 days of the primary frontier slot idle).
                if active_id and proc_state in ('idle', 'stopped', 'complete'):
                    # Processor stopped (finished a KG or was stopped externally)
                    # Check if it should continue
                    bw = state_copy.get('peer_bandwidth', {}).get(active_id, {})
                    # Canary-by-default: only block continuation when we
                    # have *evidence* (observed_cap_gb) of depletion.
                    # Without evidence we let the peer keep working;
                    # _check_and_switch will catch a real wall via the
                    # canary slowdown detector.
                    bw_depleted = _peer_bw_depleted(
                        peer, bw, cfg,
                        headroom_gb=float(BANDWIDTH_HARD_DEPLETED_GB))
                    # Honour not_before cooldown — if scheduled, demote
                    # the active peer so a different one can be picked.
                    if _peer_is_scheduled(peer):
                        log.info('Active peer %s is scheduled (not_before=%s) \u2014 letting director pick another',
                                 active_id, peer.get('not_before'))
                        with self._lock:
                            self.state['active_peer'] = None
                            active_id = None
                    elif not bw_depleted:
                        excl = _excluded_kgs(cfg, exclude_peer_id=active_id)
                        # Re-issue with the current cred/strip plan so
                        # the active peer stays pinned to its slice.
                        plan_cred = (self.state.get('frontier_cred_plan') or {}).get(active_id)
                        plan_strip = (self.state.get('frontier_strip_plan') or {}).get(active_id)
                        if not plan_cred:
                            self._refresh_peer_caps(peer)
                            plan_cred = (self._assign_cred_indices([active_id], cfg)
                                          .get(active_id))
                        if not plan_strip:
                            strips = self._frontier_cells()
                            # No prior plan: give all strips to this
                            # lone frontier; orchestrator will trim later.
                            plan_strip = [list(s) for s in strips] if strips else None
                        # Throttle: don't fire restart more than once per
                        # FRONTIER_RESTART_COOLDOWN_S unless the cred/strip
                        # plan or the exclude set actually changed. Without
                        # this we issue ~8 restarts/hour just because the
                        # processor reports idle between KGs (each KG ends
                        # in a clean subprocess exit).
                        plan_fp = [
                            sorted(int(i) for i in (plan_cred or [])),
                            [list(s) for s in (plan_strip or [])],
                            sorted(excl),
                        ]
                        last_restart = self.state.setdefault(
                            '_frontier_restart_log', {})
                        last_entry = last_restart.get(active_id) or {}
                        last_ts = float(last_entry.get('ts') or 0)
                        last_fp = last_entry.get('fp')
                        now_ts = time.time()
                        cooldown = float(
                            cfg.get('frontier_restart_cooldown_s',
                                     FRONTIER_RESTART_COOLDOWN_S))
                        plan_unchanged = (last_fp == plan_fp)
                        if plan_unchanged and (now_ts - last_ts) < cooldown:
                            log.debug(
                                'Skip restart on %s: plan unchanged and '
                                'within cooldown (%.0fs/%.0fs)',
                                active_id, now_ts - last_ts, cooldown)
                        else:
                            log.info(
                                'Restarting processor on %s (%.1f GB remaining)%s creds=%s strip=%s',
                                active_id, remaining_gb,
                                (' — excluding ' + ','.join(sorted(excl))) if excl else '',
                                plan_cred, plan_strip,
                            )
                            start_peer_processor(
                                peer.get('url'), exclude_kgs=excl,
                                cred_indices=plan_cred,
                                lat_strips=plan_strip,
                            )
                            last_restart[active_id] = {
                                'ts': now_ts, 'fp': plan_fp,
                            }
                    else:
                        log.info('Peer %s depleted, finding next peer', active_id)
                        with self._lock:
                            self.state['active_peer'] = None
                            active_id = None
                elif proc_state == 'unreachable':
                    # Don't fail over on a single timeout — heavy GPKG builds
                    # can briefly starve gunicorn. Require N consecutive misses.
                    with self._lock:
                        misses = self.state.get('unreachable_count', {})
                        misses[active_id] = misses.get(active_id, 0) + 1
                        self.state['unreachable_count'] = misses
                        n = misses[active_id]
                    if n >= UNREACHABLE_FAILOVER_THRESHOLD:
                        log.warning('Active peer %s unreachable %d times — failing over',
                                    active_id, n)
                        with self._lock:
                            self.state['unreachable_count'].pop(active_id, None)
                            self.state['active_peer'] = None
                            active_id = None
                    else:
                        log.info('Active peer %s unreachable (%d/%d) — waiting',
                                 active_id, n, UNREACHABLE_FAILOVER_THRESHOLD)
                else:
                    # Reachable — clear miss counter
                    with self._lock:
                        self.state.get('unreachable_count', {}).pop(active_id, None)

        # Enforce single-active for the FRONTIER role: stop any non-active
        # peers that are running a non-cache-only processor UNLESS the
        # director has authorised them as a parallel frontier (in
        # ``parallel_frontiers_active`` or as a key of
        # ``frontier_cred_plan``) AND the cred pool supports it.
        # Cache-only peers (no Copernicus credentials) may run in parallel —
        # they're managed by ``_orchestrate_cache_only``.
        #
        # Why both sets? ``parallel_frontiers_active`` is a current-tick
        # observation rebuilt by ``_orchestrate_parallel_frontiers``,
        # which only runs *after* this guard. ``frontier_cred_plan`` is
        # the director's persisted authorisation map. After a srv
        # restart the new worker has not yet ticked the parallel orch,
        # so trusting only ``parallel_frontiers_active`` would hard-stop
        # any inherited parallel frontier (the 2026-05-06 cascade).
        # Mirrors the same union used by ``_orchestrate_cache_only``.
        cred_plan = self.state.get('frontier_cred_plan') or {}
        strip_plan = self.state.get('frontier_strip_plan') or {}
        parallel_ok = set(self.state.get('parallel_frontiers_active') or [])
        parallel_ok |= set(cred_plan.keys())
        # Active peer is implicitly authorised — belt and braces in
        # case the plan map drifted out from under us.
        if active_id:
            parallel_ok.add(active_id)
        if active_id:
            for p in cfg.get('peers', []):
                if p['id'] != active_id and p.get('url') is not None:
                    ps = get_peer_status(p.get('url'))
                    if ps.get('state') in ('running', 'processing'):
                        if ps.get('cache_only'):
                            continue  # benign — doesn't touch credentials
                        if p['id'] in parallel_ok:
                            continue  # authorised parallel frontier
                        log.warning(
                            'Non-active peer %s is running frontier work — '
                            'stopping it (active=%s, '
                            'parallel_frontiers_active=%s, '
                            'cred_plan_keys=%s, strip_plan_keys=%s)',
                            p['id'], active_id,
                            sorted(self.state.get(
                                'parallel_frontiers_active') or []),
                            sorted(cred_plan.keys()),
                            sorted(strip_plan.keys()),
                        )
                        safely_stop_peer(p.get('url'), p['id'])

        # If no active peer, choose one
        if not active_id:
            with self._lock:
                new_peer = choose_active_peer(self.cfg, self.state)
            if new_peer:
                peer = get_peer_by_id(cfg, new_peer)
                if peer:
                    # CRITICAL: verify no other peer is running FRONTIER
                    # work before activating the new one.  Cache-only
                    # peers don't touch Copernicus credentials and may
                    # remain running in parallel.
                    blocked = False
                    # Same union as the single-active guard above:
                    # trust the persisted cred_plan keys, not just the
                    # current-tick observation set.
                    parallel_ok = set(
                        self.state.get('parallel_frontiers_active') or [])
                    parallel_ok |= set(
                        (self.state.get('frontier_cred_plan') or {}).keys())
                    for p in cfg.get('peers', []):
                        if p['id'] == new_peer:
                            continue
                        ps = get_peer_status(p.get('url'))
                        st = ps.get('state', 'unknown')
                        if st in ('running', 'processing') and not ps.get('cache_only'):
                            if p['id'] in parallel_ok:
                                continue
                            log.info('Stopping frontier peer %s before activating %s',
                                     p['id'], new_peer)
                            res = safely_stop_peer(p.get('url'), p['id'])
                            if 'error' in res:
                                log.error('Could not verify %s is stopped: %s — '
                                          'aborting activation of %s '
                                          '(credential safety)',
                                          p['id'], res.get('error'), new_peer)
                                blocked = True
                                break
                    if blocked:
                        return  # try again next tick

                    # Exclude KGs reserved for other cooled-down peers,
                    # so a substitute does useful work on a different KG.
                    excl = _excluded_kgs(cfg, exclude_peer_id=new_peer)
                    # Build credential + lat-strip plan. The active peer
                    # always gets the FIRST slice of creds and the FIRST
                    # Austria-wide lat strip; parallel-frontier peers
                    # (orchestrated separately) get the remaining slices.
                    self._refresh_peer_caps(peer)
                    plan = self._assign_cred_indices([new_peer], cfg)
                    creds_for_peer = plan.get(new_peer)
                    strips = self._frontier_cells()
                    # Lone frontier on activation: own all strips.
                    # The parallel orchestrator will redistribute later
                    # when more frontiers come online.
                    strip_for_peer = [list(s) for s in strips] if strips else None
                    log.info(
                        'Activating peer %s%s creds=%s strip=%s',
                        new_peer,
                        (' (excluding reserved KGs: ' + ','.join(sorted(excl)) + ')') if excl else '',
                        creds_for_peer, strip_for_peer,
                    )
                    result = start_peer_processor(
                        peer.get('url'), exclude_kgs=excl,
                        cred_indices=creds_for_peer,
                        lat_strips=strip_for_peer,
                    )
                    log.info('Start result for %s: %s', new_peer, result)
                    with self._lock:
                        self.state['active_peer'] = new_peer
                        self.state['last_switch'] = datetime.now(timezone.utc).isoformat()
                        # Seed the strip plan so the parallel-frontier
                        # orchestrator knows the active peer's strip.
                        sp = dict(self.state.get('frontier_strip_plan') or {})
                        if strip_for_peer:
                            sp[new_peer] = strip_for_peer
                            self.state['frontier_strip_plan'] = sp
                        cp = dict(self.state.get('frontier_cred_plan') or {})
                        if creds_for_peer:
                            cp[new_peer] = list(creds_for_peer)
                            self.state['frontier_cred_plan'] = cp
                        save_director_state(self.state)
            else:
                log.info('No peers with sufficient bandwidth available')

    # ---- cache-only peer orchestration -----------------------------

    def _peer_role(self, peer: dict) -> str:
        """Return the peer's effective role.

        ``pinned_role`` (set by user via UI) overrides ``role``.
        Values: ``frontier`` (default), ``cache_only``, ``idle``
        (never started by the director, regardless of bandwidth/whitelist).
        """
        pinned = (peer.get('pinned_role') or '').strip().lower()
        if pinned in ('idle', 'off', 'pause', 'paused', 'parked'):
            return 'idle'
        if pinned in ('cache_only', 'cache-only', 'cacheonly'):
            return 'cache_only'
        if pinned in ('frontier', 'active'):
            return 'frontier'
        role = (peer.get('role') or '').strip().lower()
        if role in ('cache_only', 'cache-only', 'cacheonly'):
            return 'cache_only'
        return 'frontier'

    # --- Credential pool & capability tracking -----------------------

    # Minimum credentials required to start a new frontier KG. Set by the
    # user as 'min_creds_per_frontier' in peers.json (default 2).
    MIN_CREDS_PER_FRONTIER_DEFAULT = 2

    @staticmethod
    def _slim_cred_pool_for_state(cred_pool: list[dict]) -> list[dict]:
        """Return assignment-only copies of ``cred_pool`` for stashing into
        ``state['credentials']`` (which is persisted to director_state.json).

        The frontier assignment path (``_valid_credentials`` /
        ``_assign_cred_indices``) only reads ``health``, ``last_status``,
        ``exhausted``, ``index`` and ``client_id`` — it NEVER touches the
        per-hour ``usage.buckets`` (168 entries/cred ≈ 6.5 KB) or
        ``by_product`` telemetry. Persisting those into the already-large
        director_state.json (and reloading them on every status recompute)
        bloated the save/load cycle enough to time out the dashboard's
        status fetch, which then rendered every credential as "no traffic".
        The dashboard itself reads the FULL ``cred_pool`` returned by
        ``_compute_status`` (a local var), so slimming the *stashed* copy
        keeps the usage sparklines intact while keeping state lean.
        """
        slim = []
        for c in cred_pool:
            d = dict(c)
            u = d.get('usage')
            if isinstance(u, dict):
                d['usage'] = {k: v for k, v in u.items()
                              if k not in ('buckets', 'by_product')}
            slim.append(d)
        return slim

    def _credential_pool(self) -> list[dict]:
        """Return the local credential list (no secrets)."""
        try:
            import copernicus as _cop
            return _cop.list_credentials()
        except Exception as e:
            log.warning('credential_pool: %s', e)
            return []

    # Throttle for the director-loop credential-health refresh. The
    # aggregate fanout (~one /api/v1/credentials GET per peer) is cheap
    # but not free; 90s keeps the assignment health fresh without piling
    # on the dashboard's own get_status fanout.
    _CRED_HEALTH_REFRESH_S = 90
    _last_cred_health_refresh: float = 0.0

    def _refresh_credential_health_if_due(self) -> None:
        """Director-loop hook: rebuild the fleet-aggregated, health-scored
        credential pool and stash it in ``state['credentials']`` so
        ``_assign_cred_indices`` / ``_valid_credentials`` rank by REAL
        per-cred health instead of the primary's flat all-1.0 local pool.

        This must run on the director-loop worker (the one that calls the
        frontier orchestrator). ``_compute_status`` also stashes the same
        aggregate, but it only runs on whichever gunicorn worker serves a
        dashboard request — which may not be the loop worker, so the
        assigner could otherwise never see scored creds. Throttled to
        ``_CRED_HEALTH_REFRESH_S``.
        """
        now = time.time()
        if now - float(self._last_cred_health_refresh or 0) < self._CRED_HEALTH_REFRESH_S:
            return
        try:
            cred_pool = self._credential_pool()
        except Exception:
            cred_pool = []
        if not cred_pool:
            return
        try:
            peers_list = list(self.cfg.get('peers', []))
            self._aggregate_credential_usage(cred_pool, peers_list)
        except Exception as e:
            log.debug('refresh_credential_health aggregate failed: %s', e)
            return
        self._last_cred_health_refresh = now
        with self._lock:
            self.state['credentials'] = self._slim_cred_pool_for_state(cred_pool)
            try:
                save_director_state(self.state)
            except Exception:
                log.debug('save_director_state during cred-health refresh failed',
                          exc_info=True)

    def _aggregate_credential_usage(self, cred_pool: list[dict],
                                     peers_list: list[dict]) -> None:
        """Merge per-peer /api/v1/credentials usage into cred_pool entries.

        The processor only runs on peers, so credential traffic is recorded
        there. The director (primary) shows zero unless we sum across the
        fleet. Keyed by client_id (stable across peers; index is identical
        when the builtin list matches but client_id is the safer key).

        Cheap: each peer's /api/v1/credentials returns ~ a few KB. Run in
        parallel and tolerate failures (legacy peers without `usage` are
        simply skipped).
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # Per-peer fetch cache (Sep 2026 bandwidth pass). This aggregate
        # used to re-poll every peer on every status recompute (~2 workers
        # × 29 peers / 30 s ≈ 7k requests/h). Usage counters on a peer
        # only move while it runs frontier work, so a 10-min TTL for
        # running peers and 30-min for the rest loses nothing visible.
        _now_f = time.time()
        _running_ids = set()
        try:
            for _p in peers_list:
                _ps = get_pushed_status(_p.get('id') or '') or {}
                if ((_ps.get('status') or {}).get('state') in ('running', 'processing')):
                    _running_ids.add(_p.get('id'))
        except Exception:
            pass

        def _fetch(peer_url: str | None, pid: str = '') -> list[dict]:
            if peer_url is None:
                return []
            ttl = _CRED_FETCH_TTL_RUNNING_S if pid in _running_ids else _CRED_FETCH_TTL_IDLE_S
            ent = _CRED_FETCH_CACHE.get(peer_url)
            if ent and (_now_f - ent[0]) < ttl:
                return ent[1]
            try:
                r = requests.get(peer_url.rstrip('/') + '/api/v1/credentials',
                                 timeout=PEER_TIMEOUT_PROBE,
                                 headers=_admin_headers())
                if not r.ok:
                    return ent[1] if ent else []
                creds = (r.json() or {}).get('credentials') or []
                _CRED_FETCH_CACHE[peer_url] = (_now_f, creds)
                return creds
            except Exception:
                return ent[1] if ent else []

        # Initialise aggregate buckets per client_id
        agg: dict[str, dict] = {}
        for c in cred_pool:
            cid = c.get('client_id')
            if not cid:
                continue
            agg[cid] = {'success_7d': 0, 'error_7d': 0, 'rotated_7d': 0,
                        'success': 0, 'error': 0, 'rotated': 0,
                        'last_use': 0, 'last_success': 0, 'last_error': 0,
                        'buckets_by_hour': {},  # h -> {s,e,r}
                        'by_product': {},
                        'sources': [],  # peer ids contributing
                        'window_hours': 168}

        # Include primary (url=None) by reading its local copernicus.list_credentials
        # so its own traffic counts when it ever runs frontier work.
        try:
            import copernicus as _cop_local
            local_creds = _cop_local.list_credentials()
            _cop_health_live = _cop_local._exhaustion_live
            _cop_exh_epoch = _cop_local._exhausted_at_epoch
        except Exception:
            local_creds = []
            _cop_health_live = lambda m: bool(m.get('exhausted'))
            _cop_exh_epoch = lambda m: float(m.get('exhausted_at') or 0)
        for c in local_creds:
            cid = c.get('client_id')
            u = c.get('usage')
            if not cid or not u or cid not in agg:
                continue
            a = agg[cid]
            contributed = False
            for k in ('success_7d', 'error_7d', 'rotated_7d',
                      'success', 'error', 'rotated'):
                v = int(u.get(k) or 0)
                a[k] += v
                if v:
                    contributed = True
            for k in ('last_use', 'last_success', 'last_error'):
                v = int(u.get(k) or 0)
                if v > a[k]:
                    a[k] = v
            for b in (u.get('buckets') or []):
                h = b.get('h')
                if h is None:
                    continue
                cell = a['buckets_by_hour'].setdefault(
                    int(h), {'s': 0, 'e': 0, 'r': 0})
                cell['s'] += int(b.get('s') or 0)
                cell['e'] += int(b.get('e') or 0)
                cell['r'] += int(b.get('r') or 0)
            for prod, pc in (u.get('by_product') or {}).items():
                dst = a['by_product'].setdefault(
                    prod, {'success': 0, 'error': 0, 'rotated': 0})
                if isinstance(pc, dict):
                    dst['success'] += int(pc.get('success') or 0)
                    dst['error'] += int(pc.get('error') or 0)
                    dst['rotated'] += int(pc.get('rotated') or 0)
            if contributed and 'primary' not in a['sources']:
                a['sources'].append('primary')

        targets = [p for p in peers_list if p.get('url')]
        if not targets:
            # Still materialise local-only contributions below.
            pass
        if targets:
          with ThreadPoolExecutor(max_workers=min(16, len(targets))) as ex:
            futs = {ex.submit(_fetch, p.get('url'), p.get('id') or ''): p for p in targets}
            try:
                done_iter = as_completed(futs, timeout=20)
            except Exception:
                done_iter = []
            for fut in done_iter:
                peer = futs[fut]
                pid = peer.get('id')
                try:
                    creds = fut.result()
                except Exception:
                    continue
                for c in creds:
                    cid = c.get('client_id')
                    u = c.get('usage')
                    if not cid or cid not in agg:
                        continue
                    a = agg[cid]
                    # Data-confirmed exhaustion (402 on a real PU
                    # request, see copernicus._probe_credential) is
                    # only ever observed on peers — the primary never
                    # makes data requests and its hourly auth-only
                    # revalidation cannot see quota. Merge the verdict
                    # so _valid_credentials stops assigning the cred to
                    # frontiers instead of every peer re-discovering it.
                    try:
                        if c.get('exhausted') and _cop_health_live(c):
                            at = _cop_exh_epoch(c)
                            if at > float(a.get('exhausted_at') or 0):
                                a['exhausted_at'] = at
                            a['exhausted'] = True
                            if pid and pid not in a.setdefault('exhausted_by', []):
                                a['exhausted_by'].append(pid)
                    except Exception:
                        pass
                    if not u:
                        continue
                    contributed = False
                    for k in ('success_7d', 'error_7d', 'rotated_7d',
                              'success', 'error', 'rotated'):
                        v = int(u.get(k) or 0)
                        a[k] += v
                        if v:
                            contributed = True
                    for k in ('last_use', 'last_success', 'last_error'):
                        v = int(u.get(k) or 0)
                        if v > a[k]:
                            a[k] = v
                    # Merge per-hour buckets
                    for b in (u.get('buckets') or []):
                        h = b.get('h')
                        if h is None:
                            continue
                        cell = a['buckets_by_hour'].setdefault(
                            int(h), {'s': 0, 'e': 0, 'r': 0})
                        cell['s'] += int(b.get('s') or 0)
                        cell['e'] += int(b.get('e') or 0)
                        cell['r'] += int(b.get('r') or 0)
                    # Merge per-product counts (best-effort)
                    for prod, pc in (u.get('by_product') or {}).items():
                        dst = a['by_product'].setdefault(
                            prod, {'success': 0, 'error': 0, 'rotated': 0})
                        if isinstance(pc, dict):
                            dst['success'] += int(pc.get('success') or 0)
                            dst['error'] += int(pc.get('error') or 0)
                            dst['rotated'] += int(pc.get('rotated') or 0)
                    if contributed and pid and pid not in a['sources']:
                        a['sources'].append(pid)

        # Materialise back onto cred_pool: convert buckets_by_hour to a
        # sorted list spanning the standard 168-hour window.
        now_h = int(time.time()) // 3600
        for c in cred_pool:
            cid = c.get('client_id')
            if not cid or cid not in agg:
                continue
            a = agg[cid]
            window = a['window_hours']
            cutoff = now_h - window
            buckets_out = []
            bb = a['buckets_by_hour']
            for h in range(cutoff + 1, now_h + 1):
                cell = bb.get(h, {'s': 0, 'e': 0, 'r': 0})
                buckets_out.append({'h': h, 's': cell['s'],
                                     'e': cell['e'], 'r': cell['r']})
            if a.get('exhausted'):
                c['exhausted'] = True
                c['exhausted_at'] = a.get('exhausted_at') or c.get('exhausted_at')
                c['exhausted_by'] = list(a.get('exhausted_by') or [])
                if (c.get('last_status') or '').lower() in ('valid', 'unchecked', ''):
                    c['last_status'] = 'exhausted'
            c['usage'] = {
                'success_7d': a['success_7d'],
                'error_7d': a['error_7d'],
                'rotated_7d': a['rotated_7d'],
                'success': a['success'],
                'error': a['error'],
                'rotated': a['rotated'],
                'last_use': a['last_use'],
                'last_success': a['last_success'],
                'last_error': a['last_error'],
                'buckets': buckets_out,
                'window_hours': window,
                'by_product': a['by_product'],
                'aggregated_from': a['sources'],
            }
            # Recompute the health score against the *aggregated* usage
            # so the director's view (process.html) matches the cred
            # ordering used by ``_assign_cred_indices``. Each peer's
            # local /api/v1/credentials still returns its own per-peer
            # score using its local usage.
            try:
                import copernicus as _cop_health
                c['health'] = _cop_health.score_credential_health(c)
            except Exception:
                pass

    def _valid_credentials(self) -> list[int]:
        """Return indices of credentials that are not exhausted.

        **Pure cache read.** Never triggers an OIDC probe. The
        director loop is the *only* call site permitted to refresh
        the credential health (via ``_refresh_credentials_if_due``);
        every other reader — including ``get_status`` running in any
        gunicorn worker — sees the cached ``last_status`` from
        ``copernicus.list_credentials()``.

        Why this is strict: in 2026-05-06 we saw the primary's worker
        pool wedge with ~50 OIDC requests/s and the listen backlog
        overflow (process.html became unreachable). Root cause: any
        cache miss in any worker thread fanned out into 8 OIDC token
        requests. Multiple threads racing past a stale timestamp
        amplified that to 64+ probes per tick. The architectural fix
        is that the request path *cannot* probe — only the director
        loop can.
        """
        # Prefer the fleet-aggregated, health-scored pool stashed by
        # ``_compute_status``; it carries the ``stalled`` clamp derived
        # from per-peer usage. Fall back to the primary's LOCAL pool only
        # when the aggregate hasn't been computed yet (fresh director).
        creds = self.state.get('credentials') or self._credential_pool()
        valid = []
        for c in creds:
            if c.get('exhausted'):
                continue
            st = (c.get('last_status') or '').lower()
            if st in ('exhausted', 'invalid'):
                continue
            # Drop credentials the health scorer has flagged dead
            # (expired account / revoked secret / hard quota): they still
            # authenticate (``last_status==valid``) but 402 on every data
            # request, so they must never be handed to a frontier. Only
            # excluded when we actually have a health verdict — a missing
            # ``health`` block (local-pool fallback) leaves the cred in.
            label = ((c.get('health') or {}).get('label') or '').lower()
            if label in ('stalled', 'unusable'):
                continue
            valid.append(int(c.get('index')))
        return valid

    def _oidc_reval_loop(self) -> None:
        """Dedicated daemon: probes OIDC credentials on its own cadence
        so the director loop never blocks on a 6-worker parallel sweep
        (which still takes a few seconds wall-clock when CDSE is slow).

        Lifecycle is bound to the owning Director instance: started in
        ``start()``, stopped in ``stop()`` via ``self._oidc_stop``.
        Critically NOT a module-global thread — on step_down or HA
        handover, the demoted instance is destroyed and its event is
        set, so a stale director can't keep poking OIDC after losing
        the role. The new Director will spawn its own loop.
        """
        # Stagger the first probe slightly so a srv restart doesn't fire
        # an OIDC sweep simultaneously with the director's first tick.
        if self._oidc_stop.wait(timeout=5.0):
            return
        log.info('oidc-reval daemon started (interval=%ds)',
                 _REVALIDATE_INTERVAL_S)
        try:
            while self._running and not self._oidc_stop.is_set():
                try:
                    # Fleet dormant — nobody is consuming credentials;
                    # don't burn OIDC round-trips (and CDSE goodwill)
                    # probing 100+ creds every 10 min for a parked fleet.
                    if (self.state.get('fleet_dormant') or {}).get('active'):
                        if self._oidc_stop.wait(timeout=30.0):
                            break
                        continue
                    self._refresh_credentials_if_due()
                except Exception:
                    log.debug('oidc-reval iteration failed', exc_info=True)
                # Re-check cadence on a tight wake schedule so stop()
                # joins quickly; the interval check inside
                # _refresh_credentials_if_due() is the real throttle.
                if self._oidc_stop.wait(timeout=30.0):
                    break
        finally:
            log.info('oidc-reval daemon stopped')

    def _refresh_credentials_if_due(self) -> None:
        """Director-loop hook: re-run OIDC probes at most once every
        ``_REVALIDATE_INTERVAL_S`` seconds. Single-flight via a process-
        wide lock + module-level timestamp; if another thread is
        already probing (extremely rare given the loop is single-
        threaded but defensive against future call-sites), this returns
        immediately. Cached ``last_status`` updates land on disk via
        ``copernicus._save_credentials_to_disk()`` so all gunicorn
        workers see fresh data without each having to probe.
        """
        global _LAST_CREDS_REVALIDATED_AT, _REVALIDATE_OWNER_THREAD
        now = time.time()
        last_mem = float(_LAST_CREDS_REVALIDATED_AT or 0)
        last_disk = float(self.state.get('_creds_revalidated_at') or 0)
        last = max(last_mem, last_disk)
        if now - last <= _REVALIDATE_INTERVAL_S:
            return
        # Single-flight — if another thread already holds the lock,
        # skip this tick. The next tick will retry once the holder is
        # done; meanwhile cached statuses remain valid.
        if not _REVALIDATE_LOCK.acquire(blocking=False):
            return
        try:
            # Re-check inside the lock (someone else may have just
            # finished a probe).
            if (time.time() - max(
                    float(_LAST_CREDS_REVALIDATED_AT or 0),
                    float(self.state.get('_creds_revalidated_at') or 0),
                )) <= _REVALIDATE_INTERVAL_S:
                return
            _REVALIDATE_OWNER_THREAD = threading.get_ident()
            try:
                import copernicus as _cop
                _before = list(_cop.list_credentials() or [])
                _cop.revalidate_all_credentials()
                _after = list(_cop.list_credentials() or [])
                # Surface a one-line summary: valid/total + any
                # transitions (ok→bad, bad→ok). Keeps the merged log
                # quiet on no-op revalidations — only logs deltas.
                def _key(c):
                    return (c.get('client_id') or '')[:12]
                def _ok(c):
                    return (c.get('last_status') or '').lower() in ('ok', 'valid')
                _b = {_key(c): _ok(c) for c in _before}
                _a = {_key(c): _ok(c) for c in _after}
                _flips = []
                for k, ok_after in _a.items():
                    if k in _b and _b[k] != ok_after:
                        _flips.append(k + (':→ok' if ok_after else ':→bad'))
                _valid = sum(1 for v in _a.values() if v)
                _total = len(_a)
                if _flips:
                    _emit_director_event(
                        'creds revalidated: ' + str(_valid) + '/' + str(_total)
                        + ' valid (' + ', '.join(_flips) + ')')
            except Exception as e:
                log.debug('revalidate_all_credentials failed: %s', e)
                return
            _LAST_CREDS_REVALIDATED_AT = time.time()
            with self._lock:
                self.state['_creds_revalidated_at'] = (
                    _LAST_CREDS_REVALIDATED_AT
                )
                try:
                    save_director_state(self.state)
                except Exception:
                    log.debug('save_director_state during revalidate failed',
                              exc_info=True)
        finally:
            _REVALIDATE_OWNER_THREAD = None
            _REVALIDATE_LOCK.release()

    def _peer_capabilities(self, ps: dict) -> set[str]:
        """Read capability flags from a peer's /api/v1/info response.

        ps is a dict that may have come from get_peer_status() (which
        already contains 'git_commit') OR from a cached /info call.
        We refresh capabilities lazily and cache them in director state.
        """
        caps = set(ps.get('capabilities') or [])
        return caps

    def _refresh_peer_caps(self, peer: dict) -> set[str]:
        """Fetch /api/v1/info from a peer and cache its capabilities.

        Also opportunistically probes /api/v1/credentials to learn how
        many creds the peer's local pool has (used to scope holder
        annotations for legacy peers that use the full local pool).
        """
        url = peer.get('url')
        pid = peer['id']
        cache = self.state.setdefault('_peer_caps', {})
        entry = cache.get(pid) or {}
        # Capabilities only change when the peer's code changes, so key
        # the cache on the commit it reports via its status push and
        # use a long TTL (was 300 s → 2 GET /info + /credentials per
        # peer per 5 min, from BOTH gunicorn workers ≈ 15 MB/h).
        _cur_commit = ''
        try:
            _cur_commit = (((get_pushed_status(pid) or {}).get('status') or {})
                           .get('git_commit') or '')[:7]
        except Exception:
            pass
        if entry and (time.time() - entry.get('at', 0)) < _PEER_CAPS_TTL_S \
                and (not _cur_commit or entry.get('commit', '') == _cur_commit):
            return set(entry.get('caps') or [])
        try:
            if url is None:
                # Local peer: import is fastest
                import copernicus as _cop  # noqa: F401
                caps = {'cred_subset_env', 'lat_strip_filter',
                        'cred_api_v1', 'parallel_frontiers'}
                cred_count = len(_cop.list_credentials())
            else:
                r = requests.get(url.rstrip('/') + '/api/v1/info',
                                 timeout=PEER_TIMEOUT_PROBE,
                                 headers=_admin_headers())
                if not r.ok:
                    return set(entry.get('caps') or [])
                d = r.json()
                caps = set(d.get('capabilities') or [])
                cred_count = entry.get('cred_count') or 4
                # Probe /api/v1/credentials (works on both upgraded and
                # legacy peers that expose it). 4 is the legacy default.
                try:
                    cr = requests.get(url.rstrip('/') + '/api/v1/credentials',
                                      timeout=PEER_TIMEOUT_PROBE,
                                      headers=_admin_headers())
                    if cr.ok:
                        cred_count = len((cr.json() or {}).get('credentials') or [])
                except Exception:
                    pass
                # Self-heal: a peer with 0 creds can't run frontier work
                # and burns CPU on 401 invalid_client storms. This happens
                # when add_peer() ran while the peer was still booting
                # (online=False at the time, so the credential bootstrap
                # in app.py:director_add_peer was skipped). Push from the
                # director's own store now that it's responding.
                if cred_count == 0:
                    try:
                        self._bootstrap_peer_credentials(url, pid)
                        # Re-probe so the cache reflects the new count.
                        cr = requests.get(url.rstrip('/') + '/api/v1/credentials',
                                          timeout=PEER_TIMEOUT_PROBE,
                                          headers=_admin_headers())
                        if cr.ok:
                            cred_count = len((cr.json() or {}).get('credentials') or [])
                    except Exception as _be:
                        log.debug('bootstrap creds to %s failed: %s', pid, _be)
        except Exception as e:
            log.debug('cap probe %s failed: %s', pid, e)
            return set(entry.get('caps') or [])
        cache[pid] = {'caps': sorted(caps), 'at': time.time(),
                      'cred_count': cred_count, 'commit': _cur_commit}
        return caps

    def _bootstrap_peer_credentials(self, url: str, pid: str) -> int:
        """Push every credential from the director's local pool to *peer*.

        Idempotent at the peer side (re-adding an existing client_id is a
        meta-update). Throttled per-peer to once every 5 min via
        ``state['_cred_bootstrap_at']`` so a transiently-flapping peer
        doesn't spin us. Marked X-Cred-Fanout=1 so the receiving peer
        does NOT re-broadcast (we're addressing it directly).
        """
        if not url:
            return 0
        now = time.time()
        with self._lock:
            book = self.state.setdefault('_cred_bootstrap_at', {})
            last = float(book.get(pid) or 0)
            if now - last < 300:
                return 0
            book[pid] = now
        try:
            import copernicus as _cop
            store = _cop.list_credentials_with_secrets() or []
        except Exception as e:
            log.debug('list_credentials_with_secrets failed: %s', e)
            return 0
        if not store:
            return 0
        hdrs = dict(_admin_headers())
        hdrs['X-Cred-Fanout'] = '1'
        ok = 0
        for c in store:
            cid = (c.get('client_id') or '').strip()
            sec = (c.get('client_secret') or '').strip()
            if not cid or not sec:
                continue
            try:
                rr = requests.post(
                    url.rstrip('/') + '/api/v1/credentials',
                    json={'client_id': cid, 'client_secret': sec,
                          'label': c.get('label', ''),
                          'notes': c.get('notes', ''),
                          'validate': False},
                    headers=hdrs, timeout=10,
                )
                if rr.ok:
                    ok += 1
            except Exception:
                pass
        if ok:
            try:
                _emit_director_event(
                    'creds bootstrapped to ' + pid + ': ' + str(ok)
                    + '/' + str(len(store)) + ' (peer had empty store)',
                    peer=pid)
            except Exception:
                pass
        return ok

    def _effective_creds_per_frontier(self, cfg: dict) -> int:
        """Legacy hook — returns floor (per-peer cred count) for the
        static path or the value derived from adaptive frontier-count
        planning. Kept because several callers (status payload,
        cred-slice assignment, cache-only orchestrator) read it.

        Static path: ``min_creds_per_frontier`` from peers.json (default 2).

        Adaptive path (default ON): scales the *target frontier count*
        from the smoothed Copernicus sub-factor (see
        :py:meth:`_target_frontier_count`) and back-derives a per-peer
        cred count = floor(len(valid) / target). With 8 valid creds:
        target=8 → per=1, target=5 → per=1 (3 peers get 1, 5 get 1 —
        see _assign_cred_indices for the uneven split), target=2 →
        per=4. Per is always ≥ 1 and never exceeds the static floor.
        """
        floor = max(1, int(cfg.get('min_creds_per_frontier',
                                    self.MIN_CREDS_PER_FRONTIER_DEFAULT)))
        if not bool(cfg.get('adaptive_creds_per_frontier', True)):
            return floor
        valid = self._valid_credentials()
        if not valid:
            return floor
        target = self._target_frontier_count(cfg, len(valid))
        # Per-peer cred count: at least 1, capped by floor. Frontiers
        # past the first ``len(valid) % target`` get the floor (= per),
        # the rest get one extra cred — but the worker only sees its
        # own slice, so reporting ``per`` here is fine for status.
        per = max(1, min(floor, len(valid) // max(1, target)))
        return per

    def _target_frontier_count(self, cfg: dict, n_valid: int) -> int:
        """Smoothly scaled target frontier count in [1, n_valid].

        Driven by the smoothed Copernicus sub-factor EMA:
          * EMA = 1.0 (clean) → target = n_valid (e.g. 8)
          * EMA = THROTTLE_MIN_FACTOR (saturated) → target = ceil(n_valid / floor)
            (e.g. 4 with floor=2)

        Linear interp between those endpoints, with sticky rounding
        (±0.4 hysteresis around the previous decision) so we don't
        flap one frontier up/down on every tick. Decision is persisted
        across ticks in ``state['_target_frontier_count']``.
        """
        floor = max(1, int(cfg.get('min_creds_per_frontier',
                                    self.MIN_CREDS_PER_FRONTIER_DEFAULT)))
        ceiling = max(1, n_valid)
        baseline = max(1, n_valid // floor)
        if ceiling <= baseline:
            return ceiling
        sub_ema = float(self._sub_factor_ema.get(
            'copernicus', THROTTLE_MAX_FACTOR))
        # Map [MIN_FACTOR, MAX_FACTOR] → [baseline, ceiling].
        span = max(1e-6, THROTTLE_MAX_FACTOR - THROTTLE_MIN_FACTOR)
        frac = (sub_ema - THROTTLE_MIN_FACTOR) / span
        frac = max(0.0, min(1.0, frac))
        raw = baseline + frac * (ceiling - baseline)
        prev = float(self.state.get('_target_frontier_count', raw))
        # Sticky rounding: only move when raw is >0.4 away from prev,
        # otherwise keep prev rounded. Prevents a single 402 from
        # bumping us 5↔6.
        if abs(raw - prev) < 0.4:
            decision = int(round(prev))
        else:
            decision = int(round(raw))
        decision = max(baseline, min(ceiling, decision))
        self.state['_target_frontier_count'] = float(decision)
        return decision

    def _max_parallel_frontiers(self, cfg: dict) -> int:
        """How many frontier peers we may run concurrently.

        Each frontier holds ``min_creds_per_frontier`` credentials — one
        active + one (or more) hot-standby spares for in-peer rotation
        on 402. With per=2 and 8 valid creds, we get 4 frontiers, each
        with its own pair (1 working + 1 spare).

        With adaptive mode and a healthy Copernicus sub-factor we drop
        per=1 — 8 valid creds give 8 frontier slots.

        Also bounded by the number of planning cells with pending work
        (``_frontier_cells``; FRONTIER_CELL_LAT×LON grid, ~34 cells at
        0.5°×1°) and by ``FRONTIER_MAX_PARALLEL``.
        """
        valid = self._valid_credentials()
        if not valid:
            return 0
        if not bool(cfg.get('adaptive_creds_per_frontier', True)):
            per = self._effective_creds_per_frontier(cfg)
            cap_creds = max(1, len(valid) // per)
        else:
            cap_creds = self._target_frontier_count(cfg, len(valid))
        # Cap on how many disjoint cells we can hand out. With ~14
        # non-empty cells across Austria this should never bind
        # — cred capacity is the real ceiling.
        cells = self._frontier_cells()
        cap_cells = max(1, len(cells))
        # End-game: fewer cells than cred capacity → let frontiers share
        # cells, bounded by the pending item count per cell.
        if cap_cells < cap_creds:
            cap_cells = max(cap_cells, self._endgame_cell_slots(cells))
        # Remember whether *work* (pending items), not credentials, is
        # the binding term — the capacity throttle is skipped then (see
        # the caller: a handful of straggler blocks is far below the
        # fleet load the factor was tuned for).
        self._frontier_work_bound = bool(cap_cells < cap_creds
                                         and cap_cells < FRONTIER_MAX_PARALLEL)
        return max(1, min(cap_creds, cap_cells, FRONTIER_MAX_PARALLEL))

    def _endgame_cell_slots(self, cells) -> int:
        """Total frontier slots when cells may be shared (see
        ``FRONTIER_ENDGAME_PER_CELL``): Σ min(per_cell_cap, pending items)."""
        items = getattr(self, '_frontier_cell_items', None) or {}
        per = max(1, FRONTIER_ENDGAME_PER_CELL)
        total = 0
        for c in cells:
            total += max(1, min(per, int(items.get(tuple(c), 1))))
        return total

    def _assign_cred_indices(self, frontier_ids: list[str], cfg: dict,
                              prior: dict | None = None,
                              sticky: set[str] | None = None) -> dict:
        """Distribute valid credential indices across frontier peers.

        Each peer gets a disjoint slice of length ``min_creds_per_frontier``
        (1 active + (per-1) hot-standby spares for in-peer rotation).
        Returns {peer_id: [cred_idx, ...]}.

        ``prior`` is the previous plan ({peer_id: [idx,...]}). When
        provided, peers in ``prior`` keep their slice if it is still
        valid (all indices still in the valid pool, length == per, no
        overlap with another peer's slice). New peers get fresh slices
        from whatever credentials remain. This avoids restarts caused
        by membership churn re-sorting the assignment.

        ``sticky`` is a set of peer ids whose prior slice must be kept
        verbatim as long as every index is still valid — no health
        eviction, no per_eff trim. Use it for peers that are currently
        RUNNING a frontier: their processor already has these creds in
        its env, and any reallocation of a still-valid held cred to
        another peer forces a hard restart (the Jul 2026 plan-drift
        loop). A sticky cred is only released when it turns invalid /
        exhausted, or when the peer stops.
        """
        valid_set_all = set(self._valid_credentials())
        # Order valid creds by *health* score (highest = best candidate)
        # so fresh, low-error, under-used credentials are picked first
        # and recently-erroring or hot creds are deprioritised. This
        # rotates the warm/hot set across peers instead of always
        # landing on the same indices. See ``copernicus.score_credential_health``.
        try:
            cred_pool = self.state.get('credentials') or self._credential_pool()
        except Exception:
            cred_pool = []
        health_by_idx: dict[int, dict] = {}
        for c in cred_pool:
            try:
                idx = int(c.get('index'))
            except Exception:
                continue
            h = c.get('health')
            if not h:
                try:
                    import copernicus as _cop
                    h = _cop.score_credential_health(c)
                except Exception:
                    h = None
            health_by_idx[idx] = h or {'score': 0.5}
        # Sort key: best health first (descending score). Among creds of
        # (near-)equal score we want a RANDOM pick rather than the lowest
        # index every time — otherwise leftover slices always land on the
        # same low-index creds, defeating load-spreading across the
        # healthy pool. We bucket the score (0.05 granularity) so clearly
        # healthier creds still strictly win, then break ties randomly.
        # Existing peers keep their slice via the prior-plan preservation
        # pass below, so this randomisation only affects *new* allocations
        # and never churns a stable assignment.
        import random as _rnd
        _jitter = {i: _rnd.random() for i in valid_set_all}
        def _rank(i: int) -> tuple:
            h = health_by_idx.get(i) or {'score': 0.5}
            score = float(h.get('score') or 0.0)
            bucket = round(score / 0.05)  # higher bucket = healthier
            return (-bucket, _jitter[i])
        valid = sorted(valid_set_all, key=_rank)
        per = self._effective_creds_per_frontier(cfg)
        out: dict[str, list[int]] = {}
        if not valid or not frontier_ids:
            return out
        n_peers = len(frontier_ids)
        # When the planner asked for more frontiers than per*N can
        # cover (uneven split: e.g. 8 creds, 5 frontiers with per=1),
        # spread the leftover creds one extra each across the first K
        # peers. Each peer thus gets per or per+1 creds. If
        # n_peers*per > len(valid) we fall back to per=1 for everyone
        # and drop the last few peers if there still aren't enough.
        if per * n_peers > len(valid):
            per_eff = max(1, len(valid) // max(1, n_peers))
        else:
            per_eff = per
        valid_set = set(valid)
        used: set[int] = set()
        prior = prior or {}
        # Keep existing assignments where possible. We trim down
        # over-allocated slices (e.g. peer was activated solo with
        # per=2, target now wants per_eff=1) so cred capacity isn't
        # hoarded by early peers. Lengths above per_eff+1 are also
        # trimmed to per_eff. This was the bug that capped parallel
        # frontiers at 5 (with 8 valid creds and target=8) because
        # the active peer's prior creds=[0,1] survived as a 2-cred
        # slice instead of being squeezed to a single index.
        # Health threshold below which we evict a held credential and
        # let the leftover pass refill from a healthier candidate.
        # 0.35 ~ "degraded" — rotation churn or recent errors.
        UNHEALTHY = 0.35
        sticky = sticky or set()
        # Pass 0 — sticky (RUNNING) peers keep their held slice
        # verbatim, minus creds that turned invalid. No health
        # eviction, no per_eff trim: reallocating a still-valid cred
        # away from a running processor forces a hard restart, which
        # costs far more than a temporarily sub-optimal slice.
        for pid in frontier_ids:
            if pid not in sticky:
                continue
            slice_ = prior.get(pid)
            if not slice_:
                continue
            slice_ = [int(i) for i in slice_
                      if int(i) in valid_set and int(i) not in used]
            if not slice_:
                continue
            out[pid] = slice_
            used.update(slice_)
        for pid in frontier_ids:
            if pid in out:
                continue
            slice_ = prior.get(pid)
            if not slice_:
                continue
            slice_ = [int(i) for i in slice_ if int(i) in valid_set]
            if not slice_:
                continue
            # Evict creds whose health has dropped — only when there's
            # a strictly healthier leftover candidate available, so a
            # globally-degraded pool doesn't cause assignment churn.
            healthy_pool = [
                i for i in valid
                if i not in used
                and i not in slice_
                and float((health_by_idx.get(i) or {}).get('score') or 0)
                    > UNHEALTHY + 0.15
            ]
            if healthy_pool:
                slice_ = [
                    i for i in slice_
                    if float((health_by_idx.get(i) or {}).get('score') or 0)
                       >= UNHEALTHY
                ]
                if not slice_:
                    continue
            # Keep deterministic order, then sort by health for the
            # in-slice rotation order (worker reads index 0 first).
            slice_ = sorted(slice_,
                            key=lambda i: -float(
                                (health_by_idx.get(i) or {}).get('score') or 0))
            if set(slice_) & used:
                slice_ = [i for i in slice_ if i not in used]
                if not slice_:
                    continue
            if len(slice_) > per_eff:
                slice_ = slice_[:per_eff]
            if len(slice_) < per_eff:
                # Defer to the leftover-distribution pass; it can top
                # up shorts from the leftover pool.
                continue
            out[pid] = slice_
            used.update(slice_)
        # Recompute extras based on what's already locked in.
        leftovers = [i for i in valid if i not in used]
        remaining_peers = sorted(p for p in frontier_ids if p not in out)
        # Distribute: each peer gets per_eff; first ``extra`` peers get +1.
        # ``extra`` is the number of leftover creds beyond per_eff*n_peers,
        # but we've already given some peers their share via ``out``
        # — so recompute as (leftovers - per_eff*remaining).
        bonus = max(0, len(leftovers) - per_eff * len(remaining_peers))
        for i, pid in enumerate(remaining_peers):
            take = per_eff + (1 if i < bonus else 0)
            slice_ = leftovers[:take]
            if len(slice_) < per_eff:
                break
            out[pid] = slice_
            leftovers = leftovers[take:]
        return out

    def _austria_lat_strips(self) -> list[tuple[float, float]]:
        """DEPRECATED. Returns 1° lat-only ranges covering Austria.

        Kept only for backward compat with old state on disk; new
        planning uses :py:meth:`_austria_cells` (1° lat × 2° lon cells).
        """
        try:
            from zenodo_cache import _lat_strips
            return [tuple(s) for s in _lat_strips()]
        except Exception:
            strips = []
            s = 46.0
            while s < 49.5:
                strips.append((round(s, 4), round(s + 1.0, 4)))
                s += 1.0
            return strips

    @staticmethod
    def _planning_grid() -> list[tuple[float, float, float, float]]:
        """Raw FRONTIER_CELL_LAT × FRONTIER_CELL_LON planning grid over
        the Austria bounding box (before empty-cell pruning)."""
        import math as _m
        try:
            from zenodo_cache import AT_WEST, AT_SOUTH, AT_EAST, AT_NORTH
        except Exception:
            AT_WEST, AT_SOUTH, AT_EAST, AT_NORTH = 9.0, 46.0, 17.5, 49.5
        h = max(0.1, FRONTIER_CELL_LAT)
        wd = max(0.1, FRONTIER_CELL_LON)
        out = []
        s = _m.floor(AT_SOUTH / h) * h
        while s < AT_NORTH:
            w = _m.floor(AT_WEST / wd) * wd
            while w < AT_EAST:
                out.append((round(s, 4), round(s + h, 4), round(w, 4), round(w + wd, 4)))
                w += wd
            s += h
        return out

    @staticmethod
    def _planning_cells_within(s: float, n: float, w: float, e: float,
                               cells: list) -> list[tuple[float, float, float, float]]:
        """Planning cells whose SW corner lies inside the (coarser) v1
        ZIP cell (s,n,w,e) — used to translate ZIP coverage into
        planning-cell coverage."""
        return [c for c in cells
                if s - 1e-9 <= c[0] < n - 1e-9 and w - 1e-9 <= c[2] < e - 1e-9]

    def _austria_cells(self) -> list[tuple[float, float, float, float]]:
        """Return the canonical (south, north, west, east) frontier
        *planning* cells that cover Austria (``_planning_grid``,
        FRONTIER_CELL_LAT × FRONTIER_CELL_LON, default 0.5°×1° = the
        tile-store shard). Historically this was the 1°×2° Zenodo ZIP
        grid so disjoint frontiers never rebuilt the same ZIP; with tile
        store v2 that coupling is gone and the grid is finer (~35
        non-empty cells vs 13) so the cell cap stops binding the
        frontier count. Cells with zero KGs are pruned so frontier
        peers never get pinned to dead cells (which causes the
        processor to start, find nothing, exit, repeat — and the
        peer's assigned credential is never used).

        Result is cached on the instance for the life of the director.
        """
        cached = getattr(self, '_austria_cells_cache', None)
        if cached is not None:
            return cached
        raw = self._planning_grid()
        # Prune cells with no KGs. kg_list.json is the canonical KG
        # list (~7850 entries), each carrying lat/lon centroids.
        try:
            kg_path = DATA_DIR / 'kg_list.json'
            if kg_path.exists():
                kgs = json.loads(kg_path.read_text())
                non_empty: set[tuple[float, float, float, float]] = set()
                for k in kgs:
                    bb = k.get('bbox') or {}
                    try:
                        lat = (float(bb['min_lat']) + float(bb['max_lat'])) / 2
                        lon = (float(bb['min_lon']) + float(bb['max_lon'])) / 2
                    except Exception:
                        lat = k.get('lat'); lon = k.get('lon')
                        if lat is None or lon is None:
                            continue
                    for c in raw:
                        s, n, w, e = c
                        if s <= lat < n and w <= lon < e:
                            non_empty.add(c)
                            break
                pruned = [c for c in raw if c in non_empty]
                if pruned:
                    dropped = len(raw) - len(pruned)
                    if dropped:
                        log.info('austria_cells: pruned %d empty cells '
                                 '(%d → %d)', dropped, len(raw), len(pruned))
                    raw = pruned
        except Exception as e:
            log.warning('austria_cells: empty-cell prune failed: %s', e)
        self._austria_cells_cache = raw
        return raw

    # --- No-work frontier restart-loop watchdog -------------------
    # A frontier whose assigned cells hold no pending work starts,
    # exits in <1 s ("Processing complete: ... in 0.0 hours"), and is
    # re-started by the next tick — forever, while pinning a
    # Copernicus credential. ``_frontier_cells`` prevents the common
    # case (cells fully processed), but the peer's own view can differ
    # (its manifest sync is ahead of ours, per-peer tombstones,
    # ``peer_claimed`` filtering, block-split drift). So we also watch
    # for the symptom: the same peer restarted with the SAME plan many
    # times in a short window. When that trips, the plan's cells are
    # quarantined so the next tick redistributes the peer onto cells
    # that actually have work.
    FRONTIER_CHURN_WINDOW_S = 30 * 60
    FRONTIER_CHURN_MAX_STARTS = 6
    BARREN_CELL_TTL_S = 6 * 3600
    # Health gate (2026-09-30 incident): at240 came back from a disk-
    # full episode as interim director, its processor exited within
    # seconds of every start (ENOSPC, not "no pending work"), and the
    # churn detector quarantined SEVEN cells in 30 min — two of which
    # had live cache-miss KGs waiting for exactly that frontier. A
    # restart loop is only evidence about the *cells* when the peer
    # itself is healthy; below this much free disk (or when the peer
    # reports an error state) the loop is attributed to the peer and
    # the cells are left alone.
    FRONTIER_CHURN_MIN_DISK_GB = 6.0

    def _churn_peer_unhealthy(self, pid: str) -> str:
        """Return a short reason if *pid* looks too unhealthy for its
        restart loop to say anything about its cells; '' if healthy."""
        try:
            st = get_pushed_status(pid) or {}
        except Exception:
            st = {}
        sysd = st.get('system') or {}
        disk = sysd.get('disk_free_gb')
        try:
            if disk is not None and float(disk) < self.FRONTIER_CHURN_MIN_DISK_GB:
                return 'disk_free=%.1fGB' % float(disk)
        except Exception:
            pass
        if st.get('state') in ('error', 'unreachable', 'crashed'):
            return 'state=%s' % st.get('state')
        return ''

    @staticmethod
    def _plan_key(creds, cells) -> str:
        try:
            _c = ','.join(str(int(c)) for c in sorted(creds or []))
        except Exception:
            _c = str(creds)
        try:
            _s = ';'.join('%g/%g/%g/%g' % tuple(x)
                          for x in sorted(tuple(y) for y in (cells or [])))
        except Exception:
            _s = str(cells)
        return _c + '|' + _s

    def _note_frontier_start_churn(self, pid: str, creds, cells) -> None:
        """Record a frontier start; quarantine cells on restart loops."""
        now = time.time()
        key = self._plan_key(creds, cells)
        trip = False
        with self._lock:
            churn = self.state.setdefault('frontier_start_churn', {})
            rec = churn.get(pid) or {}
            if rec.get('key') != key or (now - float(rec.get('first') or 0)
                                         > self.FRONTIER_CHURN_WINDOW_S):
                rec = {'key': key, 'first': now, 'n': 0}
            rec['n'] = int(rec.get('n', 0)) + 1
            rec['last'] = now
            churn[pid] = rec
            unhealthy = ''
            if rec['n'] >= self.FRONTIER_CHURN_MAX_STARTS:
                unhealthy = self._churn_peer_unhealthy(pid)
                if not unhealthy:
                    barren = self.state.setdefault('barren_cells', {})
                    meta = self.state.setdefault('barren_cells_meta', {})
                    for c in (cells or []):
                        try:
                            k = '%g/%g/%g/%g' % tuple(c)
                        except Exception:
                            continue
                        barren[k] = now
                        meta[k] = {'peer': pid, 'at': now,
                                   'starts': rec['n']}
                    trip = True
                churn.pop(pid, None)
            save_director_state(self.state)
        if unhealthy:
            log.warning('Frontier %s restarted %d× in %d min but peer is '
                        'unhealthy (%s) — NOT quarantining cells %s',
                        pid, self.FRONTIER_CHURN_MAX_STARTS,
                        self.FRONTIER_CHURN_WINDOW_S // 60, unhealthy, cells)
            try:
                _emit_director_event(
                    'restart loop: %d starts in %d min but peer unhealthy '
                    '(%s) — attributed to the peer, cells %s NOT quarantined'
                    % (self.FRONTIER_CHURN_MAX_STARTS,
                       self.FRONTIER_CHURN_WINDOW_S // 60, unhealthy,
                       ','.join('%g-%g/%g-%g' % tuple(c)
                                for c in (cells or []))),
                    peer=pid, level='warning')
            except Exception:
                pass
            return
        if trip:
            self._frontier_cells_cache = None
            log.warning('Frontier %s restarted %d× in %d min with the same '
                        'plan and no progress — quarantining cells %s for '
                        '%dh', pid, self.FRONTIER_CHURN_MAX_STARTS,
                        self.FRONTIER_CHURN_WINDOW_S // 60, cells,
                        self.BARREN_CELL_TTL_S // 3600)
            try:
                _emit_director_event(
                    'no-work restart loop: %d starts in %d min with the same '
                    'plan — cells %s quarantined for %dh, peer will be '
                    're-sliced onto cells with pending work'
                    % (self.FRONTIER_CHURN_MAX_STARTS,
                       self.FRONTIER_CHURN_WINDOW_S // 60,
                       ','.join('%g-%g/%g-%g' % tuple(c)
                                for c in (cells or [])),
                       self.BARREN_CELL_TTL_S // 3600),
                    peer=pid, level='warning')
            except Exception:
                pass

    def _clear_frontier_start_churn(self, pid: str) -> None:
        """Forget churn history for *pid* (it made real progress)."""
        if not pid:
            return
        with self._lock:
            churn = self.state.get('frontier_start_churn') or {}
            if churn.pop(pid, None) is not None:
                save_director_state(self.state)

    def _cells_with_fresh_cache_misses(self) -> dict:
        """{cell_key: newest cache-miss recorded_at (epoch)} from
        ``cache_miss_kgs.json``. A cache-only peer aborting on a miss
        in a cell is positive proof that the cell has frontier work."""
        out: dict = {}
        try:
            for rec in (self._load_cache_misses() or {}).values():
                cell = (rec or {}).get('cell')
                if not cell or len(cell) != 4:
                    continue
                try:
                    k = '%g/%g/%g/%g' % tuple(float(x) for x in cell)
                    ts = datetime.fromisoformat(
                        str(rec.get('recorded_at')).replace('Z', '+00:00')
                    ).timestamp()
                except Exception:
                    continue
                if ts > out.get(k, 0.0):
                    out[k] = ts
        except Exception:
            pass
        return out

    def _barren_cells(self) -> set:
        """Currently-quarantined cells.

        Self-healing: an entry is dropped when (a) its 6 h TTL expires,
        or (b) a cache-only peer recorded a cache miss inside that cell
        (any live miss, or one newer than the quarantine) — that is
        direct evidence the cell has pending frontier work, so the
        quarantine was a misattribution and must not hold work hostage.
        """
        now = time.time()
        raw = dict(self.state.get('barren_cells') or {})
        out = set()
        expired = []
        released = []
        misses = self._cells_with_fresh_cache_misses() if raw else {}
        for k, ts in raw.items():
            if now - float(ts or 0) > self.BARREN_CELL_TTL_S:
                expired.append(k)
                continue
            try:
                s, n, w, e = (float(x) for x in k.split('/'))
            except Exception:
                expired.append(k)
                continue
            if k in misses:
                released.append(k)
                continue
            out.add((round(s, 4), round(n, 4), round(w, 4), round(e, 4)))
        if expired or released:
            with self._lock:
                b = self.state.get('barren_cells') or {}
                m = self.state.get('barren_cells_meta') or {}
                for k in expired + released:
                    b.pop(k, None)
                    m.pop(k, None)
                save_director_state(self.state)
            self._frontier_cells_cache = None
            if released:
                try:
                    _emit_director_event(
                        'cell quarantine released early: %s — cache-only '
                        'peers hit cache misses there, so the cell has '
                        'frontier work (quarantine was a misattribution)'
                        % ','.join(_cell_key_pretty(k) for k in released),
                        peer='director', level='warning')
                except Exception:
                    pass
        return out

    def clear_barren_cells(self, cells: list | None = None) -> list:
        """Operator release of quarantined cells (all if *cells* None).
        *cells* are 'S/N/W/E' keys or 4-lists. Returns released keys."""
        want = None
        if cells:
            want = set()
            for c in cells:
                try:
                    if isinstance(c, str):
                        want.add('%g/%g/%g/%g' % tuple(
                            float(x) for x in c.replace('-', '/').split('/')))
                    else:
                        want.add('%g/%g/%g/%g' % tuple(float(x) for x in c))
                except Exception:
                    continue
        with self._lock:
            b = self.state.get('barren_cells') or {}
            m = self.state.get('barren_cells_meta') or {}
            keys = [k for k in list(b) if want is None or k in want]
            for k in keys:
                b.pop(k, None)
                m.pop(k, None)
            self.state.pop('frontier_start_churn', None)
            save_director_state(self.state)
        self._frontier_cells_cache = None
        if keys:
            _emit_director_event('cell quarantine cleared by operator: %s'
                                 % ','.join(_cell_key_pretty(k) for k in keys),
                                 peer='director')
        return keys

    def barren_cells_detail(self) -> list:
        """[{cell, peer, since, expires_in_s}] for the dashboard."""
        now = time.time()
        meta = self.state.get('barren_cells_meta') or {}
        rows = []
        for k, ts in sorted((self.state.get('barren_cells') or {}).items()):
            mt = meta.get(k) or {}
            rows.append({
                'cell': k, 'peer': mt.get('peer') or '?',
                'since': float(ts or 0),
                'expires_in_s': max(0, int(self.BARREN_CELL_TTL_S - (now - float(ts or 0)))),
            })
        return rows

    # TTL for the pending-work cell scan (kg_list + splitter expansion
    # is ~1 s; completion state only moves at KG-completion rate).
    FRONTIER_CELLS_TTL_S = 300

    def _frontier_cells(self) -> list[tuple[float, float, float, float]]:
        """Austria cells that still contain *unprocessed* KG work.

        ``_austria_cells`` only prunes cells with zero KGs **ever**. As
        the run completes region by region, cells become *exhausted*:
        every KG (and every block of every split KG) in them is already
        in the manifest. A frontier pinned to such a cell starts its
        processor, logs ``KG_CELL_FILTER active (2 cells): 6540 -> 8
        KGs`` → ``Expanded 8 large KGs into blocks: 8 → 0 items`` →
        ``Processing complete`` and exits within a second. The director
        sees a stopped-but-authorised frontier, re-issues the same plan
        next tick, and the peer restart-loops forever while holding a
        Copernicus credential hostage and never doing any work
        (observed Aug 2026 on at106 with cells 46-47/10-12 +
        46-47/16-18, whose only survivors were the two permanently
        failed ``80110-southeast-*`` blocks).

        This method applies the *same* pending computation the
        processor does — completed set (manifest + local JSON, minus
        force-requeue tombstones), permanently-failed set, and
        ``kg_splitter.maybe_split_kg`` block expansion — and returns
        only cells with ≥1 pending item. Cells are assigned by KG bbox
        *centroid*, exactly like ``KG_CELL_FILTER`` in
        ``austria_processor``, so the director's view and the peer's
        view cannot disagree.

        Falls back to ``_austria_cells()`` on any error, and also when
        the scan finds nothing pending anywhere (fleet finished / state
        files unreadable) so we never return an empty cell list — the
        caller treats that as a transient glitch and bails out.
        """
        all_cells = self._austria_cells()
        now = time.time()
        barren = self._barren_cells()
        cached = getattr(self, '_frontier_cells_cache', None)
        if cached and (now - cached[0]) < self.FRONTIER_CELLS_TTL_S:
            return cached[1]
        try:
            kg_path = DATA_DIR / 'kg_list.json'
            if not kg_path.exists():
                return all_cells
            kgs = json.loads(kg_path.read_text())
            try:
                from app import _get_completed_kgs
                completed = _get_completed_kgs()
            except Exception:
                completed = set()
            failed: set = set()
            fp = DATA_DIR / 'failed_kgs.json'
            if fp.exists():
                try:
                    failed = set(json.loads(fp.read_text()))
                except Exception:
                    failed = set()
            if not completed:
                # No completion evidence — can't judge exhaustion.
                return all_cells
            from kg_splitter import maybe_split_kg
            pending_cells: set[tuple[float, float, float, float]] = set()
            # Pending item count per cell (parent = 1, split parent =
            # number of pending blocks) — feeds end-game cell sharing.
            cell_items: dict[tuple, int] = {}
            _mf_ent = None
            for kg in kgs:
                code = kg.get('kg_code')
                bb = kg.get('bbox') or {}
                if not code or code in failed:
                    continue
                try:
                    lat = (float(bb['min_lat']) + float(bb['max_lat'])) / 2
                    lon = (float(bb['min_lon']) + float(bb['max_lon'])) / 2
                except Exception:
                    continue
                cell = None
                for c in all_cells:
                    if c[0] <= lat < c[1] and c[2] <= lon < c[3]:
                        cell = c
                        break
                if cell is None:
                    continue
                if code in completed:
                    # Parent done as a whole — nothing pending here.
                    continue
                try:
                    blocks = maybe_split_kg(kg)
                except Exception:
                    blocks = []
                n_items = 1
                if len(blocks) > 1:
                    n_items = sum(
                        1 for b in blocks
                        if b.get('kg_code') not in completed
                        and b.get('kg_code') not in failed)
                    has_work = n_items > 0
                    # Layout drift: the parent may be covered by blocks
                    # of an OLDER split layout (91123 done as 6 blocks,
                    # today's plan says 8). The processor skips those via
                    # claim collision, so a frontier pinned here finds
                    # 0 items and restart-loops into quarantine — ask the
                    # geometric oracle before calling the cell pending.
                    if has_work:
                        if _mf_ent is None:
                            _mf_ent = _manifest_entries_ro() or {}
                        if _parent_geometrically_done(code, _mf_ent) is True:
                            has_work = False
                else:
                    has_work = True
                if has_work:
                    pending_cells.add(cell)
                    cell_items[cell] = cell_items.get(cell, 0) + n_items
            self._frontier_cell_items = cell_items
            # Fleet-wide pending item count — the run-complete hold
            # (_run_complete_checks) reads this; 0 = run finished.
            self._frontier_pending_total = sum(cell_items.values())
            cells = [c for c in all_cells
                     if c in pending_cells and c not in barren]
            if not cells:
                # Everything pending is quarantined (or nothing pending
                # at all). Prefer pending-but-quarantined over the full
                # set so we don't hand out provably-dead cells.
                cells = [c for c in all_cells if c in pending_cells]
            if not cells:
                return all_cells
            if len(cells) < len(all_cells):
                dead = [c for c in all_cells if c not in cells]
                prev = set((getattr(self, '_frontier_cells_cache', None)
                            or (0, all_cells))[1])
                if set(cells) != prev:
                    log.info('frontier_cells: %d/%d cells have pending work '
                             '(exhausted: %s)', len(cells), len(all_cells),
                             ', '.join('%g-%g/%g-%g' % c for c in dead))
            self._frontier_cells_cache = (now, cells)
            return cells
        except Exception as e:
            log.warning('frontier_cells scan failed: %s', e)
            return all_cells

    def _strip_fingerprint(self, lat_south: float, lat_north: float) -> str:
        """Return a cheap fingerprint of the cache manifest for one lat strip.

        Concatenates the ``updated_at`` of every relevant ZIP for that
        strip across all required products. When ANY ZIP is rewritten
        (a new tile uploaded), the fingerprint changes and any
        cache-miss entries pinned to the old fingerprint expire.
        """
        manifest_path = DATA_DIR / 'cache_manifest.json'
        if not manifest_path.exists():
            return ''
        try:
            d = json.loads(manifest_path.read_text())
        except Exception:
            return ''
        files = d.get('files') or {}
        # Match both old strip ZIPs and new cell ZIPs that fall inside
        # this 1° lat band. Cells are 1°-tall so any cell whose south
        # is in [lat_south, lat_north) overlaps. Strips are 0.5°-tall;
        # treat any strip whose midpoint falls in this band as matching.
        parts = []
        for product in ('ndvi', 'sar', 'harmonics', 'worldcover', 'hansen'):
            prefix = ('hansen_' if product == 'hansen'
                      else f'copernicus_{product}_')
            for name, ent in sorted(files.items()):
                if (ent or {}).get('kind') == 'tile' and (ent or {}).get('product') == product:
                    try:
                        _ts = float(name[:-4].split('_')[1])
                        if lat_south - 1e-9 <= _ts < lat_north - 1e-9:
                            parts.append(name + '@' + str(ent.get('updated_at') or '-'))
                    except Exception:
                        pass
                    continue
                if not name.startswith(prefix):
                    continue
                base_n = name.replace('.zip', '')
                try:
                    if '_cell_' in base_n:
                        coords = base_n.split('_cell_', 1)[1].split('_')
                        s_val = float(coords[0])
                    elif '_strip_' in base_n:
                        coords = base_n.split('_strip_', 1)[1].split('_')
                        s_val = float(coords[0])
                    else:
                        continue
                except Exception:
                    continue
                if not (lat_south - 1e-9 <= s_val < lat_north - 1e-9):
                    continue
                parts.append(name + '@' + str(ent.get('updated_at') or '-'))
        import hashlib
        return hashlib.md5('|'.join(parts).encode()).hexdigest()[:12]

    def _strip_for_bbox(self, bb: dict) -> tuple[float, float] | None:
        s = bb.get('min_lat') or bb.get('south')
        n = bb.get('max_lat') or bb.get('north')
        if s is None or n is None:
            return None
        for ls, ln in self._cached_lat_ranges():
            if s >= ls - 1e-9 and n <= ln + 1e-9:
                return (ls, ln)
        return None

    def _load_cache_misses(self) -> dict:
        """Return {kg_code: {fingerprint, recorded_at, peer_id, strip}}."""
        p = DATA_DIR / 'cache_miss_kgs.json'
        if not p.exists():
            return {}
        try:
            return json.loads(p.read_text()) or {}
        except Exception:
            return {}

    def _save_cache_misses(self, misses: dict):
        p = DATA_DIR / 'cache_miss_kgs.json'
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = p.with_suffix('.tmp')
            tmp.write_text(json.dumps(misses, indent=2))
            os.replace(tmp, p)
        except Exception as e:
            log.warning('save cache_misses failed: %s', e)

    def record_cache_miss(self, kg_code: str, peer_id: str = '?',
                          bbox: dict | None = None,
                          tile_info: str = '') -> dict:
        """Mark a KG as cache-miss.

        Records the *cell* (1°×2°) the KG falls in and a fingerprint
        of that cell's ZIPs. Future cache-only orchestration skips the
        KG until either:
          * the cell's fingerprint changes (its tiles were rewritten
            — i.e. the frontier finished filling it), or
          * 24h elapse (anti-snowball escape valve, in case the cell
            never gets rewritten but the miss was transient).
        Strip is kept for back-compat / dashboard but no longer drives
        the clearing logic — the old strip-keyed approach kept whole
        groups of KGs locked out while the frontier finished a single
        neighbouring cell.
        """
        kg_code = str(kg_code)
        misses = self._load_cache_misses()
        strip = None
        cell = None
        if bbox:
            strip = self._strip_for_bbox(bbox)
            try:
                cell = self._cell_for_bbox(bbox)
            except Exception:
                cell = None
        cell_fp = (self._cell_fingerprint(*cell) if cell else '')
        entry = {
            'fingerprint': cell_fp,  # cell-level (preferred)
            'cell': list(cell) if cell else None,
            'strip': list(strip) if strip else None,
            'strip_fingerprint': (
                self._strip_fingerprint(*strip) if strip else ''),
            'recorded_at': datetime.now(timezone.utc).isoformat(),
            'peer_id': peer_id,
            'tile_info': tile_info,
        }
        misses[kg_code] = entry
        self._save_cache_misses(misses)
        log.info('cache_miss recorded: KG %s (cell=%s fp=%s peer=%s)',
                 kg_code, cell, cell_fp, peer_id)
        # Invalidate the cache-ready KG cache so next tick excludes this KG
        with self._lock:
            self.state.pop('_cache_ready_cache', None)
        return entry

    def _cache_miss_excluded(self) -> set:
        """Return the set of KG codes currently excluded from the
        cache-only whitelist due to unresolved cache misses.

        Clearing rules (any one triggers):
          1. The cell's fingerprint changed since the miss (its tiles
             were rewritten).
          2. Legacy entries with only a strip fingerprint: clear if
             that strip fingerprint changed (back-compat path).
          3. Anti-snowball: any entry older than 24h is dropped
             unconditionally so transient misses can't pin a KG
             forever when the cell happens not to be rewritten.
        """
        misses = self._load_cache_misses()
        if not misses:
            return set()
        cell_fp_cache: dict[tuple, str] = {}
        strip_fp_cache: dict[tuple, str] = {}
        excluded = set()
        changed = False
        for kg, ent in list(misses.items()):
            # Anti-snowball: hard 24h ceiling.
            try:
                age = (datetime.now(timezone.utc) -
                       datetime.fromisoformat(ent['recorded_at'])
                       ).total_seconds()
            except Exception:
                age = 0
            if age > 86400:
                misses.pop(kg, None)
                changed = True
                log.info('cache_miss cleared: KG %s (age %.0fh > 24h)',
                         kg, age / 3600)
                continue

            cleared = False
            cell = ent.get('cell')
            old_cell_fp = ent.get('fingerprint') or ''
            if cell:
                key = (round(cell[0], 4), round(cell[1], 4),
                       round(cell[2], 4), round(cell[3], 4))
                cur = cell_fp_cache.get(key)
                if cur is None:
                    cur = self._cell_fingerprint(*key)
                    cell_fp_cache[key] = cur
                if cur and cur != old_cell_fp:
                    misses.pop(kg, None)
                    changed = True
                    cleared = True
                    log.info('cache_miss cleared: KG %s (cell %s fp %s→%s)',
                             kg, key, old_cell_fp, cur)
            else:
                # Legacy strip-only entry. Clear on strip churn so old
                # records eventually drain.
                strip = ent.get('strip')
                if strip:
                    sk = (round(strip[0], 1), round(strip[1], 1))
                    cur = strip_fp_cache.get(sk)
                    if cur is None:
                        cur = self._strip_fingerprint(strip[0], strip[1])
                        strip_fp_cache[sk] = cur
                    old_strip_fp = (ent.get('strip_fingerprint')
                                    or old_cell_fp)
                    if cur and cur != old_strip_fp:
                        misses.pop(kg, None)
                        changed = True
                        cleared = True
                        log.info('cache_miss cleared: KG %s (legacy strip %s fp %s→%s)',
                                 kg, sk, old_strip_fp, cur)
            if not cleared:
                excluded.add(kg)
        if changed:
            self._save_cache_misses(misses)
        return excluded

    def _cached_cells(self) -> list[tuple[float, float, float, float]]:
        """Cells (s,n,w,e) fully cached for ALL required products.

        Stricter than :py:meth:`_cached_lat_ranges` — that one only
        checks lat coverage and lets a KG sneak past the pre-filter
        when its specific cell isn't yet uploaded. Used by the
        cache-ready whitelist so cache-only peers never get a KG
        whose cell is mid-fill on the frontier (which would abort
        with a cache miss and snowball into ``cache_miss_kgs.json``).
        """
        manifest_path = DATA_DIR / 'cache_manifest.json'
        if not manifest_path.exists():
            return []
        try:
            d = json.loads(manifest_path.read_text())
        except Exception:
            return []
        files = d.get('files') or {}
        per_product_cells: dict[str, set[tuple[float, float, float, float]]] = {}
        # Collect legacy strip lat-ranges per product so we can stitch
        # 0.5° strips into 1° lat bands for the austria_cells expansion.
        per_product_strip_ranges: dict[str, list[tuple[float, float]]] = {}
        for name, _entry in files.items():
            # Skip size=0 tombstones (deleted/404'd ZIPs — e.g. the Jul
            # 2026 deposit-cap loss) so they don't count as coverage.
            if not ((_entry or {}).get('size', 0) or 0):
                continue
            # Tile store v2 object (zenodo_tiles): one 0.1° tile → counts
            # toward its 1°×2° planning cell like a v1 ZIP would.
            if (_entry or {}).get('kind') == 'tile' or (name.endswith('.npz') and (_entry or {}).get('depo_id')):
                try:
                    _parts = name[:-4].split('_')
                    _prod = _entry.get('product') or _parts[0]
                    _ts, _tw = float(_parts[1]), float(_parts[2])
                    for _pc in self._austria_cells():
                        if _pc[0] - 1e-9 <= _ts < _pc[1] - 1e-9 and _pc[2] - 1e-9 <= _tw < _pc[3] - 1e-9:
                            per_product_cells.setdefault(_prod, set()).add(_pc)
                            break
                except Exception:
                    pass
                continue
            base_n = name.replace('.zip', '')
            try:
                if '_cell_' in base_n:
                    head, coords = base_n.split('_cell_', 1)
                    s, n, w, e = (float(x) for x in coords.split('_'))
                    product = head
                    if product.startswith('copernicus_'):
                        product = product[len('copernicus_'):]
                    # v1 ZIP cell (1°×2°) → every planning cell inside it
                    per_product_cells.setdefault(product, set()).update(
                        self._planning_cells_within(s, n, w, e, self._austria_cells()))
                elif '_strip_' in base_n:
                    head, coords = base_n.split('_strip_', 1)
                    s, n = (float(x) for x in coords.split('_'))
                    product = head
                    if product.startswith('copernicus_'):
                        product = product[len('copernicus_'):]
                    per_product_strip_ranges.setdefault(
                        product, []).append((s, n))
            except Exception:
                continue
        # Expand legacy strips into cell coverage: a cell (cs,cn,cw,ce)
        # is covered by legacy strips for product P iff every sub-band
        # within [cs,cn] is covered by the union of P's strips. Cheap
        # check via interval merging.
        def _merge(ranges: list[tuple[float, float]]
                   ) -> list[tuple[float, float]]:
            if not ranges:
                return []
            r = sorted(ranges)
            out = [list(r[0])]
            for s, n in r[1:]:
                if s <= out[-1][1] + 1e-9:
                    out[-1][1] = max(out[-1][1], n)
                else:
                    out.append([s, n])
            return [(a, b) for a, b in out]
        try:
            all_cells = self._austria_cells()
        except Exception:
            all_cells = []
        for product, ranges in per_product_strip_ranges.items():
            merged = _merge(ranges)
            bucket = per_product_cells.setdefault(product, set())
            for cs, cn, cw, ce in all_cells:
                for ms, mn in merged:
                    if cs >= ms - 1e-9 and cn <= mn + 1e-9:
                        bucket.add((round(cs, 4), round(cn, 4),
                                    round(cw, 4), round(ce, 4)))
                        break
        required = ['ndvi', 'sar', 'harmonics', 'worldcover', 'hansen']
        if not all(p in per_product_cells for p in required):
            return []
        common = set.intersection(
            *[per_product_cells[p] for p in required])
        return sorted(common)

    def _cell_for_bbox(self, bb: dict) -> tuple[float, float, float, float] | None:
        """Return the cached cell that fully contains *bb*; for a
        straddler (bbox crossing a planning-cell edge) the cached cell
        holding its centroid; else None."""
        s = bb.get('min_lat') or bb.get('south')
        n = bb.get('max_lat') or bb.get('north')
        w = bb.get('min_lon') or bb.get('west')
        e = bb.get('max_lon') or bb.get('east')
        if None in (s, n, w, e):
            return None
        cells = self._cached_cells()
        for cs, cn, cw, ce in cells:
            if (s >= cs - 1e-9 and n <= cn + 1e-9
                    and w >= cw - 1e-9 and e <= ce + 1e-9):
                return (cs, cn, cw, ce)
        cy, cx = (s + n) / 2.0, (w + e) / 2.0
        for cs, cn, cw, ce in cells:
            if cs - 1e-9 <= cy < cn - 1e-9 and cw - 1e-9 <= cx < ce - 1e-9:
                return (cs, cn, cw, ce)
        return None

    @staticmethod
    def _bbox_covered_by_cells(s: float, n: float, w: float, e: float,
                               cells) -> bool:
        """True when every planning cell the bbox (s,n,w,e) intersects is
        in *cells* (an iterable of (s,n,w,e) tuples aligned to the
        FRONTIER_CELL_LAT × FRONTIER_CELL_LON grid).

        The old "bbox inside ONE cached cell" test silently excluded
        every *straddler* KG (bbox crossing a 0.5°/1° cell edge, ~6 % of
        all KGs) from the cache-only whitelist for the whole run; once
        the frontier had drained the single-cell KGs, the tail of the
        run (2026-11-06: 57 of the last 59 KGs) sat fully cached but
        undispatchable while 45 cache-only peers idled.
        """
        import math as _m
        lat_step, lon_step = FRONTIER_CELL_LAT, FRONTIER_CELL_LON
        if lat_step <= 0 or lon_step <= 0:
            return False
        cset = {(round(a, 4), round(b, 4), round(c, 4), round(d, 4))
                for a, b, c, d in cells}
        lat0 = _m.floor((s + 1e-9) / lat_step)
        lat1 = _m.floor((n - 1e-9) / lat_step)
        lon0 = _m.floor((w + 1e-9) / lon_step)
        lon1 = _m.floor((e - 1e-9) / lon_step)
        if lat1 < lat0 or lon1 < lon0:
            return False
        for i in range(lat0, lat1 + 1):
            for j in range(lon0, lon1 + 1):
                key = (round(i * lat_step, 4), round((i + 1) * lat_step, 4),
                       round(j * lon_step, 4), round((j + 1) * lon_step, 4))
                if key not in cset:
                    return False
        return True

    def _cell_fingerprint(self, cs: float, cn: float,
                          cw: float, ce: float) -> str:
        """Fingerprint of all required-product ZIPs for one cell.

        Tighter than :py:meth:`_strip_fingerprint`: a miss recorded
        against cell (s,n,w,e) clears as soon as THAT cell's ZIPs are
        rewritten, instead of waiting for any tile anywhere in the
        strip.
        """
        manifest_path = DATA_DIR / 'cache_manifest.json'
        if not manifest_path.exists():
            return ''
        try:
            import json_cache
            d = json_cache.load(manifest_path) or {}
        except Exception:
            return ''
        files = d.get('files') or {}
        parts = []
        for product in ('ndvi', 'sar', 'harmonics', 'worldcover', 'hansen'):
            prefix = ('hansen_' if product == 'hansen'
                      else f'copernicus_{product}_')
            for name, ent in sorted(files.items()):
                if (ent or {}).get('kind') == 'tile' and (ent or {}).get('product') == product:
                    # tile store v2 object inside this planning cell?
                    try:
                        _p = name[:-4].split('_')
                        _ts, _tw = float(_p[1]), float(_p[2])
                        if cs - 1e-9 <= _ts < cn - 1e-9 and cw - 1e-9 <= _tw < ce - 1e-9:
                            parts.append(name + '@' + str(ent.get('updated_at') or '-'))
                    except Exception:
                        pass
                    continue
                if not name.startswith(prefix):
                    continue
                base_n = name.replace('.zip', '')
                try:
                    if '_cell_' in base_n:
                        coords = base_n.split('_cell_', 1)[1].split('_')
                        s_val, n_val, w_val, e_val = (float(x) for x in coords[:4])
                        # ZIP cell must contain this (possibly finer) planning cell
                        if not (s_val - 1e-9 <= cs < n_val - 1e-9
                                and w_val - 1e-9 <= cw < e_val - 1e-9):
                            continue
                    elif '_strip_' in base_n:
                        coords = base_n.split('_strip_', 1)[1].split('_')
                        s_val = float(coords[0])
                        if not (cs - 1e-9 <= s_val < cn - 1e-9):
                            continue
                    else:
                        continue
                except Exception:
                    continue
                parts.append(name + '@' + str(ent.get('updated_at') or '-'))
        import hashlib
        return hashlib.md5('|'.join(parts).encode()).hexdigest()[:12]

    def _cached_lat_ranges(self) -> list[tuple[float, float]]:
        """Lat strips covered by ALL required products in the Zenodo cache.

        Reads ``cache_manifest.json`` and intersects strip availability
        across ndvi, sar, harmonics, worldcover, hansen.  Returns
        list of (south, north) lat pairs.  Cheap; lets the predicate
        skip the >90 %% of KGs that fall outside any covered strip.
        """
        manifest_path = DATA_DIR / 'cache_manifest.json'
        if not manifest_path.exists():
            return []
        try:
            d = json.loads(manifest_path.read_text())
        except Exception:
            return []
        files = d.get('files') or {}
        # Parse both legacy strip names and new cell names. We only
        # care about latitude coverage here -- frontier cell
        # disjointness is enforced separately via _austria_cells().
        # Normalise to the new 1°-tall lat band so legacy 0.5°
        # strips and new 1° cells line up in the intersection.
        import math as _m
        def _norm_lat_band(s: float) -> tuple[float, float]:
            base = _m.floor(s / 1.0) * 1.0
            return (round(base, 4), round(base + 1.0, 4))
        per_product: dict[str, set[tuple[float, float]]] = {}
        for name, _entry in files.items():
            # Skip size=0 tombstones (deleted/404'd ZIPs).
            if not ((_entry or {}).get('size', 0) or 0):
                continue
            try:
                base_n = name.replace('.zip', '')
                if '_cell_' in base_n:
                    head, coords = base_n.split('_cell_', 1)
                    s, n, _w, _e = coords.split('_')
                elif '_strip_' in base_n:
                    head, coords = base_n.split('_strip_', 1)
                    s, n = coords.split('_')
                else:
                    continue
                product = head
                if product.startswith('copernicus_'):
                    product = product[len('copernicus_'):]
                per_product.setdefault(product, set()).add(
                    _norm_lat_band(float(s)))
            except Exception:
                continue
        required = ['ndvi', 'sar', 'harmonics', 'worldcover', 'hansen']
        if not all(p in per_product for p in required):
            return []
        common = set.intersection(*[per_product[p] for p in required])
        return sorted(common)

    def _product_repair_plan(self, cop_ok: bool) -> tuple[list, list]:
        """(repair_cells, repair_codes) for the ``prewarm`` block.

        Runs ``product_repair.sweep`` at most every
        ``PRODUCT_REPAIR_SWEEP_S`` (refreshes pending→fixable states from
        the Zenodo ZIP index); otherwise replays the cached plan. Cells
        are only published while Copernicus is healthy AND the fleet
        openEO sync path had no read-timeouts / cascades in the last
        hour (``app._openeo_health``). Codes are published regardless
        — they only unlock the peer-side eligibility for already-cached
        repairs.
        """
        now = time.time()
        st = self.state.get('_product_repair') or {}
        if now - float(st.get('at') or 0) >= PRODUCT_REPAIR_SWEEP_S:
            try:
                import product_repair as _pr
                r = _pr.sweep(max_cells=PRODUCT_REPAIR_MAX_CELLS)
                reg = _pr.load()
                codes = sorted(c for c, e in reg.items() if e.get('state') == 'fixable')
                st = {'at': now, 'cells': r.get('cells') or [], 'codes': codes[:PRODUCT_REPAIR_MAX_CODES],
                      'counts': {k: r.get(k) for k in ('pending', 'fixable', 'stuck', 'done')}}
                for c in r.get('promoted') or []:
                    try:
                        _emit_director_event(
                            f"repair {c}: all missing cells cached — eligible for re-upgrade "
                            f"({','.join(reg.get(c, {}).get('defects') or [])})",
                            peer='director', kg=c)
                    except Exception:
                        pass
                if r.get('promoted'):
                    with self._lock:
                        self.state.pop('_v2_cand_cache', None)  # re-rank now
                with self._lock:
                    self.state['_product_repair'] = st
            except Exception as e:
                log.debug('product_repair plan: %s', e)
                return list(st.get('cells') or []) if cop_ok else [], list(st.get('codes') or [])
        openeo_ok = True
        try:
            import app as _app
            oh = _app._openeo_health()
            # "Serving" gate, not "spotless" gate: with ~40 peers on
            # openEO there is a stray read-timeout almost every hour,
            # and the old ``not timeouts`` test kept repair cells
            # unpublished for 3 days straight (0 cells filled 09-21…24).
            # Block only on real upstream stress: a cascade / deferred
            # KG, or timeouts outnumbering successful months.
            to = int(oh.get('timeouts') or 0)
            ok_m = int(oh.get('months_ok') or 0)
            openeo_ok = not (oh.get('cascades') or oh.get('defers')) \
                and (to <= PRODUCT_REPAIR_OPENEO_MAX_TIMEOUTS or to <= ok_m)
        except Exception:
            pass
        cells = list(st.get('cells') or []) if (cop_ok and openeo_ok) else []
        if cop_ok and openeo_ok:
            # Sliver-sweep units on hold (cache-only peer hit an uncached
            # openEO cell) ride the same frontier fill; docs/sliver-sweep.md.
            try:
                import sliver_sweep as _sls
                have = {(c.get('w'), c.get('s')) for c in cells}
                for c in _sls.held_cells():
                    if (c.get('w'), c.get('s')) not in have and 'w' in c:
                        cells.append(c); have.add((c['w'], c['s']))
                    if len(cells) >= 2 * PRODUCT_REPAIR_MAX_CELLS:
                        break
            except Exception as e:
                log.debug('sweep held cells: %s', e)
        with self._lock:
            self.state['_product_repair_gate'] = {
                'cop_ok': bool(cop_ok), 'openeo_ok': bool(openeo_ok),
                'published': len(cells)}
        return cells, list(st.get('codes') or [])

    def _update_prewarm_flag(self, cache_ready_count: int):
        """Set the frontier cell pre-warm directive in cache_manifest.json.

        Demand-side TRIGGER: enable while ``cache_ready_count`` is below
        ``PREWARM_CACHE_READY_LOW`` (hysteresis off above
        ``PREWARM_CACHE_READY_HIGH``) — i.e. when the cache-only fleet is
        starved for ready KGs.

        Supply-side GATE: only enable when Copernicus is healthy
        (``sub_factor_ema['copernicus'] >= PREWARM_COP_EMA_MIN`` AND not
        circuit-paused). ``cells_per_kg`` scales with the Copernicus EMA so
        we back off the extra fetches as upstream pressure rises — mirrors
        how ``_max_cache_only_peers`` damps on ``cpu_factor``.

        Written into the manifest under ``prewarm`` so it rides the
        existing primary→peer cache_manifest sync (no restart, no new
        endpoint). Idempotent: only rewrites when the directive changes.
        """
        try:
            cop_ema = float(self._sub_factor_ema.get('copernicus',
                                                     THROTTLE_MAX_FACTOR))
        except Exception:
            cop_ema = THROTTLE_MAX_FACTOR
        cop_paused = (DATA_DIR / 'copernicus_paused').exists()

        # Hysteresis on the demand trigger using the last-published state.
        manifest_path = DATA_DIR / 'cache_manifest.json'
        prev = {}
        try:
            if manifest_path.exists():
                prev = (json.loads(manifest_path.read_text())
                        .get('prewarm') or {})
        except Exception:
            prev = {}
        was_on = bool(prev.get('enabled'))
        if was_on:
            demand = cache_ready_count < PREWARM_CACHE_READY_HIGH
        else:
            demand = cache_ready_count < PREWARM_CACHE_READY_LOW

        supply_ok = (cop_ema >= PREWARM_COP_EMA_MIN) and not cop_paused
        enabled = bool(demand and supply_ok)

        # Repairable-layer gaps (product_repair): exact cells the frontier
        # should fetch + codes the peer-side eligibility must accept even
        # though its manifest copy says "current". Same transport, gated
        # on openEO health (no timeouts / cascades in the last hour) —
        # filling the very cells that failed while openEO is still
        # timing out would just burn 240 s probes.
        repair_cells, repair_codes = self._product_repair_plan(supply_ok)
        _prev_rc = prev.get('repair_cells') or []
        _prev_codes = prev.get('repair_codes') or []

        if enabled:
            span = PREWARM_CELLS_PER_KG_MAX - PREWARM_CELLS_PER_KG_MIN
            # Linear ramp from MIN (at the gate threshold) to MAX (at 1.0).
            frac = (cop_ema - PREWARM_COP_EMA_MIN) / max(
                1e-6, (THROTTLE_MAX_FACTOR - PREWARM_COP_EMA_MIN))
            frac = max(0.0, min(1.0, frac))
            cells = PREWARM_CELLS_PER_KG_MIN + int(round(span * frac))
        else:
            cells = 0

        # No-op if unchanged (avoid churning the manifest mtime + sync).
        if (was_on == enabled
                and int(prev.get('cells_per_kg', 0) or 0) == cells
                and _prev_rc == repair_cells and _prev_codes == repair_codes):
            return
        try:
          from zenodo_cache import cache_manifest_write_lock as _cmwl
          with _cmwl():
            data = {}
            if manifest_path.exists():
                data = json.loads(manifest_path.read_text())
            data['prewarm'] = {
                'enabled': enabled,
                'cells_per_kg': cells,
                'repair_cells': repair_cells,
                'repair_codes': repair_codes,
                'updated_at': datetime.now(timezone.utc).isoformat(),
            }
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = str(manifest_path) + '.prewarm.tmp'
            with open(tmp, 'w') as f:
                json.dump(data, f, indent=2, sort_keys=True)
            os.replace(tmp, manifest_path)
            log.info('prewarm flag: enabled=%s cells_per_kg=%d '
                     '(cache_ready=%d cop_ema=%.2f paused=%s)',
                     enabled, cells, cache_ready_count, cop_ema, cop_paused)
        except Exception as e:
            log.debug('prewarm flag write failed: %s', e)

    def _compute_v2_upgrade_candidates(self, max_n: int = 400) -> list[str]:
        """Product codes (parents AND split blocks, straight from the
        manifest) that are v1-complete (committed ``_json`` + ``_full_gpkg``)
        and have no ``_json_v2`` yet. Smallest full GPKG first (cheapest
        download, quickest fleet-wide feedback). Cached 5 min. Peer-local
        strikes (``v2_upgrade_failed.json``) are honoured on the peer; the
        director additionally skips codes whose primary ingest struck out.
        """
        now = time.time()
        cached = self.state.get('_v2_cand_cache') or {}
        if cached.get('codes') is not None and (now - cached.get('at', 0)) < V2_UPGRADE_CAND_TTL_S:
            return cached['codes']
        codes: list[str] = []
        try:
            mf_path = DATA_DIR / 'zenodo_manifest.json'
            raw = json.loads(mf_path.read_text()) if mf_path.exists() else {}
            ent = raw.get('entries', raw) or {}
            skip = set()
            # Primary-ingest strike-outs only disqualify a code whose v2
            # pair is actually complete (genuine content problem). A
            # half-landed pair (json_v2, no light_gpkg_v2) fails ingest
            # with 'no committed _light_gpkg_v2 entry' — re-upgrading is
            # exactly the fix, and the fresh uploaded_at resets the
            # ingest strike (v2_ingest.pending keys on it).
            skip_ingest = set()
            try:
                import v2_ingest as _v2i
                skip_ingest = {c for c, st in _v2i.failed().items()
                               if int(st.get('n', 0)) >= _v2i.MAX_FAILS}
            except Exception:
                pass
            try:
                skip |= v2_fleet_struck_out()
            except Exception:
                pass
            # rank key: (product-line bucket, full GPKG size).  Since 2.3
            # (V2_REUPGRADE_FIRST) codes that already carry an OLDER v2
            # product (2.0/2.1/2.2) go BEFORE never-upgraded v1 codes: the
            # 2.3 change (robust top-N ranking) rewrites what every v2 JSON
            # says, so the live v2 product line is brought to one clean
            # version first; v1-only KGs already read as "not v2" and keep
            # their place in the queue (docs/v2-upgrade.md → Priority).
            from v21_products import MANIFEST_VERSION as _V21, v2_products_complete as _v2c
            import product_repair as _pr
            _repair_reg = _pr.load()
            import v24_fast as _v24
            # v2.4: the declared NE layer must be built before
            # a code is worth dispatching (the peer skips an unbuilt KG
            # without a strike, but a slice full of them churns the peer
            # to COMPLETE every few minutes).  Knob ``ne_require_built``.
            _ne_gate = bool(knob(None, 'ne_require_built', True))
            _built: set = set()
            if _ne_gate:
                try:
                    import ne_rollout as _ner
                    _built = _ner.ne_built_codes()
                except Exception as _be:  # noqa: BLE001
                    log.debug('ne built codes: %s', _be)
                if not _built:
                    _ne_gate = False        # no manifest at all → don't starve the fleet
            ranked = []
            n_reup = 0
            n_partial_wait = 0
            n_fast = 0
            n_fast_nofull = 0
            n_ne_wait = 0
            for k, e in ent.items():
                if not k.endswith('_json') or not isinstance(e, dict):
                    continue
                if 'error' in str(e.get('status', '')):
                    continue
                code = k[:-5]
                if code in skip:
                    continue
                j2 = ent.get(f'{code}_json_v2')
                if isinstance(j2, dict):
                    if _v2c(ent, code):
                        continue
                    n_reup += 1
                elif code in skip_ingest:
                    continue  # stale ingest strike without json_v2 — leave to ingest
                if _ne_gate and code.split('-', 1)[0] not in _built:
                    n_ne_wait += 1
                    continue
                # v2.4 fast path: a committed v2.3+ pair is relabelled from
                # its light GPKG (``v24_fast``) — no full GPKG needed.
                _fast = _v24.eligible(code, ent)[0]
                # Repairable-layer gap ('-partial' label, product_repair):
                # only worth a re-upgrade once every missing cell is in
                # the Zenodo cache, else the run comes back partial again.
                # Stuck codes (MAX_ATTEMPTS still partial) are parked.
                # Operator decision 2026-10-04: Copernicus-gap partials
                # WAIT for their cells and go through the FULL path (the
                # peer declines the fast path for them); only partials
                # whose sole defect is ``ne_cells`` are fast candidates.
                _newest = j2 if isinstance(j2, dict) else e
                _rr = _repair_reg.get(code)
                # Only an OPEN registry record carries live defects; a
                # ``done`` one is history (the peer re-checks the real doc).
                _ne_only = bool(_rr and _rr.get('state') != 'done'
                                and not (set(_rr.get('defects') or []) - {'ne_cells'}))
                if _pr.is_partial(_newest.get('version')) or (_rr and _rr.get('state') in ('pending', 'fixable', 'stuck')):
                    if not _rr or _rr.get('state') == 'stuck' or not _pr.fixable(code, _rr):
                        n_partial_wait += 1
                        continue
                    if not _ne_only:
                        _fast = False          # full path repairs the cells
                g = ent.get(f'{code}_full_gpkg')
                _has_full = isinstance(g, dict) and int(g.get('size') or 0) > 0 and bool(g.get('uploaded_at'))
                if not _has_full:
                    if not _fast:
                        continue
                    n_fast_nofull += 1
                if _fast:
                    n_fast += 1
                # Rank: -1 = current json_v2 but light_gpkg_v2 missing (a
                # half-landed pair; heal first — it's inconsistent on
                # Zenodo); then product<current re-upgrade vs never
                # upgraded, ordered by V2_REUPGRADE_FIRST.
                _lg = ent.get(f'{code}_light_gpkg')
                _lg2 = ent.get(f'{code}_light_gpkg_v2')
                _has_light = any(isinstance(x, dict) and int(x.get('size') or 0) > 0
                                 and 'error' not in str(x.get('status', ''))
                                 for x in (_lg, _lg2))
                if isinstance(j2, dict) and str(j2.get('version') or '') == _V21:
                    _rk = -1
                elif _fast:
                    # fast relabel: minutes, light GPKG traffic only —
                    # between half-landed (-1) and re-upgrade (0)
                    _rk = -0.5
                elif not _has_light:
                    # json + full_gpkg but NO light GPKG at all (v1 light
                    # upload failed / fresh-v2 light_gpkg_v2 PUT failed):
                    # the product triple is broken on Zenodo and the
                    # coverage oracle never requeues it (own _json is
                    # load-bearing). Upgrading from the full GPKG is the
                    # heal -> rank first (process.txt verdict 'v2heal').
                    _rk = -2
                elif isinstance(j2, dict):
                    _rk = 0 if V2_REUPGRADE_FIRST else 1
                else:
                    _rk = 1 if V2_REUPGRADE_FIRST else 0
                if _fast:
                    _sz = int((_lg2 or {}).get('size') or 0)
                else:
                    _sz = int(g.get('size') or 0)
                ranked.append((_rk, _sz, code))
            ranked.sort()
            codes = [c for _, _, c in ranked[:max_n]]
            fast_set = {c for rk, _, c in ranked if rk == -0.5}
            if n_fast or n_ne_wait:
                log.info('v2 upgrade candidates: %d fast-eligible (v2.4 relabel; %d without _full_gpkg), '
                         '%d full-path, %d partial waiting, %d waiting for NE build (%d built)',
                         n_fast, n_fast_nofull, len(ranked) - n_fast, n_partial_wait, n_ne_wait, len(_built))
            if n_partial_wait:
                log.debug('v2 upgrade candidates: %d partial code(s) waiting for cache cells / parked',
                          n_partial_wait)
            if n_reup:
                log.debug('v2 upgrade candidates: %d product<%s re-upgrade(s) queued %s '
                          'never-upgraded codes', n_reup, _V21,
                          'BEFORE' if V2_REUPGRADE_FIRST else 'after')
        except Exception as e:
            log.warning('v2 upgrade candidates: %s', e)
        with self._lock:
            self.state['_v2_cand_cache'] = {'codes': codes, 'at': now,
                                            'total': len(codes), 'fast_total': len(fast_set),
                                            'ne_wait': n_ne_wait, 'ne_built': len(_built)}
        self._v2_fast_set = fast_set          # loop-worker only; not persisted
        return codes

    def _v24_first(self, cfg: dict | None = None) -> bool:
        """True while the v2.4 legacy clean-up wave should take precedence
        over fresh cache-ready work.  Knob ``v24_first`` (director_config);
        default = legacy suspect files remain on Zenodo (ne_rollout, 60 s
        cache).  Logs once when it auto-clears."""
        v = knob(cfg, 'v24_first', None)
        if v is not None:
            return bool(v)
        try:
            import ne_rollout as _ner
            n = int((_ner.summary().get('legacy') or {}).get('suspect_files') or 0)
        except Exception:  # noqa: BLE001
            return False
        on = n > 0
        if not on and getattr(self, '_v24_first_was', True):
            log.info('v24: archive clean — v24_first auto-cleared, normal fill')
        self._v24_first_was = on
        return on

    def _v2_fast_codes(self) -> set:
        """Fast-eligible subset of the current candidate list (same cache)."""
        self._compute_v2_upgrade_candidates()
        return set(getattr(self, '_v2_fast_set', None) or ())

    def _v2_complete_fresh(self) -> set:
        """Codes whose v2 pair is complete per a FRESH manifest read (60 s
        TTL).  Post-filter for ``_v2_upgrade_fill``: the candidate list is
        cached 2 min, a fast unit finishes in ~5, so without this a code
        done moments ago is re-dispatched (the peer would re-download the
        light GPKG before its own manifest copy says 'current')."""
        now = time.time()
        c = getattr(self, '_v2_fresh_cache', None) or {}
        if c.get('codes') is not None and now - c.get('at', 0) < V2_COMPLETE_FRESH_TTL_S:
            return c['codes']
        done: set = set()
        try:
            from v21_products import v2_products_complete as _v2c
            mf_path = DATA_DIR / 'zenodo_manifest.json'
            raw = json.loads(mf_path.read_text()) if mf_path.exists() else {}
            ent = raw.get('entries', raw) or {}
            for k in ent:
                if k.endswith('_json_v2'):
                    code = k[:-8]
                    if _v2c(ent, code):
                        done.add(code)
        except Exception as e:  # noqa: BLE001
            log.debug('v2 complete fresh: %s', e)
        self._v2_fresh_cache = {'codes': done, 'at': now}
        return done

    def _v2_upgrade_fill(self, whitelist: list, cfg: dict,
                         exclude: set | None = None,
                         min_units: int = 0) -> list[str]:
        """Upgrade codes to append to the cache-only whitelist this tick
        (empty when disabled, whitelist not low, or Zenodo is unhappy).

        ``exclude`` — codes already claimed by a running peer; they are
        dropped *before* the cap is applied so the cap counts *new*
        units. ``min_units`` lifts the cap to the number of cache-only
        workers this tick — otherwise (Sep 2026) a 24-code cap minus
        ~6 in-flight claims left 18 codes for 26 workers and a handful
        of idle peers were never handed anything."""
        if not knob(cfg, 'v2_upgrade', True):
            return []
        # v2.4-first wave (docs/v2.4-rollout.md): while Zenodo still holds
        # legacy files, upgrade units are injected REGARDLESS of
        # how much fresh cache-ready work exists (2026-10-04: 200 ready
        # KGs kept this gate shut → 0 candidates dispatched, v2.4 stuck at
        # the 3 hand-run test KGs).  Fresh work stays as filler behind
        # them; ``_v24_first()`` auto-clears at suspect_files == 0.
        if len(whitelist) >= int(knob(cfg, 'v2_upgrade_fill_below_ready',
                                      V2_UPGRADE_FILL_BELOW_READY)) \
                and not self._load_v2_priority() and not self._v24_first(cfg):
            return []
        try:
            zen_rate = float((self._capacity_components.get('rates') or {}).get('zenodo') or 0.0)
        except Exception:
            zen_rate = 0.0
        if zen_rate > V2_UPGRADE_ZEN_WARN_MAX:
            log.info('v2 upgrade: Zenodo warns %.2f/min > %.1f — no upgrade '
                     'units this tick', zen_rate, V2_UPGRADE_ZEN_WARN_MAX)
            return []
        cap = int(knob(cfg, 'max_v2_upgrade_peers', MAX_V2_UPGRADE_PEERS))
        cap = max(cap, int(min_units))
        cands = self._compute_v2_upgrade_candidates()
        # Fresh-manifest post-filter: drop codes whose pair went complete
        # since the (2 min) candidate cache was built.
        done_now = self._v2_complete_fresh()
        skip = set(whitelist) | set(exclude or ()) | done_now
        pri = [c for c in self.v2_priority_codes() if c not in skip]
        rest = [c for c in cands if c not in skip and c not in set(pri)]
        cap = max(0, cap)
        ordered = pri + rest
        picked = ordered[:cap]
        # v24_fast governor: at most ``v24_fast_max_peers`` fast units in
        # flight fleet-wide (Zenodo shared-token budget, see constant).
        # ``exclude`` = codes running right now; fast ones among them use
        # slots.  Non-fast (full-path) units are not governed here.
        fast = self._v2_fast_codes()
        if fast:
            fmax = int(knob(cfg, 'v24_fast_max_peers', V24_FAST_MAX_PEERS))
            running_fast = len([c for c in (exclude or ()) if c in fast])
            slots = max(0, fmax - running_fast)
            out, n_fast_new, held = [], 0, 0
            for c in ordered:               # full list: full-path units fill behind held fast ones
                if len(out) >= cap:
                    break
                if c in fast:
                    if n_fast_new >= slots:
                        held += 1
                        continue
                    n_fast_new += 1
                out.append(c)
            if held:
                _sig = (running_fast, slots, held)
                if _sig != getattr(self, '_v24_gov_sig', None):
                    self._v24_gov_sig = _sig
                    log.info('v24 fast governor: %d fast running, %d slot(s) of %d — held back %d fast unit(s)',
                             running_fast, slots, fmax, held)
            picked = out
            with self._lock:
                self.state['v24_fast_gov'] = {'running': running_fast, 'max': fmax,
                                              'new': n_fast_new, 'at': time.time()}
        return picked

    # --- Operator-pinned v2 upgrade priority (``POST /api/v1/director/v2/priority``)
    # Kept in its own file (not director_state.json) because the POST lands
    # on whichever gunicorn worker Flask picks, while the director loop
    # runs in the other one with its own in-memory ``self.state``.
    _V2_PRIORITY_FILE = DATA_DIR / 'v2_priority.json'

    def _load_v2_priority(self) -> list[str]:
        try:
            v = json.loads(self._V2_PRIORITY_FILE.read_text())
            return [str(c) for c in v] if isinstance(v, list) else []
        except Exception:
            return []

    def _save_v2_priority(self, codes: list[str]) -> None:
        tmp = self._V2_PRIORITY_FILE.with_suffix('.tmp')
        tmp.write_text(json.dumps(codes))
        tmp.replace(self._V2_PRIORITY_FILE)

    def v2_priority_codes(self) -> list[str]:
        """Operator-pinned product codes to upgrade *first*. Pruned
        automatically once ``_json_v2`` lands (or the code is no longer
        v1-complete)."""
        codes = self._load_v2_priority()
        if not codes:
            return []
        try:
            mf_path = DATA_DIR / 'zenodo_manifest.json'
            raw = json.loads(mf_path.read_text()) if mf_path.exists() else {}
            ent = raw.get('entries', raw) or {}
        except Exception:
            return codes
        keep = []
        from v21_products import MANIFEST_VERSION as _V21, v2_products_complete as _v2c
        for c in codes:
            if _v2c(ent, c):
                log.info('v2 priority: %s upgraded to %s — dropping from priority list', c, _V21)
                continue
            g = ent.get(f'{c}_full_gpkg')
            if f'{c}_json' not in ent or not isinstance(g, dict) \
                    or int(g.get('size') or 0) <= 0:
                log.warning('v2 priority: %s not v1-complete — dropping', c)
                continue
            keep.append(c)
        if keep != codes:
            self._save_v2_priority(keep)
        return keep

    def set_v2_priority(self, codes: list, replace: bool = False) -> list[str]:
        cur = [] if replace else self._load_v2_priority()
        for c in codes:
            c = str(c).strip()
            if c and c not in cur:
                cur.append(c)
        self._save_v2_priority(cur)
        return cur

    def remove_v2_priority(self, codes: list) -> list[str]:
        drop = {str(c).strip() for c in codes}
        cur = [c for c in self._load_v2_priority() if c not in drop]
        self._save_v2_priority(cur)
        return cur

    def _compute_cache_ready_kgs(self, max_kgs: int = 200) -> list[str]:
        """Return KG codes that are fully present in the local+Zenodo cache.

        Two-stage: cheap lat-strip filter (intersects covered strips
        across all products) followed by per-cell check via
        ``tile_cache.is_kg_fully_cached(bbox)``.  Result is cached for
        5 minutes — the cache extends as the frontier peer fetches new
        tiles.
        """
        now = time.time()
        cached = self.state.get('_cache_ready_cache') or {}
        if cached.get('codes') is not None and (now - cached.get('at', 0)) < 300:
            return cached['codes']

        codes: list[str] = []
        try:
            from tile_cache import (CopernicusTileCache, HansenTileCache,
                                     is_kg_fully_cached)
            cop_cache = CopernicusTileCache()
            hansen_cache = HansenTileCache()

            # Use cell-strict filter so we never whitelist a KG
            # whose specific cell is mid-fill on the frontier —
            # the strip-level pre-filter let those through and they
            # would abort with a cache miss, snowballing into
            # cache_miss_kgs.json.
            cached_cells = self._cached_cells()
            if not cached_cells:
                log.info('Cache-ready scan: no fully-cached cell yet')
                with self._lock:
                    self.state['_cache_ready_cache'] = {'codes': [], 'at': now}
                return []

            kg_list_path = DATA_DIR / 'kg_list.json'
            kgs = []
            if kg_list_path.exists():
                kgs = json.loads(kg_list_path.read_text())
            _kg_by_code_cache = {k.get('kg_code'): k for k in kgs if k.get('kg_code')}

            try:
                from app import _get_completed_kgs
                completed = _get_completed_kgs()
            except Exception:
                completed = set()
            failed = set()
            failed_path = DATA_DIR / 'failed_kgs.json'
            if failed_path.exists():
                try:
                    failed = set(json.loads(failed_path.read_text()))
                except Exception:
                    pass

            # --- Index-authoritative completion guard. ---------------------
            # ``_get_completed_kgs()`` only ever yields *block* codes for a
            # split KG (``22017-east``/``22017-west``) — never the parent
            # ``22017`` — AND it discards any code carrying a force-requeue
            # tombstone. So an already-processed split parent leaks back into
            # this whitelist, gets expanded into blocks below, and is
            # re-dispatched to a (second) cache-only peer that races the
            # block another peer just finished (observed: at62 + at94 both on
            # 22017-west). The retry-queue force-requeue fixes (0afd538,
            # b0fcf1d) only ever touched the frontier retry-queue path — not
            # this opportunistic cache-only backfill.
            #
            # The robust cross-check is the search index ``processed`` flag
            # (per AGENTS.md it matches the db_processed counter and covers
            # all peer-synced KGs, not just locally-uploaded ones). It is
            # split-agnostic: a parent flips to processed=1 once its products
            # exist, regardless of how ``maybe_split_kg`` happens to slice it
            # *now* (block sets drift as strike counts / tile thresholds
            # change, so a manifest-vs-current-blocks structural check misses
            # KGs that were split adaptively in the past — 62013/40016/40326
            # etc.). Deliberate re-processing still flows through the
            # frontier retry queue (which honours tombstones); cache-only
            # backfill must never re-touch a KG the index already calls done.
            #
            # BUT (2026-11-07): ``processed=1`` is set as soon as ONE block
            # JSON exists, so a half-done split parent (67204: 2/6 blocks)
            # is "processed" with a real hole — 74 such parents / 138
            # fully-cached blocks were invisible to 40 idle cache-only
            # peers and crawled through the frontiers at 3.6/h. The guard
            # now asks the geometric coverage oracle
            # (``_parent_geometrically_done``): processed parents whose
            # current-layout blocks are all done stay excluded cheaply;
            # the rest are excluded only when the oracle says their
            # products (any historical layout) cover the bbox. Half-done
            # parents pass through; the block-level ``_get_completed_kgs``
            # exclusion below keeps their finished siblings out.
            processed_codes: set = set()
            try:
                import search_index as _si
                _conn = _si.get_index()._conn()
                for _row in _conn.execute(
                        'SELECT kg_code FROM kg WHERE processed=1'):
                    processed_codes.add(_row[0])
            except Exception as _e:
                log.debug('Cache-ready processed-flag guard skipped: %s', _e)
            if processed_codes:
                _mf_ent = _manifest_entries_ro()
                _half_done: list[str] = []
                if _mf_ent is not None:
                    try:
                        from kg_splitter import maybe_split_kg as _msk
                    except Exception:
                        _msk = None
                    for _pc in sorted(processed_codes - completed):
                        _kg = _kg_by_code_cache.get(_pc)
                        try:
                            _blocks = _msk(_kg) if (_msk and _kg) else []
                        except Exception:
                            _blocks = []
                        if len(_blocks) > 1 and all(
                                b.get('kg_code') in completed for b in _blocks):
                            continue  # every current-layout block done
                        if _parent_geometrically_done(_pc, _mf_ent) is False:
                            _half_done.append(_pc)
                    if _half_done:
                        processed_codes = processed_codes - set(_half_done)
                        log.info('Cache-ready scan: %d processed=1 parent(s) have a '
                                 'coverage hole — kept on the whitelist: %s',
                                 len(_half_done), ' '.join(_half_done[:12]))
                completed = completed | processed_codes

            try:
                import tile_index as ti
                year = ti.dataset_to_year(ti.DEFAULT_DATASET)
            except Exception:
                year = 2024

            def _within_cells(s: float, n: float,
                              w: float, e: float) -> bool:
                # Fast path: bbox inside one cached cell. Otherwise a
                # straddler — accept when every intersected planning
                # cell is cached (is_kg_fully_cached() below is still
                # the per-tile authority).
                for cs, cn, cw, ce in cached_cells:
                    if (s >= cs - 1e-9 and n <= cn + 1e-9
                            and w >= cw - 1e-9 and e <= ce + 1e-9):
                        return True
                return self._bbox_covered_by_cells(s, n, w, e, cached_cells)

            cache_miss_excluded = self._cache_miss_excluded()
            if cache_miss_excluded:
                log.info('Cache-ready scan: excluding %d KGs with unresolved cache misses',
                         len(cache_miss_excluded))
            scanned = 0
            prefiltered = 0
            for kg in kgs:
                code = kg.get('kg_code')
                if not code or code in completed or code in failed:
                    continue
                if code in cache_miss_excluded:
                    continue
                bb = kg.get('bbox') or {}
                w, s = bb.get('min_lon'), bb.get('min_lat')
                e, n = bb.get('max_lon'), bb.get('max_lat')
                if None in (w, s, e, n):
                    continue
                if not _within_cells(s, n, w, e):
                    continue
                prefiltered += 1
                bbox = {'west': w, 'south': s, 'east': e, 'north': n}
                scanned += 1
                try:
                    if is_kg_fully_cached(bbox, year=year,
                                          cop_cache=cop_cache,
                                          hansen_cache=hansen_cache,
                                          local_ok=False):
                        codes.append(code)
                        if len(codes) >= max_kgs:
                            break
                except Exception:
                    continue
            log.info('Cache-ready scan: %d/%d KGs in covered cells (%d), '
                     '%d fully cached (max %d)',
                     prefiltered, len(kgs), len(cached_cells),
                     len(codes), max_kgs)
        except Exception as e:
            log.warning('Cache-ready scan failed: %s', e)

        with self._lock:
            self.state['_cache_ready_cache'] = {'codes': codes, 'at': now}
        return codes

    def _orchestrate_parallel_frontiers(self):
        """Start additional frontier peers when credential capacity permits.

        Each new frontier gets:
          * a disjoint slice of valid Copernicus credentials
          * a disjoint slice of fully-cached lat strips
        Both come from the same cache_manifest used by cache-only peers.

        Skipped silently when:
          * primary active_peer is not running yet
          * fewer than 2*min_creds_per_frontier valid creds (can't fit a
            second peer)
          * peer lacks the ``cred_subset_env`` capability (graceful upgrade)
        """
        with self._lock:
            mode = read_director_mode(self.state.get('mode', 'auto'))
            self.state['mode'] = mode
            cfg = self.cfg.copy()
            state_copy = self.state.copy()
        if mode == 'paused':
            return
        _bp_pf = state_copy.get('bev_pause') or {}
        if _bp_pf.get('active') and (_bp_pf.get('scope') or 'fleet') == 'fleet':
            # Don't spin up new parallel frontiers during a fleet-wide
            # BEV outage pause. The active frontier may still be mid-
            # finalization (zenodo upload) and we let it finish; new
            # BEV-heavy work waits for the cooldown. Pool-scoped
            # pauses are enforced via per-peer not_before.
            return

        active_id = state_copy.get('active_peer')
        if not active_id:
            return


        # Cap on total concurrent frontiers (incl. active). With 8 valid
        # creds and per=2 this is min(3, 7) = 3 — we always reserve `per`
        # creds as a fleet-wide spare so a 402 can rotate without
        # colliding with another peer's slice. With adaptive mode and a
        # healthy Copernicus sub-factor we drop to per=1 — 8 frontier
        # slots, one cred per slot.

        max_par = self._max_parallel_frontiers(cfg)
        # Server-friendliness throttle: scale down when BEV / Zenodo /
        # Copernicus are getting hammered. Round, but never below 1 if
        # max_par was already ≥ 1 — we always allow the active frontier.
        _factor = float(state_copy.get('capacity_factor',
                                        self._capacity_ema))
        # End-game exemption (2026-11-08): when max_par is already bound
        # by the pending item count (the last cell's straggler blocks,
        # ≤ FRONTIER_ENDGAME_PER_CELL per cell), the Copernicus-warning
        # driven factor would park the last 3 of 8 blocks for hours while
        # ~45 peers idle — that load is a fraction of the ≥20-frontier
        # regime the factor was tuned for, so launch all of them.
        if max_par > 0 and getattr(self, '_frontier_work_bound', False):
            if _factor < 1.0:
                log.info('capacity factor %.2f ignored — frontier count %d '
                         'is work-bound (end-game stragglers)',
                         _factor, max_par)
        elif max_par > 0:
            max_par_throttled = max(1, int(round(max_par * _factor)))
            if max_par_throttled < max_par:
                log.info(
                    'capacity factor %.2f → max_parallel_frontiers %d → %d',
                    _factor, max_par, max_par_throttled,
                )
            max_par = max_par_throttled

        # Frontier peers each pin to a disjoint Austria-wide cell
        # (1° lat × 2° lon by default) so they never collide on a
        # Zenodo cache ZIP write — even when opening regions that aren't
        # yet cached. Cached vs uncached cells are equivalent here:
        # tile uploads are scoped to one cell per ZIP.
        #
        # Use ``_frontier_cells()`` (not ``_austria_cells()``): cells
        # whose every KG/block is already processed must be excluded
        # from the distribution, else the peer holding one starts its
        # processor, finds 0 pending items, exits in <1 s, and the
        # director re-issues the same dead plan every tick — a restart
        # loop that also pins a Copernicus credential (Aug 2026:
        # at106 on 46-47/10-12 + 46-47/16-18).
        strips = self._frontier_cells()
        if not strips:
            # Empty cells is transient (config glitch / startup race).
            # Do NOT wipe parallel_frontiers_active here — the
            # single-active guard in ``_check_and_switch`` consults
            # this set to decide whether to hard-stop running
            # frontiers. Wiping it caused the 2026-05-06 cascade
            # where every authorised parallel frontier was killed on
            # the next tick. Just bail out and try again next tick.
            log.debug('parallel orch: no Austria cells available, skipping')
            return

        min_reserve = int(cfg.get('min_reserve_peers', MIN_RESERVE_PEERS))
        peers = list(cfg.get('peers', []))
        total_enabled = sum(1 for p in peers
                            if p.get('enabled', True)
                            and not _peer_is_scheduled(p))

        # Already running parallel frontiers (excluding active_peer).
        # ``prev_par`` is the set of peers we authorised as parallel
        # frontiers on the previous tick. When a peer in this set
        # shows up as ``unreachable`` for a single tick (common during
        # a director takeover, a peer's gunicorn worker swap, or a
        # heavy GPKG-build that briefly starves the status endpoint)
        # we keep it in ``running`` for up to
        # ``UNREACHABLE_FAILOVER_THRESHOLD`` consecutive misses — same
        # grace the active-peer path already uses (line ~2599). Without
        # this, a single transient blip drops the peer from
        # ``running`` → it's omitted from the new
        # ``parallel_frontiers_active`` set → the next tick's
        # single-active guard hard-stops it as a non-authorised
        # frontier. That was the slow drain observed during the
        # 2026-05-06 at40-as-interim-director window: ~3 peers lost
        # over ~30 min, never auto-recovered until primary took back
        # over.
        prev_par = set(state_copy.get('parallel_frontiers_active') or [])
        retained_unreachable: list[str] = []
        running: list[str] = []
        candidates: list[dict] = []
        # {pid: (cred_indices, cell_filter)} as reported by the peer's
        # own progress.json — the env its processor subprocess ACTUALLY
        # holds. Used to self-heal the persisted frontier plans when a
        # running peer's entry was pruned on a previous tick (the
        # "have creds=[] strips=[]" empty-held restart loop, Jul 2026).
        reported_env: dict[str, tuple] = {}
        for p in peers:
            pid = p['id']
            if pid == active_id:
                continue
            if not p.get('enabled', True):
                continue
            if _peer_is_scheduled(p):
                continue
            role = self._peer_role(p)
            if role in ('idle', 'cache_only'):
                continue
            if _peer_in_warmup(p):
                continue  # fresh peer — not yet eligible for frontier work
            # Reservation holders are eligible as parallel frontiers —
            # they just process their reserved KG first (priority queue
            # ensures it is at the head, _reserved_kgs excludes other
            # peers from racing them on it). Without this, a reservation
            # holder whose pre-emption is pending (active peer mid‑KG)
            # would sit idle indefinitely instead of running parallel
            # frontier work in the meantime.
            bw = state_copy.get('peer_bandwidth', {}).get(pid, {})
            if _peer_bw_depleted(p, bw, cfg):
                continue
            ps = get_peer_status(p.get('url'))
            if ps.get('state') == 'unreachable':
                if pid in prev_par:
                    # Tolerate transient unreachable on an authorised
                    # parallel frontier. Bump per-peer miss counter;
                    # only drop after the same threshold the active
                    # peer uses.
                    with self._lock:
                        misses = self.state.setdefault(
                            'parallel_unreachable_count', {})
                        misses[pid] = int(misses.get(pid, 0)) + 1
                        n = int(misses[pid])
                    if n < UNREACHABLE_FAILOVER_THRESHOLD:
                        log.info('Parallel frontier %s unreachable '
                                 '(%d/%d) — preserving slot',
                                 pid, n, UNREACHABLE_FAILOVER_THRESHOLD)
                        retained_unreachable.append(pid)
                        continue
                    log.warning('Parallel frontier %s unreachable %d '
                                'times — releasing slot', pid, n)
                    with self._lock:
                        self.state.get(
                            'parallel_unreachable_count', {}).pop(pid, None)
                continue
            # Reachable — clear miss counter if any.
            if pid in (self.state.get('parallel_unreachable_count') or {}):
                with self._lock:
                    self.state.get(
                        'parallel_unreachable_count', {}).pop(pid, None)
            # Refresh caps opportunistically; all current peers expose
            # cred_subset_env so we no longer gate on it.
            self._refresh_peer_caps(p)
            is_running = ps.get('state') in ('running', 'processing')
            is_cache_only_run = bool(ps.get('cache_only'))
            if is_running and not is_cache_only_run:
                running.append(pid)
                # Real progress cancels the no-work restart-loop
                # suspicion: a peer that has been on a KG for >5 min
                # is doing actual work, not exiting instantly.
                try:
                    _ck = (ps.get('current_kg') or {})
                    _st = _ck.get('started_at')
                    if _st:
                        from datetime import datetime as _dtp
                        _t = _dtp.fromisoformat(str(_st))
                        if _t.tzinfo is None:
                            _t = _t.replace(tzinfo=timezone.utc)
                        _age = (datetime.now(timezone.utc)
                                - _t).total_seconds()
                        if _age > 300:
                            self._clear_frontier_start_churn(pid)
                except Exception:
                    pass
                _rc = ps.get('cred_indices')
                _rs = ps.get('cell_filter')
                if _rc or _rs:
                    reported_env[pid] = (_rc, _rs)
            else:
                candidates.append({
                    'peer': p,
                    'state': ps.get('state', 'unknown'),
                    'needs_stop_cache_only': is_running and is_cache_only_run,
                })

        # Cache-mix reservation (2026-08-22): when cache-ready KGs have
        # accumulated, keep a slice of the fleet free for cache-only
        # work instead of promoting every last peer to frontier. This
        # only binds when total_enabled (not cred capacity) is the
        # frontier ceiling — the small-fleet regime. Never trims below
        # what's already running (active + running frontiers), so it
        # drains via natural KG-boundary churn, not hard stops.
        try:
            _cache_ready_n = len(self._compute_cache_ready_kgs() or [])
        except Exception:
            _cache_ready_n = 0
        if _cache_ready_n >= CACHE_MIX_MIN_READY:
            _reserve = min(CACHE_MIX_MAX_RESERVE,
                           max(1, int(round(total_enabled * CACHE_MIX_FRAC))))
            _running_total = ((1 if active_id else 0)
                              + len(set(running) | set(retained_unreachable)))
            _target_cap = max(1, total_enabled - min_reserve - _reserve)
            _cap_frontier = max(_running_total, _target_cap)
            if max_par > _cap_frontier:
                log.info('cache-mix: %d cache-ready KGs — reserving %d '
                         'peer(s) for cache-only; max_parallel_frontiers '
                         '%d → %d', _cache_ready_n, _reserve,
                         max_par, _cap_frontier)
                max_par = _cap_frontier
            # Gentle drain: frontier processors run indefinitely, so an
            # over-cap fleet never converges on its own. Gracefully stop
            # at most ONE excess parallel frontier per tick (noisiest
            # first, never the active, never a retained-unreachable) —
            # it finishes its current KG, stops, and the cache-only
            # orchestrator picks it up on a later tick. The floor above
            # keeps its cred/strip plan intact while it drains.
            _excess_fr = _running_total - _target_cap
            if _excess_fr > 0 and running:
                _drain_order = sorted(
                    running,
                    key=lambda pid: (-_peer_noise_score(pid, state_copy),
                                     pid))
                _now_ts = time.time()
                for _pid in _drain_order:
                    if _now_ts - _LAST_GRACEFUL_STOP_TS.get(_pid, 0) < 600:
                        continue
                    _pr = get_peer_by_id(cfg, _pid)
                    if not _pr:
                        continue
                    try:
                        stop_peer_processor(_pr.get('url'), graceful=True)
                        _LAST_GRACEFUL_STOP_TS[_pid] = _now_ts
                        log.info('cache-mix: gracefully draining frontier '
                                 '%s (excess %d over target %d) — will '
                                 'flip to cache-only after current KG',
                                 _pid, _excess_fr, _target_cap)
                    except Exception as _e:
                        log.warning('cache-mix drain of %s failed: %s',
                                    _pid, _e)
                    break  # one per tick

        # Slot budget = max_par - 1 (subtract active). Also keep reserve.
        slack = max(0, total_enabled - (1 if active_id else 0)
                    - len(running) - min_reserve)
        max_add = min(slack, max_par - 1 - len(running))
        # Ramp limiter (see RAMP_*_STARTS_PER_TICK). Scale gentleness
        # to capacity factor: never burst more than a few new frontier
        # peers per tick.
        _ramp_span = RAMP_MAX_STARTS_PER_TICK - RAMP_MIN_STARTS_PER_TICK
        _ramp_cap = RAMP_MIN_STARTS_PER_TICK + int(round(_ramp_span * _factor))
        _ramp_cap = max(RAMP_MIN_STARTS_PER_TICK, _ramp_cap)
        max_add = min(max_add, _ramp_cap)

        # Stable assignment: active + sorted(running+candidates) -> ids in
        # order define which cred slice and lat strip they get.
        # Prefer idle/stopped candidates over cache-only-running ones so
        # we don't kill in-progress cache-only work when there's a free
        # peer available.
        # Order: don't preempt running cache-only peers; among the rest,
        # quiet peers first (so fresh peers absorb new frontier slots).
        # Steal-aware ordering: a fresh frontier slot is far more
        # valuable on a low-steal peer (cred efficiency). Add
        # (1/capacity - 1) so a 50%% steal peer ranks +1.0 worse than
        # a 0%% peer, ahead of the typical noise spread (<0.5).
        def _frontier_sort_key(c):
            _pid = c['peer']['id']
            _s = _peer_cpu_steal_ema(_pid, state_copy)
            if _s is None:
                _s = _peer_cpu_steal(_pid) or 0.0
            _cap = max(0.10, 1.0 - float(_s) / 100.0)
            _tier = 1 if float(_s) >= FRONTIER_HIGH_STEAL_BIAS_PCT else 0
            _pool_tier, _pool_pen = _peer_pool_bev_score(
                _pid, self.cfg, state_copy)
            return (
                1 if c.get('needs_stop_cache_only') else 0,
                _tier + _pool_tier,
                _peer_noise_score(_pid, state_copy)
                + ((1.0 / _cap) - 1.0) + _pool_pen,
                _pid,
            )
        candidates.sort(key=_frontier_sort_key)
        # Include retained_unreachable peers up front so their cred /
        # strip plan is reissued (preserving their prior slice) instead
        # of being treated as a fresh candidate and triggering a
        # plan-drift restart when they reappear.
        ordered = [active_id] + sorted(set(running) | set(retained_unreachable))
        if max_add > 0:
            ordered += sorted(c['peer']['id'] for c in candidates[:max_add])
        # De-dup while preserving order (active_id may also be in running).
        _seen: set[str] = set()
        ordered = [pid for pid in ordered
                   if pid and not (pid in _seen or _seen.add(pid))]
        ordered = ordered[:max_par]  # cap to credential capacity
        # Preserve existing peer→creds mapping where valid; only newly
        # promoted peers get fresh slices. Without this, a peer entering
        # or leaving the running set would shuffle every other peer's
        # cred slice, triggering plan-drift restart loops.
        prior_cred_plan = dict(state_copy.get('frontier_cred_plan') or {})
        prior_strip_plan_seed = dict(
            state_copy.get('frontier_strip_plan') or {})
        # Self-heal: a RUNNING frontier whose entry vanished from the
        # persisted plans (pruned by the ordered-trim on a previous
        # tick, or a restart-early-return that saved a pruned plan)
        # gets it reconstructed from the env its processor actually
        # reports. Without this, the empty held plan trips the
        # hard-conflict test below and the peer is pointlessly
        # hard-restarted every few minutes (Jul 2026 loop).
        _running_now = {active_id} | set(running)
        # Pushed status payloads are built from progress.json and do
        # NOT carry cred_indices / cell_filter (only the
        # /processing/status route synthesises them by scanning the
        # processor subprocess's /proc/<pid>/environ). So for any
        # running frontier whose held plan is empty, do a DIRECT poll
        # of the peer's status endpoint — bypassing the push cache —
        # to learn the env its processor actually holds.
        def _poll_reported_env(_pid: str) -> tuple | None:
            _pr = get_peer_by_id(cfg, _pid)
            if not _pr or not _pr.get('url'):
                return None
            try:
                r = _peer_request(
                    'GET',
                    _pr['url'].rstrip('/') + '/api/v1/processing/status',
                    peer_id=_pid, timeout=12)
                if r is None or r.status_code != 200:
                    return None
                d = r.json()
            except Exception:
                return None
            return (d.get('cred_indices'),
                    d.get('cell_filter') or d.get('lat_strip_filter'))
        for _pid in sorted(_running_now):
            if not _pid or _pid in reported_env:
                continue
            if prior_cred_plan.get(_pid) and prior_strip_plan_seed.get(_pid):
                continue
            _env = _poll_reported_env(_pid)
            if _env is not None:
                reported_env[_pid] = _env
        for _pid, (_rc, _rs) in reported_env.items():
            if _rc and not prior_cred_plan.get(_pid):
                try:
                    prior_cred_plan[_pid] = [int(i) for i in _rc]
                    log.info('Frontier %s: restored cred plan %s from '
                             'peer-reported env (held plan was empty)',
                             _pid, prior_cred_plan[_pid])
                except Exception:
                    pass
            if _rs and not prior_strip_plan_seed.get(_pid):
                try:
                    prior_strip_plan_seed[_pid] = [list(s) for s in _rs]
                    log.info('Frontier %s: restored strip plan %s from '
                             'peer-reported env (held plan was empty)',
                             _pid, prior_strip_plan_seed[_pid])
                except Exception:
                    pass
        # Sticky: RUNNING peers keep their held creds verbatim (only
        # invalid creds are released) — see _assign_cred_indices.
        cred_plan = self._assign_cred_indices(
            ordered, cfg, prior=prior_cred_plan,
            sticky=_running_now & set(ordered))

        # Lat strips: distribute ALL Austria strips contiguously across
        # the frontier peers. With 7 strips and 3 frontiers each peer
        # gets ~2-3 contiguous strips. This matters for straddler KGs
        # (the ~6% whose bbox crosses a 0.5° strip boundary): the owning
        # frontier (by centroid) reads/writes neighbour-strip cache tiles
        # for the overlap. If the neighbour strip is owned by the same
        # peer (contiguous range), there's no contention. Cross-peer
        # contention only happens at the inter-peer boundary — at most
        # N-1 such boundaries instead of one boundary per strip.
        # When fewer strips than frontiers (very rare with 7 strips),
        # tail peers get nothing — we then trim ``ordered``.
        strip_plan: dict[str, list] = {}
        # Strip-hog release bookkeeping (see block below): {pid: trimmed
        # slice} for running frontiers whose planned slice was cut back
        # to fair share this tick. Consulted by the adopt-not-restart
        # block so it doesn't re-adopt the oversized held plan.
        _hog_trimmed: dict[str, list] = {}
        n_peers = len(ordered)
        n_strips = len(strips)
        prior_strip_plan = prior_strip_plan_seed
        if n_peers > 0 and n_strips > 0:
            # Preserve existing peer→strip assignments where the prior
            # slice still fits within the canonical strip set; only fill
            # gaps for new peers from whatever strips remain. Without
            # this, churn in `ordered` re-shuffles every peer's strip
            # range each tick, triggering plan-drift restarts.
            #
            # However: cap each preserved slice at the peer's fair
            # share for the current ``n_peers``. Otherwise an existing
            # peer that was activated alone (and given ALL strips) would
            # hog them forever, leaving newly-promoted parallel
            # frontiers with empty plans → trimmed → never started.
            # Fair share: base = n_strips // n_peers, plus 1 extra for
            # the first ``n_strips % n_peers`` peers in ``ordered``.
            base_fair = n_strips // n_peers
            extra_fair = n_strips % n_peers
            fair_share = {
                pid: base_fair + (1 if i < extra_fair else 0)
                for i, pid in enumerate(ordered)
            }
            strip_set = {tuple(s) for s in strips}
            used: set[tuple[float, float]] = set()
            for pid in ordered:
                old = prior_strip_plan.get(pid) or []
                old_t = [tuple(s) for s in old]
                if not old:
                    continue
                # Drop entries no longer in the canonical strip set or
                # already taken by another peer (preserves order).
                kept = [s for s in old_t
                        if s in strip_set and s not in used]
                # Trim to fair share so leftover strips remain for new
                # frontiers — but NOT for peers currently RUNNING a
                # frontier: re-slicing a running peer's strips is
                # exactly the cosmetic drift that fed the Jul 2026
                # restart loop. Running peers release strips only when
                # they stop or the strip leaves the canonical set.
                if pid not in _running_now:
                    kept = kept[:fair_share.get(pid, 0)]
                if kept:
                    strip_plan[pid] = [list(s) for s in kept]
                    used.update(kept)
            leftover = [s for s in strips if tuple(s) not in used]
            unassigned = [pid for pid in ordered if pid not in strip_plan]
            # --- strip-hog release (Jul 2026 follow-up) ------------
            # The _running_now exemption above lets a running frontier
            # keep MORE than its fair share indefinitely — e.g. a sole
            # frontier that was once granted ALL 13 Austria cells. When
            # credentialed candidates are then starving (leftover
            # empty), every promotion gets strips=[] → trimmed /
            # skipped by the start loop → the fleet is stuck at
            # parallel=1 until the hog stops on its own (never). Fix:
            # trim the hog's PLANNED slice to fair share so leftovers
            # exist for candidates, and send the hog ONE graceful
            # (KG-boundary) stop — max one hog per tick, cooldown
            # FRONTIER_HOG_RELEASE_COOLDOWN_S — so it eventually
            # re-launches with the trimmed cell_filter env. NEVER a
            # hard stop: the in-flight KG must complete + upload.
            # Until that boundary the hog's env still covers the
            # released cells (bounded transient overlap: it works one
            # KG in one cell at a time).
            _cred_unassigned = [p for p in unassigned if cred_plan.get(p)]
            if _cred_unassigned and not leftover:
                _hogs = sorted(
                    (p for p in ordered
                     if p in _running_now
                     and len(strip_plan.get(p) or [])
                     > max(1, fair_share.get(p, 0))),
                    key=lambda p: -len(strip_plan.get(p) or []))
                for hpid in _hogs:
                    _kept = strip_plan.get(hpid) or []
                    _fair = max(1, fair_share.get(hpid, 0))
                    _released = _kept[_fair:]
                    if not _released:
                        continue
                    _trimmed = [list(s) for s in _kept[:_fair]]
                    strip_plan[hpid] = _trimmed
                    _hog_trimmed[hpid] = [list(s) for s in _trimmed]
                    used.difference_update(tuple(s) for s in _released)
                    log.info(
                        'Strip-hog release: %s planned slice trimmed '
                        '%d → %d strips (candidates starving: %s)',
                        hpid, len(_kept), len(_trimmed),
                        ','.join(_cred_unassigned))
                if _hog_trimmed:
                    leftover = [s for s in strips if tuple(s) not in used]
                    _now_ts = time.time()
                    _target = None
                    with self._lock:
                        _cool = self.state.setdefault(
                            'frontier_hog_release', {})
                        for hpid in sorted(_hog_trimmed):
                            if (_now_ts - float(_cool.get(hpid) or 0)
                                    >= FRONTIER_HOG_RELEASE_COOLDOWN_S):
                                _target = hpid
                                _cool[hpid] = _now_ts
                                break
                    if _target:
                        _hp = get_peer_by_id(cfg, _target)
                        try:
                            stop_peer_processor(
                                (_hp or {}).get('url'), graceful=True)
                            _emit_director_event(
                                'strip-hog release: graceful stop (KG '
                                'boundary) — planned slice trimmed to '
                                'fair share (%d strips); freed strips '
                                'go to starving frontier candidates'
                                % len(_hog_trimmed[_target]),
                                peer=_target)
                        except Exception as e:
                            log.warning(
                                'Strip-hog graceful stop on %s failed: '
                                '%s', _target, e)
            if unassigned and not leftover and strips:
                # End-game cell sharing (see FRONTIER_ENDGAME_PER_CELL):
                # every cell is taken but credentialed candidates are
                # starving. Hand each of them a *copy* of the cell with
                # the most spare pending items, bounded per cell by
                # min(FRONTIER_ENDGAME_PER_CELL, pending items). Peers
                # de-duplicate blocks via the cross-peer claim check.
                _items = getattr(self, '_frontier_cell_items', None) or {}
                _holders: dict[tuple, int] = {}
                for _pid, _sl in strip_plan.items():
                    for _s in _sl or []:
                        _holders[tuple(_s)] = _holders.get(tuple(_s), 0) + 1
                _shared: list[str] = []
                for pid in list(unassigned):
                    if not cred_plan.get(pid):
                        continue
                    best, best_spare = None, 0
                    for s in strips:
                        st = tuple(s)
                        cap = min(max(1, FRONTIER_ENDGAME_PER_CELL),
                                  max(1, int(_items.get(st, 1))))
                        spare = cap - _holders.get(st, 0)
                        if spare > best_spare:
                            best, best_spare = st, spare
                    if best is None:
                        break
                    strip_plan[pid] = [list(best)]
                    _holders[best] = _holders.get(best, 0) + 1
                    _shared.append('%s→%g-%g/%g-%g' % ((pid,) + best))
                if _shared:
                    unassigned = [p for p in unassigned if p not in strip_plan]
                    log.info('End-game cell sharing: %s', ', '.join(_shared))
            if unassigned and leftover:
                base = len(leftover) // len(unassigned)
                extra = len(leftover) % len(unassigned)
                # Tail-extra: existing peers may already hold extra
                # strips, so give the bonus to new peers in id order.
                offset = 0
                for i, pid in enumerate(unassigned):
                    size = base + (1 if i < extra else 0)
                    if size <= 0:
                        strip_plan[pid] = []
                        continue
                    slice_ = leftover[offset:offset + size]
                    strip_plan[pid] = [list(c) for c in slice_]
                    offset += size
            for pid in ordered:
                strip_plan.setdefault(pid, [])
        else:
            for pid in ordered:
                strip_plan[pid] = []
        # Trim peers that ended up without a strip (no cap collision).
        # RUNNING peers are exempt from the prune: dropping a running
        # frontier's entry from the persisted plans makes the next
        # tick see "have creds=[] strips=[]" and hard-restart it
        # (Jul 2026 loop). Their entries stay until they stop.
        ordered = [pid for pid in ordered
                   if strip_plan.get(pid) or pid in _running_now]
        _keep = set(ordered) | _running_now
        cred_plan = {pid: v for pid, v in cred_plan.items() if pid in _keep}
        strip_plan = {pid: v for pid, v in strip_plan.items() if pid in _keep}

        # Detect peers running with a stale plan (creds OR strips don't
        # match). Hard-stop them so the next tick re-issues with the
        # correct env. This is what catches the case where the active
        # peer was started without a lat_strips env (e.g. via the
        # legacy choose_active_peer path).
        # Use the self-healed prior plans (seeded from peer-reported
        # env above) so a running peer whose persisted entry was
        # pruned isn't judged against an empty held plan.
        old_cred_plan = prior_cred_plan
        old_strip_plan = prior_strip_plan
        peers_to_restart: list[str] = []
        # Adopt-not-restart (Jul 2026): a running frontier is only
        # hard-restarted on a *hard* conflict — all its held creds are
        # invalid, a held cred is also wanted by ANOTHER running peer
        # this tick, or its strips vanished from the canonical set.
        # Cosmetic diffs (per_eff flapping 1↔2 trims a spare cred,
        # fair-share strip re-slicing) instead ADOPT the running plan.
        # Restarting mid-KG for a cosmetic slice change caused the
        # Jul 2026 loop where every frontier was killed every few
        # minutes and no KG ever completed (progress flat for 2 days,
        # sawtooth EMA, zero copernicus/segment log lines).
        _valid_all = set(self._valid_credentials())
        _strip_set_all = {tuple(s) for s in strips}
        _running_set = {active_id} | set(running)
        _adopted_creds: set[int] = set()
        _adopted_pids: set[str] = set()
        for pid in [active_id] + list(running):
            want_creds = sorted(cred_plan.get(pid) or [])
            want_strips = sorted(
                tuple(s) for s in (strip_plan.get(pid) or [])
            )
            have_creds = sorted(old_cred_plan.get(pid) or [])
            have_strips = sorted(
                tuple(s) for s in (old_strip_plan.get(pid) or [])
            )
            if not (want_creds and want_strips):
                continue
            if want_creds == have_creds and want_strips == have_strips:
                continue
            # Empty held plan on a RUNNING peer: after self-heal this
            # means neither the persisted plans nor the peer's own
            # reported env carry creds/strips. If the peer at least
            # reports creds in its processor env, adopt the wanted
            # plan for BOOKKEEPING only (no restart) — the running
            # subprocess already has working creds; restarting it
            # gains nothing and feeds the loop. Only a peer truly
            # running without creds warrants a restart.
            if not have_creds and not have_strips:
                _renv = reported_env.get(pid)
                if _renv and _renv[0]:
                    log.info(
                        'Frontier %s: held plan empty but peer reports '
                        'cred env %s — adopting reported plan, no '
                        'restart', pid, _renv[0])
                    try:
                        cred_plan[pid] = sorted(int(i) for i in _renv[0])
                    except Exception:
                        cred_plan[pid] = want_creds
                    if _renv[1]:
                        try:
                            strip_plan[pid] = [list(s) for s in _renv[1]]
                        except Exception:
                            pass
                    _adopted_creds.update(cred_plan[pid])
                    _adopted_pids.add(pid)
                    continue
                if _renv is None:
                    # Poll failed — no EVIDENCE the peer is credless.
                    # Adopt the wanted plan for bookkeeping only and
                    # leave the peer running; a restart on missing
                    # evidence is what fed the Jul 2026 loop. If it
                    # truly has no creds, a later successful poll
                    # (empty env) will trigger the restart below.
                    log.info(
                        'Frontier %s: held plan empty, env poll '
                        'failed — adopting wanted plan creds=%s '
                        'strips=%s for bookkeeping, no restart',
                        pid, want_creds, want_strips)
                    _adopted_creds.update(want_creds)
                    _adopted_pids.add(pid)
                    continue
                # Poll succeeded and the processor env has NO creds —
                # the only empty-held case where a restart is truly
                # needed (peer started without a cred env).
            # --- hard-conflict tests on the HELD plan -------------
            held_valid = [c for c in have_creds if c in _valid_all]
            held_strips_ok = [s for s in have_strips
                              if s in _strip_set_all]
            other_running_want = set()
            for opid in _running_set:
                if opid != pid:
                    other_running_want.update(cred_plan.get(opid) or [])
            conflict = (
                not held_valid
                or not held_strips_ok
                or bool(set(held_valid) & other_running_want)
            )
            if conflict:
                log.warning(
                    'Frontier %s plan drift (hard conflict): have '
                    'creds=%s strips=%s, want creds=%s strips=%s — '
                    'restarting',
                    pid, have_creds, have_strips, want_creds,
                    want_strips,
                )
                peers_to_restart.append(pid)
                continue
            # --- adopt: keep the peer running on what it holds ----
            cred_plan[pid] = held_valid
            if pid in _hog_trimmed:
                # Strip-hog release in flight: do NOT re-adopt the
                # oversized held slice — that would re-mark the freed
                # strips as adopted and strip them back off the
                # starving candidates below, undoing the release.
                # Keep the trimmed plan; the peer's env still covers
                # the extra cells until its graceful KG-boundary stop.
                strip_plan[pid] = [list(s) for s in _hog_trimmed[pid]]
            else:
                strip_plan[pid] = [list(s) for s in held_strips_ok]
            _adopted_creds.update(held_valid)
            _adopted_pids.add(pid)
            log.info(
                'Frontier %s plan drift (cosmetic): adopting held '
                'creds=%s strips=%s (planner wanted creds=%s)',
                pid, held_valid, held_strips_ok, want_creds)
        # Creds adopted by running peers must not be double-issued to
        # not-yet-started candidates this tick. Strip them; a candidate
        # left credless is simply skipped by the start loop below.
        if _adopted_creds:
            for opid in list(cred_plan.keys()):
                if opid in _running_set or opid in _adopted_pids:
                    continue
                _new = [c for c in (cred_plan.get(opid) or [])
                        if c not in _adopted_creds]
                cred_plan[opid] = _new
        if _adopted_pids:
            _adopted_strips = set()
            for apid in _adopted_pids:
                _adopted_strips.update(
                    tuple(s) for s in (strip_plan.get(apid) or []))
            for opid in list(strip_plan.keys()):
                if opid in _running_set or opid in _adopted_pids:
                    continue
                strip_plan[opid] = [
                    s for s in (strip_plan.get(opid) or [])
                    if tuple(s) not in _adopted_strips]

        for pid in peers_to_restart:
            peer = get_peer_by_id(cfg, pid)
            if not peer:
                continue
            try:
                stop_peer_processor(peer.get('url'), graceful=False)
            except Exception as e:
                log.warning('Restart-stop %s failed: %s', pid, e)
        # If we hard-stopped any peers, return early; the next tick will
        # see them as candidates with the correct plan.
        if peers_to_restart:
            with self._lock:
                self.state['frontier_cred_plan'] = cred_plan
                self.state['frontier_strip_plan'] = strip_plan
                self.state['parallel_frontiers_active'] = [
                    pid for pid in running if pid not in peers_to_restart
                ]
                save_director_state(self.state)
            return

        # Start new candidates with their plan.
        to_start_ids = [pid for pid in ordered
                        if pid != active_id and pid not in running]
        cand_by_id = {c['peer']['id']: c for c in candidates}
        started = []
        for pid in to_start_ids:
            peer = get_peer_by_id(cfg, pid)
            if not peer:
                continue
            creds = cred_plan.get(pid)
            strips_for = strip_plan.get(pid)
            if not creds or not strips_for:
                continue
            cand = cand_by_id.get(pid) or {}
            # If the peer is currently running cache-only, stop it first so
            # we can re-start it as a frontier with the assigned creds /
            # lat-strip env. Otherwise the start API returns 409 and the
            # peer keeps doing cache-only work, leaving creds idle.
            #
            # IMPORTANT: must be a HARD stop (graceful=False). A graceful
            # stop only flips the after-KG shutdown flag (~hours away).
            # The subsequent start API would 409 and we'd ping-pong every
            # tick, with the peer never actually entering frontier mode.
            # Tile checkpoints survive the kill so the partial KG resumes
            # cheaply on the next run.
            if cand.get('needs_stop_cache_only'):
                log.info('Parallel frontier: hard-stopping cache-only run on '
                         '%s before promoting to frontier', pid)
                try:
                    stop_peer_processor(peer.get('url'), graceful=False)
                except Exception as e:
                    log.warning('Stop cache-only on %s failed: %s', pid, e)
                    continue
                # Poll until processor_state reflects stopped — the
                # subprocess can take several seconds to exit (and the
                # `Processor already running (external)` 409 will fire
                # otherwise, blocking the promotion for many ticks).
                import time as _t
                settled = False
                for _ in range(15):
                    _t.sleep(1.0)
                    ps2 = get_peer_status(peer.get('url'))
                    st = ps2.get('state', '')
                    if st in ('idle', 'stopped', 'complete', 'unreachable'):
                        settled = True
                        break
                if not settled:
                    log.warning('Parallel frontier: %s did not settle after '
                                'cache-only stop; deferring', pid)
                    continue
            # Exclude KGs reserved for OTHER peers from this peer's
            # priority queue, so we don't double-process a reservation
            # holder's KG on a parallel frontier that happens to share
            # the same lat strip.
            par_excl = _excluded_kgs(cfg, exclude_peer_id=pid)
            log.info('Parallel frontier: starting %s with creds=%s strips=%s%s',
                     pid, creds, strips_for,
                     (' (excluding ' + ','.join(sorted(par_excl)) + ')')
                     if par_excl else '')
            try:
                res = start_peer_processor(peer.get('url'),
                                            exclude_kgs=par_excl,
                                            cred_indices=creds,
                                            lat_strips=strips_for)
            except Exception as e:
                log.warning('Start parallel frontier on %s failed: %s', pid, e)
                continue
            if not isinstance(res, dict) or res.get('error'):
                # API start failed (e.g. 500 on the peer, no systemd
                # fallback for constrained starts). Don't mark this
                # peer as an active parallel frontier — otherwise its
                # cred slice is held hostage forever and the cache-only
                # orchestrator also leaves it alone.
                log.warning(
                    'Parallel frontier start on %s did not succeed: %s',
                    pid, (res or {}).get('error') or res)
                continue
            started.append(pid)
            self._note_frontier_start_churn(pid, creds, strips_for)
            # Surface the start — with its cred/strip assignment — in the
            # 24h merged log so operators see when a peer was promoted
            # and what slice it owns. (log.info above is logs/srv only.)
            try:
                _strips_txt = ','.join(
                    '%.1f-%.1f' % (s[0], s[1]) for s in strips_for) \
                    if strips_for else '-'
            except Exception:
                _strips_txt = '-'
            _emit_director_event(
                'frontier start → creds=' + ','.join(str(c) for c in creds)
                + ' strips=' + _strips_txt,
                peer=pid)

        # Diff against prior plans — emit per-peer events for any
        # cred / strip assignment that actually changed. Quiet when the
        # plan is a no-op tick (the common case).
        try:
            _all = sorted(set(cred_plan) | set(old_cred_plan)
                          | set(strip_plan) | set(old_strip_plan))
            for _pid in _all:
                _bc = sorted(old_cred_plan.get(_pid) or [])
                _ac = sorted(cred_plan.get(_pid) or [])
                _bs = [tuple(s) for s in (old_strip_plan.get(_pid) or [])]
                _as_ = [tuple(s) for s in (strip_plan.get(_pid) or [])]
                _cred_changed = _bc != _ac
                _strip_changed = sorted(_bs) != sorted(_as_)
                if not (_cred_changed or _strip_changed):
                    continue
                _parts = []
                if _cred_changed:
                    _parts.append('creds ' + (','.join(str(c) for c in _bc) or '-')
                                  + ' → ' + (','.join(str(c) for c in _ac) or '-'))
                if _strip_changed:
                    def _stxt(ss):
                        return ','.join('%.1f-%.1f' % (a, b) for a, b in ss) or '-'
                    _parts.append('strips ' + _stxt(_bs) + ' → ' + _stxt(_as_))
                _emit_director_event('plan: ' + '; '.join(_parts), peer=_pid)
        except Exception:
            pass

        with self._lock:
            # Union running + started + retained_unreachable. The
            # last term keeps temporarily-unreachable peers in the
            # authorised set so the single-active guard does not
            # hard-stop them when they reappear next tick.
            self.state['parallel_frontiers_active'] = list(
                set(running) | set(started) | set(retained_unreachable))
            self.state['frontier_cred_plan'] = cred_plan
            self.state['frontier_strip_plan'] = strip_plan
            save_director_state(self.state)

    def _orchestrate_cache_only(self):
        """Start/stop cache-only peers around the frontier peer.

        Rules:
          * keep at least ``min_reserve`` enabled peers idle (never started).
          * only enabled, non-scheduled peers with role=='cache_only' are
            considered.  If no peers are explicitly tagged cache_only,
            pick from idle frontier peers (their primary role is still
            frontier; we just borrow them for cache-only work).
          * never touch the active frontier peer.
          * cap concurrent cache-only peers at MAX_CACHE_ONLY_PEERS.
          * each cache-only peer gets a whitelist of fully-cached KGs.
          * cache-only peers without enough whitelist work are stopped
            so they free their slot.
        """
        with self._lock:
            mode = read_director_mode(self.state.get('mode', 'auto'))
            self.state['mode'] = mode
            cfg = self.cfg.copy()
            state_copy = self.state.copy()
        if mode == 'paused':
            return
        # BEV-outage pause: don't start new cache-only peers while the
        # fleet is in the cooldown window. Pool-scoped pauses are
        # enforced via per-peer not_before, so this short-circuit
        # only fires for fleet-wide pauses.
        _bp = state_copy.get('bev_pause') or {}
        if _bp.get('active') and (_bp.get('scope') or 'fleet') == 'fleet':
            return

        min_reserve = int(cfg.get('min_reserve_peers', MIN_RESERVE_PEERS))
        max_cache_only = int(cfg.get('max_cache_only_peers', MAX_CACHE_ONLY_PEERS))
        # Server-friendliness throttle (see _capacity_factor docstring).
        # Cache-only peers hit BEV + Zenodo, so this is exactly the knob
        # to turn down when those servers complain. We *do* allow zero
        # here — when servers are really angry, idling cache-only peers
        # is the right move (the active frontier still makes progress).
        _factor = float(state_copy.get('capacity_factor',
                                        self._capacity_ema))
        _max_cache_only_full = max_cache_only
        max_cache_only = max(0, int(round(max_cache_only * _factor)))
        if max_cache_only < _max_cache_only_full:
            log.info(
                'capacity factor %.2f → max_cache_only_peers %d → %d',
                _factor, _max_cache_only_full, max_cache_only,
            )


        active_frontier = state_copy.get('active_peer')
        # Peers the parallel-frontier orchestrator has authorised /
        # plans to run as additional frontiers. Cache-only must NOT
        # touch them, otherwise we ping-pong: parallel orch starts a
        # peer with creds+strips, 18s later cache-only orch hard-stops
        # and re-starts it as cache-only, then parallel re-promotes
        # the next tick. Net: only the active peer ever runs frontier.
        parallel_authorised: set[str] = set(
            state_copy.get('parallel_frontiers_active') or [])
        parallel_authorised |= set(
            (state_copy.get('frontier_cred_plan') or {}).keys())
        peers = list(cfg.get('peers', []))

        # Currently running cache-only peers (by id).
        running_cache_only: list[str] = []
        # Eligible candidate peers (enabled, not scheduled, not the
        # active frontier, has bandwidth, online).
        candidates: list[dict] = []
        unreachable = 0
        for p in peers:
            pid = p['id']
            if not p.get('enabled', True):
                continue
            if _peer_is_scheduled(p):
                continue
            if pid == active_frontier:
                continue
            if pid in parallel_authorised:
                # Owned by the parallel-frontier orchestrator — leave
                # alone, otherwise we ping-pong frontier↔cache-only.
                continue
            if p.get('reserved_kg'):
                # Holds a frontier-only reservation — leave alone.
                continue
            bw = state_copy.get('peer_bandwidth', {}).get(pid, {})
            if _peer_bw_depleted(p, bw, cfg):
                continue
            ps = get_peer_status(p.get('url'))
            st = ps.get('state', 'unknown')
            if st == 'unreachable':
                unreachable += 1
                continue
            role = self._peer_role(p)
            running = st in ('running', 'processing')
            is_cache_only_run = bool(ps.get('cache_only'))
            # Stale cached status — don't make stop/start decisions on
            # potentially out-of-date information. Treat as 'leave alone'.
            stale = bool(ps.get('_stale'))
            if running and is_cache_only_run and not stale:
                running_cache_only.append(pid)
            candidates.append({'peer': p, 'role': role, 'state': st,
                               'is_cache_only_run': is_cache_only_run,
                               'stale': stale, 'status': ps})

        # If we're over the cap (e.g. capacity factor dropped, or peers
        # were started before the cap shrank), gracefully stop the
        # excess. Pick the noisiest peers first — they're the ones
        # we want off the upstreams anyway. The graceful stop lets them
        # finish the current KG; we keep the count unchanged so we
        # don't start a fresh peer in the same tick to replace one
        # we just told to drain.
        # CPU-steal damping. exe.dev steal is almost entirely from
        # other tenants on the shared hypervisor pool, not from us —
        # so reducing peer count rarely recovers cycles 1:1. We still
        # damp the ramp *slightly* at very-high steal so:
        #   (a) we don't pile new starts on top of a wedged pool, and
        #   (b) the LPT partition has a chance to drain the heaviest
        #       KGs from the worst peers before more land.
        # The damping curve is intentionally gentle: 0.85 at steal=40%,
        # 0.70 at steal=60%, 0.55 floor. The LPT partition itself does
        # the *real* load-balancing per-peer; this is just a fleet-wide
        # ramp brake.
        try:
            _med_steal = _fleet_steal_median(list(running_cache_only))
            if _med_steal is not None and _med_steal >= 40.0:
                _cpu_cap_factor = max(0.55,
                                       1.0 - (_med_steal - 30.0) / 200.0)
                _prev_cap = max_cache_only
                max_cache_only = max(0, int(round(
                    max_cache_only * _cpu_cap_factor)))
                if max_cache_only < _prev_cap:
                    log.info(
                        'fleet steal median %.0f%% (n=%d) → cpu_factor '
                        '%.2f → max_cache_only_peers %d → %d',
                        _med_steal, len(running_cache_only),
                        _cpu_cap_factor, _prev_cap, max_cache_only,
                    )
        except Exception:
            pass

        excess = len(running_cache_only) - max_cache_only
        if excess > 0:
            # Rank running peers by noise score (highest first), then by
            # id for stability. Bias toward stopping recently-started
            # peers if scores tie (fresh starts are cheap to forfeit).
            noisy_first = sorted(
                running_cache_only,
                key=lambda pid: (-_peer_noise_score(pid, state_copy), pid),
            )
            to_stop = noisy_first[:excess]
            log.info(
                'cache-only over cap: %d running > %d max — gracefully '
                'stopping %d (noisiest first): %s',
                len(running_cache_only), max_cache_only, excess, to_stop,
            )
            # Don't re-send graceful stop within 60s — the peer's signal
            # handler sets _shutdown_requested=True but only exits after
            # finishing its current KG. Re-sending every director tick
            # spams the peer's log with SIGTERM warnings (we saw 63 in 36h
            # on at32 with only 1 actual restart). The first stop is
            # enough; let the peer drain in peace.
            now_ts = time.time()
            stop_skipped = []
            for pid in to_stop:
                last = _LAST_GRACEFUL_STOP_TS.get(pid, 0)
                if now_ts - last < 60:
                    stop_skipped.append(pid)
                    continue
                p = get_peer_by_id(cfg, pid)
                if not p:
                    continue
                try:
                    stop_peer_processor(p.get('url'), graceful=True)
                    _LAST_GRACEFUL_STOP_TS[pid] = now_ts
                except Exception as e:
                    log.warning('Graceful stop excess cache-only %s failed: %s',
                                pid, e)
            if stop_skipped:
                log.debug('Graceful stop already in flight for %d peers (<60s ago): %s',
                          len(stop_skipped), stop_skipped)
            # Deliberately *don't* shrink running_cache_only — a graceful
            # stop only takes effect when the peer finishes its current
            # KG. Until then it's still consuming an upstream slot, so
            # max_add must stay clamped to zero or negative.
            # Also drop them from `to_start` candidate pools later.
            _draining_cache_only = set(to_stop)
        else:
            _draining_cache_only = set()

        # Compute reserve target.  Reserve peers must be enabled+online
        # but idle.  Count idle-eligible peers in `candidates`.
        # 'complete' is a terminal post-run state set when a peer's
        # processor exited cleanly after finishing its last KG; it's
        # just as idle as 'stopped' from our perspective. Without
        # including it here, peers that finished a cache-only KG
        # would never be re-eligible for the next one, and the fleet
        # would drain to ~1 running cache-only peer (matches the
        # observed '1/24 cache' state).
        idle_eligible = [c for c in candidates
                         if c['state'] in ('idle', 'stopped', 'complete',
                                            'unknown')]

        # Total enabled peers excluding scheduled-out (e.g. not_before in
        # the future, like a primary that's been parked until the next
        # bandwidth cycle). Without this, the reserve math wastes slots
        # on peers we can't ever activate.
        total_enabled = sum(1 for p in peers
                            if p.get('enabled', True)
                            and not _peer_is_scheduled(p))
        # We must keep `min_reserve` peers idle.  Subtract running peers
        # (frontier active + already-running cache-only) from total.
        running_count = (1 if active_frontier else 0) + len(running_cache_only)
        # Maximum we may add such that idle remaining ≥ min_reserve.
        # idle_after_add = total_enabled - running_count - add
        # We want idle_after_add ≥ min_reserve.
        slack = max(0, total_enabled - running_count - min_reserve)
        max_add = min(slack, max_cache_only - len(running_cache_only))

        # Compute cache-ready whitelist (cheap due to caching).
        whitelist = self._compute_cache_ready_kgs()
        # v2 upgrade backfill: when the fleet is starved of cache-ready
        # work, hand idle peers v1-complete KGs to re-classify from their
        # own full GPKG. Kept separate from ``whitelist`` until after
        # block expansion (these are already product codes, and the
        # completed-block guard would otherwise drop them).
        v2_codes = self._v2_upgrade_fill(whitelist, cfg)
        if v2_codes:
            # Log on change or every 10 min — this runs every ~35 s tick.
            _sig = (len(whitelist), len(v2_codes))
            _now = time.time()
            if (_sig != getattr(self, '_v2_fill_last_sig', None)
                    or _now - getattr(self, '_v2_fill_last_log', 0) > 600):
                self._v2_fill_last_sig = _sig
                self._v2_fill_last_log = _now
                log.info('v2 upgrade: %d cache-ready KGs (gate %d%s) — adding %d upgrade '
                         'unit(s) (%d candidates)', len(whitelist),
                         V2_UPGRADE_FILL_BELOW_READY,
                         ', v24_first' if self._v24_first(cfg) else '', len(v2_codes),
                         (self.state.get('_v2_cand_cache') or {}).get('total', 0))
        # Sliver-sweep parents waiting for a worker count as cache-only
        # work too: before 2026-11-09 both "whitelist drained" returns
        # below fired first, so at the end of the run (ready_kgs=0,
        # v2 cand=0) 8 pending parents / 14 open units sat for 7 h while
        # 25 sweep-capable workers idled — the sweep assign lives at the
        # bottom of this orchestrator and was never reached.
        _sweep_pending = 0
        try:
            import sliver_sweep as _sls_pc
            _sweep_pending = _sls_pc.pending_count()
        except Exception:  # noqa: BLE001
            _sweep_pending = 0
        if not whitelist and not v2_codes and not _sweep_pending:
            # Nothing to do for cache-only peers.  Stop any that are
            # running (they'd otherwise idle-loop a fresh subprocess).
            # Use a graceful stop so the peer finishes its current KG
            # before exiting — cache-only work is pure CPU + Zenodo
            # upload, no Copernicus credentials at risk, so there's no
            # reason to kill mid-KG and waste the work.  If the
            # whitelist refills before the peer drains, the next tick
            # will see it still running and let it carry on.
            if running_cache_only:
                log.info('No cache-ready KGs — gracefully stopping %d running '
                         'cache-only peers (will exit after current KG)',
                         len(running_cache_only))
                for pid in running_cache_only:
                    p = get_peer_by_id(cfg, pid)
                    if p:
                        try:
                            stop_peer_processor(p.get('url'), graceful=True)
                        except Exception as e:
                            log.warning('Graceful stop cache-only %s failed: %s', pid, e)
            return

        if max_add <= 0 and not running_cache_only:
            # No room to add and none running — nothing to do.
            return

        # Choose new peers to start.  Prefer peers explicitly tagged
        # role=='cache_only'; fall back to idle frontier peers.
        # Within each group prefer quiet peers (low warning fingerprint)
        # so fresh / clean peers absorb load while noisy ones cool off.
        def _noise_key(c):
            return (_peer_noise_score(c['peer']['id'], state_copy),
                    c['peer']['id'])
        idle_cache_only = sorted(
            [c for c in idle_eligible if c['role'] == 'cache_only'],
            key=_noise_key)
        idle_frontier = sorted(
            [c for c in idle_eligible if c['role'] != 'cache_only'],
            key=_noise_key)
        # Ramp limiter: scale starts/tick by capacity_factor so we add
        # peers gradually when upstreams are unhappy. At factor=1.0 we
        # add up to RAMP_MAX_STARTS_PER_TICK; at THROTTLE_MIN_FACTOR
        # only RAMP_MIN_STARTS_PER_TICK. The cap shrinks max_add but the
        # next tick (~30s later) will fill any remaining slots.
        #
        # Deficit override: if the running fleet is well below cap
        # (e.g. after a fleet-wide graceful-update wave drained
        # cache-only peers at KG boundary), the 1-3 starts/tick cap
        # can't keep up with the drain rate (5 graceful/tick from
        # ``_orchestrate_stale_peer_updates``). Without an override
        # the chart drops to 0/24 cache and stays there for ~10 min
        # after every srv restart that advances the director commit.
        # When deficit > ramp_cap we lift the cap so refill matches
        # drain. Throttle still applies via ``max_cache_only`` itself
        # (already scaled by capacity_factor above).
        ramp_span = RAMP_MAX_STARTS_PER_TICK - RAMP_MIN_STARTS_PER_TICK
        ramp_cap = RAMP_MIN_STARTS_PER_TICK + int(round(ramp_span * _factor))
        ramp_cap = max(RAMP_MIN_STARTS_PER_TICK, ramp_cap)
        deficit = max_cache_only - len(running_cache_only)
        if deficit > ramp_cap and max_add > ramp_cap:
            log.info(
                'cache-only deficit %d > ramp_cap %d (running=%d/%d) '
                '— lifting ramp cap for this tick',
                deficit, ramp_cap, len(running_cache_only),
                max_cache_only,
            )
            # Cap at the deficit itself so we don't overshoot when
            # drain catches up. ``max_add`` was already clamped by
            # ``slack`` (reserve) and (max_cache_only - running),
            # so this can't push us past the cap.
            ramp_cap = min(deficit, max_add)
        max_add = min(max_add, ramp_cap)
        to_start = []
        for c in idle_cache_only + idle_frontier:
            if len(to_start) >= max_add:
                break
            to_start.append(c['peer'])

        # --- Build claimed-KG set so no two cache-only peers (and not
        # the frontier) target the same KG.  We pull each peer's current
        # KG from the status snapshot we already gathered above, plus
        # the active frontier's current_kg.
        #
        # Split-KG awareness: a peer mid-work on block ``60336-northwest``
        # also blocks the parent ``60336`` (cache-only whitelist contains
        # parent KG codes from kg_list.json, never block codes), so we
        # always add the parent for any block we see.
        import re as _re
        in_progress: set[str] = set()
        # Codes literally seen on a peer (block or unsplit parent) — as
        # opposed to parents implied by a running block. The pre-expansion
        # whitelist filter must only drop these: dropping an implied parent
        # starves its idle siblings (2026-11-07: 87011 had 1/12 blocks
        # running and 11 cache-ready blocks never dispatched while 30
        # cache-only peers sat COMPLETE for >12 h).
        in_progress_direct: set[str] = set()
        def _mark(code):
            if not code:
                return
            s = str(code)
            in_progress.add(s)
            in_progress_direct.add(s)
            m = _re.match(r'^(\d+)-[a-z][-a-z0-9]*$', s)
            if m:
                in_progress.add(m.group(1))
        # Scan ALL enabled peers (not just ``candidates``) for in-progress
        # KGs. ``candidates`` excludes peers that are filtered out by
        # eligibility — recently parked (``_peer_is_scheduled``), low
        # bandwidth, holding a ``reserved_kg``, parallel-authorised, or
        # the active frontier. Those peers can still be mid-KG: a peer
        # parked at 95%% bandwidth keeps running its current KG until
        # the next director graceful-stop / KG boundary; a peer just
        # un-parked typically hasn't picked work yet but its prior KG
        # may still be in progress; a recently-parked peer might be
        # parked-with-graceful-stop and still finishing. Missing any
        # of these from the in_progress set let another cache-only peer
        # be assigned the same KG (observed: at35+at50 on 73517 ≈23 h,
        # at61+at65 on 01614 ≈21 h, at25+at75 on 56551 ≈16 min apart).
        # Sticky ownership (10 min TTL) doesn't save us for long-
        # running KGs.
        active_states = ('running', 'processing', 'paused', 'paused_zenodo')
        for p_all in peers:
            if not p_all.get('enabled', True):
                continue
            try:
                ps2 = get_peer_status(p_all.get('url'))
            except Exception:
                ps2 = {}
            # Only mark KGs from peers whose processor is actually
            # running/paused right now. ``current_kg`` lingers in
            # progress.json after SIGTERM with state='stopped' (e.g.
            # after a fleet-wide update_peers wave) — using those stale
            # codes drains the whitelist (``30 in-progress KGs > 15
            # ready`` -> empty after filter -> 0/60 cache-only running).
            st2 = ps2.get('state')
            if st2 not in active_states:
                continue
            ckg = (ps2.get('current_kg') or {})
            _mark(ckg.get('code') if isinstance(ckg, dict) else None)
            _mark(ps2.get('in_progress'))
        # Frontier's KG — both the primary active frontier AND every
        # parallel-authorised frontier. Earlier we excluded those peers
        # from ``candidates`` (they're owned by the parallel orch), so
        # their current_kg never reached _mark — meaning the cache-only
        # whitelist filter couldn't see them, and a cache-only peer
        # could be assigned the same KG a parallel frontier was already
        # processing (observed: at60 on 72321 Mitteregg for 20h42m, then
        # at17 handed 72321 as its cache-only slice). Iterate both sets,
        # de-duplicated, to close that hole.
        frontier_ids = set()
        if active_frontier:
            frontier_ids.add(active_frontier)
        frontier_ids |= parallel_authorised
        for fid in frontier_ids:
            fp = get_peer_by_id(cfg, fid)
            if not fp:
                continue
            try:
                fps = get_peer_status(fp.get('url'))
                fst = fps.get('state')
                if fst in ('running', 'processing', 'paused',
                            'paused_zenodo'):
                    fkg = (fps.get('current_kg') or {})
                    _mark(fkg.get('code') if isinstance(fkg, dict) else None)
                    _mark(fps.get('in_progress'))
            except Exception:
                pass
        # Filter the whitelist before partitioning. Parents that are only
        # *implied* in-progress by one running block stay — the block
        # expansion below hands out their idle siblings and skips the
        # claimed one.
        whitelist = [k for k in whitelist if k not in in_progress_direct]
        if v2_codes:
            # Re-fill now that claims are known: drop in-flight codes
            # *before* the cap and size the batch to the worker count
            # (+2 slack) so every idle cache-only peer gets >=1 unit.
            _n_workers = len((set(running_cache_only)
                              | {p['id'] for p in to_start})
                             - _draining_cache_only)
            # Queue depth 2/worker (+2): at fast-path cadence a peer
            # finishes a unit every few minutes and must find the next
            # one already in its queue (mid-run injection, 2026-10-04)
            # instead of going COMPLETE and waiting for a restart.
            v2_codes = self._v2_upgrade_fill(
                whitelist, cfg, exclude=in_progress,
                min_units=2 * _n_workers + 2)
        v2_codes = [k for k in v2_codes if k not in in_progress]
        if not whitelist and not v2_codes and not _sweep_pending:
            log.info('Cache-only orchestrate: whitelist drained after excluding '
                     '%d in-progress KGs', len(in_progress))
            return

        # --- Expand parent KG codes into block codes for split KGs.
        # Each peer independently runs maybe_split_kg() at startup, but
        # they all start at block 0 — so two peers given the same parent
        # code race the first block (e.g. both grabbing 92117-west).
        # Pre-expanding here means the stride partition below hands
        # different blocks of e.g. Ramsau to different peers.
        # Honors strikes (adaptive split for repeat-failed KGs that are
        # otherwise quick from cache).
        try:
            from kg_splitter import maybe_split_kg, is_block_code
            kg_list_path = DATA_DIR / 'kg_list.json'
            kg_by_code = {}
            if kg_list_path.exists():
                for kg in json.loads(kg_list_path.read_text()):
                    c = kg.get('kg_code')
                    if c:
                        kg_by_code[c] = kg
            # Block-level guards the PARENT-granularity whitelist scan in
            # ``_compute_cache_ready_kgs`` structurally cannot apply (it only
            # ever sees parent codes from kg_list.json):
            #  * A per-block cache miss is recorded under the BLOCK code
            #    (e.g. ``91101-east`` uncacheable), but the whitelist scan
            #    only excludes the PARENT (``91101``) — which isn't in the
            #    miss set — so the bad block re-enters *here* after expansion
            #    and churns ``deferred to frontier`` every tick across
            #    successive cache-only peers (observed: 91101-east, 62201 on
            #    at44/at101/at122/at124).
            #  * A split parent can be half-done: one block already has a
            #    ``_json`` (in ``_get_completed_kgs``), the other doesn't, so
            #    the parent isn't ``processed=1`` and stays on the whitelist —
            #    the finished block must not be re-dispatched alongside its
            #    unfinished sibling.
            try:
                block_excluded = set(self._cache_miss_excluded())
            except Exception:
                block_excluded = set()
            try:
                from app import _get_completed_kgs as _gck
                block_excluded |= _gck()
            except Exception:
                pass
            expanded: list[str] = []
            n_split = 0
            n_block_excl = 0
            for code in whitelist:
                if is_block_code(code):
                    # Already a block code (e.g. queued explicitly) —
                    # honor as-is, skip in_progress / done / missed siblings.
                    if code in block_excluded:
                        n_block_excl += 1
                    elif code not in in_progress:
                        expanded.append(code)
                    continue
                kg = kg_by_code.get(code)
                if not kg:
                    expanded.append(code)
                    continue
                blocks = maybe_split_kg(kg)
                if len(blocks) > 1:
                    n_split += 1
                    for blk in blocks:
                        bc = blk['kg_code']
                        # Drop sibling blocks already claimed by some peer,
                        # already completed, or with an unresolved cache miss.
                        if bc in block_excluded:
                            n_block_excl += 1
                            continue
                        if bc in in_progress:
                            continue
                        expanded.append(bc)
                else:
                    if code not in in_progress:
                        expanded.append(code)
            if n_block_excl:
                log.info('Cache-only whitelist: dropped %d split-block code(s) '
                         'already completed or cache-missed (block-level guard)',
                         n_block_excl)
            if n_split:
                log.info('Cache-only whitelist: expanded %d parent KGs into '
                         'blocks (%d → %d codes) for cross-peer distribution',
                         n_split, len(whitelist), len(expanded))
            whitelist = expanded
            if not whitelist and not v2_codes:
                log.info('Cache-only orchestrate: whitelist empty after block expansion')
                return
        except Exception as e:
            log.warning('Block expansion failed (continuing with parent codes): %s', e)
            whitelist = [k for k in whitelist if k not in in_progress]
        if v2_codes:
            whitelist = whitelist + [c for c in v2_codes if c not in set(whitelist)]
            if not whitelist:
                return

        # --- Complexity-weighted, capacity-aware LPT partition.
        #
        # Old behaviour (kept here for context): stride by sorted peer
        # id (``whitelist[i::n]``). Disjoint and stable, but blind to
        # KG size *and* peer strength — so a 28-tile KG and a 1-tile KG
        # were equally likely to land on a 80 %-steal peer as a 0 %-
        # steal one. With exe.dev pools regularly delivering 30+ peers
        # at ≥15 %% steal, that asymmetry costs hours of wall time per
        # day.
        #
        # New behaviour: Longest-Processing-Time (LPT) bin-packing.
        # Sort the whitelist by weight (≈ tile count) descending, then
        # for each KG pick the worker minimising ``load[p] / capacity[p]``
        # where capacity = max(0.10, 1 - steal_ewma/100). Stable across
        # ticks because weights + capacities change slowly; sticky-
        # ownership pass below still wins for in-flight KGs.
        all_workers = sorted((set(running_cache_only) |
                              {p['id'] for p in to_start})
                             - _draining_cache_only)
        if not all_workers:
            return
        weights = _kg_weights(whitelist)
        # Operator-pinned v2 priority codes sort first (and hence land at
        # the head of their peer's queue).
        for _c in self._load_v2_priority():
            if _c in weights:
                weights[_c] = weights[_c] + 1e6
        # v2.4-first: upgrade units sort ahead of fresh KGs in every slice
        # (equal boost → LPT spreads them round-robin, one per peer first).
        if v2_codes and self._v24_first(cfg):
            for _c in v2_codes:
                if _c in weights:
                    weights[_c] = weights[_c] + 1e5
        capacities = {pid: _peer_cpu_capacity(pid) for pid in all_workers}
        load: dict[str, float] = {pid: 0.0 for pid in all_workers}
        slices: dict[str, list] = {pid: [] for pid in all_workers}
        # Sort by weight desc, then code for determinism. Codes with
        # equal weight then land round-robin which preserves the
        # diversification property.
        ordered_codes = sorted(whitelist,
                                key=lambda c: (-weights.get(c, 1.0), c))
        for code in ordered_codes:
            w = weights.get(code, 1.0)
            # Pick the worker with smallest projected normalised load.
            # Tiebreak on raw load, then peer id, for determinism.
            chosen = min(
                all_workers,
                key=lambda pid: (
                    (load[pid] + w) / max(capacities[pid], 0.01),
                    load[pid],
                    pid,
                ),
            )
            slices[chosen].append(code)
            load[chosen] += w
        # Log a summary so we can verify diversification in journalctl.
        try:
            nonzero = [v for v in load.values() if v > 0]
            mx = max(load.values()) if load else 0.0
            mn_nz = min(nonzero) if nonzero else 0.0
            n_used = len(nonzero)
            log.info(
                'cache-only LPT partition: workers=%d (used=%d) kgs=%d '
                'weight_total=%.0f per_used min=%.0f max=%.0f '
                'imbalance=%.2f',
                len(all_workers), n_used, len(whitelist),
                sum(load.values()), mn_nz, mx,
                (mx / max(mn_nz, 0.01)) if mn_nz else 1.0,
            )
        except Exception:
            pass

        # --- Sticky ownership: honor prior-tick assignments so a KG
        # that's already in flight on peer P doesn't get reassigned to
        # peer Q just because the stride math shifted (peer joined/left
        # all_workers, whitelist length changed, etc.). Without this, two
        # peers can be told to run the same KG within a few ticks of
        # each other; we observed at25 and at26 both processing 72321
        # Mitteregg.
        #
        # The in_progress filter above only catches duplicates *after*
        # the second peer's status reflects current_kg — there's a small
        # race window between PUT-queue and current_kg being visible.
        # Sticky ownership closes that window deterministically.
        sticky_ttl = 600.0  # seconds; covers a long-running KG comfortably
        sticky_now = time.time()
        prior = self.state.get('cache_only_assigned') or {}
        owner: dict[str, str] = {}
        worker_set = set(all_workers)
        for pid_p, info in prior.items():
            if pid_p not in worker_set:
                continue  # peer no longer eligible — release its claims
            ts = info.get('ts', 0)
            if (sticky_now - ts) > sticky_ttl:
                continue  # stale — release
            for kg in info.get('slice', []) or []:
                # First-write wins; deterministic because prior is a dict
                # keyed by pid, but iteration order is insertion order so
                # we sort to be safe.
                owner.setdefault(kg, pid_p)
        if owner:
            fixed = {pid: [kg for kg in chunk
                           if owner.get(kg, pid) == pid]
                     for pid, chunk in slices.items()}
            stolen = sum(len(slices[p]) - len(fixed[p]) for p in slices)
            if stolen:
                log.info('Cache-only sticky ownership: kept %d KGs with '
                         'their prior owners (stride wanted to reshuffle)',
                         stolen)
            slices = fixed

        # --- Sliver sweep (sliver_sweep.py, docs/sliver-sweep.md): a few
        # of the cache-only workers (knob ``sweep_peers``) get the pending
        # sweep parents at the HEAD of their slice; the unit spec goes to
        # the peer first (PUT /processing/sweep_spec) so the queue push
        # that follows turns the parents into bbox units instead of KGs.
        try:
            import sliver_sweep as _sls
            # Only peers whose running code knows the sweep protocol
            # (SWEEP_MIN_COMMIT): the rollout is graceful, so for hours
            # after a deploy most cache-only peers still run the old
            # commit and would 405 the spec push / never report.
            _cap_status = {c['peer']['id']: c.get('status') for c in candidates}
            _sweep_workers = [w for w in all_workers
                              if _peer_sweep_capable(_cap_status.get(w))]
            # Auto-scale (2026-10-27): every sweep-capable cache-only
            # worker the v2 upgrade does NOT need this tick becomes a
            # sweeper — the upgrade is the cache-only pool's first job
            # (v24_first), so as it runs out of candidates (dispatch
            # 3/3 cand today) the surplus workers would otherwise just
            # chew fresh cache-ready KGs that the frontiers also cover.
            # Floor = ``sweep_peers`` (4), ceiling = ``sweep_peers_max``
            # (24; ``sweep_peers_auto: false`` pins the floor). Workers
            # currently on a v2 unit / with the emptiest slice are
            # preferred when the sweeper set grows.
            _n_sw = int(knob(cfg, 'sweep_peers', _sls.SWEEP_PEERS_DEFAULT))
            _prefer: list[str] = []
            if knob(cfg, 'sweep_peers_auto', True) and _sweep_workers:
                _v2_running = {w for w in _sweep_workers
                               if (_cap_status.get(w) or {}).get('current_kg_v2_upgrade')}
                _v2_demand = len(v2_codes or []) + len(_v2_running)
                _cap_sw = int(knob(cfg, 'sweep_peers_max', 24))
                _n_auto = len(_sweep_workers) - _v2_demand
                _n_sw = max(_n_sw, min(_cap_sw, _n_auto))
                _prefer = sorted((w for w in _sweep_workers if w not in _v2_running),
                                 key=lambda w: (len(slices.get(w, [])), w))
                _sig = (_n_sw, len(_sweep_workers), _v2_demand)
                if _sig != getattr(self, '_sweep_auto_sig', None):
                    self._sweep_auto_sig = _sig
                    _emit_director_event(
                        f"sweep: auto-scale → {_n_sw} sweeper(s) "
                        f"(capable workers={len(_sweep_workers)}, v2 demand={_v2_demand}, "
                        f"floor={knob(cfg, 'sweep_peers', _sls.SWEEP_PEERS_DEFAULT)} cap={_cap_sw})",
                        peer='director')
            _busy = {w for w in _sweep_workers
                     if ((_cap_status.get(w) or {}).get('current_kg') or {}).get('code')}
            _sw = _sls.assign(_sweep_workers, in_progress=in_progress,
                              n_peers=_n_sw, prefer=_prefer, busy=_busy)
            for _pid, _specs in _sw.items():
                _pp = get_peer_by_id(cfg, _pid)
                if not _pp:
                    continue
                try:
                    _push_sweep_spec(_pp['url'], _specs)
                except Exception as _e:  # noqa: BLE001
                    log.warning('sweep spec push to %s failed: %s', _pid, _e)
                    # Release only when the peer does not know the route
                    # (404/405 = pre-sweep commit).  A 503 / timeout is the
                    # peer's gunicorn being busy: it still holds the last
                    # spec and is mid-unit — releasing would hand its
                    # parents to another peer and leave the in-flight unit
                    # orphaned (the 08:03 at226 churn, 2026-10-27); the
                    # spec is re-pushed next tick anyway.
                    _sc = getattr(getattr(_e, 'response', None), 'status_code', None)
                    if _sc in (404, 405):
                        try:
                            _sls.release(_pid, f'spec push failed: {str(_e)[:80]}')
                        except Exception:  # noqa: BLE001
                            pass
                    continue
                _codes = list(_specs.keys())
                slices[_pid] = _codes + [c for c in slices.get(_pid, []) if c not in _codes]
                _new = [c for c in _codes if c not in (self.state.get('sweep_pushed') or {}).get(_pid, [])]
                if _new:
                    _emit_director_event(f"sweep: {_pid} ← {len(_codes)} parent(s) "
                                         f"({', '.join(_new[:4])}{'…' if len(_new) > 4 else ''})", peer=_pid)
            # Peers that held sweep parents last tick but none now: clear
            # their spec (one tiny PUT) so a processor restart cannot
            # re-run settled units from the stale sweep_spec.json
            # (42119-sw3 on at12 every 3 min, 2026-11-09).
            for _pid in list((self.state.get('sweep_pushed') or {}).keys()):
                if _pid in _sw:
                    continue
                _pp = get_peer_by_id(cfg, _pid)
                if not _pp:
                    continue
                try:
                    _push_sweep_spec(_pp['url'], {})
                except Exception as _e:  # noqa: BLE001
                    log.debug('sweep spec clear on %s failed: %s', _pid, _e)
            with self._lock:
                self.state['sweep_pushed'] = {pid: list(sp.keys()) for pid, sp in _sw.items()}
        except Exception as _e:  # noqa: BLE001
            log.warning('sliver sweep assign failed: %s', _e)

        # Start new peers with their slice.
        for p in to_start:
            chunk = slices.get(p['id'], [])
            if not chunk:
                continue
            log.info('Starting cache-only peer %s with %d KGs (LPT %d/%d, '
                     '%d total ready)', p['id'], len(chunk),
                     all_workers.index(p['id']) + 1, len(all_workers),
                     len(whitelist))
            try:
                start_peer_processor(p.get('url'), cache_only=True,
                                     queue_whitelist=chunk,
                                     v2_upgrade=bool(knob(cfg, 'v2_upgrade', True)))
            except Exception as e:
                log.warning('Start cache-only on %s failed: %s', p['id'], e)

        # Re-sync slices to already-running peers (whitelist may have
        # grown/shrunk since they started).  Cheap PUT.  Skip peers we
        # asked to drain — we don't want to feed them more work.
        for pid in running_cache_only:
            if pid in _draining_cache_only:
                continue
            p = get_peer_by_id(cfg, pid)
            if not p:
                continue
            chunk = slices.get(pid, [])
            if not chunk:
                continue
            try:
                _push_queue_to_peer(p['url'], chunk)
            except Exception as e:
                log.debug('Resync queue to %s failed: %s', pid, e)

        # Persist sticky ownership for next tick. Only record peers that
        # actually got a non-empty slice this tick — empty-slice peers
        # have no claims to defend.
        new_assigned = {pid: {'slice': chunk, 'ts': sticky_now}
                        for pid, chunk in slices.items() if chunk}

        # Status accounting
        with self._lock:
            self.state['cache_only_active'] = list(set(
                running_cache_only + [p['id'] for p in to_start]))
            self.state['cache_only_assigned'] = new_assigned
            self.state['v2_upgrade_assigned'] = sorted(v2_codes)
            save_director_state(self.state)

    # Split-layout-drift wedge repair: how many parents to unwedge per
    # sweep (each repair invalidates a few block products and triggers a
    # re-process, so pace it like every other fleet-touching action).
    SPLIT_DRIFT_REPAIRS_PER_SWEEP = 2

    def _repair_split_drift_wedges(self) -> int:
        """Unwedge queued parent KGs stuck by split-layout drift.

        Symptom (Egg/91007, Aug 2026): the dashboard shows every block pip
        filled ("complete") yet the code never leaves the priority queue and
        no peer ever works on it. Cause: ``kg_splitter.maybe_split_kg`` is a
        function of ``kg_strikes.json``, so a KG's block layout changes as
        strikes accumulate (Egg: 8 blocks -> 15 blocks at 2 strikes). Labels
        are reused between layouts but the bboxes differ, so the manifest ends
        up holding a mix of old- and new-geometry block ``_json`` products
        that cover every current *label* while leaving a geometric hole:

          * ``process.html`` pips are label-based        -> looks complete
          * ``_kg_coverage_complete`` is geometry-based  -> never pruned
          * ``austria_processor`` skips blocks by CODE   -> nothing schedulable

        Repair: invalidate the minimal set of block products whose *current*
        bbox spans the hole (``app._split_drift_plan``), so those blocks
        become schedulable again and re-process with today's geometry. The
        parent then genuinely completes and drains from the queue.

        Runs on the same cadence as ``_prune_priority_queue`` (~2.5 min) and
        is a no-op on a healthy fleet.
        """
        try:
            from app import (_split_drift_wedged as _wedged,  # type: ignore
                             _repair_split_drift as _repair)
        except Exception:
            return 0
        queue_path = DATA_DIR / 'retry_queue.json'
        mf_path = DATA_DIR / 'zenodo_manifest.json'
        try:
            queue = json.loads(queue_path.read_text()) if queue_path.exists() else []
            mf_raw = json.loads(mf_path.read_text()) if mf_path.exists() else {}
        except Exception:
            return 0
        mentries = mf_raw.get('entries', mf_raw) or {}
        if not queue or not mentries:
            return 0
        repaired = 0
        for code in queue:
            if repaired >= self.SPLIT_DRIFT_REPAIRS_PER_SWEEP:
                break
            try:
                wedged, plan = _wedged(code, mentries)
            except Exception:
                continue
            if not wedged:
                continue
            if not plan:
                msg = (f'split-drift {code}: wedged (all current blocks '
                       f'committed but coverage incomplete) and no block '
                       f'repair closes the hole - needs manual inspection')
                log.warning(msg)
                _emit_director_event(msg, kg=str(code), level='warning')
                continue
            try:
                dropped = _repair(code, plan)
            except Exception as e:
                log.warning('split-drift %s: repair failed: %s', code, e)
                continue
            if not dropped:
                continue
            repaired += 1
            msg = (f'split-drift {code}: block layout changed between runs - '
                   f'invalidated {len(dropped)} product(s) for '
                   f'{",".join(plan)} so they re-process with the current '
                   f'geometry and the KG can drain')
            log.warning(msg)
            _emit_director_event(msg, kg=str(code), level='warning')
            # Manifest changed underneath us; re-read for the next candidate.
            try:
                mf_raw = json.loads(mf_path.read_text())
                mentries = mf_raw.get('entries', mf_raw) or {}
            except Exception:
                break
        return repaired

    def _prune_priority_queue(self) -> int:
        """Drop stale codes from the local retry_queue.json.

        The primary VM is disabled (never runs the processor), so the
        in-processor remover (`_remove_from_retry_queue` in
        ``austria_processor.py``) never fires there — leaving the
        file append-only.  Manifest entries / local JSONs from peers
        do get synced back to the primary, but the queue keeps the
        stale codes around forever and re-pushes them to peers next
        tick.  Even with ``skip_processed=True`` on the peer side that
        creates rare races where a peer slightly behind on manifest
        sync re-processes a finished KG.

        We prune codes that are either:
          * present in `_get_completed_kgs()` (locally finished),
          * a parent KG whose blocks all have ``_json`` in the manifest
            (block expansion is implicit at processor startup).

        Tombstoned codes (force-requeue) are exempt — they survive in
        the queue by design.
        """
        queue_path = DATA_DIR / 'retry_queue.json'
        try:
            queue = json.loads(queue_path.read_text()) if queue_path.exists() else []
        except Exception:
            return 0
        if not queue:
            return 0
        try:
            from app import _get_completed_kgs, _tombstone_path  # type: ignore
            completed = _get_completed_kgs()
        except Exception:
            completed = set()
            _tombstone_path = None  # type: ignore
        tombstoned: set = set()
        try:
            if _tombstone_path and _tombstone_path.exists():  # type: ignore
                import re as _re
                tdata = json.loads(_tombstone_path.read_text())  # type: ignore
                if isinstance(tdata, dict):
                    for tk in tdata.keys():
                        m = _re.match(r'^(\d+(?:-[a-z][-a-z0-9]*)?)_', tk)
                        if m:
                            tombstoned.add(m.group(1))
        except Exception:
            pass
        # --- Break the force-requeue self-perpetuation loop. ---------------
        # A force-requeue stamps ``<kg>_requeue`` (and partial product keys)
        # as tombstones and keeps the code in retry_queue.json. Tombstoned
        # codes are exempt from staleness below, so the code re-pushes to a
        # peer with skip_processed=False forever; the peer's /processing/queue
        # handler re-stamps ``<kg>_requeue=now`` for any completed code, and
        # ``_get_completed_kgs`` drops tombstoned codes from ``completed`` — so
        # the KG reprocesses on a loop (delete+re-upload its gpkgs every cycle,
        # producing recurring 404 windows). Once the KG has genuinely
        # *re-completed* (a fresh ``_json`` manifest entry uploaded AFTER the
        # tombstone), drop its ``_requeue``/product tombstones so it counts as
        # completed again and falls out of the queue. Mirrors the satisfied-
        # tombstone sweep in app.py's priority-queue GET handler, but runs on
        # the director loop so the on-disk queue is actually drained.
        try:
            import re as _re2
            _mf_path = DATA_DIR / 'zenodo_manifest.json'
            _mf_entries = {}
            if _mf_path.exists():
                _mf_data = json.loads(_mf_path.read_text())
                _mf_entries = _mf_data.get('entries', _mf_data) or {}
            _tdata = {}
            if _tombstone_path and _tombstone_path.exists():  # type: ignore
                _td = json.loads(_tombstone_path.read_text())  # type: ignore
                if isinstance(_td, dict):
                    _tdata = _td
            try:
                from app import _requeue_recompleted  # type: ignore
            except Exception:
                _requeue_recompleted = None  # type: ignore
            try:
                from app import _kg_structurally_complete  # type: ignore
            except Exception:
                _kg_structurally_complete = None  # type: ignore
            _drop_keys = []
            _recompleted = set()
            for _tk, _tv in _tdata.items():
                _m = _re2.match(r'^(\d+(?:-[a-z][-a-z0-9]*)?)_', _tk)
                if not _m:
                    continue
                _kg = _m.group(1)
                # Split-aware re-completion: a split parent never emits a
                # parent ``_json`` — it completes via per-block ``_json``
                # entries. The old parent-only check left split parents
                # (e.g. 49106/22017/40320) re-stamping their ``_requeue``
                # tombstone every tick and never draining. Use the shared
                # helper that recognises all-blocks-complete.
                _done = False
                if _requeue_recompleted is not None:
                    try:
                        _done = _requeue_recompleted(_kg, _tv, _mf_entries)
                    except Exception:
                        _done = False
                # A loop can bump the requeue tombstone ahead of the block
                # uploads, so a strict ts-ordered check never fires. If the
                # KG is structurally complete right now (split-aware), the
                # requeue tombstone is stale by definition — drop it.
                #
                # EXCEPT for a ``keep_products`` requeue (v2_regen: the v1
                # ``_json`` is deliberately kept, only the verified-gone
                # ``_full_gpkg`` is tombstoned + deleted). Such a KG is
                # structurally complete *by construction* the moment it is
                # requeued, so this fallback dropped the tombstones 2-3 min
                # after every v2_regen requeue (Sep 2026: 20 codes), the
                # GET prune evicted the code, peer-sync re-imported the
                # dead ``_full_gpkg`` pointer, and the regen silently
                # reverted — while v2_regen kept counting them as
                # outstanding (cap 20 → the whole pipeline froze). Rule:
                # the structural shortcut only applies once every product
                # key tombstoned in the same batch (identical ts) is back
                # in the manifest, i.e. the fresh run actually replaced it.
                if not _done and _kg_structurally_complete is not None:
                    try:
                        _batch = [k for k, v in _tdata.items()
                                  if v == _tv and not k.endswith('_requeue')
                                  and _re2.match(r'^' + _re2.escape(_kg) + r'(-[a-z][-a-z0-9]*)?_', k)]
                        _missing = [k for k in _batch if not isinstance(_mf_entries.get(k), dict)]
                        if not _missing:
                            _done = _kg_structurally_complete(_kg, _mf_entries)
                    except Exception:
                        _done = False
                if not _done:
                    _je = _mf_entries.get(_kg + '_json')
                    if isinstance(_je, dict):
                        _ua = _je.get('uploaded_at', '') or ''
                        _done = bool(_ua) and str(_ua) > str(_tv)
                if _done:
                    _drop_keys.append(_tk)
                    _recompleted.add(_kg)
            if _drop_keys:
                for _k in _drop_keys:
                    _tdata.pop(_k, None)
                # Hard-delete the satisfied keys from the in-memory dict AND
                # disk via app's helper (a plain file rewrite here races the
                # gunicorn workers' union-on-persist, which would resurrect
                # them). Fall back to a direct rewrite only if the import
                # fails.
                try:
                    from app import _drop_tombstone_keys  # type: ignore
                    _drop_tombstone_keys(_drop_keys)
                except Exception:
                    try:
                        _tmp = _tombstone_path.with_suffix('.tmp')  # type: ignore
                        _tmp.write_text(json.dumps(_tdata, indent=2))
                        _tmp.replace(_tombstone_path)  # type: ignore
                    except Exception as _e:
                        log.warning('prune retry_queue: tombstone rewrite failed: %s', _e)
                # These KGs are completed again — un-exempt them so the
                # staleness check below drops them from the queue.
                tombstoned -= _recompleted
                completed |= _recompleted
                log.info('prune retry_queue: cleared %d satisfied requeue '
                         'tombstone(s) for %d re-completed KG(s): %s',
                         len(_drop_keys), len(_recompleted),
                         sorted(_recompleted)[:10])
        except Exception as _e:
            log.warning('prune retry_queue: requeue-loop resolution failed: %s', _e)
        # Parent→done check via the app.py coverage oracle (split-layout-drift
        # immune), mirroring the GET handler so on-disk pruning agrees with
        # what the dashboard does at view time. The old
        # ``len(done_blocks) >= len(maybe_split_kg(now))`` check left
        # historically-split-but-geographically-complete KGs (40326/62013)
        # queued forever because maybe_split now returns a single block whose
        # parent ``_json`` was never emitted.
        try:
            _mf_path_pd = DATA_DIR / 'zenodo_manifest.json'
            _mf_pd = {}
            if _mf_path_pd.exists():
                _mf_raw_pd = json.loads(_mf_path_pd.read_text())
                _mf_pd = _mf_raw_pd.get('entries', _mf_raw_pd) or {}
            from app import _kg_coverage_complete as _cov_pd  # type: ignore

            def _parent_done(code: str) -> bool:
                try:
                    done, _ = _cov_pd(code, _mf_pd)
                    return bool(done)
                except Exception:
                    return False
        except Exception:
            _parent_done = None

        def _is_stale(c: str) -> bool:
            if c in tombstoned:
                return False
            if c in completed:
                return True
            if _parent_done and _parent_done(c):
                return True
            return False
        kept = [c for c in queue if not _is_stale(c)]
        dropped = len(queue) - len(kept)
        if dropped <= 0:
            return 0
        try:
            tmp = queue_path.with_suffix('.tmp')
            tmp.write_text(json.dumps(kept))
            tmp.replace(queue_path)
            log.info('prune retry_queue: dropped %d stale code(s); %d remain',
                     dropped, len(kept))
        except Exception as _e:
            log.warning('prune retry_queue write failed: %s', _e)
            return 0
        return dropped

    def _sync_queue_to_active(self):
        """Push the director's local priority queue to the active remote peer.

        Only syncs if the queue has changed since the last sync (compares hash).
        """
        with self._lock:
            active_id = self.state.get('active_peer')
        if not active_id:
            return
        peer = get_peer_by_id(self.cfg, active_id)
        if not peer or peer.get('url') is None:
            return  # local peer doesn't need sync

        # Read current queue
        queue_path = DATA_DIR / 'retry_queue.json'
        try:
            queue = json.loads(queue_path.read_text()) if queue_path.exists() else []
        except Exception:
            queue = []

        # Hash check — skip if unchanged
        import hashlib
        # Key the hash on the recipient too: a new active peer must get
        # the (unchanged) queue once, else it only ever sees the copy it
        # got at start (cell-filtered, see austria_processor priority block).
        q_hash = hashlib.md5(json.dumps([active_id, queue]).encode()).hexdigest()
        with self._lock:
            if self.state.get('_last_queue_hash') == q_hash:
                return

        # Push to peer (excluding KGs reserved for cooled-down peers)
        with self._lock:
            cfg = self.cfg.copy()
        excl = _excluded_kgs(cfg, exclude_peer_id=active_id)
        result = sync_queue_to_peer(peer['url'], exclude=excl)
        if 'error' not in result:
            with self._lock:
                self.state['_last_queue_hash'] = q_hash

    def _loop(self):
        """Main director loop."""
        time.sleep(5)  # startup delay
        sync_counter = 0
        while self._running:
            # Cross-worker stop guard. ``stop()`` on the singleton only
            # affects the worker that called it; another worker that
            # had `_director` constructed (e.g. via ``get_director()``
            # for status reads) keeps its own loop running. Disk flags
            # are the source of truth: if the operator / takeover /
            # handover removed ``is_director`` (or wrote
            # ``stepped_down``) we must self-terminate. Without this,
            # a stepped-down primary kept hammering peers with
            # whitelist PUTs / identity broadcasts while the new
            # director (at63) was already running, producing the very
            # split-brain the file flag is supposed to prevent
            # (2026-05-10 incident).
            if not config.is_director():
                log.warning('director loop self-stopping: SRTM_PRIMARY_URL is set '
                            '(this instance is a worker)')
                self._running = False
                # Release fcntl lock so a future takeover in this same
                # process can acquire it cleanly.
                fd = getattr(self, '_lock_fd', None)
                if fd is not None:
                    try:
                        import fcntl as _fcntl
                        _fcntl.flock(fd, _fcntl.LOCK_UN)
                        fd.close()
                    except Exception:
                        pass
                    self._lock_fd = None
                return
            try:
                # Re-read config + state from disk each tick so cross-worker
                # changes (e.g. /director/activate from another gunicorn worker)
                # are picked up by the worker actually running the loop.
                try:
                    disk_cfg = load_peers_config()
                    disk_state = load_director_state()
                    with self._lock:
                        self.cfg = disk_cfg
                        # Merge: prefer disk values for active_peer/mode/last_switch
                        # but keep our in-memory bandwidth + unreachable_count.
                        for key in ('active_peer', 'mode', 'last_switch'):
                            if key in disk_state:
                                self.state[key] = disk_state[key]
                        # bev_pause is written by other workers via
                        # clear_bev_pause() / the dashboard chip; the
                        # director-loop worker must pick that up or it
                        # keeps the in-mem 'active=True' and ignores the
                        # operator clear. See 2026-05-28 cross-worker
                        # desync incident.
                        if 'bev_pause' in disk_state:
                            self.state['bev_pause'] = disk_state['bev_pause']
                        if 'bev_pool_escalation' in disk_state:
                            self.state['bev_pool_escalation'] = (
                                disk_state['bev_pool_escalation'])
                    # Re-apply live operator intents (release-hold / clear-
                    # cooldown served by the OTHER gunicorn worker) onto our
                    # freshly-reloaded cfg, BEFORE any mid-tick save can
                    # resurrect the cleared field from this loop's snapshot.
                    # See the operator-intent-journal section above.
                    _intent_dirty = False
                    try:
                        with self._lock:
                            _intent_dirty = apply_peer_intents(self.cfg)
                    except Exception:
                        log.exception('apply_peer_intents failed')
                    # Stamp first_seen on legacy peers that lack it so
                    # the warmup hold doesn't retroactively block them.
                    # New peers added via the API already get a stamp.
                    _now_iso = datetime.now(timezone.utc).isoformat()
                    _dirty = False
                    for _p in self.cfg.get('peers', []):
                        if not _p.get('first_seen'):
                            _p['first_seen'] = _now_iso
                            _dirty = True
                    if _dirty or _intent_dirty:
                        save_peers_config(self.cfg)
                except Exception:
                    pass
                _was_dormant = bool(
                    (self.state.get('fleet_dormant') or {}).get('active'))
                _was_complete = bool(
                    (self.state.get('run_complete') or {}).get('active'))
                if not (_was_dormant or _was_complete):
                    self._update_bandwidth()
                # Credential revalidation moved off the director loop
                # into a dedicated daemon thread (see _oidc_reval_loop)
                # so a 30 s parallel sweep can't stall this tick.
                # Capacity factor: poll each peer's processing status once
                # per tick (cheap; we already do it implicitly inside the
                # orchestrators) and use the warning_rates field to derive
                # how aggressively we should run. We stash both the factor
                # and the components on self.state so the dashboard can
                # show the user *why* we slowed down.
                try:
                    from concurrent.futures import (
                        ThreadPoolExecutor as _Tpe,
                        TimeoutError as _Fto,
                    )
                    _peers = list(self.cfg.get('peers', []))
                    _statuses: dict[str, dict] = {}
                    if _was_dormant or _was_complete:
                        # Dormant / run-complete: no network fanout. Evaluate pushed
                        # heartbeats only (a rebooting peer pushes to
                        # us within ~30 s and wakes the fleet); the
                        # probe sweep below covers push-less peers.
                        for _p in _peers:
                            if not _p.get('url'):
                                continue
                            _pu = get_pushed_status(_p['id'])
                            if _pu is not None:
                                _d = dict(_pu.get('status') or {})
                                _d['_pushed'] = True
                                _statuses[_p['id']] = _d
                            else:
                                _statuses[_p['id']] = {
                                    'state': 'unreachable'}
                        raise _DormantSkipFanout()
                    with _Tpe(max_workers=BANDWIDTH_POLL_CONCURRENCY,
                                thread_name_prefix='dir-cap') as _ex:
                        _futs = {_ex.submit(get_peer_status, p.get('url')): p
                                 for p in _peers}
                        _deadline = time.time() + 12
                        for _f, _p in list(_futs.items()):
                            try:
                                _statuses[_p['id']] = _f.result(
                                    timeout=max(0.1, _deadline - time.time())
                                )
                            except (_Fto, Exception):
                                _statuses[_p['id']] = {'state': 'unreachable'}
                    factor = self._capacity_factor(_statuses)
                    # Per-peer warning fingerprint for load-shifting:
                    # let cleaner peers carry more work. Stored in state
                    # so choose_active_peer + orchestrators can use it.
                    _peer_wr: dict[str, dict] = {}
                    for _pid, _ps in (_statuses or {}).items():
                        _peer_wr[_pid] = (_ps or {}).get('warning_rates') or {}
                    # Slow per-peer noise EMA across ALL warning kinds.
                    # Persists across director restarts via director_state.json,
                    # so a peer that misbehaved an hour ago stays penalised
                    # even though its 5/10-min sliding windows have decayed.
                    with self._lock:
                        _long = dict(self.state.get('peer_noise_long_ema') or {})
                        _alpha = PEER_NOISE_LONG_EMA_ALPHA
                        for _pid, _wr in _peer_wr.items():
                            _prev = dict(_long.get(_pid) or {})
                            for _kind in ('bev', 'zenodo', 'copernicus'):
                                _r = float(((_wr.get(_kind) or {}).get('5m')) or 0.0)
                                _p = float(_prev.get(_kind) or 0.0)
                                # Asymmetric: ramp up fast on new pressure,
                                # decay slow when quiet. We take max of the
                                # standard EMA and the current rate so a
                                # spike registers immediately.
                                _new = max(_r, _alpha * _r + (1.0 - _alpha) * _p)
                                _prev[_kind] = round(_new, 4)
                            _long[_pid] = _prev
                        # Drop entries for peers no longer in the fleet.
                        _live = {p['id'] for p in self.cfg.get('peers', [])}
                        for _pid in list(_long.keys()):
                            if _pid not in _live:
                                _long.pop(_pid, None)
                        self.state['peer_noise_long_ema'] = _long
                        self.state['capacity_factor'] = round(factor, 3)
                        self.state['capacity_components'] = (
                            self._capacity_components)
                        self.state['capacity_ema_persisted'] = round(
                            self._capacity_ema, 4)
                        self.state['sub_factor_ema'] = {
                            k: round(float(v), 4)
                            for k, v in self._sub_factor_ema.items()
                        }
                        # Persist the rolling sparkline history so other
                        # gunicorn workers + a fresh director after restart
                        # all see the same chart immediately.
                        self.state['capacity_history'] = [
                            _capacity_history_entry(_e)
                            for _e in list(self._capacity_history)
                        ]
                        self.state['peer_warning_rates'] = _peer_wr
                        # Per-peer CPU-steal EMA. Tick cadence ~30 s,
                        # alpha=0.033 → ~15 min half-life. Only update
                        # when the peer reported a fresh sample this
                        # tick — unreachable peers keep their previous
                        # value (they'll either come back and resume
                        # smoothing, or get dropped below when no
                        # longer in the fleet config).
                        _stl_ema = dict(
                            self.state.get('peer_steal_ema') or {})
                        _stl_a = PEER_STEAL_EMA_ALPHA
                        for _pid, _ps in (_statuses or {}).items():
                            _sys = (_ps or {}).get('system') or {}
                            _perf = _sys.get('perf') or {}
                            _v = _perf.get('cpu_steal_ewma')
                            if _v is None:
                                _v = _sys.get('cpu_steal')
                            if not isinstance(_v, (int, float)):
                                continue
                            _prev_s = _stl_ema.get(_pid)
                            if isinstance(_prev_s, (int, float)):
                                _new_s = (_stl_a * float(_v)
                                          + (1.0 - _stl_a) * float(_prev_s))
                            else:
                                _new_s = float(_v)
                            _stl_ema[_pid] = round(_new_s, 2)
                        for _pid in list(_stl_ema.keys()):
                            if _pid not in _live:
                                _stl_ema.pop(_pid, None)
                        self.state['peer_steal_ema'] = _stl_ema
                except RuntimeError as _re:
                    # ``cannot schedule new futures after interpreter
                    # shutdown`` fires when an *old* gunicorn worker is
                    # mid-teardown during a srv restart but its loop
                    # thread hasn't been joined yet. Benign — the new
                    # worker's loop takes over. Self-stop quietly
                    # instead of dumping a scary traceback every tick.
                    if 'interpreter shutdown' in str(_re):
                        log.info('director loop: interpreter shutting down '
                                 '— stopping capacity poll')
                        self._running = False
                        return
                    log.exception('capacity factor computation failed')
                except _DormantSkipFanout:
                    pass  # dormant: pushed-only snapshot, no fanout
                except Exception:
                    log.exception('capacity factor computation failed')
                # Fleet-dormant gate: when every remote peer is
                # unreachable for FLEET_DORMANT_AFTER_S, hold. Only the
                # primary-park enforcer + a light probe sweep run; all
                # orchestration below is skipped until a peer returns.
                try:
                    _dormant = self._update_fleet_dormancy(
                        locals().get('_statuses') or {})
                except Exception:
                    log.exception('fleet dormancy tracking failed')
                    _dormant = False
                if _dormant:
                    try:
                        self._enforce_primary_park()
                    except Exception:
                        log.exception('primary park enforce failed')
                    try:
                        self._fleet_dormant_probe()
                    except Exception:
                        log.exception('dormant probe sweep failed')
                    with self._lock:
                        save_director_state(self.state)
                    time.sleep(DIRECTOR_POLL_INTERVAL)
                    continue
                # Run-complete gate: whole run finished → hold (no
                # frontier/cache-only/sweep starts, no fanout). Pushed
                # heartbeats are still evaluated; wakes on new work.
                try:
                    _complete = self._update_run_complete(
                        locals().get('_statuses') or {})
                except Exception:
                    log.exception('run-complete tracking failed')
                    _complete = False
                if _complete:
                    try:
                        self._enforce_primary_park()
                    except Exception:
                        log.exception('primary park enforce failed')
                    with self._lock:
                        save_director_state(self.state)
                    time.sleep(DIRECTOR_POLL_INTERVAL)
                    continue
                with self._lock:
                    if _clear_completed_reservations(self.cfg, self.state):
                        save_peers_config(self.cfg)
                # Belt-and-braces: ensure primary stays parked every
                # tick, before any peer scheduling decisions.
                try:
                    self._enforce_primary_park()
                except Exception:
                    log.exception('primary park enforce failed')
                # Hourly sweep: release park-until-renewal records we
                # wrote on a *guessed* nominal budget without canary-
                # shaping evidence. exe.dev's real per-account limits and
                # billing anchors are unknown; canary-by-default means
                # we only park on observed throughput collapse.
                try:
                    self._release_unverified_bw_parks()
                except Exception:
                    log.exception('release_unverified_bw_parks failed')
                # Belt-and-braces: the *active director* must also not
                # carry processing load. Director duty already costs
                # 60-peer fanout bandwidth; layering frontier work on
                # top is what burned the primary on 2026-05.
                try:
                    self._enforce_director_self_park()
                except Exception:
                    log.exception('director self-park enforce failed')
                self._check_and_switch()
                # Canary health: park peers running past the global
                # budget if exe.dev visibly throttles them or they
                # start polluting the warning stream.
                try:
                    self._check_canary_health()
                except Exception:
                    log.exception('Canary health check error')
                # Rolling-steal auto-park: peers stuck on a contended
                # hypervisor host get a 30 min cooldown so we stop
                # burning Copernicus credits / wall-time on them.
                try:
                    self._check_steal_health()
                except Exception:
                    log.exception('Steal health check error')
                # Stuck-KG watchdog: hard-restart peers wedged inside a
                # KG (progress fingerprint frozen for STUCK_KG_FROZEN_S).
                # Uses the same fresh _statuses snapshot computed above.
                try:
                    self._check_stuck_kg(
                        locals().get('_statuses') or {})
                except Exception:
                    log.exception('Stuck-KG watchdog error')
                # Duplicate-KG reconciler: stop laggards when two+ peers
                # ended up on the same KG outside the dispatch-claim
                # window. Reuses the SAME `_statuses` snapshot — no extra
                # fanout — and caps stops/tick (anti-herd).
                try:
                    self._check_duplicate_kgs(
                        locals().get('_statuses') or {})
                except Exception:
                    log.exception('Duplicate-KG reconciler error')
                # BEV-outage detection: fleet-wide escalating pause
                # when BEV emits sustained errors across many peers.
                # Runs after capacity_factor was computed above (uses
                # the same `_statuses` snapshot).
                try:
                    self._check_bev_outage(
                        locals().get('_statuses') or {})
                except Exception:
                    log.exception('BEV outage check error')
                # Zenodo-degraded circuit (fleet write failures) + hourly
                # cache_manifest ↔ deposit reconcile (background thread).
                try:
                    self._check_zenodo_degraded()
                except Exception:
                    log.exception('Zenodo circuit check error')
                try:
                    self._check_v2_regen()
                except Exception:
                    log.exception('v2_regen check error')
                try:
                    self._check_v2_orphans()
                except Exception:
                    log.exception('v2_orphan sweep error')
                try:
                    self._check_empty_drafts()
                except Exception:
                    log.exception('empty-draft sweep error')
                try:
                    self._check_json_heal()
                except Exception:
                    log.exception('json_heal sweep error')
                try:
                    self._v24_audit_tick()
                except Exception:
                    log.exception('v2.4 audit tick error')
                try:
                    self._reconcile_cache_manifest_if_due()
                except Exception:
                    log.exception('Cache reconcile scheduling error')
                # Canary-probe parked /24 pools so a 20-min route flap
                # doesn't waste the full 1h/4h/12h/24h cooldown. One
                # peer per pool gets a single HEAD-range probe to
                # _BEV_TEST_URL every ~5 min; on 206+TIFF magic we
                # clear not_before on every peer in that /24 early.
                try:
                    self._canary_probe_parked_pools(
                        locals().get('_statuses') or {})
                except Exception:
                    log.exception('BEV pool canary probe error')
                # IP-pool block detection: park peers in an egress /24
                # that shows a disproportionate BEV warn rate.
                try:
                    self._check_ip_pool_block(
                        locals().get('_statuses') or {})
                except Exception:
                    log.exception('IP-pool block check error')
                # Per-peer bandwidth-wall enforcement (low-water /
                # hard-depleted park-until-renewal) for non-active
                # peers. _check_and_switch handles the active one.
                try:
                    self._enforce_peer_bandwidth_walls()
                except Exception:
                    log.exception('Peer bandwidth wall enforce error')
                # Refresh the fleet-aggregated credential HEALTH scores
                # before any frontier assignment so creds are ranked by
                # real per-peer usage (incl. the ``stalled`` clamp), not
                # the primary's flat local 1.0 pool. Must run on the loop
                # worker since the assigner reads state['credentials'].
                try:
                    self._refresh_credential_health_if_due()
                except Exception:
                    log.exception('Credential-health refresh error')
                # Parallel-frontier orchestration: start additional
                # frontier peers when credential capacity permits.
                try:
                    self._orchestrate_parallel_frontiers()
                except Exception:
                    log.exception('Parallel-frontier orchestration error')
                # Cache-only orchestration runs alongside the frontier
                # peer.  It only ever starts/stops peers that are NOT the
                # active frontier and that have no reservation.
                try:
                    self._orchestrate_cache_only()
                except Exception:
                    log.exception('Cache-only orchestration error')
                # Fix #2: publish the frontier cell pre-warm directive
                # (rides the cache_manifest sync to peers). Demand-side
                # trigger = cache-ready starvation; supply-side gate =
                # Copernicus health. Reads the count just refreshed by
                # _orchestrate_cache_only's _compute_cache_ready_kgs.
                try:
                    _cr = (self.state.get('_cache_ready_cache') or {})
                    self._update_prewarm_flag(len(_cr.get('codes') or []))
                except Exception:
                    log.exception('Pre-warm flag update error')
                # Record fleet-shape sample for the Progress card
                # sparkline. Counts as observed *after* orchestration
                # this tick — gives the truest 30s-resolution view of
                # what's actually running.
                try:
                    _statuses_for_hist = locals().get('_statuses') or {}
                    _fr = len(self.state.get(
                        'parallel_frontiers_active') or [])
                    # Match status()'s definition exactly: cache_only_run
                    # (peer self-report) AND running/processing. Using
                    # ps['cache_only'] alone misses peers whose status
                    # poll didn't include the flag this tick.
                    _co = 0
                    for _pid, _ps in _statuses_for_hist.items():
                        if not isinstance(_ps, dict):
                            continue
                        if (_ps.get('cache_only') and
                                _ps.get('state') in (
                                    'running', 'processing')):
                            _co += 1
                    _cr_cache = self.state.get(
                        '_cache_ready_cache') or {}
                    _cr = len(_cr_cache.get('codes') or [])
                    self._peer_history.append((
                        int(time.time()), int(_fr), int(_co), int(_cr),
                    ))
                    self.state['peer_history'] = [
                        {'t': t, 'fr': fr, 'co': co, 'cr': cr}
                        for (t, fr, co, cr) in list(self._peer_history)
                    ]
                except Exception:
                    log.exception('peer-history sample failed')
                # Sync queue every 5 iterations (~2.5 min at 30s interval)
                sync_counter += 1
                if sync_counter >= 5:
                    try:
                        self._repair_split_drift_wedges()
                    except Exception:
                        log.exception('split-drift wedge repair failed')
                    try:
                        self._prune_priority_queue()
                    except Exception:
                        log.exception('priority queue prune failed')
                    # GC per-peer state rows for ids no longer in the
                    # roster (removed by another gunicorn worker, or by
                    # an older director that lacked the GC-on-remove
                    # path). Keeps director_state.json bounded.
                    try:
                        with self._lock:
                            live = {p['id'] for p in
                                    (self.cfg.get('peers') or [])
                                    if p.get('id')}
                            dead = set()
                            for _k in _PEER_KEYED_STATE_MAPS:
                                _m = self.state.get(_k)
                                if isinstance(_m, dict):
                                    dead |= (set(_m) - live)
                            n = _gc_peer_state(self.state, dead)
                        if n:
                            log.info('director_state GC: dropped %d row(s) '
                                     'for %d removed peer(s)', n, len(dead))
                    except Exception:
                        log.exception('director_state peer GC failed')
                    self._sync_queue_to_active()
                    sync_counter = 0
                with self._lock:
                    save_director_state(self.state)
            except Exception:
                log.exception('Director loop error')
            time.sleep(DIRECTOR_POLL_INTERVAL)


# Singleton
_director: PeerDirector | None = None


def get_director() -> PeerDirector:
    global _director
    if _director is None:
        _director = PeerDirector()
    return _director
