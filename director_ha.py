"""Director high-availability — shadow election, watchdog & handover.

Design
------
One peer is the *director* at any time, marked by
``data/austria_processor/is_director``.
The director nominates a *shadow* each tick (most-reliable enabled peer)
and pushes its state snapshot to it every ``SHADOW_SYNC_INTERVAL`` seconds.
The shadow stages the snapshot under ``data/austria_processor/shadow/`` and
holds it warm.

Every peer runs a watchdog thread that pings the current director's
``/api/v1/director/heartbeat`` every ``WATCHDOG_INTERVAL`` seconds. If the
shadow misses ``WATCHDOG_MISS_THRESHOLD`` consecutive heartbeats it promotes
itself: installs the staged snapshot, writes ``is_director``, restarts the
director loop in-process, and broadcasts ``/api/v1/director/announce`` to
all peers so they flip ``zenodo_lock_url.txt`` and their watchdog target.
The old director, if it ever returns, sees ``stepped_down`` (written by the
announce handler) and refuses to start its director loop — it lives on as a
plain peer until manually re-promoted.

Manual handover is the same dance, initiated by
``POST /api/v1/director/handover?to=<peer_id>`` on the current director.
"""
from __future__ import annotations

import json
import logging
import os
import re
import socket
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import requests

log = logging.getLogger(__name__)

DATA_DIR = Path('data/austria_processor')
IS_DIRECTOR_FLAG = DATA_DIR / 'is_director'
STEPPED_DOWN_FLAG = DATA_DIR / 'stepped_down'
SELF_FILE = DATA_DIR / 'self.json'
ZENODO_LOCK_URL_FILE = DATA_DIR / 'zenodo_lock_url.txt'
SHADOW_DIR = DATA_DIR / 'shadow'
SHADOW_META_FILE = SHADOW_DIR / 'meta.json'
ADMIN_TOKEN_PATH = Path('data/admin_token')

# ── HA kill-switch (2026-10-23 security hardening) ───────────────
# Automatic failover means any VM that can present a cluster token can
# claim the director role (announce / takeover / step_down / identity
# director_url) and redirect the whole fleet — including every peer's
# token-bearing traffic — to itself.  HA is therefore OFF by default:
# the primary is the only director, peers refuse to re-point, the
# watchdog never promotes.  Re-enable deliberately by creating
# ``data/ha_enabled`` on the primary (owner decision, never automatic).
HA_ENABLE_FLAG = Path('data/ha_enabled')
# Only this host may ever be ``director_url`` while HA is disabled.
PRIMARY_HOST = 'example.invalid'


def ha_enabled() -> bool:
    try:
        return HA_ENABLE_FLAG.exists()
    except Exception:
        return False


def director_url_allowed(url: str | None) -> bool:
    """While HA is off, peers only accept the pinned primary as director."""
    if ha_enabled() or not url:
        return True
    try:
        from urllib.parse import urlparse
        host = (urlparse(url).hostname or '').lower().rstrip('.')
        return host == PRIMARY_HOST
    except Exception:
        return False

# Files that make up a director snapshot. All small JSON; total < 200 KB.
# (cache_manifest.json + manifest_tombstones.json already sync via the
# cluster sync thread; we include them in the snapshot so a fresh director
# has them immediately rather than waiting for the next sync tick.)
SNAPSHOT_FILES: tuple[str, ...] = (
    'director_state.json',
    'kg_strikes.json',
    'failure_counts.json',
    'cache_miss_kgs.json',
    'deferred_kgs.json',
    'retry_queue.json',
    'failed_kgs.json',
    'partial_kgs.json',
    'v2_strikes_fleet.json',
    'manifest_tombstones.json',
    'manifest_tombstone_drops.json',
    'copernicus_credentials.json',
    'copernicus_credential_usage.json',
    'peers.json',
    # operator knobs incl. vct_bev_peers — a shadow without it flipped the
    # whole fleet to ne_source=umfeld on takeover (2026-10-15 14:21, 46
    # peers; umfeld's cadastre is 410 → cadastre-less KGs)
    'director_config.json',
    'cache_manifest.json',
    'checkpoint_registry.json',
)
# Plain-text peer URL list — handled separately because it isn't JSON.
SNAPSHOT_TEXT_FILES: tuple[str, ...] = ('peer_urls.txt',)

WATCHDOG_INTERVAL = 60           # seconds between heartbeat probes
# 12 misses × 30s = 6 minutes before a shadow promotes itself. Bumped
# from 6 (3 min) on 2026-05-10 after two cascading takeovers driven by
# exe.dev cross-region proxy flaps (LAX→FRA): primary remained healthy
# locally throughout, but its public proxy dropped HTTPS for ~3 min,
# tripping a shadow's watchdog. Each takeover cost minutes of
# orchestration outage and exposed cross-worker step_down bugs. Real
# director failures (process crash, host down, kernel panic) still
# fail over within 6 min — acceptable.
# Sep 2026 bandwidth pass: probe every 60 s (was 30) with half the miss
# threshold — same 6-min failover, half the heartbeat connections
# (30 peers × TLS handshake each = the dominant cost of a ~200 B ping).
WATCHDOG_MISS_THRESHOLD = 6      # consecutive misses → shadow takes over
WATCHDOG_TIMEOUT = (3, 5)        # (connect, read)
# Snapshot cadence (Sep 2026 bandwidth pass): a *light* snapshot every
# SHADOW_SYNC_INTERVAL (director_state minus the multi-MB telemetry
# rings, only files whose content changed, gzipped) and a *full*
# snapshot every SHADOW_FULL_INTERVAL / on shadow change. Failover only
# fires after WATCHDOG_MISS_THRESHOLD × WATCHDOG_INTERVAL = 6 min, so a
# 2-min-old snapshot is as good as a 30-s-old one for the takeover.
SHADOW_SYNC_INTERVAL = 120       # seconds between (light) snapshot pushes
SHADOW_FULL_INTERVAL = 1800      # seconds between full snapshot pushes
# director_state.json keys that are pure telemetry history (charts /
# forensics); omitted from light snapshots, carried by the full ones.
SNAPSHOT_HEAVY_STATE_KEYS: tuple[str, ...] = (
    'ip_pools_history', 'canary_history', 'capacity_history',
    'fleet_proxy_history', 'peer_history', 'shadow_log_archive_last',
)
HEARTBEAT_GRACE = 90             # seconds before director is considered dead

# ── Failover second-signal gates (2026-09-30 incident) ──────────
#
# "Director unreachable *from the shadow*" is not "director dead". On
# 2026-09-30 the primary was alive and orchestrating all morning while
# its inbound exe.dev proxy path flapped; the shadow promoted itself 7×
# and the primary handed back each time. Before promoting, the shadow
# now requires corroboration:
#   (a) alive-signal — the director's snapshot pushes to us *stopped*
#       (a push that arrived after our first miss proves it is alive);
#   (b) quorum — ≥ QUORUM_MIN_FAIL other peers also fail to reach the
#       director's heartbeat (probed via their /api/v1/director/probe),
#       and fewer than QUORUM_MIN_OK can reach it.
# If we are isolated (no peer answers our probe request) we hold: a
# shadow that cannot talk to anyone cannot usefully direct anyone.
QUORUM_PROBE_PEERS = 6           # peers asked per tick (parallel)
QUORUM_MIN_FAIL = 2              # ≥ N others must also fail the director
QUORUM_MIN_OK = 2                # ≥ N others reaching it → hold
QUORUM_TIMEOUT = (3, 12)         # per probe request (peer does a (3,5) GET)
# Flap dampener: N watchdog takeovers fleet-wide inside FLAP_WINDOW_S →
# (shadow) miss threshold ×FLAP_MISS_MULT before another promotion, and
# (interim director) hold the role FLAP_LOCKOUT_S after its takeover
# before auto-handing back to primary. Events live in a local file on
# every node (announces reach every peer, so the view is fleet-wide).
FLAP_FILE = DATA_DIR / 'ha_flaps.json'
FLAP_WINDOW_S = 6 * 3600
FLAP_MAX_TAKEOVERS = 3
FLAP_MISS_MULT = 3
FLAP_LOCKOUT_S = 2 * 3600
# Live merged-log ring replication director → shadow (delta, gzipped).
COMBINED_LOG_PUSH_INTERVAL = 300


# ── Peer-URL canonicalisation ───────────────────────────────────
#
# Every peer's URL is fully determined by its id. ``primary`` lives at
# ``example.invalid``; ``atN`` lives at ``srtm-lidar-atN.exe.xyz``.
# Anything else in peers.json is corruption (cascading-handover bug, manual
# edit, propagated stale snapshot, ...) and must be repaired before it can
# spread.  See the split-brain incident note in AGENTS.md.

def canonical_peer_url(peer_id: str | None) -> str | None:
    """Return the canonical URL for ``peer_id`` or None if unknown.

    ``primary`` → https://example.invalid/k   (token-gated tier, see api_tiers.py)
    ``atNNN``  → https://srtm-lidar-atNNN.exe.xyz:8000
    Any other id (custom deploy, hand-rolled peer): unknown → None,
    leave the existing URL alone.
    """
    if not peer_id:
        return None
    if peer_id == 'primary':
        # Since Oct 2026 the primary's :8000 app is the hidden tier behind
        # the public app: reach it via the /k prefix (api_tiers.py). Works
        # before and after the exe.dev default-port switch.
        return 'https://example.invalid/k'
    if re.match(r'^at\d+$', peer_id):
        return f'https://srtm-lidar-{peer_id}.exe.xyz:8000'
    return None


def sanitise_peers_json(*, allow_local_url_none: bool = True,
                       me: str | None = None) -> dict:
    """Enforce canonical URLs in ``peers.json``. Returns a report dict.

    * Every peer with a recognised id gets its URL reset to the canonical
      form derived from the id, EXCEPT the local entry which may carry
      ``url=None`` (the canonical local marker).
    * Peers with unrecognised ids are left untouched.
    * Duplicate entries (same id appearing twice) are collapsed to one.
    * Idempotent and safe to call repeatedly.
    """
    info: dict = {'changes': [], 'errors': []}
    pj = DATA_DIR / 'peers.json'
    try:
        cfg = json.loads(pj.read_text())
    except Exception as e:
        info['errors'].append(f'read: {e}')
        return info
    if not isinstance(cfg, dict) or not isinstance(cfg.get('peers'), list):
        info['errors'].append('peers.json structure invalid')
        return info
    if me is None:
        me = self_id()
    seen: dict = {}
    deduped: list = []
    for p in cfg['peers']:
        if not isinstance(p, dict):
            continue
        pid = p.get('id')
        if not pid:
            continue
        if pid in seen:
            # Merge: prefer the entry with non-None URL or more recent fields.
            prev = seen[pid]
            if prev.get('url') is None and p.get('url') is not None:
                idx = deduped.index(prev)
                deduped[idx] = p
                seen[pid] = p
            info['changes'].append(f'dedup duplicate entry for {pid}')
            continue
        seen[pid] = p
        deduped.append(p)
    cfg['peers'] = deduped
    for p in deduped:
        pid = p.get('id')
        url = p.get('url')
        canon = canonical_peer_url(pid)
        if canon is None:
            continue  # unrecognised id, leave alone
        if pid == me and allow_local_url_none:
            if url not in (None, canon):
                info['changes'].append(
                    f'{pid}: url={url!r} ≠ None (local) and ≠ canon → None')
                p['url'] = None
            # otherwise keep whatever it is (None or canon both fine)
            continue
        if url != canon:
            info['changes'].append(f'{pid}: {url!r} → {canon}')
            p['url'] = canon
    if info['changes']:
        try:
            _atomic_write(pj, json.dumps(cfg, indent=2))
        except Exception as e:
            info['errors'].append(f'write: {e}')
    return info


# ── Self identity ────────────────────────────────────────────────

def _derive_self_id_from_hostname() -> str:
    """Derive a peer id from hostname (e.g. ``srtm-lidar-at3`` → ``at3``,
    ``srtm-lidar-at`` → ``primary``).
    """
    h = socket.gethostname().split('.')[0]
    if h.startswith('srtm-lidar-'):
        suffix = h[len('srtm-lidar-'):]
        return 'primary' if suffix == 'at' else suffix
    return h


def load_self() -> dict:
    """Return ``{id, url, director_url}`` for this VM.

    Falls back to derived defaults when the file is missing so a fresh
    install on the primary still works without a self.json.
    """
    try:
        d = json.loads(SELF_FILE.read_text())
        if isinstance(d, dict) and d.get('id'):
            # Normalise a stale primary URL: since the exe.dev default
            # port moved to 8100 (2026-10-03), ``example.invalid:8000``
            # is owner-only and NOT reachable from peers — the edge probe
            # tripped EDGE-INBOUND-UNREACHABLE on it. Canonical is ``/k``.
            canon = canonical_peer_url(d.get('id'))
            if canon and d.get('url') != canon and d.get('id') == 'primary':
                d['url'] = canon
                try:
                    save_self(d)
                except Exception:
                    pass
            return d
    except Exception:
        pass
    # Best-effort defaults.
    sid = _derive_self_id_from_hostname()
    h = socket.gethostname().split('.')[0]
    derived_url = f'https://{h}.exe.xyz:8000' if h.startswith('srtm-lidar-') else None
    out = {'id': sid, 'url': derived_url, 'director_url': None}
    try:
        if ZENODO_LOCK_URL_FILE.exists():
            out['director_url'] = ZENODO_LOCK_URL_FILE.read_text().strip() or None
    except Exception:
        pass
    # Persist so later loads are stable.
    try:
        save_self(out)
    except Exception:
        pass
    return out


def save_self(d: dict) -> None:
    SELF_FILE.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=SELF_FILE.parent, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(d, f, indent=2)
        os.replace(tmp, SELF_FILE)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def self_id() -> str:
    return load_self().get('id') or socket.gethostname()


def self_url() -> str | None:
    return load_self().get('url')


def director_url() -> str | None:
    """URL of the current cluster director, as known by *this* VM.

    Returns None when this VM **is** the director.
    """
    if IS_DIRECTOR_FLAG.exists():
        return None
    s = load_self()
    if s.get('director_url'):
        return s['director_url']
    try:
        if ZENODO_LOCK_URL_FILE.exists():
            return ZENODO_LOCK_URL_FILE.read_text().strip() or None
    except Exception:
        pass
    return None


def set_director_url(url: str | None) -> None:
    """Update both ``self.json`` and ``zenodo_lock_url.txt``.

    Setting ``url=None`` removes both files (this VM is the director).
    """
    if not director_url_allowed(url):
        log.error('set_director_url: refusing %r — HA disabled, director '
                  'pinned to %s', url, PRIMARY_HOST)
        return
    s = load_self()
    s['director_url'] = url
    save_self(s)
    if url:
        ZENODO_LOCK_URL_FILE.parent.mkdir(parents=True, exist_ok=True)
        ZENODO_LOCK_URL_FILE.write_text(url + '\n')
    else:
        try:
            ZENODO_LOCK_URL_FILE.unlink()
        except FileNotFoundError:
            pass


# ── Snapshot helpers ─────────────────────────────────────────────

def _admin_headers() -> dict:
    try:
        tok = ADMIN_TOKEN_PATH.read_text().strip()
        if tok:
            return {'X-Admin-Token': tok}
    except Exception:
        pass
    return {}


def build_snapshot(light: bool = False,
                   unchanged: set[str] | None = None) -> dict:
    """Read every snapshot file from disk and return a JSON-safe dict.

    ``light=True`` strips SNAPSHOT_HEAVY_STATE_KEYS from director_state
    and marks the snapshot partial; ``unchanged`` names files the shadow
    already holds byte-identically (omitted from the payload). The
    receiving ``stage_snapshot`` keeps its staged copy for omitted /
    stripped parts. Only send partial snapshots to a shadow running
    this code (director shadow election already requires commit match).
    """
    # Sanitise peers.json BEFORE reading it into the snapshot. Otherwise
    # any corruption (cascading-handover URL drift, manual edit) would
    # propagate to the shadow on the next push.
    try:
        sa = sanitise_peers_json()
        if sa.get('changes'):
            log.warning('build_snapshot: sanitised peers.json: %s',
                        sa['changes'])
    except Exception as e:
        log.warning('build_snapshot: peers.json sanitise failed: %s', e)
    snap: dict[str, dict | str | None] = {}
    omitted: list[str] = []
    stripped: list[str] = []
    for name in SNAPSHOT_FILES:
        if unchanged and name in unchanged:
            omitted.append(name)
            continue
        p = DATA_DIR / name
        if not p.exists():
            snap[name] = None
            continue
        try:
            v = json.loads(p.read_text())
            if light and name == 'director_state.json' and isinstance(v, dict):
                v = {k: x for k, x in v.items()
                     if k not in SNAPSHOT_HEAVY_STATE_KEYS}
                stripped = [k for k in SNAPSHOT_HEAVY_STATE_KEYS]
            snap[name] = v
        except Exception as e:
            log.warning('snapshot read %s failed: %s', name, e)
            snap[name] = None
    text: dict[str, str | None] = {}
    for name in SNAPSHOT_TEXT_FILES:
        p = DATA_DIR / name
        try:
            text[name] = p.read_text() if p.exists() else None
        except Exception:
            text[name] = None
    snap['_text'] = text
    snap['_meta'] = {
        'origin': self_id(),
        'origin_url': self_url(),
        'ts': datetime.now(timezone.utc).isoformat(),
        'ts_epoch': time.time(),
    }
    if omitted or stripped:
        snap['_meta']['partial'] = True
        snap['_meta']['omitted'] = omitted
        snap['_meta']['stripped_state_keys'] = stripped
    return snap


def snapshot_file_hashes() -> dict[str, str]:
    """md5 of every snapshot file's on-disk bytes (cheap: all small
    except director_state, ~4 MB)."""
    import hashlib as _hl
    out: dict[str, str] = {}
    for name in list(SNAPSHOT_FILES) + list(SNAPSHOT_TEXT_FILES):
        p = DATA_DIR / name
        try:
            out[name] = _hl.md5(p.read_bytes()).hexdigest() if p.exists() else 'absent'
        except Exception:
            out[name] = 'err'
    return out


def stage_snapshot(snap: dict) -> None:
    """Write a received snapshot into ``shadow/`` (warm standby)."""
    SHADOW_DIR.mkdir(parents=True, exist_ok=True)
    meta_in = snap.get('_meta') or {}
    partial = bool(meta_in.get('partial'))
    omitted = set(meta_in.get('omitted') or ())
    stripped = tuple(meta_in.get('stripped_state_keys') or ())
    for name in SNAPSHOT_FILES:
        out = SHADOW_DIR / name
        if partial and (name in omitted or name not in snap):
            continue   # shadow keeps its staged copy
        v = snap.get(name)
        if (partial and stripped and name == 'director_state.json'
                and isinstance(v, dict)):
            # Re-attach the telemetry rings from the previous full push.
            try:
                prev = json.loads(out.read_text()) if out.exists() else {}
                for k in stripped:
                    if k in prev and k not in v:
                        v[k] = prev[k]
            except Exception:
                pass
        if v is None:
            try:
                out.unlink()
            except FileNotFoundError:
                pass
            continue
        _atomic_write(out, json.dumps(v, indent=2))
    for name, v in (snap.get('_text') or {}).items():
        out = SHADOW_DIR / name
        if partial and name in omitted:
            continue
        if v is None:
            try:
                out.unlink()
            except FileNotFoundError:
                pass
            continue
        _atomic_write(out, v)
    meta = dict(snap.get('_meta') or {})
    meta['received_ts'] = time.time()
    _atomic_write(SHADOW_META_FILE, json.dumps(meta, indent=2))


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(content)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def install_snapshot_from_shadow() -> dict:
    """Promote staged snapshot files into ``data/austria_processor/``.

    Called during takeover. Each file is moved atomically.
    Files missing from shadow are left as-is (e.g. retry_queue.json that
    we already have locally is preserved).
    Returns ``{installed: [...], skipped: [...], meta: ...}``.
    """
    if not SHADOW_DIR.exists():
        return {'installed': [], 'skipped': list(SNAPSHOT_FILES),
                'meta': None, 'error': 'no shadow staged'}
    installed: list[str] = []
    skipped: list[str] = []
    for name in list(SNAPSHOT_FILES) + list(SNAPSHOT_TEXT_FILES):
        src = SHADOW_DIR / name
        dst = DATA_DIR / name
        if not src.exists():
            skipped.append(name)
            continue
        try:
            content = src.read_bytes()
            # retry_queue.json: merge (union, dedup, preserve order)
            # rather than replace, so KGs added on the demoted director
            # in the gap between last snapshot push and takeover aren't
            # lost. Snapshot order wins (it represents the authoritative
            # priority); local-only codes append at the tail.
            # copernicus_credentials.json: merge by client_id rather than
            # replace. User-added credentials registered on the *new*
            # director after a takeover (or on either director between
            # snapshot pushes) would otherwise vanish when the role swings
            # back: the local file gets clobbered by the older snapshot.
            # Builtins are always kept fresh from the snapshot's view; user
            # entries are unioned by client_id, with the snapshot side
            # winning on conflict (the new director just probed them).
            if name == 'copernicus_credentials.json' and dst.exists():
                try:
                    snap_d = json.loads(content) or {}
                    local_d = json.loads(dst.read_bytes()) or {}
                    snap_creds = snap_d.get('credentials', []) if isinstance(snap_d, dict) else []
                    local_creds = local_d.get('credentials', []) if isinstance(local_d, dict) else []
                    # Tombstones: union of both sides. A delete on
                    # either side must survive an HA fan-out, otherwise
                    # a stale director's snapshot would resurrect the
                    # cred (the deletion-resurrection bug fixed in
                    # copernicus.py).
                    snap_tombs = snap_d.get('tombstones') or [] if isinstance(snap_d, dict) else []
                    local_tombs = local_d.get('tombstones') or [] if isinstance(local_d, dict) else []
                    tombs = sorted({(t or '').strip() for t in list(snap_tombs) + list(local_tombs) if (t or '').strip()})
                    tombs_set = set(tombs)
                    if isinstance(snap_creds, list) and isinstance(local_creds, list):
                        by_id: dict = {}
                        order: list = []
                        # snapshot wins on conflict — but we still want
                        # local-only entries to survive. Tombstoned ids
                        # are dropped (deletion-survives-snapshot).
                        for c in list(snap_creds) + list(local_creds):
                            if not isinstance(c, dict):
                                continue
                            cid = (c.get('client_id') or '').strip()
                            if not cid or cid in tombs_set:
                                continue
                            if cid not in by_id:
                                by_id[cid] = c
                                order.append(cid)
                        merged_creds = [by_id[cid] for cid in order]
                        snap_d['tombstones'] = tombs
                        added_back = [
                            cid for cid in by_id
                            if cid not in {(c.get('client_id') or '').strip()
                                            for c in snap_creds if isinstance(c, dict)}
                        ]
                        if added_back:
                            log.info('install_snapshot copernicus_credentials: '
                                     'preserved %d local-only cred(s): %s',
                                     len(added_back),
                                     [c[:18] + '…' for c in added_back])
                        snap_d['credentials'] = merged_creds
                        content = json.dumps(snap_d, indent=2).encode()
                except Exception as e:
                    log.warning('install_snapshot copernicus_credentials merge '
                                'failed (%s); using snapshot as-is', e)
            if name == 'retry_queue.json' and dst.exists():
                try:
                    snap_q = json.loads(content) or []
                    local_q = json.loads(dst.read_bytes()) or []
                    if isinstance(snap_q, list) and isinstance(local_q, list):
                        seen = set()
                        merged: list = []
                        for code in list(snap_q) + list(local_q):
                            if code in seen:
                                continue
                            seen.add(code)
                            merged.append(code)
                        if merged != snap_q:
                            log.info('install_snapshot retry_queue: merged '
                                     '%d snapshot + %d local → %d',
                                     len(snap_q), len(local_q), len(merged))
                        content = json.dumps(merged).encode()
                except Exception as e:
                    log.warning('install_snapshot retry_queue merge failed '
                                '(%s); using snapshot as-is', e)
            _atomic_write_bytes(dst, content)
            installed.append(name)
        except Exception as e:
            log.warning('install_snapshot %s failed: %s', name, e)
            skipped.append(name)
    meta = None
    try:
        meta = json.loads(SHADOW_META_FILE.read_text())
    except Exception:
        pass
    return {'installed': installed, 'skipped': skipped, 'meta': meta}


def install_snapshot_inline(snap: dict) -> dict:
    """Install a snapshot dict directly (used by manual handover)."""
    stage_snapshot(snap)
    return install_snapshot_from_shadow()


def _normalise_local_identity_in_peers(prev_director_url: str | None) -> dict:
    """Fix up ``peers.json`` and ``peer_urls.txt`` after a snapshot install.

    Snapshots use ``url=None`` to mark the *origin's* local entry. We
    must rewire only that single entry — NEVER touch other peers'
    URLs (cascading handovers used to clobber every ``url=None`` entry
    with stale ``prev_director_url`` values, which is how the cluster
    ended up with multiple peers all pointing at the same URL).

    Rules:
    * Promote our own entry to ``url=None`` (canonical local marker).
    * If the *previous director's* entry is ``url=None`` after install
      (because the snapshot came from them), set its url to
      ``prev_director_url``. Identify that entry by ``id == origin`` in
      ``shadow/meta.json``, NOT by “any other url=None”.
    * Leave every other peer entry alone.
    """
    me = self_id()
    my_url = (self_url() or '').rstrip('/')
    prev_url = (prev_director_url or '').rstrip('/') or None
    # Read the snapshot meta to learn the previous director's id.
    prev_id = None
    try:
        meta = json.loads(SHADOW_META_FILE.read_text())
        prev_id = meta.get('origin')
    except Exception:
        pass
    info: dict = {'changes': [], 'prev_id': prev_id, 'prev_url': prev_url}
    pj = DATA_DIR / 'peers.json'
    try:
        cfg = json.loads(pj.read_text())
    except Exception as e:
        info['peers_json_error'] = str(e)[:200]
        cfg = None
    if isinstance(cfg, dict) and isinstance(cfg.get('peers'), list):
        peers = cfg['peers']
        # 0. Hard-canonicalise every recognised peer's URL. This is the
        #    last line of defence against URL drift propagated through
        #    snapshots from a previously-corrupted director.
        for p in peers:
            pid = p.get('id')
            if pid == me:
                continue
            canon = canonical_peer_url(pid)
            if canon and p.get('url') != canon:
                info['changes'].append(
                    f'canonicalise {pid}: {p.get("url")!r} → {canon}')
                p['url'] = canon
        # 1. Promote our own entry to url=None.
        promoted = False
        for p in peers:
            if p.get('id') == me:
                if p.get('url') is not None:
                    info['changes'].append(
                        f'set {me} url to None (local marker)')
                p['url'] = None
                p['enabled'] = True
                promoted = True
                break
        if not promoted:
            peers.insert(0, {'id': me, 'url': None, 'enabled': True})
            info['changes'].append(f'added missing local entry {me}')
        # 2. Heal the previous director's entry IFF identified by
        #    snapshot origin and currently url=None and not us.
        if prev_id and prev_id != me and prev_url:
            for p in peers:
                if p.get('id') == prev_id and p.get('url') is None:
                    p['url'] = prev_url
                    info['changes'].append(
                        f'set {prev_id} url to prev_director {prev_url}')
                    break
        try:
            _atomic_write(pj, json.dumps(cfg, indent=2))
        except Exception as e:
            info['peers_json_write_error'] = str(e)[:200]
    # peer_urls.txt: drop our URL, ensure prev director URL is present.
    pu = DATA_DIR / 'peer_urls.txt'
    try:
        if pu.exists():
            urls = [u.strip().rstrip('/') for u in pu.read_text().splitlines()
                    if u.strip()]
        else:
            urls = []
        out: list[str] = []
        for u in urls:
            if my_url and u == my_url:
                continue
            if u not in out:
                out.append(u)
        if prev_url and prev_url not in out:
            out.append(prev_url)
            info['changes'].append(f'added prev_director {prev_url} to peer_urls')
        if out != urls:
            _atomic_write(pu, '\n'.join(out) + ('\n' if out else ''))
    except Exception as e:
        info['peer_urls_error'] = str(e)[:200]
    return info


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix='.tmp')
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(data)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ── Director ↔ shadow plumbing ───────────────────────────────────

# ── Log archive shadow sync ────────────────────────────────────
#
# The persistent merged log lives in two places:
#   data/combined_log_24h.jsonl       — live ring (rebuilt from peers
#                                       on takeover via the existing
#                                       _combined_log_bootstrap_once)
#   data/log_archive/YYYY-MM-DD.jsonl.gz — long-term forensic record
#
# Only the archive needs replication: the live ring is regenerated
# automatically from peers' own ``recent_log`` ring buffers after
# takeover. We piggyback on the shadow loop but with our own throttle
# so as not to inflate director→shadow traffic. In steady state we push
# only TODAY's file once an hour (older days are immutable on disk and
# already replicated). A full sweep runs once when the shadow changes.

LOG_ARCHIVE_DIR = Path('data/log_archive')
LOG_ARCHIVE_PUSH_INTERVAL = 3600     # seconds between today-only pushes
LOG_ARCHIVE_MAX_BLOB = 32 * 1024 * 1024  # hard per-day blob cap


def _list_archive_days() -> list[Path]:
    if not LOG_ARCHIVE_DIR.exists():
        return []
    try:
        return sorted([p for p in LOG_ARCHIVE_DIR.iterdir()
                       if p.name.endswith('.jsonl.gz')])
    except Exception:
        return []


def _push_one_archive_day(shadow_url: str, path: Path,
                          known_sha: dict[str, str]) -> dict:
    """Push a single day's archive file. Returns response dict.

    Skips the network round-trip entirely if we already have an SHA for
    this file (same path + size cached) — typical for older days that
    don't change. Today's file is checked size-first to dodge re-hashing
    a large gzip on every tick.
    """
    import base64 as _b64, hashlib as _hl, requests as _rq
    try:
        size = path.stat().st_size
    except Exception as e:
        return {'error': f'stat: {e}'}
    if size == 0 or size > LOG_ARCHIVE_MAX_BLOB:
        return {'skipped': True, 'reason': f'size={size}'}
    cache_key = f'{path.name}:{size}'
    blob = path.read_bytes()
    sha = _hl.sha256(blob).hexdigest()
    if known_sha.get(cache_key) == sha:
        return {'status': 'cached', 'size': size}
    day = path.name[:-len('.jsonl.gz')]
    headers = {}
    try:
        tok = ADMIN_TOKEN_PATH.read_text().strip()
        if tok:
            headers['X-Admin-Token'] = tok
    except Exception:
        pass
    # Append delta (Oct 2026 bandwidth pass): archive files are only ever
    # gzip-*appended* (multi-member, app._archive_lines), so when the
    # shadow already holds a byte-prefix of this file we ship just the
    # tail. ``known_sha['<name>:last_size']`` remembers the size the
    # shadow last confirmed; the prefix sha is verified locally against
    # the cached entry so a rewritten file falls through to a full push.
    # The shadow re-checks (size + base sha) and answers 409 on any
    # mismatch → full push. Previously the growing day (~1-1.5 MB, b64)
    # was re-sent whole every hour (~30-40 MB/day of primary egress).
    last_key = f'{path.name}:last_size'
    try:
        base_size = int(known_sha.get(last_key) or 0)
    except Exception:
        base_size = 0
    base_sha = known_sha.get(f'{path.name}:{base_size}') if base_size else None
    if (base_sha and 0 < base_size < size
            and _hl.sha256(blob[:base_size]).hexdigest() == base_sha):
        payload = {
            'day': day,
            'base_size': base_size,
            'base_sha256': base_sha,
            'gz_b64': _b64.b64encode(blob[base_size:]).decode('ascii'),
            'size': size,
            'sha256': sha,
        }
        try:
            r = _rq.put(shadow_url.rstrip('/') + '/api/v1/director/log_archive',
                        json=payload, headers=headers, timeout=30)
            if r.ok:
                try:
                    jr = r.json()
                except Exception:
                    jr = {'status': 'ok'}
                if (jr or {}).get('status') in ('staged', 'unchanged', 'ok'):
                    known_sha[cache_key] = sha
                    known_sha[last_key] = str(size)
                    jr['delta_bytes'] = size - base_size
                    return jr
            # 409 base_mismatch / 400 sha mismatch (pre-delta shadow
            # treats the tail as a whole blob) / anything else → full.
        except Exception:
            pass
    payload = {
        'day': day,
        'gz_b64': _b64.b64encode(blob).decode('ascii'),
        'size': size,
        'sha256': sha,
    }
    try:
        r = _rq.put(shadow_url.rstrip('/') + '/api/v1/director/log_archive',
                    json=payload, headers=headers, timeout=30)
        if r.ok:
            try:
                jr = r.json()
            except Exception:
                jr = {'status': 'ok'}
            known_sha[cache_key] = sha
            known_sha[last_key] = str(size)
            return jr
        return {'error': f'http {r.status_code}: {r.text[:200]}'}
    except Exception as e:
        return {'error': str(e)[:200]}


def log_archive_inventory() -> dict:
    """``{day: {size, sha256}}`` for every local archive file."""
    import hashlib as _hl
    out: dict[str, dict] = {}
    for p in _list_archive_days():
        try:
            b = p.read_bytes()
            out[p.name[:-len('.jsonl.gz')]] = {
                'size': len(b), 'sha256': _hl.sha256(b).hexdigest()}
        except Exception:
            continue
    return {'days': out, 'count': len(out)}


def push_log_archive_to_shadow(shadow_url: str, *, full: bool,
                               cache: dict[str, str]) -> dict:
    """Send today's archive (and on ``full=True`` every other day too)
    to the shadow. ``cache`` is a per-director dict that memoises
    ``{filename:size: sha256}`` so we skip already-pushed bytes."""
    days = _list_archive_days()
    if not days:
        return {'pushed': 0, 'reason': 'no archive'}
    if full:
        # Prime the sha cache from the shadow's inventory so a shadow
        # re-election doesn't re-ship ~140 days (~16 MB) it already has.
        try:
            import requests as _rq
            r = _rq.get(shadow_url.rstrip('/') + '/api/v1/director/log_archive',
                        headers=_admin_headers(), timeout=(5, 20))
            if r.ok:
                inv = (r.json() or {}).get('days') or {}
                for day, ent in inv.items():
                    cache[f'{day}.jsonl.gz:{ent.get("size")}'] = ent.get('sha256')
                    # lets the first push after (re-)election be an
                    # append delta on top of what the shadow holds
                    cache[f'{day}.jsonl.gz:last_size'] = str(ent.get('size') or 0)
        except Exception:
            pass
    # Steady state: push the archive days that are still *growing*. The
    # 24 h ring prunes lines once they are a day old, so the file being
    # appended to right now is *yesterday's* date (today's file does not
    # exist until tomorrow) — matching on today's name shipped nothing
    # ('no targets' every hour, Oct 2026) and the shadow only ever got
    # the archive on a full sweep. Use mtime instead: anything written
    # in the last 48 h. Unchanged files are skipped via the sha cache.
    _cutoff = time.time() - 2 * 86400

    def _recent(p):
        try:
            return p.stat().st_mtime >= _cutoff
        except OSError:
            return False
    targets = days if full else [p for p in days if _recent(p)]
    if not targets:
        return {'pushed': 0, 'reason': 'no targets'}
    results: dict[str, dict] = {}
    pushed = 0
    for p in targets:
        res = _push_one_archive_day(shadow_url, p, cache)
        results[p.name] = res
        if res.get('status') == 'staged':
            pushed += 1
    return {'pushed': pushed, 'targets': len(targets), 'detail': results}


# ── Live merged-log ring replication (director → shadow, delta) ──────
#
# Item 5 of the 2026-09-30 post-mortem: the merged 24 h log lived only on
# the director; an outage left an 8 h hole that had to be rebuilt from
# every peer's raw processor.log. The director now ships the ring as a
# gzipped *delta* (entries newer than the last acked watermark) every
# COMBINED_LOG_PUSH_INTERVAL, and a fresh director pulls the previous
# director's ring on takeover. Both paths import through app.py's
# per-entry-dedup importer, so re-sends are harmless.

COMBINED_LOG_PATH = Path('data/combined_log_24h.jsonl')


def _read_combined_log_since(since_ts: str) -> list[dict]:
    out: list[dict] = []
    try:
        if not COMBINED_LOG_PATH.exists():
            return out
        with open(COMBINED_LOG_PATH, 'rb') as fh:
            for raw in fh:
                try:
                    e = json.loads(raw)
                except Exception:
                    continue
                if isinstance(e, dict) and (e.get('ts') or '') > since_ts:
                    out.append(e)
    except Exception as e:
        log.debug('combined log read failed: %s', e)
    return out


def push_combined_log_delta_to_shadow(shadow_url: str, cache: dict,
                                      timeout: tuple = (5, 60)) -> dict:
    """Send merged-log entries newer than ``cache['wm']`` (ISO ts) to the
    shadow. ``cache`` is per-shadow state owned by the caller; on a
    shadow change pass a fresh dict so the new shadow gets the whole
    24 h ring (≈8 MB raw → ~1 MB gz, once). Returns the ack."""
    import gzip as _gz
    wm = cache.get('wm') or ''
    entries = _read_combined_log_since(wm)
    if not entries:
        return {'status': 'noop', 'sent': 0}
    entries.sort(key=lambda e: e.get('ts', ''))
    # Bound a single push (≤ 20k lines) — the rest goes next interval.
    entries = entries[:20000]
    body = '\n'.join(json.dumps(e, ensure_ascii=False) for e in entries)
    gz = _gz.compress(body.encode('utf-8'), 6)
    r = requests.put(shadow_url.rstrip('/') + '/api/v1/director/combined_log_delta',
                     data=gz, headers={**_admin_headers(),
                                       'Content-Type': 'application/octet-stream',
                                       'Content-Encoding': 'gzip'},
                     timeout=timeout)
    if not r.ok:
        return {'status': f'http_{r.status_code}', 'sent': 0}
    ack = r.json() if r.headers.get('content-type', '').startswith('application/json') else {}
    cache['wm'] = entries[-1].get('ts', wm)
    cache['last_ts'] = time.time()
    return {'status': 'ok', 'sent': len(entries), 'bytes': len(gz),
            'imported': (ack or {}).get('imported')}


def pull_combined_log_from(director_url_str: str, hours: float = 24.0) -> dict:
    """Fetch the previous director's live ring via its public history
    endpoint and import locally (dedup). Best-effort."""
    try:
        r = requests.get(director_url_str.rstrip('/') + '/api/v1/director/log/history',
                         params={'hours': hours, 'limit': 50000},
                         headers={**_admin_headers(), 'Accept-Encoding': 'gzip'},
                         timeout=(5, 120))
        if not r.ok:
            return {'status': f'http_{r.status_code}'}
        entries = (r.json() or {}).get('entries') or []
        import app as _app  # noqa: WPS433 — importer lives with the ring
        n = _app._combined_log_import_entries(entries)
        log.info('takeover: imported %d/%d merged-log entries from %s',
                 n, len(entries), director_url_str)
        return {'status': 'ok', 'fetched': len(entries), 'imported': n}
    except Exception as e:
        log.warning('takeover: merged-log pull from %s failed: %s',
                    director_url_str, e)
        return {'status': 'error', 'error': str(e)[:120]}


def push_snapshot_to_shadow(shadow_url: str, snap: dict | None = None,
                            timeout: tuple = (5, 20)) -> dict:
    """PUT a snapshot to shadow. Returns the peer's response or error dict."""
    if snap is None:
        snap = build_snapshot()
    import gzip as _gz
    raw = json.dumps(snap).encode('utf-8')
    h = dict(_admin_headers())
    h['Content-Type'] = 'application/json'
    h['Content-Encoding'] = 'gzip'
    try:
        r = requests.put(
            shadow_url.rstrip('/') + '/api/v1/director/snapshot',
            data=_gz.compress(raw, compresslevel=6), headers=h,
            timeout=timeout,
        )
        if r.status_code in (400, 415, 500):
            # Shadow predates the gzip request hook — plain retry.
            h.pop('Content-Encoding', None)
            r = requests.put(
                shadow_url.rstrip('/') + '/api/v1/director/snapshot',
                data=raw, headers=h, timeout=timeout)
        if r.ok:
            jr = r.json()
            if isinstance(jr, dict):
                jr['bytes_sent'] = len(raw)
            return jr
        return {'error': f'http_{r.status_code}', 'body': r.text[:200]}
    except Exception as e:
        return {'error': str(e)[:200]}


def ping_heartbeat(director_url_str: str,
                   timeout: tuple = WATCHDOG_TIMEOUT) -> dict | None:
    try:
        r = requests.get(
            director_url_str.rstrip('/') + '/api/v1/director/heartbeat',
            headers=_admin_headers(), timeout=timeout,
        )
        if r.ok:
            return r.json()
    except Exception:
        return None
    return None


def announce_to_peer(peer_url: str, payload: dict,
                     timeout: tuple = (5, 8)) -> dict:
    try:
        r = requests.post(
            peer_url.rstrip('/') + '/api/v1/director/announce',
            json=payload, headers=_admin_headers(), timeout=timeout,
        )
        if r.ok:
            return r.json()
        return {'error': f'http_{r.status_code}'}
    except Exception as e:
        return {'error': str(e)[:200]}


def broadcast_announce(peer_urls: Iterable[str], payload: dict,
                       max_workers: int = 10) -> dict[str, dict]:
    from concurrent.futures import ThreadPoolExecutor
    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=max_workers,
                            thread_name_prefix='dir-announce') as ex:
        futs = {ex.submit(announce_to_peer, u, payload): u for u in peer_urls}
        for f, url in futs.items():
            try:
                results[url] = f.result(timeout=12)
            except Exception as e:
                results[url] = {'error': str(e)[:200]}
    return results


# ── Watchdog (runs on every VM) ──────────────────────────────────

class _WatchdogState:
    def __init__(self) -> None:
        self.misses = 0
        self.misses_started_ts = 0.0
        self.last_hold_reason: str | None = None
        self.last_heartbeat: dict | None = None
        self.last_director_url: str | None = None
        self.takeover_in_flight = False
        self.lock = threading.Lock()


_WATCHDOG = _WatchdogState()


def watchdog_state() -> dict:
    with _WATCHDOG.lock:
        return {
            'misses': _WATCHDOG.misses,
            'misses_started_ts': _WATCHDOG.misses_started_ts,
            'hold_reason': _WATCHDOG.last_hold_reason,
            'flaps_6h': flap_count(),
            'last_heartbeat': _WATCHDOG.last_heartbeat,
            'last_director_url': _WATCHDOG.last_director_url,
            'takeover_in_flight': _WATCHDOG.takeover_in_flight,
            'is_director': IS_DIRECTOR_FLAG.exists(),
            'stepped_down': STEPPED_DOWN_FLAG.exists(),
            'self': load_self(),
            'shadow_meta': _read_json_safe(SHADOW_META_FILE),
        }


def _read_json_safe(p: Path):
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def start_watchdog() -> None:
    """Spawn a daemon thread that watches the director liveness."""
    t = threading.Thread(target=_watchdog_loop, daemon=True,
                         name='director-watchdog')
    t.start()


def _watchdog_loop() -> None:
    time.sleep(20)  # let services boot
    # One watchdog per VM: both gunicorn workers used to probe the
    # director (2× heartbeat traffic). fcntl lock is released if the
    # holding worker dies, so the sibling takes over within a poll.
    import fcntl as _fcntl
    _lp = DATA_DIR / 'watchdog.lock'
    _lp.parent.mkdir(parents=True, exist_ok=True)
    _fh = open(_lp, 'w')
    while True:
        try:
            _fcntl.flock(_fh, _fcntl.LOCK_EX | _fcntl.LOCK_NB)
            break
        except OSError:
            time.sleep(WATCHDOG_INTERVAL)
    while True:
        try:
            _watchdog_tick()
        except Exception as e:
            log.warning('director watchdog tick failed: %s', e)
        time.sleep(WATCHDOG_INTERVAL)


def _watchdog_tick() -> None:
    if not ha_enabled():
        with _WATCHDOG.lock:
            _WATCHDOG.misses = 0
            _WATCHDOG.takeover_in_flight = False
            _WATCHDOG.last_hold_reason = 'ha_disabled'
        return
    if IS_DIRECTOR_FLAG.exists():
        # We *are* the director — nothing to watch.
        with _WATCHDOG.lock:
            _WATCHDOG.misses = 0
            _WATCHDOG.takeover_in_flight = False
        return
    durl = director_url()
    if not durl:
        return
    with _WATCHDOG.lock:
        _WATCHDOG.last_director_url = durl
    hb = ping_heartbeat(durl)
    if hb is not None:
        with _WATCHDOG.lock:
            _WATCHDOG.misses = 0
            _WATCHDOG.misses_started_ts = 0.0
            _WATCHDOG.last_hold_reason = None
            _WATCHDOG.last_heartbeat = hb
        return
    with _WATCHDOG.lock:
        if _WATCHDOG.misses == 0:
            _WATCHDOG.misses_started_ts = time.time()
        _WATCHDOG.misses += 1
        misses = _WATCHDOG.misses
        in_flight = _WATCHDOG.takeover_in_flight
        misses_started = _WATCHDOG.misses_started_ts
    # Flap dampener: after FLAP_MAX_TAKEOVERS takeovers in the window,
    # demand a much longer outage before we promote again.
    threshold = WATCHDOG_MISS_THRESHOLD
    flaps = flap_count()
    if flaps >= FLAP_MAX_TAKEOVERS:
        threshold = WATCHDOG_MISS_THRESHOLD * FLAP_MISS_MULT
    if misses < threshold or in_flight:
        return
    # Only the *shadow* takes over. Read shadow meta to confirm.
    meta = _read_json_safe(SHADOW_META_FILE) or {}
    designated_shadow = (meta.get('shadow_id')
                         or (meta.get('director_state') or {}).get('shadow_peer'))
    me = self_id()
    if designated_shadow != me:
        log.info('director watchdog: %d misses but I am not shadow '
                 '(shadow=%s, me=%s) — waiting', misses, designated_shadow, me)
        return
    # Freshness gate: stale shadow_meta is the #1 cause of split-brain.
    # A peer that *was* shadow once, then was de-elected, keeps a stale
    # meta.json forever. On any later heartbeat blip the watchdog reads
    # 'shadow_id == me' and re-promotes. We refuse to promote unless the
    # meta was received recently (3 × push interval = 90 s).
    received_ts = float(meta.get('received_ts') or 0.0)
    age = time.time() - received_ts
    # Must exceed the miss window: a dead director's last push predates
    # the 6-min miss threshold by definition (pre-Sep-2026 this was 3 ×
    # 30 s = 90 s and could refuse a legitimate takeover).
    SHADOW_META_FRESH_S = max(
        SHADOW_SYNC_INTERVAL * 3,
        WATCHDOG_MISS_THRESHOLD * WATCHDOG_INTERVAL + 2 * SHADOW_SYNC_INTERVAL)
    if received_ts <= 0 or age > SHADOW_META_FRESH_S:
        log.warning('director watchdog: shadow meta stale (age=%.0fs > %ds) — '
                    'refusing to promote, will wait for fresh push',
                    age, SHADOW_META_FRESH_S)
        return
    # (a) Alive-signal: a snapshot push that landed AFTER our first miss
    # proves the director process is alive and has outbound connectivity
    # to us. Our inbound path to it is what's broken — hold.
    if misses_started and received_ts > misses_started:
        _hold('director_alive_pushing',
              'director watchdog: %d misses on %s but its snapshot push '
              'landed %.0fs ago (after first miss) — director alive, our '
              'path to it is broken; holding (edge/proxy issue)',
              misses, durl, age)
        return
    # (b) Quorum: do other peers see the director?
    q = peer_quorum_probe(durl, exclude_urls=[durl, self_url() or ''])
    R, O, F = q['responded'], q['ok'], q['fail']
    if R == 0:
        _hold('isolated', 'director watchdog: %d misses on %s but no peer '
              'answered our quorum probe (%d asked) — we look isolated; '
              'holding', misses, durl, q['asked'])
        return
    if O >= QUORUM_MIN_OK or (R < 3 and O >= 1):
        _hold('quorum_sees_director',
              'director watchdog: %d misses on %s but %d/%d peers reach it '
              '(%s) — holding, our path is the problem',
              misses, durl, O, R, ','.join(q['ok_peers'][:4]))
        return
    if F < QUORUM_MIN_FAIL and R >= 3:
        _hold('quorum_inconclusive',
              'director watchdog: %d misses on %s, quorum inconclusive '
              '(ok=%d fail=%d of %d) — holding one more tick',
              misses, durl, O, F, R)
        return
    log.warning('director watchdog: director %s missed %d heartbeats '
                '(threshold=%d, flaps_6h=%d) and %d/%d peers confirm it '
                'unreachable (%s) — promoting self (%s) to director',
                durl, misses, threshold, flaps, F, R,
                ','.join(q['fail_peers'][:4]), me)
    with _WATCHDOG.lock:
        _WATCHDOG.takeover_in_flight = True
    try:
        _do_takeover(reason=f'watchdog_{misses}_misses_quorum_{F}of{R}',
                     prev_director_url=durl)
    finally:
        with _WATCHDOG.lock:
            _WATCHDOG.takeover_in_flight = False


def _hold(reason: str, fmt: str, *args) -> None:
    """Log a promotion hold once per distinct reason (then every ~10 min
    via the caller's tick cadence: WARNING first, INFO afterwards)."""
    with _WATCHDOG.lock:
        first = _WATCHDOG.last_hold_reason != reason
        _WATCHDOG.last_hold_reason = reason
    (log.warning if first else log.info)(fmt, *args)


# ── Second-signal helpers: peer quorum probe + flap ledger ────────

def probe_url_from_here(target_url: str, timeout: tuple = WATCHDOG_TIMEOUT) -> dict:
    """GET ``<target>/api/v1/director/heartbeat`` from THIS node. Used by
    the ``/api/v1/director/probe`` endpoint so other nodes can ask "can
    *you* reach X?" (out-of-band reachability, item 4 of the 2026-09-30
    post-mortem)."""
    t0 = time.time()
    out = {'target': target_url, 'from': self_id(), 'ok': False,
           'status': None, 'elapsed_ms': None, 'error': None}
    try:
        r = requests.get(target_url.rstrip('/') + '/api/v1/director/heartbeat',
                         headers=_admin_headers(), timeout=timeout)
        out['status'] = r.status_code
        out['ok'] = bool(r.ok)          # 200 = director; 410 = not director
    except Exception as e:
        out['error'] = str(e)[:120]
    out['elapsed_ms'] = int((time.time() - t0) * 1000)
    return out


def peer_quorum_probe(target_url: str, exclude_urls: Iterable[str] = (),
                      max_peers: int = QUORUM_PROBE_PEERS,
                      peer_urls: list[str] | None = None) -> dict:
    """Ask up to ``max_peers`` other peers whether THEY can reach
    ``target_url``'s director heartbeat. Returns counts + peer ids.

    ``responded`` counts peers that answered *us* (our own connectivity);
    ``ok`` / ``fail`` split those by whether they reached the target.
    Rotates the sample each call (random) so a single odd peer can't
    dominate."""
    import random
    from concurrent.futures import ThreadPoolExecutor
    excl = {u.rstrip('/') for u in exclude_urls if u}
    urls = [u for u in (peer_urls if peer_urls is not None else _all_peer_urls())
            if u.rstrip('/') not in excl]
    random.shuffle(urls)
    urls = urls[:max_peers]
    res = {'target': target_url, 'asked': len(urls), 'responded': 0,
           'ok': 0, 'fail': 0, 'http_any': 0, 'ok_peers': [],
           'fail_peers': [], 'silent': []}

    def _one(u: str):
        try:
            r = requests.get(u.rstrip('/') + '/api/v1/director/probe',
                             params={'url': target_url},
                             headers=_admin_headers(), timeout=QUORUM_TIMEOUT)
            if r.ok:
                return u, r.json()
        except Exception:
            pass
        return u, None

    if not urls:
        return res
    with ThreadPoolExecutor(max_workers=min(8, len(urls))) as ex:
        for u, body in ex.map(_one, urls):
            pid = _peer_id_from_url(u)
            if not isinstance(body, dict):
                res['silent'].append(pid)
                continue
            res['responded'] += 1
            if body.get('status') is not None:
                res['http_any'] += 1
            if body.get('ok'):
                res['ok'] += 1
                res['ok_peers'].append(pid)
            else:
                res['fail'] += 1
                res['fail_peers'].append(pid)
    return res


def _peer_id_from_url(u: str) -> str:
    try:
        host = u.split('//', 1)[1].split('/', 1)[0].split(':', 1)[0]
        sub = host.split('.', 1)[0]           # srtm-lidar-at42
        tail = sub.rsplit('-', 1)[-1]         # at42 | at
        return 'primary' if tail == 'at' else tail
    except Exception:
        return u


def flap_record(event: str, **extra) -> None:
    """Append an HA role event (takeover / handover / stepdown / announce)
    to the local ledger. Bounded to the last 200 events."""
    try:
        ev = _read_json_safe(FLAP_FILE) or []
        if not isinstance(ev, list):
            ev = []
        ev.append({'ts': time.time(),
                   'iso': datetime.now(timezone.utc).isoformat(),
                   'event': event, 'me': self_id(), **extra})
        ev = ev[-200:]
        FLAP_FILE.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(FLAP_FILE, json.dumps(ev))
    except Exception as e:
        log.debug('flap_record failed: %s', e)


def flap_events(window_s: float = FLAP_WINDOW_S) -> list[dict]:
    ev = _read_json_safe(FLAP_FILE) or []
    cutoff = time.time() - window_s
    return [e for e in ev if isinstance(e, dict) and float(e.get('ts') or 0) >= cutoff]


def flap_count(window_s: float = FLAP_WINDOW_S) -> int:
    """Number of *distinct* director takeovers seen fleet-wide in the
    window. Dedups (new_director_id, minute) so a takeover we both
    performed and were announced about counts once."""
    seen = set()
    for e in flap_events(window_s):
        if e.get('event') not in ('takeover', 'announce_takeover'):
            continue
        key = (e.get('new_director_id') or e.get('me'), int(float(e.get('ts') or 0) // 120))
        seen.add(key)
    return len(seen)


def flap_lockout_remaining_s() -> float:
    """Seconds an interim director must still hold the role (0 if none).
    Active only when ≥ FLAP_MAX_TAKEOVERS takeovers happened in the
    window; measured from OUR most recent takeover."""
    if flap_count() < FLAP_MAX_TAKEOVERS:
        return 0.0
    mine = [float(e.get('ts') or 0) for e in flap_events()
            if e.get('event') == 'takeover' and e.get('me') == self_id()]
    if not mine:
        return 0.0
    return max(0.0, max(mine) + FLAP_LOCKOUT_S - time.time())


# ── Takeover & step-down ─────────────────────────────────────────

def _do_takeover(reason: str, prev_director_url: str | None = None,
                snapshot_inline: dict | None = None) -> dict:
    """Promote this VM to director. Idempotent.

    Steps:
      1. Install snapshot (from inline payload or staged shadow/).
      2. Write ``is_director`` flag, remove ``stepped_down``.
      3. Remove our own ``zenodo_lock_url.txt`` / clear ``director_url``
         (we are now the broker).
      4. Start the director loop in-process.
      5. Broadcast announce to every peer (peers flip their pointer).
      6. Tell the previous director (if reachable) to step down.
    """
    if not ha_enabled():
        log.error('takeover refused (%s): HA disabled', reason)
        return {'error': 'ha_disabled', 'reason': reason}
    if snapshot_inline is not None:
        install_result = install_snapshot_inline(snapshot_inline)
    else:
        install_result = install_snapshot_from_shadow()
    log.info('takeover: snapshot install: %s', install_result)
    # Snapshots use url=None as a 'this host' marker which now points
    # at the previous director, not us. Rewire peers.json/peer_urls.txt
    # before the director loop reads them.
    try:
        ident_fix = _normalise_local_identity_in_peers(prev_director_url)
        log.info('takeover: identity normalise: %s', ident_fix)
        install_result = dict(install_result or {})
        install_result['identity_fix'] = ident_fix
    except Exception as e:
        log.warning('takeover: identity normalise failed: %s', e)
    try:
        STEPPED_DOWN_FLAG.unlink()
    except FileNotFoundError:
        pass
    IS_DIRECTOR_FLAG.parent.mkdir(parents=True, exist_ok=True)
    IS_DIRECTOR_FLAG.write_text(f'{self_id()} (took over via {reason} '
                                f'at {datetime.now(timezone.utc).isoformat()})\n')
    flap_record('takeover', reason=reason, new_director_id=self_id(),
                prev_director_url=prev_director_url)
    # We are the director now → no zenodo_lock_url pointer.
    set_director_url(None)
    # Restart director loop in-process. Replace the singleton so
    # __init__ re-reads the freshly-installed director_state.json
    # (capacity_ema, capacity_history, peer_noise_long_ema, ...).
    try:
        import peer_director as pd  # noqa: WPS433
        try:
            old = pd.get_director()
            old.stop()
        except Exception:
            pass
        try:
            pd._director = None  # force fresh instance on next get_director()
        except Exception:
            pass
        d = pd.get_director()
        d.start()
    except Exception as e:
        log.warning('takeover: director restart failed: %s', e)
    # Announce to every peer.
    my_url = self_url() or ''
    payload = {
        'new_director_id': self_id(),
        'new_director_url': my_url,
        'prev_director_url': prev_director_url,
        'reason': reason,
        'ts': datetime.now(timezone.utc).isoformat(),
    }
    # _all_peer_urls() only returns peers with non-null URLs, which used
    # to skip the previous director (whose url=None marker we just
    # rewrote, but the old director's local entry is canonical url=None
    # in *its* peers.json). Make sure the previous director URL is in
    # the announce list explicitly.
    peer_urls = _all_peer_urls(exclude=[my_url])
    if prev_director_url:
        prev_norm = prev_director_url.rstrip('/')
        if prev_norm and prev_norm not in (my_url.rstrip('/'),) \
                and prev_norm not in peer_urls:
            peer_urls.append(prev_norm)
    results = broadcast_announce(peer_urls, payload)
    log.info('takeover: announced to %d peers', len(results))
    # Tell the old director to step down (best-effort).
    if prev_director_url and prev_director_url not in (my_url, ''):
        try:
            r = requests.post(
                prev_director_url.rstrip('/') + '/api/v1/director/step_down',
                json=payload, headers=_admin_headers(), timeout=(3, 5),
            )
            log.info('takeover: step_down on %s: %s',
                     prev_director_url, r.status_code)
        except Exception as e:
            log.info('takeover: prev director %s unreachable for step_down: %s',
                     prev_director_url, e)
    # Catch-up reconciliation: any uploads that happened during the
    # handover window may not be reflected in the snapshot we just
    # installed.  Pull every peer's manifest in parallel and merge
    # timestamp-aware so we have a fully-consistent view immediately.
    # Best-effort — the regular peer-sync loop is the safety net.
    try:
        import requests as _req, threading as _th
        def _reconcile():
            try:
                _req.post('http://127.0.0.1:8000/api/v1/manifest/reconcile',
                          headers=_admin_headers(), timeout=60)
                log.info('takeover: triggered manifest reconcile')
            except Exception as e:
                log.warning('takeover: reconcile trigger failed: %s', e)
        # Fire async — don't block the takeover response on a 16-way fan-out.
        _th.Thread(target=_reconcile, daemon=True, name='takeover-reconcile').start()
    except Exception as e:
        log.warning('takeover: reconcile setup failed: %s', e)
    # Merged-log continuity: pull the previous director's live 24 h ring
    # (it holds everything the fleet logged while *it* directed) and
    # import with per-entry dedup. Async + best-effort; the shadow delta
    # push already covers the steady state, this covers the tail between
    # the last delta and the handover.
    if prev_director_url and prev_director_url not in (my_url, ''):
        try:
            import threading as _th2
            _th2.Thread(target=pull_combined_log_from, args=(prev_director_url,),
                        daemon=True, name='takeover-logpull').start()
        except Exception as e:
            log.debug('takeover: log pull setup failed: %s', e)
    return {'status': 'promoted', 'install': install_result,
            'announced': len(results), 'reason': reason}


def _all_peer_urls(exclude: Iterable[str] = ()) -> list[str]:
    excl = {u.rstrip('/') for u in exclude if u}
    out: list[str] = []
    try:
        cfg = json.loads((DATA_DIR / 'peers.json').read_text())
        for p in cfg.get('peers', []):
            u = (p.get('url') or '').rstrip('/')
            if u and u not in excl:
                out.append(u)
    except Exception:
        pass
    return out


def step_down(announce_payload: dict) -> dict:
    """Voluntarily relinquish director role.

    Removes ``is_director``, writes ``stepped_down`` (so a cold restart
    won't auto-revive us as director), updates pointers, stops the
    director loop. Safe to call when we already aren't the director.
    """
    new_url = (announce_payload or {}).get('new_director_url') or None
    if not ha_enabled():
        log.error('step_down refused: HA disabled (payload=%s)',
                  {k: v for k, v in (announce_payload or {}).items() if k != 'snapshot'})
        return {'error': 'ha_disabled'}
    was_director = IS_DIRECTOR_FLAG.exists()
    try:
        IS_DIRECTOR_FLAG.unlink()
    except FileNotFoundError:
        pass
    if was_director:
        flap_record('stepdown',
                    new_director_id=(announce_payload or {}).get('new_director_id'),
                    reason=(announce_payload or {}).get('reason', 'step_down'))
    # Invalidate any stale shadow_meta. We are no longer the shadow
    # of anyone (we just stepped down). Without this, a future heartbeat
    # blip would let our watchdog re-promote based on a meta file that
    # still says 'shadow_id == me'. This is the cascade trigger.
    try:
        SHADOW_META_FILE.unlink()
    except FileNotFoundError:
        pass
    except Exception:
        pass
    STEPPED_DOWN_FLAG.write_text(
        json.dumps({
            'ts': datetime.now(timezone.utc).isoformat(),
            'new_director_id': (announce_payload or {}).get('new_director_id'),
            'new_director_url': new_url,
            'reason': (announce_payload or {}).get('reason', 'step_down'),
        }, indent=2))
    if new_url:
        set_director_url(new_url)
    if was_director:
        try:
            import peer_director as pd  # noqa: WPS433
            pd.get_director().stop()
            log.warning('step_down: director loop stopped (handed over to %s)',
                        new_url or '?')
        except Exception as e:
            log.warning('step_down: failed to stop director loop: %s', e)
    return {'status': 'stepped_down', 'was_director': was_director,
            'new_director_url': new_url}


def accept_announce(payload: dict) -> dict:
    """Handle an inbound ``/director/announce`` notification.

    If the announce names *us* as the new director, ignore (we already know).
    If we currently believe ourselves to be the director, step down.
    Otherwise just flip the pointer to the new director URL.
    """
    new_id = payload.get('new_director_id')
    new_url = payload.get('new_director_url')
    if not new_url:
        return {'status': 'rejected', 'reason': 'no_new_director_url'}
    if not ha_enabled() and not director_url_allowed(new_url):
        log.error('announce refused: HA disabled, %s is not the pinned '
                  'primary (new_id=%s)', new_url, new_id)
        return {'status': 'rejected', 'reason': 'ha_disabled'}
    if new_id == self_id():
        return {'status': 'self', 'note': 'I am the new director'}
    # Fleet-wide flap ledger: every peer hears every takeover.
    if str(payload.get('reason') or '').startswith('watchdog'):
        flap_record('announce_takeover', new_director_id=new_id,
                    reason=payload.get('reason'))
    if IS_DIRECTOR_FLAG.exists():
        return step_down(payload)
    set_director_url(new_url)
    # Same invariant as in step_down: any peer that receives an announce
    # is now a follower of ``new_id``, so its prior shadow_meta (if any)
    # is stale by definition.
    try:
        SHADOW_META_FILE.unlink()
    except FileNotFoundError:
        pass
    except Exception:
        pass
    # Clear watchdog miss counter so we don't immediately try to re-takeover.
    with _WATCHDOG.lock:
        _WATCHDOG.misses = 0
        _WATCHDOG.last_director_url = new_url
    return {'status': 'pointed_at', 'director_url': new_url}


# ── Manual handover (from current director) ──────────────────────

def do_handover(target_peer_id: str, target_peer_url: str) -> dict:
    """Initiated *by* the current director. Ships state to target and
    asks it to take over, then steps down locally.
    """
    if not IS_DIRECTOR_FLAG.exists():
        return {'error': 'not_director',
                'note': 'Only the current director may initiate handover.'}
    if not ha_enabled():
        return {'error': 'ha_disabled',
                'note': 'create data/ha_enabled on the primary to allow handover'}
    snap = build_snapshot()
    # Annotate snapshot with shadow_id so the target's takeover gate accepts it.
    snap['_meta'] = dict(snap.get('_meta') or {})
    snap['_meta']['shadow_id'] = target_peer_id
    payload = {
        'reason': 'manual_handover',
        'prev_director_url': self_url(),
        'snapshot': snap,
    }
    try:
        r = requests.post(
            target_peer_url.rstrip('/') + '/api/v1/director/takeover',
            json=payload, headers=_admin_headers(), timeout=(5, 30),
        )
        if not r.ok:
            return {'error': f'http_{r.status_code}', 'body': r.text[:200]}
        body = r.json()
    except Exception as e:
        return {'error': str(e)[:200]}
    # Target will broadcast announce (which will tell us to step down too,
    # but step down here proactively so the loop on this box stops fast).
    flap_record('handover', new_director_id=target_peer_id)
    step_down({
        'new_director_id': target_peer_id,
        'new_director_url': target_peer_url,
        'reason': 'manual_handover',
    })
    return {'status': 'handed_over', 'target': target_peer_id,
            'target_response': body}
