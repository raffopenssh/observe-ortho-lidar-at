"""fleet_secrets — provider secrets (Zenodo / umfeld / GitHub) leased from the
primary, held in memory on peers, never on a peer's disk, never in git.

Why
---
Before Oct 2026 the Zenodo token was a string literal in three modules
(so in every clone and in git history), the umfeld and GitHub tokens were
*pushed* by the director onto each peer's ``data/`` directory, and nothing
recorded which VM held what. A compromised peer therefore owned every
provider credential forever, and rotating one meant touching 50 VMs.

Model
-----
* The **primary** keeps the only durable copy: ``data/secrets.json`` (0600,
  gitignored) ``{"rev": N, "zenodo": …, "umfeld": …, "github": …}``.
  ``python3 fleet_secrets.py --set zenodo`` (value on stdin) bumps ``rev``.
* A **peer** calls ``GET <director>/api/v1/secrets/lease`` with its own
  peer token (``peer_tokens.PEER_SCOPE``) and gets the current bundle. It
  is cached in process memory only (``token(kind)``); nothing is written
  to disk, and any legacy ``data/umfeld_token`` / ``data/github_token``
  left by the old push mechanism is scrubbed after the first lease.
* **Bandwidth**: the lease is ~300 B and is fetched once per process plus
  on change. Change detection piggybacks on the status-push ack the peer
  already receives every ~30 s (``secrets_rev`` field → ``note_rev``), so
  there is no extra polling. A provider 401 triggers one re-lease
  (``invalidate``), rate-limited.
* **Audit / revocation**: every lease is recorded in
  ``data/secret_leases.json`` (peer → ts, rev, ip) and surfaces as a
  director event on rev change; ``data/secrets_deny`` (one peer id per
  line) refuses further leases to a suspect VM. Rotating a provider token
  = rotate at the provider, ``--set`` on the primary, fleet picks it up
  within one push interval; the old value dies everywhere at once.

What this does *not* do: a peer that is already compromised while it
holds a lease can read its own memory. The gain is central rotation,
role-scoped delivery, no on-disk/in-git copies, and an audit trail —
see ``docs/security-tokens.md → Provider secrets``.

Zenodo offers no scoped or short-lived tokens, so the Zenodo value is the
full account token; the only hard blast-radius limit there is the
safe-deserialisation layer (``safe_load.py``) that stops a poisoned
Zenodo artefact from executing code on the peers that read it.
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
from pathlib import Path

KINDS = ('zenodo', 'umfeld', 'github')
ENV_NAMES = {'zenodo': 'ZENODO_TOKEN', 'umfeld': 'UMFELD_TOKEN', 'github': 'GITHUB_TOKEN'}

DATA = Path(os.environ.get('SRTM_DATA_DIR') or (Path(__file__).resolve().parent / 'data'))
STORE = DATA / 'secrets.json'
LEASES = DATA / 'secret_leases.json'
DENY = DATA / 'secrets_deny'
# legacy on-disk copies the old director push left on peers
LEGACY_FILES = {'umfeld': DATA / 'umfeld_token', 'github': DATA / 'github_token'}

LEASE_PATH = '/api/v1/secrets/lease'
MIN_REFRESH_INTERVAL_S = 20.0     # floor between lease fetches (401 storms)
LEASE_TIMEOUT_S = (10, 30)

_lock = threading.Lock()
_cache: dict = {'rev': None, 'vals': {}, 'fetched': 0.0, 'last_try': 0.0,
                'source': None, 'err': None}
_event_hook = None   # callable(kind:str, msg:str) — app.py plugs director_event in


# ----------------------------------------------------------------------
# role helpers
# ----------------------------------------------------------------------

def is_primary() -> bool:
    try:
        import peer_tokens
        return peer_tokens.is_primary_role()
    except Exception:
        return False


def director_base() -> str | None:
    """Base URL of the director as seen from this VM (``…/k`` on peers)."""
    env = (os.environ.get('ZENODO_LOCK_URL') or '').strip()
    cands = [env]
    try:
        s = json.loads((DATA / 'austria_processor' / 'self.json').read_text())
        cands.append((s.get('director_url') or '').strip())
    except Exception:
        pass
    try:
        cands.append((DATA / 'austria_processor' / 'zenodo_lock_url.txt').read_text().strip())
    except Exception:
        pass
    for c in cands:
        if c and 'srtm-lidar-' in c:
            return c.rstrip('/')
    return None


def _own_peer_token() -> str:
    try:
        return (DATA / 'admin_token').read_text().strip()
    except Exception:
        return ''


# ----------------------------------------------------------------------
# primary: durable store + lease issuance
# ----------------------------------------------------------------------

def _load_store() -> dict:
    try:
        d = json.loads(STORE.read_text() or '{}')
        if isinstance(d, dict):
            d.setdefault('rev', 0)
            return d
    except FileNotFoundError:
        pass
    except Exception:
        pass
    return {'rev': 0}


def _write_secret_file(p: Path, text: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + '.tmp')
    with open(tmp, 'w') as f:
        os.fchmod(f.fileno(), 0o600)
        f.write(text)
    os.replace(tmp, p)
    os.chmod(p, 0o600)


def set_secret(kind: str, value: str) -> int:
    """Primary CLI: store ``value`` for ``kind``; bumps ``rev``. Returns rev."""
    if kind not in KINDS:
        raise ValueError(f'unknown secret kind {kind!r} (one of {KINDS})')
    value = (value or '').strip()
    if len(value) < 16:
        raise ValueError('secret too short')
    with _lock:
        d = _load_store()
        if d.get(kind) == value:
            return int(d['rev'])
        d[kind] = value
        d['rev'] = int(d.get('rev') or 0) + 1
        d['updated_at'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        _write_secret_file(STORE, json.dumps(d, indent=1) + '\n')
        _cache.update(rev=None, vals={}, fetched=0.0)
        return int(d['rev'])


def store_rev() -> int:
    """Current store revision (primary); 0 when no store exists."""
    return int(_load_store().get('rev') or 0)


def denied() -> set:
    try:
        return {l.strip() for l in DENY.read_text().splitlines() if l.strip() and not l.startswith('#')}
    except Exception:
        return set()


def set_denied(peer_id: str, deny: bool) -> set:
    s = denied()
    (s.add if deny else s.discard)(peer_id)
    _write_secret_file(DENY, '\n'.join(sorted(s)) + ('\n' if s else ''))
    return s


def lease_for(peer_id: str, ip: str = '') -> dict | None:
    """Bundle for ``peer_id`` or ``None`` when the peer is denied / no store.
    Records the lease (audit) and emits a director event on rev change."""
    d = _load_store()
    if not any(d.get(k) for k in KINDS):
        return None
    if peer_id in denied():
        _emit('secret-lease-denied', f'{peer_id} from {ip}')
        return None
    rev = int(d.get('rev') or 0)
    prev = _record_lease(peer_id, rev, ip)
    if prev != rev:
        _emit('secret-lease', f'{peer_id} rev={rev} (was {prev}) from {ip}')
    return {'rev': rev, 'ttl_s': 6 * 3600,
            **{k: d.get(k) for k in KINDS if d.get(k)}}


def _record_lease(peer_id: str, rev: int, ip: str):
    with _lock:
        try:
            L = json.loads(LEASES.read_text() or '{}')
        except Exception:
            L = {}
        prev = (L.get(peer_id) or {}).get('rev')
        e = L.get(peer_id) or {}
        e.update(rev=rev, ts=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), ip=ip,
                 n=int(e.get('n') or 0) + 1)
        L[peer_id] = e
        try:
            _write_secret_file(LEASES, json.dumps(L, indent=1, sort_keys=True) + '\n')
        except Exception:
            pass
    return prev


def leases() -> dict:
    try:
        return json.loads(LEASES.read_text() or '{}')
    except Exception:
        return {}


def _emit(kind: str, msg: str) -> None:
    try:
        if _event_hook:
            _event_hook(kind, msg)
    except Exception:
        pass


def set_event_hook(fn) -> None:
    global _event_hook
    _event_hook = fn


# ----------------------------------------------------------------------
# client side (peers *and* primary processes)
# ----------------------------------------------------------------------

def _legacy_disk(kind: str) -> str:
    p = LEGACY_FILES.get(kind)
    try:
        return p.read_text().strip() if p else ''
    except Exception:
        return ''


def refresh(force: bool = False, reason: str = '') -> bool:
    """(Re)fetch the bundle. Primary: read the store. Peer: GET the lease.
    Returns True when a bundle is now cached."""
    now = time.time()
    with _lock:
        if not force and now - _cache['last_try'] < MIN_REFRESH_INTERVAL_S:
            return bool(_cache['vals'])
        _cache['last_try'] = now
    if is_primary():
        d = _load_store()
        vals = {k: d[k] for k in KINDS if d.get(k)}
        with _lock:
            _cache.update(rev=int(d.get('rev') or 0), vals=vals, fetched=now,
                          source='store', err=None)
        return bool(vals)
    base = director_base()
    if not base:
        with _lock:
            _cache['err'] = 'no director url'
        return bool(_cache['vals'])
    try:
        import requests
        hdr = {'X-Admin-Token': _own_peer_token()}
        r = requests.get(base + LEASE_PATH, headers=hdr, timeout=LEASE_TIMEOUT_S)
        if r.status_code != 200:
            raise RuntimeError(f'lease HTTP {r.status_code}')
        b = r.json()
        vals = {k: b[k] for k in KINDS if b.get(k)}
        if not vals:
            raise RuntimeError('empty lease')
        with _lock:
            _cache.update(rev=int(b.get('rev') or 0), vals=vals, fetched=time.time(),
                          source='lease', err=None)
        _scrub_legacy()
        return True
    except Exception as e:
        with _lock:
            _cache['err'] = f'{type(e).__name__}: {e}'[:160]
        return bool(_cache['vals'])


def _scrub_legacy() -> None:
    """Peers: remove on-disk copies the old push mechanism left behind."""
    if is_primary():
        return
    for p in LEGACY_FILES.values():
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass


def token(kind: str, required: bool = False) -> str:
    """Current value for ``kind``. Order: process env override → cached
    bundle → (fetch) → legacy disk file. ``required`` raises when empty."""
    if kind not in KINDS:
        raise ValueError(kind)
    env = (os.environ.get(ENV_NAMES[kind]) or '').strip()
    if env:
        return env
    v = _cache['vals'].get(kind)
    if not v:
        refresh()
        v = _cache['vals'].get(kind) or _legacy_disk(kind)
    if not v and required:
        raise RuntimeError(f'{kind} secret unavailable ({_cache.get("err") or "no lease"})')
    return v or ''


def note_rev(rev) -> None:
    """Called with the ``secrets_rev`` the director put in a status-push
    ack. Re-leases only when it differs from what we hold."""
    try:
        rev = int(rev)
    except Exception:
        return
    if rev and rev != _cache['rev']:
        threading.Thread(target=refresh, kwargs={'force': True, 'reason': 'rev'},
                         name='secrets-refresh', daemon=True).start()


def invalidate(kind: str = '') -> None:
    """A provider answered 401/403 — try one re-lease (rate-limited)."""
    refresh(force=False, reason=f'invalidate:{kind}')


def cached_rev():
    return _cache['rev']


def export_env() -> dict:
    """Env overrides for child processes (git, the KG subprocess)."""
    out = {}
    for k in KINDS:
        v = token(k)
        if v:
            out[ENV_NAMES[k]] = v
    return out


def status() -> dict:
    """Telemetry — never includes values."""
    return {'rev': _cache['rev'], 'have': sorted(k for k, v in _cache['vals'].items() if v),
            'source': _cache['source'], 'age_s': (int(time.time() - _cache['fetched']) if _cache['fetched'] else None),
            'err': _cache['err']}


# ----------------------------------------------------------------------
# CLI (primary)
# ----------------------------------------------------------------------

def _main(argv: list[str]) -> int:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--set', metavar='KIND', help='store a secret (value from --value or stdin)')
    ap.add_argument('--value')
    ap.add_argument('--status', action='store_true')
    ap.add_argument('--leases', action='store_true')
    ap.add_argument('--deny', metavar='PEER')
    ap.add_argument('--allow', metavar='PEER')
    a = ap.parse_args(argv)
    if a.set:
        v = a.value if a.value is not None else sys.stdin.read()
        print(f'{a.set}: rev={set_secret(a.set, v)}')
    if a.deny:
        print('denied:', sorted(set_denied(a.deny, True)))
    if a.allow:
        print('denied:', sorted(set_denied(a.allow, False)))
    if a.status or not (a.set or a.deny or a.allow or a.leases):
        d = _load_store()
        print(f"rev={d.get('rev')} have={[k for k in KINDS if d.get(k)]} updated={d.get('updated_at')} denied={sorted(denied())}")
    if a.leases:
        for p, e in sorted(leases().items()):
            print(f"{p:10s} rev={e.get('rev')} n={e.get('n')} {e.get('ts')} {e.get('ip')}")
    return 0


if __name__ == '__main__':
    sys.exit(_main(sys.argv[1:]))
