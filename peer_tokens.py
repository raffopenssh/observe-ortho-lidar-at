"""Per-peer cluster tokens + scoped auth (2026-10-23 security hardening).

Before: one shared ``data/admin_token`` on every VM.  Any single
compromised peer held a token that could stop/start/update/re-point the
entire fleet, read the director snapshot (Copernicus credentials, peer
roster) and rotate the token itself.

Now three token classes (``classify``):

* **master** — ``data/admin_token`` on the *primary*.  Full access.  Known
  only to the primary and the owner (dashboard).  Never sent to a peer.
* **peer**   — each peer's own ``data/admin_token`` (unique per host).  The
  primary keeps the map in ``data/peer_tokens.json`` (0600, gitignored,
  never part of any snapshot) and uses the right one for every outbound
  call (``install_outbound_hook`` rewrites ``X-Admin-Token`` per
  destination host).  On the primary a peer token only unlocks the
  **PEER_SCOPE** allow-list (status pushes, Zenodo lock, cache-manifest
  sync, product fetches…) — nothing under ``/admin``, nothing that
  starts/stops/updates/re-points other peers, no snapshot.
* **mesh**   — ``data/mesh_token``, fleet-shared, scope = **MESH_SCOPE**
  only (peer→peer cache-manifest delta PUT).  A leaked mesh token can at
  worst poison a peer's cache-manifest view.

Rotation never re-uses a channel the attacker could own: each peer's new
token is pushed *to that peer only* (``/api/v1/admin/install_token``,
authenticated with that peer's current token); the master is rotated
last and only on the primary.  See ``docs/security-tokens.md``.

CLI (run on the primary):
    python3 peer_tokens.py --rotate            # all enabled peers + master
    python3 peer_tokens.py --rotate --peer at12
    python3 peer_tokens.py --new-peer at77     # mint + print token for deploy.sh
    python3 peer_tokens.py --status
    python3 peer_tokens.py --purge-legacy      # forget the pre-rotation master
"""
from __future__ import annotations

import hmac
import json
import logging
import os
import secrets
import sys
import threading
import time
from pathlib import Path
from urllib.parse import urlparse

log = logging.getLogger(__name__)

ADMIN_TOKEN_PATH = Path('data/admin_token')
PEER_TOKENS_PATH = Path('data/peer_tokens.json')
MESH_TOKEN_PATH = Path('data/mesh_token')
HEADER = 'X-Admin-Token'
LEGACY_KEY = '_legacy_master'

# ---------------------------------------------------------------- scopes
# Allow-list (prefix, methods|None=any) for *peer* tokens on the primary.
# Everything not listed → 403 + a 'peer-scope-deny' director event, so a
# forgotten legitimate path shows up in /process.txt?q=peer-scope within
# minutes instead of silently working with full rights.
PEER_SCOPE = (
    ('/api/v1/zenodo/lock', None),
    ('/api/v1/ne/declared/', ('GET',)),
    ('/api/v1/director/vct_reported', None),
    ('/api/v1/director/peer_status', ('POST',)),
    ('/api/v1/director/peers/add', ('POST',)),   # self-registration only (handler checks host == token owner)
    ('/api/v1/director/probe', ('GET',)),
    ('/api/v1/director/heartbeat', ('GET',)),
    ('/api/v1/processing/cache_manifest', ('GET', 'PUT')),
    ('/api/v1/processing/kg_strikes', ('GET', 'PUT')),
    ('/api/v1/processing/cache_misses', ('GET', 'POST', 'PUT')),
    ('/api/v1/processing/partial_kgs', ('GET', 'POST', 'PUT')),
    ('/api/v1/processing/product', ('GET',)),
    ('/api/v1/processing/tiles', ('GET',)),
    ('/api/v1/manifest/push', ('POST', 'PUT')),
    ('/api/v1/manifest/reconcile', ('POST',)),
    ('/api/v1/admin/bevdirect_binary', ('GET',)),
    ('/api/v1/credentials', ('GET',)),
    ('/api/v1/net_stats', ('GET',)),
    ('/api/v1/secrets/lease', ('GET',)),   # fleet_secrets: provider tokens, memory-only on peers
)

# Peer→peer: only the cache-manifest delta sync.
MESH_SCOPE = (
    ('/api/v1/processing/cache_manifest', ('GET', 'PUT')),
)


def _scope_allows(scope, path: str, method: str) -> bool:
    for prefix, methods in scope:
        if path == prefix or path.startswith(prefix.rstrip('/') + '/') \
                or (prefix.endswith('/') and path.startswith(prefix)):
            return methods is None or method.upper() in methods
    return False


def peer_scope_allowed(path: str, method: str) -> bool:
    return _scope_allows(PEER_SCOPE, path, method)


def mesh_scope_allowed(path: str, method: str) -> bool:
    return _scope_allows(MESH_SCOPE, path, method)


# ---------------------------------------------------------------- files
_cache = {'t': 0.0, 'peers': {}, 'mesh': '', 'mtime': 0.0}
_lock = threading.Lock()


def _read_text(p: Path) -> str:
    try:
        return p.read_text().strip()
    except FileNotFoundError:
        return ''
    except Exception as e:
        log.warning('peer_tokens: cannot read %s: %s', p, e)
        return ''


def _write_secret(p: Path, content: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + '.tmp')
    with open(tmp, 'w') as f:
        f.write(content)
    os.chmod(tmp, 0o600)
    os.replace(tmp, p)


def load_peer_tokens(force: bool = False) -> dict:
    """``{host: token, ...}`` (+ ``_legacy_master``). Cached 5 s."""
    now = time.time()
    with _lock:
        if not force and now - _cache['t'] < 5:
            return _cache['peers']
        d: dict = {}
        raw = _read_text(PEER_TOKENS_PATH)
        if raw:
            try:
                d = json.loads(raw) or {}
            except Exception as e:
                log.error('peer_tokens: %s unparsable: %s', PEER_TOKENS_PATH, e)
                d = _cache['peers']
        _cache['peers'] = d
        _cache['mesh'] = _read_text(MESH_TOKEN_PATH)
        _cache['t'] = now
        return d


def save_peer_tokens(d: dict) -> None:
    with _lock:
        _write_secret(PEER_TOKENS_PATH, json.dumps(d, indent=1, sort_keys=True))
        _cache['peers'] = dict(d)
        _cache['t'] = time.time()


def mesh_token() -> str:
    load_peer_tokens()
    if not _cache['mesh']:
        _cache['mesh'] = _read_text(MESH_TOKEN_PATH)
    return _cache['mesh']


IS_DIRECTOR_FLAG = Path('data/austria_processor/is_director')


def is_primary_role() -> bool:
    """The primary = the VM holding the peer-token map (or the director
    flag). Decides owner-channel enforcement for the master and the
    outbound rewrite direction."""
    return PEER_TOKENS_PATH.exists() or IS_DIRECTOR_FLAG.exists()


def generate() -> str:
    return secrets.token_urlsafe(32)


def host_of(url: str | None) -> str:
    try:
        return (urlparse(url or '').hostname or '').lower().rstrip('.')
    except Exception:
        return ''


def token_for_host(host: str) -> str | None:
    return load_peer_tokens().get(host) or None


def all_peer_tokens() -> set:
    return {v for k, v in load_peer_tokens().items()
            if not k.startswith('_') and isinstance(v, str) and len(v) >= 16}


# ---------------------------------------------------------------- classify
def classify(tok: str | None, master: str) -> str | None:
    """'master' | 'peer' | 'mesh' | None for a presented token."""
    if not tok or len(tok) < 16:
        return None
    if master and hmac.compare_digest(tok, master):
        return 'master'
    for t in all_peer_tokens():
        if hmac.compare_digest(tok, t):
            return 'peer'
    m = mesh_token()
    if m and hmac.compare_digest(tok, m):
        return 'mesh'
    return None


def peer_for_token(tok: str) -> str | None:
    for k, v in load_peer_tokens().items():
        if not k.startswith('_') and isinstance(v, str) and hmac.compare_digest(tok, v):
            return k
    return None


# ---------------------------------------------------------------- outbound
_hook_installed = False
_LOCAL_HOSTS = {'', 'localhost', '127.0.0.1', '::1'}


def _self_hosts() -> set:
    hs = set()
    try:
        s = json.loads(Path('data/austria_processor/self.json').read_text())
        hs.add(host_of(s.get('url')))
    except Exception:
        pass
    return hs - {''}


def _director_host() -> str:
    try:
        s = json.loads(Path('data/austria_processor/self.json').read_text())
        if s.get('director_url'):
            return host_of(s['director_url'])
    except Exception:
        pass
    try:
        return host_of(Path('data/austria_processor/zenodo_lock_url.txt').read_text().strip())
    except Exception:
        return ''


def outbound_token(url: str, presented: str | None) -> str | None:
    """Token to send to ``url`` given what the caller put in the header.

    Returns the (possibly replaced) token, or '' to strip the header.
    """
    host = host_of(url)
    if host in _LOCAL_HOSTS or host in _self_hosts():
        return presented
    if is_primary_role():
        t = token_for_host(host)
        if t:
            return t
        if presented and presented == _read_text(ADMIN_TOKEN_PATH):
            # Master must never leave the primary.  Fallback for peers
            # not yet rotated: the recorded legacy master (if any).
            legacy = load_peer_tokens().get(LEGACY_KEY)
            if legacy:
                return legacy
            log.warning('peer_tokens: stripping master token from outbound %s %s',
                        host, url.split('?')[0][:120])
            return ''
        return presented
    # Peer role: own token is only meaningful at the director.
    if host == _director_host():
        return presented
    m = mesh_token()
    if presented and m:
        return m
    return presented


def install_outbound_hook() -> None:
    """Monkeypatch ``requests.Session.request`` so every outbound call
    carrying ``X-Admin-Token`` gets the right token for its destination.
    Covers requests.get/post/…, http_pool sessions, director_ha, vct_fleet."""
    global _hook_installed
    if _hook_installed:
        return
    try:
        import requests
    except Exception:
        return
    _orig = requests.Session.request

    def _request(self, method, url, *a, **kw):
        try:
            headers = kw.get('headers')
            presented = None
            if headers and HEADER in headers:
                presented = headers.get(HEADER)
            elif HEADER in (self.headers or {}):
                presented = self.headers.get(HEADER)
            if presented is not None:
                want = outbound_token(str(url), presented)
                if want != presented:
                    h = dict(headers or {})
                    if want:
                        h[HEADER] = want
                    else:
                        h[HEADER] = ''
                    kw['headers'] = h
        except Exception as e:  # never break a request over this
            log.debug('peer_tokens hook: %s', e)
        return _orig(self, method, url, *a, **kw)

    requests.Session.request = _request
    _hook_installed = True


# ---------------------------------------------------------------- rotation
def _peers_config() -> list[dict]:
    try:
        cfg = json.loads(Path('data/austria_processor/peers.json').read_text())
        return [p for p in cfg.get('peers', []) if p.get('url')]
    except Exception as e:
        print(f'cannot read peers.json: {e}', file=sys.stderr)
        return []


def _install_on_peer(url: str, current: str, new_tok: str, mesh: str, timeout=20) -> dict:
    import requests
    try:
        r = requests.post(url.rstrip('/') + '/api/v1/admin/install_token',
                          json={'new_token': new_tok, 'current_token': current,
                                'mesh_token': mesh},
                          headers={HEADER: current}, timeout=timeout)
        if r.ok:
            return {'status': 'installed', 'mesh': bool((r.json() or {}).get('mesh'))}
        return {'status': f'http_{r.status_code}', 'body': r.text[:160]}
    except Exception as e:
        return {'status': 'error', 'error': str(e)[:160]}


def rotate(peer_ids: list[str] | None = None, rotate_master: bool = True,
           only_enabled: bool = True) -> dict:
    """Rotate every peer (or the given ids) to a unique token, then the
    master.  Safe to re-run: a peer that already has a mapped token is
    re-rotated using that mapping; one that failed last time is retried
    with the recorded legacy master."""
    master = _read_text(ADMIN_TOKEN_PATH)
    if not master:
        raise SystemExit('no data/admin_token on this VM — run on the primary')
    d = load_peer_tokens(force=True)
    mesh = mesh_token() or generate()
    if not MESH_TOKEN_PATH.exists():
        _write_secret(MESH_TOKEN_PATH, mesh)
    if LEGACY_KEY not in d and not any(not k.startswith('_') for k in d):
        d[LEGACY_KEY] = master           # first rotation: remember old shared token
        save_peer_tokens(d)
    out: dict = {}
    for p in _peers_config():
        if peer_ids and p.get('id') not in peer_ids:
            continue
        if only_enabled and not p.get('enabled', True) and not peer_ids:
            out[p['id']] = {'status': 'skipped_disabled'}
            continue
        host = host_of(p['url'])
        current = d.get(host) or d.get(LEGACY_KEY) or master
        new_tok = generate()
        res = _install_on_peer(p['url'], current, new_tok, mesh)
        if res.get('status') == 'installed':
            d[host] = new_tok
            save_peer_tokens(d)
        out[p['id']] = {'host': host, **res}
        print(f"{p['id']:8s} {host:36s} {res.get('status')}"
              + (f" {res.get('error') or res.get('body','')}" if res.get('status') != 'installed' else ''))
    if rotate_master:
        new_master = generate()
        _write_secret(ADMIN_TOKEN_PATH, new_master)
        out['_master'] = 'rotated'
        print('\nMASTER token rotated. New owner token (dashboard / curl):')
        print(new_master)
    return out


def status() -> dict:
    d = load_peer_tokens(force=True)
    peers = _peers_config()
    mapped = [p['id'] for p in peers if d.get(host_of(p['url']))]
    unmapped = [p['id'] for p in peers if not d.get(host_of(p['url']))]
    return {'mapped': mapped, 'unmapped': unmapped,
            'legacy_master_kept': LEGACY_KEY in d,
            'mesh_token': MESH_TOKEN_PATH.exists()}


def _main(argv: list[str]) -> int:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--rotate', action='store_true')
    ap.add_argument('--peer', action='append', help='restrict --rotate to peer id(s)')
    ap.add_argument('--no-master', action='store_true', help='do not rotate the master')
    ap.add_argument('--new-peer', metavar='HOST_OR_ID', help='mint a token for a new peer')
    ap.add_argument('--status', action='store_true')
    ap.add_argument('--purge-legacy', action='store_true')
    a = ap.parse_args(argv)
    if a.rotate:
        rotate(a.peer, rotate_master=not a.no_master and not a.peer)
        return 0
    if a.new_peer:
        h = a.new_peer if '.' in a.new_peer else f'srtm-lidar-{a.new_peer}.exe.xyz'
        d = load_peer_tokens(force=True)
        t = d.get(h) or generate()
        d[h] = t
        save_peer_tokens(d)
        if not MESH_TOKEN_PATH.exists():
            _write_secret(MESH_TOKEN_PATH, generate())
        print(f'{h}\nADMIN_TOKEN={t}\nMESH_TOKEN={mesh_token()}')
        return 0
    if a.purge_legacy:
        d = load_peer_tokens(force=True)
        d.pop(LEGACY_KEY, None)
        save_peer_tokens(d)
        print('legacy master forgotten')
        return 0
    print(json.dumps(status(), indent=1))
    return 0


if __name__ == '__main__':
    sys.exit(_main(sys.argv[1:]))
