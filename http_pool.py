"""Connection reuse for module-level ``requests.get/post/put`` calls.

``requests.get(...)`` builds a throw-away ``Session`` per call, so every
director fanout / peer push / heartbeat pays a fresh TCP + TLS handshake
(~6 KB on the wire through the exe.dev proxy). At ~10k requests/h on
the primary that is tens of MB/h of pure handshake overhead — a large
slice of the bandwidth budget once the payloads themselves are slim.

``install()`` swaps ``requests.api.request`` for a version that routes
through one process-wide pooled ``Session`` (urllib3 pools are
thread-safe; the Session carries no cookies/auth so sharing is safe).
Servers that close idle keep-alive connections are handled by urllib3
transparently (stale connection → reconnect). ``requests.Session``
used explicitly elsewhere is untouched.
"""
from __future__ import annotations

import threading

_installed = False
_lock = threading.Lock()
_session = None


def session():
    global _session
    if _session is None:
        with _lock:
            if _session is None:
                import requests
                from requests.adapters import HTTPAdapter
                s = requests.Session()
                s.trust_env = True
                ad = HTTPAdapter(pool_connections=64, pool_maxsize=16,
                                 max_retries=0, pool_block=False)
                s.mount('https://', ad)
                s.mount('http://', ad)
                _session = s
    return _session


def _inject_umfeld_auth(url, kwargs) -> None:
    """Add ``Authorization: Bearer`` for the token-gated cadastre upstream
    (example.invalid/k/…) when the caller did not set one. One hook here
    covers austria_processor / cadastre / app alike."""
    try:
        import umfeld
        if not isinstance(url, str) or not umfeld.is_umfeld_url(url):
            return
        h = kwargs.get('headers')
        if h and any(k.lower() in ('authorization', 'x-api-key') for k in h):
            return
        auth = umfeld.headers()
        if not auth:
            # Fail loudly as a *transport* error rather than let the /k
            # tier answer 404 (which the processor would read as "KG has
            # no cadastre" and bake an empty parcel layer).
            import requests
            raise requests.ConnectionError(
                'umfeld token missing (data/umfeld_token) — cannot reach ' + url)
        kwargs['headers'] = dict(h or {}, **auth)
    except ImportError:
        pass


def _inject_tier_auth(url, kwargs) -> None:
    """Every call to a cluster host (``srtm-lidar-*.exe.xyz``) carries the
    cluster admin token as ``X-Admin-Token`` unless the caller set auth.

    * peer → primary goes through the token-gated ``/k`` tier;
    * director/shadow → peer and peer → peer hit ``:8000`` of a VM that
      answers *only* to token holders (non-primary peers have no owner-port
      bypass — see ``app._enforce_private_only``). Dozens of call sites
      (status polls, zenodo_lock, manifest push, cache_manifest sync, HA
      snapshots, …) — one hook, so no call can silently lose contact."""
    try:
        if not isinstance(url, str) or 'srtm-lidar-' not in url:
            return
        from urllib.parse import urlsplit
        import api_tiers
        u = urlsplit(url)
        if not api_tiers.is_cluster_host(u.hostname):
            return
        h = kwargs.get('headers') or {}
        if any(k.lower() in ('x-admin-token', 'authorization', 'x-api-key') for k in h):
            return
        from pathlib import Path
        tok = Path('data/admin_token').read_text().strip()
        if tok:
            kwargs['headers'] = dict(h, **{'X-Admin-Token': tok})
    except Exception:
        pass


def install() -> None:
    global _installed
    if _installed:
        return
    try:
        import requests
        import requests.api as _api
    except Exception:
        return

    def request(method, url, **kwargs):
        _inject_umfeld_auth(url, kwargs)
        _inject_tier_auth(url, kwargs)
        return session().request(method=method, url=url, **kwargs)

    _api.request = request
    requests.request = request   # ``requests.request`` is bound at import time
    # requests.get/post/... call ``request`` by name inside requests.api,
    # so rebinding ``_api.request`` above already routes them. Re-export
    # for callers that imported the helpers before install() ran.
    def get(url, params=None, **kw):
        kw.setdefault('allow_redirects', True)
        return request('get', url, params=params, **kw)

    def head(url, **kw):
        kw.setdefault('allow_redirects', False)
        return request('head', url, **kw)

    def post(url, data=None, json=None, **kw):
        return request('post', url, data=data, json=json, **kw)

    def put(url, data=None, **kw):
        return request('put', url, data=data, **kw)

    def patch(url, data=None, **kw):
        return request('patch', url, data=data, **kw)

    def delete(url, **kw):
        return request('delete', url, **kw)

    for fn in (get, head, post, put, patch, delete):
        setattr(_api, fn.__name__, fn)
        setattr(requests, fn.__name__, fn)
    _installed = True
