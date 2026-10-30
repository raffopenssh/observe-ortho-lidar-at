"""Connection reuse for module-level ``requests.get/post/put`` calls.

``requests.get(...)`` builds a throw-away ``Session`` per call, so every
poll / push / heartbeat pays a fresh TCP + TLS handshake.  ``install()``
swaps ``requests.api.request`` for a version that routes through one
process-wide pooled ``Session`` (urllib3 pools are thread-safe; the
Session carries no cookies/auth so sharing is safe).  ``requests.Session``
used explicitly elsewhere is untouched.

Multi-VM only: a request to ``config.PRIMARY_URL`` carries
``X-Operator-Token`` (``config.operator_token()``) when the caller did not
set one.  In single-VM mode (``PRIMARY_URL`` empty) no header is ever added.
"""
from __future__ import annotations

import threading

_installed = False
_lock = threading.Lock()
_session = None

USER_AGENT = "observe-ortho-lidar-at/1.0 (+https://github.com/raffopenssh/observe-ortho-lidar-at)"


def session():
    global _session
    if _session is None:
        with _lock:
            if _session is None:
                import requests
                from requests.adapters import HTTPAdapter
                s = requests.Session()
                s.trust_env = True
                s.headers["User-Agent"] = USER_AGENT
                ad = HTTPAdapter(pool_connections=64, pool_maxsize=16,
                                 max_retries=0, pool_block=False)
                s.mount('https://', ad)
                s.mount('http://', ad)
                _session = s
    return _session


def _inject_operator_token(url, kwargs) -> None:
    """``X-Operator-Token`` toward ``config.PRIMARY_URL`` only (multi-VM)."""
    try:
        import config
        base = config.PRIMARY_URL
        if not base or not isinstance(url, str) or not url.startswith(base + "/") and url != base:
            return
        h = kwargs.get('headers') or {}
        if any(k.lower() in ('x-operator-token', 'authorization') for k in h):
            return
        tok = config.operator_token()
        if tok:
            kwargs['headers'] = dict(h, **{'X-Operator-Token': tok})
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
        _inject_operator_token(url, kwargs)
        return session().request(method=method, url=url, **kwargs)

    _api.request = request
    requests.request = request   # ``requests.request`` is bound at import time

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
