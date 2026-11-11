"""public_segmentation — the Leaflet analysis UI on the public tier.

``GET /segmentation`` serves ``static/index.html`` with every ``/*PRIVATE*/ …
/*/PRIVATE*/`` block removed (private context toggles, evaluation panels,
admin-only dashboard links / pollers) and ``window.SRTM_PUBLIC = true`` injected.  The
endpoints that page needs are relayed to the full ``app.py`` on
``config.APP_URL`` (loopback) through an explicit allow-list (:data:`ROUTES`) with the
``X-Srtm-Public: 1`` header, which makes app.py (see its *public-tier relay*
section):

* disable every private context layer and force ``source=auto`` — the frozen v2.x
  products answer first (``product_segments``), live BEV only when the area
  has no finished product yet;
* cap the area at ``PUBLIC_MAX_AREA_KM2`` (5 km²);
* keep shares in ``data/shares/public/`` and async tasks tagged, so the
  tokenless UI can never list / load / poll anything the VM operator made;
* answer 404 on every admin path (defence in depth — the allow-list already
  never forwards them).

Every JSON body that comes back is additionally passed through
:func:`scrub` — keys or strings carrying a ``cells_api.FORBIDDEN_WORDS`` token
are dropped / masked — and ``tests/test_public_segmentation.py`` greps the
rendered page, this module and the docs section for the same word list.

Guards (per worker, same primitives as the tile endpoints): a token bucket per
client on the heavy POSTs and a concurrency gate so a burst of analyses cannot
saturate ``srv`` (which itself serialises heavy tasks through
``_TASK_SEMAPHORE``).
"""
from __future__ import annotations

import gzip
import json
import logging
import os
import re
import threading
import time
from pathlib import Path

from flask import Blueprint, Response, request

import config
import service_guard as sg

log = logging.getLogger('public_segmentation')
bp = Blueprint('public_segmentation', __name__)

PUBLIC_HDR = 'X-Srtm-Public'
UI_PATH = Path(__file__).resolve().parent / 'static' / 'index.html'
PRIVATE_OPEN, PRIVATE_CLOSE = '/*PRIVATE*/', '/*/PRIVATE*/'
UPSTREAM = config.APP_URL

#: (method, path-regex) pairs the public UI may reach on app.py.  Anything
#: else stays 404 on this tier — extend here, never with a catch-all.
ROUTES: tuple[tuple[str, str, str], ...] = (
    # analysis (async segment + sync elevation / terrain)
    ('POST', r'/api/v1/segment', 'heavy'),
    ('POST', r'/api/v1/elevation', 'heavy'),
    ('POST', r'/api/v1/terrain', 'heavy'),
    ('GET', r'/api/v1/segment/progress', 'light'),
    ('GET', r'/api/v1/segment/result', 'light'),
    ('POST', r'/api/v1/segment/abort', 'light'),
    # raster overlays (segment raster from cache / products; the rest read BEV directly)
    ('POST', r'/api/v1/segment/overlay', 'heavy'),
    ('POST', r'/api/v1/(dtm|lidar|ortho|cir|hansen)/overlay', 'heavy'),
    # exports
    ('POST', r'/api/v1/export/(geopackage|kml|mbtiles)', 'heavy'),
    ('GET', r'/api/v1/export/(geopackage|mbtiles)/download/[A-Za-z0-9\-]+', 'light'),
    # geometry upload / availability / attribution
    ('POST', r'/api/v1/parse-geometry', 'light'),
    ('GET', r'/api/v1/layers', 'light'),
    ('GET', r'/api/v1/attribution', 'light'),
    # shares (own namespace on app.py)
    ('GET', r'/api/v1/shares', 'light'),
    ('POST', r'/api/v1/share', 'light'),
    ('GET', r'/api/v1/share/[A-Za-z0-9_\-]+', 'light'),
    ('GET', r'/api/v1/share/[A-Za-z0-9_\-]+/download\.gpkg', 'heavy'),
    ('POST', r'/api/v1/share/[A-Za-z0-9_\-]+/rename', 'light'),
    # forestry tree service — KEYED: needs a reporter key we assigned
    # (data/reporter_keys.json; self-issued trk_ keys do not qualify)
    ('POST', r'/api/v[23]/trees(/by-polygons)?', 'keyed'),
    ('POST', r'/api/v[23]/changes/trees(/by-polygons)?', 'keyed'),
)
#: path prefixes public_api's 404 handler offers to :func:`relay`
RELAY_PREFIXES = ('/api/v1/', '/api/v2/', '/api/v3/')
KEYED_HDR = 'X-Srtm-Keyed'
_ROUTE_RE = [(m, re.compile('^' + p + '$'), kind) for m, p, kind in ROUTES]

_HOP = {'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization', 'te',
        'trailers', 'transfer-encoding', 'upgrade', 'host', 'content-length'}
#: never forwarded — app.py must see the relay as a public-tier request; any
#: operator credential a client might send is dropped here.
_STRIP_IN = _HOP | {'x-forwarded-for', 'x-forwarded-host', 'x-real-ip', 'authorization',
                    'x-operator-token', 'x-api-key', 'cookie', PUBLIC_HDR.lower(), 'x-srtm-tier',
                    'x-reporter-key', 'x-srtm-keyed'}

HEAVY_GATE = sg.Gate('segmentation_heavy', 2, 3.0, env='SEG_MAX_CONCURRENT')
HEAVY_RATE = sg.RateLimiter('segmentation_heavy', 0.5, 20, env='SEG_RATE_PER_S')   # 30/min sustained
LIGHT_RATE = sg.RateLimiter('segmentation_light', 10, 100, env='SEG_LIGHT_RATE_PER_S')
UPSTREAM_TIMEOUT = (5, float(os.environ.get('SEG_UPSTREAM_TIMEOUT_S', '900')))

_SESSION = None
_SESSION_LOCK = threading.Lock()


def is_primary() -> bool:
    """The UI + relay exist on the instance that serves the public tier: in
    single-VM mode always; in multi-VM mode only when this host is not a
    worker pointed at a remote director (``config.PRIMARY_URL`` empty)."""
    return not config.PRIMARY_URL


def _session():
    global _SESSION
    with _SESSION_LOCK:
        if _SESSION is None:
            import requests
            from requests.adapters import HTTPAdapter
            s = requests.Session()
            s.trust_env = False
            s.mount('http://', HTTPAdapter(pool_connections=4, pool_maxsize=32, max_retries=0))
            _SESSION = s
        return _SESSION


# --------------------------------------------------------------------------
# vocabulary scrub
# --------------------------------------------------------------------------
def _forbidden() -> tuple[str, ...]:
    import cells_api
    return cells_api.FORBIDDEN_WORDS


_WORD_RE = None
_TOKEN_SET: frozenset | None = None


def _compile():
    global _WORD_RE, _TOKEN_SET
    if _WORD_RE is None:
        words = _forbidden()
        _TOKEN_SET = frozenset(w.lower() for w in words)
        _WORD_RE = re.compile(r'(?<![\w])(?:' + '|'.join(re.escape(w) for w in words) + r')(?![\w])',
                              re.IGNORECASE)
    return _WORD_RE, _TOKEN_SET


def key_banned(key: str) -> bool:
    """True when any ``_``/``-``/space separated token of *key* is a banned word."""
    _, toks = _compile()
    return any(t in toks for t in re.split(r'[_\-\s]+', key.lower()) if t)


def scrub(obj):
    """Recursively drop banned keys and mask banned words inside strings."""
    rx, _ = _compile()
    if isinstance(obj, dict):
        return {k: scrub(v) for k, v in obj.items() if not (isinstance(k, str) and key_banned(k))}
    if isinstance(obj, list):
        return [scrub(v) for v in obj]
    if isinstance(obj, str):
        return rx.sub('…', obj) if rx.search(obj) else obj
    return obj


def scrub_json_bytes(body: bytes) -> bytes:
    try:
        return json.dumps(scrub(json.loads(body)), separators=(',', ':'), ensure_ascii=False).encode()
    except Exception:  # noqa: BLE001
        # unparsable → never leak the raw body
        return b'{"error":"upstream returned a non-JSON body"}'


# --------------------------------------------------------------------------
# the page
# --------------------------------------------------------------------------
_page_cache: dict = {}
_page_lock = threading.Lock()


def render_page(src: str | None = None) -> str:
    """``static/index.html`` minus the ``/*PRIVATE*/`` blocks, plus the public flag."""
    if src is None:
        src = UI_PATH.read_text(encoding='utf-8')
    out, pos = [], 0
    while True:
        a = src.find(PRIVATE_OPEN, pos)
        if a < 0:
            out.append(src[pos:])
            break
        b = src.find(PRIVATE_CLOSE, a)
        if b < 0:
            raise ValueError('unterminated /*PRIVATE*/ block in static/index.html')
        out.append(src[pos:a])
        pos = b + len(PRIVATE_CLOSE)
    html = ''.join(out)
    flag = ('<script>window.SRTM_PUBLIC=true;window.SRTM_PUBLIC_MAX_KM2='
            + json.dumps(float(os.environ.get('PUBLIC_MAX_AREA_KM2', '5'))) + ';</script>')
    i = html.find('<head>')
    html = html[:i + 6] + flag + html[i + 6:] if i >= 0 else flag + html
    return html


def _page() -> str:
    try:
        mt = UI_PATH.stat().st_mtime
    except OSError:
        return '<h1>UI unavailable</h1>'
    with _page_lock:
        hit = _page_cache.get('p')
        if hit and hit[0] == mt:
            return hit[1]
        html = render_page()
        _page_cache['p'] = (mt, html)
        return html


@bp.route('/segmentation')
@bp.route('/segmentation.html')
def segmentation_page():
    if not is_primary():
        from flask import abort
        abort(404)
    r = Response(_page(), mimetype='text/html')
    r.headers['Cache-Control'] = 'public, max-age=300'
    r.headers['X-Robots-Tag'] = 'noindex'
    return r


# --------------------------------------------------------------------------
# relay
# --------------------------------------------------------------------------
def route_kind(method: str, path: str) -> str | None:
    for m, rx, kind in _ROUTE_RE:
        if m == method and rx.match(path):
            return kind
    return None


def _limited(resp_json: dict, retry: float) -> Response:
    r = Response(json.dumps(resp_json), status=429, mimetype='application/json')
    r.headers['Retry-After'] = str(int(retry) + 1)
    r.headers['Cache-Control'] = 'no-store'
    return r


def keyed_holder() -> str | None:
    """Holder of an operator-assigned reporter key on this request
    (``X-Reporter-Key`` / ``Authorization: Bearer`` / ``?reporter_key=``), else None."""
    import hmac
    import public_flags as pf
    k = request.headers.get('X-Reporter-Key') or request.args.get('reporter_key') or ''
    if not k:
        a = request.headers.get('Authorization') or ''
        k = a[7:] if a.lower().startswith('bearer ') else ''
    k = k.strip()
    if not k:
        return None
    for kk, holder in pf.reporter_keys().items():
        if hmac.compare_digest(kk.encode(), k.encode()):
            return holder
    return None


def relay():
    """Forward the current request to app.py if (method, path) is allow-listed.
    Returns ``None`` when it is not (the caller answers 404)."""
    if not is_primary():
        return None
    kind = route_kind(request.method, request.path)
    if kind is None:
        return None
    key = sg.client_key(request)
    holder = None
    if kind == 'keyed':
        holder = keyed_holder()
        if holder is None:
            r = Response(json.dumps({'error': 'reporter key required',
                                     'detail': 'send the reporter key we assigned to your organisation as '
                                               'X-Reporter-Key (self-issued keys do not open the tree service)'}),
                         status=401, mimetype='application/json')
            r.headers['Cache-Control'] = 'no-store'
            return r
        kind, key = 'heavy', 'k:' + holder
    ok, _left, retry = (HEAVY_RATE if kind == 'heavy' else LIGHT_RATE).take(key)
    if not ok:
        return _limited({'error': 'rate_limited', 'detail': 'too many analysis requests from this client'}, retry)
    url = UPSTREAM + request.path
    if request.query_string:
        url += '?' + request.query_string.decode('latin-1')
    headers = {k: v for k, v in request.headers.items() if k.lower() not in _STRIP_IN}
    headers[PUBLIC_HDR] = '1'
    headers['X-Public-Client'] = key
    if holder:
        headers[KEYED_HDR] = holder
        if 'reporter_key=' in url:       # never forward / log the key itself
            url = re.sub(r'([?&])reporter_key=[^&]*&?', r'\1', url).rstrip('?&')
    # The relay carries the client IP so app.py never treats a relayed
    # request as loopback (its loopback rule is what gates mutating routes).
    headers['X-Forwarded-For'] = key
    headers['X-Forwarded-Proto'] = request.headers.get('X-Forwarded-Proto', 'https')
    headers['X-Forwarded-Host'] = request.headers.get('X-Forwarded-Host') or request.host
    headers['Accept-Encoding'] = 'identity'          # we need to read + scrub JSON bodies
    # One loopback connection per relay: srv's gthread workers drop idle keep-alive
    # sockets after 2 s, and a pooled connection reused right then dies with
    # "Connection reset by peer" → a spurious 502 that fails the UI's whole poll loop.
    headers['Connection'] = 'close'
    body = request.get_data() if request.method == 'POST' else None
    gate = HEAVY_GATE if kind == 'heavy' else None
    t0 = time.time()
    try:
        if gate is not None:
            gate.__enter__()
        try:
            up = _session().request(request.method, url, headers=headers, data=body, stream=True,
                                    timeout=UPSTREAM_TIMEOUT, allow_redirects=False)
        finally:
            if gate is not None:
                gate.__exit__(None, None, None)
    except sg.Overloaded:
        raise
    except Exception as e:  # noqa: BLE001
        log.warning('segmentation relay %s %s: %s', request.method, request.path, e)
        return Response(json.dumps({'error': 'analysis backend unavailable'}), status=502,
                        mimetype='application/json')
    ct = (up.headers.get('Content-Type') or '').split(';')[0].strip()
    out_headers = [(k, v) for k, v in up.raw.headers.items()
                   if k.lower() not in _HOP | {'set-cookie', 'content-encoding', 'etag'}]
    out_headers.append(('Cache-Control', 'no-store'))
    if ct in ('application/json', 'application/geo+json'):
        raw = up.raw.read(decode_content=True)
        up.close()
        if up.headers.get('Content-Encoding') == 'gzip' and raw[:2] == b'\x1f\x8b':
            raw = gzip.decompress(raw)
        data = scrub_json_bytes(raw)
        out_headers = [(k, v) for k, v in out_headers if k.lower() != 'content-length']
        log.info('segmentation relay %s %s → %d (%d KB, %.1fs)', request.method, request.path,
                 up.status_code, len(data) // 1024, time.time() - t0)
        return Response(data, status=up.status_code, headers=out_headers, mimetype='application/json')

    def _gen():
        try:
            for chunk in up.raw.stream(64 * 1024, decode_content=False):
                if chunk:
                    yield chunk
        finally:
            up.close()
    return Response(_gen(), status=up.status_code, headers=out_headers, direct_passthrough=True)
