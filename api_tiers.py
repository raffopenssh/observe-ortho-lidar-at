"""api_tiers — the two tiers of this service (mirrors umfeld-at's split).

PUBLIC  https://example.invalid/            ``public_api.py`` (own
        gunicorn, port 8100 → becomes the exe.dev default port). Parcel-free,
        bbox/point-keyed landscape data. No token.
PRIVATE https://example.invalid/k/…         everything ``app.py`` has,
        1:1 the old paths with ``/k`` prepended, bearer-token gated, proxied
        by the public app to 127.0.0.1:8000. Unadvertised: no/invalid token →
        the same 404 as an unknown route.

Shared here so both apps agree on tokens and on the prefix:

* ``PRIVATE_PREFIX`` = ``/k``.  ``app.py`` also strips a leading ``/k`` itself
  (``StripPrefixMiddleware``) so ``…/k/api/v1/x`` works whether the request
  reaches :8000 directly (before the port switch) or via the public proxy.
* Valid tokens = the cluster admin token (``data/admin_token`` — every peer
  has it, so peers reach the director through ``/k`` unchanged) plus any
  extra client tokens in ``data/api_tokens`` (one per line, ≥16 chars; one
  per client so one can be rotated alone).
* Transport: ``Authorization: Bearer``, ``X-API-Key``, ``X-Admin-Token``,
  ``?token=`` (download URLs) or the ``srtm_k`` cookie (dashboards: open any
  page once with ``?token=`` and the cookie is set).
* ``private_only()`` — ``data/private_only`` exists → app.py answers 404 to
  every non-loopback request without a valid token (flip after the fleet
  has moved to ``/k``). Until then :8000 behaves as before.
"""
from __future__ import annotations

import hmac
import os
import time
from pathlib import Path

PRIVATE_PREFIX = '/k'
PUBLIC_HOST = 'example.invalid'
PUBLIC_BASE = f'https://{PUBLIC_HOST}'
PRIVATE_BASE = PUBLIC_BASE + PRIVATE_PREFIX
PRIVATE_UPSTREAM = os.environ.get('SRTM_PRIVATE_UPSTREAM', 'http://127.0.0.1:8000')

ADMIN_TOKEN_PATH = Path('data/admin_token')
API_TOKENS_PATH = Path('data/api_tokens')
PRIVATE_ONLY_FLAG = Path('data/private_only')
COOKIE = 'srtm_k'

# --- /k retirement hooks (docs/ne-public-api.md §7) -------------------------
# Landscape routes of the hidden tier that have a public, cell-keyed
# replacement in cells_api.  When ``data/k_landscape_off`` exists they answer
# 410 even with a valid token.  Director / processor / admin / credential /
# zenodo-lock / manifest / net_stats / process.* routes are NOT listed — peers
# need them.  Prefixes are matched against the path *after* ``/k``.
K_LANDSCAPE_OFF_FLAG = Path('data/k_landscape_off')
PRIVATE_ROUTES_LANDSCAPE = (
    '/api/v1/parcel/', '/api/v1/parcels/', '/api/v1/query/parcels', '/api/v1/query/nature',
    '/api/v1/query/compound', '/api/v1/query', '/api/v1/lookup', '/api/v1/cadastre/',
    '/api/v1/flags', '/llm/kg/', '/llm/kgs', '/llm/manifest.json',
)
# per-KG sub-resources under /api/v1/kg/<code>/… that are retired (profile,
# buildings, new_buildings, infrastructure); /api/v1/kg/<code> itself stays
# (public tier has its own).
PRIVATE_KG_SUBROUTES_LANDSCAPE = ('profile', 'buildings', 'new_buildings', 'infrastructure', 'parcels')
LANDSCAPE_GONE_SEE = '/api/v1/docs/llm.txt#cells'


def k_landscape_off() -> bool:
    return K_LANDSCAPE_OFF_FLAG.exists()


def landscape_route_retired(path: str) -> bool:
    """``path`` = request path with or without the ``/k`` prefix."""
    if path.startswith(PRIVATE_PREFIX + '/'):
        path = path[len(PRIVATE_PREFIX):]
    if any(path.startswith(pfx) or path == pfx.rstrip('/') for pfx in PRIVATE_ROUTES_LANDSCAPE):
        return True
    if path.startswith('/api/v1/kg/'):
        rest = path[len('/api/v1/kg/'):].split('/', 2)
        if len(rest) >= 2 and rest[1] in PRIVATE_KG_SUBROUTES_LANDSCAPE:
            return True
    return False

_cache = {'t': 0.0, 'tokens': frozenset()}


def tokens() -> frozenset:
    """All accepted tokens; files re-read at most every 5 s."""
    now = time.time()
    if now - _cache['t'] < 5:
        return _cache['tokens']
    toks = set()
    for p in (ADMIN_TOKEN_PATH, API_TOKENS_PATH):
        try:
            for line in p.read_text().splitlines():
                line = line.strip()
                if len(line) >= 16 and not line.startswith('#'):
                    toks.add(line)
        except FileNotFoundError:
            pass
        except Exception:
            pass
    _cache['tokens'] = frozenset(toks)
    _cache['t'] = now
    return _cache['tokens']


def token_valid(tok: str | None) -> bool:
    if not tok:
        return False
    return any(hmac.compare_digest(tok, t) for t in tokens())


def request_token(req) -> str | None:
    """Extract the presented token from a Flask request."""
    a = req.headers.get('Authorization') or ''
    if len(a) > 7 and a[:7].lower() == 'bearer ':
        return a[7:].strip()
    for h in ('X-API-Key', 'X-Admin-Token'):
        v = req.headers.get(h)
        if v:
            return v.strip()
    v = req.args.get('token') or req.args.get('admin_token')
    if v:
        return v.strip()
    return req.cookies.get(COOKIE) or req.cookies.get('admin_token')


def private_only() -> bool:
    return PRIVATE_ONLY_FLAG.exists()


# --- cluster hosts / peer gate ----------------------------------------------
# Every VM of the cluster is ``srtm-lidar-<id>.exe.xyz`` (primary = ``-at``).
# ``http_pool._inject_tier_auth`` adds the admin token to *every* outbound
# call to such a host, because non-primary peers answer their ``:8000`` only
# to token holders (``app._enforce_private_only`` → ``peer_gate_response``):
# the owner-port bypass that keeps the primary's dashboards usable without
# ``?token=`` does NOT apply on peers — they must never show the legacy app
# / docs, only a note that they are peers of the primary + a redirect.
CLUSTER_HOST_SUFFIX = '.exe.xyz'
CLUSTER_HOST_PREFIX = 'srtm-lidar-'

# GET paths a peer answers without a token (liveness only; no data).
PEER_OPEN_GET_PATHS = frozenset({'/api/v1/ping'})


def is_cluster_host(hostname: str | None) -> bool:
    h = (hostname or '').lower().rstrip('.')
    return h.startswith(CLUSTER_HOST_PREFIX) and h.endswith(CLUSTER_HOST_SUFFIX)


PEER_NOTE_HTML = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<title>{host} — processing peer of srtm-lidar-at</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow">
<noscript><meta http-equiv="refresh" content="{secs};url={landing}"></noscript>
{style}
<style>
/* peer note: subtle motion only (progress rule, cell-wave, fade-in, cursor) */
.bar{{position:fixed;left:0;top:0;height:2px;width:100%;background:var(--rule);z-index:9}}
.bar i{{display:block;height:100%;width:0;background:var(--acc);
  animation:fill {secs}s cubic-bezier(.4,0,.2,1) forwards}}
@keyframes fill{{to{{width:100%}}}}
.in{{animation:in .5s ease-out both}}
.in:nth-of-type(2){{animation-delay:.08s}}.in:nth-of-type(3){{animation-delay:.16s}}
.in:nth-of-type(4){{animation-delay:.24s}}.in:nth-of-type(5){{animation-delay:.32s}}
@keyframes in{{from{{opacity:0;transform:translateY(4px)}}to{{opacity:1;transform:none}}}}
.cells{{display:block;width:100%;height:28px;margin:.4rem 0 1.2rem;overflow:visible}}
.cells use{{fill:var(--rule);stroke:none;animation:wave 2.6s ease-in-out infinite}}
@keyframes wave{{0%,100%{{fill:var(--rule);transform:translateY(0)}}
  18%{{fill:var(--acc);transform:translateY(-2px)}}36%{{fill:var(--rule);transform:translateY(0)}}}}
.cur::after{{content:"▍";color:var(--acc);margin-left:.1em;animation:blink 1.1s steps(1) infinite}}
@keyframes blink{{50%{{opacity:0}}}}
#n{{color:var(--fg);font-weight:700}}
@media(prefers-reduced-motion:reduce){{.bar i,.in,.cells use,.cur::after{{animation:none}}.bar i{{width:100%}}}}
</style></head><body>
<div class="bar" aria-hidden="true"><i></i></div>
<h1 class="in">{host} — processing peer <span class="stage">worker</span></h1>
<svg class="cells in" viewBox="0 0 790 28" aria-hidden="true" preserveAspectRatio="xMinYMid slice">
<defs><polygon id="h" points="9,0 18,5 18,15 9,20 0,15 0,5"/></defs>
<use href="#h" x="0" y="0" style="animation-delay:0.00s"/><use href="#h" x="22" y="6" style="animation-delay:0.11s"/><use href="#h" x="44" y="0" style="animation-delay:0.22s"/><use href="#h" x="66" y="6" style="animation-delay:0.33s"/><use href="#h" x="88" y="0" style="animation-delay:0.44s"/><use href="#h" x="110" y="6" style="animation-delay:0.55s"/><use href="#h" x="132" y="0" style="animation-delay:0.66s"/><use href="#h" x="154" y="6" style="animation-delay:0.77s"/><use href="#h" x="176" y="0" style="animation-delay:0.88s"/><use href="#h" x="198" y="6" style="animation-delay:0.99s"/><use href="#h" x="220" y="0" style="animation-delay:1.10s"/><use href="#h" x="242" y="6" style="animation-delay:1.21s"/><use href="#h" x="264" y="0" style="animation-delay:1.32s"/><use href="#h" x="286" y="6" style="animation-delay:1.43s"/><use href="#h" x="308" y="0" style="animation-delay:1.54s"/><use href="#h" x="330" y="6" style="animation-delay:1.65s"/><use href="#h" x="352" y="0" style="animation-delay:1.76s"/><use href="#h" x="374" y="6" style="animation-delay:1.87s"/><use href="#h" x="396" y="0" style="animation-delay:1.98s"/><use href="#h" x="418" y="6" style="animation-delay:2.09s"/><use href="#h" x="440" y="0" style="animation-delay:2.20s"/><use href="#h" x="462" y="6" style="animation-delay:2.31s"/><use href="#h" x="484" y="0" style="animation-delay:2.42s"/><use href="#h" x="506" y="6" style="animation-delay:2.53s"/><use href="#h" x="528" y="0" style="animation-delay:2.64s"/><use href="#h" x="550" y="6" style="animation-delay:2.75s"/><use href="#h" x="572" y="0" style="animation-delay:2.86s"/><use href="#h" x="594" y="6" style="animation-delay:2.97s"/><use href="#h" x="616" y="0" style="animation-delay:3.08s"/><use href="#h" x="638" y="6" style="animation-delay:3.19s"/><use href="#h" x="660" y="0" style="animation-delay:3.30s"/><use href="#h" x="682" y="6" style="animation-delay:3.41s"/><use href="#h" x="704" y="0" style="animation-delay:3.52s"/><use href="#h" x="726" y="6" style="animation-delay:3.63s"/><use href="#h" x="748" y="0" style="animation-delay:3.74s"/><use href="#h" x="770" y="6" style="animation-delay:3.85s"/></svg>
<p class="in">This VM is one of the worker nodes of the <strong>srtm-lidar-at</strong> cluster: it segments
Katastralgemeinden from LiDAR + satellite data and uploads the products. It has no public interface
of its own — the landscape API, documentation and dashboards live on the primary.</p>
<pre class="in">→ <a href="{landing}">{landing}</a>   <span class="cov">redirecting in <span id="n">{secs}</span> s</span><span class="cur"></span></pre>
<div class="in">
<h2>Primary</h2>
<table><tr><th>Need</th><th>Where</th></tr>
<tr><td>Landscape API + docs</td><td><a href="{landing}">{landing}</a> · <a href="{landing}api/v1/docs/llm.txt">/api/v1/docs/llm.txt</a></td></tr>
<tr><td>Coverage</td><td><a href="{landing}api/v1/status">/api/v1/status</a></td></tr>
<tr><td>Licences &amp; attribution</td><td><a href="{landing}api/v1/license">/api/v1/license</a></td></tr></table>
<p class="muted">Operators: peer endpoints answer only with the cluster token
(<code>X-Admin-Token</code> / <code>?token=</code>); without it this is all you get.</p>
</div>
<script>
(function(){{var n={secs},el=document.getElementById('n'),t0=Date.now();
function tick(){{var left=Math.max(0,n-(Date.now()-t0)/1000);el.textContent=Math.ceil(left);
if(left<=0){{location.replace({landing_js});return;}}setTimeout(tick,200);}}tick();}})();
</script>
</body></html>
"""

PEER_NOTE_REDIRECT_S = 15


def _page_style() -> str:
    try:
        from public_docs import PAGE_STYLE
        return PAGE_STYLE
    except Exception:
        return '<style>body{font:14px/1.6 ui-monospace,monospace;max-width:84ch;margin:3rem auto;padding:0 1.25rem}</style>'


def peer_note_html(host: str | None = None) -> str:
    import json as _json
    landing = PUBLIC_BASE + '/'
    return PEER_NOTE_HTML.format(landing=landing, landing_js=_json.dumps(landing),
                                 host=host or 'This VM', style=_page_style(),
                                 secs=PEER_NOTE_REDIRECT_S)


class StripPrefixMiddleware:
    """WSGI: ``/k/foo`` → ``/foo`` (and ``/k`` → ``/``), recording the fact
    in ``environ['srtm.tier'] = 'private'`` so handlers can tell."""

    def __init__(self, app, prefix=PRIVATE_PREFIX):
        self.app = app
        self.prefix = prefix

    def __call__(self, environ, start_response):
        p = environ.get('PATH_INFO') or ''
        if p == self.prefix or p.startswith(self.prefix + '/'):
            environ['PATH_INFO'] = p[len(self.prefix):] or '/'
            environ['SCRIPT_NAME'] = (environ.get('SCRIPT_NAME') or '') + self.prefix
            environ['srtm.tier'] = 'private'
        return self.app(environ, start_response)
