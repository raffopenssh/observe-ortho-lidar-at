"""public_api — the public, parcel-free tier of srtm-lidar-at.

Runs as its own gunicorn (``srv_public.service``, port 8100; the exe.dev
default port is switched to it so it answers on https://example.invalid/).

    PUBLIC   /api/v1/…  /llm.txt  /tiles/…    landscape_query.py — bbox / point /
                                              KG-code keyed, nothing cadastre-derived,
                                              index + grid25 fast path only (no Zenodo)
    PRIVATE  /k/…                              token-gated reverse proxy to the full
                                              app.py on 127.0.0.1:8000 (api_tiers.py);
                                              404 without a valid token

Never imports ``app.py`` (that would start a second director / sync / index
thread set). ``hillshade_tiles`` is reused as-is.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import time

from flask import Flask, Response, g, jsonify, request

import api_tiers as tiers
import landscape_query as lq
import mem_lru
import public_docs as docs
import service_guard as sg

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s')
log = logging.getLogger('public_api')

app = Flask(__name__, static_folder=None)
try:
    import hillshade_tiles as _hs
    app.register_blueprint(_hs.bp)              # /tiles/hillshade|slope/z/x/y.png
except Exception as e:  # noqa: BLE001
    log.warning('hillshade tiles unavailable: %s', e)
try:
    import cells_api as _cells                   # /api/v1/cells*, /api/v1/kg/<code>/cells (docs/ne-public-api.md)
    app.register_blueprint(_cells.cells_bp)
except Exception as e:  # noqa: BLE001
    _cells = None
    log.warning('cells api unavailable: %s', e)
try:
    import public_flags as _flags                # /api/v1/flags* (docs/public-flags.md)
    app.register_blueprint(_flags.flags_bp)
except Exception as e:  # noqa: BLE001
    _flags = None
    log.warning('flags api unavailable: %s', e)

try:
    import public_segmentation as _seg           # /segmentation UI + allow-listed relay to app.py
    app.register_blueprint(_seg.bp)
except Exception as e:  # noqa: BLE001
    _seg = None
    log.warning('segmentation ui unavailable: %s', e)

import attributions as _attr
ATTRIBUTION = _attr.attribution_short() + ' · Derived landscape segmentation (c) srtm-lidar-at, CC BY 4.0.'
LICENSE_URL = _attr.OUTPUT_LICENSE_URL
# HTTP headers must be latin-1: fold the typographic characters.
ATTRIBUTION_HDR = ATTRIBUTION.replace('–', '-').replace('·', ';').replace('©', '(c)').encode('latin-1', 'replace').decode('latin-1')
MAX_AGE = 120

_SESSION = None


def _session():
    global _SESSION
    if _SESSION is None:
        import requests
        from requests.adapters import HTTPAdapter
        s = requests.Session()
        s.trust_env = False
        s.mount('http://', HTTPAdapter(pool_connections=8, pool_maxsize=64, max_retries=0))
        _SESSION = s
    return _SESSION


# --------------------------------------------------------------------------
# response helpers
# --------------------------------------------------------------------------
def _json(payload, status=200, max_age=MAX_AGE, etag=None):
    body = json.dumps(payload, separators=(',', ':'), ensure_ascii=False,
                      indent=1 if request.args.get('pretty') else None).encode()
    resp = Response(body, status=status, mimetype='application/json')
    uh = payload.get('universe_hash') if isinstance(payload, dict) else None
    if uh:
        resp.headers['X-KG-Universe-Hash'] = uh
    if status == 200 and request.method == 'GET':
        etag = etag or ('W/"' + hashlib.md5(body).hexdigest()[:20] + '"')
        resp.headers['ETag'] = etag
        resp.headers['Cache-Control'] = f'public, max-age={max_age}'
        if request.headers.get('If-None-Match') == etag:
            return Response(status=304, headers={'ETag': etag})
    else:
        resp.headers['Cache-Control'] = 'no-store'
    return resp


def _err(msg, status=400, **extra):
    return _json(dict({'error': msg}, **extra), status)


def _not_found():
    r = _json({'error': 'not_found', 'docs': '/api/v1/docs/llm.txt'}, 404)
    return r


@app.after_request
def _common_headers(resp):
    p = request.path or ''
    if not p.startswith(tiers.PRIVATE_PREFIX):
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Headers'] = 'Content-Type, If-None-Match, Authorization'
        resp.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
        resp.headers.setdefault('X-Data-Attribution', ATTRIBUTION_HDR)
        resp.headers.setdefault('Link', f'<{LICENSE_URL}>; rel="license"')
        # gzip JSON/text/binary ≥ 1 KiB (not h3b — already deflated)
        try:
            ct = (resp.content_type or '').split(';')[0]
            if (request.method in ('GET', 'POST') and resp.status_code == 200 and not resp.direct_passthrough
                    and ct in ('application/json', 'text/plain', 'text/html', 'text/csv',
                               'application/geo+json', 'application/octet-stream')
                    and 'gzip' in (request.headers.get('Accept-Encoding') or '')
                    and not resp.headers.get('Content-Encoding')):
                data = resp.get_data()
                if len(data) >= 1024:
                    # level 6 for the usual small bodies; level 1 past 512 KiB (cells
                    # JSON / columns): ~3× faster, ~10 % larger, CPU is the budget here
                    resp.set_data(gzip.compress(data, 6 if len(data) < 512 * 1024 else 1))
                    resp.headers['Content-Encoding'] = 'gzip'
                    resp.headers.add('Vary', 'Accept-Encoding')
        except Exception:  # noqa: BLE001
            pass
    return resp


@app.before_request
def _preflight():
    if request.method == 'OPTIONS' and not (request.path or '').startswith(tiers.PRIVATE_PREFIX):
        return Response(status=204)
    g.t0 = time.time()
    sg.WATCHDOG.check()          # ≤ 1 RSS sample / 5 s / worker; sheds caches or recycles (service_guard)
    return None


@app.errorhandler(sg.Overloaded)
def _overloaded(e):
    r = _json({'error': 'overloaded', 'detail': 'retry after the indicated delay'}, 503)
    r.headers['Retry-After'] = str(e.retry_after)
    r.headers['Cache-Control'] = 'no-store'
    return r


def _guard_dict() -> dict:
    """Per-worker resource picture (this is one of two gunicorn workers)."""
    d = {'watchdog': sg.WATCHDOG.stats(), 'cgroup': sg.cgroup_memory(),
         'caches': mem_lru.stats_all(), 'caches_total_mb': mem_lru.total_mb()}
    try:
        import hillshade_tiles as _hs
        d['tiles'] = _hs.stats()
    except Exception:  # noqa: BLE001
        pass
    return d


@app.route('/api/v1/health')
def health():
    """Cheap liveness/readiness probe — no index or store access. 200 while the
    worker is healthy, 503 once the memory watchdog has flagged it for recycle
    (so the healthcheck timer / any LB stops routing to it)."""
    wd = sg.WATCHDOG
    wd.check(force=True)
    body = {'ok': not wd.recycling, 'rss_mb': round(wd.last_rss_mb), 'pid': os.getpid(),
            'caches_mb': mem_lru.total_mb(), 'ts': int(time.time())}
    r = _json(body, 200 if body['ok'] else 503, max_age=0)
    r.headers['Cache-Control'] = 'no-store'
    return r


# --------------------------------------------------------------------------
# PRIVATE tier: /k/… → 127.0.0.1:8000 with the prefix kept (app.py strips it)
# --------------------------------------------------------------------------
_HOP = {'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization', 'te',
        'trailers', 'transfer-encoding', 'upgrade', 'host', 'content-length'}


@app.route(tiers.PRIVATE_PREFIX, defaults={'path': ''},
           methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD', 'OPTIONS'])
@app.route(tiers.PRIVATE_PREFIX + '/<path:path>',
           methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD', 'OPTIONS'])
def private_tier(path):
    tok = tiers.request_token(request)
    if not tiers.token_valid(tok) and not tiers.process_txt_pwd_ok(request):
        return _not_found()
    if tiers.k_landscape_off() and tiers.landscape_route_retired(request.path):
        return _json({'gone': True, 'see': tiers.LANDSCAPE_GONE_SEE}, 410)
    url = tiers.PRIVATE_UPSTREAM + request.path         # keep /k — app.py strips it
    if request.query_string:
        url += '?' + request.query_string.decode('latin-1')
    headers = {k: v for k, v in request.headers.items() if k.lower() not in _HOP}
    # app.py's loopback bypass requires X-Forwarded-For to be absent → always set it,
    # so the upstream applies its own admin-token checks to the forwarded headers.
    xff = request.headers.get('X-Forwarded-For') or request.remote_addr or '0.0.0.0'
    headers['X-Forwarded-For'] = xff
    headers['X-Forwarded-Proto'] = request.headers.get('X-Forwarded-Proto', 'https')
    headers['X-Srtm-Tier'] = 'private'
    # requests would add its own Accept-Encoding: gzip and we stream raw bytes →
    # only ask upstream for what the client can decode.
    headers['Accept-Encoding'] = request.headers.get('Accept-Encoding') or 'identity'
    body = request.get_data() if request.method in ('POST', 'PUT', 'PATCH', 'DELETE') else None
    try:
        up = _session().request(request.method, url, headers=headers, data=body,
                                stream=True, timeout=(5, 600), allow_redirects=False)
    except Exception as e:  # noqa: BLE001
        log.warning('private tier upstream error %s %s: %s', request.method, request.path, e)
        return _json({'error': 'upstream unavailable'}, 502)
    out_headers = [(k, v) for k, v in up.raw.headers.items() if k.lower() not in _HOP]
    out_headers.append(('X-Robots-Tag', 'noindex, nofollow'))
    out_headers.append(('Vary', 'Authorization'))
    if tok and request.args.get('token') and request.cookies.get(tiers.COOKIE) != tok:
        out_headers.append(('Set-Cookie', f'{tiers.COOKIE}={tok}; Max-Age=2592000; Path=/k; '
                                          f'Secure; HttpOnly; SameSite=Lax'))
    if _is_text_docs(request.path):
        data = up.raw.read(decode_content=True)
        out_headers = [(k, v) for k, v in out_headers if k.lower() not in ('content-encoding', 'content-length')]
        return Response(_rewrite_doc_prefixes(data), status=up.status_code, headers=out_headers)

    def _gen():
        try:
            for chunk in up.raw.stream(64 * 1024, decode_content=False):
                if chunk:
                    yield chunk
        finally:
            up.close()
    resp = Response(_gen(), status=up.status_code, headers=out_headers, direct_passthrough=True)
    return resp


def _is_text_docs(p: str) -> bool:
    p = p[len(tiers.PRIVATE_PREFIX):]
    return p in ('/llm.txt', '/api/v1/docs/llm.txt')


def _rewrite_doc_prefixes(b: bytes) -> bytes:
    """Prepend /k to absolute API paths in the private docs so examples paste."""
    import re
    txt = b.decode('utf-8', 'replace')
    txt = re.sub(r'(?<![\w/.:])(?<!/k)(/(?:api|llm|tiles|process)(?:/|\.txt|\.html))',
                 tiers.PRIVATE_PREFIX + r'\1', txt)
    txt = txt.replace(':8000/', '/' + tiers.PRIVATE_PREFIX.strip('/') + '/').replace('/k/k/', '/k/')
    return txt.encode()


# --------------------------------------------------------------------------
# PUBLIC tier
# --------------------------------------------------------------------------
_DOCS_TTL_S = 600
_docs_cache: dict = {}   # kind -> (rendered_at, body)


def _status_dict() -> dict:
    """Same payload /api/v1/status serves; lq.status() is itself 5-min cached."""
    st = dict(lq.status())
    if _cells is not None:
        try:
            st['cells'] = _cells.index_status()
        except Exception as e:  # noqa: BLE001
            st['cells'] = {'ready': False, 'error': str(e)}
    return st


def render_docs(kind: str) -> str:
    """LLM_TXT / INDEX_HTML with the live coverage block substituted.

    Rendered once per worker per _DOCS_TTL_S (10 min); on a status failure the
    marker is dropped and the previous render (if any) is kept, so the docs
    never 5xx and never cost more than one cheap SQL count per 10 min."""
    now = time.time()
    hit = _docs_cache.get(kind)
    if hit and now - hit[0] < _DOCS_TTL_S:
        return hit[1]
    tmpl = docs.LLM_TXT if kind == 'txt' else docs.INDEX_HTML
    try:
        st = _status_dict()
        block = docs.coverage_txt(st) if kind == 'txt' else docs.coverage_html(st)
    except Exception as e:  # noqa: BLE001
        log.warning('docs coverage block: %s', e)
        if hit:
            _docs_cache[kind] = (now, hit[1])
            return hit[1]
        block = ''
    body = tmpl.replace(docs.COVERAGE_MARK, block)
    _docs_cache[kind] = (now, body)
    return body


@app.route('/')
def root():
    r = Response(render_docs('html'), mimetype='text/html')
    r.headers['Cache-Control'] = 'public, max-age=600'
    return r


@app.route('/llm.txt')
@app.route('/api/v1/docs/llm.txt')
@app.route('/api/v1/docs')
def llm_txt():
    r = Response(render_docs('txt'), mimetype='text/plain; charset=utf-8')
    r.headers['Cache-Control'] = 'public, max-age=600'
    return r


@app.route('/api/v1/license')
def license_():
    return _json(docs.license_block(), max_age=3600)


@app.route('/api/v1/status')
def status():
    try:
        st = dict(lq.status())
    except Exception as e:  # noqa: BLE001
        return _err(str(e), 503)
    if _cells is not None:
        try:
            st['cells'] = _cells.index_status()
        except Exception as e:  # noqa: BLE001
            st['cells'] = {'ready': False, 'error': str(e)}
        try:
            st['als'] = _als_status()
        except Exception as e:  # noqa: BLE001
            st['als'] = {'error': str(e)}
    if request.args.get('guard'):
        st['guard'] = _guard_dict()
    return _json(st, max_age=60)


_als_cache: dict = {}


def _als_status() -> dict:
    """``als`` summary for /status (als_years.status_summary), 5-min cached per worker."""
    now = time.time()
    hit = _als_cache.get('v')
    if hit and now - hit[0] < 300:
        return hit[1]
    import als_years
    v = als_years.status_summary()
    _als_cache['v'] = (now, v)
    return v


def _bbox_or_err():
    try:
        bb = lq.parse_bbox(request.args)
    except ValueError as e:
        return None, _err(str(e))
    if bb is None:
        return None, _err('bbox=west,south,east,north (or west/south/east/north, or lon/lat[&radius_m]) required')
    return bb, None


@app.route('/api/v1/landscape')
@app.route('/api/v1/context')
def landscape():
    """THE call: what did the LiDAR / satellite segmentation learn about this
    bbox (or point)?  Terrain, land cover, trees, built-up heights, forest
    loss, KG coverage + readiness."""
    bb, err = _bbox_or_err()
    if err:
        return err
    inc = {x.strip() for x in (request.args.get('include') or '').split(',') if x.strip()}
    try:
        d = lq.landscape_bbox(*bb,
                              want_trees=not inc or 'trees' in inc,
                              want_buildings=not inc or 'buildings' in inc,
                              want_landmarks='landmarks' in inc,
                              tree_limit=int(request.args.get('tree_limit', 25)))
    except Exception as e:  # noqa: BLE001
        log.exception('landscape %s', bb)
        return _err(str(e), 500)
    if 'cells' in inc and _cells is not None:
        try:
            d['cells'] = _cells.landscape_cells_block(*bb)
        except Exception as e:  # noqa: BLE001
            log.warning('landscape cells block %s: %s', bb, e)
            d['cells'] = {'ready': False, 'error': str(e)}
    d['meta'] = _meta()
    if not d.get('ready'):
        d['retry_after_s'] = 1800
    return _json(d)


@app.route('/api/v1/heightfield')
def heightfield():
    bb, err = _bbox_or_err()
    if err:
        return err
    try:
        payload, st = lq.heightfield_bbox(*bb, cell=int(request.args.get('cell', 25)),
                                          want_landcover=request.args.get('landcover', '') in ('1', 'true', 'yes'))
    except Exception as e:  # noqa: BLE001
        log.exception('heightfield %s', bb)
        return _err(str(e), 500)
    if st == 200:
        payload['meta'] = _meta()
    return _json(payload, st, max_age=3600)


@app.route('/api/v1/trees/bbox')
def trees_bbox():
    bb, err = _bbox_or_err()
    if err:
        return err
    try:
        t = lq.trees_bbox(*bb, min_height=float(request.args.get('min_height', lq.TREE_H_MIN)),
                          limit=int(request.args.get('limit', 2000)))
    except Exception as e:  # noqa: BLE001
        return _err(str(e), 500)
    fmt = request.args.get('format', 'json')
    if fmt == 'geojson':
        fc = {'type': 'FeatureCollection',
              'features': [{'type': 'Feature', 'geometry': {'type': 'Point', 'coordinates': [x['lon'], x['lat']]},
                            'properties': {k: v for k, v in x.items() if k not in ('lon', 'lat')}}
                           for x in t['trees']],
              'meta': _meta(count=len(t['trees']), truncated=t['truncated'], dropped_qa=t['dropped_qa'],
                            kgs=t['kgs'], **docs.TREES_META)}
        return _json(fc)
    return _json({'bbox': list(bb), 'trees': t['trees'], 'count': len(t['trees']),
                  'truncated': t['truncated'], 'dropped_qa': t['dropped_qa'], 'kgs': t['kgs'],
                  'ready': bool(t['kgs']), 'meta': _meta(**docs.TREES_META)})


@app.route('/api/v1/landmarks/bbox')
def landmarks_bbox():
    bb, err = _bbox_or_err()
    if err:
        return err
    try:
        t = lq.landmarks_bbox(*bb)
    except Exception as e:  # noqa: BLE001
        return _err(str(e), 500)
    return _json({'bbox': list(bb), 'landmarks': t['landmarks'], 'count': len(t['landmarks']),
                  'kgs': t['kgs'], 'ready': bool(t['kgs']),
                  'meta': _meta(note='tall man-made verticals from the per-KG top-N-by-type table; '
                                     'QA flags (high/critical) and implausible heights removed',
                                types={k: {'min_h': a, 'max_h': b} for k, (a, b) in lq.LANDMARK_TYPES.items()})})


@app.route('/api/v1/buildings/bbox')
def buildings_bbox():
    bb, err = _bbox_or_err()
    if err:
        return err
    try:
        b = lq.buildings_bbox(*bb, limit=int(request.args.get('limit', 3000)))
    except Exception as e:  # noqa: BLE001
        return _err(str(e), 500)
    return _json({'bbox': list(bb), 'buildings': b['buildings'], 'count': len(b['buildings']),
                  'truncated': b['truncated'], 'kgs': b['kgs'], 'ready': bool(b['kgs']),
                  'meta': _meta(note='LiDAR nDSM heights at the centroid of each built-up object we '
                                     'segmented. No ids, outlines or areas — join to your own '
                                     'building outlines by point-in-polygon (centroids agree to ~0.2 m).')})


@app.route('/api/v1/kgs')
@app.route('/llm/kgs')
@app.route('/llm/manifest.json')
def kgs():
    """KG coverage: which Katastralgemeinden are processed (bbox or whole
    country, paginated). Codes/names only — a register, not cadastre."""
    try:
        bb = lq.parse_bbox(request.args) if any(k in request.args for k in ('bbox', 'west', 'lon')) else None
    except ValueError as e:
        return _err(str(e))
    processed_only = request.args.get('processed_only', '1') not in ('0', 'false', 'no')
    codes = request.args.get('codes')
    st = lq.status()
    if request.args.get('fields') == 'codes':
        # Peer universe check (umfeld-at contract): codes only, enough to
        # recompute universe_hash. Hash is the ETag so If-None-Match → 304.
        try:
            cl = lq.kg_codes(processed_only=processed_only)
        except Exception as e:  # noqa: BLE001
            return _err(str(e), 500)
        return _json({'codes': cl, 'count': len(cl), 'kgs_total': st['kgs_total'],
                      'universe_hash': st['universe_hash'],
                      'universe_hash_recipe': st['universe_hash_recipe'],
                      'meta': _meta()}, max_age=3600,
                     etag=f'"{st["universe_hash"]}"' if not processed_only else None)
    try:
        if codes:
            want = [c.strip() for c in codes.split(',') if c.strip()][:200]
            rows = [r_ for c in want if (r_ := lq.kg_row(c))]
            total = len(rows)
        elif bb:
            rows = lq.kgs_in_bbox(*bb, processed_only=processed_only); total = len(rows)
        else:
            limit = max(1, min(int(request.args.get('limit', 1000)), 5000))
            offset = max(0, int(request.args.get('offset', 0)))
            rows, total = lq.kgs_list(processed_only=processed_only, limit=limit, offset=offset)
    except Exception as e:  # noqa: BLE001
        return _err(str(e), 500)
    out = [lq.kg_public(r_) for r_ in rows]
    for d in out:
        d['url'] = f'/api/v1/kg/{d["kg_code"]}'
    return _json({'kgs': out, 'total': total, 'count': len(out),
                  'offset': int(request.args.get('offset', 0)) if not (codes or bb) else 0,
                  'kgs_processed': st['kgs_processed'], 'kgs_total': st['kgs_total'],
                  'kgs_ne_served': st.get('kgs_ne_served'),
                  'universe_hash': st['universe_hash'],
                  'universe_hash_recipe': st['universe_hash_recipe'],
                  'manifest_generation': st.get('manifest_generation'),
                  'cells_generation': st.get('cells_generation'),
                  'meta': _meta()}, max_age=300)


@app.route('/api/v1/kg/<code>')
@app.route('/llm/kg/<code>')
@app.route('/llm/kg/<code>.json')
def kg(code):
    code = code.strip()
    if not (code.isdigit() and len(code) == 5):
        return _err('kg code must be 5 digits', 400)
    try:
        d = lq.kg_summary(code)
    except Exception as e:  # noqa: BLE001
        return _err(str(e), 500)
    if d is None:
        return _err(f'unknown KG {code}', 404)
    d['meta'] = _meta()
    if not d.get('ready'):
        d['retry_after_s'] = 3600
        return _json(d, 202, max_age=0)
    d['links'] = {
        'landscape': f'/api/v1/landscape?bbox={",".join(str(x) for x in d["bbox"])}',
        'trees': f'/api/v1/trees/bbox?bbox={",".join(str(x) for x in d["bbox"])}&min_height=20',
        'heightfield': f'/api/v1/heightfield?bbox={",".join(str(x) for x in d["bbox"])}&cell=50',
        'cells': f'/api/v1/kg/{code}/cells',
    }
    if _cells is not None:
        try:
            d['cells'] = _cells.kg_cells_block(code)
        except Exception as e:  # noqa: BLE001
            d['cells'] = {'ready': False, 'error': str(e)}
        try:
            d['als'] = _cells.kg_als_block(code)     # real ALS flight years (als_years.py)
        except Exception as e:  # noqa: BLE001
            d['als'] = {'error': str(e)}
    return _json(d, max_age=600)


def _meta(**extra):
    m = {'service': 'srtm-lidar-at', 'license': 'CC-BY-4.0', 'license_url': LICENSE_URL,
         'attribution': ATTRIBUTION, 'docs': '/api/v1/docs/llm.txt'}
    t0 = getattr(g, 't0', None)
    if t0:
        m['duration_ms'] = int((time.time() - t0) * 1000)
    m.update(extra)
    return m


@app.errorhandler(404)
def _404(_e):
    # unmatched /api/v1/* may be one of the analysis endpoints the public
    # /segmentation UI relays to app.py (explicit allow-list, docs/api-tiers.md)
    if _seg is not None and (request.path or '').startswith('/api/v1/'):
        try:
            r = _seg.relay()
        except sg.Overloaded as e:
            return _overloaded(e)
        if r is not None:
            return r
    return _not_found()


@app.errorhandler(405)
def _405(_e):
    return _err('method not allowed', 405)


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=int(os.environ.get('PORT', 8100)), threaded=True)
