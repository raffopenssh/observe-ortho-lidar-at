"""public_api — the public read-only tier.

Runs as its own gunicorn (``systemd/srv_public.service``, ``config.PUBLIC_PORT``).

    PUBLIC   /api/v1/…  /llm.txt  /tiles/…    landscape_query.py — bbox / point /
                                              KG-code keyed, index + grid25 fast
                                              path only (no Zenodo in the request path)
    /segmentation                             public_segmentation.py — UI + an
                                              explicit allow-list relay of analysis
                                              routes to app.py (config.APP_URL)

Never imports ``app.py`` (that would start a second director / sync / index
thread set). ``hillshade_tiles`` is reused as-is.
"""
from __future__ import annotations

import email.utils
import gzip
import hashlib
import json
import logging
import os
import time

from flask import Flask, Response, g, jsonify, request

import config
import landscape_query as lq
import mem_lru
import public_docs as docs
import service_guard as sg

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s')
log = logging.getLogger('public_api')

app = Flask(__name__, static_folder=None)
# Body-size cap: the biggest legitimate public body is a /parse-geometry upload
# (relayed to app.py, which applies its own cap) — 32 MiB is generous.
app.config['MAX_CONTENT_LENGTH'] = config.env_int('SRTM_PUBLIC_MAX_BODY_MB', 32) * 1024 * 1024
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
    import stats_unit as _stats                  # /api/v1/stats/unit (stats-envelope/1)
    app.register_blueprint(_stats.stats_bp)
except Exception as e:  # noqa: BLE001
    _stats = None
    log.warning('stats unit api unavailable: %s', e)
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
    # header value hygiene: never emit a literal None/null/empty on any X-* header
    # (downstream consumers hash X-Cells-Epoch into cache state; str(None) churned it)
    for k in [k for k, v in resp.headers.items() if k.startswith('X-') and (v or '').strip().lower() in ('', 'none', 'null')]:
        del resp.headers[k]
    if True:
        # CORS: wildcard origin only for read-only requests; the relayed
        # /segmentation POSTs are same-origin (served from this host).
        if request.method in ('GET', 'HEAD', 'OPTIONS'):
            resp.headers['Access-Control-Allow-Origin'] = '*'
            resp.headers['Access-Control-Allow-Headers'] = 'Content-Type, If-None-Match'
            resp.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
            resp.headers['Access-Control-Expose-Headers'] = ('ETag, Last-Modified, Cache-Control, Content-Encoding, Retry-After, '
                                                             'X-Cells-Gen, X-Cells-Epoch, X-Cells-Source, X-KG-Universe-Hash, X-Data-Attribution')
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
    if request.method == 'OPTIONS':
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


TREES_REV = 'trees-c1'          # bump when the /trees/bbox body layout or QA rules change (part of the ETag)
TREES_MAX_AGE = 86400          # answers change only when a KG is reprocessed; ETag revalidation is index-only


def _flag_arg(name: str, default: bool) -> bool:
    v = request.args.get(name)
    if v is None or v == '':
        return default
    return str(v).strip().lower() not in ('0', 'false', 'no', 'off')


@app.route('/api/v1/trees/bbox')
def trees_bbox():
    """Tree apices (default) or structures (``objects=structures``) in a bbox,
    tallest first, straight from the NE cells corpus.  ``realistic=0`` turns
    the plausibility / flag filter off.  ``ready`` is honest: false while a KG
    of the bbox is still pending or the cells index is unusable."""
    bb, err = _bbox_or_err()
    if err:
        return err
    kind = (request.args.get('objects') or request.args.get('kind') or 'trees').strip().lower()
    if kind in ('structure', 'structures', 'buildings', 'nontree', 'non_tree'):
        kind = 'structures'
    if kind not in ('trees', 'structures'):
        return _err('objects must be trees | structures', 400)
    types = None
    if request.args.get('types'):
        types = {t.strip().lower() for t in request.args['types'].split(',') if t.strip()}
    realistic = _flag_arg('realistic', True)
    import ne_trees
    fmt = (request.args.get('format') or 'json').strip().lower()
    if fmt not in ('json', 'geojson', 'columns'):
        return _err('format must be json | geojson | columns', 400)
    max_limit = ne_trees.MAX_LIMIT_COLUMNS if fmt == 'columns' else ne_trees.MAX_LIMIT
    try:
        min_h = float(request.args.get('min_height') or 0)
        max_h = request.args.get('max_height')
        max_h = float(max_h) if max_h not in (None, '') else None
        limit = int(request.args.get('limit') or (max_limit if fmt == 'columns' else ne_trees.DEFAULT_LIMIT))
    except ValueError:
        return _err('min_height / max_height / limit must be numbers', 400)
    if max_h is not None and max_h <= min_h:
        return _err('max_height must be greater than min_height (window is min_height <= h < max_height)', 400)
    if limit > max_limit:
        return _err(f'limit must be <= {max_limit} for format={fmt}', 400)
    fields = None
    if request.args.get('fields'):
        fields = tuple(f.strip() for f in request.args['fields'].split(',') if f.strip())
        bad = [f for f in fields if f not in ne_trees.COLUMN_FIELDS[kind]]
        if bad:
            return _err(f'unknown fields: {",".join(bad)}', 400, allowed=list(ne_trees.COLUMN_FIELDS[kind]))
    # Strong ETag from the per-KG NE file state (neo1_digest changes only when
    # a KG is reprocessed / re-ingested) + the canonical query + code revision.
    # Computed BEFORE the scan so a matching If-None-Match is index-only.
    st = ne_trees.bbox_state(*bb)
    canon = '&'.join(f'{k}={v}' for k, v in sorted(request.args.items(multi=True)) if k != 'pretty')
    etag = '"t-' + hashlib.sha256(('|'.join(st['digests']) + '\n' + canon + '\n' + TREES_REV).encode()).hexdigest()[:20] + '"'
    cacheable = st['ready']
    cc = f'public, max-age={TREES_MAX_AGE}' if cacheable else 'no-store'
    lm = email.utils.formatdate(st['mtime'], usegmt=True) if st['mtime'] else None
    hdrs = {'ETag': etag, 'Cache-Control': cc}
    if lm:
        hdrs['Last-Modified'] = lm
    if cacheable:
        inm = request.headers.get('If-None-Match') or ''
        if etag in [x.strip() for x in inm.split(',')] or inm.strip() == '*':
            return Response(status=304, headers=hdrs)
    try:
        if fmt == 'columns':
            t = ne_trees.columns_bbox(*bb, kind, fields=fields, min_height=min_h, max_height=max_h, limit=limit,
                                      realistic=realistic, types=types)
        else:
            t = lq.trees_bbox(*bb, min_height=min_h, max_height=max_h, limit=limit,
                              realistic=realistic, kind=kind, types=types)
    except ValueError as e:
        return _err(str(e), 400)
    except Exception as e:  # noqa: BLE001
        log.exception('trees_bbox %s', bb)
        return _err(str(e), 500)
    key = 'trees' if kind == 'trees' else 'structures'
    ready = bool(t['ready'])
    n_out = t['n'] if fmt == 'columns' else len(t['trees'])
    extra = {}
    if not n_out:
        sti = ne_trees.status()
        if not sti['ok']:
            ready = False
            extra['error'] = sti.get('error') or 'cells index unusable'
            extra['retry_after_s'] = 600
    if t['pending_kgs']:
        extra['pending_kgs'] = t['pending_kgs']; extra['retry_after_s'] = 300
    if t['not_processed']:
        extra['not_processed'] = t['not_processed']
    if not ready:
        hdrs['Cache-Control'] = 'no-store'
    meta = dict(docs.TREES_META if kind == 'trees' else docs.STRUCTURES_META, realistic=realistic,
                min_height=t['min_height'], max_height=t['max_height'], limit=t['limit'],
                max_limit=max_limit, chunks_scanned=t['chunks_scanned'], ms=t['ms'])
    if fmt == 'columns':
        meta['columns_note'] = docs.TREES_COLUMNS_NOTE
        body = {'bbox': list(bb), 'objects': kind, 'format': 'columns', 'fields': t['fields'], 'order': t['order'],
                'columns': t['columns'], 'count': n_out, 'truncated': t['truncated'], 'dropped_qa': t['dropped_qa'],
                'kgs': t['kgs'], 'ready': ready, **extra, 'meta': _meta(**meta)}
    elif fmt == 'geojson':
        body = {'type': 'FeatureCollection',
                'features': [{'type': 'Feature', 'geometry': {'type': 'Point', 'coordinates': [x['lon'], x['lat']]},
                              'properties': {k: v for k, v in x.items() if k not in ('lon', 'lat')}}
                             for x in t['trees']],
                'meta': _meta(count=n_out, truncated=t['truncated'], dropped_qa=t['dropped_qa'],
                              kgs=t['kgs'], ready=ready, **extra, **meta)}
    else:
        body = {'bbox': list(bb), 'objects': kind, key: t['trees'], 'count': n_out,
                'truncated': t['truncated'], 'dropped_qa': t['dropped_qa'], 'kgs': t['kgs'],
                'ready': ready, **extra, 'meta': _meta(**meta)}
    resp = Response(json.dumps(body, separators=(',', ':'), ensure_ascii=False,
                               indent=1 if request.args.get('pretty') else None).encode(),
                    mimetype='application/json')
    for k, v in hdrs.items():
        resp.headers[k] = v
    return resp


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
    country, paginated). Codes/names only — a register."""
    try:
        bb = lq.parse_bbox(request.args) if any(k in request.args for k in ('bbox', 'west', 'lon')) else None
    except ValueError as e:
        return _err(str(e))
    processed_only = request.args.get('processed_only', '1') not in ('0', 'false', 'no')
    codes = request.args.get('codes')
    st = lq.status()
    fields = {f.strip() for f in (request.args.get('fields') or '').split(',') if f.strip()}
    if fields == {'codes'}:
        # Universe check for downstream consumers: codes only, enough to
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
    if fields == {'codes', 'als'}:
        # Bulk ALS flight-year block (one GET instead of 6 500):
        # per KG {kg_code, als:{newest, oldest, dominant, dominant_share,
        # precision, reflown_share}}; als=null where no cells product /
        # backfill pending.  Served straight from the als_kg cache.
        # Strong ETag + Last-Modified from the als_kg cache fingerprint
        # (row count / newest computed_at / content hash) x universe_hash,
        # so a daily If-None-Match costs a COUNT() instead of 944 KB.
        try:
            import als_years
            fp = als_years.bulk_fingerprint()
        except Exception:  # noqa: BLE001
            fp = None
        etag = None
        last_mod = None
        if fp:
            etag = f'"{hashlib.md5((fp[0] + "|" + str(st["universe_hash"]) + "|" + str(int(processed_only))).encode()).hexdigest()[:24]}"'
            last_mod = fp[1]
            if request.headers.get('If-None-Match') == etag:
                return Response(status=304, headers=_als_hdrs(etag, last_mod))
        try:
            cl = lq.kg_codes(processed_only=processed_only)
            blocks = als_years.bulk_slim(cl)
        except Exception as e:  # noqa: BLE001
            return _err(str(e), 500)
        rows = [{'kg_code': c, 'als': blocks.get(c)} for c in cl]
        resp = _json({'kgs': rows, 'count': len(rows), 'als_count': len(blocks),
                      'kgs_total': st['kgs_total'], 'universe_hash': st['universe_hash'],
                      'cells_generation': st.get('cells_generation'),
                      'meta': _meta(note='slim als block, no histograms; full block with years/'
                                         'coverage/newest_hist/oldest_hist at /api/v1/kg/<code>. '
                                         'dominant = flight year covering the largest cell share '
                                         '(dominant_share); newest = max year present, may be a sliver '
                                         '(share in coverage at /api/v1/kg/<code>).')},
                     max_age=1800, etag=etag)
        if etag:
            resp.headers.update(_als_hdrs(etag, last_mod))
        return resp
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


def _als_hdrs(etag, last_mod):
    h = {'ETag': etag, 'Cache-Control': 'public, max-age=1800'}
    if last_mod:
        h['Last-Modified'] = time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime(last_mod))
    return h


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
    # /segmentation UI relays to app.py (explicit allow-list, public_segmentation.ROUTES)
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
    app.run(host=config.PUBLIC_BIND, port=config.PUBLIC_PORT, threaded=True)
