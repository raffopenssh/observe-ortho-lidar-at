"""llm_api — small machine-readable helper endpoints (warm-up hint, sampled trees).

Everything here answers from ``data/search_index.db`` (kg / kg_trees) —
never from a KG JSON, never from a GPKG — so every call is a few ms and
costs no Zenodo / BEV bandwidth on the metered primary. "Warm" therefore
means "ingested into the index"; the only cold state is a KG whose product
exists on Zenodo but hasn't been ingested yet (``ready:false`` +
``retry_after_s``).

Endpoints (CORS ``*``, ETag via app after_request):
  POST /api/v1/prewarm?kgs=  warm-up hint (ALL-4)
  /api/v1/trees/bbox         sampled tree apices (LID-2)

The public tier (``public_api.py`` / ``public_docs.py``) carries the
``llm.txt`` documentation.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path

from flask import Blueprint, jsonify, request

import search_index as si

log = logging.getLogger(__name__)

bp = Blueprint('llm_api', __name__)

# Read-only paths that browsers may call cross-origin (GET/HEAD/OPTIONS only).
CORS_PREFIXES = ('/api/v1/trees', '/api/v1/kg/', '/api/v1/prewarm', '/tiles/',
                 '/api/v1/index/status')


def cors_path(path: str) -> bool:
    return any(path.startswith(p) for p in CORS_PREFIXES)


def add_cors(resp):
    if request.method not in ('GET', 'HEAD', 'OPTIONS'):
        return resp
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Methods'] = 'GET, HEAD, OPTIONS'
    resp.headers['Access-Control-Allow-Headers'] = 'Content-Type, If-None-Match'
    resp.headers['Access-Control-Expose-Headers'] = 'ETag, X-Cache, X-Resolution-M'
    resp.headers['Access-Control-Max-Age'] = '86400'
    return resp


# ── helpers ─────────────────────────────────────────────────────────────

def _idx():
    return si.get_index()


def _code_ok(code: str) -> bool:
    return isinstance(code, str) and len(code) == 5 and code.isdigit()


def _no_data(code, extra=None, status=404):
    body = {'kg_code': code, 'error': 'no_data'}
    if extra:
        body.update(extra)
    return jsonify(body), status


_completed_cache = {'t': 0.0, 'set': frozenset()}
_completed_lock = threading.Lock()


def completed_codes() -> frozenset:
    """Parent KG codes with a committed ``_json`` on Zenodo (60 s cache).
    Cheap manifest-key scan — used to tell *pending ingest* from *never
    processed* for the ``ready:false`` contract."""
    now = time.time()
    if now - _completed_cache['t'] < 60:
        return _completed_cache['set']
    with _completed_lock:
        if now - _completed_cache['t'] < 60:
            return _completed_cache['set']
        out = set()
        try:
            mp = Path('data/austria_processor/zenodo_manifest.json')
            m = json.loads(mp.read_text())
            for k, v in (m.get('entries', m) or {}).items():
                if k.endswith('_json') and 'error' not in str((v or {}).get('status', '')):
                    out.add(k[:-5].split('-', 1)[0])
        except Exception as e:
            log.debug('completed_codes: %s', e)
        _completed_cache['set'] = frozenset(out)
        _completed_cache['t'] = time.time()
        return _completed_cache['set']


def _kg_state(code: str, idx=None):
    """'indexed' | 'pending' (product on Zenodo, not ingested yet) |
    'unprocessed' (known KG, no product) | 'unknown'."""
    idx = idx or _idx()
    c = idx._conn()
    row = c.execute('SELECT processed FROM kg WHERE kg_code=?', (code,)).fetchone()
    if row is None:
        return 'unknown'
    if row[0]:
        return 'indexed'
    return 'pending' if code in completed_codes() else 'unprocessed'


def pending_kgs_in_bbox(w, s, e, n, idx=None) -> list:
    """KGs touching bbox whose product exists but is not yet indexed."""
    idx = idx or _idx()
    allk = idx.kgs_in_bbox(w, s, e, n, processed_only=False)
    done = set(idx.kgs_in_bbox(w, s, e, n, processed_only=True))
    comp = completed_codes()
    return [k for k in allk if k not in done and k in comp]


# ── POST /api/v1/prewarm ────────────────────────────────────────────────

@bp.route('/api/v1/prewarm', methods=['POST', 'GET'])
def prewarm():
    raw = request.args.get('kgs') or ''
    if not raw:
        body = request.get_json(silent=True) or {}
        v = body.get('kgs') or []
        raw = ','.join(v) if isinstance(v, list) else str(v)
    codes = []
    for x in raw.split(','):
        x = x.strip()
        if x and x not in codes:
            codes.append(x)
    if not codes:
        return jsonify({'error': 'kgs required (comma list, ≤50)'}), 400
    codes = codes[:50]
    idx = _idx()
    # ``queued`` is always empty (no backfill step) — kept for client compatibility.
    out = {'already_warm': [], 'queued': [], 'pending': [], 'not_processed': [], 'unknown': []}
    for code in codes:
        if not _code_ok(code):
            out['unknown'].append(code)
            continue
        st = _kg_state(code, idx)
        if st == 'indexed':
            # an indexed KG is warm by definition.
            out['already_warm'].append(code)
        elif st == 'pending':
            out['pending'].append(code)
        elif st == 'unprocessed':
            out['not_processed'].append(code)
        else:
            out['unknown'].append(code)
    all_warm = not out['queued'] and not out['pending']
    out['ready'] = all_warm
    if not all_warm:
        out['retry_after_s'] = 30 if out['queued'] else 300
    return jsonify(out), (200 if all_warm else 202)


# ── GET /api/v1/trees/bbox ──────────────────────────────────────────────

def _bbox_args():
    a = request.args
    if a.get('bbox'):
        w, s, e, n = [float(x) for x in a['bbox'].split(',')]
    else:
        w = float(a['west']); s = float(a['south'])
        e = float(a['east']); n = float(a['north'])
    if w > e:
        w, e = e, w
    if s > n:
        s, n = n, s
    return w, s, e, n


@bp.route('/api/v1/trees/bbox')
def trees_bbox():
    """Private-tier mirror of the public route (same engine: ne_trees over the
    NE cells corpus; legacy ``kg_trees`` is empty since the Kataster purge).
    Keeps the h_m / rf_conf field names of the old contract."""
    try:
        w, s, e, n = _bbox_args()
    except Exception:
        return jsonify({'error': 'west,south,east,north (or bbox=w,s,e,n) required'}), 400
    if (e - w) * (n - s) > 0.25:
        return jsonify({'error': 'bbox too large (max 0.25 deg²)'}), 400
    import ne_trees
    min_h = float(request.args.get('min_height', 0) or 0)
    max_h = request.args.get('max_height')
    max_h = float(max_h) if max_h not in (None, '') else None
    limit = int(request.args.get('limit', 2000) or 2000)
    if max_h is not None and max_h <= min_h:
        return jsonify({'error': 'max_height must be greater than min_height'}), 400
    if limit > ne_trees.MAX_LIMIT:
        return jsonify({'error': f'limit must be <= {ne_trees.MAX_LIMIT}'}), 400
    realistic = str(request.args.get('realistic', '1')).lower() not in ('0', 'false', 'no', 'off')
    r = ne_trees.trees_bbox(w, s, e, n, min_height=min_h, max_height=max_h, limit=limit, realistic=realistic)
    trees = [{'lon': t['lon'], 'lat': t['lat'], 'h_m': t['height_m'], 'crown_d_m': t['crown_d_m'],
              'rf_conf': None, 'kg_code': t['kg_code'], 'cell': t['cell']} for t in r['trees']]
    body = {
        'trees': trees, 'count': len(trees), 'truncated': r['truncated'],
        'ready': r['ready'], 'kgs': r['kgs'], 'min_height': min_h, 'max_height': max_h,
        'limit': min(limit, ne_trees.MAX_LIMIT), 'realistic': realistic, 'dropped_qa': r['dropped_qa'],
        'source': 'NE cells corpus (data/ne/*.h3b) via ne_trees — every apex h ≥ 3 m, tallest first',
        'complete': True,
    }
    if r['pending_kgs']:
        body['pending_kgs'] = r['pending_kgs']
        body['retry_after_s'] = 300
    if not trees:
        st = ne_trees.status()
        if not st['ok']:
            body['ready'] = False; body['error'] = st.get('error')
    if not r['kgs'] and not r['pending_kgs']:
        body['coverage'] = 'none'
    return jsonify(body)
