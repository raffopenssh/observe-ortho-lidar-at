"""National tree statistics for the public tier (``GET /api/v1/trees/stats``).

Answers "how many trees does the dataset hold" with ONE definition:

    realistic apices  = what ``/api/v1/trees/bbox`` returns by default
                        (LiDAR crown tops h >= 3 m, realistic=1), each counted
                        once (border cells deduplicated across KG products).

Source = the national res-8 rollup (``cells_national``: ``tree_n`` / ``tree_hh``),
so the numbers move only with its generation.  Nothing is built on the request
path beyond one sub-second pass over the stored 5 MB table per generation and
worker (kept in memory; no store of its own — the bins / national table stay the
only copy of the data).

NOT the same thing: ``tree_segments`` (index ``kg.tree_count``) = segmentation
objects of type ``tree`` — crown clumps / stands, ~3.4 apices each.

    python3 tree_stats.py            # print the national block
"""
from __future__ import annotations

import gzip
import json
import logging
import threading
import time

import numpy as np

log = logging.getLogger(__name__)

MIN_HEIGHT_M = 3.0
ELEV_EDGES = (0, 300, 500, 750, 1000, 1250, 1500, 1750, 2000, 2250, 9000)   # m a.s.l., res-8 hexagon mean
AUX_TTL_S = 3600.0

DEFINITIONS = {
    'apices': 'LiDAR crown tops (local maxima of the 1 m canopy height model inside tree segments), h >= 3 m, '
              'after the realistic filter (saturated >= 60 m, spikes, linear noise, flagged rows hidden) — exactly '
              'the default answer of /api/v1/trees/bbox. One apex per dominant / co-dominant crown: understorey '
              'stems under a closed canopy are not seen, so this is a canopy-tree count, not a stem count.',
    'dedup': 'each res-12 cell is served from one KG product, so apices in the overlap of two products count once.',
    'tree_segments': 'segmentation objects of type tree (crown clumps / stands, one polygon may hold several '
                     'apices) summed over the KG index — the former tree_count. Not a tree count, and NOT '
                     'deduplicated: KG product bboxes overlap (~3x the national area in total), so the national '
                     'sum counts border segments several times. Use it per KG only.',
    'height_bands': 'half-open [min, max) m, the same windows as /trees/bbox min_height / max_height.',
    'by_elevation': 'banded by the mean elevation of the H3 res-8 hexagon (~0.74 km2) holding the apex.',
    'by_state': 'apices in the cells each KG product serves (deduplicated, before the realistic filter: ~0.05 % '
                'more than apices) summed per Bundesland of the KG; a product covers its KG bbox, so cells near a '
                'state border may count for the neighbour state.',
    'forest_cover_km2': 'sum of the observed forest (wald) cover share over all cells — observed land cover, not the '
                        'legal forest area.',
    'area_km2': 'observed area = res-12 cells with LiDAR under them (includes the bbox overhang across the border).',
}

_lock = threading.Lock()
_cache: dict = {'gen': None, 'v': None}
_aux: dict = {'t': 0.0, 'v': None}


def _bands(names, counts) -> list[dict]:
    tot = int(counts.sum()) or 1
    out = []
    for nm, v in zip(names, counts.tolist()):
        lo, _, hi = nm.partition('-')
        lo_f = float(lo.rstrip('+'))
        out.append({'band': nm, 'min_m': lo_f, 'max_m': float(hi) if hi else None,
                    'apices': int(v), 'share': round(v / tot, 4)})
    return out


def compute() -> dict | None:
    """National block from the stored res-8 table (None while it was never built)."""
    import cells_national as cn
    m = cn.read_meta()
    p = cn.NAT_DIR / 'r8.nex.gz'
    if not m or not p.exists():
        return None
    hdr, c = cn.decode(gzip.decompress(p.read_bytes()))
    names = hdr.get('tree_hh_bands') or []
    hh = np.asarray(c['tree_hh'], np.int64).reshape(-1, len(names))
    tn = hh.sum(1)
    tot = hh.sum(0)
    n_obs = np.asarray(c['n_obs'], np.int64)
    try:
        import h3
        ids = np.asarray(c['id'], np.uint64)
        a8 = np.fromiter((h3.cell_area(h3.int_to_str(int(i)), unit='m^2') for i in ids), np.float64, len(ids))
        cell_m2 = a8 / 7 ** (int(hdr.get('child_res', 12)) - int(hdr['res']))
    except Exception as e:  # noqa: BLE001
        log.debug('tree_stats: h3 area fallback: %s', e)
        cell_m2 = np.full(len(n_obs), 307.0)
    area = n_obs * cell_m2                                   # m2 observed per res-8 row
    area_km2 = float(area.sum() / 1e6)
    groups = hdr.get('groups') or []
    forest_km2 = None
    if 'wald' in groups and 'cover' in c:
        w = np.asarray(c['cover'], np.float64).reshape(len(n_obs), -1)[:, groups.index('wald')] / 255.0
        forest_km2 = float((w * np.asarray(c['n_cells'], np.int64) * cell_m2).sum() / 1e6)
    cum = np.cumsum(tot[::-1])[::-1]
    at_least = [{'min_height_m': float(nm.partition('-')[0].rstrip('+')), 'apices': int(v)}
                for nm, v in zip(names, cum.tolist())]
    total = int(tot.sum())
    elev = np.asarray(c['elev_mean'], np.int64)
    ok = elev != 65535
    tall_from = next((k for k, nm in enumerate(names) if nm.startswith('30')), len(names))
    by_elev = []
    for lo, hi in zip(ELEV_EDGES[:-1], ELEV_EDGES[1:]):
        sel = ok & (elev >= lo) & (elev < hi)
        n = int(tn[sel].sum()); a = float(area[sel].sum() / 1e6)
        hs = hh[sel].sum(0)
        mids = np.array([4.0] + [5 * k + 2.5 for k in range(1, len(names))])
        by_elev.append({'elev_min_m': lo, 'elev_max_m': hi if hi < 9000 else None, 'area_km2': round(a, 1),
                        'apices': n, 'apices_per_ha': round(n / (a * 100), 1) if a else None,
                        'share_ge_30m': round(float(hs[tall_from:].sum()) / n, 4) if n else None,
                        'height_mean_m_approx': round(float((hs * mids).sum()) / n, 1) if n else None})
    return {
        'scope': 'national', 'country': 'AT', 'min_height_m': MIN_HEIGHT_M,
        'apices': total,
        'height_bands': _bands(names, tot),
        'at_least': at_least,
        'area_km2': round(area_km2, 1),
        'apices_per_ha': round(total / (area_km2 * 100), 2) if area_km2 else None,
        'forest_cover_km2': round(forest_km2, 1) if forest_km2 is not None else None,
        'by_elevation': by_elev,
        'kgs': m.get('kgs'), 'gen': m.get('gen'), 'epoch': m.get('epoch'), 'built_at': m.get('built_at'),
        'source': '/api/v1/cells/national?res=8&fields=tree_n,tree_hh,elev_mean,n_obs',
    }


def _aux_block() -> dict:
    """Slow-moving side counts (QA-hidden, system-added, raw stored, tree segments); each part optional."""
    now = time.time()
    if _aux['v'] is not None and now - _aux['t'] < AUX_TTL_S:
        return _aux['v']
    out: dict = {}
    try:
        import tree_qa
        out['hidden_by_realistic_filter'] = int(tree_qa.status().get('apices_hidden') or 0)
    except Exception as e:  # noqa: BLE001
        log.debug('tree_stats: tree_qa: %s', e)
    try:
        import rebin_added
        s = rebin_added.status()
        out.update(stored_raw=int(s['stored']), border_duplicates=int(s.get('lost') or 0),
                   added_trees=int(s.get('added') or 0))
    except Exception as e:  # noqa: BLE001
        log.debug('tree_stats: rebin_added: %s', e)
    try:
        out.update(_per_kg())
    except Exception as e:  # noqa: BLE001
        log.debug('tree_stats: per-kg: %s', e)
    _aux.update(t=now, v=out)
    return out


def national() -> dict | None:
    """Cached national block (+ side counts); None while the national table does not exist."""
    import cells_national as cn
    m = cn.meta()
    if not m:
        return None
    gen = m.get('gen')
    v = _cache['v'] if _cache['gen'] == gen else None
    if v is None:
        with _lock:
            v = _cache['v'] if _cache['gen'] == gen else None
            if v is None:
                v = compute()
                if v is None:
                    return None
                _cache.update(gen=gen, v=v)
    out = dict(v)
    aux = _aux_block()
    if aux.get('tree_segments'):
        out['tree_segments'] = aux['tree_segments']
    if aux.get('by_state'):
        out['by_state'] = aux['by_state']
    out['accounting'] = {k: aux[k] for k in ('stored_raw', 'border_duplicates', 'hidden_by_realistic_filter',
                                             'added_trees') if k in aux}
    out['definitions'] = DEFINITIONS
    return out


def _per_kg() -> dict:
    """One pass over the index + the rebin store: per-KG apices / tree segments, state roll-up."""
    import sqlite3
    import landscape_query as lq
    rows = lq.idx().execute('SELECT kg_code, state_code, state_name, tree_count '
                            'FROM kg WHERE processed=1 ORDER BY kg_code').fetchall()
    own: dict = {}
    try:
        import rebin_added
        con = sqlite3.connect(f'file:{rebin_added.DB_PATH}?mode=ro', uri=True, timeout=2)
        try:
            own = {int(k): max(0, int(s_) - int(l_ or 0))
                   for k, s_, l_ in con.execute('SELECT kg, served, lost FROM kgs WHERE ok=1 AND served IS NOT NULL')}
        finally:
            con.close()
    except Exception as e:  # noqa: BLE001
        log.debug('tree_stats: rebin store: %s', e)
    codes, ap, seg, st = [], [], [], {}
    for code, sc, sn, tc in rows:
        try:
            a = own.get(int(code))
        except ValueError:
            a = None
        codes.append(code); ap.append(a); seg.append(int(tc or 0))
        d = st.setdefault(sc or '', {'state_code': sc or None, 'state_name': sn or None, 'kgs': 0, 'apices': 0})
        d['kgs'] += 1; d['apices'] += a or 0
    return {'tree_segments': sum(seg), 'by_state': [st[k] for k in sorted(st)] if own else None,
            'kg': {'kg_code': codes, 'apices': ap} if own else None}


def by_kg() -> dict | None:
    """Per-KG lookup table {kg_code[] sorted, apices[]} (None when the rebin store is absent)."""
    return _aux_block().get('kg')


def kg_apices(code: str) -> int | None:
    """Apices (h >= 3 m) in the cells this KG's product serves — border cells owned by a neighbour product
    excluded, before the realistic filter (which hides ~0.05 %).  None when unknown."""
    t = by_kg()
    if not t:
        return None
    import bisect
    i = bisect.bisect_left(t['kg_code'], code)
    return t['apices'][i] if i < len(t['kg_code']) and t['kg_code'][i] == code else None


if __name__ == '__main__':
    print(json.dumps(national(), indent=1, ensure_ascii=False))
