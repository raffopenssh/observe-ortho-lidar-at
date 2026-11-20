"""tree_crown — "these tops are one tree": the ``crown`` hint on a spurious tree flag.

A broad crown (beech, oak, old lime) often carries several apices.  A reader
flags the EXTRA (lower) top:  layer ``trees``, claim ``spurious``,
``expected: {absent: true, hint: "crown", of: {tree_id | lon, lat}}``.

Design (docs/tree-user.md → Same tree):
  * nothing new on disk: the report is an ordinary ``report`` row of
    ``public_flags.sqlite``.  Only its subject key differs — the apex itself
    (``-(x << 32 | y)``, EPSG:3035 integer metres) instead of the res-12 cell,
    so several extra tops in ONE cell stay separate rows / subjects.
  * ``of`` is resolved to a corpus apex at write time and stored normalised
    (``{tree_id?, lon, lat, x, y}``); a parent that is itself somebody's extra
    top is replaced by its root, at write time and again at read time.
  * read side (``ne_trees._flags`` → ``collect``): the extra apex is hidden like
    any reader-flagged apex; its ``crown_m2`` (kept in ``observed`` at write
    time) is added to the parent's row.  No corpus rebuild, no extra query —
    the rows ride in the per-KG flag cache that exists anyway.
  * ``labels()`` = the merge pairs as a training / tuning set for the apex
    finder (``python3 tree_crown.py --labels``).
"""
from __future__ import annotations

import json
import math

import numpy as np

HINT = 'crown'
OF_MAX_M = 25.0              # "of" must be an apex within this distance of the extra top
APEX_M = 2.5                 # a point names the apex within this (= ne_trees.FLAG_APEX_M)
ID_M = 1.5                   # a tree_id names the apex within this (tree_user.ID_SHIFT_M)
MAX_HOPS = 8
_KEY_FLOOR = -(1 << 32)      # apex subject keys lie below; KG subjects are -1 … -99999


def apex_skey(x, y) -> int:
    return -((int(x) << 32) | int(y))


def is_apex_skey(skey) -> bool:
    return skey is not None and int(skey) < _KEY_FLOOR


def skey_xy(skey) -> tuple[int, int]:
    k = -int(skey)
    return k >> 32, k & 0xFFFFFFFF


def is_crown(expected) -> bool:
    return isinstance(expected, dict) and str(expected.get('hint') or '').strip().lower() == HINT


def _bad(msg: str, **x):
    import public_flags as pf
    return pf.FlagError(400, msg, reason='bad_of', **x)


def _nearest(c: dict, px: float, py: float, tol: float, h=None):
    d = np.hypot(c['x'] - px, c['y'] - py)
    near = np.flatnonzero(d <= tol)
    if not near.size:
        return None
    if h is not None:
        same = near[np.abs(c['h'][near] - h) <= 0.26]
        if same.size:
            near = same
    return int(near[np.argmin(d[near])])


def _ids(c):
    import public_flags as pf
    pf._load_dict(c)
    return pf._DICT.get(('layer', 'trees')), pf._DICT.get(('claim', 'spurious'))


def _parent_of(c, x: int, y: int):
    """The stored ``of`` of an open / confirmed crown flag whose subject is apex (x, y) — newest wins."""
    lid, cid = _ids(c)
    if lid is None or cid is None:
        return None
    for (e,) in c.execute('SELECT expected FROM report WHERE skey=? AND layer=? AND claim=? AND status IN (0,1) '
                          'ORDER BY ts_last DESC', (apex_skey(x, y), lid, cid)):
        try:
            o = (json.loads(e) if e else {}).get('of')
            if isinstance(o, dict) and 'x' in o and 'y' in o:
                return o
        except ValueError:
            pass
    return None


def bind(c, body: dict, subj: dict) -> dict:
    """Validate a crown-hinted spurious flag → {skey, expected, observed, public, warnings}.
    Raises FlagError (404 no_apex / 400 bad_of) before anything is written."""
    import public_flags as pf
    import tree_user as tu
    exp = dict(body['expected'])
    of = exp.get('of')
    if not isinstance(of, dict):
        raise _bad('hint "crown" needs "of": {tree_id | lon, lat} — the tree this top belongs to')
    px, py = tu._to_3035(subj['lon'], subj['lat'])
    ap = tu._apices_near(subj['lon'], subj['lat'], OF_MAX_M + 2 * APEX_M)
    obs = body.get('observed') if isinstance(body.get('observed'), dict) else {}
    try:
        oh = float(obs['h_m']) if obs.get('h_m') is not None else None
    except (TypeError, ValueError):
        oh = None
    s = _nearest(ap, px, py, ID_M if body.get('tree_id') else APEX_M, oh) if ap is not None else None
    if s is None:
        raise pf.FlagError(404, 'no apex of ours at the subject point — hint "crown" flags an apex of /api/v1/trees/bbox',
                           reason='no_apex')
    tid = None
    try:
        tid = tu.parse_tree_id(of.get('tree_id'))
    except pf.FlagError as e:
        raise _bad(f'of.tree_id: {e.msg}')
    if tid is not None:
        ox, oy, tol = tid[0] / 10.0, tid[1] / 10.0, ID_M
    else:
        try:
            lon, lat = float(of['lon']), float(of['lat'])
            if not pf._in_at(lon, lat):
                raise ValueError
        except (KeyError, TypeError, ValueError):
            raise _bad('"of" needs a tree_id or lon, lat inside Austria')
        (ox, oy), tol = tu._to_3035(lon, lat), APEX_M
    o = _nearest(ap, ox, oy, tol)
    if o is None:
        raise _bad(f'"of" names no apex of ours within {OF_MAX_M:g} m of the subject')
    if o == s:
        raise _bad('"of" is the subject itself — name the tree the extra top belongs to')
    sx, sy = int(ap['x'][s]), int(ap['y'][s])
    dist = float(math.hypot(int(ap['x'][o]) - sx, int(ap['y'][o]) - sy))
    if dist > OF_MAX_M:
        raise _bad(f'"of" is {dist:.1f} m from the subject (max {OF_MAX_M:g} m)', dist_m=round(dist, 1))
    warnings = []
    if float(ap['h'][s]) > float(ap['h'][o]) + 0.26:
        warnings.append('the subject is taller than "of" — flag the LOWER top and name the taller one in "of"')
    root = {'lon': round(float(ap['lon'][o]), 6), 'lat': round(float(ap['lat'][o]), 6),
            'x': int(ap['x'][o]), 'y': int(ap['y'][o])}
    if tid is not None:
        root['tree_id'] = tu.tree_id_str(tid)
    else:                                           # an id somebody already reported this apex under
        try:
            st = tu.state(int(ap['kg'][o])); i = tu._state_idx(st, root['x'], root['y'])
            if i is not None and st['tid'][i] and not st['added'][i]:
                root['tree_id'] = st['tid'][i]
        except Exception:  # noqa: BLE001
            pass
    rebound = False
    seen = {(root['x'], root['y'])}
    for _ in range(MAX_HOPS):                       # "of" is itself an extra top → its root (no chains)
        p = _parent_of(c, root['x'], root['y'])
        if p is None:
            break
        k = (int(p['x']), int(p['y']))
        if k == (sx, sy) or k in seen:
            raise _bad('"of" is itself flagged as an extra top of the subject — withdraw that flag first')
        seen.add(k)
        root = {k_: p[k_] for k_ in ('tree_id', 'lon', 'lat', 'x', 'y') if p.get(k_) is not None}
        rebound = True
    exp.update(absent=True, hint=HINT, of=root)
    observed = dict(obs)
    observed.setdefault('h_m', round(float(ap['h'][s]), 2))
    observed.setdefault('crown_m2', round(float(ap['crown_m2'][s]), 1))
    pub = {'of': root, 'dist_m': round(dist, 1)}
    if rebound:
        pub['rebound'] = True                       # your "of" was an extra top itself: bound to its tree
    kg = str(ap['kg'][s]).zfill(5)                  # the KG FILE that serves the apex (read side is per file)
    return {'skey': apex_skey(sx, sy), 'kg': kg, 'epoch': pf._epoch_now(kg), 'expected': exp, 'observed': observed, 'public': pub, 'warnings': warnings}


def own_key(c, subj: dict, rep: bytes):
    """Retract of trees / spurious at a point: the apex-keyed row it means — the reporter's own,
    else their withdrawn marker, else somebody else's (→ 403); None = the cell row as before."""
    import tree_user as tu
    lid, cid = _ids(c)
    if lid is None or cid is None:
        return None
    px, py = tu._to_3035(subj['lon'], subj['lat'])
    x0, y0 = int(math.floor(px - APEX_M)), int(math.floor(py - APEX_M))
    n = int(2 * APEX_M) + 2
    keys = sorted(((x - px) ** 2 + (y - py) ** 2, apex_skey(x, y)) for x in range(x0, x0 + n) for y in range(y0, y0 + n)
                  if (x - px) ** 2 + (y - py) ** 2 <= APEX_M ** 2)
    ks = [k for _, k in keys]
    q = ','.join('?' * len(ks))
    rows = c.execute(f'SELECT skey, reporter FROM report WHERE skey IN ({q}) AND layer=? AND claim=?', (*ks, lid, cid)).fetchall()
    mine = {r[0] for r in rows if bytes(r[1]) == rep}
    wd = {r[0] for r in c.execute(f'SELECT skey FROM withdrawn WHERE skey IN ({q}) AND layer=? AND claim=? AND reporter=?',
                                  (*ks, lid, cid, rep))}
    other = {r[0] for r in rows}
    if not mine and c.execute('SELECT 1 FROM report WHERE skey=? AND layer=? AND claim=? AND reporter=?',
                              (subj['skey'], lid, cid, rep)).fetchone():
        return None                                 # their plain (cell-keyed) flag is the live one
    for pool in (mine, wd, other):
        k = next((k for k in ks if k in pool), None)
        if k is not None:
            return k
    return None


# === SECTION: read side (called from ne_trees._flags, cached there per KG) ===

def merges(rows, epoch_now):
    """rows = [(skey, expected json, observed json, epoch)] of one KG's open / confirmed tree flags →
    (parent keys int64 (x << 32 | y), crown_m2 to add) sorted by key, chains resolved to the root.  Only flags
    made against the product epoch being served take part (after a rebuild the extra top may be gone)."""
    par: dict = {}; add: dict = {}
    for skey, e, o, ep in rows:
        if not is_apex_skey(skey) or not e or '"crown"' not in e:
            continue
        try:
            of = json.loads(e).get('of') or {}
            k = (int(of['x']), int(of['y']))
        except (ValueError, KeyError, TypeError, AttributeError):
            continue
        ch = skey_xy(skey)
        par[ch] = k
        if epoch_now and ep == epoch_now and ch not in add:
            try:
                add[ch] = float((json.loads(o) if o else {}).get('crown_m2') or 0.0)
            except (ValueError, TypeError, AttributeError):
                pass
    tot: dict = {}
    for ch, a in add.items():
        k = par[ch]; seen = {ch}
        while k in par and k not in seen and len(seen) <= MAX_HOPS:
            seen.add(k); k = par[k]
        if a > 0 and k not in seen:
            tot[k] = tot.get(k, 0.0) + a
    if not tot:
        return None
    ks = sorted(tot)
    return (np.array([(x << 32) | y for x, y in ks], np.int64), np.array([tot[k] for k in ks], np.float64))


def apply(out: dict, ms: list) -> int:
    """Add the merged crown areas to the parents present in a collect() result (column dict)."""
    mk = np.concatenate([m[0] for m in ms]); mv = np.concatenate([m[1] for m in ms])
    o = np.argsort(mk, kind='stable'); mk = mk[o]; mv = mv[o]
    key = (out['x'].astype(np.int64) << 32) | out['y'].astype(np.int64)
    pos = np.searchsorted(mk, key); pos[pos >= mk.size] = 0
    hit = np.flatnonzero(mk[pos] == key)
    if hit.size:
        out['crown_m2'][hit] += mv[pos[hit]].astype(out['crown_m2'].dtype)
    return int(hit.size)


# === SECTION: training labels ===

def labels(kg: str | None = None, limit: int = 100000) -> list:
    """Merge pairs (over-segmented crowns): one per flagged extra top, root-resolved."""
    import public_flags as pf
    c = pf._conn()
    lid, cid = _ids(c)
    if lid is None or cid is None:
        return []
    q = ('SELECT skey, kg, expected, observed, epoch, status, COUNT(*) AS reps, SUM(n) AS n, MAX(ts_last) AS ts '
         'FROM report WHERE layer=? AND claim=? AND status IN (0,1) AND skey < ? AND expected LIKE ?')
    a = [lid, cid, _KEY_FLOOR, f'%"hint":"{HINT}"%']
    if kg:
        q += ' AND kg=?'; a.append(str(kg).zfill(5))
    rows = c.execute(q + ' GROUP BY skey ORDER BY ts DESC LIMIT ?', (*a, int(limit))).fetchall()
    par = {}
    for r in rows:
        try:
            of = json.loads(r['expected'])['of']; par[skey_xy(r['skey'])] = (int(of['x']), int(of['y']))
        except (ValueError, KeyError, TypeError):
            pass
    out = []; ep_now: dict = {}
    for r in rows:
        ch = skey_xy(r['skey'])
        if ch not in par:
            continue
        k = par[ch]; seen = {ch}
        while k in par and k not in seen and len(seen) <= MAX_HOPS:
            seen.add(k); k = par[k]
        if k in seen:
            continue
        try:
            o = json.loads(r['observed']) if r['observed'] else {}
        except ValueError:
            o = {}
        if r['kg'] not in ep_now:
            ep_now[r['kg']] = pf._epoch_now(r['kg'])
        out.append({'kg': r['kg'], 'x': ch[0], 'y': ch[1], 'h_m': o.get('h_m'), 'crown_m2': o.get('crown_m2'),
                    'of_x': k[0], 'of_y': k[1], 'dist_m': round(math.hypot(k[0] - ch[0], k[1] - ch[1]), 1),
                    'reporters': r['reps'], 'confirmed': r['status'] == 1, 'epoch': r['epoch'],
                    'superseded': bool(r['epoch'] and ep_now[r['kg']] and ep_now[r['kg']] != r['epoch']), 'ts': r['ts']})
    return out


if __name__ == '__main__':
    import argparse
    ap_ = argparse.ArgumentParser()
    ap_.add_argument('--labels', action='store_true', help='JSONL of merge pairs (EPSG:3035 metres)')
    ap_.add_argument('--kg'); ap_.add_argument('--status', action='store_true')
    a_ = ap_.parse_args()
    ls = labels(a_.kg)
    if a_.labels:
        for l in ls:
            print(json.dumps(l, separators=(',', ':')))
    else:
        print(json.dumps({'pairs': len(ls), 'kgs': len({l['kg'] for l in ls}), 'superseded': sum(l['superseded'] for l in ls),
                          'multi_reporter': sum(l['reporters'] > 1 for l in ls)}))
