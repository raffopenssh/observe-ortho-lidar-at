"""Check A: off-file apices vs. the nearest apex another KG file serves."""
import sys, os, json, time, numpy as np
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import ne_rebin, cells_api as ca, ne_trees
h3 = ne_rebin._h3n()

def others(kg, off, shifts=None):
    """per off apex: (stored_elsewhere bool, served_elsewhere bool, nearest dist, dh, dx, dy, other kg)"""
    n = off['idx'].size
    par = np.fromiter((h3.cell_to_parent(int(c), 10) for c in off['cell']), np.uint64, count=n)
    need = set()
    for p in np.unique(par).tolist():
        need.update(int(c) for c in h3.grid_disk(int(p), 1))
    rows = [r for r in ca._chunk_rows(np.array(sorted(need), np.uint64))]
    by = {}
    for r in rows: by.setdefault(int(r['r10']), []).append(r)
    X=[];Y=[];H=[];K=[]; stored=set(); hdrs={}
    for r10, rs in by.items():
        for r in rs:
            if r['kg'] == kg: continue
            hd = hdrs.get(r['kg']) or hdrs.setdefault(r['kg'], ca.header(r['kg']))
            if hd is None: continue
            ch = ca._load_chunk(r['kg'], r, hd, qa=False)
            stored.update(ch.ids.tolist())
            d = ne_trees._decoded_chunk(r['kg'], r, hd, 'trees')
            if not d or not d['n']: continue
            m = ne_trees._own_mask(d, r['kg'], r10, rs) if len(rs) > 1 else None
            sel = slice(None) if m is None else m
            X.append(d['x'][sel]); Y.append(d['y'][sel]); H.append(d['h'][sel]); K.append(np.full(len(d['x'][sel]), int(r['kg'])))
    st_else = np.fromiter((int(c) in stored for c in off['cell']), bool, count=n)
    dist = np.full(n, np.inf); dh = np.full(n, np.nan); ddx = np.full(n, np.nan); ddy = np.full(n, np.nan); ok = np.zeros(n, np.int64); extra = {tuple(s): np.full(n, np.nan) for s in shifts or ()}
    if X:
        X=np.concatenate(X).astype(np.float64);Y=np.concatenate(Y).astype(np.float64);H=np.concatenate(H);K=np.concatenate(K)
        from scipy.spatial import cKDTree
        t = cKDTree(np.c_[X, Y])
        dd, ii = t.query(np.c_[off['x'], off['y']].astype(np.float64), k=1)
        dist = dd; dh = off['h_m'] - H[ii]; ddx = X[ii] - off['x']; ddy = Y[ii] - off['y']; ok = K[ii]
        if shifts:      # best match (smallest |dh| within Chebyshev 2 m) around pos + shift, per shift
            P = np.c_[off['x'], off['y']].astype(np.float64)
            for s in shifts:
                best = np.full(n, np.nan)
                for i, nb in enumerate(t.query_ball_point(P + np.array(s, float), 2.0, p=np.inf)):
                    if nb:
                        best[i] = np.abs(off['h_m'][i] - H[nb]).min()
                extra[tuple(s)] = best
    if shifts:
        return st_else, dist, dh, ddx, ddy, ok, extra
    return st_else, dist, dh, ddx, ddy, ok

if __name__ == '__main__':
    DB=[0,1,2,3,5,1e9]; HB=[0.5,1,2,1e9]
    tot = np.zeros((5,4), int)
    for kg in sys.argv[1:]:
        t0=time.time(); st, off = ne_rebin.scan(kg)
        se, dist, dh, ddx, ddy, ok = others(kg, off)
        print(json.dumps(st), 'other_file', int(se.sum()), 'no_row', int((~se).sum()), f'{time.time()-t0:.0f}s')
        m = se
        hist = np.zeros((5,4), int)
        di = np.clip(np.searchsorted(DB, dist[m], side='right')-1, 0, 4); hi = np.searchsorted(HB, np.abs(dh[m]), side='left').clip(0,3)
        np.add.at(hist, (di, hi), 1); tot += hist
        print('  dist\\|dh|  <=0.5  <=1  <=2  >2');
        for i,l in enumerate(['0-1','1-2','2-3','3-5','>5']): print(f'  {l:>5} m', hist[i])
        near = m & (dist < 12)
        if near.any():
            # offsets of the nearest match (frame-offset clustering?)
            from collections import Counter
            c = Counter(zip(ddx[near].astype(int).tolist(), ddy[near].astype(int).tolist(), )).most_common(6)
            print('  top (dx,dy) of nearest other-file apex:', c, ' exact pos+h:', int(((dist==0)&(dh==0)&m).sum()))
            print('  other kgs:', Counter(ok[m].tolist()).most_common(5), 'no_row nearest<2m&|dh|<=1:', int(((~se)&(dist<=2)&(np.abs(dh)<=1)).sum()))
    print('TOTAL'); print(tot)
