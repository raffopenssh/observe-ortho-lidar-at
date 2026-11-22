"""The measured model behind rebin_added.TWIN_DH (docs/tree-cell-assignment.md → Twin rule tw1).

    nice python3 tools/rebin_twin_model.py            # collect (≈ 10 min, cached in /tmp/rebin_twin_ev.pkl) + report
    python3 tools/rebin_twin_model.py --report        # report from the cache

Sample (fixed seeds): 92124, 01005, 61047, 30003 + 25 frame + 25 plain KGs (seed 1) + 20 frame + 60 plain (seed 2).
Per off-file apex A (height class of A) and per apex B another file serves near it:
  observed  pairs per (Chebyshev offset D, |dh| band)
  chance    pairs expected if A and B were unrelated trees = (density of other-file apices 8–12 m around A, same
            |dh| band) × g(D, band), g = pair density of two DIFFERENT apices of ONE file at that offset relative to
            6–8 m (what the detector can put side by side; max of raw and 3×3-smoothed, two files' rasters may sit
            one metre apart)
  P(twin)   = 1 − chance / observed.  A cell (class, D, band) is a twin cell when P ≥ 0.5 (the decision with the
            smallest expected count error), bands contiguous from dh = 0, |dh| ≤ 2 m at most (beyond: two
            inventories that disagree about the spot, the apex is kept)."""
import os
import pickle
import random
import sys
import glob
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np

CACHE = '/tmp/rebin_twin_ev.pkl'
HC = [0, 5, 10, 20, 30, 99]; HL = ['<5', '5-10', '10-20', '20-30', '>=30']
DHB = [0, 0.25, 0.5, 1, 2, 99]; DL = ['=0', '<=.25', '<=.5', '<=1', '<=2', '>2']; NB = len(DL)
R = 12.0


def collect():
    import ne_rebin, cells_api as ca, ne_trees, ne_pos_fix
    from scipy.spatial import cKDTree
    h3 = ne_rebin._h3n()

    def ev(kg, off):
        n = off['idx'].size
        par = np.fromiter((h3.cell_to_parent(int(c), 10) for c in off['cell']), np.uint64, count=n)
        need = set()
        for p in np.unique(par).tolist():
            need.update(int(c) for c in h3.grid_disk(int(p), 1))
        by = {}
        for r in ca._chunk_rows(np.array(sorted(need), np.uint64)):
            by.setdefault(int(r['r10']), []).append(r)
        X = []; Y = []; H = []; K = []; stored = set(); hdrs = {}
        for r10, rs in by.items():
            for r in rs:
                if r['kg'] == kg:
                    continue
                hd = hdrs.get(r['kg']) or hdrs.setdefault(r['kg'], ca.header(r['kg']))
                if hd is None:
                    continue
                stored.update(ca._load_chunk(r['kg'], r, hd, qa=False).ids.tolist())
                d = ne_trees._decoded_chunk(r['kg'], r, hd, 'trees')
                if not d or not d['n']:
                    continue
                m = ne_trees._own_mask(d, r['kg'], r10, rs) if len(rs) > 1 else None
                sel = slice(None) if m is None else m
                X.append(d['x'][sel]); Y.append(d['y'][sel]); H.append(d['h'][sel]); K.append(np.full(len(d['x'][sel]), int(r['kg'])))
        out = dict(kg=kg, se=np.fromiter((int(c) in stored for c in off['cell']), bool, count=n), h=off['h_m'].astype(np.float32),
                   frame=ne_pos_fix.offset(kg))
        if X:
            X = np.concatenate(X).astype(np.float64); Y = np.concatenate(Y).astype(np.float64)
            H = np.concatenate(H).astype(np.float32); K = np.concatenate(K)
            t = cKDTree(np.c_[X, Y]); P = np.c_[off['x'], off['y']].astype(np.float64)
            nb = t.query_ball_point(P, R)
            ii = np.repeat(np.arange(n), [len(v) for v in nb]); jj = np.fromiter((j for v in nb for j in v), np.int64)
            out.update(ni=ii.astype(np.int32), ndx=(X[jj] - P[ii, 0]).astype(np.float32), ndy=(Y[jj] - P[ii, 1]).astype(np.float32),
                       nh=H[jj], nk=K[jj].astype(np.int32))
            pr = t.query_pairs(8.0, output_type='ndarray')          # two apices of ONE file: what the detector resolves
            if len(pr):
                pr = pr[K[pr[:, 0]] == K[pr[:, 1]]]
                if len(pr) > 200000:
                    pr = pr[np.random.RandomState(0).choice(len(pr), 200000, replace=False)]
                out['pairs'] = np.c_[H[pr[:, 0]], H[pr[:, 1]], X[pr[:, 0]] - X[pr[:, 1]], Y[pr[:, 0]] - Y[pr[:, 1]]].astype(np.float32)
        return out

    fr = sorted(ne_pos_fix._load()['off']); random.seed(1)
    allk = sorted(os.path.basename(p)[:-4] for p in glob.glob('data/ne/*.h3b') if len(os.path.basename(p)) == 9)
    kgs = ['92124', '01005', '61047', '30003'] + random.sample(fr, 25) + random.sample([k for k in allk if k not in fr], 25)
    random.seed(2); kgs += [k for k in random.sample(fr, 20) + random.sample([k for k in allk if k not in fr], 60) if k not in kgs]
    E = []; t0 = time.time()
    for i, kg in enumerate(kgs):
        st, off = ne_rebin.scan(kg); e = ev(kg, off); e['st'] = st; E.append(e)
        if i % 20 == 19:
            print(f'  {i + 1}/{len(kgs)} KGs, {time.time() - t0:.0f}s', flush=True)
    pickle.dump(E, open(CACHE, 'wb'))
    return E


def band(dh):
    return np.where(dh == 0, 0, np.searchsorted(DHB, dh, side='left'))


def report(E):
    from scipy.ndimage import uniform_filter
    import rebin_added as ra
    A68 = np.pi * (64 - 36); A812 = np.pi * (144 - 64); W = 7
    smap = np.zeros((5, NB, 2 * W + 1, 2 * W + 1)); sring = np.zeros((5, NB))
    for e in E:
        p = e.get('pairs')
        if p is None:
            continue
        for a, sg in ((0, 1), (1, -1)):
            hc = np.clip(np.searchsorted(HC, p[:, a], side='right') - 1, 0, 4); db = band(np.abs(p[:, 0] - p[:, 1]))
            dx = (sg * p[:, 2]).astype(int); dy = (sg * p[:, 3]).astype(int); d = np.hypot(dx, dy)
            k = (np.abs(dx) <= W) & (np.abs(dy) <= W); np.add.at(smap, (hc[k], db[k], dx[k] + W, dy[k] + W), 1)
            k = (d > 6) & (d <= 8); np.add.at(sring, (hc[k], db[k]), 1)
    g = smap / (sring[:, :, None, None] / A68 + 1e-9)
    gb = np.maximum(g, uniform_filter(g, size=(1, 1, 3, 3), mode='constant'))
    yy, xx = np.mgrid[-W:W + 1, -W:W + 1]; CH = np.maximum(np.abs(xx), np.abs(yy))
    obs = np.zeros((5, 7, NB)); ann = np.zeros((5, NB)); N = np.zeros(5, int)
    fobs = np.zeros((5, 2)); stored = off = 0
    for e in E:
        stored += e['st']['stored']; off += e['se'].size
        if 'ni' not in e:
            continue
        se = e['se']; h = e['h']; hc = np.clip(np.searchsorted(HC, h, side='right') - 1, 0, 4); np.add.at(N, hc[se], 1)
        ni = e['ni']; dx = e['ndx']; dy = e['ndy']; dh = np.abs(e['nh'] - h[ni]); keep = se[ni]
        ch = np.maximum(np.abs(dx), np.abs(dy)).astype(int); d = np.hypot(dx, dy); db = band(dh)
        k = keep & (ch <= 6); np.add.at(obs, (hc[ni[k]], ch[k], db[k]), 1)
        k = keep & (d > 8) & (d <= 12); np.add.at(ann, (hc[ni[k]], db[k]), 1)
        fs = e['frame'] or (0, 0)                                   # frame rule: B exactly at A − (frame_A − frame_B)
        import ne_pos_fix
        fo = np.array([ne_pos_fix.offset(str(int(q)).zfill(5)) or (0, 0) for q in e['nk']]).reshape(-1, 2)
        k = keep & ((fo != np.array(fs)).any(1)) & (dx == fo[:, 0] - fs[0]) & (dy == fo[:, 1] - fs[1]) & (db <= 1)
        np.add.at(fobs, (hc[ni[k]], db[k]), 1)
    print(f'{len(E)} KGs, {stored} stored apices, {off} off-file; other_file per height class: {dict(zip(HL, N.tolist()))}')
    print('\nsame-file pair density g relative to 6-8 m (0 = the detector never puts two apices there), raw, all |dh|:')
    for c in range(5):
        print(f'  h {HL[c]:>6}: ' + '  '.join(f'D={s}: {smap[c][:, CH == s].sum() / max(1, (CH == s).sum()) / (sring[c].sum() / A68):.2f}' for s in range(0, 5)))
    lo = [r[0] for r in ra.TWIN_DH]
    print('\nobserved / chance → P(twin) %   [* = twin cell of the coded rule ' + ra.TWIN_ID + ', ~ = twin when the other apex is the taller one]')
    tot = {}
    for c in range(5):
        tol = ra.TWIN_DH[max(i for i, v in enumerate(lo) if v <= HC[c])][1]
        print(f'-- h {HL[c]} m (n={N[c]})' + ''.join(f'{l:>19}' for l in DL))
        fp = fn = dups = 0.0
        for s in range(0, 5):
            row = ''; derived = -1; open_ = True
            for b in range(NB):
                ex = ann[c, b] / A812 * gb[c, b][CH == s].sum(); o = obs[c, s, b]
                P = 1 - ex / o if o else 0.0
                coded = s <= ra.TWIN_D and DHB[b] <= tol[s] if b < NB - 1 else False
                if s <= 2:
                    if open_ and b < NB - 1 and ((o >= 5 and P >= 0.5) or (o < 5 and ex < 0.5)):
                        derived = b
                    else:
                        open_ = False
                    if coded:
                        fp += min(ex, o); dups += o
                    else:
                        fn += max(o - ex, 0)
                half = b == NB - 1 and s <= ra.TWIN_D and tol[s] >= ra.TWIN_COVER      # twin only when the other apex is the taller
                row += f'{int(o):6d}/{ex:6.1f} {("%3.0f" % (100 * P)) if o >= 5 else "  -":>4}{"*" if coded else "~" if half else " "}'
            print(f'   D={s} ' + row + (f'   derived max |dh|: {DL[derived] if derived >= 0 else "none"}' if s <= 2 else ''))
        tot[HL[c]] = (dups, fp, fn)
        print(f'   rule {ra.TWIN_ID}: {dups:.0f} twin pairs, of them ≈ {fp:.1f} chance pairs (a real tree not added); ≈ {fn:.1f} twins among the kept pairs (double count)')
    print('\nframe rule (other apex exactly at position − (frame_A − frame_B), files in different frames): pairs with dh = 0 / dh = 0.25')
    for c in range(5):
        print(f'  h {HL[c]:>6}: {int(fobs[c, 0])} / {int(fobs[c, 1])}')


if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if '--report' in sys.argv and os.path.exists(CACHE):
        E = pickle.load(open(CACHE, 'rb'))
    else:
        E = collect()
    report(E)
