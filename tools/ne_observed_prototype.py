#!/usr/bin/env python3
"""PROTOTYPE (not wired in): observed-layer per H3 res-12 NE cell from our light v2 GPKG.

Status 2026-10-03: part A (umfeld-at NE cell sanity/privacy check) RUN and measured.
Part B (raster → per-cell observed record) WRITTEN BUT NOT RUN — the 63330 light_v2
GPKG download (897 MB, Zenodo @~1.5 MB/s) was still in flight when work was stopped.
Usage: python3 tools/ne_observed_prototype.py <ne_cells.json[.gz]> [<light_v2.gpkg>]
"""
import sys, json, gzip, io, time, zlib, struct, sqlite3, collections, statistics as st
import numpy as np, h3

GROUPS = ['geb','bau','acker','gruen','wald','wasser','verkehr','alpen','sonst']
# our segment_type codes (object_segmentation.ALL_TYPE_NAMES) → 9 NE groups
SEG2GRP = {10:'geb',11:'geb',110:'geb',
           12:'bau',15:'bau',16:'bau',19:'bau',22:'bau',50:'bau',51:'bau',53:'bau',54:'bau',130:'bau',131:'bau',
           30:'acker',40:'acker',120:'acker',
           2:'gruen',3:'gruen',4:'gruen',31:'gruen',32:'gruen',33:'gruen',103:'gruen',121:'gruen',122:'gruen',
           1:'wald',52:'wald',101:'wald',102:'wald',
           5:'wasser',6:'wasser',106:'wasser',
           20:'verkehr',21:'verkehr',23:'verkehr',24:'verkehr',115:'verkehr',
           41:'alpen',42:'alpen'}
LUT = np.full(256, GROUPS.index('sonst'), np.uint8)
for k,g in SEG2GRP.items(): LUT[k] = GROUPS.index(g)
A12 = h3.average_hexagon_area(12, unit='m^2')

def load_ne(path):
    raw = open(path,'rb').read()
    if raw[:2] == b'\x1f\x8b': raw = gzip.decompress(raw)
    return json.loads(raw)

def sanity(cells):
    """Part A — measured on Kohlschwarz 63330 (see /tmp/obs_prototype.md)."""
    out = {}
    land = np.array([r['land'] for r in cells.values()]); slu = np.array([sum(r['lu'].values())/255 for r in cells.values()])
    cov = np.array([r['cov'] for r in cells.values()], float)
    out['n'] = len(cells); out['land_eq1'] = int((land>=0.999).sum()); out['land_lt_0.5'] = int((land<0.5).sum())
    out['cov_lt_0.9'] = int((cov<0.9).sum()); out['cov_gt_1.05'] = int((cov>1.05).sum()); out['cov_max'] = float(cov.max())
    out['lu_empty'] = int((slu==0).sum()); out['slu_gt_1'] = int((slu>1).sum())
    out['bld_n_hist'] = sorted(collections.Counter(r['bld_n'] for r in cells.values()).items())
    one = [r for r in cells.values() if r['bld_n']==1]
    out['bld_n_eq1'] = len(one); out['bld_n_eq1_coh1'] = sum(1 for r in one if r.get('bld_orient') and r['bld_orient'][0]>=0.99)
    iso = [r['bld_cover']*A12 for c,r in cells.items() if r['bld_n']==1 and
           all(cells.get(n,{}).get('lu',{}).get('geb',0)==0 for n in h3.grid_disk(c,1) if n!=c)]
    out['bld_n_eq1_isolated'] = len(iso); out['isolated_area_p10_50_90'] = np.percentile(iso,[10,50,90]).round().tolist() if iso else None
    out['k_rule_cells'] = sum(1 for r in cells.values() if r['n_parc']>=5); out['bld_p50_cells'] = sum(1 for r in cells.values() if 'bld_p50' in r)
    out['geb_cells_no_centroid'] = sum(1 for r in cells.values() if r['bld_n']==0 and r['lu'].get('geb',0)>0)
    return out

# ---------------- Part B (not yet run) ----------------
def read_segment_type(gpkg):
    """palette PNG tiles → uint8 array + affine (like v2_verify._segment_type_nonzero)."""
    from PIL import Image; import rasterio
    c = sqlite3.connect(f'file:{gpkg}?mode=ro', uri=True)
    z = c.execute("SELECT MAX(zoom_level) FROM gpkg_tile_matrix WHERE table_name='segment_type'").fetchone()[0]
    tw,th,mw,mh = c.execute("SELECT tile_width,tile_height,matrix_width,matrix_height FROM gpkg_tile_matrix WHERE table_name='segment_type' AND zoom_level=?",(z,)).fetchone()
    full = np.zeros((mh*th, mw*tw), np.uint8)
    for col,rw,blob in c.execute("SELECT tile_column,tile_row,tile_data FROM segment_type WHERE zoom_level=?",(z,)):
        im = Image.open(io.BytesIO(blob)); a = np.array(im if im.mode in ('P','L') else im.convert('L'), np.uint8)
        full[rw*th:rw*th+a.shape[0], col*tw:col*tw+a.shape[1]] = a
    with rasterio.open(f'GPKG:{gpkg}:segment_type') as ds: tf = ds.transform
    return full, tf

def cell_labels(cell_ids, shape, tf, method='rasterize'):
    """H3 cells → int32 label raster (index+1 into cell_ids, 0 = none). Returns (labels, seconds).
    method 'rasterize': h3.cell_to_boundary → shapely → rasterio.features.rasterize (EPSG:4326→3035 via pyproj).
    method 'pixels': h3.latlng_to_cell on pixel centres at `stride` m (scalar h3 call ≈0.5 µs → 16 M px ≈ 9 s; 5 m stride ≈ 0.4 s) then np.repeat."""
    import rasterio.features, pyproj
    from shapely.geometry import Polygon
    from shapely.ops import transform as shtf
    t = time.time()
    to3035 = pyproj.Transformer.from_crs(4326, 3035, always_xy=True).transform
    if method == 'rasterize':
        shapes = ((shtf(to3035, Polygon([(lng,lat) for lat,lng in h3.cell_to_boundary(c)])), i+1) for i,c in enumerate(cell_ids))
        lab = rasterio.features.rasterize(shapes, out_shape=shape, transform=tf, dtype='int32', all_touched=False)
    else:
        stride = 5; idx = {c:i+1 for i,c in enumerate(cell_ids)}
        rows = np.arange(0, shape[0], stride); cols = np.arange(0, shape[1], stride)
        xs, ys = tf * (cols[None,:]+0.5, rows[:,None]+0.5)
        to4326 = pyproj.Transformer.from_crs(3035, 4326, always_xy=True).transform
        lng, lat = to4326(np.broadcast_to(xs,(len(rows),len(cols))).ravel(), np.broadcast_to(ys,(len(rows),len(cols))).ravel())
        small = np.array([idx.get(h3.latlng_to_cell(a,b,12),0) for a,b in zip(lat,lng)], np.int32).reshape(len(rows),len(cols))
        lab = np.repeat(np.repeat(small, stride, 0), stride, 1)[:shape[0],:shape[1]]
    return lab, time.time()-t

def observed(cells, gpkg):
    import rasterio, fiona, pyproj
    seg, tf = read_segment_type(gpkg)
    ids = list(cells); lab, t_r = cell_labels(ids, seg.shape, tf); n = len(ids)+1
    cnt = np.bincount(lab.ravel(), minlength=n).astype(float); cnt[cnt==0] = np.nan
    grp = LUT[seg]
    obs_lu = np.stack([np.bincount(lab.ravel(), weights=(grp==g).ravel(), minlength=n)/cnt for g in range(9)], 1)
    roof = np.bincount(lab.ravel(), weights=np.isin(seg,(10,11,110)).ravel(), minlength=n)/cnt
    with rasterio.open(f'GPKG:{gpkg}:segment_height') as ds: h = ds.read(1).astype(float)  # nDSM 1 m
    h = h[:seg.shape[0],:seg.shape[1]]; hv = np.nan_to_num(h, nan=0.0)
    h_mean = np.bincount(lab.ravel(), weights=hv.ravel(), minlength=n)/cnt
    order = np.lexsort((hv.ravel(), lab.ravel())); ls = lab.ravel()[order]; hs = hv.ravel()[order]
    starts = np.searchsorted(ls, np.arange(n)); ends = np.searchsorted(ls, np.arange(n), 'right')
    h_max = np.where(ends>starts, hs[np.maximum(ends-1,0)], np.nan)
    h_p90 = np.where(ends>starts, hs[np.minimum(starts+((ends-starts)*0.9).astype(int), len(hs)-1)], np.nan)
    with rasterio.open(f'GPKG:{gpkg}:terrain_coarse_dtm') as ds: dtm = ds.read(1).astype(float)   # 5 m
    with rasterio.open(f'GPKG:{gpkg}:terrain_coarse_slope') as ds: slp = ds.read(1).astype(float)
    lab5 = lab[::5, ::5][:dtm.shape[0],:dtm.shape[1]]; dtm = dtm[:lab5.shape[0],:lab5.shape[1]]; slp = slp[:lab5.shape[0],:lab5.shape[1]]
    c5 = np.bincount(lab5.ravel(), minlength=n).astype(float); c5[c5==0] = np.nan
    elev = np.bincount(lab5.ravel(), weights=dtm.ravel(), minlength=n)/c5; slope = np.bincount(lab5.ravel(), weights=slp.ravel(), minlength=n)/c5
    tree_n = np.zeros(n, int); tree_h = np.zeros(n)
    to4326 = pyproj.Transformer.from_crs(3035, 4326, always_xy=True).transform; idx = {c:i+1 for i,c in enumerate(ids)}
    with fiona.open(gpkg, layer='tree_apices') as src:
        for f in src:
            x,y = f['geometry']['coordinates'][:2]; lng,lat = to4326(x,y); i = idx.get(h3.latlng_to_cell(lat,lng,12))
            if i: tree_n[i]+=1; tree_h[i] = max(tree_h[i], float(f['properties'].get('height_m') or f['properties'].get('height') or 0))
    import rasterio.features
    with fiona.open(gpkg, layer='tree_crowns') as src:
        crown = rasterio.features.rasterize(((f['geometry'],1) for f in src), out_shape=seg.shape, transform=tf, dtype='uint8')
    crown_frac = np.bincount(lab.ravel(), weights=crown.ravel(), minlength=n)/cnt
    recs = {}
    for i,c in enumerate(ids, 1):
        if np.isnan(cnt[i]): continue
        d = cells[c]; lu = {g: int(round(obs_lu[i,gi]*255)) for gi,g in enumerate(GROUPS) if obs_lu[i,gi] >= 1/255}
        recs[c] = dict(obs_lu=lu, obs_bld_cover=round(float(roof[i]),3), h_mean=round(float(h_mean[i]),1), h_max=round(float(h_max[i]),1),
                       h_p90=round(float(h_p90[i]),1), tree_n=int(tree_n[i]), tree_h_max=round(float(tree_h[i]),1),
                       crown_frac=round(float(crown_frac[i]),3), elev_mean=round(float(elev[i]),1), slope_mean=round(float(slope[i]),1),
                       dh_mean=None,  # multi-date DTM only in full GPKG (v1) — not in light v2
                       verdict=verdict(d, lu, roof[i], h_p90[i]))
    return recs, t_r

def verdict(decl, obs, roof, hp90):
    D = {k:v/255 for k,v in decl['lu'].items()}; O = {k:v/255 for k,v in obs.items()}
    sealedD = D.get('geb',0)+D.get('bau',0)+D.get('verkehr',0)
    if D.get('wald',0) >= .5 and O.get('wald',0) < .2 and (np.isnan(hp90) or hp90 < 3): return 'forest_loss'
    if D.get('wald',0) < .2 and O.get('wald',0) >= .5 and hp90 >= 5: return 'forest_gain'
    if D.get('geb',0) < .02 and roof >= .10 and hp90 >= 2.5: return 'building_undeclared'
    if sealedD < .1 and O.get('bau',0)+O.get('verkehr',0) >= .4: return 'sealed_undeclared'
    if sealedD >= .6 and O.get('gruen',0)+O.get('acker',0) >= .6 and roof < .05: return 'green_undeclared'
    if not D or not O: return 'mismatch'
    d = max(D, key=D.get); o = max(O, key=O.get)
    return 'consistent' if d == o or {d,o} in ({'gruen','acker'},{'alpen','gruen'},{'alpen','sonst'}) else 'mismatch'

VERD = ['consistent','forest_loss','forest_gain','sealed_undeclared','building_undeclared','green_undeclared','mismatch']
def pack(c, r):
    b = struct.pack('<Q', int(c,16)) + bytes([len(r['obs_lu'])]) + b''.join(bytes([GROUPS.index(k),v]) for k,v in r['obs_lu'].items())
    q = lambda v,s,m: int(min(m, max(0, round((v or 0)*s))))
    return b + struct.pack('<BBBBBBBHBB', q(r['obs_bld_cover'],255,255), q(r['h_mean'],2,255), q(r['h_max'],2,255), q(r['h_p90'],2,255),
                           min(255,r['tree_n']), q(r['tree_h_max'],2,255), q(r['crown_frac'],255,255), q(r['elev_mean'],1,65535),
                           q(r['slope_mean'],1,255), VERD.index(r['verdict']))

def measure(cells, recs):
    blob = b''.join(pack(c,r) for c,r in sorted(recs.items())); z = zlib.compress(blob, 9)
    js = json.dumps(recs).encode(); jz = gzip.compress(js, 9)
    conf = collections.defaultdict(float)
    for c,r in recs.items():
        d = cells[c]['lu']; o = r['obs_lu']
        if d and o: conf[(max(d,key=d.get), max(o,key=o.get))] += A12/1e4
    decl_geb = sum(cells[c]['lu'].get('geb',0)/255*A12 for c in recs); obs_geb = sum(r['obs_bld_cover']*A12 for r in recs.values())
    inter = sum(min(cells[c]['lu'].get('geb',0)/255, r['obs_bld_cover'])*A12 for c,r in recs.items())
    roll = {res: len({h3.cell_to_parent(c,res) for c in recs}) for res in (11,10)}
    return dict(bin_raw=len(blob), bin_deflate=len(z), bin_B_per_cell=len(z)/len(recs), json_gz=len(jz), json_gz_B_per_cell=len(jz)/len(recs),
                rollup_cells=roll, rollup_bytes_est={r: n*len(z)/len(recs) for r,n in roll.items()},
                geb_precision=inter/obs_geb if obs_geb else None, geb_recall=inter/decl_geb if decl_geb else None,
                confusion_ha={f'{a}->{b}': round(v,2) for (a,b),v in sorted(conf.items(), key=lambda kv:-kv[1])},
                verdicts=collections.Counter(r['verdict'] for r in recs.values()))

if __name__ == '__main__':
    ne = load_ne(sys.argv[1]); cells = ne['cells']
    print(json.dumps(sanity(cells), indent=1, default=str))
    if len(sys.argv) > 2:
        t = time.time(); recs, t_r = observed(cells, sys.argv[2])
        print('observed layer: %d cells, %.1f s total (label raster %.1f s)' % (len(recs), time.time()-t, t_r))
        print(json.dumps(measure(cells, recs), indent=1, default=str))
        json.dump(recs, open('/tmp/obs_proto/observed.json','w'))
