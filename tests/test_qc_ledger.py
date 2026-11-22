"""QC ledger: detector join, tile-envelope correlation, timing, manifest link, guards.
Synthetic, no network, no live store:  python3 tests/test_qc_ledger.py"""
import json, os, sys, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
tmp = tempfile.mkdtemp()
os.environ['QC_LEDGER_DB'] = tmp + '/qc.sqlite'; os.environ['QC_LEDGER_SUMMARY'] = tmp + '/qc.json'
import numpy as np
import qc_ledger as q

# ── a tiny country: KG 00001 (parent, 4 tiles, partial), 00002 (split: 2 blocks, whole shift + a sweep
#    unit), 00003 (zero), 00004 (mixed, no tiles yet)
def bb(i, j):      # 1.5 km-ish tiles, WGS84
    return [13.0 + 0.02 * i, 47.0 + 0.0135 * j, 13.02 + 0.02 * i, 47.0135 + 0.0135 * j]

def doc(n, off=0, up=True, fallback=()):
    tiles = [{'tile_index': k + 1, 'bbox_wgs': bb(off + k % 2, k // 2)} for k in range(n)]
    d = {'data_quality': {'tiles': tiles}, 'generated_at': '2026-10-20T00:00:00'}
    if up:
        d['upgraded_from'] = {'v1_generated_at': '2026-05-01T10:00:00'}
        d['data_quality']['v2_source'] = {'by_layer': {'DTM': {'gpkg': n - len(fallback), 'fallback': len(fallback)}},
                                          'fallback_tiles': ['%d,%d' % q.envelope(tiles[k - 1]['bbox_wgs'])[:2] for k in fallback]}
    return d

DOCS = {'00001': doc(4, fallback=(4,)), '00002-north': doc(2, 10), '00002-south': doc(2, 20), '00002-sw1': doc(1, 30, up=False),
        '00003': doc(2, 40, up=False)}
env = {c: {t[0]: t[1:] for t in q.doc_tiles(d)} for c, d in DOCS.items()}

env_1 = tuple(env['00001'][1])

class Fake(q.Sources):
    n_sig = 0
    def sig(self): return 'sig%d' % self.n_sig
    def pos_fix(self):
        e = env['00001']
        return {'kgs': {
            '00001': {'verdict': 'partial', 'windows': [{'v': 'shift'}, {'v': 'zero'}], 'off': [-5, 5],
                      'tiles': [[*e[1], 1], [*e[2], 1], [*e[3], 0], [*e[4], -1]], 'tv': 1},
            '00002': {'verdict': 'shift', 'windows': [{'v': 'shift'}], 'off': [-5, 5]},
            '00003': {'verdict': 'zero', 'windows': [{'v': 'zero'}]},
            '00004': {'verdict': 'mixed', 'windows': []}}}
    def grid25_index(self):
        return {'checked': {'00001': {'tiles': [{'v': 'bad', 'tile': 1, 'rc': [0, 0, 60, 60], 'med': 4.2, 'n': 30},
                                                {'v': 'ok', 'tile': 3, 'rc': [60, 0, 120, 60], 'med': 0.03, 'n': 30},
                                                {'v': 'unchecked', 'tile': 2}], 'edges_done': True},
                            '00002-north': {'tiles': [{'v': 'bad', 'tile': 2, 'rc': [0, 0, 9, 9], 'med': 300.0, 'n': 9}]}},
                'patched': {'00001': {'blocks': [[0, 0, 60, 60]], 'rim_blocks': [[0, 0, 1, 1]]}},
                'edges': {'00001': [{'tile': 1, 'axis': 'col', 'k': 60, 'lo': 0, 'hi': 60, 'run': 20, 'step': 7.5}]}}
    def registration(self): return {'offsets': {'00002-north': [-5, 5]}, 'mixed': ['00001']}
    def label_mix(self): return {'00002': {'state': 'done', 'blocks': ['00002-south'], 'requeued_at': 'x'}, '00003': {'state': 'fp'}}
    def repair(self): return {'00002-north': {'state': 'stuck', 'defects': ['harmonics'], 'attempts': 3}}
    def origin(self): return {'00002-sw1_json_v2': {'peer': 'at9', 'ts': '2026-11-01T00:00:00'}}
    def manifest(self):
        m = {}
        for c in DOCS:
            for s, t, v in (('_json', '2026-11-05T00:00:00', 'v1'), ('_full_gpkg', '2026-05-01T11:00:00', 'v1'), ('_json_v2', '2026-10-20T01:00:00', 'v2.4'),
                            ('_light_gpkg', '2026-05-01T11:00:00', 'v1')):
                if '-sw' in c and s in ('_json', '_full_gpkg', '_light_gpkg'):
                    continue
                m[c + s] = {'uploaded_at': t if '-sw' not in c else '2026-11-01T00:00:00', 'version': v, 'status': 'removed' if s == '_light_gpkg' else ''}
        m['00001_requeue'] = {'x': 1}; m['00001_json_error'] = 'boom'
        return m
    def rebin(self):
        return {'resolved': True, 'scanned': 4, 'shifts': [['00004', '00001', -5.0, 5.0, 40, 12]],
                'kgs': [{'kg': '00001', 'stored': 100, 'served': 95, 'off': 9, 'added': 4, 'no_row': 1, 'other_file': 3, 'dup': 1, 'qa': 0, 'ok': 1,
                         'err': None, 'lost': 0, 'ldup': 0, 'lost_cell': 0},
                        {'kg': '00003', 'stored': 10, 'served': 9, 'off': 0, 'added': 0, 'ok': 0, 'err': None}]}
    def border_m(self, env): return 0 if env == env_1 else 9000
    def linear(self): return [{'kg': '00003', 'n_apex': 500, 'n_flag': 40, 'n_runs': 3, 'ts': 0}]
    def archive_sidecar(self): return {'00001': [['2026-05-01T09:59:00', 'at7', 1, 4, 0, 0, 0, 4, 12], ['2026-10-20T00:00:00', 'at8', 4, 4, 0, 0, 4, 4, 100]]}
    def codes_for_parent(self, kg): return [c for c in DOCS if c.split('-')[0] == kg]
    def doc(self, code): return DOCS.get(code, {})
    def log(self, code):
        if code != '00001':
            return []
        L = lambda ts, peer, msg, level='info': {'ts': ts, 'peer': peer, 'msg': msg, 'level': level}
        return [L('2026-04-30T08:00:00', 'at5', 'Starting KG 00001'), L('2026-04-30T09:00:00', 'at5', 'tile 1/4 ▶ lidar'),
                L('2026-04-30T09:30:00', 'at5', 'KG 00001: aborting after tile 2 — tile checkpoints preserved'),
                L('2026-04-30T09:31:00', 'at5', 'chkpt: uploaded 00001 tile-tar to Zenodo (2 tiles)'),
                L('2026-05-01T09:00:00', 'at7', 'Starting KG 00001'), L('2026-05-01T09:20:00', 'at7', 'tile 3/4 ▶ segment'),
                L('2026-05-01T09:58:00', 'at7', 'gpkg_full — 4 tiles, 900 objects'),
                L('2026-10-20T00:00:00', 'at8', 'v2up: KG 00001 upgraded to v2', 'success')]

src = Fake()
s = q.build(src)
C = s['classes']
# 1) one row per (code, tile|whole, class); KG-keyed tiles land on the owning product
assert C['pos_shift']['verdicts'] == {'partial': 1, 'moved': 2, 'kept': 1, 'unmeasured': 1, 'shift': 1, 'zero': 1, 'mixed': 1}, C['pos_shift']
assert C['pos_shift']['tiles'] == 3 and C['pos_shift']['open'] == 3      # unmeasured tile, its partial KG, the mixed KG
assert C['pos_sweep_unit']['products'] == 1 and C['pos_sweep_unit']['open'] == 1, C['pos_sweep_unit']
assert C['grid25_stale']['tiles'] == 2 and C['grid25_stale']['handled'] == 1 and C['grid25_stale']['open'] == 1 and C['grid25_stale']['clear'] == 1
assert C['grid25_edge']['handled'] == 1 and C['grid25_shift']['products'] == 1 and C['grid25_mixed']['open'] == 1
assert C['label_mix']['products'] == 1 and C['label_mix']['clear'] == 1          # done → block target; fp is clear
assert C['layer_gap']['open'] == 1 and C['linear_noise']['handled'] == 1
assert C['apex_frame_pair']['open'] == 1 and C['apex_unserved']['products'] == 1 and C['apex_invariant']['open'] == 1
print('ok rows')

# 2) THE correlation, by tile envelope: tile 1 moved+stale, tile 3 kept+ok; whole-KG shift 00002 → its block tile stale
xt = s['overlap_tiles']['pos_x_grid25']
assert xt == {'moved': {'bad': 2, 'ok': 0}, 'kept': {'bad': 0, 'ok': 1}}, xt
assert s['overlap_tiles']['pos_x_v2_fallback'] == {'moved': {'bev_reread': 0, 'from_gpkg': 2, 'unknown': 0},
                                                    'kept': {'bev_reread': 0, 'from_gpkg': 1, 'unknown': 0}}, s['overlap_tiles']
assert s['overlap_tiles']['edge_on_stale'] == [1, 1]
assert s['overlap_tiles']['grid25_x_border'] == {'crosses': {'bad': 1, 'ok': 0, 'bad_ge8m': 0}, '>=5km': {'bad': 1, 'ok': 1, 'bad_ge8m': 1}}, s['overlap_tiles']
assert q.border_band(None) == 'unknown' and q.border_band(1499) == '<1.5km'
tj = q.tile_join()
assert len(tj) == 1 and tj[0]['n'] == 3 and '00001#1:pos_shift=moved' in tj[0]['faults'] and 'grid25_stale=bad' in tj[0]['faults'], tj
assert s['overlap_parents']['grid25_stale&pos_shift:partial'] == [1, 2, 1], s['overlap_parents']
print('ok overlap')

# 3) WHEN: manifest (never written), origin, document, log, archive
k = q.for_key('00001_full_gpkg')
t = k['product']['timing']
assert k['state'] == 'open' and k['suffix'] == 'full_gpkg' and k['product']['uploaded_at'] == '2026-10-20T01:00:00' and k['product']['version'] == 'v2.4'
assert k['product']['v1_uploaded_at'] == '2026-05-01T11:00:00' and '_requeue' not in json.dumps(k['product']['keys'])
assert t['origin'] == 'upgraded' and t['era'] == 'pre-sidecar' and t['v1_peer'] == 'at7' and t['v1_sessions'] == 2 and t['v1_peers'] == 2, t
assert t['v1_resumes'] == 1 and t['chkpt_tar'] == 1 and t['chkpt_tar_foreign'] == 1 and t['final_tiles'] == [3] and t['v2_peer'] == 'at8', t
assert t['sidecar']['dtm'] == [1, 4] and t['sidecar']['peer'] == 'at7' and q.sidecar_class(t) == 'part' and t['v2_dtm'] == [3, 1], t
assert k['product']['peer'] == 'at8'
r4 = [r for r in k['rows'] if r['tile'] == 4][0]
assert r4['verdict'] == 'unmeasured' and r4['envelope_3035'] == list(env['00001'][4])
b = q.for_key('00002-north_json_v2')           # block: own rows + the whole-KG rows of its parent
assert {(r['fault'], r['scope']) for r in b['rows']} >= {('grid25_stale', 'product'), ('layer_gap', 'product'), ('pos_shift', 'parent')}, b['rows']
sw = q.for_key('00002-sw1')
assert sw['product']['kind'] == 'sweep' and sw['product']['peer'] == 'at9' and sw['rows'][0]['fault'] == 'pos_sweep_unit'
assert q.for_key('99999_json')['state'] == 'clean' and q.split_key('00001_requeue') == (None, None)
assert q.split_key('00002-north_light_gpkg_v2') == ('00002-north', 'light_gpkg_v2')
print('ok when / manifest link')

# 4) text: one line, worst list with manifest keys (removed keys not listed)
line = q.text_line(s)
assert line.startswith('qc: ') and '\n' not in line and 'pos moved 2/2 kept 0/1' in line and 'g25_stale 2p/2t open 1' in line, line
dl = q.detail_lines(3)
assert dl[1].split()[0] == '00001' and 'keys full_gpkg json json_v2' in dl[1] and 'light_gpkg' not in dl[1].split('keys')[1], dl[1]
assert 'sidecar dtm 1/4 (3 re-read)' in dl[1] and 'pre-sidecar' in dl[1]
kl = q.key_lines('00001_json_v2')
assert kl[0].startswith('qc key 00001_json_v2: product 00001 (parent) state open') and any('tile 4' in l and 'unmeasured' in l for l in kl), kl[:3]
assert 'no ledger rows' in q.key_lines('99999')[-1]
print('ok text')

# 5) idempotent + skipped when no input changed + atomically replaced
b0 = open(os.environ['QC_LEDGER_DB'], 'rb').read()
assert q.build(src).get('unchanged') is True
s2 = q.build(src, force=True)
dump = lambda: list(__import__('sqlite3').connect(os.environ['QC_LEDGER_DB']).execute('SELECT * FROM row ORDER BY 1,2,3'))
d1 = dump(); q.build(src, force=True)
assert dump() == d1 and {k: v for k, v in s2.items() if k not in ('built_at', 'secs')} == {k: v for k, v in s.items() if k not in ('built_at', 'secs')}
Fake.n_sig = 1
assert not q.build(src).get('unchanged')
assert not [f for f in os.listdir(tmp) if '.tmp' in f]
print('ok idempotent')

# 6) guards for the next run (docs/qc-lessons.md)
import austria_processor as ap, v2_verify as vv, v21_products  # noqa: E402,F401
import grid25_patch as gp
# 6a) pre-upload warning for a tile whose terrain does not continue its neighbours (stale / foreign tile raster)
yy, xx = np.mgrid[0:120, 0:120]
elev = (500 + 0.4 * xx + 0.2 * yy).astype(np.float32)
x0, y0 = q._to3035().transform(13.0, 47.027)
tiles = [{'tile_index': 1, 'bbox_wgs': [13.0, 47.0, 13.02, 47.027]}, {'tile_index': 2, 'bbox_wgs': [13.02, 47.0, 13.04, 47.027]}]
v2 = {'kg_code': '00009', 'data_quality': {'tiles': tiles}}
rep = vv.Report() if callable(getattr(vv, 'Report', None)) else None
def run(e):
    r = vv.Report(); vv.check_grid25_tile_edges(v2, {'elev': e, 'x0': x0, 'y0': y0}, r); return r
r = run(elev)
assert r['ok'] and not r['warnings'], r['warnings']
g = {'elev': elev, 'x0': x0, 'y0': y0}
(_, r0, c0, r1, c1) = gp.tiles_of_doc(v2, g)[1]
bad = elev.copy(); bad[r0:r1, c0:c1] += 40.0                # tile 2 from somewhere else
r = run(bad)
assert r['ok'] and len(r['warnings']) == 1 and 'grid25_tile_edges' in r['warnings'][0] and 'tile' in r['warnings'][0], r
print('ok guard: grid25_tile_edges (warning, never fatal)')
# 6b) the stitch names the tiles it re-read; the ledger parses the extended line
ap._STITCH_REREAD_TILES[:] = []
import inspect
src_txt = inspect.getsource(ap._read_dtm_for_tile)
assert '_STITCH_REREAD_TILES' in src_txt and 'pad=0.0' in src_txt and '_align_tdata_to_tile' in src_txt
m = q._SIDECAR_RX.search('KG 1: sidecar gpkg_full: dtm=2/4 dtm_multi=0/0 ortho=1/4 hit_pct=38% (BEV reads saved: 3/8) reread_dtm_tiles=2,4')
assert m and m.group(1) == '2'
print('ok guard: reread tile list')
print('ALL OK')
