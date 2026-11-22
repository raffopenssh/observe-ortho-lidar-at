"""Product generations (qc_generation.py) + the ne_pos_fix pieces they drive: sweep-unit keep cells,
`regenerated` (per-tile retire with frame history), fix retirement on ingest, the operator's
reprocess list.  Temp stores only — no corpus, no network."""
import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
tmp = Path(tempfile.mkdtemp())
os.environ['NE_POS_FIX'] = str(tmp / 'pos.json')
os.environ['GRID25_PATCH_DIR'] = str(tmp / 'g25')
os.environ['GRID25_REGISTRATION'] = str(tmp / 'reg.json')
os.environ['QC_GENERATIONS'] = str(tmp / 'gen.sqlite')
os.environ['QC_REPROCESS'] = str(tmp / 'rp.json')

import numpy as np  # noqa: E402
import grid25_patch as gp  # noqa: E402
import grid25_registration as gr  # noqa: E402
import ne_pos_fix as n  # noqa: E402
import qc_generation as qg  # noqa: E402

TA = [0, 0, 1600, 1600]; TB = [1400, 0, 3000, 1600]; TC = [0, 1400, 1600, 3000]


def _write(kgs):
    (tmp / 'pos.json').write_text(json.dumps({'kgs': kgs}))
    os.utime(tmp / 'pos.json', ns=(_write.t, _write.t)); _write.t += 10 ** 9      # mtime-cached state


_write.t = 10 ** 18


def test_keep():
    import cells_api as ca
    import h3
    lon, lat = ca._tf('EPSG:3035', 'EPSG:4326').transform(4_700_000.0, 2_700_000.0)
    cell = h3.latlng_to_cell(lat, lon, 12)
    ids = np.asarray(sorted({h3.str_to_int(c) for c in h3.grid_disk(cell, 1)}), np.uint64)
    (tmp / 'ne_pos_keep').mkdir(exist_ok=True)
    np.save(tmp / 'ne_pos_keep' / '00007.npy', ids)
    _write({'00007': {'verdict': 'shift', 'off': [-5, 5], 'ts': 50, 'windows': [], 'keep_n': int(ids.size), 'keep_crc': 'abcd0123',
                      'keep_units': {'00007-sw1': int(ids.size)}},
            '00008': {'verdict': 'shift', 'off': [-5, 5], 'ts': 50, 'windows': []}})
    assert n.offset('7') == (-5, 5)                                   # still a whole-KG frame for every "whole" caller
    mv = n.moved('00007', [4_700_000.0, 4_700_300.0], [2_700_000.0, 2_700_000.0])
    assert mv.tolist() == [False, True], mv                           # inside a kept cell: not moved
    assert n.tag('00007') == '|p-5+5~kabcd0123' and n.tag('00008') == '|p-5+5'
    assert not n.moves_point(['00007'], 4_700_000.0, 2_700_000.0) and n.moves_point(['00007'], 4_700_300.0, 2_700_000.0)
    assert n.frames('00007')['partial'] and 'partial' not in n.frames('00008')
    # fix_words: one row in the kept cell (2 trees), one far away (1 tree); words carry dx=dy=0 (centre)
    cen = np.array([[4_700_000.0, 2_700_000.0], [4_700_300.0, 2_700_000.0]])
    w = np.array([1 << 10, 2 << 10, 3 << 10], '<u4')
    out = np.frombuffer(n.fix_words('00007', w.tobytes(), b'', lambda: (cen, [2, 1], [0, 0]))[0], '<u4')
    assert out[0] == w[0] and out[1] == w[1] and out[2] != w[2], out
    assert (np.frombuffer(n.fix_words('00008', w.tobytes(), b'')[0], '<u4') != w).all()
    assert (np.frombuffer(n.fix_words('00007', w.tobytes(), b'')[0], '<u4') != w).all()     # no cell context: whole move as before


def test_regenerated():
    n._code_tiles_orig = n._code_tiles
    # split KG moved as a whole: block -east (tile TB) regenerated → partial, TB kept, history remembers
    _write({'00010': {'verdict': 'shift', 'off': [-5, 5], 'ts': 50, 'windows': []},
            '00011': {'verdict': 'partial', 'off': [-5, 5], 'ts': 50, 'tv': 2, 'windows': [], 'tiles': [TA + [1], TB + [0], TC + [-1]]},
            '00012': {'verdict': 'shift', 'off': [-5, 5], 'ts': 50, 'windows': []},
            '00013': {'verdict': 'mixed', 'windows': []}})
    r = n.regenerated('00010-east', tiles=[TB], all_tiles=[TA, TB])
    assert r == {'action': 'tiles'}, r
    assert n.offset('10') is None and n.any_offset('10') == (-5, 5)
    assert n.moved('00010', [800, 2500], [800, 800]).tolist() == [True, False]
    assert n.moves_point(['00010'], 2500, 800)          # a copy made BEFORE the fix still needs the old delta there
    assert n.frames('00010')['partial']
    assert n.regenerated('00010-east', tiles=[TB], all_tiles=[TA, TB]) == {'action': None}   # idempotent
    r = n.regenerated('00010-west', tiles=[TA], all_tiles=[TA, TB])
    assert r == {'action': 'retired'}, r
    v = json.loads((tmp / 'pos.json').read_text())['kgs']['00010']
    assert v['verdict'] == 'retired' and v['was'] == [-5, 5] and len(v['was_tiles']) == 2 and set(v['regen']) == {'00010-east', '00010-west'}
    assert n.any_offset('10') is None and not n.moved('00010', [800], [800]).any() and n.moves_point(['00010'], 800, 800)
    # partial KG: its moved tile regenerated → retired; frames stay
    assert n.regenerated('00011-a', tiles=[TA], all_tiles=None) == {'action': 'retired'}
    assert n.frames('00011')['delta'] == (-5, 5)
    assert n.moves_point(['00011'], 800, 800) and not n.moves_point(['00011'], 2500, 800)
    # unsplit parent → retire(); never-moved parent → fresh; sweep unit → nothing
    assert n.regenerated('00012') == {'action': 'retired'} and n.any_offset('12') is None and n.frames('00012')
    assert n.regenerated('00013') == {'action': 'fresh'}
    assert json.loads((tmp / 'pos.json').read_text())['kgs']['00013']['verdict'] == 'fresh'
    assert n.regenerated('00012-sw1') == {'action': None}


def test_retire_stores():
    (tmp / 'g25').mkdir(exist_ok=True)
    np.savez_compressed(tmp / 'g25' / '00020.npz', e0=np.zeros((2, 2), np.int16))
    (tmp / 'g25' / 'index.json').write_text(json.dumps({'version': 7, 'checked': {
        '00020': {'tiles': [{'v': 'bad', 'tile': 1}, {'v': 'ok', 'tile': 2}]}, '00021': {'tiles': [{'v': 'ok', 'tile': 1}]}},
        'patched': {'00020': {'grid': [0, 0, 2, 2], 'blocks': [[0, 0, 2, 2]], 'ts': 1}}, 'edges': {'00020': [{'tile': 1}]}}))
    (tmp / 'reg.json').write_text(json.dumps({'version': 3, 'offsets': {'00020': [-5, 5], '00022': [-5, 5]}, 'mixed': ['00020', '00023']}))
    r = gp.retire('00020', '2026-12-01T00:00:00')
    assert r['bad'] == 1 and r['patched'] and r['edges'] == 1, r
    d = json.loads((tmp / 'g25' / 'index.json').read_text())
    assert d['version'] == 7 and '00020' not in d['checked'] and '00020' not in d['patched'] and '00020' not in d['edges']
    assert d['retired']['00020']['bad'] == 1 and '00021' in d['checked'] and not (tmp / 'g25' / '00020.npz').exists()
    assert gp.retire('00020') is None and gp.apply('00020', {'elev': np.zeros((2, 2)), 'x0': 0, 'y0': 0}) is False
    assert gr.retire('00020') == [-5, 5] and gr.retire('00020') is None and gr.retire('00023') == [0, 0]
    d = json.loads((tmp / 'reg.json').read_text())
    assert d['version'] == 3 and d['offsets'] == {'00022': [-5, 5]} and d['mixed'] == [] and set(d['retired']) == {'00020', '00023'}


def test_generations():
    n._code_tiles = lambda code: [TB]
    n.kg_tiles = lambda kg: np.asarray([TA, TB], float)
    _write({'00030': {'verdict': 'shift', 'off': [-5, 5], 'ts': 50, 'windows': []}})
    (tmp / 'g25' / 'index.json').write_text(json.dumps({'version': 7, 'checked': {'00030-east': {'tiles': [{'v': 'bad', 'tile': 1}]}},
                                                        'patched': {'00030-east': {'grid': [0, 0, 1, 1], 'blocks': [[0, 0, 1, 1]], 'ts': 1}}}))
    (tmp / 'reg.json').write_text(json.dumps({'version': 3, 'offsets': {'00030-east': [-5, 5]}, 'mixed': []}))
    gp._ST['mtime'] = None                 # the test rewrote the index within one mtime tick
    old = {'generated_at': '2026-07-01T00:00:00+00:00', 'upgraded_from': {'v1_generated_at': '2026-06-01T00:00:00'},
           'data_quality': {'tiles': [{'tile_index': 1}]}}
    new = {'generated_at': '2026-12-01T00:00:00+00:00', 'data_quality': {'tiles': [{'tile_index': 1}, {'tile_index': 2, 'restored': True}]}}
    r0 = qg.on_ingest('00030-east', old)
    assert r0 and not r0['retired'] and not r0['fresh'], r0             # first sight, no bootstrap stamp: documented, nothing retired
    assert qg.on_ingest('00030-east', old) is None                     # same generation: no-op
    assert json.loads((tmp / 'reg.json').read_text())['offsets'] == {'00030-east': [-5, 5]}
    # operator list: nothing enrolled by itself
    assert qg.sweep() == {'requeued': 0, 'outstanding': 0}
    import kg_v2_store as kvs
    kvs.has = lambda c: c != '00099'; kvs.get_meta = lambda c: {'generated_at': old['generated_at']}
    e = qg.enroll(['00030-east', '00030-sw1', '00099'], why='test')
    assert e['enrolled'] == ['00030-east'] and set(e['skipped']) == {'00030-sw1', '00099'}, e
    posted = []
    qg._post_requeue = lambda code: posted.append(code) or True
    qg._busy_elsewhere = lambda code, ent: None
    import label_mix as lm
    lm._manifest_entries = lambda: {}
    s = qg.sweep()
    assert posted == ['00030-east'] and s['requeued'] == 1, (posted, s)
    assert qg.sweep()['requeued'] == 0 and posted == ['00030-east']    # in flight: not posted twice
    # an UPGRADED new generation closes nothing and retires nothing
    up = dict(old, generated_at='2026-11-25T00:00:00+00:00')
    r1 = qg.on_ingest('00030-east', up)
    assert r1 and not r1['retired'] and qg._load()['codes']['00030-east']['state'] == 'requeued'
    # the FRESH generation: fixes retired, quality documented, list closed
    r2 = qg.on_ingest('00030-east', new, version_label='v2.4')
    assert r2['fresh'] and set(r2['retired']) == {'grid25_registration', 'grid25_patch', 'ne_pos_fix'}, r2
    assert r2['quality']['tiles'] == 2 and r2['quality']['restored'] == 1 and r2['prev_generated_at'] == up['generated_at']
    assert json.loads((tmp / 'reg.json').read_text())['offsets'] == {}
    assert n.moved('00030', [800, 2500], [800, 800]).tolist() == [True, False]
    st = qg._load()['codes']['00030-east']
    assert st['state'] == 'done' and st['retired'] == ['grid25_patch', 'grid25_registration', 'ne_pos_fix'], st
    g = qg.generations(acted_only=True)
    assert [x['generated_at'] for x in g] == [new['generated_at'], up['generated_at']], g
    line = qg.status_line()
    assert line.startswith('qc_reprocess: list done 1') and 'registration 1 · patch 1 · position 1' in line, line
    # bootstrap stamp: a product first seen later, generated after it, counts as new
    c = qg._conn(); c.execute("INSERT INTO meta VALUES ('bootstrap_at', '2026-11-22T00:00:00+00:00')"); c.close()
    (tmp / 'reg.json').write_text(json.dumps({'version': 3, 'offsets': {'00031': [-5, 5]}, 'mixed': []}))
    assert qg.on_ingest('00031', dict(new))['retired'].get('grid25_registration') == [-5, 5]
    assert qg.on_ingest('00032', dict(old))['retired'] == {}


if __name__ == '__main__':
    test_keep(); test_regenerated(); test_retire_stores(); test_generations()
    print('ALL OK')
