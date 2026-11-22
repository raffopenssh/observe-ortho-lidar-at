"""Partly shifted KGs (ne_pos_fix verdict ``partial``): the offset applies per processing tile.
Temp state, synthetic tiles — no corpus, no network."""
import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
tmp = Path(tempfile.mkdtemp())
os.environ['NE_POS_FIX'] = str(tmp / 'pos.json')

import numpy as np  # noqa: E402
import ne_pos_fix as n  # noqa: E402

# two 1600 m tiles overlapping 200 m (x 1400..1600); west one shifted, east one not, a third unmeasured
T = [[0, 0, 1600, 1600, 1], [1400, 0, 3000, 1600, 0], [0, 1400, 1600, 3000, -1]]


def main() -> int:
    (tmp / 'pos.json').write_text(json.dumps({'kgs': {
        '00002': {'verdict': 'partial', 'off': [-5, 5], 'tiles': T, 'ts': 50, 'tv': 2, 'windows': []},
        '00003': {'verdict': 'shift', 'off': [-5, 5], 'ts': 60, 'windows': []},
        '00004': {'verdict': 'mixed', 'windows': []}}}))
    assert n.offset('2') is None and n.any_offset('2') == (-5, 5) and n.partial('00002')[0] == (-5, 5)
    assert n.offset('3') == (-5, 5) and n.any_offset('3') == (-5, 5) and n.any_offset('4') is None
    # ownership = highest feather weight, first tile on ties, -1 outside
    t4 = np.asarray(T, float)[:, :4]
    own = n.tile_owner(t4, [800, 1450, 1500, 1550, 2500, 800, 5000], [800, 800, 800, 800, 800, 2500, 800])
    assert own.tolist() == [0, 0, 0, 1, 1, 2, -1], own
    mv = n.moved('00002', [800, 1450, 1550, 2500, 800, 5000], [800, 800, 800, 800, 2500, 800])
    assert mv.tolist() == [True, True, False, False, False, False], mv          # unmeasured / outside: not moved
    assert n.moved('00003', [1, 2], [3, 4]).all() and not n.moved('00004', [1], [1]).any()
    assert n.moves_point(['00002'], 800, 800) and not n.moves_point(['00002'], 2500, 800)
    assert n.moves_point(['00002', '00003'], 2500, 800)                           # a whole-KG frame always moves
    # tags: untouched '', whole KG as before, partial carries the tile digest
    assert n.tag('00004') == '' and n.tag('00003') == '|p-5+5' and n.tag('00002').startswith('|p-5+5~')
    f = n.frames('00002')
    assert f['frame'] == 'p-5+5' and f['partial'] and tuple(f['delta']) == (-5, 5) and 'partial' not in n.frames('00003')
    # words: 3 cells (centres), 2 + 1 + 1 trees, 1 structure in the last
    cen = np.array([[800., 800.], [2500., 800.], [1548., 800.]])
    tw = np.array([3 | (4 << 5) | (77 << 10), (32 - 2) | (1 << 5), 0, 3 | (0 << 5)], '<u4')   # last: x = 1551 → east tile
    bw = np.array([(32 - 4) | (2 << 5) | (9 << 10)], '<u8')                                    # x = 1544 → east tile too
    cells = lambda: (cen, [2, 1, 1], [0, 0, 1])     # noqa: E731
    t2, b2 = n.fix_words('00002', tw.tobytes(), bw.tobytes(), cells)
    t2 = np.frombuffer(t2, '<u4'); b2 = np.frombuffer(b2, '<u8')
    dec = lambda w: (int(n._s5(np.array([w & 31]))[0]), int(n._s5(np.array([(w >> 5) & 31]))[0]))   # noqa: E731
    assert [dec(int(w)) for w in t2] == [(-2, 9), (-7, 6), (0, 0), (3, 0)], [dec(int(w)) for w in t2]
    assert int(t2[0]) >> 10 == 77 and dec(int(b2[0])) == (-4, 2) and int(b2[0]) >> 10 == 9
    # without cell positions a partial KG is left alone; a whole KG moves every word, with or without them
    assert n.fix_words('00002', tw.tobytes(), bw.tobytes()) == (tw.tobytes(), bw.tobytes())
    t3 = np.frombuffer(n.fix_words('00003', tw.tobytes(), b'')[0], '<u4')
    assert [dec(int(w)) for w in t3] == [(-2, 9), (-7, 6), (-5, 5), (-2, 5)]
    # retire keeps the tile list (stale copies still resolve per tile); the flag payload names the tiles
    exp = {}
    import public_flags
    public_flags.auto_kg = lambda kg, src, lay, claim, expected, observed=None: exp.update(json.loads(expected)) or {'written': 1}
    n._flag('00002', json.loads((tmp / 'pos.json').read_text())['kgs']['00002'])
    assert exp['partial'] and exp['applied'] and exp['tiles'] == {'moved': 1, 'kept': 1, 'unmeasured': 1} \
        and exp['moved_tiles'] == [[0, 0, 1600, 1600]] and exp['frame'] == 'p-5+5', exp
    assert n.retire('00002')
    v = json.loads((tmp / 'pos.json').read_text())['kgs']['00002']
    assert v['verdict'] == 'retired' and v['was_tiles'] == T and n.any_offset('00002') is None and n.frames('00002')['partial']
    # per-tile decision rule on recorded windows only
    assert n._clean({'v': 'shift', 'sub': [3, 0, 1]}) == 'shift' and n._clean({'v': 'shift', 'sub': [3, 1, 0]}) is None \
        and n._clean({'v': 'zero', 'sub': [0, 4, 0]}) == 'zero' and n._clean({'v': 'unclear', 'sub': [1, 0, 3]}) is None
    print('ok')
    return 0


if __name__ == '__main__':
    sys.exit(main())
