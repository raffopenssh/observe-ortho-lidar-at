"""als_years: real ALS flight year per cell / KG (docs/ne-public-api.md → ALS acquisition year)."""
import numpy as np
import sys, pathlib; sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
import als_years as A


def test_block_basic():
    new = np.array([2022, 2022, 2009, A.NULL], np.uint16)
    old = np.array([2009, 2009, 2009, A.NULL], np.uint16)
    b = A.block_from_years(new, old)
    assert b['newest'] == 2022 and b['oldest'] == 2009 and b['years'] == [2009, 2022]
    assert b['coverage'] == 0.5 and b['dominant'] == 2022 and b['cells_known'] == 3
    assert abs(b['reflown_share'] - 2 / 3) < 1e-3
    assert b['source'].startswith('data.gv.at ')


def test_block_weighted_and_empty():
    b = A.block_from_years(np.array([2017, 2010], np.uint16), np.array([2010, 2010], np.uint16), weights=[1, 9], precision='chunk')
    assert b['newest'] == 2017 and b['coverage'] == 0.1 and b['dominant'] == 2010 and b['precision'] == 'chunk'
    e = A.block_from_years(np.zeros(0, np.uint16), np.zeros(0, np.uint16))
    assert e['newest'] is None and e['cells_n'] == 0


def test_lookup_known_point():
    if not A.available():
        return
    # Graz area (EPSG:3035) — inside Austria, must resolve to a year in both mosaics
    new, old = A.cell_years_3035(np.array([4680000.0]), np.array([2690000.0]))
    assert 2005 < int(new[0]) < 2030 and 2005 < int(old[0]) < 2030
    # far outside → null
    n2, _ = A.cell_years_3035(np.array([1.0]), np.array([1.0]))
    assert int(n2[0]) == A.NULL


def test_header_block_shape():
    h = A.header_block(np.array([2022], np.uint16), np.array([2009], np.uint16))
    assert set(h) >= {'newest', 'oldest', 'source', 'coverage', 'years', 'mosaics'}


if __name__ == '__main__':
    for k, f in list(globals().items()):
        if k.startswith('test_'):
            f(); print(k, 'ok')
