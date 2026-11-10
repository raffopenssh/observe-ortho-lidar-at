"""Regression: ``cells_api.index_file`` must keep working after ``ne_trees``
widens ``chunk`` with ``hmax``/``bmax`` (ca178ac).  A positional INSERT broke
every post-merge index call (2026-11-09: 87104 Finkenberg, 81201, 81206, 77012
stayed ``pending`` in /trees/bbox although their h3b was on disk)."""
import sqlite3, sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import cells_api, ne_trees


def test_index_file_with_hmax_columns():
    src = sorted(cells_api.NE_DIR.glob('*.h3b'), key=lambda p: p.stat().st_size)
    if not src:
        print('skip: no h3b corpus'); return
    c = sqlite3.connect(':memory:')
    c.executescript(cells_api.SCHEMA)
    ne_trees.ensure_schema(c)
    assert ne_trees._have_cols(c)
    r = cells_api.index_file(src[0], conn=c)
    n = c.execute('SELECT count(*) FROM chunk WHERE kg=?', (r['kg'],)).fetchone()[0]
    assert n == r['chunks'] > 0, (n, r)
    assert c.execute('SELECT count(*) FROM file WHERE kg=?', (r['kg'],)).fetchone()[0] == 1


if __name__ == '__main__':
    test_index_file_with_hmax_columns()
    print('ok')
