"""A corpus file newer than its index rows (a write whose ``index_file`` never ran) must be served
whole — digest from the live header, every chunk of the file, also ones the old chunk list lacks —
and ``cells_api.reindex_lagging`` must heal the index.  Runs on a copy of one small corpus file with
a private index (the live index is not touched).

    python3 tests/test_index_lag.py
"""
import os
import shutil
import sys
import tempfile
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def test_lagging_file_is_served_and_healed():
    import cells_api as ca
    import ne_h3
    src = None
    for p in sorted(ne_h3.NE_DIR.glob('[0-9]*.h3b'), key=lambda p: p.stat().st_size):
        if p.stat().st_size > 50_000:
            src = p; break
    if src is None:
        print('  (no corpus — skipped)'); return
    kg = src.stem
    d = Path(tempfile.mkdtemp())
    shutil.copy2(src, d / src.name)
    saved = (ca.NE_DIR, ca.INDEX_DB, getattr(ca._local, 'cells_idx', None))
    ca.NE_DIR = d; ca.INDEX_DB = d / 'cells.sqlite'; ca._local.cells_idx = None
    ca._files_cache.update(t=0.0, v={}, lag=set()); ca._header_cache_drop(kg)
    try:
        ca.index_file(d / src.name)
        hdr = ca.header(kg, with_chunks=True)
        r10s = np.array(sorted(int(c['r10'], 16) for c in hdr['chunks']), np.uint64)
        assert len(r10s) >= 4
        assert ca.lagging_files(0) == [] and not ca._files_cache.get('lag')
        full = sorted((r['r10'], r['off'], r['len'], r['trees']) for r in ca._chunk_rows(r10s))
        assert len(full) == len(r10s)
        # the index as an older file version left it: fewer chunks, wrong offsets, old digest, old mtime
        c = ca._idx_conn(write=True)
        gone = [int(x) for x in r10s[: len(r10s) // 2]]
        c.executemany('DELETE FROM chunk WHERE kg=? AND r10=?', [(kg, x) for x in gone])
        c.execute('UPDATE chunk SET off = off + 7 WHERE kg=?', (kg,))
        c.execute("UPDATE file SET neo1_digest='old', mtime = mtime - 1000 WHERE kg=?", (kg,))
        c.commit()
        ca._files_cache['t'] = 0.0
        f = ca._files()[kg]
        assert f['neo1_digest'] == hdr['neo1_digest'] and ca._files_cache['lag'] == {kg}, f
        # a query that only asks for chunks the old list never had still gets them — from the header
        got = sorted((r['r10'], r['off'], r['len'], r['trees']) for r in ca._chunk_rows(np.array(gone, np.uint64)))
        assert got == [x for x in full if x[0] in set(gone)], (len(got), len(gone))
        assert sorted((r['r10'], r['off'], r['len'], r['trees']) for r in ca._chunk_rows(r10s)) == full
        # the writer's own few seconds are respected; then the sweep heals the index
        assert ca.lagging_files(10 ** 9) == [] and ca.lagging_files(0) == [kg]
        assert ca.reindex_lagging(0) == [kg]
        ca._files_cache['t'] = 0.0
        assert ca.lagging_files(0) == [] and not ca._files()[kg] is None and not ca._files_cache['lag']
        assert c.execute('SELECT COUNT(*) FROM chunk WHERE kg=?', (kg,)).fetchone()[0] == len(r10s)
        assert c.execute('SELECT neo1_digest FROM file WHERE kg=?', (kg,)).fetchone()[0] == hdr['neo1_digest']
    finally:
        try:
            ca._local.cells_idx.close()
        except Exception:  # noqa: BLE001
            pass
        ca.NE_DIR, ca.INDEX_DB, ca._local.cells_idx = saved
        ca._files_cache.update(t=0.0, v={}, lag=set()); ca._header_cache_drop(kg)
        shutil.rmtree(d, ignore_errors=True)


def test_dirty_merge_indexes_the_final_path():
    """ne_ingest's dirty-merge writes a dot-prefixed temp (not indexed) and must index after the swap."""
    s = (ROOT / 'ne_ingest.py').read_text()
    i = s.index('os.replace(tmp_out, path_for(parent))')
    assert '_index_cells(path_for(parent))' in s[i:i + 400]


if __name__ == '__main__':
    for name, fn in list(globals().items()):
        if name.startswith('test_') and callable(fn):
            fn(); print('ok', name)
    print('all ok')
