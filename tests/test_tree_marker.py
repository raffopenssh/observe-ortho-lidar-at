#!/usr/bin/env python3
"""tree_qa.mark_file: header-only rewrite of an existing corpus file.

* body bytes, digests and mtime unchanged; ``net1.tree_rule`` == what ``ne_h3.write_h3b`` writes
  (one-file ``merge_blocks``); verify_h3b ok;
* readers: a header cached before the rewrite is revalidated by size (``cells_api.header``), a
  request still holding the OLD header reads the right chunk bytes (``cells_api.body_base``);
* second call = no-op.

    python3 tests/test_tree_marker.py      (needs data/ne/*.h3b; skipped without)
"""
import hashlib
import os
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import ne_h3                  # noqa: E402
import tree_rule              # noqa: E402


def test_mark_file():
    import cells_api as ca
    import ne_ingest
    import tree_qa
    src = None
    for p in sorted(ne_h3.NE_DIR.glob('*.h3b'), key=lambda p: p.stat().st_size):
        h = ne_h3.read_h3b_header(p)
        if not tree_rule.built_with(h) and h['net1']['rows_n'] > 20000 and p.stat().st_size > 2 << 20:
            src = p; break
    if src is None:
        print('  (no unmarked data/ne/*.h3b — skipped)'); return
    kg = src.stem
    d = Path(tempfile.mkdtemp()); p = d / src.name
    shutil.copy2(src, p)
    os.utime(p, ns=(src.stat().st_atime_ns, src.stat().st_mtime_ns - 10 ** 12))
    real_path, real_conn = ca._file_path, ca._idx_conn
    ca._file_path = lambda k: d / f'{str(k).zfill(5)}.h3b'
    ca._idx_conn = lambda write=False: (_ for _ in ()).throw(RuntimeError('index not touched in the test')) if write else real_conn()
    try:
        ca._header_cache_drop(kg)
        h0 = ca.header(kg, with_chunks=True); st0 = p.stat()
        rows = [dict(c, r10=int(c['r10'], 16), trees=int(c.get('trees') or 0), bld=int(c.get('bld') or 0)) for c in h0['chunks']]
        pick = [r for r in rows if r['trees']][:: max(1, len(rows) // 25)]
        before = [hashlib.sha1(ca._load_chunk(kg, r, h0, raw=True).trees.tobytes()).hexdigest() for r in pick]
        body0 = hashlib.sha1(p.read_bytes()[h0['body_offset']:]).hexdigest()
        r = tree_qa.mark_file(kg)
        assert r['marked'], r
        st1 = p.stat()
        assert st1.st_mtime_ns == st0.st_mtime_ns and st1.st_size > st0.st_size and r['stat'] == [st1.st_mtime_ns, st1.st_size]
        assert ne_h3.verify_h3b(p)['ok']
        # a request that still holds the old header reads the same chunks
        assert [hashlib.sha1(ca._load_chunk(kg, x, h0, raw=True).trees.tobytes()).hexdigest() for x in pick] == before
        h1 = ca.header(kg, with_chunks=True)                    # cached header revalidated by size
        assert h1 is not h0 and h1['body_offset'] > h0['body_offset'] and tree_rule.built_with(h1)
        assert hashlib.sha1(p.read_bytes()[h1['body_offset']:]).hexdigest() == body0
        for k in ('neo1_digest', 'cells_n'):
            assert h1[k] == h0[k]
        assert h1['net1']['digest'] == h0['net1']['digest'] and [c for c in h1['chunks']] == h0['chunks']
        # == the writer's marker for the same content
        out = d / 'merged.h3b'
        ne_ingest.merge_blocks(kg, [src], out, complete=True)
        want = ne_h3.read_h3b_header(out)['net1']['tree_rule']
        assert h1['net1']['tree_rule'] == want, (r, want['hidden'])
        # with the index's hmax shortcut: same marker
        shutil.copy2(src, p); os.utime(p, ns=(st0.st_atime_ns, st0.st_mtime_ns))
        hm = {int(r10): h for r10, h in real_conn().execute('SELECT r10, hmax FROM chunk WHERE kg=?', (kg,))}
        assert tree_qa.mark_file(kg, hm)['marked'] and ne_h3.read_h3b_header(p)['net1']['tree_rule'] == want
        assert tree_qa.mark_file(kg)['marked'] is False         # idempotent
        print(f"  ({kg}: {want['hidden']} rows in {len(want['rows'])} chunks, header +{st1.st_size - st0.st_size} B)")
    finally:
        ca._file_path, ca._idx_conn = real_path, real_conn
        ca._header_cache_drop(kg)
        shutil.rmtree(d, ignore_errors=True)


if __name__ == '__main__':
    for n, f in sorted(globals().items()):
        if n.startswith('test_') and callable(f):
            f(); print('ok', n)
    print('all ok')
