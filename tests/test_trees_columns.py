"""/api/v1/trees/bbox format=columns: layout, limits, band partition, ETag state.
Needs the live NE corpus (skips when the cells index is empty)."""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
import ne_trees

BB = (10.075, 47.125, 10.1, 47.15)


def main() -> int:
    if not ne_trees.status()['ok']:
        print('skip: no cells index'); return 0
    st = ne_trees.bbox_state(*BB)
    if not st['ready']:
        print('skip: bbox not ready'); return 0
    allr = ne_trees.columns_bbox(*BB, 'trees', min_height=3, limit=ne_trees.MAX_LIMIT_COLUMNS)
    assert allr['fields'] == ['lon', 'lat', 'h_m', 'crown_m2'] and allr['order'] == 'height_desc'
    n = allr['n']; assert n > 0 and not allr['truncated']
    assert all(len(v) == n for v in allr['columns'].values())
    h = allr['columns']['h_m']; assert h == sorted(h, reverse=True)
    bands = [(3, 8), (8, 14), (14, 20), (20, None)]
    tot = sum(ne_trees.columns_bbox(*BB, 'trees', min_height=a, max_height=b, limit=ne_trees.MAX_LIMIT_COLUMNS)['n'] for a, b in bands)
    assert tot == n, (tot, n)
    rows = ne_trees.points_bbox(*BB, 'trees', min_height=3, max_height=8, limit=20000)
    cols = ne_trees.columns_bbox(*BB, 'trees', min_height=3, max_height=8)
    assert len(rows['trees']) == cols['n']
    assert abs(rows['trees'][0]['lon'] - cols['columns']['lon'][0]) < 1e-5
    ex = ne_trees.columns_bbox(*BB, 'trees', min_height=30, limit=5, fields=('x', 'y', 'cell', 'kg_code', 'species', 'ndsm_only'))
    assert ex['truncated'] and ex['n'] == 5 and isinstance(ex['columns']['x'][0], int) and ex['columns']['ndsm_only'][0] in (0, 1)
    try:
        ne_trees.columns_bbox(*BB, 'trees', fields=('nope',)); raise SystemExit('expected ValueError')
    except ValueError:
        pass
    assert st['digests'] and st['mtime'] > 0
    print(f'ok: {n} apices, bands partition, {len(st["digests"])} KG digests')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
