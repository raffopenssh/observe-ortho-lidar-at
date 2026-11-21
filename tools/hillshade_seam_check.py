#!/usr/bin/env python3
"""Seam regression check for /tiles/hillshade (docs/hillshade-seams.md).

    python3 tools/hillshade_seam_check.py --tile 16/34514/22989     # one tile (repeatable)
    python3 tools/hillshade_seam_check.py --country [--z 15]         # all of Austria, hours, niced
    python3 tools/hillshade_seam_check.py --country --sample 2000    # random subset
    python3 tools/hillshade_seam_check.py --status                   # last country report

Exit status 1 when any seam is found (expected: 0).  The country run
renders span×span tile blocks straight from the renderer (`hillshade_tiles.
shade`, no HTTP, no cache, no render gate — the public tile service is not
touched), resumes from its state file and writes
``data/hillshade_seam_report.json``.
"""
import argparse, json, math, os, random, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

REPORT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'hillshade_seam_report.json')
AT = (9.45, 46.35, 17.2, 49.05)


def check_block(z, x, y, span=1, merge_fn=None):
    """→ (seams, has_data).  Seam rows/cols are block-relative px."""
    import hillshade_tiles as hs, hillshade_seams as sm, grid25_sample as g25
    fps = []
    r = hs.shade(z, x, y, span=span, footprints=fps, merge_fn=merge_fn)
    if r is None:
        return [], False
    grey, alpha = r
    lat = hs.tile_bounds(z, x, y)[3]
    mpp = 156543.03392 * math.cos(math.radians(lat)) / 2 ** z
    band = int(math.ceil((g25.FEATHER_CELLS + 2) * 25.0 / mpp))
    seams = sm.find_seams(grey, alpha, [f[1:-1, 1:-1] for f in fps], band_px=band)
    for s in seams:
        s['tile'] = f"{z}/{x + s['col'] // 256}/{y + s['row'] // 256}"
        s['px'] = [s['row'] % 256, s['col'] % 256]
    return seams, True


def xy(lon, lat, z):
    n = 2 ** z
    return (int((lon + 180) / 360 * n),
            int((1 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2 * n))


def country(z, span, sample=None, budget_s=None):
    import hillshade_tiles as hs
    x0, y1 = xy(AT[0], AT[1], z); x1, y0 = xy(AT[2], AT[3], z)
    blocks = [(x, y) for x in range(x0 - x0 % span, x1 + 1, span) for y in range(y0 - y0 % span, y1 + 1, span)]
    ver = hs.tileset_version()
    state = {}
    if not sample:
        try:
            state = json.load(open(REPORT))
            if state.get('tileset_version') != ver or state.get('z') != z or state.get('span') != span:
                state = {}
        except Exception:
            state = {}
    else:
        random.Random(1).shuffle(blocks); blocks = blocks[:sample]
    done = int(state.get('next', 0)); seams = state.get('seams', []); nd = int(state.get('tiles_with_data', 0))
    t0 = time.time(); spent = float(state.get('secs', 0))

    def save(final):
        d = {'tileset_version': ver, 'z': z, 'span': span, 'next': done, 'blocks': len(blocks),
             'complete': final, 'tiles_with_data': nd, 'seam_count': len(seams), 'seams': seams[:500],
             'secs': round(spent + time.time() - t0), 'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
             'rule': 'step across product boundary zone > 3x median of 4 neighbour steps (floor 1.5), run >= 40 px'}
        if not sample:
            tmp = REPORT + '.tmp'; json.dump(d, open(tmp, 'w')); os.replace(tmp, REPORT)
        return d
    for i in range(done, len(blocks)):
        x, y = blocks[i]
        try:
            s, has = check_block(z, x, y, span)
        except Exception as e:  # noqa: BLE001
            print('ERR', z, x, y, e, file=sys.stderr); s, has = [], False
        if has:
            nd += span * span
        seams += s
        done = i + 1
        if i % 50 == 0:
            save(False)
            print(f'{done}/{len(blocks)} blocks, {len(seams)} seams, {time.time() - t0:.0f}s', flush=True)
        if budget_s and time.time() - t0 > budget_s:
            break
    return save(done >= len(blocks))


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--tile', action='append', default=[])
    ap.add_argument('--country', action='store_true')
    ap.add_argument('--status', action='store_true')
    ap.add_argument('--z', type=int, default=15)
    ap.add_argument('--span', type=int, default=4)
    ap.add_argument('--sample', type=int)
    ap.add_argument('--legacy', action='store_true', help='use the pre-v4 hard hand-over mosaic (detector self-test)')
    a = ap.parse_args()
    try:
        os.nice(15)
    except OSError:
        pass
    if a.status:
        try:
            d = json.load(open(REPORT)); d.pop('seams', None); print(json.dumps(d))
        except Exception as e:  # noqa: BLE001
            print(json.dumps({'present': False, 'error': str(e)}))
        sys.exit(0)
    total = 0
    if a.tile:
        import hillshade_seams as sm
        for t in a.tile:
            z, x, y = map(int, t.split('/'))
            s, has = check_block(z, x, y, 1, sm.merge_legacy if a.legacy else None)
            total += len(s)
            print(t, 'data' if has else 'nodata', 'seams', len(s), json.dumps(s[:4]))
    if a.country:
        d = country(a.z, a.span, a.sample)
        total += d['seam_count']
        print(json.dumps({k: v for k, v in d.items() if k != 'seams'})); print(json.dumps(d['seams'][:10]))
    sys.exit(1 if total else 0)
