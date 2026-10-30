#!/usr/bin/env python3
"""Benchmark the public cells API against docs/ne-public-api.md §4.

    python3 tools/bench_cells_api.py [--base http://localhost:<PUBLIC_PORT>] [--kg 63330] [--n 15]

Picks a KG that is on disk (data/ne/<kg>.h3b), derives bboxes from its cells,
and prints p50 / p95 latency + body bytes (gzip on the wire) per row of the
§4 table.  Also reports srv_public RSS when run on the primary.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import statistics
import struct
import sys
import time
import urllib.request
import zlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

TARGETS = {
    'point k=0 obs json': 4,
    'bbox 0.01° json': 12,
    'bbox ~16k cells columns': 80,
    'bbox ~16k cells obs+trees+structures json': 250,
    'bbox ~16k cells bin all layers': 80,
    'bbox res=11 json': 80,
    'cell (all layers)': 4,
    'chunk res 9 h3b': 10,
    'whole-KG h3b': 5,
    'landscape include=cells': 120,
}


def fetch(url, gz=True):
    req = urllib.request.Request(url, headers={'Accept-Encoding': 'gzip'} if gz else {})
    t = time.perf_counter()
    with urllib.request.urlopen(req, timeout=60) as r:
        body = r.read()
        enc = r.headers.get('Content-Encoding')
        status = r.status
    dt = (time.perf_counter() - t) * 1000
    raw = gzip.decompress(body) if enc == 'gzip' else body
    return status, dt, len(body), len(raw), raw


def bench(name, url, n):
    lat = []; wire = raw = 0; status = 0
    for i in range(n):
        try:
            status, dt, wire, raw, _ = fetch(url)
        except urllib.error.HTTPError as e:
            status = e.code; dt = 0
        lat.append(dt)
    lat.sort()
    p50 = statistics.median(lat); p95 = lat[min(len(lat) - 1, int(len(lat) * 0.95))]
    tgt = TARGETS.get(name)
    ok = '' if tgt is None else ('OK ' if p50 < tgt else 'MISS')
    print(f'{name:46s} {status} p50 {p50:7.1f} ms  p95 {p95:7.1f} ms  wire {wire/1024:8.1f} KB  raw {raw/1024:8.1f} KB'
          f'  target {tgt or "-":>4} {ok}')
    return p50


def main():
    ap = argparse.ArgumentParser()
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    import config
    ap.add_argument('--base', default=os.environ.get('CELLS_BASE', f'http://127.0.0.1:{config.PUBLIC_PORT}'))
    ap.add_argument('--kg', default=None)
    ap.add_argument('--n', type=int, default=15)
    a = ap.parse_args()
    base = a.base.rstrip('/')
    st = json.loads(fetch(base + '/api/v1/status')[4])
    print('status.cells:', st.get('cells'))
    import cells_api as ca
    import h3.api.numpy_int as hn
    kg = a.kg or sorted(p.stem for p in ca.NE_DIR.glob('*.h3b'))[-1]
    hdr = ca.header(kg, with_chunks=True)
    # centre of the KG ≈ the median chunk's first cell
    chunks = sorted(hdr['chunks'], key=lambda c: -c['cells'])
    cid = int(chunks[0]['first'], 16)
    lat, lon = hn.cell_to_latlng(cid)
    print(f'KG {kg}: {hdr["cells_n"]} cells, {len(hdr["chunks"])} chunks; anchor {lon:.4f},{lat:.4f}')
    # ~16k cells: res-12 cell ≈ 307 m²; 16 000 cells ≈ 4.9 km² → square side ≈ 2.2 km ≈ 0.0199° lat, /cos(lat) lon
    import math
    side_lat = 0.0192; side_lon = side_lat / math.cos(math.radians(lat))
    b16 = f'{lon - side_lon/2:.5f},{lat - side_lat/2:.5f},{lon + side_lon/2:.5f},{lat + side_lat/2:.5f}'
    b01 = f'{lon - 0.005:.5f},{lat - 0.005:.5f},{lon + 0.005:.5f},{lat + 0.005:.5f}'
    n = a.n
    # warm
    for u in (f'/api/v1/cells?bbox={b16}&format=columns', f'/api/v1/kg/{kg}/cells'):
        try:
            fetch(base + u)
        except Exception as e:  # noqa: BLE001
            print('warm', u, e)
    bench('point k=0 obs json', f'{base}/api/v1/cells?lon={lon:.5f}&lat={lat:.5f}', n)
    bench('bbox 0.01° json', f'{base}/api/v1/cells?bbox={b01}', n)
    r = json.loads(fetch(f'{base}/api/v1/cells?bbox={b16}&format=columns')[4])
    print(f'  (~16k box has {r["meta"]["n"]} cells, kgs {r["meta"]["kgs"]})')
    bench('bbox ~16k cells columns', f'{base}/api/v1/cells?bbox={b16}&format=columns', n)
    bench('bbox ~16k cells obs+trees+structures json', f'{base}/api/v1/cells?bbox={b16}&layers=obs,trees,structures', max(3, n // 3))
    bench('bbox ~16k cells bin all layers', f'{base}/api/v1/cells?bbox={b16}&format=bin&layers=obs,trees,structures', n)
    bench('bbox res=11 json', f'{base}/api/v1/cells?bbox={b16}&res=11', n)
    bench('cell (all layers)', f'{base}/api/v1/cells/{cid:x}', n)
    r9 = format(ca.ne_h3.h3_parent(cid, 9), 'x')
    bench('chunk res 9 h3b', f'{base}/api/v1/cells/chunk/{r9}?format=h3b', n)
    bench('whole-KG h3b', f'{base}/api/v1/kg/{kg}/cells', n)
    bench('landscape include=cells', f'{base}/api/v1/landscape?bbox={b16}&include=cells', max(3, n // 3))
    # verify the whole-KG container has no first-slice bytes + inflates
    blob = fetch(f'{base}/api/v1/kg/{kg}/cells')[4]
    hl = struct.unpack_from('<I', blob, 5)[0]
    h = json.loads(blob[9:9 + hl])
    ch = h['chunks'][0]
    raw = zlib.decompress(blob[h['body_offset'] + ch['off']: h['body_offset'] + ch['off'] + ch['len']])
    assert h['record_bytes']['nec1'] == 0 and len(raw) == ch['cells'] * 41 + ch['trees'] * 4 + ch['bld'] * 8
    print(f'whole-KG h3b: {len(blob)/1024:.0f} KB, {len(h["chunks"])} chunks, nec1=0 OK')
    try:
        import subprocess
        out = subprocess.run(['systemctl', 'show', 'srv_public', '-p', 'MemoryCurrent'], capture_output=True, text=True).stdout.strip()
        print('srv_public', out)
    except Exception:  # noqa: BLE001
        pass


if __name__ == '__main__':
    main()
