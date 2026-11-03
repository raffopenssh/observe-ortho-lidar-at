"""cells_blocks — prebuilt, id-free, zstd 0.1° blocks (``NEXB`` v2) and the
national res-10 centre-child sample (``/api/v1/cells/sample``).

Both are **derived from the stored 0.025° bins** (``cells_bins``) — no new
per-cell computation.  A downstream national glyph filter pulls the sample
once per epoch and blocks per view; both must be cheap to serve (disk → send) and small on the wire.

Blocks (``GET /api/v1/cells?bbox=<0.1° block>&format=bin&block=1[&fields=…]``)
-------------------------------------------------------------------------
The ids are gone.  The cell set of a bin is the *shared enumeration*
"all res-12 cells whose centre lies in the half-open bin
[w, w+0.025) × [s, s+0.025), ascending H3" (= ``h3.polygon_to_cells``
centre rule, exactly what ``cells_bins.build_bin`` stores), so the client
enumerates the same ids itself and only checks ``n`` + ``ids_xor`` (xor of
the u64 ids) per bin; on a mismatch it re-requests with ``ids=1`` (the
NEXB v1 frames-with-ids form, unchanged).

Container ``NEXB`` v2 (little-endian)::

    b'NEXB' | u32 header_len | header JSON | present bitmap | col_0 | col_1 | …

* ``header.v = 2``, ``header.ids = 'bitmap'``, ``header.n`` = rows of the
  whole block = Σ bins[k].n, rows = the 16 bin enumerations concatenated in
  row-major bin order (``cells_bins.block_bins``: k = (j − 4·bj)·4 + (i − 4·bi)).
* ``header.bins[k] = {i, j, n, row0, ids_xor (hex u64), gen, complete}`` —
  ``row0`` = first row of the bin in the block-planar columns.
* ``present`` = ceil(n/8) bytes, bit r (LSB-first: byte r>>3, bit r&7) = 1
  when row r of the enumeration carries a row.  Today every enumerated cell
  has a row (terrain columns may be nodata), so the bitmap is all ones —
  it is there so the enumeration can shrink later without a format change.
* columns = ``header.columns[]`` in order, each ``n × itemsize`` (planar,
  shape[9] for ``cover``), same dtypes / units / nodata as the bin ``NEX1``.
* ``ETag "B-<block gen>-<hash(fields|bitmap)>"`` (strong), ``X-Cells-Gen``,
  ``X-Cells-Epoch`` (the ne-observe product epoch), ``X-Cells-Block``,
  ``X-Cells-Partial``.  Body ``Content-Encoding: zstd`` when the client
  accepts it (always prebuilt as one zstd frame on disk;
  ``data/ne/nexb/<bi>_<bj>.<fh>.nexb.zst`` + ``.json`` meta), else inflated
  and gzip'd by ``public_api``.

Sample (``GET /api/v1/cells/sample?res=10&format=bin[&fields=…]``)
-----------------------------------------------------------------
One row per res-10 cell of Austria = its res-12 **centre child**
(``h3.cell_to_center_child(parent, 12)``; bit rule: digits 11 and 12 of
the res-12 index are 0), ascending H3 (= ascending parent order), every
cell with DTM (``terrain_src != 0``), ≈ 5.6 M rows.  NEX1 container
(``header.sample_res``, ``rule``), ``ids: delta_u64`` then planar columns.
Stored once per generation as the full column superset
(``data/ne/sample/r10.nex.zst`` + ``r10.json``), ``fields=`` slices are
cached per generation (``sample/cache``).  Built only by the prewarm CLI
(``cells_bins.py --prewarm``, systemd ``cells-bins.timer``), never in
gunicorn.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import struct
import threading
import time
from collections import OrderedDict
from pathlib import Path

import numpy as np

import cells_api as ca
import cells_bins as cb

log = logging.getLogger('cells_blocks')

# === SECTION: constants ===

ALGO = 'nexb-2'
SAMPLE_ALGO = 'sample-2'         # sample-2 (2026-11-03): + tree_h_n / tree_h ragged lists
BLOCK_DIR = ca.NE_DIR / 'nexb'          # NOT data/ne/blocks — that holds the split-KG .h3b products
SAMPLE_DIR = ca.NE_DIR / 'sample'
SAMPLE_CACHE = SAMPLE_DIR / 'cache'
FIELDSETS_PATH = BLOCK_DIR / 'fieldsets.json'
ZSTD_LEVEL = 9                   # one level everywhere → strong ETag = byte-identical body
SAMPLE_ZSTD_LEVEL = 19
SAMPLE_RES = (9, 10, 11)
SAMPLE_DEFAULT_RES = 10
MAX_FIELDSETS = 8
SAMPLE_CACHE_FILES = 16

# the nine columns a downstream glyph filter uses
GLYPH_FIELDS = ('bld_h_max', 'bld_h_p50', 'stories_max', 'roof_dom', 'canopy_h_p90',
                'aspect_deg', 'slope_mean', 'slope_p90', 'elev_mean')
# cheap extras (< ~2 MB each nationally, zstd) not used by the glyph filter yet
SAMPLE_EXTRA_FIELDS = ('obs', 'terrain_src', 'bld_n', 'bld_area_sum', 'struct_h_max', 'canopy_h_p50',
                       'lc25', 'tree_n', 'dh_m', 'consistency', 'forest_loss_year', 'phenology',
                       'als_year_newest')
SAMPLE_DEFAULT_FIELDS = GLYPH_FIELDS + SAMPLE_EXTRA_FIELDS
# superset stored on disk: everything except the 9-wide cover block
SAMPLE_STORED_FIELDS = tuple(f for f in cb.FIELDS if f not in ('id', 'cover'))

_meta_lock = threading.Lock()
_zstd = None


def _z():
    global _zstd
    if _zstd is None:
        import zstandard
        _zstd = zstandard
    return _zstd


def zcompress(b: bytes, level: int = ZSTD_LEVEL) -> bytes:
    return _z().ZstdCompressor(level=level).compress(b)


def zdecompress(b: bytes) -> bytes:
    return _z().ZstdDecompressor().decompress(b, max_output_size=1 << 31)


def fields_hash(fields) -> str:
    return hashlib.sha256((','.join(fields) + '|bitmap').encode()).hexdigest()[:8]


def block_etag(gen: str, fields) -> str:
    return cb.block_etag(gen, list(fields), 'bitmap')


# === SECTION: block body ===

def _bitmap(n: int) -> bytes:
    b = np.full((n + 7) // 8, 0xFF, np.uint8)
    if n % 8:
        b[-1] = (1 << (n % 8)) - 1
    return b.tobytes()


def build_block(bi: int, bj: int, fields, *, rebuild: bool = False) -> tuple[dict, bytes, list[dict]]:
    """→ (header, raw NEXB v2 body, bin inputs).  Loads (builds if needed) the
    16 bins via :func:`cells_bins.load_bin`."""
    fields = cb.with_ragged_counts([f for f in fields if f != 'id'])
    cols = {f: [] for f in fields}
    entries = []; gens = []; kgs = set(); epochs = []; inps = []
    row0 = 0; legend: dict = {}
    for i, j in cb.block_bins(bi, bj):
        hdr, c, inp = cb.load_bin(i, j, rebuild=rebuild, need=fields)
        ids = np.ascontiguousarray(c['id']).astype(np.uint64)
        n = int(ids.size)
        x = int(np.bitwise_xor.reduce(ids)) if n else 0
        entries.append({'i': i, 'j': j, 'n': n, 'row0': row0, 'ids_xor': f'{x:016x}', 'gen': hdr['gen'],
                        'complete': bool(hdr.get('complete'))})
        for f in fields:
            cols[f].append(np.ascontiguousarray(c[f]))
        gens.append(hdr['gen']); kgs.update(hdr.get('kgs') or []); inps.append(inp)
        if hdr.get('epoch'):
            epochs.append(str(hdr['epoch']))
        legend.update(hdr.get('lc25_legend') or {})
        row0 += n
    n = row0
    gen = cb.block_gen(gens)
    w, s, e, nn = cb.block_bbox(bi, bj)
    specs = []; parts = [_bitmap(n)]
    for f in fields:
        a = np.concatenate(cols[f]) if cols[f] else np.zeros(0, cb.NP_DT[cb.FIELDS[f]['dtype']])
        a = np.ascontiguousarray(a)
        specs.append(cb._col_spec(f, a)); parts.append(a.tobytes())
    partial = any(x['missing'] or x['partial_files'] for x in inps)
    h = {'algo': ALGO, 'container': 'nexb', 'v': 2, 'ids': 'bitmap', 'block_deg': cb.BLOCK_DEG, 'bin_deg': cb.BIN_DEG,
         'ij': [bi, bj], 'bbox': [w, s, e, nn], 'gen': gen, 'epoch': max(epochs) if epochs else ca.epoch_hdr(None),
         'fields': fields, 'columns': specs, 'n': n, 'present_bytes': (n + 7) // 8,
         'enumeration': 'per bin: all res-12 cells whose centre lies in the half-open bin [w,w+0.025)×[s,s+0.025) '
                        '(h3 polygonToCells centre rule), ascending H3; rows = bins concatenated in header.bins order',
         'order': 'row-major: j south→north, i west→east', 'bins': entries, 'kgs': sorted(kgs),
         'partial': partial, 'lc25_legend': legend, 'building_types': list(cb.BUILDING_TYPES)}
    hb = json.dumps(h, separators=(',', ':'), ensure_ascii=False).encode()
    return h, cb.BLOCK_MAGIC + struct.pack('<I', len(hb)) + hb + b''.join(parts), inps


def decode_block(blob: bytes) -> tuple[dict, np.ndarray, 'OrderedDict[str, np.ndarray]']:
    """NEXB v2 (raw, after zstd) → (header, present bool[n], columns)."""
    if blob[:4] != cb.BLOCK_MAGIC:
        raise ValueError('not a NEXB blob')
    ln = struct.unpack('<I', blob[4:8])[0]
    hdr = json.loads(blob[8:8 + ln])
    if hdr.get('v') != 2:
        raise ValueError('not a NEXB v2 blob (use cells_bins.decode_block)')
    off = 8 + ln; n = int(hdr['n'])
    pb = np.frombuffer(blob[off:off + hdr['present_bytes']], np.uint8); off += hdr['present_bytes']
    present = np.unpackbits(pb, bitorder='little')[:n].astype(bool)
    cols: 'OrderedDict[str, np.ndarray]' = OrderedDict()
    for c in hdr['columns']:
        nb = int(c['bytes'])
        a = np.frombuffer(blob[off:off + nb], cb.NP_DT[c['dtype']])
        if c.get('shape') and c['shape'] != 'ragged':
            a = a.reshape((n, *c['shape']))
        cols[c['name']] = a; off += nb
    return hdr, present, cols


# === SECTION: block store ===

def _paths(bi: int, bj: int, fields) -> tuple[Path, Path]:
    stem = f'{bi}_{bj}.{fields_hash(fields)}'
    return BLOCK_DIR / f'{stem}.nexb.zst', BLOCK_DIR / f'{stem}.json'


def stored_meta(bi: int, bj: int, fields) -> dict | None:
    _, mp = _paths(bi, bj, fields)
    try:
        return json.loads(mp.read_text())
    except (OSError, ValueError):
        return None


def current_gen(bi: int, bj: int) -> tuple[str, list[dict]]:
    inps = [cb.bin_inputs(i, j) for i, j in cb.block_bins(bi, bj)]
    return cb.block_gen([x['gen'] for x in inps]), inps


def store_block(bi: int, bj: int, fields, *, rebuild: bool = False) -> dict:
    """Build + write ``<bi>_<bj>.<fh>.nexb.zst`` and its meta → meta."""
    t0 = time.time()
    hdr, raw, inps = build_block(bi, bj, fields, rebuild=rebuild)
    z = zcompress(raw)
    zp, mp = _paths(bi, bj, fields)
    meta = {'bi': bi, 'bj': bj, 'gen': hdr['gen'], 'etag': block_etag(hdr['gen'], fields), 'bytes': len(z),
            'raw_bytes': len(raw), 'n': hdr['n'], 'epoch': hdr.get('epoch'), 'fields': list(fields),
            'fh': fields_hash(fields), 'partial': bool(hdr.get('partial')), 'kgs': hdr.get('kgs'),
            'bins_n': [e['n'] for e in hdr['bins']], 'built_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'build_ms': int((time.time() - t0) * 1000), 'algo': ALGO, 'zstd_level': ZSTD_LEVEL}
    BLOCK_DIR.mkdir(parents=True, exist_ok=True)
    tmp = zp.with_name(zp.name + '.tmp'); tmp.write_bytes(z); os.replace(tmp, zp)
    tmp = mp.with_name(mp.name + '.tmp'); tmp.write_text(json.dumps(meta)); os.replace(tmp, mp)
    return meta


def serve_block(bi: int, bj: int, fields, *, rebuild: bool = False) -> tuple[bytes, dict, str, dict]:
    """→ (zstd body, headers, etag, meta).  Disk when current, else build+store."""
    fields = cb.with_ragged_counts([f for f in fields if f != 'id'])
    gen, inps = current_gen(bi, bj)
    zp, _ = _paths(bi, bj, fields)
    meta = None if rebuild else stored_meta(bi, bj, fields)
    body = None
    if meta and meta.get('gen') == gen and meta.get('algo') == ALGO and meta.get('zstd_level') == ZSTD_LEVEL:
        try:
            body = zp.read_bytes()
            if len(body) != meta['bytes']:
                body = None
        except OSError:
            body = None
    if body is None:
        with cb._lock_for(('block', bi, bj, fields_hash(fields))):
            meta = None if rebuild else stored_meta(bi, bj, fields)
            if meta and meta.get('gen') == gen and meta.get('algo') == ALGO and meta.get('zstd_level') == ZSTD_LEVEL:
                body = zp.read_bytes()
            else:
                meta = store_block(bi, bj, fields, rebuild=rebuild)
                body = zp.read_bytes()
        remember_fieldset(fields)
    etag = meta['etag']
    hx = {'X-Cells-Gen': meta['gen'], 'X-Cells-Block': f'{bi},{bj}', 'X-Cells-Record': f'nexb2/{ALGO}',
          'X-Cells-Kgs': ','.join(meta.get('kgs') or []), 'X-Cells-Fields': ','.join(fields), 'X-Cells-Ids': 'bitmap',
          'X-Cells-Layout': 'nexb2=' + ','.join(str(x) for x in meta.get('bins_n') or []),
          'X-Cells-Built': meta.get('built_at'), 'X-Cells-Raw-Bytes': str(meta.get('raw_bytes'))}
    hx = ca._with_epoch(hx, meta.get('epoch'))
    if meta.get('partial'):
        hx['X-Cells-Partial'] = '1'
    return body, hx, etag, meta


def quick_etag(bi: int, bj: int, fields) -> tuple[str, str, list[dict]]:
    gen, inps = current_gen(bi, bj)
    return block_etag(gen, cb.with_ragged_counts([f for f in fields if f != 'id'])), gen, inps


# === SECTION: field-set registry (what to prewarm) ===

def prewarm_fieldsets() -> list[list[str]]:
    """Default + glyph field sets + every set a block request used (≤ 8)."""
    out = [list(cb.DEFAULT_FIELDS), list(GLYPH_FIELDS)]
    try:
        for fs in json.loads(FIELDSETS_PATH.read_text()).get('fieldsets') or []:
            fs = [f for f in fs if f in cb.FIELDS and f != 'id']
            if fs and fs not in out:
                out.append(fs)
    except (OSError, ValueError):
        pass
    return out[:2 + MAX_FIELDSETS]


def remember_fieldset(fields) -> None:
    fields = [f for f in fields if f != 'id']
    if fields in (list(cb.DEFAULT_FIELDS), list(GLYPH_FIELDS)):
        return
    with _meta_lock:
        try:
            d = json.loads(FIELDSETS_PATH.read_text())
        except (OSError, ValueError):
            d = {'fieldsets': []}
        fss = [fs for fs in d.get('fieldsets') or [] if fs != fields]
        fss.insert(0, fields)
        d['fieldsets'] = fss[:MAX_FIELDSETS]; d['updated_at'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        try:
            BLOCK_DIR.mkdir(parents=True, exist_ok=True)
            tmp = FIELDSETS_PATH.with_suffix('.tmp'); tmp.write_text(json.dumps(d)); os.replace(tmp, FIELDSETS_PATH)
        except OSError as e:
            log.debug('cells_blocks: fieldsets: %s', e)


# === SECTION: manifest (every block touching a published product) ===

_manifest_cache: dict = {}
_manifest_guard = threading.Lock()


def block_gens() -> tuple[str, dict]:
    """(bin-manifest generation, {(bi,bj): (gen, bins_in_manifest, complete)}) for
    every block with ≥ 1 manifest bin; bins outside the manifest get their
    (empty-input) generation from ``bin_inputs``."""
    m = cb.manifest(); b = m['bins']
    key = m['generation']
    with _manifest_guard:
        hit = _manifest_cache.get('gens')
        if hit and hit[0] == key:
            return key, hit[1]
    bg = {(i, j): (g, c) for i, j, g, c in zip(b['i'], b['j'], b['gen'], b['complete'])}
    per: dict = {}
    for (i, j) in bg:
        per.setdefault((i // cb.BLOCK_N, j // cb.BLOCK_N), None)
    out = {}
    for bij in sorted(per):
        gens = []; nman = 0; comp = True
        for ij in cb.block_bins(*bij):
            if ij in bg:
                gens.append(bg[ij][0]); nman += 1; comp = comp and bool(bg[ij][1])
            else:
                gens.append(cb.bin_inputs(*ij)['gen'])
        out[bij] = (cb.block_gen(gens), nman, comp)
    with _manifest_guard:
        _manifest_cache['gens'] = (key, out)
    return key, out


def manifest(fields) -> dict:
    """``/api/v1/cells/blocks``: one row per block — bi, bj, gen, etag, bytes,
    stored (prebuilt for this field set), current (stored gen == gen), n,
    bins_n (manifest bins), complete."""
    fields = [f for f in fields if f != 'id']
    fh = fields_hash(fields)
    gen_key, gens = block_gens()
    ck = (gen_key, fh, int(time.time() // 20))
    with _manifest_guard:
        hit = _manifest_cache.get('manifest')
        if hit and hit[0] == ck:
            return hit[1]
    t0 = time.time()
    BI = []; BJ = []; G = []; ET = []; BY = []; ST = []; CU = []; N = []; BN = []; C = []
    n_cur = 0; n_stored = 0; bytes_total = 0
    for (bi, bj), (g, nman, comp) in gens.items():
        meta = stored_meta(bi, bj, fields)
        ok = bool(meta and meta.get('algo') == ALGO and meta.get('zstd_level') == ZSTD_LEVEL)
        cur = ok and meta.get('gen') == g
        BI.append(bi); BJ.append(bj); G.append(g); ET.append(block_etag(g, fields))
        BY.append(int(meta['bytes']) if ok else None); ST.append(int(ok)); CU.append(int(cur))
        N.append(int(meta['n']) if ok else None); BN.append(nman); C.append(int(comp))
        n_cur += cur; n_stored += ok; bytes_total += int(meta['bytes']) if ok else 0
    v = {'block_deg': cb.BLOCK_DEG, 'bin_deg': cb.BIN_DEG, 'algo': ALGO, 'ids': 'bitmap', 'fields': fields, 'fh': fh,
         'epoch': ca.epoch_hdr(None), 'generation': hashlib.sha256((gen_key + '|' + fh).encode()).hexdigest()[:16],
         'n': len(BI), 'prebuilt_current': n_cur, 'prebuilt_any': n_stored, 'bytes_total': bytes_total,
         'blocks': {'bi': BI, 'bj': BJ, 'gen': G, 'etag': ET, 'bytes': BY, 'stored': ST, 'current': CU, 'n': N,
                    'bins_n': BN, 'complete': C},
         'bbox_of': 'w = bi·block_deg, s = bj·block_deg, e = w + block_deg, n = s + block_deg (half-open)',
         'request': 'GET /api/v1/cells?bbox=w,s,e,n&format=bin&block=1&fields=' + ','.join(fields),
         'computed_ms': int((time.time() - t0) * 1000)}
    with _manifest_guard:
        _manifest_cache['manifest'] = (ck, v)
    return v


def blocks_status(fields=None) -> dict:
    m = manifest(fields or cb.DEFAULT_FIELDS)
    return {'blocks': m['n'], 'prebuilt_current': m['prebuilt_current'], 'prebuilt_any': m['prebuilt_any'],
            'bytes_total': m['bytes_total'], 'fieldsets': [','.join(f) for f in prewarm_fieldsets()]}


# === SECTION: prewarm (called from cells_bins.prewarm) ===

def prewarm_blocks(budget_s: float | None = None, limit: int | None = None, verbose: bool = True,
                   jobs: int = 1) -> dict:
    """Build / refresh every manifest block whose stored blob (per prewarm
    field set) is missing or stale.  Missing first, then stale."""
    from concurrent.futures import ThreadPoolExecutor
    t0 = time.time()
    _, gens = block_gens()
    todo_missing = []; todo_stale = []; n_ok = 0
    for fs in prewarm_fieldsets():
        for (bi, bj), (g, _, _) in gens.items():
            meta = stored_meta(bi, bj, fs)
            ok = bool(meta and meta.get('algo') == ALGO and meta.get('zstd_level') == ZSTD_LEVEL)
            if ok and meta.get('gen') == g:
                n_ok += 1
            elif ok:
                todo_stale.append((bi, bj, fs))
            else:
                todo_missing.append((bi, bj, fs))
    todo = todo_missing + todo_stale
    if limit:
        todo = todo[:limit]
    n_built = 0; n_err = 0; stop = False; lock = threading.Lock()

    def one(job):
        nonlocal n_built, n_err, stop
        if stop or (budget_s is not None and time.time() - t0 > budget_s):
            stop = True
            return
        try:
            store_block(job[0], job[1], job[2])
            with lock:
                n_built += 1
                if verbose and n_built % 25 == 0:
                    print(f'  blocks {n_built}/{len(todo)} (+{n_ok} current) … {time.time() - t0:.0f}s', flush=True)
        except Exception as e:  # noqa: BLE001
            with lock:
                n_err += 1
            log.warning('cells_blocks: prewarm %s: %s', job[:2], e)

    if jobs > 1:
        with ThreadPoolExecutor(max_workers=jobs) as ex:
            list(ex.map(one, todo))
    else:
        for job in todo:
            one(job)
            if stop:
                break
    with _manifest_guard:
        _manifest_cache.pop('manifest', None)
    return {'blocks': len(gens), 'fieldsets': len(prewarm_fieldsets()), 'built': n_built, 'errors': n_err,
            'current_before': n_ok, 'missing_before': len(todo_missing), 'stale_before': len(todo_stale),
            'remaining': len(todo_missing) + len(todo_stale) - n_built, 'stopped_on_budget': bool(stop),
            'seconds': round(time.time() - t0, 1)}


# === SECTION: national sample ===

def _sample_mask(ids: np.ndarray, res: int) -> np.ndarray:
    """True where the res-12 id is the centre child of its res-``res`` parent:
    digits res+1 … 12 all zero (digit k sits at bits 3·(15−k) … +2)."""
    mask = (1 << (3 * (12 - res))) - 1
    return ((ids >> np.uint64(9)) & np.uint64(mask)) == 0


def _sample_paths(res: int) -> tuple[Path, Path]:
    return SAMPLE_DIR / f'r{res}.nex.zst', SAMPLE_DIR / f'r{res}.json'


def sample_gen() -> str:
    return hashlib.sha256(f'{SAMPLE_ALGO}|{cb.manifest()["generation"]}'.encode()).hexdigest()[:24]


def sample_meta(res: int) -> dict | None:
    try:
        return json.loads(_sample_paths(res)[1].read_text())
    except (OSError, ValueError):
        return None


def sample_etag(gen: str, res: int, fields) -> str:
    rep = hashlib.sha256(','.join(fields).encode()).hexdigest()[:8]
    return f'"S-r{res}-{gen}-{rep}"'


def _sample_header(res: int, n: int, gen: str, epoch, fields, extra: dict | None = None) -> dict:
    h = {'algo': SAMPLE_ALGO, 'container': 'nex1', 'sample_res': res, 'h3_res': ca.RES, 'n': n, 'gen': gen,
         'epoch': epoch, 'fields': list(fields), 'ids': 'delta_u64',
         'rule': f'one row per res-{res} cell of Austria = its res-12 centre child (h3 cellToCenterChild(parent, 12)); '
                 'ascending H3 (= ascending parent order); every cell with DTM (terrain_src != 0)',
         'source': 'cells_bins (0.025° NEX1 bins): .h3b rows + NET1 + NEB1 + grid25 + roof hints — no new per-cell computation',
         'building_types': list(cb.BUILDING_TYPES)}
    h.update(extra or {})
    return h


def build_sample(res: int = SAMPLE_DEFAULT_RES, *, force: bool = False, verbose: bool = True,
                 budget_s: float | None = None) -> dict:
    """Reduce every stored bin to its centre-child rows → ``sample/r<res>.nex.zst``.
    Bins missing on disk are skipped (counted in ``bins_missing``) — the next
    prewarm pass builds them and the generation then differs → rebuild."""
    with cb._lock_for(('sample', res)):      # one builder at a time (timer + CLI)
        return _build_sample_locked(res, force=force, verbose=verbose, budget_s=budget_s, t0=time.time())


def _build_sample_locked(res, *, force, verbose, budget_s, t0):
    """Memory-bounded: pass 1 appends every bin's selected rows per field to a
    spill file (bin order) and keeps only the ids; then the H3 permutation is
    applied one column at a time while streaming straight into the zstd
    writer.  Peak RSS ≈ ids + permutation + two columns (~150 MB) instead of
    3 × the raw body (~1 GB) — the full-corpus rebuild after an algo / epoch
    bump runs inside the 2.5 GB cells-bins cgroup together with the bin builder."""
    gen = sample_gen()
    old = sample_meta(res)
    if old and old.get('gen') == gen and not force:
        return dict(old, skipped=True)
    m = cb.manifest(); b = m['bins']
    fields = list(SAMPLE_STORED_FIELDS)
    spill = SAMPLE_DIR / f'spill-r{res}'
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    if spill.exists():
        for q in spill.iterdir():
            q.unlink()
    spill.mkdir(exist_ok=True)
    fh = {f: open(spill / f, 'wb') for f in fields}
    ids_parts = []
    n_bins = 0; n_missing = 0; n_stale = 0; n_rows = 0; epochs = set(); legend: dict = {}
    order = sorted(zip(b['i'], b['j'], b['gen']))
    try:
        for k, (i, j, g) in enumerate(order):
            if budget_s is not None and time.time() - t0 > budget_s:
                return {'aborted': 'budget', 'bins_done': k, 'bins': len(order), 'seconds': round(time.time() - t0, 1)}
            sg = cb.stored_gen(i, j)
            if sg is None:
                n_missing += 1
                continue
            if sg != g:
                n_stale += 1
            try:
                hdr, c, _ = cb.load_bin(i, j, need=fields)
            except Exception as e:  # noqa: BLE001
                log.warning('cells_blocks: sample bin %d_%d: %s', i, j, e)
                n_missing += 1
                continue
            ids = np.ascontiguousarray(c['id']).astype(np.uint64)
            sel = _sample_mask(ids, res) & (c['terrain_src'] != 0)
            n_bins += 1
            if hdr.get('epoch'):
                epochs.add(str(hdr['epoch']))
            legend.update(hdr.get('lc25_legend') or {})
            if not sel.any():
                continue
            ids_parts.append(ids[sel])
            for f in fields:
                if cb.FIELDS[f].get('shape') == 'ragged':
                    a = cb.ragged_take(c[cb.RAGGED[f]], c[f], sel)[1]
                else:
                    a = c[f][sel]
                fh[f].write(np.ascontiguousarray(a).tobytes())
            n_rows += int(sel.sum())
            if verbose and k % 2000 == 0:
                print(f'  sample r{res}: bin {k}/{len(order)} rows={n_rows} … {time.time() - t0:.0f}s', flush=True)
        for f in fields:
            fh[f].close()
        ids = np.concatenate(ids_parts) if ids_parts else np.zeros(0, np.uint64)
        del ids_parts
        o = np.argsort(ids, kind='stable')
        n = int(ids.size)
        epoch = max(epochs) if epochs else ca.epoch_hdr(None)
        hdr = _sample_header(res, n, gen, epoch, fields,
                             {'lc25_legend': legend, 'bins_n': n_bins, 'bins_missing': n_missing, 'bins_stale_at_build': n_stale})

        def col(f):
            return np.fromfile(spill / f, cb.NP_DT[cb.FIELDS[f]['dtype']])

        specs = [cb._col_spec('id', ids)]
        for f in fields:
            fd = cb.FIELDS[f]
            nb = (spill / f).stat().st_size
            if fd.get('shape') != 'ragged' and fd.get('shape'):
                nb = n * int(np.prod(fd['shape'])) * np.dtype(cb.NP_DT[fd['dtype']]).itemsize
            specs.append(dict(cb._col_spec(f, np.zeros(0, cb.NP_DT[fd['dtype']])), bytes=int(nb)))
        h = dict(hdr); h['ids'] = 'delta_u64'; h['columns'] = specs; h['fields'] = fields
        hb = json.dumps(h, separators=(',', ':'), ensure_ascii=False).encode()
        zp, mp = _sample_paths(res)
        tmp = zp.with_name(zp.name + '.tmp')
        raw_bytes = 0
        with open(tmp, 'wb') as out, _z().ZstdCompressor(level=SAMPLE_ZSTD_LEVEL).stream_writer(out) as zw:
            def put(bb):
                nonlocal raw_bytes
                raw_bytes += len(bb); zw.write(bb)
            put(cb.MAGIC + struct.pack('<I', len(hb)) + hb)
            put(np.diff(ids[o].astype(np.int64), prepend=0).astype('<u8').tobytes())
            for f in fields:
                fd = cb.FIELDS[f]
                a = col(f)
                if fd.get('shape') == 'ragged':
                    a = cb.ragged_take(col(cb.RAGGED[f]), a, o)[1]
                elif fd.get('shape'):
                    a = a.reshape((n, *fd['shape']))[o]
                else:
                    a = a[o]
                put(np.ascontiguousarray(a).tobytes()); del a
        z_bytes = tmp.stat().st_size
        os.replace(tmp, zp)
        meta = {'res': res, 'gen': gen, 'epoch': epoch, 'n': n, 'fields': fields, 'bytes': z_bytes, 'raw_bytes': raw_bytes,
                'bins_n': n_bins, 'bins_missing': n_missing, 'bins_stale_at_build': n_stale, 'algo': SAMPLE_ALGO,
                'built_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'seconds': round(time.time() - t0, 1),
                'default_fields': list(SAMPLE_DEFAULT_FIELDS), 'glyph_fields': list(GLYPH_FIELDS)}
        tmp = mp.with_name(mp.name + '.tmp'); tmp.write_text(json.dumps(meta)); os.replace(tmp, mp)
    finally:
        for f in fh.values():
            try:
                f.close()
            except Exception:  # noqa: BLE001
                pass
        try:
            for q in spill.iterdir():
                q.unlink()
            spill.rmdir()
        except OSError:
            pass
    # drop slices of older generations
    try:
        for p in SAMPLE_CACHE.glob(f'r{res}-*.nex.zst'):
            if f'-{gen}-' not in p.name:
                p.unlink()
    except OSError:
        pass
    return meta


def parse_sample_fields(raw: str | None) -> list[str]:
    if not raw:
        return list(SAMPLE_DEFAULT_FIELDS)
    r = raw.strip().lower()
    if r in ('*', 'all'):
        return list(SAMPLE_STORED_FIELDS)
    if r == 'glyph':
        return list(GLYPH_FIELDS)
    if r == 'default':
        return list(SAMPLE_DEFAULT_FIELDS)
    fs = [x.strip() for x in raw.split(',') if x.strip() and x.strip() != 'id']
    bad = [f for f in fs if f not in SAMPLE_STORED_FIELDS]
    if bad:
        raise ca.CellsError(400, f'unknown sample fields {bad}', fields=list(SAMPLE_STORED_FIELDS))
    return cb.with_ragged_counts(fs)


_sample_lock = threading.Lock()


def serve_sample(res: int, fields) -> tuple[bytes, dict, str, dict]:
    """→ (zstd body, headers, etag, meta).  Whole stored superset when
    ``fields`` == stored order, else a cached slice (re-encoded, zstd)."""
    meta = sample_meta(res)
    if not meta:
        raise ca.CellsError(404, f'national sample r{res} not built yet (cells-bins.timer builds it)',
                            retry_after_s=ca.RETRY_AFTER_S)
    zp, _ = _sample_paths(res)
    fields = list(fields)
    etag = sample_etag(meta['gen'], res, fields)
    if fields == list(meta['fields']):
        body = zp.read_bytes()
    else:
        rep = hashlib.sha256(','.join(fields).encode()).hexdigest()[:8]
        cp = SAMPLE_CACHE / f'r{res}-{meta["gen"]}-{rep}.nex.zst'
        with _sample_lock:
            try:
                body = cp.read_bytes()
            except OSError:
                hdr, cols = cb.decode(zdecompress(zp.read_bytes()))
                hdr = dict(hdr); hdr.pop('columns', None)
                raw = cb.encode(hdr, cols, fields, 'delta_u64')
                body = zcompress(raw, 12)
                SAMPLE_CACHE.mkdir(parents=True, exist_ok=True)
                tmp = cp.with_suffix('.tmp'); tmp.write_bytes(body); os.replace(tmp, cp)
                try:
                    ps = sorted(SAMPLE_CACHE.glob('*.nex.zst'), key=lambda p: p.stat().st_mtime)
                    for p in ps[:-SAMPLE_CACHE_FILES]:
                        p.unlink()
                except OSError:
                    pass
    hx = {'X-Cells-Gen': meta['gen'], 'X-Cells-Record': f'nex1/{SAMPLE_ALGO}', 'X-Cells-Sample-Res': str(res),
          'X-Cells-Fields': ','.join(fields), 'X-Cells-Layout': f'nex1={meta["n"]}', 'X-Cells-Built': meta.get('built_at'),
          'X-Cells-Raw-Bytes': str(meta.get('raw_bytes')) if fields == list(meta['fields']) else None}
    hx = ca._with_epoch(hx, meta.get('epoch'))
    if meta.get('bins_missing'):
        hx['X-Cells-Partial'] = '1'
    return body, hx, etag, meta


def sample_status() -> dict:
    gen = sample_gen()
    out = {'gen': gen}
    for r in SAMPLE_RES:
        m = sample_meta(r)
        out[f'r{r}'] = None if not m else {'gen': m['gen'], 'current': m['gen'] == gen, 'n': m['n'], 'bytes': m['bytes'],
                                          'built_at': m['built_at'], 'bins_missing': m['bins_missing'], 'epoch': m.get('epoch')}
    return out


# === SECTION: docs ===

def dict_block() -> dict:
    return {
        'blocks_v2': {
            'what': 'id-free, prebuilt, zstd 0.1° block (16 bins) — the default block=1 form since 2026-11-03; ids=1 keeps NEXB v1',
            'request': 'GET /api/v1/cells?bbox=w,s,e,n&format=bin&block=1[&fields=…] with w=bi·0.1, s=bj·0.1, e=w+0.1, n=s+0.1; '
                       'send Accept-Encoding: zstd (else the body is inflated and gzip\'d)',
            'container': "'NEXB' | u32 header_len | header JSON {v:2, ids:'bitmap', gen, epoch, ij:[bi,bj], bbox, fields, "
                         'columns[{name,dtype,bytes,shape?,nodata?,unit}], n, present_bytes, bins:[{i,j,n,row0,ids_xor,gen,complete}×16], '
                         'kgs, partial, lc25_legend} | present bitmap (ceil(n/8) B, bit r = byte r>>3 bit r&7) | col_0 … col_k '
                         '(planar, n rows each, rows = the 16 bin enumerations concatenated in header.bins order)',
            'enumeration': 'per bin: all res-12 cells whose centre lies in the half-open bin [w,w+0.025)×[s,s+0.025) '
                           '(H3 polygonToCells centre rule), ascending H3. Check bins[k].n and bins[k].ids_xor (xor of the u64 '
                           'ids, hex) against your own enumeration; on mismatch re-request with ids=1 (NEXB v1 with ids)',
            'order': 'row-major: k = (j - 4·bj)·4 + (i - 4·bi) — latitude rows south→north, west→east within a row',
            'headers': {'ETag': 'strong: "B-<block gen>-<hash(fields|bitmap)>"; If-None-Match → 304',
                        'X-Cells-Gen': 'block gen = sha256 of the 16 bin gens — key your copy on this',
                        'X-Cells-Epoch': 'the ne-observe product epoch (same value as X-Cells-Epoch on /cells and /cells/bins)',
                        'X-Cells-Ids': 'bitmap', 'X-Cells-Layout': 'nexb2=n0,…,n15', 'Content-Encoding': 'zstd (when accepted)',
                        'X-Cells-Partial': '1 when a KG in the block has no published cells yet or is partial'},
            'manifest': 'GET /api/v1/cells/blocks[?fields=…][&epoch=…] → columns {bi, bj, gen, etag, bytes, stored, current, n, '
                        'bins_n, complete} for every block touching a published product (bytes/n null until prebuilt for that '
                        'field set); epoch= must equal the current X-Cells-Epoch or you get 409 with the current one',
            'precompute': 'cells-bins.timer (hourly, 50 min budget, nice 15) builds bins → blocks (default + glyph field sets + '
                          'any field set a block request used) → the national sample; a block that is requested before it is '
                          'prebuilt is built on the spot and stored (first request slow, then disk)',
            'size': '≈ 0.7–1.1 MB zstd per block for the 9 glyph fields (ids gone), ≈ 2 MB for the 18 defaults (cover[9])',
        },
        'sample': {
            'what': 'national res-10 sample: one row per res-10 cell of Austria = its res-12 centre child, ascending H3, '
                    'every cell with DTM (≈ 5.6 M rows), built from the stored bins once per generation',
            'request': 'GET /api/v1/cells/sample?res=10&format=bin[&fields=glyph|default|*|a,b,c] (res 9 / 11 too); '
                       'format=json or meta=1 → build meta; send Accept-Encoding: zstd',
            'container': "NEX1 — 'NEX1' | u32 header_len | header JSON {sample_res, n, gen, epoch, fields, ids:'delta_u64', "
                         'columns[…], rule, bins_missing} | ids (u64 delta: first absolute, then differences) | planar columns',
            'ids': 'res-12 centre child of the res-10 parent (h3 cellToCenterChild(parent,12)); parent = cellToParent(id,10)',
            'fields': {'glyph': list(GLYPH_FIELDS), 'default': list(SAMPLE_DEFAULT_FIELDS), '*': list(SAMPLE_STORED_FIELDS)},
            'headers': {'ETag': 'strong: "S-r<res>-<gen>-<hash(fields)>"; If-None-Match → 304',
                        'X-Cells-Gen': 'sample gen (changes when any bin generation changed — key your copy on this)',
                        'X-Cells-Epoch': 'the ne-observe product epoch', 'X-Cells-Built': 'build time',
                        'X-Cells-Partial': '1 when bins were missing on disk at build time (header.bins_missing)',
                        'Content-Encoding': 'zstd (when accepted)'},
            'size': '≈ 26 MB for fields=glyph, ≈ 40 MB default, ≈ 100 MB for *',
        },
    }


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--block', help='bi,bj → build/store the block and print the meta')
    ap.add_argument('--fields', help='comma list (default: cells_bins defaults; glyph = the 9 glyph fields)')
    ap.add_argument('--prewarm', action='store_true'); ap.add_argument('--budget-s', type=float); ap.add_argument('--jobs', type=int, default=1)
    ap.add_argument('--sample', type=int, nargs='?', const=SAMPLE_DEFAULT_RES, help='build the national sample at res (default 10)')
    ap.add_argument('--force', action='store_true'); ap.add_argument('--status', action='store_true'); ap.add_argument('--quiet', action='store_true')
    a = ap.parse_args()
    logging.basicConfig(level=logging.WARNING if a.quiet else logging.INFO)
    fs = list(GLYPH_FIELDS) if a.fields == 'glyph' else (cb.parse_fields(a.fields) if a.fields else list(cb.DEFAULT_FIELDS))
    if a.block:
        bi, bj = (int(x) for x in a.block.split(','))
        t = time.time(); meta = store_block(bi, bj, fs, rebuild=a.force); print(json.dumps(meta, indent=1)); print('ms', int((time.time() - t) * 1000))
        body, _, _, _ = serve_block(bi, bj, fs); hdr, present, cols = decode_block(zdecompress(body))
        print('rows', hdr['n'], 'present', int(present.sum()), 'cols', list(cols), 'zstd bytes', len(body))
    elif a.prewarm:
        print(json.dumps(prewarm_blocks(a.budget_s, verbose=not a.quiet, jobs=max(1, a.jobs)), indent=1))
    elif a.sample:
        print(json.dumps(build_sample(a.sample, force=a.force, verbose=not a.quiet), indent=1))
    elif a.status:
        print(json.dumps({'blocks': blocks_status(), 'sample': sample_status()}, indent=1))
