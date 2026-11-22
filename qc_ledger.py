"""qc_ledger — ONE ledger over the read-time fault detectors of the Austria run.

The detectors stay the source of truth; this module only *joins* them (docs/qc-lessons.md):

    ne_pos_fix.py       data/ne_pos_fix.json                  trees / structures 5 m off (per KG / per tile)
    rebin_added.py      data/rebin_added.sqlite               off-file / lost-cell apices, shift file pairs
    grid25_patch.py     data/grid25_patches/index.json        stale 25 m terrain tiles, internal tile edges
    grid25_registration data/grid25_registration.json         5 m shifted / mixed grid25 products
    label_mix.py        data/austria_processor/label_mix.json restored-checkpoint label-id collision
    product_repair.py   data/austria_processor/product_repair.json   cache-fillable layer gaps (-partial)
    ne_linear.py        data/linear_noise.sqlite              straight chains of noise apices (hidden)

One row per (product code, processing tile | whole product, fault class).  WHO is a product:

    parent   '22138'            whole KG, own tile grid (austria_processor._compute_tile_grid, 1.5 km
                                tiles, 100 m overlap, last column / row overshoots the bbox)
    block    '49006-north-1'    kg_splitter.maybe_split_kg (> 22 tiles or strikes): own bbox, OWN tile
                                grid, own products and manifest keys; tile_index restarts per block
    sweep    '63308-sw1'        sliver_sweep unit: add-on product of the parent (clipped grid, fresh,
                                no _json / _full_gpkg), merged into data/ne/<parent>.h3b

KG-keyed detectors (ne_pos_fix, rebin_added, ne_linear, label_mix on a merged .h3b) see the *parent*;
product-keyed ones (grid25_*, product_repair) the product code.  Tiles are therefore identified by their
EPSG:3035 envelope (the same rounding as ``ne_pos_fix.kg_tiles``) and joined across detectors on it.

The WHEN comes from the Zenodo manifest (upload time / version per key — read only, never written),
``product_origin`` (uploading peer, 10-day TTL), the product document (``upgraded_from``,
``data_quality.v2_source`` = which tiles the v2 upgrade re-read from BEV) and the harvested per-KG
processor log in ``kg_v2_store`` (= the per-day log archive, indexed by code): sessions, peers,
checkpoint tars, and the ``sidecar gpkg_full: dtm=a/b`` line that says how many tile rasters the v1
stitch had to re-read.

Cheap by construction: local files only (no BEV, no network), a full rebuild is ~1 min niced, skipped
when no input changed (``sig``).  The store is rebuilt into a temp file and renamed — idempotent, and a
reader never sees a half-built ledger.

    python3 qc_ledger.py --build [--force]     # hourly: ExecStartPost of ne-linear.service
    python3 qc_ledger.py --status              # = the `qc:` line on /process.txt
    python3 qc_ledger.py --worst 20            # = /process.txt?qc=20
    python3 qc_ledger.py --key 22138_full_gpkg # QC state behind one manifest key (derived join)
    python3 qc_ledger.py --report              # overlap + timing tables (docs/qc-lessons.md numbers)
"""
from __future__ import annotations

import collections
import hashlib
import json
import os
import re
import sqlite3
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATA = HERE / 'data'
DB_PATH = Path(os.environ.get('QC_LEDGER_DB', str(DATA / 'qc_ledger.sqlite')))
SUMMARY_PATH = Path(os.environ.get('QC_LEDGER_SUMMARY', str(DATA / 'qc_ledger.json')))
VERSION = 2
SIDECAR_COMMIT_DATE = '2026-05-29'      # dd08d74 (2026-05-28): raster sidecars; v1 GPKGs before it re-read every tile
ALIGN_COMMIT = 'eead178'                # _align_tdata_to_tile: no new shifted rasters

# fault class → (detector, tile level?, one-line meaning)
FAULTS = {
    'pos_shift':       ('ne_pos_fix', True, 'trees/structures 5 m off (-5,+5 m EPSG:3035)'),
    'pos_sweep_unit':  ('ne_pos_fix', False, 'fresh sweep unit inside a KG that is moved as a whole'),
    'apex_frame_pair': ('rebin_added', False, 'file pair whose equal-height apices sit at one 4-6 m offset'),
    'apex_unserved':   ('rebin_added', False, 'off-file / lost-cell apices re-added as system trees'),
    'apex_invariant':  ('rebin_added', False, 'stored != served - lost + added + dup + qa'),
    'grid25_stale':    ('grid25_patch', True, '25 m terrain tile does not match BEV'),
    'grid25_edge':     ('grid25_patch', True, 'step along an internal tile edge'),
    'grid25_shift':    ('grid25_registration', False, 'grid25 of the product 5 m off as a whole'),
    'grid25_mixed':    ('grid25_registration', False, 'grid25 shifted in some tiles only'),
    'label_mix':       ('label_mix', False, 'restored tile kept the label ids of the first run'),
    'layer_gap':       ('product_repair', False, 'cache-fillable layer missing (-partial)'),
    'linear_noise':    ('ne_linear', False, 'straight chains of noise apices'),
    'regenerated':     ('qc_generation', False, 'new product generation ingested: fixes of the old one retired, new quality documented'),
}
# classes whose products get the (costlier) document + log timing; the rest only manifest times
TIMED = ('pos_shift', 'pos_sweep_unit', 'apex_frame_pair', 'grid25_stale', 'grid25_edge', 'grid25_shift',
         'grid25_mixed', 'label_mix', 'layer_gap')
SUFFIXES = ('_light_gpkg_v2', '_light_gpkg', '_full_gpkg', '_json_v2', '_ne_h3b', '_json')
_SWEEP_RX = re.compile(r'^\d+-sw\d+$')
_SIDECAR_RX = re.compile(r'sidecar gpkg_full: dtm=(\d+)/(\d+) dtm_multi=(\d+)/(\d+) ortho=(\d+)/(\d+) hit_pct=(\d+)%')
_TILE_RX = re.compile(r'^tile (\d+)/(\d+) \u25b6 (lidar|ortho|segment|terrain)')


def kind_of(code: str) -> str:
    code = str(code)
    if _SWEEP_RX.match(code):
        return 'sweep'
    return 'block' if '-' in code else 'parent'


def parent_of(code: str) -> str:
    return str(code).split('-', 1)[0]


def split_key(key: str):
    """Manifest key → (product code, suffix) or (None, None) for marker keys (_requeue, _error, …)."""
    for s in SUFFIXES:
        if key.endswith(s):
            return key[:-len(s)], s[1:]
    return None, None


# ── sources (the live stores; tests substitute a fake with the same methods) ─────────────────────────

class Sources:
    AP = DATA / 'austria_processor'
    FILES = {
        'pos': DATA / 'ne_pos_fix.json', 'g25': DATA / 'grid25_patches' / 'index.json',
        'reg': DATA / 'grid25_registration.json', 'rebin': DATA / 'rebin_added.sqlite',
        'label_mix': AP / 'label_mix.json', 'repair': AP / 'product_repair.json',
        'linear': DATA / 'linear_noise.sqlite', 'manifest': AP / 'zenodo_manifest.json',
        'origin': AP / 'product_origin.json', 'store': DATA / 'kg_v2_store.db',
        'gen': DATA / 'qc_generations.sqlite', 'reprocess': AP / 'qc_reprocess.json',
    }

    def _json(self, k):
        try:
            return json.loads(self.FILES[k].read_text())
        except Exception:  # noqa: BLE001
            return {}

    def sig(self) -> str:
        """Changes when any input changed.  label_mix.json is rewritten every tick (file audit
        state) → its ``codes`` are hashed instead of the mtime."""
        p = [f'v{VERSION}']
        for k, f in sorted(self.FILES.items()):
            if k == 'label_mix':
                p.append(k + ':' + hashlib.sha1(json.dumps(self.label_mix(), sort_keys=True).encode()).hexdigest()[:12])
                continue
            try:
                st = f.stat(); p.append(f'{k}:{st.st_mtime_ns}:{st.st_size}')
            except OSError:
                p.append(f'{k}:-')
        return hashlib.sha1('|'.join(p).encode()).hexdigest()[:16]

    def pos_fix(self) -> dict:
        return self._json('pos')

    def grid25_index(self) -> dict:
        return self._json('g25')

    def registration(self) -> dict:
        return self._json('reg')

    def label_mix(self) -> dict:
        return (self._json('label_mix').get('codes') or {})

    def repair(self) -> dict:
        return self._json('repair')

    def origin(self) -> dict:
        return self._json('origin')

    def manifest(self) -> dict:
        return (self._json('manifest').get('entries') or {})

    def _ro(self, k):
        f = self.FILES[k]
        if not f.exists():
            return None
        c = sqlite3.connect(f'file:{f}?mode=ro', uri=True, timeout=20)
        c.row_factory = sqlite3.Row
        return c

    def rebin(self) -> dict:
        """{'resolved': bool, 'shifts': [[kg, other, dx, dy, matched, with_neighbour]], 'kgs': [row dicts]}"""
        out = {'resolved': False, 'shifts': [], 'kgs': [], 'scanned': 0}
        try:
            c = self._ro('rebin')
            if c is None:
                return out
            m = dict(c.execute('SELECT k, v FROM meta').fetchall())
            out['resolved'] = bool(m.get('resolved')) and m.get('resolved') != '0'
            out['shifts'] = json.loads(m.get('shifts') or '[]')
            out['scanned'] = c.execute('SELECT COUNT(*) FROM kgs WHERE token IS NOT NULL').fetchone()[0]
            out['kgs'] = [dict(r) for r in c.execute(
                'SELECT kg, stored, served, off, added, no_row, other_file, dup, qa, ok, err, lost, ldup, lost_cell '
                'FROM kgs WHERE token IS NOT NULL AND (added > 0 OR ok = 0 OR err IS NOT NULL)')]
            c.close()
        except Exception as e:  # noqa: BLE001
            out['error'] = str(e)[:120]
        return out

    def generations(self) -> list:
        """Product generations that replaced an earlier one (``qc_generation.on_ingest``)."""
        try:
            import qc_generation as qg
            return qg.generations(acted_only=True)
        except Exception:  # noqa: BLE001
            return []

    def reprocess(self) -> dict:
        return (self._json('reprocess') or {}).get('codes') or {}

    def linear(self) -> list:
        try:
            c = self._ro('linear')
            if c is None:
                return []
            r = [dict(x) for x in c.execute('SELECT kg, n_apex, n_flag, n_runs, ts FROM kg WHERE n_runs > 0')]
            c.close()
            return r
        except Exception:  # noqa: BLE001
            return []

    ARCHIVE_DIR = DATA / 'log_archive'
    ARCHIVE_CACHE = DATA / 'qc_ledger_archive.json'

    def archive_sidecar(self) -> dict:
        """{code: [[ts, peer, dtm_hit, dtm_total, multi_hit, multi_total, ortho_hit, ortho_total, hit_pct]]} from the
        per-day log archive (data/log_archive/*.jsonl.gz).  The harvested per-KG ring keeps a sample; the
        archive has every `sidecar gpkg_full` line.  Incremental: a day file is read once (name + size)."""
        import gzip
        try:
            cache = json.loads(self.ARCHIVE_CACHE.read_text())
        except Exception:  # noqa: BLE001
            cache = {}
        if cache.get('v') != 1:
            cache = {'v': 1, 'days': {}, 'sidecar': {}}
        dirty = False
        today = time.strftime('%Y-%m-%d', time.gmtime())
        for f in sorted(self.ARCHIVE_DIR.glob('*.jsonl.gz')) if self.ARCHIVE_DIR.exists() else []:
            sz = f.stat().st_size
            if cache['days'].get(f.name) == sz or f.name.startswith(today):
                continue
            try:
                with gzip.open(f, 'rt', errors='replace') as fh:
                    for line in fh:
                        if 'sidecar gpkg_full' not in line:
                            continue
                        try:
                            l = json.loads(line)
                        except Exception:  # noqa: BLE001
                            continue
                        m = _SIDECAR_RX.search(l.get('msg', '')); c = re.match(r'KG ([0-9][0-9a-z-]*):', l.get('msg', ''))
                        code = l.get('kg') or (c.group(1) if c else '')
                        if m and code:
                            rec = [l['ts'][:19], l.get('peer')] + [int(x) for x in m.groups()]
                            lst = cache['sidecar'].setdefault(code, [])
                            if rec not in lst:
                                lst.append(rec); lst.sort()
            except Exception:  # noqa: BLE001
                continue
            cache['days'][f.name] = sz; dirty = True
        if dirty:
            tmp = self.ARCHIVE_CACHE.with_suffix(f'.tmp{os.getpid()}')
            tmp.write_text(json.dumps(cache)); os.replace(tmp, self.ARCHIVE_CACHE)
        return cache['sidecar']

    # product documents / logs (kg_v2_store, local SQLite)
    def codes_for_parent(self, kg) -> list:
        import kg_v2_store as kvs
        return list(kvs.codes_for_parent(str(kg)))

    def doc(self, code) -> dict:
        import kg_v2_store as kvs
        try:
            return kvs.get(code) or {}
        except Exception:  # noqa: BLE001
            return {}

    def log(self, code) -> list:
        import kg_v2_store as kvs
        try:
            return kvs.get_log(code, include_parent=False) or []
        except Exception:  # noqa: BLE001
            return []


# ── tiles ────────────────────────────────────────────────────────────────────────────────────

_TF = {}


def _to3035():
    if 'tf' not in _TF:
        from pyproj import Transformer
        _TF['tf'] = Transformer.from_crs('EPSG:4326', 'EPSG:3035', always_xy=True)
    return _TF['tf']


def envelope(bbox_wgs) -> tuple:
    """EPSG:3035 integer envelope of a tile's WGS84 bbox — the rounding of ``ne_pos_fix.kg_tiles``."""
    w, s, e, n = bbox_wgs
    x, y = _to3035().transform([w, e, e, w], [s, s, n, n])
    return (round(min(x)), round(min(y)), round(max(x)), round(max(y)))


def doc_tiles(doc: dict) -> list:
    """[(tile_index, x0, y0, x1, y1)] of a product document (``data_quality.tiles``)."""
    out = []
    for i, t in enumerate((doc.get('data_quality') or {}).get('tiles') or []):
        if t.get('bbox_wgs'):
            out.append((int(t.get('tile_index') or i + 1),) + envelope(t['bbox_wgs']))
    return out


def border_m(env):
    """Distance (m) of a tile envelope to the national outline (``austria_mask`` = the NE server's
    outline, local file): 0 = the tile crosses the border, None = no outline.  Evidence for
    `grid25_stale`: BEV used to serve elevation beyond the border and later withdrew it."""
    try:
        if 'line' not in _TF:
            import austria_mask
            _TF['line'] = (austria_mask._load() or {}).get('line')
        if _TF['line'] is None or env is None:
            return None
        from shapely.geometry import box
        return int(round(box(*env).distance(_TF['line'])))
    except Exception:  # noqa: BLE001
        return None


def border_band(d) -> str:
    return 'unknown' if d is None else 'crosses' if d == 0 else '<1.5km' if d < 1500 else '<5km' if d < 5000 else '>=5km'


def _fallback_corners(doc: dict) -> set:
    """SW corners of the tiles the v2 upgrade did NOT take from the v1 GPKG (``v2_source``: BEV re-read)."""
    out = set()
    for k in ((doc.get('data_quality') or {}).get('v2_source') or {}).get('fallback_tiles') or []:
        try:
            a, b = str(k).split(',')[:2]
            out.add((int(float(a)), int(float(b))))
        except Exception:  # noqa: BLE001
            pass
    return out


def _near(env, corners, tol=12) -> bool:
    return any(abs(env[0] - a) <= tol and abs(env[1] - b) <= tol for a, b in corners)


# ── timing (WHEN / WHO) ───────────────────────────────────────────────────────────────────────────────

def timing(code: str, doc: dict, log: list, keys: dict, sidecar: list | None = None) -> dict:
    """Processing history of one product from its document, manifest keys and harvested log."""
    t: dict = {}
    up = doc.get('upgraded_from') or {}
    v1_gen = up.get('v1_generated_at') or ''
    t['origin'] = 'upgraded' if up else ('fresh' if doc else 'nodoc')
    if v1_gen:
        t['v1_generated_at'] = v1_gen[:19]
    full = (keys.get('full_gpkg') or [None])[0] or ''
    v1_ref = (v1_gen or full or '')[:19]
    if v1_ref:
        t['era'] = 'pre-sidecar' if v1_ref < SIDECAR_COMMIT_DATE else 'post-sidecar'
    vs = (doc.get('data_quality') or {}).get('v2_source') or {}
    if vs:
        d = (vs.get('by_layer') or {}).get('DTM') or {}
        t['v2_dtm'] = [d.get('gpkg', 0), d.get('fallback', 0)]
        if d.get('max_invalid_frac'):
            t['v2_dtm_invalid_max'] = d['max_invalid_frac']
    if sidecar:                     # archive lines: the one of the v1 stitch = the last at or before the v1 document (+1 day)
        day = v1_ref[:10]
        pick = [r for r in sidecar if not day or r[0][:10] <= day or r[0][:13] <= v1_ref[:13]]
        r = (pick or sidecar)[-1]
        t['sidecar'] = {'dtm': r[2:4], 'dtm_multi': r[4:6], 'ortho': r[6:8], 'hit_pct': r[8], 'ts': r[0], 'peer': r[1]}
    if not log:
        return t
    # the v1 build ends with the last 'gpkg_full —' line at or before the v1 document time (+2 h slack);
    # everything later is the v2 upgrade / sweep / repair history
    lim = (v1_ref[:13] if v1_ref else '9999')
    full_i = None
    for i, l in enumerate(log):
        if 'gpkg_full \u2014' in l.get('msg', '') and (not v1_ref or l['ts'][:10] <= v1_ref[:10] or l['ts'][:13] <= lim):
            full_i = i
    v1log = log[:full_i + 1] if full_i is not None else ([] if up or not v1_ref else log)
    if full_i is not None:
        t['v1_peer'] = log[full_i].get('peer'); t['v1_stitch_ts'] = log[full_i]['ts'][:19]
    starts = [l for l in v1log if l.get('msg', '').startswith('Starting KG')]
    if starts:
        t['v1_sessions'] = len(starts); t['v1_peers'] = len({l.get('peer') for l in starts})
        t['v1_first_ts'] = starts[0]['ts'][:19]
    t['v1_resumes'] = sum(1 for l in v1log if 'checkpoints preserved' in l.get('msg', '') or 'aborting after tile' in l.get('msg', ''))
    tar = [l for l in v1log if l.get('msg', '').startswith('chkpt: uploaded')]
    if tar:
        t['chkpt_tar'] = len(tar)
        t['chkpt_tar_foreign'] = int(any(l.get('peer') != t.get('v1_peer') for l in tar))
    for l in log[(full_i or 0):]:
        m = _SIDECAR_RX.search(l.get('msg', ''))
        if m and 'sidecar' not in t:
            g = [int(x) for x in m.groups()]
            t['sidecar'] = {'dtm': g[0:2], 'dtm_multi': g[2:4], 'ortho': g[4:6], 'hit_pct': g[6]}
            break
    if full_i is not None and starts:
        # tiles the stitching peer computed itself in its last session (a sampled progress relay: a
        # tile seen here was fresh; one not seen was restored from a checkpoint OR just not sampled)
        peer = t['v1_peer']; s0 = max((i for i, l in enumerate(v1log) if l.get('peer') == peer and l['msg'].startswith('Starting KG')), default=None)
        if s0 is not None:
            seen = sorted({int(m.group(1)) for l in v1log[s0:] if l.get('peer') == peer for m in [_TILE_RX.match(l.get('msg', ''))] if m})
            t['final_tiles'] = seen
    for l in reversed(log):
        if 'upgraded to v2' in l.get('msg', ''):
            t['v2_peer'] = l.get('peer'); t['v2_ts'] = l['ts'][:19]
            break
    if t['origin'] == 'fresh' and full_i is None:
        for l in reversed(log):
            if l.get('level') == 'success':
                t['v2_peer'] = l.get('peer'); t['v2_ts'] = l['ts'][:19]
                break
    return t


def sidecar_class(t: dict) -> str:
    """all | part | none (the v1 stitch re-read every tile) | - (no sidecar line: pre-sidecar or not logged)"""
    s = (t or {}).get('sidecar')
    if not s:
        return '-'
    a, b = s['dtm']
    return 'all' if a == b else ('none' if a == 0 else 'part')


# ── build ───────────────────────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE row (code TEXT NOT NULL, tile INTEGER NOT NULL, fault TEXT NOT NULL, detector TEXT NOT NULL,
    verdict TEXT, status TEXT NOT NULL,      -- open | handled | clear (measured, no fault)
    action TEXT,                             -- what the read path does: moved | patched | registered | blended | hidden | added | requeued | none
    parent TEXT NOT NULL, kind TEXT NOT NULL,
    x0 INTEGER, y0 INTEGER, x1 INTEGER, y1 INTEGER,   -- EPSG:3035 tile envelope (NULL = whole product)
    ev TEXT,                                 -- evidence numbers of the detector (JSON)
    PRIMARY KEY (code, tile, fault)) WITHOUT ROWID;
CREATE INDEX row_env ON row (x0, y0, x1, y1);
CREATE INDEX row_parent ON row (parent);
CREATE TABLE tile (code TEXT NOT NULL, tile INTEGER NOT NULL, parent TEXT NOT NULL, x0 INTEGER, y0 INTEGER, x1 INTEGER, y1 INTEGER,
    v2_fallback INTEGER,                     -- 1 = the v2 upgrade re-read this tile from BEV (not from the v1 GPKG)
    final_seen INTEGER,                      -- 1 = computed by the stitching peer in its last session (sampled log)
    PRIMARY KEY (code, tile)) WITHOUT ROWID;
CREATE TABLE product (code TEXT PRIMARY KEY, parent TEXT NOT NULL, kind TEXT NOT NULL, n_tiles INTEGER,
    uploaded_at TEXT, version TEXT, peer TEXT,      -- served product: newest of _json_v2 / _light_gpkg_v2 / _json
    v1_uploaded_at TEXT, v1_version TEXT,           -- _full_gpkg = the rasters the faults were baked into
    keys TEXT,                                      -- {suffix: [uploaded_at, version, status]} manifest join
    timing TEXT,                                    -- timing() JSON
    n_open INTEGER, n_handled INTEGER, n_classes INTEGER, score REAL) WITHOUT ROWID;
CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID;
"""


def _manifest_index(entries: dict) -> dict:
    """{code: {suffix: [uploaded_at, version, status]}} — product keys only."""
    idx: dict = {}
    for k, e in entries.items():
        if not isinstance(e, dict):
            continue
        code, suf = split_key(k)
        if code is None:
            continue
        idx.setdefault(code, {})[suf] = [str(e.get('uploaded_at') or '')[:19], e.get('version') or '', e.get('status') or '']
    return idx


def collect(src: Sources) -> dict:
    """All ledger rows from the detector stores.  → {'rows': [...], 'tiles': {...}, 'inputs': {...}}"""
    rows: dict = {}
    tiles: dict = {}                    # (code, tile) → [parent, x0, y0, x1, y1, v2_fallback, final_seen]
    docs: dict = {}
    by_parent: dict = {}
    inputs: dict = {}

    self_border = src.border_m if hasattr(src, 'border_m') else border_m

    def doc(code):
        if code not in docs:        # keep only what the ledger reads — a product document is 0.2–2 MB
            d = src.doc(code) or {}
            dq = d.get('data_quality') or {}
            docs[code] = {'generated_at': d.get('generated_at'), 'upgraded_from': d.get('upgraded_from'),
                          'data_quality': {'v2_source': dq.get('v2_source'),
                                           'tiles': [{'tile_index': t.get('tile_index'), 'bbox_wgs': t.get('bbox_wgs')}
                                                     for t in dq.get('tiles') or [] if isinstance(t, dict)]}} if d else {}
        return docs[code]

    def product_tiles(code):
        """Register the tiles of one product; → [(tile_index, env)]"""
        d = doc(code); fb = _fallback_corners(d); out = []
        for ti, *env in doc_tiles(d):
            env = tuple(env)
            tiles.setdefault((code, ti), [parent_of(code), *env, int(_near(env, fb)) if (d.get('upgraded_from') or fb) else None, None])
            out.append((ti, env))
        return out

    def codes_of(kg):
        if kg not in by_parent:
            cs = [c for c in src.codes_for_parent(kg)]
            by_parent[kg] = [kg] * (kg in cs) + sorted(c for c in cs if c != kg)
        return by_parent[kg]

    def add(code, tile, fault, verdict, status, action, ev, env=None):
        rows[(code, int(tile), fault)] = {
            'code': code, 'tile': int(tile), 'fault': fault, 'detector': FAULTS[fault][0], 'verdict': verdict,
            'status': status, 'action': action, 'parent': parent_of(code), 'kind': kind_of(code),
            'env': tuple(env) if env else None, 'ev': ev}

    # 1 ── ne_pos_fix: KG-keyed; tiles are mapped onto the product (parent / block) that owns the envelope
    pos = src.pos_fix(); pk = pos.get('kgs') or {}
    inputs['ne_pos_fix'] = {'kgs': len(pk), 'updated_at': pos.get('updated_at'),
                            'per_tile_todo': sum(1 for v in pk.values() if v.get('verdict') in ('mixed', 'unclear') and v.get('tv') is None)}
    for kg, v in sorted(pk.items()):
        vd = v.get('verdict'); w = v.get('windows') or []
        ev = {'windows': len(w), 'shifted': sum(1 for x in w if x.get('v') == 'shift'), 'zero': sum(1 for x in w if x.get('v') == 'zero')}
        if v.get('off'):
            ev['moved_m'] = v['off']
        if v.get('was_verdict'):
            ev['was'] = v['was_verdict']
        if v.get('retired'):
            ev['retired'] = v['retired']
        cs = codes_of(kg)
        blocks = [c for c in cs if kind_of(c) == 'block']; sweeps = [c for c in cs if kind_of(c) == 'sweep']
        if blocks:
            ev['blocks'] = len(blocks)
        if sweeps:
            ev['sweep_units'] = len(sweeps)
        if vd == 'partial':
            tl = v.get('tiles') or []
            ev['tiles'] = {'moved': sum(1 for t in tl if t[4] > 0), 'kept': sum(1 for t in tl if t[4] == 0), 'unmeasured': sum(1 for t in tl if t[4] < 0)}
            add(kg, -1, 'pos_shift', 'partial', 'open' if ev['tiles']['unmeasured'] else 'handled', 'moved', ev)
            own = {}
            for c in cs:
                if kind_of(c) != 'sweep':
                    for ti, env in product_tiles(c):
                        own.setdefault(env, (c, ti))
            for i, t in enumerate(tl):
                env = tuple(int(round(q)) for q in t[:4]); fl = int(t[4])
                hit = own.get(env)
                if hit is None:             # product re-ingested since the measurement: nearest envelope within 3 m
                    hit = next((o for e2, o in own.items() if max(abs(a - b) for a, b in zip(e2, env)) <= 3), None)
                code, ti = hit if hit else (kg, 1000 + i)
                add(code, ti, 'pos_shift', {1: 'moved', 0: 'kept', -1: 'unmeasured'}[max(-1, min(1, fl))],
                    {1: 'handled', 0: 'clear', -1: 'open'}[max(-1, min(1, fl))], 'moved' if fl > 0 else 'none',
                    {'kg': kg, 'mapped': bool(hit)}, env)
        elif vd == 'shift':
            add(kg, -1, 'pos_shift', 'shift', 'handled', 'none' if v.get('retired') else 'moved', ev)
            for c in sweeps:                # a sweep unit is a fresh product on the true grid, but its cells are merged
                d = doc(c)                  # into the parent .h3b and move with the KG → 5 m off the other way
                ku = (v.get('keep_units') or {})
                if c in ku:                 # ne_pos_fix.sweep_keep: the cells the unit supplied are exempt from the move
                    add(c, -1, 'pos_sweep_unit', 'kept', 'handled', 'kept',
                        {'kg': kg, 'generated_at': str(d.get('generated_at') or '')[:19], 'cells_kept': ku[c]})
                else:
                    add(c, -1, 'pos_sweep_unit', 'suspect', 'open', 'moved',
                        {'kg': kg, 'generated_at': str(d.get('generated_at') or '')[:19], 'tiles': len(doc_tiles(d))})
        elif vd in ('mixed', 'unclear'):
            add(kg, -1, 'pos_shift', vd, 'open', 'none', ev)
        elif vd == 'zero':
            add(kg, -1, 'pos_shift', 'zero', 'clear', 'none', ev)
        if vd in ('shift', 'mixed', 'unclear', 'zero'):
            for c in cs:                    # tiles registered for the envelope join with grid25_patch
                if kind_of(c) != 'sweep':
                    product_tiles(c)

    # 2 ── rebin_added: shift file pairs (to-do list of ne_pos_fix), added trees, invariant
    rb = src.rebin()
    inputs['rebin_added'] = {'resolved': rb.get('resolved'), 'scanned': rb.get('scanned'), 'shift_pairs': len(rb.get('shifts') or []),
                             **({'error': rb['error']} if rb.get('error') else {})}
    pairs: dict = {}
    for s in rb.get('shifts') or []:
        kg = str(s[0]).zfill(5)
        pairs.setdefault(kg, []).append({'other': str(s[1]).zfill(5), 'dx': s[2], 'dy': s[3], 'matched': s[4], 'with_neighbour': s[5]})
    for kg, pl in sorted(pairs.items()):
        pv = (pk.get(kg) or {}).get('verdict'); ov = {p['other']: (pk.get(p['other']) or {}).get('verdict') for p in pl}
        add(kg, -1, 'apex_frame_pair', 'dup_shift', 'open', 'hidden-twin',
            {'pairs': pl, 'matched': sum(p['matched'] for p in pl), 'pos_fix': pv, 'pos_fix_other': ov})
    for r in rb.get('kgs') or []:
        kg = str(r['kg']).zfill(5)
        if r.get('err') or r.get('ok') == 0:
            add(kg, -1, 'apex_invariant', 'error' if r.get('err') else 'bad', 'open', 'none',
                {k: r.get(k) for k in ('stored', 'served', 'off', 'added', 'dup', 'qa', 'lost') if r.get(k) is not None} | ({'err': str(r['err'])[:80]} if r.get('err') else {}))
        if (r.get('added') or 0) > 0:
            add(kg, -1, 'apex_unserved', 'added', 'handled', 'added',
                {k: r.get(k) or 0 for k in ('stored', 'off', 'lost', 'added', 'no_row', 'other_file', 'lost_cell')})

    # 3 ── grid25_patch (product-keyed, tile index of the product) + registration
    g = src.grid25_index(); ch = g.get('checked') or {}; pa = g.get('patched') or {}
    inputs['grid25_patch'] = {'checked': len(ch), 'patched': len(pa), 'edges': len(g.get('edges') or {}), 'updated_at': g.get('updated_at')}
    for code, e in sorted(ch.items()):
        tl = dict(product_tiles(code))
        blocks = {tuple(b) for b in (pa.get(code) or {}).get('blocks') or []}
        rim = len((pa.get(code) or {}).get('rim_blocks') or [])
        for t in e.get('tiles') or []:
            v = t.get('v')
            if v not in ('bad', 'ok'):
                continue
            ti = int(t.get('tile') or 0); ev = {k: t[k] for k in ('n', 'med', 'p90', 'by', 'by_grow') if k in t}
            bm = self_border(tl.get(ti))
            if bm is not None:
                ev['border_m'] = bm
            if v == 'bad':
                patched = tuple(t.get('rc') or ()) in blocks
                if rim and patched:
                    ev['rim'] = True
                add(code, ti, 'grid25_stale', 'bad', 'handled' if patched else 'open', 'patched' if patched else 'none', ev, tl.get(ti))
            else:
                add(code, ti, 'grid25_stale', 'ok', 'clear', 'none', ev, tl.get(ti))
    for code, ed in sorted((g.get('edges') or {}).items()):
        tl = dict(product_tiles(code)); done = bool((ch.get(code) or {}).get('edges_done'))
        per: dict = {}
        for x in ed:
            per.setdefault(int(x.get('tile') or 0), []).append(x)
        for ti, xs in sorted(per.items()):
            stale = rows.get((code, ti, 'grid25_stale'), {}).get('verdict') == 'bad'
            add(code, ti, 'grid25_edge', 'step', 'handled' if done else 'open',
                ('patched' if stale else 'checked') if done else 'none',
                {'edges': len(xs), 'step_max': max(float(x.get('step') or 0) for x in xs), 'run_max': max(int(x.get('run') or 0) for x in xs),
                 'axes': sorted({x.get('axis') for x in xs})}, tl.get(ti))
    reg = src.registration()
    inputs['grid25_registration'] = {'built_at': reg.get('built_at'), 'shifted': len(reg.get('offsets') or {}), 'mixed': len(reg.get('mixed') or [])}
    for code, off in sorted((reg.get('offsets') or {}).items()):
        add(code, -1, 'grid25_shift', 'shift', 'handled', 'registered', {'offset_m': off})
        product_tiles(code)
    for code in sorted(reg.get('mixed') or []):
        n_p = sum(1 for (c, _t, f), r in rows.items() if c == code and f == 'grid25_stale' and r['action'] == 'patched')
        add(code, -1, 'grid25_mixed', 'mixed', 'open', 'blended', {'tiles_patched': n_p})
        product_tiles(code)

    # 4 ── label_mix (parent-keyed audit, block targets), product_repair (product-keyed), ne_linear (KG-keyed)
    lm = src.label_mix()
    inputs['label_mix'] = {'codes': len(lm)}
    for code, e in sorted(lm.items()):
        st = e.get('state') or 'detected'
        status = {'done': 'handled', 'fp': 'clear'}.get(st, 'open')
        ev = {k: e[k] for k in ('why', 'detected_at', 'requeued_at', 'attempts', 'note') if e.get(k)}
        for c in (e.get('blocks') or [code]):
            add(c, -1, 'label_mix', st, status, 'requeued' if e.get('requeued_at') else 'none', dict(ev, audited=code))
    rp = src.repair()
    inputs['product_repair'] = {'codes': len(rp)}
    for code, e in sorted(rp.items()):
        if not isinstance(e, dict):
            continue
        st = e.get('state') or 'pending'
        add(code, -1, 'layer_gap', st, 'handled' if st == 'done' else 'open', 're-upgrade' if st == 'done' else 'none',
            {k: e[k] for k in ('defects', 'tiles', 'missing_cells', 'attempts', 'detected_at', 'product_ts', 'version') if e.get(k) not in (None, '', [])})
    gens = src.generations()
    rp = src.reprocess()
    inputs['qc_generation'] = {'generations': len(gens), 'reprocess': dict(collections.Counter(e.get('state') for e in rp.values()))}
    for g in gens:
        q = g.get('quality') or {}
        bad_now = bool(q.get('restored') or q.get('edges') or q.get('missing') or 'partial' in str(q.get('label') or ''))
        add(g['code'], -1, 'regenerated', 'fresh' if g.get('fresh') else 'upgraded', 'handled' if not bad_now else 'open',
            'regenerated' if g.get('retired') else 'none',
            {'generated_at': str(g['generated_at'])[:19], 'prev_generated_at': str(g.get('prev_generated_at') or '')[:19],
             'was': [f"{b['fault']}={b['verdict']}" + (f"x{b['n']}" if b.get('n', 1) > 1 else '') for b in (g.get('before') or [])][:8],
             'retired': sorted(g.get('retired') or {}), 'now': q, 'reprocess': (rp.get(g['code']) or {}).get('why')})
    for code, e in rp.items():
        if e.get('state') in ('enrolled', 'requeued', 'stuck'):
            add(code, -1, 'regenerated', e['state'], 'open', 'none',
                {k: e[k] for k in ('why', 'enrolled_at', 'requeued_at', 'attempts', 'note') if e.get(k)})
    for code, e in ((src.grid25_index() or {}).get('retired') or {}).items():
        if e.get('bad'):
            add(code, -1, 'grid25_stale', 'regenerated', 'handled', 'regenerated',
                {'stale_tiles_before': e['bad'], 'tiles': e.get('tiles'), 'generated_at': str(e.get('generated_at') or '')[:19]})
    ln = src.linear()
    inputs['ne_linear'] = {'kgs': len(ln)}
    for r in ln:
        add(str(r['kg']).zfill(5), -1, 'linear_noise', 'runs', 'handled', 'hidden',
            {'runs': r['n_runs'], 'apices_hidden': r['n_flag'], 'apices': r['n_apex']})
    return {'rows': rows, 'tiles': tiles, 'docs': docs, 'doc': doc, 'inputs': inputs}


def _score(rs: list) -> float:
    """Ranking of a product for the worst list: open before handled, tile faults by count, more classes first."""
    op = sum(1 for r in rs if r['status'] == 'open'); ha = sum(1 for r in rs if r['status'] == 'handled')
    cl = len({r['fault'] for r in rs if r['status'] != 'clear'})
    return round(cl * 10 + op * 3 + ha * 0.5, 1)


def build(src: Sources | None = None, db_path: Path | None = None, summary_path: Path | None = None, force: bool = False,
          now: float | None = None) -> dict:
    """Rebuild the ledger from the detector stores.  No-op (returns the stored summary) when no input changed."""
    src = src or Sources(); db_path = Path(db_path or DB_PATH); summary_path = Path(summary_path or SUMMARY_PATH)
    sig = src.sig()
    if not force and db_path.exists():
        s = summary(summary_path)
        if s.get('sig') == sig and s.get('version') == VERSION:
            s['unchanged'] = True
            return s
    t0 = time.time()
    col = collect(src); rows = col['rows']; tiles = col['tiles']
    midx = _manifest_index(src.manifest()); origin = src.origin()
    arch = src.archive_sidecar() if hasattr(src, 'archive_sidecar') else {}
    by_code: dict = {}
    for r in rows.values():
        by_code.setdefault(r['code'], []).append(r)
    timed_parents = {r['parent'] for r in rows.values() if r['fault'] in TIMED and r['status'] != 'clear'}
    products = []
    for code in sorted(by_code):
        rs = by_code[code]; keys = midx.get(code) or {}
        served = next((keys[s] for s in ('json_v2', 'light_gpkg_v2', 'json') if s in keys and keys[s][2] != 'removed'), ['', '', ''])
        tm = {}
        if parent_of(code) in timed_parents and any(r['fault'] in TIMED for r in rs):
            d = col['doc'](code)
            tm = timing(code, d, src.log(code), keys, arch.get(code))
            seen = set(tm.get('final_tiles') or [])
            if tm.get('v1_stitch_ts'):
                for (c, ti), tv in tiles.items():
                    if c == code:
                        tv[6] = int(ti in seen)
        peer = None
        for s in ('json_v2', 'ne_h3b', 'light_gpkg_v2'):
            o = origin.get(f'{code}_{s}')
            if o and (not keys.get(s) or str(o.get('ts') or '')[:19] == keys[s][0]):
                peer = o.get('peer'); break
        peer = peer or tm.get('v2_peer') or (tm.get('v1_peer') if tm.get('origin') != 'upgraded' else None)
        live = [r for r in rs if r['status'] != 'clear']
        products.append((code, parent_of(code), kind_of(code), sum(1 for (c, _t) in tiles if c == code) or None,
                         served[0], served[1], peer, (keys.get('full_gpkg') or ['', ''])[0], (keys.get('full_gpkg') or ['', ''])[1],
                         json.dumps(keys, sort_keys=True), json.dumps(tm, sort_keys=True),
                         sum(1 for r in live if r['status'] == 'open'), sum(1 for r in live if r['status'] == 'handled'),
                         len({r['fault'] for r in live}), _score(rs)))
    tmp = db_path.with_suffix(f'.tmp{os.getpid()}')
    if tmp.exists():
        tmp.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(tmp)
    con.executescript(_SCHEMA)
    con.executemany('INSERT INTO row VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)', [
        (r['code'], r['tile'], r['fault'], r['detector'], r['verdict'], r['status'], r['action'], r['parent'], r['kind'],
         *(r['env'] or (None, None, None, None)), json.dumps(r['ev'], sort_keys=True)) for _k, r in sorted(rows.items())])
    con.executemany('INSERT INTO tile VALUES (?,?,?,?,?,?,?,?,?)', [(c, ti, *v) for (c, ti), v in sorted(tiles.items())])
    con.executemany('INSERT INTO product VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', products)
    con.commit()
    s = _summarise(con)
    built = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(now or time.time()))
    s.update(version=VERSION, sig=sig, built_at=built, secs=round(time.time() - t0, 1), inputs=col['inputs'])
    con.executemany('INSERT INTO meta VALUES (?,?)', [('version', str(VERSION)), ('sig', sig), ('built_at', built)])
    con.commit(); con.close()
    os.replace(tmp, db_path)
    t2 = summary_path.with_suffix(f'.tmp{os.getpid()}')
    t2.write_text(json.dumps(s, sort_keys=True))
    os.replace(t2, summary_path)
    _ST['mtime'] = None
    return s


# ── summary / correlation ───────────────────────────────────────────────────────────────────────────

def _summarise(con) -> dict:
    q = lambda sql, *a: con.execute(sql, a).fetchall()     # noqa: E731
    classes: dict = {}
    for f in FAULTS:
        r = q("SELECT COUNT(DISTINCT CASE WHEN status!='clear' THEN code END), COUNT(DISTINCT CASE WHEN status!='clear' THEN parent END), "
              "SUM(tile>=0 AND status!='clear'), SUM(status='open'), SUM(status='handled'), SUM(status='clear'), "
              "COUNT(DISTINCT CASE WHEN status='open' THEN code END) FROM row WHERE fault=?", f)[0]
        if not any(r):
            continue
        classes[f] = {'products': r[0] or 0, 'parents': r[1] or 0, 'tiles': r[2] or 0, 'open': r[3] or 0, 'handled': r[4] or 0,
                      'clear': r[5] or 0, 'open_products': r[6] or 0,
                      'verdicts': {v or '-': n for v, n in q('SELECT verdict, COUNT(*) FROM row WHERE fault=? GROUP BY 1', f)}}
    tot = q("SELECT COUNT(DISTINCT code), COUNT(DISTINCT parent), COUNT(DISTINCT CASE WHEN tile>=0 THEN code||':'||tile END), "
            "COUNT(DISTINCT CASE WHEN status='open' THEN code END), SUM(status='open'), SUM(status='handled') FROM row WHERE status!='clear'")[0]
    out = {'classes': classes,
           'totals': {'products': tot[0] or 0, 'parents': tot[1] or 0, 'tiles': tot[2] or 0, 'open_products': tot[3] or 0,
                      'open': tot[4] or 0, 'handled': tot[5] or 0}}

    # tile-level overlap, joined on the tile envelope: the tile's position verdict (its own tile row of a
    # partial KG, else the whole-KG verdict of its parent) × the grid25 BEV check of the same envelope
    pos_tile = {}
    for x0, y0, x1, y1, v in q("SELECT x0,y0,x1,y1,verdict FROM row WHERE fault='pos_shift' AND tile>=0 AND x0 IS NOT NULL"):
        pos_tile[(x0, y0, x1, y1)] = v
    whole = {p: v for p, v in q("SELECT parent, verdict FROM row WHERE fault='pos_shift' AND tile<0")}
    tinfo = {(x0, y0, x1, y1): (parent, fb, fs) for parent, x0, y0, x1, y1, fb, fs in q('SELECT parent,x0,y0,x1,y1,v2_fallback,final_seen FROM tile')}

    def pos_of(env):
        if env in pos_tile:
            return pos_tile[env]
        w = whole.get((tinfo.get(env) or ('',))[0])
        return {'shift': 'moved', 'zero': 'kept'}.get(w, w if w in ('mixed', 'unclear') else None)
    xt: dict = {}
    for x0, y0, x1, y1, v in q("SELECT x0,y0,x1,y1,verdict FROM row WHERE fault='grid25_stale' AND x0 IS NOT NULL"):
        p = pos_of((x0, y0, x1, y1)) or 'no_verdict'
        xt.setdefault(p, {'bad': 0, 'ok': 0})[v] += 1
    out['overlap_tiles'] = {'pos_x_grid25': xt}
    fx: dict = {}; sx: dict = {}
    for env, v in pos_tile.items():
        _p, fb, fs = tinfo.get(env) or (None, None, None)
        if v in ('moved', 'kept'):
            fx.setdefault(v, {'bev_reread': 0, 'from_gpkg': 0, 'unknown': 0})[{1: 'bev_reread', 0: 'from_gpkg'}.get(fb, 'unknown')] += 1
            sx.setdefault(v, {'seen': 0, 'not_seen': 0, 'unknown': 0})[{1: 'seen', 0: 'not_seen'}.get(fs, 'unknown')] += 1
    out['overlap_tiles']['pos_x_v2_fallback'] = fx
    out['overlap_tiles']['pos_x_final_session'] = sx
    gx: dict = {}
    for x0, y0, x1, y1, v in q("SELECT x0,y0,x1,y1,verdict FROM row WHERE fault='grid25_stale' AND x0 IS NOT NULL"):
        fb = (tinfo.get((x0, y0, x1, y1)) or (None, None, None))[1]
        gx.setdefault(v, {'bev_reread': 0, 'from_gpkg': 0, 'unknown': 0})[{1: 'bev_reread', 0: 'from_gpkg'}.get(fb, 'unknown')] += 1
    out['overlap_tiles']['grid25_x_v2_fallback'] = gx
    bx: dict = {}
    for v, ev in q("SELECT verdict, ev FROM row WHERE fault='grid25_stale'"):
        e = json.loads(ev or '{}')
        d = bx.setdefault(border_band(e.get('border_m')), {'bad': 0, 'ok': 0, 'bad_ge8m': 0})
        d[v] += 1
        if v == 'bad' and (e.get('med') or 0) >= 8:
            d['bad_ge8m'] += 1
    out['overlap_tiles']['grid25_x_border'] = bx
    n_edge_stale = q("SELECT COUNT(*) FROM row a JOIN row b ON a.code=b.code AND a.tile=b.tile WHERE a.fault='grid25_edge' AND b.fault='grid25_stale' AND b.verdict='bad'")[0][0]
    out['overlap_tiles']['edge_on_stale'] = [n_edge_stale, q("SELECT COUNT(*) FROM row WHERE fault='grid25_edge'")[0][0]]

    # parent-level co-occurrence of fault classes
    sets: dict = {}
    for f, p, v in q("SELECT fault, parent, verdict FROM row WHERE status!='clear'"):
        sets.setdefault(f, set()).add(p)
        if f == 'pos_shift' and v in ('shift', 'partial', 'mixed', 'unclear'):
            sets.setdefault('pos_shift:' + v, set()).add(p)
    pairs = {}
    names = sorted(sets)
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            if a.split(':')[0] == b.split(':')[0]:
                continue
            n = len(sets[a] & sets[b])
            if n:
                pairs[f'{a}&{b}'] = [n, len(sets[a]), len(sets[b])]
    out['overlap_parents'] = pairs

    # timing: position verdict / stale terrain × era of the v1 rasters × sidecar hits of the v1 stitch
    tim = {p: json.loads(t or '{}') for p, t in q("SELECT code, timing FROM product")}
    tx: dict = {}
    for code, v in q("SELECT code, verdict FROM row WHERE fault='pos_shift' AND tile<0"):
        ts = [tim[c] for c in tim if parent_of(c) == code and tim[c].get('era')] if code not in tim or not tim[code].get('era') else [tim[code]]
        for t in ts[:1] or [{}]:
            k = f"{t.get('era', 'unknown')}/sidecar={sidecar_class(t)}"
            tx.setdefault(v, {}); tx[v][k] = tx[v].get(k, 0) + 1
    out['timing'] = {'pos_shift': tx}
    gm: dict = {}; gp: dict = {}
    for code, n in q("SELECT code, COUNT(*) FROM row WHERE fault='grid25_stale' AND verdict='bad' GROUP BY 1"):
        t = tim.get(code) or {}
        m = (t.get('v1_generated_at') or t.get('v1_stitch_ts') or 'unknown')[:7]
        gm[m] = gm.get(m, 0) + n
        if t.get('v1_peer'):
            gp[t['v1_peer']] = gp.get(t['v1_peer'], 0) + n
    out['timing']['grid25_stale_by_v1_month'] = gm
    out['timing']['grid25_stale_top_peers'] = dict(sorted(gp.items(), key=lambda kv: -kv[1])[:8])
    return out


_ST = {'mtime': None, 'doc': {}}
_LOCK = threading.Lock()


def summary(path: Path | None = None) -> dict:
    """The stored summary (tiny JSON, mtime-cached) — the only thing the /process.txt line reads."""
    path = Path(path or SUMMARY_PATH)
    if path != SUMMARY_PATH:
        try:
            return json.loads(path.read_text())
        except Exception:  # noqa: BLE001
            return {}
    with _LOCK:
        try:
            mt = path.stat().st_mtime_ns
        except OSError:
            return {}
        if _ST['mtime'] != mt:
            try:
                _ST['doc'] = json.loads(path.read_text()); _ST['mtime'] = mt
            except Exception:  # noqa: BLE001
                return {}
        return dict(_ST['doc'])


def _ro(db_path=None):
    p = Path(db_path or DB_PATH)
    if not p.exists():
        return None
    c = sqlite3.connect(f'file:{p}?mode=ro', uri=True, timeout=5)
    c.row_factory = sqlite3.Row
    return c


_SHORT = {'pos_shift': 'pos', 'pos_sweep_unit': 'pos_sweep', 'apex_frame_pair': 'apex_pair', 'apex_unserved': 'apex_added',
          'apex_invariant': 'apex_inv', 'grid25_stale': 'g25_stale', 'grid25_edge': 'g25_edge', 'grid25_shift': 'g25_shift',
          'grid25_mixed': 'g25_mixed', 'label_mix': 'label_mix', 'layer_gap': 'layer_gap', 'linear_noise': 'linear',
          'regenerated': 'regen'}


def text_line(s: dict | None = None, now: float | None = None) -> str:
    """The `qc:` line.  Per class `name products[/tiles]t (open N)`; `∩` = tile-envelope join."""
    s = s if s is not None else summary()
    if not s.get('classes'):
        return 'qc: no ledger yet (python3 qc_ledger.py --build)'
    t = s['totals']; parts = []
    for f in FAULTS:
        c = s['classes'].get(f)
        if not c:
            continue
        p = f"{_SHORT[f]} {c['products']}p" + (f"/{c['tiles']}t" if c['tiles'] else '')
        if f == 'pos_shift':
            v = c['verdicts']
            p += f" (shift {v.get('shift', 0)} partial {v.get('partial', 0)} mixed {v.get('mixed', 0)} unclear {v.get('unclear', 0)}" \
                 f" · tiles moved {v.get('moved', 0)} kept {v.get('kept', 0)} unmeasured {v.get('unmeasured', 0)})"
        p += f" open {c['open']}" if c['open'] else ' handled'
        parts.append(p)
    xt = (s.get('overlap_tiles') or {}).get('pos_x_grid25') or {}

    def cell(k):
        d = xt.get(k) or {}
        return f"{d.get('bad', 0)}/{d.get('bad', 0) + d.get('ok', 0)}"
    gs = s['classes'].get('grid25_stale') or {}
    ov = (f"overlap(tile envelope, a cross-tab — terrain of {gs.get('handled', 0)} of {gs.get('handled', 0) + gs.get('open', 0)} stale tiles is patched): "
          f"g25_stale / BEV-checked tiles by position verdict — pos moved {cell('moved')} kept {cell('kept')} "
          f"mixed/unclear {(xt.get('mixed') or {}).get('bad', 0) + (xt.get('unclear') or {}).get('bad', 0)}/"
          f"{sum((xt.get(k) or {}).get(b, 0) for k in ('mixed', 'unclear') for b in ('bad', 'ok'))} no pos verdict {cell('no_verdict')}")
    bx = (s.get('overlap_tiles') or {}).get('grid25_x_border') or {}
    if bx:
        ov += ' · g25_stale by distance to the border: ' + ' '.join(
            f"{k} {bx[k]['bad']}/{bx[k]['bad'] + bx[k]['ok']}" for k in ('crosses', '<1.5km', '<5km', '>=5km', 'unknown') if k in bx)
    fx = (s.get('overlap_tiles') or {}).get('pos_x_v2_fallback') or {}
    if fx:
        ov += ' · v2 BEV re-read: ' + ' '.join(f"{k} {d.get('bev_reread', 0)}/{d.get('bev_reread', 0) + d.get('from_gpkg', 0)}" for k, d in sorted(fx.items()))
    pend = []
    inp = s.get('inputs') or {}
    if (inp.get('ne_pos_fix') or {}).get('per_tile_todo'):
        pend.append(f"ne_pos_fix per-tile todo {inp['ne_pos_fix']['per_tile_todo']}")
    if inp.get('rebin_added') and not inp['rebin_added'].get('resolved'):
        pend.append('rebin resolve pending')
    age = ''
    try:
        import calendar
        a = (now or time.time()) - calendar.timegm(time.strptime(s['built_at'], '%Y-%m-%dT%H:%M:%SZ'))
        age = f" ({a / 3600:.1f}h ago)" if a < 6 * 3600 else f" ({a / 3600:.0f}h ago — STALE, ne-linear.service runs the build)"
    except Exception:  # noqa: BLE001
        pass
    return (f"qc: {t['products']} products / {t['tiles']} tiles in {t['parents']} KGs with ledger rows — open {t['open']} rows in {t['open_products']} products, "
            f"handled {t['handled']} · " + ' · '.join(parts) + ' · ' + ov
            + (' · inputs incomplete: ' + ', '.join(pend) if pend else '')
            + f" · built {s.get('built_at', '-')}{age} · ?qc=N worst products, docs/qc-lessons.md")


def worst(n: int = 20, db_path=None) -> list:
    """The n worst products: score, classes, WHEN, manifest keys."""
    con = _ro(db_path)
    if con is None:
        return []
    out = []
    for p in con.execute('SELECT * FROM product WHERE n_classes > 0 ORDER BY score DESC, code LIMIT ?', (int(n),)):
        rs = con.execute("SELECT fault, tile, verdict, status, action FROM row WHERE code=? AND status!='clear' ORDER BY fault, tile", (p['code'],)).fetchall()
        cls: dict = {}
        for r in rs:
            c = cls.setdefault(r['fault'], {'n': 0, 'open': 0, 'verdicts': set(), 'tiles': 0})
            c['n'] += 1; c['open'] += r['status'] == 'open'; c['verdicts'].add(r['verdict']); c['tiles'] += r['tile'] >= 0
        out.append({'code': p['code'], 'kind': p['kind'], 'score': p['score'], 'open': p['n_open'], 'handled': p['n_handled'],
                    'classes': {f: {'rows': c['n'], 'open': c['open'], 'tiles': c['tiles'], 'verdicts': sorted(v for v in c['verdicts'] if v)} for f, c in cls.items()},
                    'uploaded_at': p['uploaded_at'], 'version': p['version'], 'peer': p['peer'],
                    'v1_uploaded_at': p['v1_uploaded_at'], 'timing': json.loads(p['timing'] or '{}'),
                    'keys': sorted(f"{p['code']}_{s}" for s, v in json.loads(p['keys'] or '{}').items() if v[2] != 'removed')})
    con.close()
    return out


def detail_lines(limit: int = 20, db_path=None) -> list:
    """/process.txt?qc=N rows."""
    out = [f'qc worst {limit} (score = 10/class + 3/open row + 0.5/handled row; t = tile rows; WHEN = served product upload / v1 raster build):']
    for w in worst(limit, db_path):
        tm = w['timing']; sc = tm.get('sidecar')
        cl = ' '.join(f"{_SHORT[f]}[{'+'.join(c['verdicts'])}" + (f" {c['tiles']}t" if c['tiles'] else '') + (f" open {c['open']}" if c['open'] else '') + ']'
                      for f, c in w['classes'].items())
        when = f"up {w['uploaded_at'] or '-'} {w['version'] or '-'}" + (f" by {w['peer']}" if w['peer'] else '')
        v1 = tm.get('v1_generated_at') or tm.get('v1_stitch_ts') or w['v1_uploaded_at']
        if v1:
            when += f" · v1 {v1[:16]}" + (f" by {tm['v1_peer']}" if tm.get('v1_peer') else '') + (f" {tm['era']}" if tm.get('era') else '')
        if tm.get('v1_sessions'):
            when += f" sessions {tm['v1_sessions']}/{tm.get('v1_peers', '?')} peers"
        if sc:
            when += f" sidecar dtm {sc['dtm'][0]}/{sc['dtm'][1]} ({sc['dtm'][1] - sc['dtm'][0]} re-read)"
        if tm.get('chkpt_tar'):
            when += ' chkpt-tar' + ('(foreign)' if tm.get('chkpt_tar_foreign') else '')
        if tm.get('v2_dtm'):
            when += f" v2 dtm gpkg/bev {tm['v2_dtm'][0]}/{tm['v2_dtm'][1]}"
        out.append(f"  {w['code']:<18} {w['score']:>6} {cl} · {when} · keys {' '.join(k[len(w['code']) + 1:] for k in w['keys']) or '-'}")
    if len(out) == 1:
        out.append('  (no ledger — python3 qc_ledger.py --build)')
    return out


def for_key(key: str, db_path=None) -> dict:
    """QC state behind a manifest key (or a product code) — THE manifest link: a derived read-only
    join, nothing is written into zenodo_manifest.json.  Rows of the product itself plus the
    whole-KG rows of its parent (KG-keyed detectors) with ``scope: 'parent'``."""
    code, suf = split_key(key)
    if code is None:
        code, suf = key, None
    con = _ro(db_path)
    if con is None:
        return {'key': key, 'code': code, 'ledger': False}
    par = parent_of(code)

    def rows(c, scope):
        return [{'fault': r['fault'], 'tile': r['tile'] if r['tile'] >= 0 else None, 'detector': r['detector'], 'verdict': r['verdict'],
                 'status': r['status'], 'action': r['action'], 'scope': scope,
                 'envelope_3035': [r['x0'], r['y0'], r['x1'], r['y1']] if r['x0'] is not None else None, 'evidence': json.loads(r['ev'] or '{}')}
                for r in con.execute('SELECT * FROM row WHERE code=? ORDER BY fault, tile', (c,))]
    out = {'key': key, 'code': code, 'suffix': suf, 'ledger': True, 'rows': rows(code, 'product')}
    if par != code:
        out['rows'] += [r for r in rows(par, 'parent') if r['tile'] is None]
    p = con.execute('SELECT * FROM product WHERE code=?', (code,)).fetchone()
    if p:
        out['product'] = {'kind': p['kind'], 'uploaded_at': p['uploaded_at'], 'version': p['version'], 'peer': p['peer'],
                          'v1_uploaded_at': p['v1_uploaded_at'], 'keys': json.loads(p['keys'] or '{}'), 'timing': json.loads(p['timing'] or '{}')}
    live = [r for r in out['rows'] if r['status'] != 'clear']
    out['state'] = 'open' if any(r['status'] == 'open' for r in live) else ('handled' if live else 'clean')
    b = con.execute("SELECT v FROM meta WHERE k='built_at'").fetchone()
    out['built_at'] = b[0] if b else None
    con.close()
    return out


def key_lines(key: str, db_path=None) -> list:
    """/process.txt?qc=<manifest key | product code> rows."""
    k = for_key(key, db_path)
    if not k.get('ledger'):
        return [f'qc key {key}: no ledger']
    p = k.get('product') or {}
    out = [f"qc key {key}: product {k['code']} ({p.get('kind', kind_of(k['code']))}) state {k['state']} · "
           f"up {p.get('uploaded_at') or '-'} {p.get('version') or '-'}" + (f" by {p['peer']}" if p.get('peer') else '')
           + ' · keys ' + (' '.join(f"{s}@{v[0][:10]}{'(' + v[2] + ')' if v[2] else ''}" for s, v in sorted((p.get('keys') or {}).items())) or '-')]
    if p.get('timing'):
        out.append('  timing ' + json.dumps(p['timing'], sort_keys=True, separators=(',', ':'))[:700])
    for r in k['rows'][:120]:
        out.append(f"  {r['fault']:<16} {('tile ' + str(r['tile'])) if r['tile'] is not None else 'product':<9} {r['scope']:<7} {r['detector']:<19} "
                   f"{r['verdict'] or '-':<10} {r['status']:<7} {r['action'] or '-':<11} "
                   + (f"env {r['envelope_3035']} " if r['envelope_3035'] else '')
                   + json.dumps(r['evidence'], sort_keys=True, separators=(',', ':'))[:260])
    if not k['rows']:
        out.append('  (no ledger rows — no detector has a finding for this product)')
    return out


def tile_join(db_path=None, limit: int = 50) -> list:
    """Tiles (by envelope) that carry rows of more than one fault class — 'is the tile with the 5 m
    tree shift also a stale grid25 tile?'"""
    con = _ro(db_path)
    if con is None:
        return []
    out = [dict(r) for r in con.execute(
        "SELECT x0, y0, x1, y1, GROUP_CONCAT(code || '#' || tile || ':' || fault || '=' || verdict, ' ') AS faults, COUNT(DISTINCT fault) AS n "
        "FROM row WHERE x0 IS NOT NULL AND status != 'clear' GROUP BY x0, y0, x1, y1 HAVING n > 1 ORDER BY n DESC, x0, y0 LIMIT ?", (limit,))]
    con.close()
    return out


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--build', action='store_true'); ap.add_argument('--force', action='store_true')
    ap.add_argument('--status', action='store_true'); ap.add_argument('--worst', type=int)
    ap.add_argument('--key'); ap.add_argument('--report', action='store_true'); ap.add_argument('--tiles', type=int)
    a = ap.parse_args()
    if a.build:
        r = build(force=a.force)
        print(json.dumps({k: r.get(k) for k in ('built_at', 'secs', 'unchanged', 'totals')}))
    if a.worst:
        print('\n'.join(detail_lines(a.worst)))
    if a.key:
        print(json.dumps(for_key(a.key), indent=1))
    if a.tiles:
        for r in tile_join(limit=a.tiles):
            print(r)
    if a.report:
        s = summary()
        print(json.dumps({k: s.get(k) for k in ('totals', 'classes', 'overlap_tiles', 'overlap_parents', 'timing', 'inputs')}, indent=1))
    if a.status or not (a.build or a.worst or a.key or a.report or a.tiles):
        print(text_line())
