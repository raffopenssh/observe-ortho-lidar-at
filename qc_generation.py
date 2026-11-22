"""Product generations — ONE hook for every (re)generated product, and the operator's reprocess list.

A *generation* of a product code is one ``generated_at`` of its ``_json_v2`` document.  Every
read-time fix of the 2026 run (docs/qc-lessons.md) was measured on ONE generation's rasters:

* ``grid25_registration`` — the 5 m terrain offset of a product upgraded from a shifted v1 GPKG,
* ``grid25_patch``        — tiles rebuilt from BEV + per-tile verdicts + edge findings,
* ``ne_pos_fix``          — tree / structure positions moved per KG or per processing tile.

When a product is regenerated FRESH (a full run, no ``upgraded_from``: a QC reprocess, a label-mix
rerun, and — the reason this is generic — every product of a future v2.5 roll-out) those fixes
describe rasters that no longer exist.  :func:`on_ingest` (called by ``v2_ingest`` right after the
store write) then, once per (code, generated_at):

1. snapshots the faults the QC ledger held against the old generation (``before``),
2. retires the fixes of that code (``grid25_registration.retire`` / ``grid25_patch.retire`` /
   ``ne_pos_fix.regenerated`` — frame history stays, see docs/tree-user.md → Position frames),
3. documents the NEW quality from the document alone (tiles, restored-from-checkpoint tiles,
   internal tile-edge cliffs, ``-partial`` label, model / product version) — no BEV, no network,
4. appends the row to ``data/qc_generations.sqlite`` (the ledger's ``generation`` source), and
5. closes the code on the reprocess list.

An upgraded generation (``upgraded_from`` set: rasters inherited) is documented too but retires
nothing.

Reprocess list (``data/austria_processor/qc_reprocess.json``): NOTHING is enrolled automatically.
The operator enrols codes (``--enroll-fault grid25_stale`` / ``--enroll CODE…``); the director's
audit tick (:func:`sweep`, next to ``label_mix.sweep``) then requeues them a few at a time with
``keep_products`` + ``fresh_run`` (``POST /processing/queue``): NOTHING is deleted — not on Zenodo
(product files are overwritten in place by ``zenodo_client._replace_in_bucket``, the old bytes are
served until the new PUT commits) and not in the manifest (every entry incl. ``_full_gpkg`` stays
referenced).  The ``<code>_fresh`` marker tombstone only tells the processor not to REUSE the full
GPKG on Zenodo (``austria_processor`` manifest-aware reuse) but to rebuild it from BEV.  The store
document, grid25 and the merged cells file are replaced by the ingest of the new generation.
A future v2.5 that must keep v2.4 next to it needs new manifest keys / filenames, not this path —
this one regenerates a product *of the same version* in place.

    python3 qc_generation.py --status
    python3 qc_generation.py --enroll-fault grid25_stale [--dry]
    python3 qc_generation.py --enroll 92116 84114 --why 'operator'
    python3 qc_generation.py --cancel 92116
    python3 qc_generation.py --bootstrap          # once: one row per stored product, no actions
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parent
DB = Path(os.environ.get('QC_GENERATIONS', str(ROOT / 'data' / 'qc_generations.sqlite')))
STATE = Path(os.environ.get('QC_REPROCESS', str(ROOT / 'data' / 'austria_processor' / 'qc_reprocess.json')))
LEDGER = ROOT / 'data' / 'qc_ledger.sqlite'
DATA_DIR = ROOT / 'data' / 'austria_processor'
_LOCK = threading.Lock()

MAX_REQUEUE_PER_TICK = 2        # the audit tick runs every ~10 min
MAX_OUTSTANDING = 6             # products in flight (each re-reads BEV on a peer and re-uploads 0.5-3 GB)
MAX_ATTEMPTS = 2
LOST_AFTER_S = 8 * 3600        # a block run takes 1-3 h once dispatched
STALL_AFTER_S = 36 * 3600


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def _ts(s) -> float:
    try:
        return datetime.fromisoformat(str(s).replace('Z', '+00:00')).timestamp()
    except Exception:  # noqa: BLE001
        return 0.0


# === SECTION: generation store ===

def _conn() -> sqlite3.Connection:
    DB.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DB), timeout=20, isolation_level=None)
    c.execute('PRAGMA journal_mode=WAL')
    c.execute('CREATE TABLE IF NOT EXISTS gen (code TEXT NOT NULL, generated_at TEXT NOT NULL, at TEXT, parent TEXT, '
              'product_version TEXT, model TEXT, fresh INTEGER, prev_generated_at TEXT, uploaded_at TEXT, '
              'before TEXT, retired TEXT, quality TEXT, PRIMARY KEY (code, generated_at))')
    c.execute('CREATE INDEX IF NOT EXISTS gen_at ON gen(at)')
    c.execute('CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)')
    return c


def quality(code: str, doc: dict, version_label: str = '') -> dict:
    """What the document itself says about this generation (cheap, local)."""
    dq = doc.get('data_quality') or {}
    tiles = dq.get('tiles') or []
    src = dq.get('v2_source') or {}
    q = {'tiles': len(tiles), 'restored': sum(1 for t in tiles if t.get('restored')),
         'fresh': not doc.get('upgraded_from'), 'label': version_label or ''}
    if doc.get('upgraded_from'):
        q['v1_generated_at'] = str((doc.get('upgraded_from') or {}).get('v1_generated_at') or '')[:19]
        q['bev_reread_tiles'] = len(src.get('fallback_tiles') or [])
    miss = [k for k in ('ndvi', 'sar', 'ndvi_harmonics') if not doc.get(k)]
    if miss:
        q['missing'] = miss
    try:        # internal tile-edge cliffs of the 25 m terrain (the rule of v2_verify.check_grid25_tile_edges)
        import grid25_patch as gp
        import kg_v2_store as kvs
        g = kvs.get_grid25(code)
        if g and g.get('elev') is not None:
            q['edges'] = len(gp.edges(code, {'elev': g['elev'], 'x0': g['x0'], 'y0': g['y0']}, tiles=gp.tiles_of_doc(doc, g)) or [])
    except Exception as e:  # noqa: BLE001
        q['edges_err'] = repr(e)[:80]
    return q


def _ledger_before(code: str) -> list:
    """Fault rows the ledger holds against the code (and its parent-level rows) right now."""
    if not LEDGER.exists():
        return []
    try:
        c = sqlite3.connect(f'file:{LEDGER}?mode=ro', uri=True, timeout=5)
        try:
            kg = code.split('-')[0]
            rows = c.execute('SELECT code, fault, verdict, status, COUNT(*) FROM row WHERE code IN (?, ?) '
                             'GROUP BY code, fault, verdict, status ORDER BY 1, 2', (code, kg)).fetchall()
        finally:
            c.close()
        return [{'code': r[0], 'fault': r[1], 'verdict': r[2], 'state': r[3], 'n': r[4]} for r in rows]
    except Exception:  # noqa: BLE001
        return []


def retire_fixes(code: str, generated_at: str = '') -> dict:
    """Drop the read-time fixes measured on the previous generation of *code* (fresh generation only)."""
    out = {}
    try:
        import grid25_registration as gr
        r = gr.retire(code, generated_at)
        if r is not None:
            out['grid25_registration'] = r
    except Exception as e:  # noqa: BLE001
        out['grid25_registration_err'] = repr(e)[:120]
    try:
        import grid25_patch as gp
        r = gp.retire(code, generated_at)
        if r is not None:
            out['grid25_patch'] = r
    except Exception as e:  # noqa: BLE001
        out['grid25_patch_err'] = repr(e)[:120]
    try:
        import ne_pos_fix as pf
        r = pf.regenerated(code)
        if r.get('action'):
            out['ne_pos_fix'] = r['action']
            try:
                kg = code.split('-')[0].zfill(5)
                pf._flag(kg, json.loads(pf.PATH.read_text())['kgs'][kg])
            except Exception:  # noqa: BLE001
                pass
    except Exception as e:  # noqa: BLE001
        out['ne_pos_fix_err'] = repr(e)[:120]
    return out


def on_ingest(code: str, doc: dict, uploaded_at: str = '', version_label: str = '', act: bool = True) -> dict | None:
    """Hook of ``v2_ingest`` (after the store write).  Returns the generation row when this
    (code, generated_at) is new, else None.  Never raises."""
    try:
        import kg_splitter
        code = str(code)
        gen = str(doc.get('generated_at') or uploaded_at or '')
        if not gen:
            return None
        with _LOCK:
            c = _conn()
            try:
                if c.execute('SELECT 1 FROM gen WHERE code=? AND generated_at=?', (code, gen)).fetchone():
                    return None
                prev = c.execute('SELECT generated_at FROM gen WHERE code=? ORDER BY at DESC, rowid DESC LIMIT 1', (code,)).fetchone()
                boot = (c.execute("SELECT v FROM meta WHERE k='bootstrap_at'").fetchone() or [''])[0]
                fresh = not doc.get('upgraded_from')
                sweep_unit = kg_splitter.is_sweep_code(code)
                # a generation is NEW (vs. first sight of an old product) when we saw another one
                # before, or it was generated after the store was bootstrapped
                new = bool(prev) or (bool(boot) and _ts(gen) > _ts(boot))
                before = _ledger_before(code) if (act and new) else []
                retired = retire_fixes(code, gen) if (act and new and fresh and not sweep_unit) else {}
                q = quality(code, doc, version_label)
                row = {'code': code, 'generated_at': gen, 'at': _now(), 'parent': code.split('-')[0],
                       'product_version': str(doc.get('product_version') or doc.get('version') or ''),
                       'model': str(doc.get('model_version') or (doc.get('classification') or {}).get('model_version') or ''),
                       'fresh': int(fresh), 'prev_generated_at': prev[0] if prev else None, 'uploaded_at': uploaded_at or '',
                       'before': before, 'retired': retired, 'quality': q}
                c.execute('INSERT OR IGNORE INTO gen VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                          (row['code'], gen, row['at'], row['parent'], row['product_version'], row['model'], row['fresh'],
                           row['prev_generated_at'], row['uploaded_at'], json.dumps(before), json.dumps(retired), json.dumps(q)))
            finally:
                c.close()
        if act and new:
            _close_reprocess(code, row)
            if retired or before:
                _event(f"qc_generation: {code} new {'fresh' if fresh else 'upgraded'} generation {gen[:16]} — "
                       f"was {_brief_before(before)}; retired {', '.join(sorted(retired)) or 'nothing'}; "
                       f"now tiles={q.get('tiles')} restored={q.get('restored')} edges={q.get('edges')}"
                       + (f" label={q['label']}" if q.get('label') else ''), code)
        return row
    except Exception as e:  # noqa: BLE001 — documentation must never break an ingest
        log.warning('qc_generation.on_ingest %s: %s', code, e)
        return None


def _brief_before(before: list) -> str:
    return ', '.join(f"{b['fault']}={b['verdict']}" + (f"×{b['n']}" if b['n'] > 1 else '') for b in before[:6]) or 'clean'


def _event(msg: str, code: str = '', level: str = 'info') -> None:
    try:
        _app = sys.modules.get('app')       # only inside the web / director process — never import it from a CLI
        if _app is None:
            raise RuntimeError('no app')
        _app.director_event(msg, peer='director', kg=code, level=level)
    except Exception:  # noqa: BLE001
        log.info(msg)


def bootstrap(progress=None) -> dict:
    """One row per product already in the store (no actions).  From then on a generation is new
    when it differs from the row here or postdates ``bootstrap_at``."""
    import kg_v2_store as kvs
    n = 0
    c = _conn()
    try:
        have = {r[0] for r in c.execute('SELECT DISTINCT code FROM gen')}
        src = kvs._conn()
        rows = src.execute('SELECT code, parent, generated_at, uploaded_at, ingested_at, product_version, model FROM kg_v2').fetchall()
        c.execute('BEGIN')
        for code, parent, gen, up, ing, pv, model in rows:
            if code in have or not (gen or up):
                continue
            c.execute('INSERT OR IGNORE INTO gen VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                      (code, gen or up, ing or up or '', parent or code.split('-')[0], pv or '', model or '', None, None, up or '',
                       '[]', '{}', json.dumps({'bootstrap': True})))
            n += 1
        c.execute('COMMIT')
        if not c.execute("SELECT 1 FROM meta WHERE k='bootstrap_at'").fetchone():
            c.execute("INSERT INTO meta VALUES ('bootstrap_at', ?)", (_now(),))
    finally:
        c.close()
    return {'rows_added': n, 'codes': len(rows)}


def generations(since_s: float | None = None, acted_only: bool = False) -> list:
    """Rows (newest first) — the QC ledger's ``generation`` source."""
    if not DB.exists():
        return []
    c = _conn()
    try:
        q = 'SELECT code, generated_at, at, parent, product_version, model, fresh, prev_generated_at, uploaded_at, before, retired, quality FROM gen'
        w = []; a = []
        if since_s:
            w.append('at >= ?'); a.append(datetime.fromtimestamp(time.time() - since_s, timezone.utc).isoformat(timespec='seconds'))
        if acted_only:
            w.append("prev_generated_at IS NOT NULL")
        rows = c.execute(q + (' WHERE ' + ' AND '.join(w) if w else '') + ' ORDER BY at DESC, rowid DESC', a).fetchall()
    finally:
        c.close()
    ks = ('code', 'generated_at', 'at', 'parent', 'product_version', 'model', 'fresh', 'prev_generated_at', 'uploaded_at')
    return [dict(zip(ks, r[:9]), before=json.loads(r[9] or '[]'), retired=json.loads(r[10] or '{}'), quality=json.loads(r[11] or '{}'))
            for r in rows]


# === SECTION: reprocess list (operator-enrolled, director-paced) ===

def _load() -> dict:
    try:
        d = json.loads(STATE.read_text())
        if isinstance(d, dict):
            d.setdefault('codes', {})
            return d
    except Exception:  # noqa: BLE001
        pass
    return {'codes': {}}


def _save(d: dict) -> None:
    STATE.parent.mkdir(parents=True, exist_ok=True)
    d['t'] = _now()
    tmp = STATE.with_suffix('.tmp')
    tmp.write_text(json.dumps(d, indent=1))
    os.replace(tmp, STATE)


def ledger_codes(fault: str) -> list:
    """Product codes with a not-ok row of *fault* in the QC ledger (sweep units excluded: they are
    not queueable products — the sliver sweep owns them)."""
    import kg_splitter
    c = sqlite3.connect(f'file:{LEDGER}?mode=ro', uri=True, timeout=5)
    try:
        rows = c.execute("SELECT DISTINCT code FROM row WHERE fault=? AND verdict NOT IN ('ok','zero','regenerated') ORDER BY 1", (fault,)).fetchall()
    finally:
        c.close()
    return [r[0] for r in rows if not kg_splitter.is_sweep_code(r[0])]


def enroll(codes, why: str = 'operator', dry: bool = False) -> dict:
    """Put product codes (a parent code of an unsplit KG, or block codes) on the reprocess list."""
    import kg_splitter
    import kg_v2_store as kvs
    out = {'enrolled': [], 'skipped': {}}
    with _LOCK:
        d = _load()
        for code in codes:
            code = str(code)
            if kg_splitter.is_sweep_code(code):
                out['skipped'][code] = 'sweep unit'
            elif not kvs.has(code):
                out['skipped'][code] = 'no stored product with this code (split parent? enrol its blocks)'
            elif (d['codes'].get(code) or {}).get('state') in ('enrolled', 'requeued'):
                out['skipped'][code] = 'already on the list'
            else:
                d['codes'][code] = {'state': 'enrolled', 'why': why, 'enrolled_at': _now(), 'attempts': 0,
                                    'generated_at': kvs.get_meta(code).get('generated_at')}
                out['enrolled'].append(code)
        if not dry and out['enrolled']:
            _save(d)
    return out


def cancel(codes) -> int:
    n = 0
    with _LOCK:
        d = _load()
        for code in codes:
            e = d['codes'].get(str(code))
            if e and e.get('state') == 'enrolled':
                e.update(state='cancelled', note='operator'); n += 1
        if n:
            _save(d)
    return n


def _close_reprocess(code: str, row: dict) -> None:
    with _LOCK:
        d = _load()
        e = d['codes'].get(code)
        if not e or e.get('state') not in ('requeued', 'enrolled', 'stuck'):
            return
        if not row.get('fresh'):
            e['note'] = f"ingested generation {row['generated_at'][:16]} is NOT fresh (upgraded_from) — still waiting"
        else:
            e.update(state='done', done_at=_now(), new_generated_at=row['generated_at'], retired=sorted(row.get('retired') or {}),
                     quality=row.get('quality'), note='')
        _save(d)


def _busy_elsewhere(code: str, ent: dict) -> str | None:
    """Another rebuild mechanism holds this code → wait, never stack two reruns."""
    kg = code.split('-')[0]
    try:
        import label_mix as lm
        for c2, e in (lm._load().get('codes') or {}).items():
            if e.get('state') in ('detected', 'requeued') and (c2 in (code, kg) or code in (e.get('blocks') or [])):
                return 'label_mix rerun'
    except Exception:  # noqa: BLE001
        pass
    try:
        vr = json.loads((DATA_DIR / 'v2_regen.json').read_text())
        e = vr.get(code) or vr.get(kg)
        if isinstance(e, dict) and e.get('state') in ('requeued', 'pending', 'confirmed'):
            return 'v2_regen'
    except Exception:  # noqa: BLE001
        pass
    try:
        pr = json.loads((DATA_DIR / 'product_repair.json').read_text())
        e = (pr.get('codes') or pr).get(code)
        if isinstance(e, dict) and e.get('state') in ('pending', 'fixable'):
            return 'product_repair'
    except Exception:  # noqa: BLE001
        pass
    return None


def _retry_queue() -> set:
    try:
        q = json.loads((DATA_DIR / 'retry_queue.json').read_text())
        return {str(x) for x in q} if isinstance(q, list) else set()
    except Exception:  # noqa: BLE001
        return set()


def _post_requeue(code: str) -> bool:
    try:
        import requests
        headers = {}
        try:
            tok = (ROOT / 'data' / 'admin_token').read_text().strip()
            if tok:
                headers['X-Admin-Token'] = tok
        except Exception:  # noqa: BLE001
            pass
        r = requests.post('http://127.0.0.1:8000/api/v1/processing/queue',
                          json={'kgs': [code], 'position': 0, 'skip_processed': False, 'keep_products': True,
                                'fresh_run': True}, timeout=30, headers=headers)
        return r.ok
    except Exception as ex:  # noqa: BLE001
        log.warning('qc_reprocess %s: requeue POST failed: %s', code, ex)
        return False


def sweep(requeue: bool = True) -> dict:
    """Director audit tick: pace the operator's list.  Enrols nothing."""
    import label_mix as lm
    with _LOCK:
        d = _load()
    codes = d['codes']
    if not any(e.get('state') in ('enrolled', 'requeued') for e in codes.values()):
        return {'requeued': 0, 'outstanding': 0}
    ent = lm._manifest_entries()
    queue = _retry_queue()
    now = time.time(); events = []; changed = False
    for code, e in sorted(codes.items()):
        if e.get('state') != 'requeued':
            continue
        j = ent.get(f'{code}_json')
        fresh_json = lm._live(j) and str(j.get('uploaded_at') or '') > str(e.get('requeued_at') or '')
        age = now - _ts(e.get('requeued_at'))
        if fresh_json:
            # the generation hook closes it on ingest; note the upload meanwhile
            if not e.get('uploaded_at'):
                e['uploaded_at'] = j.get('uploaded_at'); e['note'] = 'fresh products uploaded — waiting for the ingest'; changed = True
        elif code not in queue and age > LOST_AFTER_S:
            # left the queue long ago and no fresh `_json` → the requeue was reverted (GET prune) or the run died
            e.update(state='enrolled' if int(e.get('attempts', 0)) < MAX_ATTEMPTS else 'stuck', note='requeue reverted (prune / peer-sync)')
            changed = True
            events.append((f"qc_reprocess: {code} requeue was reverted — {e['state']}", code, 'warning'))
        elif age > STALL_AFTER_S and not e.get('stall_noted'):
            e['stall_noted'] = True; changed = True
            events.append((f'qc_reprocess: {code} requeued {age / 3600:.0f} h ago, no fresh products yet', code, 'warning'))
    outstanding = sum(1 for e in codes.values() if e.get('state') == 'requeued')
    n = 0
    if requeue:
        for code, e in sorted(codes.items(), key=lambda ce: (ce[1].get('enrolled_at', ''), ce[0])):
            if e.get('state') != 'enrolled' or n >= MAX_REQUEUE_PER_TICK or outstanding >= MAX_OUTSTANDING:
                continue
            busy = _busy_elsewhere(code, ent)
            if busy:
                if e.get('note') != f'waiting: {busy}':
                    e['note'] = f'waiting: {busy}'; changed = True
                continue
            if int(e.get('attempts', 0)) >= MAX_ATTEMPTS:
                e.update(state='stuck', note=f'{MAX_ATTEMPTS} requeues without a fresh generation'); changed = True
                continue
            if code in queue or _post_requeue(code):
                e.update(state='requeued', requeued_at=_now(), attempts=int(e.get('attempts', 0)) + 1,
                         note='already queued' if code in queue else '')
                e.pop('uploaded_at', None); e.pop('stall_noted', None)
                n += 1; outstanding += 1; changed = True
                events.append((f"qc_reprocess: {code} requeued for a fresh run ({e.get('why')}; products stay served, "
                               f"attempt {e['attempts']})", code, 'info'))
            else:
                e['note'] = f'requeue POST failed {_now()}'; changed = True
    if changed:
        with _LOCK:
            cur = _load()
            for code, e in codes.items():        # the ingest hook may have closed a code meanwhile
                if (cur['codes'].get(code) or {}).get('state') == 'done':
                    continue
                cur['codes'][code] = e
            _save(cur)
    for msg, code, level in events:
        _event(msg, code, level)
    return {'requeued': n, 'outstanding': outstanding, 'events': len(events)}


def summary() -> dict:
    d = _load()
    by = {}
    for e in d['codes'].values():
        by[e.get('state', '?')] = by.get(e.get('state', '?'), 0) + 1
    g = {'rows': 0, 'new_24h': 0, 'fresh_24h': 0, 'acted': 0, 'retired': {}, 'bootstrap_at': None, 'last': None}
    if DB.exists():
        c = _conn()
        try:
            g['rows'] = c.execute('SELECT COUNT(*) FROM gen').fetchone()[0]
            g['bootstrap_at'] = (c.execute("SELECT v FROM meta WHERE k='bootstrap_at'").fetchone() or [None])[0]
            since = datetime.fromtimestamp(time.time() - 86400, timezone.utc).isoformat(timespec='seconds')
            g['new_24h'], g['fresh_24h'] = c.execute("SELECT COUNT(*), COALESCE(SUM(fresh), 0) FROM gen WHERE at >= ? AND quality NOT LIKE '%bootstrap%'", (since,)).fetchone()
            for (r,) in c.execute("SELECT retired FROM gen WHERE retired NOT IN ('{}', '')"):
                g['acted'] += 1
                for k in json.loads(r):
                    if not k.endswith('_err'):
                        g['retired'][k] = g['retired'].get(k, 0) + 1
            r = c.execute("SELECT code, at FROM gen WHERE quality NOT LIKE '%bootstrap%' ORDER BY at DESC LIMIT 1").fetchone()
            g['last'] = {'code': r[0], 'at': r[1]} if r else None
        finally:
            c.close()
    rows = [dict(code=c_, **{k: e.get(k) for k in ('state', 'why', 'attempts', 'requeued_at', 'done_at', 'note', 'retired', 'quality')})
            for c_, e in sorted(d['codes'].items(), key=lambda ce: ({'requeued': 0, 'stuck': 1, 'enrolled': 2}.get(ce[1].get('state'), 3), ce[0]))]
    return {'counts': by, 'total': len(d['codes']), 'generations': g, 'rows': rows, 't': d.get('t')}


def status_line(max_rows: int = 8) -> str | None:
    """The ``qc_reprocess:`` line of /process.txt."""
    s = summary()
    g = s['generations']
    if not s['total'] and not g['rows']:
        return None
    by = s['counts']
    ret = g['retired']
    line = ('qc_reprocess: list ' + (' · '.join(f'{k} {by[k]}' for k in ('enrolled', 'requeued', 'done', 'stuck', 'cancelled') if by.get(k)) or 'empty')
            + f' (operator-enrolled, director-paced {MAX_REQUEUE_PER_TICK}/tick ≤{MAX_OUTSTANDING} in flight; old products stay served until replaced)'
            + f" | generations documented on ingest: {g['rows']} products, last 24h {g['new_24h']} new ({g['fresh_24h']} fresh)"
            + f" | fixes retired by a fresh generation: registration {ret.get('grid25_registration', 0)} · patch {ret.get('grid25_patch', 0)} · position {ret.get('ne_pos_fix', 0)}"
            + (f" | last {g['last']['code']} {g['last']['at'][:16]}" if g['last'] else ''))
    out = [line]
    for r in [r for r in s['rows'] if r['state'] in ('requeued', 'stuck')][:max_rows]:
        out.append(f"  qc_reprocess {r['code']}: {r['state']} since {str(r.get('requeued_at') or '')[:16]} attempt {r.get('attempts')}"
                   + (f" — {r['note']}" if r.get('note') else ''))
    done = [r for r in s['rows'] if r['state'] == 'done']
    for r in sorted(done, key=lambda r: r.get('done_at') or '', reverse=True)[:3]:
        q = r.get('quality') or {}
        out.append(f"  qc_reprocess {r['code']}: done {str(r.get('done_at') or '')[:16]} — retired {','.join(r.get('retired') or []) or 'nothing'}; "
                   f"now tiles={q.get('tiles')} restored={q.get('restored')} edges={q.get('edges')} {q.get('label') or ''}".rstrip())
    return '\n'.join(out)


if __name__ == '__main__':
    a = sys.argv[1:]
    dry = '--dry' in a
    why = a[a.index('--why') + 1] if '--why' in a else None
    if '--bootstrap' in a:
        print(json.dumps(bootstrap()))
    elif '--enroll-fault' in a:
        f = a[a.index('--enroll-fault') + 1]
        print(json.dumps(enroll(ledger_codes(f), why or f'qc ledger: {f}', dry=dry), indent=1))
    elif '--enroll' in a:
        cs = [x for x in a[a.index('--enroll') + 1:] if not x.startswith('--') and x != why]
        print(json.dumps(enroll(cs, why or 'operator', dry=dry), indent=1))
    elif '--cancel' in a:
        print(cancel([x for x in a[a.index('--cancel') + 1:] if not x.startswith('--')]))
    elif '--sweep' in a:
        print(json.dumps(sweep(requeue='--no-requeue' not in a)))
    else:
        print(status_line() or 'qc_reprocess: nothing enrolled, no generations documented')
