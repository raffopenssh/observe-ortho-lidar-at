"""Re-create a missing v1 ``_json`` completion marker from ``_json_v2``.

Why
---
``<code>_json`` (the v1-shaped, legacy-stripped summary) is the fleet-wide
COMPLETION MARKER: ``app._kg_coverage_complete``, the processor's
``completed_codes``, ``zen_stall`` and the dashboard all key on it.  Fresh
v2.4 KGs write it last (full → light_v2 → ne_h3b → json_v2 → json).  Between
Oct 4 and Oct 13 2026 the role-data eviction loop on demoted peers purged
``json/*.json`` every 10 min, which deleted the freshly built marker before
its upload on ~half of all fresh KGs: 413 codes ended with
``_full_gpkg`` + ``_light_gpkg_v2`` + ``_json_v2`` (+ ``_ne_h3b``) but no
``_json`` — read as incomplete, re-picked by the pending sweep and re-run
from scratch (~1.5 h each, forever, since the re-run lost it again).

``kg_json_v2`` is a lossless encoding of the very same ``json_summary`` dict
(``roundtrip_ok``), so the primary can rebuild the marker itself: decode the
``_json_v2`` blob (own ``kg_v2_store`` first, Zenodo download as fallback),
strip any legacy residue (pre-2.4 blobs), write ``<code>.json`` and stream
it to Zenodo as ``<code>_json`` with ``status=cf``.  Runs on the director
loop (``peer_director._check_json_heal``), a few codes per tick, under the
fleet Zenodo lock; skips codes that are queued / running on a peer (the peer
will upload its own marker).  The ``json_heal:`` line in ``/process.txt``
and ``director_event`` rows (``?q=json_heal``) audit it.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

DATA_DIR = Path("data/austria_processor")
JSON_DIR = DATA_DIR / "json"
TMP_DIR = JSON_DIR / "heal_tmp"
MANIFEST_PATH = DATA_DIR / "zenodo_manifest.json"

MIN_AGE_S = 30 * 60          # json_v2 must be this old (peer's own chain had time to finish)
MAX_PER_TICK = 4             # primary egress is metered (~0.3-1 MB per json)
_CODE_RX = re.compile(r'^(\d+(?:-[a-z][-a-z0-9]*)?)_json_v2$')


def _committed(e) -> bool:
    return isinstance(e, dict) and int(e.get('size') or 0) > 0 \
        and 'error' not in str(e.get('status') or '') and e.get('status') != 'removed'


def candidates(entries: dict, *, now: float | None = None) -> list[tuple[str, dict]]:
    """``[(code, json_v2_entry)]`` lacking a committed ``_json`` while
    full + light_v2 + json_v2 are all committed and json_v2 is ≥ MIN_AGE_S old."""
    now = now or time.time()
    out = []
    for k, e in entries.items():
        m = _CODE_RX.match(k)
        if not m or not _committed(e):
            continue
        code = m.group(1)
        if _committed(entries.get(f'{code}_json')):
            continue
        if not (_committed(entries.get(f'{code}_full_gpkg'))
                and _committed(entries.get(f'{code}_light_gpkg_v2'))):
            continue
        try:
            ts = datetime.fromisoformat(str(e.get('uploaded_at')).replace('Z', '+00:00')).timestamp()
        except Exception:
            continue
        if now - ts < MIN_AGE_S:
            continue
        out.append((code, e))
    out.sort(key=lambda t: str(t[1].get('uploaded_at')))   # oldest first
    return out


def _load_doc(code: str, entry: dict, manifest) -> dict:
    import kg_json_v2
    blob = None
    try:
        import kg_v2_store
        blob = kg_v2_store.get_blob(code)
    except Exception as ex:  # noqa: BLE001
        log.debug('json_heal %s: store miss (%s)', code, ex)
    if blob is None:
        import zenodo_client as _zc
        TMP_DIR.mkdir(parents=True, exist_ok=True)
        p = _zc.Client(token=_zc.DEFAULT_TOKEN).download(f'{code}_json_v2', TMP_DIR, manifest,
                                                         verify_checksum=False)
        try:
            blob = Path(p).read_bytes()
        finally:
            try:
                os.unlink(p)
            except OSError:
                pass
    return kg_json_v2.decode(blob)


def heal_one(code: str, entry: dict, manifest=None, client=None) -> dict:
    """Rebuild + upload ``<code>_json``.  Returns ``{ok, size, source}``."""
    import zenodo_client as _zc
    from zenodo_lock import zenodo_upload_lock
    from austria_processor import strip_legacy_sections
    manifest = manifest or _zc.Manifest(str(MANIFEST_PATH))
    client = client or _zc.Client(token=_zc.DEFAULT_TOKEN)
    doc = strip_legacy_sections(_load_doc(code, entry, manifest))
    doc.setdefault('kg_code', code)
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    out_p = TMP_DIR / f'{code}.json'
    with open(out_p, 'w') as fh:
        json.dump(doc, fh, separators=(',', ':'), default=str)
    v2ver = str(entry.get('version') or '')
    version = 'v2-partial' if v2ver.endswith('-partial') else 'v2'
    dq = doc.get('data_quality') or {}
    name = doc.get('kg_name') or (doc.get('kg') or {}).get('name') or ''
    meta = lambda k, fn, v, _n=name, _dq=dq: _zc.landscape_metadata(  # noqa: E731
        code, _n, v, 'json', quality_score=_dq.get('quality_score', 0) or 0,
        quality_grade=_dq.get('quality_grade', '') or '',
        available_layers=_dq.get('available_layers'), missing_layers=_dq.get('missing_layers'))
    with zenodo_upload_lock(purpose='json_heal', kg=code, max_wait_s=180):
        ent = client.upload_stream(f'{code}_json', str(out_p), version, meta, manifest, delete_after=True)
    ent.status = 'cf'
    manifest.set(f'{code}_json', ent)
    manifest.save()
    return {'ok': True, 'size': int(ent.size or 0), 'version': version}


def sweep(*, running: set | None = None, queued: set | None = None,
          max_per_tick: int = MAX_PER_TICK, event=None) -> dict:
    """One director tick: heal up to *max_per_tick* candidates.  *running*
    / *queued* codes (and their parents/blocks) are skipped — a peer owns
    them and will upload its own marker."""
    try:
        raw = json.loads(MANIFEST_PATH.read_text())
        entries = raw.get('entries', raw) or {}
    except Exception as ex:  # noqa: BLE001
        return {'error': f'manifest: {ex}'}
    cands = candidates(entries)
    skip = set(running or ()) | set(queued or ())
    todo = [(c, e) for c, e in cands if c not in skip and c.split('-')[0] not in skip]
    res = {'candidates': len(cands), 'skipped_busy': len(cands) - len(todo),
           'healed': [], 'failed': []}
    if not todo:
        return res
    import zenodo_client as _zc
    manifest = _zc.Manifest(str(MANIFEST_PATH))
    client = _zc.Client(token=_zc.DEFAULT_TOKEN)
    for code, e in todo[:max_per_tick]:
        try:
            r = heal_one(code, e, manifest, client)
            res['healed'].append(code)
            msg = (f'json_heal: {code} _json rebuilt from _json_v2 ({r["size"]} B, {r["version"]}) — '
                   f'completion marker restored, {len(cands) - len(res["healed"])} left')
            log.info(msg)
            if event:
                event(msg, kg=code)
        except Exception as ex:  # noqa: BLE001
            res['failed'].append(code)
            log.warning('json_heal: %s failed: %s', code, ex)
            if event:
                event(f'json_heal: {code} failed: {ex}', kg=code, level='warning')
    res['remaining'] = len(cands) - len(res['healed'])
    return res


if __name__ == '__main__':  # ad-hoc: python3 json_heal.py [N]
    import sys
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    print(json.dumps(sweep(max_per_tick=n), indent=1))
