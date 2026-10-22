#!/usr/bin/env python3
"""Backfill the primary's merged 24h log after a director outage.

While the primary is not director, peers push their `recent_log` (200-entry
ring) to whoever *is* director, so the primary's `data/combined_log_24h.jsonl`
ends up with a hole.  Interim directors only hold what was pushed to them
(and bootstrap from the same tiny rings), so their rings are lossy too.

Two sources, merged and de-duplicated:

1. `GET <interim>/api/v1/director/log/history?hours=H` on every peer that
   held the director role (their rings are already in combined-log form).
2. `GET <peer>/api/v1/processing/log?lines=N` on EVERY enabled peer -- the
   raw processor.log goes back days.  We reconstruct combined-log entries
   for WARNING/ERROR lines (same suppression list as the processor's
   `_WarningRelayHandler`) plus a few key INFO lines (uploads, KG start,
   cache-miss aborts), restricted to the outage window.  Tagged
   `recovered: true`.

Usage (on the primary):
  scripts/recover_combined_log.py --since 2026-09-30T05:45 --until 2026-09-30T14:40 \
      --interim at240,at231,at204 [--dry-run]

The ring is rewritten atomically (sorted by ts).
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import re
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
RING = ROOT / 'data' / 'combined_log_24h.jsonl'
PEERS = ROOT / 'data' / 'austria_processor' / 'peers.json'
TOKEN = (ROOT / 'data' / 'admin_token').read_text().strip()
HDR = {'X-Admin-Token': TOKEN, 'Accept-Encoding': 'gzip'}

# Mirror of austria_processor._WarningRelayHandler._SUPPRESS
SUPPRESS = (
    "DeprecationWarning: 'Memory' driver is deprecated",
    "Failed to parse API error response",
    "NaN nodata value cannot be recorded",
    "CPLE_AppDefined:Request for ",
    "Intersection is empty Window(",
    "urllib3.connectionpool: Retrying (Retry(",
    "rasterio._env: CPLE_AppDefined in DeprecationWarning",
)
DOWNGRADE = ("Preflight process graph validation failed",)
RAW_RE = re.compile(
    r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),(\d{3}) (\w+) (\S+?): (.*)$')
KG_RE = re.compile(r'\bKG (\d{5}(?:-[a-z]+(?:-\d+)?)?)\b')
INFO_KEEP = ('upload_stream:', 'Creating new deposition',
             'Starting KG', 'aborting after tile', 'cache-only mode hit')


def load_peers() -> list[dict]:
    p = json.load(open(PEERS))
    peers = p['peers'] if isinstance(p, dict) and 'peers' in p else p
    peers = list(peers.values()) if isinstance(peers, dict) else peers
    return [x for x in peers if x.get('url') and x.get('enabled', True)
            and x.get('id') != 'primary']


def fetch_interim(peer: dict, hours: float) -> list[dict]:
    try:
        r = requests.get(peer['url'].rstrip('/') +
                         f'/api/v1/director/log/history?hours={hours:.1f}&limit=50000',
                         headers=HDR, timeout=120)
        r.raise_for_status()
        return r.json().get('entries', [])
    except Exception as e:
        print(f'  interim {peer["id"]}: FAILED {e}', file=sys.stderr)
        return []


def fetch_raw(peer: dict, since: str, until: str) -> list[dict]:
    try:
        r = requests.get(peer['url'].rstrip('/') +
                         '/api/v1/processing/log?lines=400000',
                         headers=HDR, timeout=180)
        r.raise_for_status()
        lines = r.json().get('lines', [])
    except Exception as e:
        print(f'  raw {peer["id"]}: FAILED {e}', file=sys.stderr)
        return []
    out: list[dict] = []
    cur_kg = ''
    lo, hi = since.replace('T', ' '), until.replace('T', ' ')
    for ln in lines:
        m = RAW_RE.match(ln)
        if not m:
            continue
        ts, ms, lvl, name, msg = m.groups()
        if ts < lo or ts > hi:
            continue
        km = KG_RE.search(msg)
        if km:
            cur_kg = km.group(1)
        lvl = lvl.upper()
        body = f'{name}: {msg}'
        if lvl in ('WARNING', 'ERROR', 'CRITICAL'):
            if any(s in body for s in SUPPRESS):
                continue
            level = 'error' if lvl != 'WARNING' else 'warning'
            if level == 'error' and any(s in body for s in DOWNGRADE):
                level = 'warning'
        elif lvl == 'INFO' and any(k in msg for k in INFO_KEEP):
            level = 'info'
        else:
            continue
        kg = cur_kg
        text = body if (not kg or f'KG {kg}' in body) else f'KG {kg}: {body}'
        iso = ts.replace(' ', 'T') + f'.{ms}000+00:00'
        out.append({'ts': iso, 'peer': peer['id'], 'level': level,
                    'msg': text, 'kg': kg, 'recovered': True})
    return out


def norm(msg: str) -> str:
    return re.sub(r'\d+', '#', msg)[:160]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--since', required=True)
    ap.add_argument('--until', required=True)
    ap.add_argument('--interim', default='')
    ap.add_argument('--dry-run', action='store_true')
    a = ap.parse_args()
    peers = load_peers()
    byid = {p['id']: p for p in peers}

    ring: list[dict] = []
    for ln in open(RING, encoding='utf-8'):
        try:
            ring.append(json.loads(ln))
        except Exception:
            pass
    print(f'ring: {len(ring)} entries')

    exact = {(e.get('ts'), e.get('peer'), e.get('msg')) for e in ring}
    fuzzy: dict[tuple, list[datetime]] = {}
    for e in ring:
        try:
            fuzzy.setdefault((e.get('peer'), norm(e.get('msg', ''))), []).append(
                datetime.fromisoformat(e['ts']))
        except Exception:
            pass

    def is_dup(e: dict, window_s: float) -> bool:
        if (e['ts'], e['peer'], e['msg']) in exact:
            return True
        lst = fuzzy.get((e['peer'], norm(e['msg'])))
        if not lst:
            return False
        t = datetime.fromisoformat(e['ts'])
        return any(abs((x - t).total_seconds()) <= window_s for x in lst)

    def add(e: dict) -> None:
        exact.add((e['ts'], e['peer'], e['msg']))
        fuzzy.setdefault((e['peer'], norm(e['msg'])), []).append(
            datetime.fromisoformat(e['ts']))
        ring.append(e)

    added_interim = 0
    hours = (datetime.now(timezone.utc)
             - datetime.fromisoformat(a.since + '+00:00')).total_seconds() / 3600 + 1
    for pid in [x for x in a.interim.split(',') if x]:
        if pid not in byid:
            print(f'  interim {pid}: not in peers.json', file=sys.stderr)
            continue
        ents = fetch_interim(byid[pid], hours)
        n = 0
        for e in ents:
            if not e.get('ts') or is_dup(e, 1.0):
                continue
            add(e); n += 1
        added_interim += n
        print(f'  interim {pid}: {len(ents)} fetched, {n} new')

    added_raw = 0
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch_raw, p, a.since, a.until): p for p in peers}
        for f in cf.as_completed(futs):
            p = futs[f]
            ents = f.result()
            last: dict[tuple, datetime] = {}
            n = 0
            for e in sorted(ents, key=lambda x: x['ts']):
                k = (e['level'], norm(e['msg']), e['kg'])
                t = datetime.fromisoformat(e['ts'])
                if k in last and (t - last[k]).total_seconds() < 600:
                    continue
                last[k] = t
                if is_dup(e, 120.0):
                    continue
                add(e); n += 1
            added_raw += n
            print(f'  raw {p["id"]}: {len(ents)} candidates, {n} new')

    print(f'added: interim={added_interim} raw={added_raw} -> ring {len(ring)}')
    if a.dry_run:
        return 0
    ring.sort(key=lambda e: e.get('ts', ''))
    fd, tmp = tempfile.mkstemp(dir=str(RING.parent), prefix='.combined_log_')
    with os.fdopen(fd, 'w', encoding='utf-8') as fh:
        for e in ring:
            fh.write(json.dumps(e, ensure_ascii=False) + '\n')
    os.replace(tmp, RING)
    print(f'wrote {RING} ({RING.stat().st_size / 1e6:.1f} MB)')
    return 0


if __name__ == '__main__':
    sys.exit(main())
