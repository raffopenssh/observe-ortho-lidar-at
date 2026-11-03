#!/usr/bin/env python3
"""Fleet patch audit: compare every peer's OS package versions + pip pins
against the director (``GET /api/v1/admin/pkgstat`` on each VM).

Usage (on the director):
  python3 tools/fleet_pkgstat.py            # table of deviations only
  python3 tools/fleet_pkgstat.py --fresh    # bypass the 10-min per-VM cache
  python3 tools/fleet_pkgstat.py --json     # raw per-VM dump

Peers come from ``config.peers()`` (``SRTM_PEERS``; empty in single-VM
mode — then only the local instance is shown) and are reached with the
``X-Operator-Token`` header (``SRTM_OPERATOR_TOKEN``).  Read-only and tiny
(~2 KB/peer).  Peers older than the commit that added the endpoint answer
404 -> listed as ``no-endpoint``.
"""
import argparse, json, sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
import os; os.chdir(REPO)
import config  # noqa: E402


def _peer_id(url: str) -> str:
    from urllib.parse import urlparse
    host = (urlparse(url).hostname or url).strip().lower()
    return host.split('.')[0] or host


def fetch(url, headers, fresh):
    try:
        r = requests.get(url.rstrip('/') + '/api/v1/admin/pkgstat',
                         params={'fresh': '1'} if fresh else None,
                         headers=headers or {}, timeout=90)
        if r.status_code != 200:
            return {'_err': f'http {r.status_code}' + (' (no-endpoint/old commit?)' if r.status_code == 404 else '')}
        return r.json()
    except Exception as e:
        return {'_err': type(e).__name__}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--fresh', action='store_true')
    ap.add_argument('--json', action='store_true')
    a = ap.parse_args()
    ref = fetch(config.APP_URL, None, a.fresh)          # loopback: no header needed
    tok = config.operator_token()
    hdrs = {'X-Operator-Token': tok} if tok else {}
    peers = [(_peer_id(u), u) for u in config.peers()]
    with ThreadPoolExecutor(8) as ex:
        res = dict(zip([pid for pid, _ in peers],
                       ex.map(lambda pu: fetch(pu[1], hdrs, a.fresh), peers)))
    if a.json:
        print(json.dumps({'primary': ref, 'peers': res}, indent=1)); return 0
    rd = ref.get('dpkg', {})
    print(f"primary: sec_pending={ref.get('security_pending')} reboot={ref.get('reboot_required')} "
          f"kernel={ref.get('kernel')} pip_drift={ref.get('pip_drift') or '-'}")
    ok = bad = err = 0
    for pid, d in sorted(res.items()):
        if '_err' in d:
            err += 1; print(f'  {pid:12s} ERR {d["_err"]}'); continue
        diff = {k: f'{d.get("dpkg", {}).get(k)}' for k, v in rd.items() if d.get('dpkg', {}).get(k) != v}
        flags = []
        if d.get('security_pending'): flags.append(f"sec_pending={d['security_pending']}")
        if d.get('security_pending_watch'): flags.append('watch=' + ','.join(d['security_pending_watch']))
        if d.get('reboot_required'): flags.append('REBOOT')
        if d.get('pip_drift'): flags.append('pip_drift=' + ','.join(d['pip_drift']))
        if diff: flags.append('dpkg!=primary ' + ' '.join(f'{k}={v}' for k, v in diff.items()))
        if flags:
            bad += 1; print(f'  {pid:12s} ' + ' '.join(flags))
        else:
            ok += 1
    print(f'peers: match={ok} deviating={bad} unreachable={err} (of {len(res)})')
    return 0


if __name__ == '__main__':
    sys.exit(main())
