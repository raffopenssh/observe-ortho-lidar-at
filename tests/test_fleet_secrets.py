"""fleet_secrets: store, lease, deny, audit, peer-side client (fake director).
Run: python3 tests/test_fleet_secrets.py (no services needed)."""
import json, os, sys, tempfile, threading
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

tmp = tempfile.mkdtemp()
os.environ['SRTM_DATA_DIR'] = tmp
for k in ('ZENODO_TOKEN', 'UMFELD_TOKEN', 'GITHUB_TOKEN'):
    os.environ.pop(k, None)
import fleet_secrets as fs
assert fs.STORE == Path(tmp) / 'secrets.json'

# --- primary side ---
fs.is_primary = lambda: True
assert fs.store_rev() == 0 and fs.lease_for('at1') is None
r1 = fs.set_secret('zenodo', 'Z' * 40); r2 = fs.set_secret('github', 'github_pat_' + 'g' * 30)
assert (r1, r2) == (1, 2) and oct(fs.STORE.stat().st_mode & 0o777) == '0o600'
assert fs.set_secret('github', 'github_pat_' + 'g' * 30) == 2, 'idempotent set must not bump rev'
ev = []
fs.set_event_hook(lambda k, m: ev.append((k, m)))
b = fs.lease_for('at1', '1.2.3.4')
assert b['rev'] == 2 and b['zenodo'] == 'Z' * 40 and 'umfeld' not in b
assert fs.leases()['at1']['n'] == 1 and ev[-1][0] == 'secret-lease'
fs.lease_for('at1', '1.2.3.4'); assert len(ev) == 1, 'same rev: no second event'
fs.set_denied('at1', True); assert fs.lease_for('at1') is None and ev[-1][0] == 'secret-lease-denied'
fs.set_denied('at1', False); assert fs.lease_for('at1')['rev'] == 2
assert fs.token('zenodo') == 'Z' * 40 and fs.status()['source'] == 'store'
try:
    fs.token('umfeld', required=True); raise SystemExit('FAIL: missing secret not raised')
except RuntimeError:
    pass

# --- peer side against a fake director ---
class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        if self.path != fs.LEASE_PATH or self.headers.get('X-Admin-Token') != 'peer-tok':
            self.send_response(401); self.end_headers(); return
        body = json.dumps({'rev': 7, 'zenodo': 'LEASED' * 4, 'umfeld': 'U' * 20}).encode()
        self.send_response(200); self.send_header('Content-Type', 'application/json'); self.end_headers(); self.wfile.write(body)
srv = HTTPServer(('127.0.0.1', 0), H); threading.Thread(target=srv.serve_forever, daemon=True).start()
fs.is_primary = lambda: False
fs._cache.update(rev=None, vals={}, fetched=0.0, last_try=0.0)
(Path(tmp) / 'admin_token').write_text('peer-tok\n')
(Path(tmp) / 'umfeld_token').write_text('legacy-disk-umfeld-token\n')
fs.director_base = lambda: f'http://127.0.0.1:{srv.server_port}'
assert fs.token('zenodo') == 'LEASED' * 4 and fs.cached_rev() == 7
assert not (Path(tmp) / 'umfeld_token').exists(), 'legacy disk copy must be scrubbed after a lease'
assert fs.token('umfeld') == 'U' * 20 and fs.token('github') == ''
assert set(fs.export_env()) == {'ZENODO_TOKEN', 'UMFELD_TOKEN'}
# rev signal from the status-push ack: unchanged rev -> no fetch; changed -> refetch
fs._cache['vals']['zenodo'] = 'stale'
fs.note_rev(7); import time; time.sleep(0.3); assert fs.token('zenodo') == 'stale'
fs.note_rev(8); time.sleep(0.5); assert fs.token('zenodo') == 'LEASED' * 4
# wrong own token -> lease refused -> keeps old cache, legacy fallback for missing kinds
(Path(tmp) / 'admin_token').write_text('wrong\n'); fs._cache.update(vals={}, rev=None, last_try=0.0)
(Path(tmp) / 'github_token').write_text('legacy-gh-token-xxxxx\n')
assert fs.token('github') == 'legacy-gh-token-xxxxx' and 'HTTP 401' in fs.status()['err']
print('OK: fleet_secrets store / lease / deny / audit / peer client')
