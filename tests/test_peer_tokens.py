"""peer_tokens: scope tables, classification, outbound rewriting, HA pin.
Run: python3 tests/test_peer_tokens.py (no services needed)."""
import json, os, sys, tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
tmp = Path(tempfile.mkdtemp())
os.chdir(tmp)
(tmp / 'data' / 'austria_processor').mkdir(parents=True)

import peer_tokens as pt  # noqa: E402

fails = []
def check(cond, msg):
    if not cond:
        fails.append(msg); print('FAIL', msg)

# scopes
check(pt.peer_scope_allowed('/api/v1/zenodo/lock', 'POST'), 'zenodo lock')
check(pt.peer_scope_allowed('/api/v1/zenodo/lock/heartbeat', 'POST'), 'zenodo lock hb')
check(pt.peer_scope_allowed('/api/v1/processing/cache_manifest', 'PUT'), 'cm put')
check(pt.peer_scope_allowed('/api/v1/ne/declared/61047', 'GET'), 'ne declared')
check(pt.peer_scope_allowed('/api/v1/admin/bevdirect_binary', 'GET'), 'bevdirect bin')
for bad in ('/api/v1/admin/update', '/api/v1/admin/token', '/api/v1/admin/install_token',
            '/api/v1/director/takeover', '/api/v1/director/announce', '/api/v1/director/snapshot',
            '/api/v1/director/update_peers', '/api/v1/director/peers', '/api/v1/processing/start',
            '/api/v1/processing/stop', '/api/v1/credentials', '/api/v1/director/mode',
            '/api/v1/admin/proc_env', '/api/v1/director/handover'):
    check(not pt.peer_scope_allowed(bad, 'POST'), f'peer must not POST {bad}')
check(not pt.peer_scope_allowed('/api/v1/processing/product', 'DELETE'), 'product delete')
check(pt.mesh_scope_allowed('/api/v1/processing/cache_manifest', 'PUT'), 'mesh cm')
check(not pt.mesh_scope_allowed('/api/v1/zenodo/lock', 'POST'), 'mesh zenodo')
check(not pt.mesh_scope_allowed('/api/v1/admin/update', 'POST'), 'mesh admin')

# files + classify
master = pt.generate()
pt._write_secret(pt.ADMIN_TOKEN_PATH, master)
pA, pB, mesh = pt.generate(), pt.generate(), pt.generate()
pt.save_peer_tokens({'example.invalid': pA, 'example.invalid': pB, pt.LEGACY_KEY: 'legacy-master-token-xx'})
pt._write_secret(pt.MESH_TOKEN_PATH, mesh)
check(oct(pt.PEER_TOKENS_PATH.stat().st_mode)[-3:] == '600', 'peer_tokens 0600')
check(pt.classify(master, master) == 'master', 'classify master')
check(pt.classify(pA, master) == 'peer', 'classify peer')
check(pt.classify(mesh, master) == 'mesh', 'classify mesh')
check(pt.classify('legacy-master-token-xx', master) is None, 'legacy master is not a valid inbound token')
check(pt.classify('x' * 40, master) is None, 'classify unknown')
check(pt.classify('', master) is None, 'classify empty')
check(pt.peer_for_token(pB) == 'example.invalid', 'peer_for_token')

# outbound on the primary: master never leaves, per-host rewrite
(tmp / 'data/austria_processor/self.json').write_text(json.dumps(
    {'id': 'primary', 'url': 'https://example.invalid/k', 'director_url': None}))
check(pt.outbound_token('https://example.invalid:8000/api/v1/processing/stop', master) == pA, 'rewrite→peer A')
check(pt.outbound_token('https://example.invalid:8000/x', master) == pB, 'rewrite→peer B')
check(pt.outbound_token('https://example.invalid:8000/x', master) == 'legacy-master-token-xx', 'unmapped→legacy')
check(pt.outbound_token('http://127.0.0.1:8000/x', master) == master, 'loopback keeps master')
check(pt.outbound_token('https://example.invalid/k/x', master) == master, 'self keeps master')
check(pt.outbound_token('https://example.invalid/k/x', 'other-bearer-ish-token-value') == 'other-bearer-ish-token-value', 'foreign non-master untouched')
pt.save_peer_tokens({'example.invalid': pA})
check(pt.outbound_token('https://example.invalid:8000/x', master) == '', 'unmapped, no legacy → stripped')

# outbound on a peer: own token only to the director, mesh elsewhere
pt.PEER_TOKENS_PATH.unlink()
pt.load_peer_tokens(force=True)
(tmp / 'data/austria_processor/self.json').write_text(json.dumps(
    {'id': 'at1', 'url': 'https://example.invalid:8000', 'director_url': 'https://example.invalid/k'}))
pt._write_secret(pt.ADMIN_TOKEN_PATH, pA)
check(pt.outbound_token('https://example.invalid/k/api/v1/zenodo/lock', pA) == pA, 'peer→director own token')
check(pt.outbound_token('https://example.invalid:8000/api/v1/processing/cache_manifest', pA) == mesh, 'peer→peer mesh')

# requests hook end-to-end (no network: intercept at Session.request)
import requests
pt.install_outbound_hook()
seen = {}
_real = requests.Session.request
def fake(self, method, url, *a, **kw):
    seen['h'] = dict(kw.get('headers') or {}); raise RuntimeError('stop')
# hook wraps the original; swap the original it captured
import types
orig_name = pt.install_outbound_hook.__globals__
try:
    requests.get('https://example.invalid:8000/api/v1/processing/cache_manifest',
                 headers={'X-Admin-Token': pA}, timeout=0.001)
except Exception:
    pass
# Can't observe without a transport; use a prepared-request level check instead
s = requests.Session()
class _Stop(Exception): pass
def _send(self, req, **kw):
    seen['h'] = dict(req.headers); raise _Stop()
s.send = types.MethodType(_send, s)
try:
    s.request('PUT', 'https://example.invalid:8000/api/v1/processing/cache_manifest',
              headers={'X-Admin-Token': pA})
except _Stop:
    pass
check(seen.get('h', {}).get('X-Admin-Token') == mesh, f'hook rewrote header ({seen.get("h", {}).get("X-Admin-Token", "")[:6]}…)')

# HA pin
import director_ha as dha
check(not dha.ha_enabled(), 'HA disabled by default')
check(dha.director_url_allowed('https://example.invalid/k'), 'primary allowed')
check(not dha.director_url_allowed('https://example.invalid:8000'), 'peer as director refused')
check(dha._do_takeover('test').get('error') == 'ha_disabled', 'takeover refused')
check(dha.accept_announce({'new_director_id': 'at12', 'new_director_url': 'https://example.invalid:8000'}).get('reason') == 'ha_disabled', 'announce refused')
check(dha.step_down({}).get('error') == 'ha_disabled', 'step_down refused')
dha.set_director_url('https://example.invalid:8000')
check(json.loads((tmp / 'data/austria_processor/self.json').read_text()).get('director_url') == 'https://example.invalid/k', 'set_director_url pinned')

print('FAILURES:', len(fails)) if fails else print('OK: all peer_tokens / HA checks passed')
sys.exit(1 if fails else 0)
