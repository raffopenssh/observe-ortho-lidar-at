"""python3 tests/test_service_guard.py — client key + whole-tier ClientGuard."""
import os, sys, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import service_guard as sg


class Req:
    def __init__(self, ra, xff=None):
        self.remote_addr = ra
        self.headers = {'X-Forwarded-For': xff} if xff else {}


# exe.dev appends the real client; forged leading hops must not pick the bucket
assert sg.client_key(Req('127.0.0.1', '6.6.6.6, 1.2.3.4, 1.2.3.4')) == '1.2.3.4'
assert sg.client_key(Req('127.0.0.1', '1.2.3.4')) == '1.2.3.4'
# direct LAN connection: header ignored
assert sg.client_key(Req('10.42.0.9', '6.6.6.6')) == '10.42.0.9'
assert sg.is_local(Req('127.0.0.1')) and not sg.is_local(Req('127.0.0.1', '1.2.3.4'))
assert not sg.is_local(Req('10.42.0.9'))

with tempfile.NamedTemporaryFile('w', suffix='.txt', delete=False) as f:
    f.write('# friends\n9.9.9.9\n8.8.0.0/16  # net\nbogus\n')
sg.ClientGuard.FRIENDS_PATH = f.name
os.environ.update(PUBLIC_RATE_PER_S='1', PUBLIC_RATE_PER_S_BURST='5', PUBLIC_INFLIGHT_MAX='2',
                  PUBLIC_INFLIGHT_FRIEND='4', PUBLIC_FRIEND_X='10')
cg = sg.ClientGuard()
assert cg.enter('1.1.1.1')[0] and cg.enter('1.1.1.1')[0]
ok, _, why = cg.enter('1.1.1.1'); assert not ok and why == 'too_many_in_flight'
assert cg.enter('2.2.2.2')[0]                       # other clients unaffected
cg.leave('1.1.1.1'); assert cg.enter('1.1.1.1')[0]
cg.leave('1.1.1.1'); cg.leave('1.1.1.1')
assert cg.enter('1.1.1.1')[0]; cg.leave('1.1.1.1')   # 5th token
ok, _, why = cg.enter('1.1.1.1'); assert not ok and why == 'rate_limited', why   # burst 5 spent
assert cg.enter('1.1.1.1', count_inflight=False)[0] is False
for _ in range(4):
    assert cg.enter('8.8.4.4')[0]                   # friend: in-flight 4
assert cg.enter('8.8.4.4')[2] == 'too_many_in_flight'
for _ in range(4):
    cg.leave('8.8.4.4')
assert all(cg.enter('9.9.9.9', count_inflight=False)[0] for _ in range(40))   # friend burst 50
assert cg._in.get('1.1.1.1') is None
os.unlink(f.name)
print('ok')
