"""Contributors of the public tree / flag reports, grouped for moderation (internal only).

Anybody may report (species, measures, added trees, flags) — no account.  What we keep of a
contributor is the 8-byte salted hash ``reporter`` (public_flags._reporter_hash); nothing here adds
to that.  This module only *reads the reports by that hash* and lets an operator

* list contributors with what they did (``--list``), one contributor's reports (``--show H``),
* give several hashes one internal label (``--group NAME H…``: one person, several browsers),
* mask a contributor / a group (``--mask H|NAME``): their **species** votes (default) or everything
  they said about trees (``--all``) stop counting — now and for whatever they send later,
* take that back (``--unmask``).

Nothing is deleted or rewritten: a mask is ONE row in ``rmod`` (public_flags.sqlite); the reports
stay in ``tree_ver`` and in ``/trees/taxa/history`` (``masked``).  ``tree_user._derive`` skips the
masked votes.  Each mask change appends one moderation row (op 3, no ``ref``) per affected tree, so
the tree gets a new ``rev``: per-KG state, /trees/bbox ETags and ``/trees/taxa?since=`` readers follow
with no other signal.  Handles (the hash as hex) never leave this box: no public route reads ``rmod``
labels or returns a handle.

    python3 tree_mod.py --list [--days 30]      python3 tree_mod.py --show 97c71156
    python3 tree_mod.py --group ch-1 97c71156 b0a20168
    python3 tree_mod.py --mask ch-1 --note "wrong species, mail 2026-11-27"   [--all] [--dry]
    python3 tree_mod.py --unmask ch-1
    python3 tree_mod.py --link ACCT H…          (see below)
Test: python3 tests/test_tree_mod.py     /process.txt line: tree_mod:

Accounts (prepared, not collected yet).  The NE server will later tie its users to an e-mail key and to
an export subscription key (the accounting product).  ``rmod.acct`` holds that link, privacy first:

* we never see or store an address or a key: the NE server sends a *pseudonym* of its key id (its own keyed
  hash), and we store only ``HMAC(our salt, "a:" + pseudonym)[:12]`` — a join key, not an identity.
  Our DB alone names nobody; the NE server's alone does not know our hashes; a join needs the NE server to present
  the pseudonym (accounting: "usage of account X" → hash X, look up).
* a keyed caller's reporter hash is HMAC(salt, "k:<holder>:<reporter>") and the NE server keeps sending the
  same per-browser ``reporter``, so ONE link row attaches everything that browser reported before
  and after — nothing is re-sent, no report changes, nothing is copied into the reports.
* ``--unlink`` erases the link (the reports stay as anonymous as they were); ``rmod`` rows that
  carry nothing any more are deleted.  Free-text ``note`` is for the operator's reason, no names.
Today links are set by hand (``--link``); the wire for the NE server is one optional field next to
``reporter`` (docs/tree-user.md → Accounts) and is NOT read by any route yet.  Groups, masks and the
list take an account pseudonym exactly like a group label.
"""
from __future__ import annotations

import sys
import time

import public_flags as pf
import tree_user as tu

SCHEMA = ('CREATE TABLE IF NOT EXISTS rmod (reporter BLOB PRIMARY KEY, grp TEXT, mask INTEGER NOT NULL DEFAULT 0, '
          'ts INTEGER NOT NULL, note TEXT, acct BLOB) WITHOUT ROWID')
ACCT_MAX = 80
DEFAULT_HOLDER = 'ne-server'             # the verified key holder that sends account pseudonyms


ACCT_RE = None


def valid_acct(p) -> bool:
    """A pseudonym: 16–80 chars of [A-Za-z0-9_.:-] — long enough to be a hash, cannot be an address."""
    global ACCT_RE
    if ACCT_RE is None:
        import re
        ACCT_RE = re.compile(r'[A-Za-z0-9_.:\-]{16,%d}' % ACCT_MAX)
    return isinstance(p, str) and ACCT_RE.fullmatch(p) is not None


def acct_hash(pseudonym: str, holder: str | None = None) -> bytes:
    """What we keep of an account: a salted hash of the pseudonym the key holder sends (never the value).
    Scoped by the key holder, so one holder cannot read, claim or erase another holder's accounts."""
    import hashlib
    import hmac
    v = f'a:{holder}:{pseudonym}' if holder else 'a:' + pseudonym
    return hmac.new(pf._salt(), v.encode(), hashlib.blake2s).digest()[:12]


def _holders() -> list:
    return [None] + sorted({DEFAULT_HOLDER} | {v for v in pf.reporter_keys().values() if pf.SELF_MARK not in v})


MASK_NAMES = {0: '-', tu.MASK_SPECIES: 'species', tu.MASK_ALL: 'all', tu.MASK_SPECIES | tu.MASK_ALL: 'all'}
NEAR_M, NEAR_S = 300, 48 * 3600          # "possibly the same person": other hashes reporting this close in space and time


def _conn():
    c = tu._conn()
    if getattr(pf._TLS, 'tm_c', None) is not c:
        c.execute(SCHEMA)
        if 'acct' not in {r[1] for r in c.execute('PRAGMA table_info(rmod)')}:
            try:
                c.execute('ALTER TABLE rmod ADD COLUMN acct BLOB')
            except Exception:  # noqa: BLE001 — another process won the race
                pass
        pf._TLS.tm_c = c
    return c


def _machine() -> set:
    m = {tu.system_reporter(s) for s in tu.SYSTEM_SOURCES} | {b''}
    try:
        m |= {bytes(r[0]) for r in _conn().execute(
            "SELECT DISTINCT reporter FROM report WHERE source IN (SELECT id FROM dict WHERE kind='source' AND val LIKE 'auto-%')")}
    except Exception:  # noqa: BLE001
        pass
    return m


def handle(rep: bytes) -> str:
    return bytes(rep).hex()


def resolve(c, names) -> list:
    """handles (hex prefix ≥ 6) and / or group labels → reporter hashes."""
    out = []
    known = {bytes(r[0]) for r in c.execute('SELECT DISTINCT reporter FROM tree_ver')} \
        | {bytes(r[0]) for r in c.execute('SELECT DISTINCT reporter FROM report')} \
        | {bytes(r[0]) for r in c.execute('SELECT reporter FROM rmod')}
    known -= _machine()
    for n in names:
        g = [bytes(r[0]) for r in c.execute('SELECT reporter FROM rmod WHERE grp=? OR acct IN (%s)'
                                                  % ','.join('?' * len(_holders())), (n, *(acct_hash(n, h) for h in _holders())))]
        if not g:
            p = n.lower()
            g = [k for k in known if len(p) >= 6 and k.hex().startswith(p)]
            if len(g) != 1:
                raise SystemExit(f'{n}: {"no" if not g else "several"} contributor(s) / no such group')
        out += [k for k in g if k not in out]
    return out


def contributors(days: float | None = None) -> list:
    """One dict per contributor hash, newest activity first."""
    c = _conn()
    mach = _machine()
    since = int(time.time() - days * 86400) if days else 0
    mod = {bytes(r['reporter']): r for r in c.execute('SELECT * FROM rmod')}
    by: dict = {}
    rows = c.execute('SELECT rev, ts, x, y, kg, op, sp, uh_dm, dbh_mm, reporter, source, org, meta, note FROM tree_ver '
                     'WHERE length(reporter) = 8 AND op != 1 OR (op = 1 AND reporter NOT IN (%s)) ORDER BY rev'
                     % ','.join('?' * len(mach)), tuple(mach)).fetchall()
    trees: dict = {}                         # (kg, x, y) -> {reporter: latest species}
    for r in rows:
        rep = bytes(r['reporter'])
        if rep in mach:
            continue
        d = by.setdefault(rep, {'rep': rep, 'first': r['ts'], 'last': r['ts'], 'versions': 0, 'species': 0, 'measures': 0, 'adds': 0,
                                'retracts': 0, 'undos': 0, 'notes': 0, 'states': 0, 'trees': set(), 'kgs': set(), 'taxa': {},
                                'src': set(), 'org': set(), 'ts': [], 'pts': [], 'flags': 0, 'flag_claims': {}})
        d['last'] = max(d['last'], r['ts']); d['versions'] += 1; d['ts'].append(r['ts'])
        d['trees'].add((r['kg'], r['x'], r['y'])); d['kgs'].add(r['kg']); d['pts'].append((r['x'], r['y'], r['ts']))
        if r['source']:
            d['src'].add(pf._dict_val(r['source']))
        if r['org']:
            d['org'].add(pf._dict_val(r['org']))
        if r['op'] == tu.OP_RETRACT:
            d['retracts'] += 1; trees.get((r['kg'], r['x'], r['y']), {}).pop(rep, None)
        elif r['op'] == tu.OP_MODERATE:
            d['undos'] += 1
        else:
            d['adds'] += r['op'] == tu.OP_ADD
            if r['sp'] is not None:
                t = pf._dict_val(r['sp']); d['species'] += 1; d['taxa'][t] = d['taxa'].get(t, 0) + 1
                trees.setdefault((r['kg'], r['x'], r['y']), {})[rep] = r['sp']
            d['measures'] += r['uh_dm'] is not None or r['dbh_mm'] is not None
            d['notes'] += bool(r['note']); d['states'] += bool(((r['meta'] or 0) >> tu.ST_SHIFT) & 15)
    for r in c.execute('SELECT reporter, ts, ts_last, n, claim, kg, source FROM report'):
        rep = bytes(r['reporter'])
        if rep in mach:
            continue
        d = by.setdefault(rep, {'rep': rep, 'first': r['ts'], 'last': r['ts_last'], 'versions': 0, 'species': 0, 'measures': 0, 'adds': 0,
                                'retracts': 0, 'undos': 0, 'notes': 0, 'states': 0, 'trees': set(), 'kgs': set(), 'taxa': {},
                                'src': set(), 'org': set(), 'ts': [], 'pts': [], 'flags': 0, 'flag_claims': {}})
        d['first'] = min(d['first'], r['ts']); d['last'] = max(d['last'], r['ts_last']); d['flags'] += 1
        cl = pf._dict_val(r['claim']); d['flag_claims'][cl] = d['flag_claims'].get(cl, 0) + 1
        if r['source']:
            d['src'].add(pf._dict_val(r['source']))
    # trees where this contributor's species differs from somebody else's
    for votes in trees.values():
        if len(set(votes.values())) > 1:
            for rep in votes:
                by[rep]['conflicts'] = by[rep].get('conflicts', 0) + 1
    out = []
    for rep, d in by.items():
        if d['last'] < since:
            continue
        m = mod.get(rep)
        ts = sorted(d['ts'])
        burst = max((sum(1 for u in ts if t <= u < t + 300) for t in ts), default=0)
        near = sorted({o['rep'].hex()[:8] for o in by.values() if o['rep'] != rep and any(
            abs(a[0] - b[0]) <= NEAR_M and abs(a[1] - b[1]) <= NEAR_M and abs(a[2] - b[2]) <= NEAR_S
            for a in d['pts'][:200] for b in o['pts'][:200])})
        out.append({'handle': rep.hex(), 'group': m['grp'] if m else None, 'acct': ('a:' + bytes(m['acct']).hex()[:6]) if m and m['acct'] else None, 'mask': MASK_NAMES[m['mask'] & 3] if m else '-',
                    'mod_note': m['note'] if m else None, 'by': sorted(d['org']), 'sources': sorted(d['src']),
                    'first': d['first'], 'last': d['last'], 'versions': d['versions'], 'trees': len(d['trees']),
                    'kgs': sorted(f'{k:05d}' for k in d['kgs']), 'species': d['species'], 'taxa': d['taxa'],
                    'measures': d['measures'], 'adds': int(d['adds']), 'retracts': d['retracts'], 'undos': d['undos'],
                    'notes': d['notes'], 'states': d['states'], 'flags': d['flags'], 'flag_claims': d['flag_claims'],
                    'conflicts': d.get('conflicts', 0), 'burst_5min': burst, 'near': near})
    out.sort(key=lambda d: -d['last'])
    return out


def show(names) -> list:
    """Every tree version of the contributors / groups, oldest first (with lon / lat)."""
    c = _conn()
    reps = resolve(c, names)
    tf = tu._tf('EPSG:3035', 'EPSG:4326')
    out = []
    for r in c.execute('SELECT rev, ts, x, y, kg, op, sp, h_dm, uh_dm, dbh_mm, reporter, ref, note, meta FROM tree_ver WHERE reporter IN (%s) '
                       'ORDER BY rev' % ','.join('?' * len(reps)), reps):
        lon, lat = tf.transform(r['x'], r['y'])
        st = ((r['meta'] or 0) >> tu.ST_SHIFT) & 15
        cur = tu.tree_at(r['kg'], r['x'], r['y']) or {}
        out.append({'rev': r['rev'], 'ts': r['ts'], 'handle': bytes(r['reporter']).hex()[:8], 'kg': f"{r['kg']:05d}",
                    'lon': round(lon, 6), 'lat': round(lat, 6), 'op': 'undo' if r['op'] == 3 else tu.OP_NAMES[r['op']],
                    'taxon': pf._dict_val(r['sp']) if r['sp'] else None, 'apex_h_m': None if r['h_dm'] is None else r['h_dm'] / 10,
                    'h_m': None if r['uh_dm'] is None else r['uh_dm'] / 10, 'dbh_cm': None if r['dbh_mm'] is None else r['dbh_mm'] / 10,
                    'state': pf.TREE_STATES[st - 1] if 0 < st < 15 else None, 'note': r['note'], 'ref': r['ref'],
                    'shown_now': cur.get('taxon')})
    return out


def _touch(c, reps, now) -> int:
    """One moderation row (no ref: hides nothing by itself) per tree the contributors wrote on → new rev."""
    trees = c.execute('SELECT DISTINCT kg, x, y FROM tree_ver WHERE reporter IN (%s)' % ','.join('?' * len(reps)), reps).fetchall()
    c.executemany('INSERT INTO tree_ver(ts, x, y, kg, op, reporter) VALUES (?,?,?,?,3,?)',
                  [(now, t['x'], t['y'], t['kg'], b'') for t in trees])
    return len(trees)


def set_mask(names, bits: int, note: str | None = None, dry: bool = False) -> dict:
    c = _conn()
    reps = resolve(c, names)
    now = int(time.time())
    out = {'contributors': [r.hex()[:8] for r in reps], 'mask': MASK_NAMES[bits & 3], 'changed': 0, 'trees': 0}
    with pf._LOCK:
        cur = {bytes(r[0]): r[1] for r in c.execute('SELECT reporter, mask FROM rmod')}
        ch = [r for r in reps if cur.get(r, 0) != bits]
        out['changed'] = len(ch)
        if dry or not ch:
            out['trees'] = len(c.execute('SELECT DISTINCT kg, x, y FROM tree_ver WHERE reporter IN (%s)' % ','.join('?' * len(ch)), ch).fetchall()) if ch else 0
            return out
        c.execute('BEGIN IMMEDIATE')
        try:
            for r in ch:
                c.execute('INSERT INTO rmod(reporter, mask, ts, note) VALUES (?,?,?,?) ON CONFLICT(reporter) DO UPDATE SET '
                          'mask=excluded.mask, ts=excluded.ts, note=COALESCE(excluded.note, rmod.note)', (r, bits, now, note))
            out['trees'] = _touch(c, ch, now)
            c.execute('COMMIT')
        except Exception:
            c.execute('ROLLBACK')
            raise
    return out


def set_group(name: str | None, names, note: str | None = None) -> dict:
    c = _conn()
    reps = resolve(c, names)
    now = int(time.time())
    with pf._LOCK:
        for r in reps:
            c.execute('INSERT INTO rmod(reporter, grp, ts, note) VALUES (?,?,?,?) ON CONFLICT(reporter) DO UPDATE SET '
                      'grp=excluded.grp, note=COALESCE(excluded.note, rmod.note)', (r, name, now, note))
        c.commit()
    return {'group': name, 'contributors': [r.hex()[:8] for r in reps]}


def link(acct: str | None, names, holder: str | None = DEFAULT_HOLDER) -> dict:
    """By hand: attach reporter hashes to an account pseudonym of ``holder`` (None = erase the link)."""
    if acct is not None and not valid_acct(acct):
        raise SystemExit(f'acct: a pseudonymous key id, 16–{ACCT_MAX} chars of [A-Za-z0-9_.:-], never an address')
    c = _conn()
    reps = resolve(c, names)
    h = acct_hash(acct, holder) if acct is not None else None
    _set_acct(c, reps, h)
    return {'acct': ('a:' + h.hex()[:6]) if h else None, 'contributors': [r.hex()[:8] for r in reps]}


def _set_acct(c, reps, h) -> int:
    now = int(time.time()); n = 0
    with pf._LOCK:
        for r in reps:
            n += c.execute('INSERT INTO rmod(reporter, acct, ts) VALUES (?,?,?) ON CONFLICT(reporter) DO UPDATE SET '
                           'acct=excluded.acct WHERE rmod.acct IS NOT excluded.acct', (r, h, now)).rowcount
        c.execute('DELETE FROM rmod WHERE acct IS NULL AND grp IS NULL AND mask = 0')      # nothing left to remember
        c.commit()
    return n


# === SECTION: the wire (POST /flags "account", GET|DELETE /flags/account — verified key holders only) ===

_seen: dict = {}                              # (reporter hash) -> acct hash already stored: no DB touch per report
_SEEN_MAX = 50_000


def link_wire(holder: str, pseudonym: str, reps) -> int:
    """Called by POST /api/v1/flags for a verified key holder: the reporters of this call belong to
    the holder's account ``pseudonym``.  Idempotent; one tiny write the first time a browser is seen."""
    h = acct_hash(pseudonym, holder)
    todo = [r for r in set(reps) if _seen.get(r) != h]
    if not todo:
        return 0
    c = _conn()
    have = {bytes(r[0]) for r in c.execute('SELECT reporter FROM rmod WHERE acct=?', (h,))}
    n = _set_acct(c, [r for r in todo if r not in have], h) if any(r not in have for r in todo) else 0
    if len(_seen) > _SEEN_MAX:
        _seen.clear()
    for r in todo:
        _seen[r] = h
    return n


def usage(holder: str, pseudonym: str, since: int | None = None, until: int | None = None) -> dict:
    """Counts of what the account's linked browsers reported (accounting).  No positions, no handles."""
    c = _conn()
    reps = [bytes(r[0]) for r in c.execute('SELECT reporter FROM rmod WHERE acct=?', (acct_hash(pseudonym, holder),))]
    out = {'linked': bool(reps), 'reporters': len(reps), 'tree_versions': 0, 'species': 0, 'measures': 0, 'added': 0,
           'retracted': 0, 'trees': 0, 'flags': 0, 'reports': 0, 'first_ts': None, 'last_ts': None,
           'since': since, 'until': until}
    if not reps:
        return out
    lo, hi = int(since or 0), int(until or 1 << 40)
    q = ','.join('?' * len(reps))
    r = c.execute('SELECT COUNT(*) FILTER (WHERE op IN (0,1)), COUNT(*) FILTER (WHERE op IN (0,1) AND sp IS NOT NULL), '
                  'COUNT(*) FILTER (WHERE op IN (0,1) AND (uh_dm IS NOT NULL OR dbh_mm IS NOT NULL)), '
                  'COUNT(*) FILTER (WHERE op = 1), COUNT(*) FILTER (WHERE op = 2), '
                  "COUNT(DISTINCT kg || ':' || x || ':' || y), MIN(ts), MAX(ts) "
                  f'FROM tree_ver WHERE reporter IN ({q}) AND op IN (0,1,2) AND ts >= ? AND ts < ?', (*reps, lo, hi)).fetchone()
    f = c.execute(f'SELECT COUNT(*), MIN(ts), MAX(ts_last) FROM report WHERE reporter IN ({q}) AND ts >= ? AND ts < ?',
                  (*reps, lo, hi)).fetchone()
    ts = [t for t in (r[6], r[7], f[1], f[2]) if t is not None]
    out.update(tree_versions=r[0], species=r[1], measures=r[2], added=r[3], retracted=r[4], trees=r[5], flags=f[0],
               reports=r[0] + r[4] + f[0], first_ts=min(ts) if ts else None, last_ts=max(ts) if ts else None)
    return out


def unlink_wire(holder: str, pseudonym: str) -> int:
    """Erase an account link (account deleted at the key holder).  Reports stay, as anonymous as before."""
    c = _conn()
    h = acct_hash(pseudonym, holder)
    reps = [bytes(r[0]) for r in c.execute('SELECT reporter FROM rmod WHERE acct=?', (h,))]
    for r in reps:
        _seen.pop(r, None)
    return _set_acct(c, reps, None) if reps else 0


def status() -> dict:
    c = _conn()
    mach = tuple(_machine())
    q = ','.join('?' * len(mach))
    now = int(time.time())
    r = c.execute(f'SELECT COUNT(DISTINCT reporter), COUNT(*), SUM(sp IS NOT NULL), COUNT(DISTINCT reporter) FILTER (WHERE ts > ?), '
                  f'COUNT(*) FILTER (WHERE ts > ?), MAX(ts) FROM tree_ver WHERE reporter NOT IN ({q})', (now - 7 * 86400, now - 7 * 86400, *mach)).fetchone()
    m = c.execute('SELECT COUNT(*) FILTER (WHERE mask != 0), COUNT(DISTINCT grp), COUNT(DISTINCT acct) FROM rmod').fetchone()
    return {'contributors': r[0], 'versions': r[1], 'species_versions': r[2] or 0, 'contributors_7d': r[3], 'versions_7d': r[4],
            'last': r[5], 'masked': m[0], 'groups': m[1], 'accounts': m[2]}


def text_line() -> str:
    s = status()
    age = '-' if not s['last'] else f"{(time.time() - s['last']) / 3600:.1f}h"
    return (f"tree_mod: contributors {s['contributors']} ({s['contributors_7d']} in 7d) · versions {s['versions']} "
            f"({s['versions_7d']} in 7d, species {s['species_versions']}) · last {age} ago · masked {s['masked']} · groups {s['groups']} · accounts {s['accounts']}"
            f" · python3 tree_mod.py --list")


def _t(ts) -> str:
    return time.strftime('%m-%d %H:%M', time.gmtime(ts))


def main(argv) -> int:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--list', action='store_true'); ap.add_argument('--days', type=float)
    ap.add_argument('--show', nargs='+'); ap.add_argument('--group', nargs='+', metavar=('NAME', 'H'))
    ap.add_argument('--link', nargs='+', metavar=('ACCT', 'H')); ap.add_argument('--holder', default=DEFAULT_HOLDER)
    ap.add_argument('--unlink', nargs='+')
    ap.add_argument('--ungroup', nargs='+'); ap.add_argument('--mask', nargs='+'); ap.add_argument('--unmask', nargs='+')
    ap.add_argument('--all', action='store_true', help='mask everything the contributor said about trees, not only species')
    ap.add_argument('--note'); ap.add_argument('--dry', action='store_true'); ap.add_argument('--status', action='store_true')
    a = ap.parse_args(argv)
    if a.group:
        print(set_group(a.group[0], a.group[1:], a.note))
    if a.link:
        print(link(a.link[0], a.link[1:], a.holder))
    if a.unlink:
        print(link(None, a.unlink))
    if a.ungroup:
        print(set_group(None, a.ungroup))
    if a.mask:
        print(set_mask(a.mask, tu.MASK_ALL if a.all else tu.MASK_SPECIES, a.note, a.dry))
    if a.unmask:
        print(set_mask(a.unmask, 0, a.note, a.dry))
    if a.show:
        for r in show(a.show):
            print(f"{r['rev']:>7} {_t(r['ts'])} {r['handle']} {r['kg']} {r['lon']:.6f},{r['lat']:.6f} {r['op']:<8} "
                  + ' '.join(f'{k}={r[k]!r}' for k in ('taxon', 'apex_h_m', 'h_m', 'dbh_cm', 'state', 'note', 'ref', 'shown_now') if r[k] is not None))
    if a.status:
        print(text_line())
    if a.list or not (a.link or a.unlink or a.group or a.ungroup or a.mask or a.unmask or a.show or a.status):
        print('handle   grp/acct  mask    last(UTC)   first       ver trees sp  meas add flags confl burst kgs / taxa / by / near')
        for d in contributors(a.days):
            taxa = ','.join(f'{k}×{v}' for k, v in sorted(d['taxa'].items(), key=lambda kv: -kv[1]))
            print(f"{d['handle'][:8]} {(d['group'] or d['acct'] or '-'):<9} {d['mask']:<7} {_t(d['last'])} {_t(d['first'])} {d['versions']:>4} {d['trees']:>5} "
                  f"{d['species']:>3} {d['measures']:>4} {d['adds']:>3} {d['flags']:>5} {d['conflicts']:>5} {d['burst_5min']:>5} "
                  f"{','.join(d['kgs'][:4])}{'…' if len(d['kgs']) > 4 else ''} / {taxa or '-'} / {','.join(d['by'] or d['sources']) or '-'}"
                  f"{' / near ' + ','.join(d['near']) if d['near'] else ''}")
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
