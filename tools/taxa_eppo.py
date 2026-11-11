#!/usr/bin/env python3
"""Regenerate the ``_EPPO`` block of tree_taxa.py from Wikidata (P225 taxon
name → P3031 EPPO code).  One SPARQL request; run by hand after adding rows,
review the diff, bump ``tree_taxa.REV``.

    python3 tools/taxa_eppo.py            # print what would change
    python3 tools/taxa_eppo.py --write
"""
import json
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import tree_taxa as tt  # noqa: E402


def fetch(names):
    vals = ' '.join(json.dumps(n) for n in names)
    q = 'SELECT ?n ?c WHERE { VALUES ?n { %s } ?t wdt:P225 ?n ; wdt:P3031 ?c . }' % vals
    req = urllib.request.Request('https://query.wikidata.org/sparql', data=urllib.parse.urlencode(
        {'query': q, 'format': 'json'}).encode(), headers={'User-Agent': 'srtm-lidar-at taxa dict (one-off)'})
    out: dict = {}
    for b in json.load(urllib.request.urlopen(req, timeout=90))['results']['bindings']:
        out.setdefault(b['n']['value'], set()).add(b['c']['value'])
    return out


def main():
    want = {}
    for row in tt.TABLE:
        tx = row[0]
        if tx in tt.GROUPS:
            continue
        want[tx] = tx
        if ' x ' in tx:
            want[tx.replace(' x ', ' × ')] = tx
    syn = {}                                     # Latin synonyms in alt: used only where the accepted name has no code
    for row in tt.TABLE:
        for a in row[6]:
            if re.fullmatch(r'[A-Z][a-z]+ (x )?[a-z]+', a):
                syn[a] = row[0]
                syn[a.replace(' x ', ' × ')] = row[0]
    got = fetch(sorted(set(want) | set(syn)))
    for name in [n for n in got if n in syn and n not in want]:
        tx = syn[name]
        if not any(want.get(n) == tx for n in got):
            print('via synonym:', tx, '<-', name, sorted(got[name]))
            want[name] = tx
        else:
            del got[name]
    eppo: dict = {}
    for name, codes in got.items():
        tx = want[name]
        codes = codes | set(eppo.get(tx, ()))
        genus = ' ' not in tx
        # species codes are 5 letters, genus codes "1" + 3–4 letters + "G"; anything else is not ours
        ok = sorted(c for c in codes if re.fullmatch(r'1[A-Z0-9]{3,4}G' if genus else r'[A-Z][A-Z0-9]{4,5}', c))
        if len(ok) == 1:
            eppo[tx] = ok
        elif ok:
            print('ambiguous, skipped:', tx, ok)
    missing = [t for t in want.values() if t not in eppo]
    print(f'{len(eppo)} codes, {len(set(missing))} without: {sorted(set(missing))}')
    block = '_EPPO: dict = {\n' + ''.join(f'    {k!r}: {tuple(v)!r},\n' for k, v in sorted(eppo.items())) + '}\n'
    if '--write' in sys.argv:
        p = ROOT / 'tree_taxa.py'
        s = p.read_text()
        s = re.sub(r'(# --- EPPO BEGIN ---\n).*?(# --- EPPO END ---)', lambda m: m.group(1) + block + m.group(2), s, flags=re.S)
        p.write_text(s)
        print('written')
    else:
        print(block)


if __name__ == '__main__':
    main()
