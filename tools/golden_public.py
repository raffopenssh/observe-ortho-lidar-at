#!/usr/bin/env python3
"""golden_public — snapshot / compare the surfaces that must NOT change
during the Kataster purge (docs/kataster-purge.md).

  python3 tools/golden_public.py capture  data/golden/pre.json
  python3 tools/golden_public.py compare  data/golden/pre.json [--token-file data/admin_token]
  python3 tools/golden_public.py kgs      # print the sampled KG codes

Samples: a fixed, seeded set of KG codes from the search index (per
product_version bucket so v1 / v2.3 / v2.4 KGs are all covered) and the
bboxes derived from them.  For every request the response is normalised
(volatile keys dropped, floats rounded) and hashed.  ``compare`` exits 1 on
any difference and prints the first diverging key path per request.
Public tier (:8100) needs no token; the surviving private routes (:8000)
are called with the admin token.  Read-only — safe on the live service.
"""
from __future__ import annotations
import json, random, sqlite3, sys, hashlib, time, urllib.request, urllib.error
from pathlib import Path

PUB = 'http://127.0.0.1:8100'
PRIV = 'http://127.0.0.1:8000'
N_PER_BUCKET = 6
SEED = 20261013
VOLATILE = {'ts', 'generated_at', 'rendered_at', 'served_at', 'rss_mb', 'pid', 'uptime_s',
            'cache', 'x_cache', 'elapsed_ms', 'took_ms', 'rebuilt_at', 'index_rebuilt_at',
            'fetched_at', 'age_s', 'duration_ms', 'history', '_links', 'zenodo_json_url', 'zenodo_light_gpkg_url',
            'zenodo_full_gpkg_url', 'zenodo_json_size', 'zenodo_light_gpkg_size', 'zenodo_full_gpkg_size',
            'parcel_id'}  # parcel_id: purged field (docs/kataster-purge.md Step 2) — expected to vanish


def sample_kgs() -> list[str]:
    c = sqlite3.connect('file:data/search_index.db?mode=ro', uri=True)
    rows = c.execute('SELECT kg_code, product_version, min_lon, min_lat, max_lon, max_lat FROM kg '
                     'WHERE min_lon IS NOT NULL').fetchall()
    by: dict[str, list] = {}
    for r in rows:
        by.setdefault(r[1] or 'none', []).append(r)
    rnd = random.Random(SEED)
    out = []
    for v in sorted(by):
        pool = sorted(by[v])
        out += rnd.sample(pool, min(N_PER_BUCKET, len(pool)))
    return out


def requests_for(kgs) -> list[tuple[str, str]]:
    req = [('pub', '/api/v1/status'), ('pub', '/api/v1/kgs'), ('pub', '/api/v1/cells/dict'),
           ('pub', '/llm.txt'), ('pub', '/api/v1/docs/llm.txt'), ('pub', '/api/v1/license'),
           ('priv', '/api/v1/ping')]  # /process.txt is live telemetry, never byte-stable
    for code, _v, w, s, e, n in kgs:
        cw, cs = (w + e) / 2, (s + n) / 2
        bb = f'{cw-0.004:.4f},{cs-0.004:.4f},{cw+0.004:.4f},{cs+0.004:.4f}'
        req += [('pub', f'/api/v1/kg/{code}'), ('pub', f'/llm/kg/{code}'),
                ('pub', f'/api/v1/landscape?bbox={bb}'), ('pub', f'/api/v1/landscape?bbox={bb}&include=cells'),
                ('pub', f'/api/v1/trees/bbox?bbox={bb}'), ('pub', f'/api/v1/buildings/bbox?bbox={bb}'),
                ('pub', f'/api/v1/landmarks/bbox?bbox={bb}'), ('pub', f'/api/v1/cells?bbox={bb}&layers=obs,trees,structures'),
                ('pub', f'/api/v1/kg/{code}/cells?format=json'),
                ('priv', f'/api/v1/kg/{code}/landcover'),  # no /segments: lazily downloads the GPKG from Zenodo
                ('priv', f'/api/v1/trees/bbox?bbox={bb}'),
                ('priv', f'/api/v1/flags?kg={code}&kind=top_tree&limit=200')]
    return req


def norm(o):
    if isinstance(o, dict):
        return {k: norm(v) for k, v in sorted(o.items()) if k not in VOLATILE}
    if isinstance(o, list):
        return [norm(x) for x in o]
    if isinstance(o, float):
        return round(o, 4)
    return o


def fetch(tier, path, token):
    url = (PUB if tier == 'pub' else PRIV) + path
    r = urllib.request.Request(url, headers={'X-Admin-Token': token} if tier == 'priv' else {})
    try:
        with urllib.request.urlopen(r, timeout=120) as resp:
            body, code = resp.read(), resp.status
    except urllib.error.HTTPError as e:
        body, code = e.read(), e.code
    try:
        j = norm(json.loads(body))
        return {'code': code, 'json': j, 'sha': hashlib.sha256(json.dumps(j, sort_keys=True).encode()).hexdigest()}
    except Exception:
        return {'code': code, 'sha': hashlib.sha256(body).hexdigest(), 'len': len(body)}


def first_diff(a, b, path=''):
    if type(a) != type(b):
        return path or '/'
    if isinstance(a, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a or k not in b:
                return f'{path}/{k}'
            d = first_diff(a[k], b[k], f'{path}/{k}')
            if d:
                return d
    elif isinstance(a, list):
        if len(a) != len(b):
            return f'{path}[len {len(a)}!={len(b)}]'
        for i, (x, y) in enumerate(zip(a, b)):
            d = first_diff(x, y, f'{path}[{i}]')
            if d:
                return d
    elif a != b:
        return path or '/'
    return None


def main():
    if len(sys.argv) < 2:
        print(__doc__); sys.exit(2)
    cmd = sys.argv[1]
    token = Path('data/admin_token').read_text().strip()
    kgs = sample_kgs()
    if cmd == 'kgs':
        for k in kgs:
            print(k[0], k[1])
        return
    reqs = requests_for(kgs)
    if cmd == 'capture':
        out = {'t': time.time(), 'kgs': [k[0] for k in kgs], 'r': {}}
        for tier, p in reqs:
            out['r'][f'{tier} {p}'] = fetch(tier, p, token)
        Path(sys.argv[2]).parent.mkdir(parents=True, exist_ok=True)
        Path(sys.argv[2]).write_text(json.dumps(out))
        print(f'captured {len(reqs)} requests → {sys.argv[2]}')
        return
    if cmd == 'compare':
        ref = json.loads(Path(sys.argv[2]).read_text())
        bad = 0
        for tier, p in reqs:
            k = f'{tier} {p}'
            now, then = fetch(tier, p, token), ref['r'].get(k)
            if then is None:
                print('NEW ', k); continue
            if 'json' in then:  # re-normalise: VOLATILE may have grown since capture
                then = dict(then, json=norm(then['json']))
                then['sha'] = hashlib.sha256(json.dumps(then['json'], sort_keys=True).encode()).hexdigest()
            if now['code'] != then['code']:
                bad += 1; print(f'CODE {then["code"]}→{now["code"]}  {k}'); continue
            if now['sha'] != then['sha']:
                bad += 1
                d = first_diff(then.get('json'), now.get('json')) if 'json' in now else 'body'
                print(f'DIFF {k}  at {d}')
        print(f'{len(reqs)} requests, {bad} differ')
        sys.exit(1 if bad else 0)


if __name__ == '__main__':
    main()
