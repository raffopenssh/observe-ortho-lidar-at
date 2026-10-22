# Public flags — "this looks wrong" for everything the public tier serves

`public_flags.py` · Blueprint `flags_bp` mounted by `public_api.py` (port 8100) ·
contract in `public_docs.FLAGS_TXT` (`/api/v1/docs/llm.txt#flags`) ·
test `python3 tests/test_public_flags.py` (also enforces the cells vocabulary rule
on this module + its docs block).

## Why / scope

umfeld-at's landing-page glyph paints our *observed* cover group per cell
(`/ne/glyph/tile?v=obs`) and wanted a way to flag awkward cells; siedler-style
clients want the same for structure heights and giant trees. The old
`/api/v1/flags*` + `/feedback` on the private tier are object-ref keyed
(`feedback_db.py`, QA rules in `quality_flags.py`) and parcel-era — **not**
reusable publicly. This is the public, parcel-free replacement, scoped to
every public layer (`LAYERS`), not just cells.

Hard rules (same as `docs/ne-public-api.md §0`): subjects are **H3 res-12
cells or KG codes only**; no object ids, no geometry, no identifiers; the
vocabulary test greps this module + `FLAGS_TXT` for `cells_api.FORBIDDEN_WORDS`.
Flags never change what we serve — they are QA evidence (`status` is ours).

## Storage (`data/public_flags.sqlite`, WAL)

One `report` row per `(skey, layer, claim, reporter)`; a repeat **folds**
(`n += 1`, `ts_last`, newer `expected`/`note` win, first `observed`/`epoch`
kept, a `fixed`/`stale` row re-opens). `skey` = cell id (res 12) or `-kg`.
Strings (`layer`, `claim`, `source`) live in a `dict` table; coords are int32
micro-degrees; `reporter` is an 8-byte HMAC(salt, id|IP) — `data/public_flags.salt`
(0600, auto-created), raw ids/IPs are never stored. `observed`/`expected`
compact JSON ≤ 512/256 B, `note` ≤ 280 chars (control chars stripped).
Row ≈ 150–300 B → 1 M rows ≈ 250 MB worst case. Pruning every 500 writes:
closed rows (rejected/fixed/stale) older than 180 d, and the oldest non-open
rows past `MAX_ROWS_DB = 2 M`. Indexes: `cell`, `kg`, `(lat_e6, lon_e6)`
(bbox), `ts_last`, `status`.

**Snapshot**: when a reporter omits `observed` and the cell is on disk we
store what we serve *now* (`snapshot_cell`: dominant group + share, h_max,
trees_n, structures_n, consistency; nearest apex/structure within 30 m for
the trees/structures layers) plus the product epoch — one cached chunk read
via `cells_api.select`. Reads compare the row's epoch with the KG's current
header epoch (`cells_api.header`, LRU) and mark `superseded: true`.

## Endpoints (all `/api/v1/flags…`, public tier)

| | |
|---|---|
| `POST /flags` | one report or `{reports:[…]}` ≤ 50 → 201 `{id, folded, n, status, subject, epoch, served, snapshot?, tally, warnings?}` |
| `GET /flags?bbox=\|cell=\|ids=\|lon=&lat=&k=\|kg=` | grouped per subject (default) or `group=report`; filters `layer`, `claim`, `status`, `since`, `limit`, `notes=0`; `format=json\|columns` |
| `GET /flags/<id>` | one row + tally |
| `GET /flags/heat?res=6..12[&status=all][&kg=][&layer=]` | national roll-up `{cells, n, open, reporters}` — 5-min in-process cache, cleared on write |
| `GET /flags/stats` | totals, by layer/claim/source/status, 24h/7d, top KGs, db bytes, rate-limit counters |
| `GET /flags/dict` | enums + limits |
| `POST /flags/<id>/status` | **admin token** (`api_tiers.token_valid`): `open\|confirmed\|rejected\|fixed\|stale` + note; 404 without token |
| `GET /cells/<h3>?flags=1` | cells dossier with `flags: tally\|null` inline (max-age 60, tally in ETag) |

Bandwidth/CPU: reads are ETag'd (sha of body) + gzip'd by `public_api`'s
after_request; grouped rows are ~120 B each; heat is a few KB nationally.
Limits: POST 1 r/s sustained / burst 10 per client (batch cost = √n),
500 reports/client/day (in-memory, per UTC day), body ≤ 32 KB; GET 5 r/s /
burst 20; bbox span ≤ 0.05° (use `/heat` for overviews). Env overrides
`FLAGS_POST_RATE_PER_S[_BURST]`, `FLAGS_GET_RATE_PER_S[_BURST]`.

## Operating it

```bash
curl -s 'localhost:8100/api/v1/flags/stats?pretty=1'
curl -s 'localhost:8100/api/v1/flags?kg=63349&status=all&group=report&pretty=1'
# triage: mark a row
curl -s -XPOST -H "X-Admin-Token: $(cat data/admin_token)" -H 'Content-Type: application/json' \
  localhost:8100/api/v1/flags/12/status -d '{"status":"confirmed","note":"mast, not a tree — rule added"}'
sqlite3 data/public_flags.sqlite 'SELECT status, COUNT(*) FROM report GROUP BY 1'
```

Not yet wired (next steps, in order of value): a `flags:` line in
`/process.txt` (open count, 24 h rate, top KGs); feeding `confirmed`
`too_tall`/`wrong_type` rows into `quality_flags` rules / RF retraining
triggers; auto-`stale` when a KG's product epoch moves (today it is a
read-side `superseded` marker only).
