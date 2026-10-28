# Public cells API — spec (`cells_api.py`, public tier)

Status 2026-10-04: **implemented** (`cells_api.py`, commits 5971290 +
follow-ups) — live on `https://example.invalid/api/v1/cells…`.
See §9 for what was measured and where the implementation deviates.
Companion to umfeld-at's H3 read path (`/k/api/v1/ne/cell|cells|chunk`,
public version pending there). We serve **what we measured** per H3 cell;
umfeld-at serves the statistical land-use layer. Same keys, same query
shapes, same limits → a client zips both by cell id. Nothing here is a
dependency on umfeld-at being fast or reachable.

## 0. Hard rules

1. **Vocabulary.** Public code, docs, JSON keys, error strings and headers
   never contain: cadastre/Kataster, parcel/Grundstück/Gst, footprint,
   outline, GNR/EZ/Grundbuch, owner, address, "declared", "register",
   "lu" (umfeld's layer), `kg_share`, `n_parc`, `bld_cover` (theirs).
   Enforce with a unit test that greps `cells_api.py` + its docs block
   (`tests/test_cells_vocab.py`, word list in `cells_api.FORBIDDEN_WORDS`).
   Our own vocabulary: *cell, cover, structures, trees, terrain, change,
   phenology, consistency*.
2. **Request path reads only `data/ne/*.h3b` + `data/ne/cells.sqlite`**
   (+ `kg` table of `search_index.db` for code→bbox). No Zenodo, BEV,
   umfeld, GPKG, JSON blobs. Not on disk → 404 `ready:false`.
3. **No NEC1 bytes leave this host via the public tier.** The h3b chunk
   holds `shuffle(NEC1)|shuffle(NEO1)|shuffle(NET1)|shuffle(NEB1)`; every
   format we emit is rebuilt from the NEO1/NET1/NEB1 slices only
   (`format=h3b` re-deflates without the NEC1 slice — see §4).
4. **Standalone module.** `cells_api.py` = pure functions + one Flask
   `Blueprint` (`cells_bp`), imports only `ne_h3`, `h3`, `numpy`, `sqlite3`,
   `pyproj`. Mounted by `public_api.py`. No import of `app`, `peer_director`,
   `search_index` (code→bbox goes through a 3-line sqlite read of its own).
   This is what lets us delete `/k` later (§7) without touching this file.

## 1. Data on disk

* `data/ne/<parent>.h3b` — written by `ne_ingest` (exists). Header
  `chunks[]` = one entry per res-10 cell: `r10, off, len, raw_len, cells,
  trees, bld, first`.
* **New** `data/ne/cells.sqlite` (WAL, `synchronous=NORMAL`), owned by
  `ne_ingest` (insert after each successful file write / merge; `DELETE
  WHERE kg=?` before re-insert; `cells_api --rebuild` rescans headers):
  ```sql
  CREATE TABLE chunk (r10 INTEGER NOT NULL, kg TEXT NOT NULL, off INTEGER, len INTEGER,
                      raw_len INTEGER, cells INTEGER, trees INTEGER, bld INTEGER,
                      PRIMARY KEY (r10, kg)) WITHOUT ROWID;
  CREATE TABLE file  (kg TEXT PRIMARY KEY, body_offset INTEGER, cells_n INTEGER, epoch TEXT,
                      product_version TEXT, neo1_digest TEXT, partial INTEGER, mtime REAL,
                      w REAL, s REAL, e REAL, n REAL);          -- bbox from cell centres
  CREATE TABLE meta  (k TEXT PRIMARY KEY, v TEXT);              -- schema=1, rebuilt_at
  ```
  Size: ~45 B/row × 5.6 M res-10 chunks for all of Austria ≈ 250 MB
  (vs ~10 GB corpus). Today: 10 files → a few KB.
* Resolution in the request path: `bbox → h3.polygon_to_cells(res 10)`
  (+ `h3.grid_ring` 1 so border cells whose res-12 children poke out are
  caught) → `SELECT … FROM chunk WHERE r10 IN (…)` (≤ 2 000 ids per
  statement; 0.05° box ≈ 1 500 res-10 cells) → group by `kg` → one
  `open()` per file, `seek`+`read(len)` per chunk, `zlib.decompress`,
  `ne_h3._unshuffle`. Decode NEO1 with a numpy structured dtype mirroring
  `ne_h3.OBS_REC` (no per-row Python); NET1/NEB1 rows via `np.frombuffer(
  '<u4'|'<u8')` + bit ops. Filter res-12 ids to the query (`np.isin`).
* **Dedupe across KGs**: a border res-12 cell appears in every KG it
  touches. Keep the copy from the chunk with the largest `cells` for that
  r10 (ties → lowest kg). Record `kgs[]` in meta so the client sees the
  provenance.
* Caches (process-local, both bounded): inflated chunk LRU 4 096 entries
  (`(kg, r10)` → dict of numpy arrays, ~10 KB each ≈ 40 MB max);
  header LRU 512 (`body_offset`, `epoch`, `summary`). No corpus-wide
  structures in RAM. sqlite connection per thread.

## 2. Endpoints (all GET unless noted, all on `:8100` / `https://example.invalid`)

```
/api/v1/cells?bbox=w,s,e,n            res-12 cells in a box (span ≤ 0.05°, ≤ 16 384 cells → 413)
/api/v1/cells?lon=&lat=[&k=0..3]      cell under a point + k rings
/api/v1/cells?ids=h3,h3,…             ≤ 512 ids (res 12; coarser ids expand to their res-12 children)
POST /api/v1/cells {"ids":[…]}        ≤ 16 384 ids
/api/v1/cells/<h3>                    one cell, all layers (obs + trees + structures), always JSON
/api/v1/cells/chunk/<h3>              res 9 or 10 chunk(s) for bulk clients; format=h3b|bin|json
/api/v1/cells/dict                    field layout, units, code tables, group labels, limits, epoch pin
/api/v1/kg/<code>/cells               whole-KG container (public product file): format=h3b (default) | json summary
/api/v1/landscape?…&include=cells     existing endpoint gains a per-bbox roll-up block (§5)
/api/v1/status                        gains cells{kgs_ready, cells_total, index_rebuilt_at}
```

Common params: `layers=obs,trees,structures` (default `obs`; `/cells/<h3>`
always all), `res=12|11|10` (server-side roll-up, §3), `fields=a,b,c`
(subset of obs field names), `format=json|columns|bin|h3b|csv|geojson`,
`centres=1` (add lon/lat of the metre-snapped EPSG:3035 cell centre),
`pretty=1`. Cross-param limits: `geojson` ≤ 4 096 cells (hex polygons),
`trees`/`structures` layers only at res 12, `h3b` only on `/chunk` and
`/kg/<code>/cells`.

Not-ready answers: `404 {"ready": false, "status": "not_processed" |
"partial", "retry_after_s": 3600, "kgs_missing": […]}` with
`Retry-After`. A bbox that is partly covered answers 200 with
`meta.partial:true` + `meta.kgs_missing` — never a mix of silence and data
without saying so. Split parents still merging (`partial_cells` in the
header) → the covered cells + `meta.partial:true`.

Headers: `ETag` = `"c-" + sha256(sorted neo1_digests of touched files +
canonical query)[:16]`; `If-None-Match` → 304. `Cache-Control: public,
max-age=86400` (products are frozen per epoch; a re-upgrade changes the
digest and therefore the ETag). `X-Cells-Source: disk`, `X-Cells-Partial:
1` when partial, `X-Data-Attribution` + `Link rel=license` as the other
public routes. CORS `*`. gzip ≥ 1 KiB by the existing `_common_headers`
(not for `h3b`, already deflated). Rate hint in docs: ≤ 5 r/s sustained,
burst 20 (no enforcement beyond the proxy's).

## 3. Record → public JSON (one cell, res 12)

```json
{"cell": "8c1f2a8b6c4ddff", "res": 12, "kg": "63330",
 "cover": {"geb": 0.02, "bau": 0, "acker": 0, "gruen": 0.31, "wald": 0.61, "wasser": 0,
           "verkehr": 0.04, "alpen": 0, "sonst": 0.02},
 "structures_cover": 0.02, "structures_n": 1,
 "height": {"mean": 9.5, "p90": 21.0, "max": 27.5},
 "trees": {"n": 14, "h_max": 27.5, "canopy": 0.58},
 "ndvi": {"mean": 0.71, "amp": 0.22}, "phenology": "forest",
 "change": {"dh_m": -0.25, "dtm_m": 0.0, "forest_loss_year": null, "als_years": [2022, 2024]},
 "terrain": {"elev_m": 612, "slope": 14, "slope_p90": 23, "aspect": "SE", "roughness": 1.3, "tri": 0.8, "tpi": -0.5},
 "consistency": "consistent"}
```

* Decode exactly `ne_h3.OBS_FIELDS` with `OBS_UNITS` (255 / -128 / 65535 →
  `null`). `cover` = `obs_lu[9]`/255 in `ne_h3.GROUPS` order (the 9 group
  codes are shared with umfeld-at's layer — keep the codes, label them in
  `/dict`: geb=buildings, bau=construction & parking, acker=cropland,
  gruen=grass/garden/orchard/vineyard/shrub/hedge, wald=trees, wasser=water,
  verkehr=roads & paths, alpen=rock/bare/glacier, sonst=other).
* `consistency` = `ne_h3.VERDICTS` renamed for the public surface:
  `consistent, forest_loss, forest_gain, sealed_new, structure_new,
  green_new, unknown`. `/dict` explains it as "our observation vs. the
  statistical land-use layer of umfeld-at for the same cell (frozen
  epoch), computed when the product was built". The name mapping lives in
  one dict in `cells_api.py` (`CONSISTENCY_PUBLIC`).
* `trees[]` (layer `trees`, res 12 only): `{dx, dy, lon, lat, h_m, crown_m2,
  species, vitality, ndsm_only}` from the 4 B NET1 word (`dx:5 dy:5 h:8
  (0.25 m) crown:6 (log2·5) species:4 vitality:3 flag:1`; codes
  `ne_h3.SPECIES_CODES` / `VITALITY_CODES`). lon/lat = cell centre (3035,
  metre-snapped, `ne_h3.cell_centres_3035`) + (dx, dy) → 4326. `n`
  saturates at 255 per cell; `meta.trees_truncated_cells` counts those.
* `structures[]` (layer `structures`): `{dx, dy, lon, lat, type, h_max_m,
  h_robust_m, area_m2, conf, dh_m}` from the 8 B NEB1 word (`dx:5 dy:5
  h_max:8 h_robust:8 area:16 type:4 conf:4 dh:8`; `ne_h3.BLD_TYPES`).
  These are *our segmented objects* with LiDAR heights — same posture as
  the existing `/buildings/bbox` (centroid + height, no outlines).
* `format=columns`: `{"cells":[…hex], "kg":[…], "cover":[[9 × u8]…],
  "h_mean":[…], …}` raw quantised integers + a `units` block copied from
  `/dict` — ~40 B/cell before gzip, the format for map clients.
* `format=bin`: `cells_n × 41 B` raw NEO1 rows, little-endian, sorted by
  cell id, `Content-Type: application/octet-stream`, header
  `X-Cells-Record: neo1/41` (+ `trees`/`structures` appended as
  `X-Cells-Layout: neo1=N,net1=T,neb1=B` with per-cell counts derivable
  from `tree_n`/`structures_n`). gzip by middleware.
* `format=h3b` (chunk / whole-KG only): `H3B1`-style container **without
  the NEC1 slice**: header JSON (`kg, epoch, product_version, chunks[]`
  with `cells, trees, bld, off, len`, `record_bytes{neo1,net1,neb1}`,
  `chunk_layout: "zlib(shuffle(NEO1)|shuffle(NET1)|shuffle(NEB1))"`,
  `neo1_digest`) + re-deflated chunks (level 6, ~1 ms/chunk; cached per
  `(kg, r10)` in the chunk LRU as bytes). Whole-KG answers are built once
  per `(kg, neo1_digest)` into `data/ne/pub/<kg>.h3b` on first request
  (≤ 1 MB, ~200 ms) and then `send_file`d with Range support —
  the public download of the product without the third-party layer.
* `res=11|10` roll-up (obs only): weighted by res-12 cell count — `cover`,
  `structures_cover`, `ndvi.*`, `terrain.*` (except `aspect` → mode),
  `height.mean` mean; `height.p90` mean of p90s (documented); `height.max`,
  `trees.h_max` max; `trees.n`, `structures_n` sum; `trees.canopy` mean;
  `phenology`/`consistency` → mode + `consistency_hist[7]`; `als_years`
  union; `change.dh_m` mean; `forest_loss_year` max. Returns `n_children`.
  Implemented with `np.add.reduceat` on parent-sorted arrays — one pass.

## 4. Performance / resource targets (measure before merging)

| metric | target | how |
|---|---|---|
| point, k=0, obs | p50 < 4 ms | 1 sqlite PK read + 1 chunk inflate (cached after) |
| bbox 0.01°×0.01° (~330 cells) json | p50 < 12 ms | ~7 chunks |
| bbox 0.05° (~16 k cells) columns | p50 < 80 ms, body < 350 KB gz | ~340 chunks, numpy decode |
| 16 k cells + trees + structures json | < 250 ms, < 2.5 MB gz | — |
| whole-KG h3b | < 5 ms after first build | file send |
| RSS of `srv_public` | +≤ 60 MB over today | bounded LRUs only |
| disk | `cells.sqlite` ≤ 250 MB at 8 440 KGs; `data/ne/pub/` ≤ 1.5 GB (LRU 1 000 files) | — |
| primary egress | h3b/columns default in docs; json capped 16 384 cells | — |

Benchmark script `tools/bench_cells_api.py` (hits localhost:8100, prints
p50/p95 + bytes for the table above). CPU: no Python loop over cells in
any format except `geojson` boundary generation (h3 vectorised
`cells_to_h3shape` where available).

## 5. Integration with existing public routes

* `/api/v1/landscape?include=cells`: block `cells{n, res:12, cover{…mean},
  structures_n, trees_n, canopy, height{mean,p90,max}, consistency_hist,
  phenology_hist, change{dh_m, forest_loss_cells}, partial}` — the res-10
  roll-up code path over the bbox cells. Cheap, answers "what is HERE"
  at cell fidelity (today's block is 25 m grid + sampled trees).
* `/api/v1/kg/<code>`: add `cells: {ready, cells_n, epoch, summary}` from
  the h3b header `summary` (already computed by the processor).
* `/api/v1/trees/bbox` and `/buildings/bbox`: **unchanged** now; once
  `data/ne` covers a KG, these may be answered from NET1/NEB1 (complete
  apex set h ≥ 3 m instead of sampled `kg_trees`) — flag `source:
  "cells"` in meta. Phase 2, behind `cells_api.has_kg(code)`.
* `public_docs.LLM_TXT`: new section "Cells (H3 res 12, ~300 m²)" — a
  copy of §2 + §3 condensed; vocabulary test covers it.

## 6. What this gives siedler / umfeld-at

* siedler (viewport tiles ≈ 0.02°): one `columns` call per tile for a
  heat layer (canopy, structure heights, change, consistency), one
  `/cells/<h3>` on click for the full dossier incl. every tree and
  structure with heights; `k`-rings for "around this point". Join to its
  own lot geometry client-side by cell id — nothing lot-shaped is needed
  from us.
* umfeld-at: its `/context?include=ne` can call `/api/v1/cells?lon&lat`
  (or ingest the posted NEO1 it already has) — identical semantics, so
  `obs` served by either side decodes with the same `/dict`.
* Both: `/kg/<code>/cells` h3b for bulk/offline use (the Zenodo product
  minus the third-party layer; CC BY 4.0, attribution in header).

## 7. Retiring `/k` — DONE 2026-10-13

> Record: `docs/kataster-purge.md`.

Every *landscape* question the hidden tier answered has a public,
cell-keyed equivalent (the handlers in the left column are deleted;
`api_tiers.landscape_route_retired` answers 410 with `see:
/api/v1/docs/llm.txt#cells`):

| former `/k` route | public replacement |
|---|---|
| `/api/v1/parcel/<id>`, `/parcel/<id>/detail` | `/api/v1/cells?lon&lat&k=1` or ids of the client's own geometry |
| `/api/v1/query/parcels?bbox` | `/api/v1/cells?bbox&layers=obs,trees` |
| `/api/v1/parcels/batch`, `/parcels/landscape`, `/query/nature`, `/query/compound` | client-side over `columns` (all filter fields are per cell); `consistency` replaces `new_buildings` |
| `/api/v1/kg/<c>/buildings`, `/kg/<c>/new_buildings`, `/kg/<c>/infrastructure` | `/api/v1/cells?bbox=<kg bbox>&layers=structures` / `/kg/<c>/cells` |
| `/api/v1/kg/<c>/profile` | `/api/v1/kg/<c>` (`cells.summary`) |
| `/llm/kg/<c>?parcels=1`, `/api/v1/flags` | gone |
| `/api/v1/lookup`, `/cadastre/*` relays | clients call umfeld-at directly |

`cells_api` never imports from `app.py`; `landscape_query.py` is the only
shared helper. `public_docs.LLM_TXT` is the only client-facing doc.

## 8. Not in scope / later

* HA: `data/ne/` + `cells.sqlite` are primary-only today (shadow does not
  replicate them; `director_ha.SNAPSHOT_FILES`). Add `data/ne/` to the
  shadow sync once the corpus is > a few GB — separate task.
* PNG hex tiles, vector tiles (MVT) of cells — `columns` + client h3 is
  enough for siedler's Leaflet; revisit on request.
* Serving NET1/NEB1 through umfeld-at — stays on our side (their
  contract).

## 9. Implementation notes (2026-10-04)

* Files: `cells_api.py` (blueprint + index + formats), `tests/test_cells_vocab.py`
  (vocabulary grep over source, `public_docs.CELLS_TXT`, `/cells/dict`, one cell
  record, one public h3b header), `tools/bench_cells_api.py` (§4 table against
  `:8100`). Index maintenance: `python3 cells_api.py --rebuild` or
  `python3 ne_ingest.py --rebuild`; `ne_ingest` calls `cells_api.index_file()`
  after every parent write / merge (`_index_cells`, best-effort).
* **Measured on the primary** (`srv_public`, gunicorn 2×24, 19 KGs on disk):

  | metric | target | measured p50 |
  |---|---|---|
  | point k=0 obs json | < 4 ms | 2–3 ms |
  | 307-cell box json (0.0035°) | — | 7 ms |
  | 0.01°×0.01° box json (**2 733 cells**, not ~330: 0.84 km² / 307 m²) | < 12 ms | 24 ms (gzip incl.) |
  | 7.2 k cells columns | < 80 ms | 31 ms, 170 KB wire |
  | 7.2 k cells + trees + structures json | < 250 ms | 193 ms, 0.8 MB wire |
  | 7.2 k cells bin all layers | — | 39 ms, 204 KB |
  | res=11 roll-up json | < 80 ms | 30 ms |
  | `/cells/<h3>` all layers | < 4 ms | 2 ms |
  | chunk res 9 h3b | — | 1 ms |
  | whole-KG h3b (after first build, 153 ms for 52 k cells) | < 5 ms | 1 ms |
  | `/landscape?include=cells` (7 k cells) | — | 72 ms |
  | `srv_public` RSS | +≤ 60 MB | 213 MB total after the bench (was ~180 MB) |

  The §4 cell counts were under-estimated ~8× (0.05° span ≈ 68 k res-12
  cells); the hard caps (span ≤ 0.05° **and** ≤ 16 384 cells → 413) govern,
  so a 16 k-cell box is ~0.023° square. Per-cell cost ≈ 8 µs json / 3 µs
  columns / 2 µs bin.
* Deviations / decisions:
  - `meta.partial` means *cells requested but not on disk* (`cells_missing`),
    not "a KG bbox intersects the query" — KG bboxes overlap so much that the
    latter would flag nearly every border point. `kgs_missing` (intersecting
    KGs without a product) is still reported, informationally.
  - Dedupe priority is *observed copy → larger chunk → lowest kg* (the spec's
    rule plus an observed-first tie-break, so a border cell that one product
    saw and another did not never comes back as nodata).
  - `format=columns` at res 11/10 carries aggregated floats (not u8) + `n_children`.
  - Per-cell JSON builds pause the cyclic GC (`_NoGC`, ≥ 1 000 cells) — 3×
    faster; `public_api` gzips bodies ≥ 512 KiB at level 1.
  - `orjson` is used when importable (added to `requirements.txt`), stdlib
    `json` otherwise.
  - Header LRU holds the slim header (no `chunks[]`); whole-KG builds read the
    full header once. Pub files are named `<kg>-<neo1_digest>.h3b` so a
    re-ingested product is rebuilt on first request and the stale file dropped.
  - `/kg/<code>` gains `cells{ready, cells_n, epoch, partial, summary, url}` and
    `links.cells`; `/status` gains `cells{kgs_ready, cells_total, index_rebuilt_at, epoch}`.
  - §7 hook: `api_tiers.PRIVATE_ROUTES_LANDSCAPE` + `PRIVATE_KG_SUBROUTES_LANDSCAPE`
    + `landscape_route_retired(path)`; `touch data/k_landscape_off` → those `/k`
    paths answer 410 `{"gone":true,"see":"/api/v1/docs/llm.txt#cells"}` even
    with a valid token (verified; director/processing/admin paths unaffected).

## 10. Publication invariant: `product_version == "v2.4"` ⇒ cells served (2026-10-07)

Siedler bug report: `/api/v1/kg/19454` said `v2.4 / ready:true` while
`/api/v1/cells` listed 19454 in `kgs_missing`. Root cause is structural —
the KG list flips to v2.4 when the peer commits `_json_v2`, but the cells
file reaches `data/ne/` only through `ne_ingest` (metered, `ne_ingest_mb_per_h`
= 15 MB/h → a 470 MB backlog drains in ~30 h; split parents stay `partial`
until every block is pulled). Audit at fix time: 1846 v2.4 rows, 274 with
no cells file, 90 with a partial file.

Fix (public tier only, the index / director telemetry is untouched):

* `cells_api.ne_state(code, pv)` → `ne_ready`, `ne_status`
  (`served | partial | pending | not_processed`), `ne_cells_published_at`
  (file mtime), `retry_after_s`. `pending` = KG list has a `v2.4*` product,
  no file yet. `partial` = file present with `partial_cells`.
* `landscape_query.kg_public` emits `product_version` **`v2.4-pending`**
  instead of `v2.4` while `ne_ready` is false (raw value in
  `product_version_registry`), adds the `ne_*` fields and bumps `updated_at`
  to `ne_cells_published_at` → the md5 ETag of `/kgs`, `/llm/manifest.json`,
  `/kg/<code>` changes when cells go live. `v2.4-partial` rows are never
  promoted (they don't claim cells).
* `cells_api.cells_generation()` (`c<files>-<partial>-<newest mtime>-<cells>`)
  → `/status.cells_generation`, `manifest_generation`, `kgs_ne_served`;
  `/kgs` carries the same. `index_status.kgs_ready` excludes partial files.
* `/cells` meta + 404 body: `kgs_missing` (unchanged, union) +
  `kgs_pending` / `kgs_not_processed` + `retry_after_s`; 404 `status:
  "pending"` when only pending KGs intersect.
* Per-row cost: `_files()` is cached 30 s; `ne_state` with `pv` supplied
  does no extra sqlite read, so `/kgs?limit=5000` stays ~90 ms.

## Green-ground override (`ne_green_fix.py`, 2026-10-26)

The segv2 classifier labels dull-ortho meadows `parking` / `rock` (NE groups
`bau` / `alpen`) although the fused S2 NDVI says vegetation (KG 63330 cell
`8c1e108e86e1bff`; fleet-wide median 26 % of bau cells). Until the model is
retrained the correction is applied **at read time only — no `.h3b`, Zenodo
product or JSON is rewritten**:

* rule: cell `ndvi_mean` (fused S2) ≥ 0.5 and phenology ≠ `road_or_bare` →
  `obs_lu[bau] + obs_lu[alpen]` → `obs_lu[gruen]`, verdict recomputed with
  the `build_observed` rules (`unknown` stays). Idempotent, deterministic.
* where: `cells_api._load_chunk` (public cells API, chunk LRU / pub container
  / ETags keyed on `FIX_ID`), `ne_h3.neo1_section_from_h3b` (what the primary
  POSTs to umfeld-at; section header carries `green_fix*`).
* propagation: `ne_ingest._our_digest` returns the *fixed* digest (cached in
  `data/ne/index.json` under `green_fix`, ≤ `ne_green_fix_compute_per_tick`
  (400) fresh computations per tick), so `confirm_umfeld` sees a mismatch and
  re-POSTs every parent at `ne_post_max_per_tick` (knob, 15 → ~180/h, ~0.3 MB
  each) — ~1.5 days for the corpus. Knob `ne_green_fix=false` turns it off.
* the fixed export is labelled `source: srtm-lidar-at@2.4.1` (epoch stays
  `2026-10`; `ne_h3.OBS_SOURCE_FIXED`) so umfeld's `/ne/manifest`
  `kgs[kg].obs.source` shows which KGs carry it. Bytes served by umfeld
  (`GET example.invalid/api/v1/ne/cell/{h3}`) == bytes from `/api/v1/cells`
  once the resync has reached the KG.
* **standing rule — resync by digest, never blindly**: `confirm_umfeld`
  compares `kgs[kg].obs.digest` with our fixed digest and re-POSTs only on
  mismatch (`replaced:true`; `unchanged:true` is free). Limits honoured:
  ≥0.6 s between POSTs (`POST_MIN_GAP_S`), `ne_post_gib_per_day` (2 GiB default, 5 GiB set 2026-10-28,
  umfeld's token limit is 4 GiB/day) tracked in `index.json → stats.post_day`
  (`DAY-CAP-HOLD` on the `resync(by digest)` line). When our digest for a KG
  changes the retry ladder (`post_attempts`/`post_next`) resets.
* **which index row describes the file on disk** (`ne_ingest._disk_row`): a
  whole-parent product ingested *after* the last block merge overwrites the
  merged file — its `files` row wins over the stale `parents` row. Before
  this (03212, 01806, 2026-10-26) a bevdirect-cell NEO1 was POSTed bare →
  409 "cell set ≠ our NEC1 lu cell set" every 30 min; now NEC1+NEO1.
* a *genuinely* lu-declared file that 409s on cell set while umfeld has
  `bev == null` is parked (`needs_bevdirect`, shown on the resync line) until
  its digest changes — it has to be reprocessed on a bev-mode peer.
* `/process.txt` → `ne_ingest:` block, lines `green_fix(green-1): umfeld_confirmed=…`
  and `resync(by digest): today N POSTs M MiB …`.
* `summary.observed_ha` in headers is still pre-fix (documented in the pub header).
* test: `python3 tests/test_ne_green_fix.py`; audit one file:
  `python3 ne_green_fix.py data/ne/63330.h3b`.
