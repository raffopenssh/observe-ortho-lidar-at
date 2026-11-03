# Public cells API (`cells_api.py`, public tier `:8100`)

The cells API serves **what we measured** per H3 res-12 cell (≈ 300 m²):
land-cover mix, structure cover and heights, trees, NDVI / phenology,
terrain, multi-epoch change and a consistency verdict. It is the public,
cell-keyed way to ask "what is here" — nothing in it is keyed by any
property or ownership unit. Design of the cell records: `docs/ne-v24-design.md`.

## 0. Hard rules

1. **Vocabulary.** Public code, docs, JSON keys, error strings and headers
   never use property-register terms (`cells_api.FORBIDDEN_WORDS`, enforced
   by `tests/test_cells_vocab.py` over the source, `public_docs.CELLS_TXT`,
   `/cells/dict`, one cell record and one public h3b header). Our
   vocabulary: *cell, cover, structures, trees, terrain, change, phenology,
   consistency*. Run the test before every edit.
2. **Request path reads only `data/ne/*.h3b` + `data/ne/cells.sqlite`**
   (+ the `kg` table of `search_index.db` for code → bbox). No Zenodo, BEV,
   GPKG or JSON blobs. Not on disk → 404 `ready:false`.
3. **The reference land-use slice never leaves the host.** An h3b chunk
   holds `shuffle(NEC1)|shuffle(NEO1)|shuffle(NET1)|shuffle(NEB1)`; every
   emitted format is rebuilt from the observed slices NEO1/NET1/NEB1 only
   (`format=h3b` re-deflates without NEC1).
4. **Standalone module.** `cells_api.py` = pure functions + one Flask
   `Blueprint` (`cells_bp`), importing only `ne_h3`, `h3`, `numpy`,
   `sqlite3`, `pyproj`. Mounted by `public_api.py`; never imports `app`,
   `peer_director` or `search_index`.

## 1. Data on disk

**Epoch pin.** Every `X-Cells-Epoch` / `epoch` / `ne_epoch` value the public
tier emits is `cells_api.NE_EPOCH` (`2026-10`), one value corpus-wide. The
`YYYY-MM` in a product's own header (`neo1.epoch`, stamped by `ne_h3.build`)
is only the **build month** — products built in 2026-10 and 2026-11 are the
same epoch (same algorithm, vocabulary, sources) and are exposed as
`ne_built` / `built` instead. Downstream consumers hash the epoch header
into their cache state, so it must only move when the observed product
semantics change (bump the constant together with `ne_h3.ALGO_OBS`), never
per build.


* `data/ne/<parent>.h3b` — written by the processor (unsplit KGs) or by
  `ne_ingest` (split parents merged from their blocks, header
  `merged_from`, `partial_cells` until complete). Header `chunks[]` = one
  entry per res-10 cell: `r10, off, len, raw_len, cells, trees, bld, first`.
  Readers: `ne_h3.read_h3b_header`, `read_h3b_chunk`.
* `data/ne/cells.sqlite` (WAL) — chunk index owned by `ne_ingest`
  (`cells_api.index_file()` after every write / merge;
  `python3 cells_api.py --rebuild` rescans all headers):

  ```sql
  CREATE TABLE chunk (r10 INTEGER NOT NULL, kg TEXT NOT NULL, off INTEGER, len INTEGER,
                      raw_len INTEGER, cells INTEGER, trees INTEGER, bld INTEGER,
                      PRIMARY KEY (r10, kg)) WITHOUT ROWID;
  CREATE TABLE file  (kg TEXT PRIMARY KEY, body_offset INTEGER, cells_n INTEGER, epoch TEXT,
                      product_version TEXT, neo1_digest TEXT, partial INTEGER, mtime REAL,
                      w REAL, s REAL, e REAL, n REAL);
  CREATE TABLE meta  (k TEXT PRIMARY KEY, v TEXT);
  -- plus summary(kg, mtime, js) and als_kg(…) caches
  ```
  ≈ 45 B/row × 5.6 M res-10 chunks ≈ 250 MB for all of Austria (vs ~10 GB
  corpus).
* Resolution in the request path: `bbox → h3.polygon_to_cells(res 10)`
  (+ `grid_ring` 1) → `SELECT … FROM chunk WHERE r10 IN (…)` → group by
  `kg` → one `open()` per file, `seek` + `read(len)` per chunk,
  `zlib.decompress`, `ne_h3._unshuffle` → numpy structured decode of
  `ne_h3.OBS_REC` (no per-row Python) → filter res-12 ids (`np.isin`).
* **Dedupe across KGs**: a border res-12 cell appears in every KG it
  touches. Priority: observed copy → larger chunk → lowest kg; `meta.kgs[]`
  records provenance.
* Caches (process-local, byte-bounded `mem_lru.ByteLRU`): inflated chunk
  LRU (`CELLS_CHUNK_MB`, default 64) and header LRU (`CELLS_HEADER_MB`,
  default 24). No corpus-wide structures in RAM; sqlite connection per
  thread.

## 2. Endpoints (GET unless noted)

```
/api/v1/cells?bbox=w,s,e,n            res-12 cells in a box (span ≤ 0.05°, ≤ 16 384 cells → 413)
/api/v1/cells?lon=&lat=[&k=0..3]      cell under a point + k rings
/api/v1/cells?ids=h3,h3,…             ≤ 512 ids (coarser ids expand to their res-12 children)
POST /api/v1/cells {"ids":[…]}        ≤ 16 384 ids
/api/v1/cells/<h3>                    one cell, all layers, always JSON (`?flags=1` adds public flags)
/api/v1/cells/chunk/<h3>              res 9 or 10 chunk(s) for bulk clients; format=h3b|bin|json
/api/v1/cells/dict                    field layout, units, code tables, group labels, limits, epoch
/api/v1/cells/national?res=9|8        Austria-wide aggregate (§7)
/api/v1/kg/<code>/cells               whole-KG container: format=h3b (default) | json summary
/api/v1/kgs/summary?codes=|gemeinde=|district=|state=   merged summary over a KG set (§6)
/api/v1/landscape?…&include=cells     per-bbox roll-up block
/api/v1/status                        cells{kgs_ready, cells_total, index_rebuilt_at, epoch, generation}
```

Common params: `layers=obs,trees,structures` (default `obs`; `/cells/<h3>`
always all), `res=12|11|10` (server-side roll-up), `fields=a,b,c`,
`format=json|columns|bin|h3b|csv|geojson`, `centres=1`, `pretty=1`.
Limits: `geojson` ≤ 4 096 cells, `trees`/`structures` only at res 12,
`h3b` only on `/chunk` and `/kg/<code>/cells`.

Not-ready answers: `404 {"ready": false, "status": "not_processed" |
"pending" | "partial", "retry_after_s": 3600, "kgs_missing": […]}` with
`Retry-After`. A partly covered bbox answers 200 with `meta.partial:true`
(= cells requested but not on disk) + `meta.kgs_missing` /
`kgs_pending` / `kgs_not_processed`.

Headers: `ETag` (sha256 of the touched files' digests + canonical query,
`If-None-Match` → 304), `Cache-Control: public, max-age=86400`,
`X-Cells-Source: disk`, `X-Cells-Partial: 1`, `X-Data-Attribution` +
`Link rel=license`, CORS `*`, gzip ≥ 1 KiB (not for `h3b`). Please stay
≤ 5 r/s sustained, burst 20.

## 3. Record → public JSON (one cell, res 12)

```json
{"cell": "8c1f2a8b6c4ddff", "res": 12, "kg": "63330",
 "cover": {"geb": 0.02, "bau": 0, "acker": 0, "gruen": 0.31, "wald": 0.61, "wasser": 0,
           "verkehr": 0.04, "alpen": 0, "sonst": 0.02},
 "structures_cover": 0.02, "structures_n": 1,
 "height": {"mean": 9.5, "p90": 21.0, "max": 27.5},
 "trees": {"n": 14, "h_max": 27.5, "canopy": 0.58},
 "ndvi": {"mean": 0.71, "amp": 0.22}, "phenology": "forest",
 "change": {"dh_m": -0.25, "dtm_m": 0.0, "forest_loss_year": null,
            "als_year_newest": 2024, "als_year_oldest": 2022},
 "terrain": {"elev_m": 612, "slope": 14, "slope_p90": 23, "aspect": "SE",
             "roughness": 1.3, "tri": 0.8, "tpi": -0.5},
 "consistency": "consistent"}
```

* Decoded exactly from `ne_h3.OBS_FIELDS` with `OBS_UNITS` (255 / −128 /
  65535 → `null`). `cover` = `obs_lu[9]`/255 in `ne_h3.GROUPS` order
  (geb = buildings, bau = construction & parking, acker = cropland, gruen =
  grass/garden/orchard/vineyard/shrub/hedge, wald = trees, wasser = water,
  verkehr = roads & paths, alpen = rock/bare/glacier, sonst = other).
* `consistency` = `ne_h3.VERDICTS` mapped through `CONSISTENCY_PUBLIC`:
  `consistent, forest_loss, forest_gain, sealed_new, structure_new,
  green_new, unknown` — our observation versus the reference land-use
  layer that was available when the product was built; `unknown` when no
  reference layer was available (the default in a fresh single-VM setup).
* `trees[]` (res 12 only): `{dx, dy, lon, lat, h_m, crown_m2, species,
  vitality, ndsm_only}` from the 4 B NET1 word (`dx:5 dy:5 h:8 (0.25 m)
  crown:6 (log2·5) species:4 vitality:3 flag:1`; `ne_h3.SPECIES_CODES` /
  `VITALITY_CODES`). `n` saturates at 255; `meta.trees_truncated_cells`.
* `structures[]`: `{dx, dy, lon, lat, type, h_max_m, h_robust_m, area_m2,
  conf, dh_m}` from the 8 B NEB1 word (`ne_h3.BLD_TYPES`) — our segmented
  man-made objects with LiDAR heights (centroid + height, no outlines).
* `format=columns`: `{"cells":[…], "kg":[…], "cover":[[9 × u8]…],
  "h_mean":[…], …}` raw quantised integers + a `units` block — ~40 B/cell
  before gzip, the format for map clients.
* `format=bin`: `cells_n × 41 B` raw NEO1 rows, little-endian, sorted by
  cell id; `X-Cells-Record: neo1/41`, `X-Cells-Layout: neo1=N,net1=T,neb1=B`.
* `format=h3b` (chunk / whole-KG only): `H3B1`-style container **without
  the NEC1 slice**. Whole-KG answers are built once per `(kg, neo1_digest)`
  into `data/ne/pub/<kg>-<digest>.h3b` (≤ 1 MB, ~200 ms) and then served
  with Range support — the public download of the product.
* `res=11|10` roll-up (obs only): count-weighted means for `cover`,
  `structures_cover`, `ndvi.*`, `terrain.*` (aspect → mode); `height.max` /
  `trees.h_max` max; `trees.n` / `structures_n` sum; `phenology` /
  `consistency` → mode + `consistency_hist[7]`; `als_year_newest` max,
  `als_year_oldest` min; returns `n_children`. One `np.add.reduceat` pass.

### ALS acquisition year (`als_years.py`)

The frozen row's `flight_years` bitmask is 0 wherever the last flight was
≤ 2019, which conflates "old flight" with "no data". The BEV flight-block
overlay (`data/als_acquisition/flight_blocks_<mosaic>.geojson`, `Flugjahr`
per block; series `5ce253fc-b7c5-4362-97af-6556c18a45d9` on data.gv.at) is
therefore evaluated **at read time, at the cell centre**. v2.4 observed
values come from the `20240915` mosaic → `als_year_newest`; `dh_m` /
`dtm_m` are newest − oldest (`20220915`) → `als_year_oldest`. Equal years
⇒ not re-flown, `dh_m` is noise (`reflown_share` per KG).

| surface | field |
|---|---|
| `/cells` json / csv | `change.als_year_newest`, `change.als_year_oldest` (roll-up max / min) |
| `/cells?format=columns` | `als_year_newest`, `als_year_oldest` u16, **65535 = null** |
| `bin`, `/chunk`, `h3b` | frozen rows, unchanged |
| `/cells/dict` | `derived_fields`, `als{…}` |
| `/kg/<code>`, `/kg/<code>/cells?format=json` | `als{years, newest, oldest, dominant, dominant_share, coverage, reflown_share, …}` |
| `/status` | `als{kgs_with_als_year, year_hist, …, backfill}` |

Per-cell years are computed once per inflated chunk (`Chunk.als()`) from
the cached centres (two in-memory STRtrees, ~450 polygons); the per-KG
block lives in `cells.sqlite.als_kg`. CLI: `python3 als_years.py
--status|--backfill|--kg <code>`; test `tests/test_als_years.py`.

## 4. Performance (measured, 2 workers × 24 threads)

| request | p50 |
|---|---|
| point k=0 obs json | 2–3 ms |
| 0.01° × 0.01° box json (≈ 2 700 cells) | 24 ms |
| 7.2 k cells `columns` | 31 ms, 170 KB wire |
| 7.2 k cells + trees + structures json | 193 ms, 0.8 MB wire |
| res=11 roll-up json | 30 ms |
| `/cells/<h3>` all layers | 2 ms |
| whole-KG h3b after first build | 1 ms |

Per-cell cost ≈ 8 µs json / 3 µs columns / 2 µs bin. Per-cell JSON builds
pause the cyclic GC (`_NoGC`, ≥ 1 000 cells); `orjson` is used when
importable. Benchmark: `python3 tools/bench_cells_api.py` against
`localhost:8100`.

## 5. Publication invariant: `product_version == "v2.4"` ⇒ cells served

The KG list flips to v2.4 when `_json_v2` is committed, but the cells
file reaches `data/ne/` only through ingest (split parents stay `partial`
until every block is merged). To keep the two surfaces consistent:

* `cells_api.ne_state(code, pv)` → `ne_ready`, `ne_status`
  (`served | partial | pending | not_processed`), `ne_cells_published_at`,
  `retry_after_s`.
* `landscape_query.kg_public` emits `product_version` **`v2.4-pending`**
  instead of `v2.4` while `ne_ready` is false (raw value in
  `product_version_registry`) and bumps `updated_at` so ETags change when
  cells go live. `v2.4-partial` rows are never promoted.
* `cells_api.cells_generation()` → `/status.cells_generation`; `/cells`
  404 bodies carry `kgs_pending` / `kgs_not_processed` + `retry_after_s`.

### Green-ground override (`ne_green_fix.py`)

The segv2 classifier labels some dull-ortho meadows `parking` / `rock`
(groups `bau` / `alpen`) although the fused Sentinel-2 NDVI says
vegetation. Until the model is retrained the correction is applied **at
read time only** — no `.h3b`, Zenodo product or JSON is rewritten: cell
`ndvi_mean ≥ 0.5` and phenology ≠ `road_or_bare` → `obs_lu[bau] +
obs_lu[alpen]` moves to `obs_lu[gruen]`, verdict recomputed. Applied in
`cells_api._load_chunk` (chunk LRU / pub container / ETags keyed on
`FIX_ID`) and `ne_h3.neo1_section_from_h3b`. `summary.observed_ha` in
headers is pre-fix (documented). Test `tests/test_ne_green_fix.py`; audit
one file: `python3 ne_green_fix.py data/ne/63330.h3b`.

## 6. Multi-KG summary `/api/v1/kgs/summary`

Scope (exactly one): `codes=63330,63331,…` (≤ 500), `gemeinde=61630`
(5-digit GKZ), `district=616` (3), `state=6` (1). Vienna's districts
(`district=901…923`) are unsupported (400); Vienna as a whole is
`district=900` / `gemeinde=90001`. Unknown filter value → 404; unknown KG
codes → `kgs_unknown`.

Response: `{scope, kgs, kgs_processed, incomplete, kgs_pending_n,
kgs_not_processed_n, kgs_partial_n, summary, meta}`; `summary` has the
keys of the per-KG `/kg/<code>/cells?format=json → summary` block merged
by `cells_api.merge_summaries` (sums for counts / hectares, min / max for
elevation, weighted slope mean, newest `epoch`). Header-only cost:
per-KG summaries persist in `cells.sqlite.summary`, with a 256-entry
response cache keyed on `(scope, codes, cells_generation())`. ETag +
`Cache-Control: public, max-age=600`.

## 6a. Per-bin NEX1 export, blocks and the builder (`cells_bins.py`)

`GET /api/v1/cells?bbox=<0.025° bin>&format=bin&fields=…` returns one
columnar `NEX1` frame per bin (container documented in `cells_bins.py` and
`/cells/dict → bins`). Measured by a downstream index before this change:
0.7–0.8 s cold per bin (the bin was **built on first request**: h3b join +
grid25 + roof hints + ALS overlay), 50 ms warm; a 0.1° view = 16 requests
≈ 12 s.

* **`block=1`** — a `0.1°`-aligned bbox (`w = bi·0.1, s = bj·0.1`) answers
  the 4 × 4 bins in one body, container **`NEXB`**:

  ```
  b'NEXB' | u32 header_len | header JSON | frame_0 | … | frame_15
  header = {gen, ij:[bi,bj], bbox, fields, ids, order,
            bins:[{i, j, off, len, gen, n, complete} × 16], cells_total}
  ```

  `frame_k` is **byte-identical** to the single-bin `NEX1` answer for
  `bins[k]` with the same `fields` / `ids`; `off` is relative to the end of
  the header (`8 + header_len`). Order is row-major: `k = (j − 4·bj)·4 +
  (i − 4·bi)` — latitude rows south → north, west → east within a row.
  ETag `"B-<sha256 of the 16 bin gens>-<fields hash>"`, `X-Cells-Gen` = that
  block gen, `X-Cells-Block: bi,bj`, `X-Cells-Layout: nexb=n0,…,n15`,
  `X-Cells-Partial: 1` when any bin is partial. Bins with no product
  underneath still get an (all-unknown) frame, so the client never has to
  special-case holes. `python3 cells_bins.py --block bi,bj` prints one.
* **Precompute** — `cells-bins.timer` (**hourly, 50 min budget**,
  `python3 cells_bins.py --prewarm --budget-s 3000 --jobs 2`; bins → blocks
  → sample, see §6b) builds every bin of the manifest that is missing on
  disk (first) or whose stored generation differs from the current one
  (after a KG re-ingest / grid25 re-ingest), and resumes until all of Austria (~16 k
  bins, ~3.5 GB) is current. `python3 cells_bins.py --status` /
  `GET /cells/bins?status=1` → `{bins, stored_current, missing, stale,
  complete, last_run}`. The on-disk generation is read from the blob header
  only (`stored_gen`, ≤ 16 KiB per file), so a no-op pass costs ~2 s. The
  request path still builds a bin on demand when it is missing or stale.
* **Serving** — disk → inflate → slice columns → gzip (`public_api`
  after-request, `Accept-Encoding: gzip`, `Vary`) ≈ 15–45 ms per bin cold
  (process LRU empty), `ETag` + `If-None-Match → 304`, `Cache-Control:
  public, max-age=86400`. Measured (localhost, 5 dense bins, 9 fields,
  fresh worker): **before 0.5–1.4 s (build) → after 13–25 ms** (first
  request on a fresh worker ~150 ms). Only `4xx`/`5xx` carry `no-store`.

## 6b. Id-free zstd blocks (NEXB v2), `/cells/blocks`, national sample `/cells/sample` (`cells_blocks.py`, 2026-11-03)

Downstream glyph filters now run nationally. Both additions are **derived from the stored 0.025° bins** — no new per-cell
computation; the only epoch/version signal is the existing `X-Cells-Epoch`
(the ne-observe product epoch, `2026-11` today) plus a content generation.

* **`block=1` default = NEXB v2** (`ids=1` / `ids=u64` keep the v1
  frames-with-ids form byte for byte). No ids: the cell set of a bin is the
  shared enumeration *all res-12 cells whose centre lies in the half-open bin
  [w,w+0.025)×[s,s+0.025), ascending H3* (= `h3.polygon_to_cells` centre
  rule = what `build_bin` stores). Header carries per bin `n`, `row0`,
  `ids_xor` (xor of the u64 ids, hex) and `gen`; the client enumerates
  itself, checks `n`/`ids_xor`, and falls back to `ids=1` on mismatch. Body
  = header | present bitmap (ceil(n/8) B, all ones today — reserved so the
  enumeration may shrink later) | planar columns (n rows each, the 16 bin
  enumerations concatenated in row-major bin order). Container documented in
  `cells_blocks.py` / `/cells/dict → blocks_v2`; decoder
  `cells_blocks.decode_block`.
* **Prebuilt + zstd**: `data/ne/nexb/<bi>_<bj>.<fields-hash>.nexb.zst` +
  `.json` meta `{gen, etag, bytes, n, epoch, fields, partial, bins_n,
  built_at}`; one zstd level (9) everywhere so the strong ETag
  `"B-<gen>-<hash(fields|bitmap)>"` is byte-exact. Served with
  `Content-Encoding: zstd` when accepted (else inflated → `public_api`
  gzips), `Vary`, `X-Cells-Gen`, `X-Cells-Epoch`, `X-Cells-Ids: bitmap`,
  `X-Cells-Layout: nexb2=n0,…`, `X-Cells-Zstd-Bytes`. Measured: 9 glyph
  fields ≈ 0.7–1.1 MB (was ≈ 1.1 MB gz with ids), 18 defaults ≈ 1.3–2.1 MB; warm serve < 100 ms
  (ETag check 2 ms, then a file read); a block requested before it is
  prebuilt is built on the spot (~0.15 s when its bins are stored, ~10 s
  when not) and stored.
* **`GET /api/v1/cells/blocks[?fields=…][&epoch=…][&format=json]`** —
  columns `{bi, bj, gen, etag, bytes, stored, current, n, bins_n, complete}`
  for every block touching a published product; `bytes`/`n` null until
  prebuilt for that field set (`stored`), `current` = stored gen == gen.
  ETag `"blocks-<generation>"`; `epoch=` ≠ current `X-Cells-Epoch` → 409
  `{epoch}`. `?status=1` → builder/sample status (also on `/cells/bins?status=1`).
* **`GET /api/v1/cells/sample?res=10&format=bin[&fields=glyph|default|*|a,b]`**
  — one row per res-10 cell of Austria = its res-12 **centre child**
  (`cellToCenterChild(parent,12)`; bit rule `_sample_mask`: digits res+1…12
  zero, verified against h3), ascending H3 (= parent order), every cell
  with DTM (`terrain_src != 0`), ≈ 5.6 M rows. NEX1 container (`sample_res`,
  `rule`, `ids: delta_u64`, planar columns, bin dtypes/units/nodata).
  `fields=glyph` = the nine glyph-filter fields (`GLYPH_FIELDS`, ≈ 26 MB);
  default adds the cheap extras not used by the glyph filter
  (`SAMPLE_EXTRA_FIELDS`: obs, terrain_src, bld_n, bld_area_sum,
  struct_h_max, canopy_h_p50, lc25, tree_n, dh_m, consistency,
  forest_loss_year, phenology, als_year_newest — ≈ +12 MB); `*` = every
  bin column except `cover` (≈ 100 MB). Stored superset
  `data/ne/sample/r10.nex.zst` (zstd 19) + `r10.json`; slices cached in
  `sample/cache` (LRU 16). ETag `"S-r<res>-<gen>-<hash(fields)>"`,
  `X-Cells-Gen` = `sha(sample-1 | bin-manifest generation)`,
  `X-Cells-Epoch`, `X-Cells-Built`, `X-Cells-Partial` when bins were
  missing on disk at build time (`header.bins_missing`). `format=json` /
  `meta=1` → build meta (+ `current`). 404 + `Retry-After` before the
  first build. res 9 / 11 are accepted too.
* **Prewarm = the existing builder, extended**: `cells-bins.timer` is now
  **hourly, 50 min budget** (`cells_bins.py --prewarm --budget-s 3000 --jobs
  2`, nice 15, idle IO): phase 1 bins (missing → stale, as before), phase 2
  blocks for `cells_blocks.prewarm_fieldsets()` = defaults + glyph + up to 8
  field sets seen in real block requests (`data/ne/nexb/fieldsets.json`,
  `remember_fieldset`), phase 3 the sample (skipped when its gen is
  current). A pass over an all-current corpus costs seconds, so new blocks
  are picked up within the hour of a product landing; never run in gunicorn.
  `python3 cells_blocks.py --status | --block bi,bj [--fields glyph] |
  --prewarm | --sample 10 [--force]`.
* **Test**: `python3 tests/test_cells_blocks.py` (centre-child rule vs h3,
  v2 codec + xor checksum vs v1 frames, routes incl. 304 / gzip fallback /
  ids=1 / manifest / sample).

## 7. National aggregate `/api/v1/cells/national` (`cells_national.py`)

One row per res-9 H3 parent (~1.05 M rows; `res=8` ≈ 150 k), Austria-wide,
for overview tiles: per field the mean over the res-12 children where the
field is non-null (`*_top` = max), `verdict` = u16 counts per consistency
class, `n_cells` / `n_obs`. Same container / dtypes / units / nodata as
the 0.025° bins (`cells_bins.py`, `format=bin&fields=`).

* **Build**: `python3 cells_national.py --build` (~10 min, ~1 GB RAM over
  5.4 M chunks) → `data/ne/national/r9.nex.gz`, `r8.nex.gz`, `meta.json`.
  `cells-national.timer` runs it daily; skipped when
  `gen = sha(ALGO | green_fix | cells_generation())` is unchanged. **Never
  build inside gunicorn.**
* **Serve**: no `fields=` → the stored gz as is; `fields=` → decode + slice
  + gzip once, cached under `data/ne/national/cache/` (LRU 24,
  single-flight). ETag `"n-r<res>-<gen>-<fields>"`, `X-Cells-Gen`,
  `X-Cells-Epoch`, `X-Cells-Built`, `max-age=86400`; 404 + `Retry-After`
  before the first build.
* Test: `python3 tests/test_cells_national.py`.

## 8. Operations

```bash
python3 tests/test_cells_vocab.py                  # before every edit
python3 cells_api.py --rebuild                     # if /status.cells disagrees with ls data/ne/*.h3b
curl -s 'localhost:8100/api/v1/cells?lon=15.60&lat=48.41&k=1' | python3 -m json.tool
curl -s localhost:8100/api/v1/cells/dict | python3 -m json.tool | head
```

Split-parent ingest and merge mechanics: `ne_ingest.py` (thread in
`app.py`; `ne_ingest:` line in `/process.txt`; knob
`POST /api/v1/director/config {"ne_ingest_enabled": false}` stops pulls).

## 12c. Stats envelope cell lists — `POST /api/v1/stats/unit?unit=cells` (`stats_unit.py`, 2026-11-03)

A sibling statistical NE service's `f=<expr>` filter knows the matching res-12
cells; this service does not (the grammar spans all sibling services' columns). So for a filtered unit
it POSTs the explicit cell set (spec: `stats-envelope.md §6`)
instead of a unit key. Before this we answered 405 and the caller showed our
columns as unavailable for every filtered row.

* Body: gzip'd `"NEI1" · u32 n · n × u64 LE` res-12 ids, sorted ascending,
  ≤ 1 M (`CELLS_MAX_N`; 413 above, 400 on bad magic / wrong res / truncated).
  `parse_cells_body` bounds the inflate at 8 MB + ε so a gzip bomb cannot
  grow past the limit.
* `unit_cells(ids, scale)`: `ca.select(ids)` → `_acc_from_table` exactly as
  bbox units; unknown ids simply have no row (`coverage.cells_known <
  cells_total`, note "N cells outside any product"); the KG-missing note is
  suppressed because the bbox-of-list spans KGs the list never touches.
  Every listed cell weighs 1 — the `kg=` border weighting does not apply.
* `scale` (default 1; 49 for the national 1-in-49 sample where one res-12
  cell stands for its res-10 cell): `cells_n = n × scale`, `area_ha = Σ cell
  areas × scale` (`_cells_area_m2`: exact per res-10 parent, res-8 ancestor
  / 2401 above 60 k parents), `_scale_figures` multiplies every `_ha` / `_n`
  figure (incl. `cover_ha.*`, `consistency_*`), leaves means / `_pct` /
  extremes / years alone; `coverage.cells_known` is scaled too so
  `complete` keeps its meaning.
* Cache key `cl:<sha256(ids)[:16]>:<n>` + bbox fingerprint, persisted only
  for n > 20 k; the usual 3.5 s budget → 202 + `Retry-After` for cold long
  lists (0.1°×0.1° = 273 k cells: 3.6 s cold; its 1-in-49 sample: 0.4 s).
* Invariant: the 49 res-12 children of a res-10 cell as a list ≡
  `?h3=<that res-10 cell>` figure for figure; the full 0.1° list and its
  1-in-49 sample × 49 agree within ~3 %.
