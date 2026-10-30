# Search index — mental model

SQLite at `data/search_index.db` (FTS5 + R-tree + b-tree indexes, roughly
1 GB for all of Austria). All queries < 50 ms. Built at startup if
missing; auto-updated when new KG JSON documents / `_json_v2` blobs appear
(60 s poll). Manual rebuild: `POST /api/v1/index/rebuild` (incremental;
`?force=1` drops & recreates — hours for 7 850 KGs). Single KG:
`SearchIndex().update_kg(code)`.

The index is **derived data** — it can always be rebuilt from the product
documents (`data/austria_processor/json/*.json` and `kg_v2_store.db`).

## Tables

```
kg                       ← one row per parent KG (product_version, bbox, admin names,
│                           terrain / NDVI / SAR / Hansen totals, RF stats, dominant_type …)
├─ kg_landcover          ← per-type area summary
├─ kg_hansen             ← forest loss by year
├─ kg_classification     ← per-type RF stats (count, height, confidence)
├─ kg_divergence         ← RF ↔ rule-based disagreement details
├─ kg_type_top           ← top-N segments per type per KG (ranking / landmarks)
├─ kg_buildings          ← legacy per-building rollup, frozen (v2.4 docs carry no footprints)
└─ kg_trees              ← legacy sampled apices, frozen; the full apex set is the
                            light GPKG `tree_apices` layer / the cells API

kg_rtree (virtual)       ← spatial index over kg.bbox
fts_kg (virtual)         ← full text over name / state / district / gemeinde
index_meta               ← build timestamp, kg_count, schema_version
```

The index is keyed by KG code only (parent KGs). It never contains
per-object rows or any per-property data; per-tree / per-pixel detail
lives in the light GPKG and in the NE cells (`docs/ne-public-api.md`).

## How a KG record is built (`_enrich_kg`)

```
kg_docs.load(code)   (kg_v2_store first, json/<code>.json second, or block aggregator)
      │
      ├─→ kg row
      ├─→ kg_landcover       (one row per object_type)
      ├─→ kg_hansen          (one row per loss_year)
      ├─→ kg_classification  (per-type confidence + height stats)
      ├─→ kg_type_top        (top-N segments by area / volume / height / confidence)
      └─→ fts_kg, kg_rtree   (one row each)
```

**Split KGs** (`49006-south`, `49006-north`, …): documents land per block.
`_enrich_kg_from_blocks(parent, blocks)` aggregates them into one parent
row — weighted average for area-weighted scalars, sum for additive
counts, min/max for extrema, union of nested arrays (`top_10_*`,
`top_by_type`), one merged `ne` summary. Only the parent code is visible
through the API; block codes appear only in file names and Zenodo
entries.

### File selection (`_select_kg_files_for_parent`)

For each parent the freshest consistent set of files is picked, using the
Zenodo manifest's `uploaded_at` as source of truth (then `generated_at`,
then mtime):

1. files whose `<code>_error` manifest entry is newer than `<code>_json`
   are discarded (failed runs leave stale JSONs);
2. if only the plain side has a committed `_json`, use the plain file;
3. if only the block side has committed uploads, use the blocks;
4. otherwise take the side with the most recent timestamp and drop the
   other wholesale (no double counting).

`/api/v1/kg/<code>` uses the same selector (`merged_kg_json`) and returns
the merged document shape for split KGs.

### Admin endpoints (operator API, loopback)

```
POST /api/v1/admin/reindex_split_kgs            # re-enrich every parent with block files on disk
POST /api/v1/admin/backfill_jsons_from_manifest # download missing _json uploads from Zenodo
   body: {limit?: 200, codes?: [...], dry_run?: false}
```

## Query taxonomy

```
query_kg(code)                                         single KG summary + Zenodo links
query_bbox(w, s, e, n, …) / query_point(lon, lat, r)   spatial (R-tree)
query_admin(level, code= | name=)                       state / district / gemeinde
aggregate_state / _district / _gemeinde / _country
query_text(q, limit, offset)                            FTS5 over name / admin
query_type_ranking(object_type, metric)                 area | volume | height | rf_conf
query_high_confidence_type / query_low_confidence / query_confidence_ranking
query_hansen_loss(year_from, year_to, min_loss)
query_new_buildings(min_count) / query_processed(…)
query_divergence(min_pct, rf_type, final_type) / query_divergence_pairs()
query_trees_bbox(w, s, e, n, min_height=20, limit)      legacy kg_trees rows
query_top_features(feature_type, object_type=, …)
query_segments(…) / query_segments_detail / query_segment_points(kg, bbox=, type_filter=)
                                                        via GpkgCache
```

The public, cell-keyed way to ask "what is here" is `/api/v1/cells`
(`docs/ne-public-api.md`); the index answers KG-level questions
(`/api/v1/kg/<code>`, `/insights`, `/index/status`).

## Schema mutation safety

`_migrate()` runs on every open and adds missing columns idempotently
(`ALTER TABLE … ADD COLUMN` guarded by PRAGMA introspection). To add a
column:

1. add it to the `CREATE TABLE` in `_schema_stmts`;
2. add an `ALTER TABLE` block in `_migrate()`;
3. populate it in `_enrich_kg` (and `_enrich_kg_from_blocks`);
4. optionally `CREATE INDEX IF NOT EXISTS`;
5. rebuild: `python3 -c "from search_index import SearchIndex; SearchIndex().build()"`.

Never drop a column casually — add a new one and migrate readers.

## GPKG cache (lazy detail loading)

`GpkgCache` downloads light GPKGs from Zenodo on demand for the per-KG
detail endpoints. Files live in `data/gpkg_cache/<code>_<variant>.gpkg`
(variant `light_v2` is shared with the v3 tree service); LRU eviction at
`GPKG_CACHE_MAX_BYTES`. `_query_gpkg_layer(...)` reads via fiona/pyogrio.

`_resolve_gpkg(kg, variant, wait, prefer_v1)` resolves local processor
output → cache → Zenodo, taking link, size and md5 from the manifest
(`<code>_light_gpkg_v2` first, then `_light_gpkg`) with a fallback to the
index's `zenodo_*_gpkg_url`.

### Downloads go through `zenodo_fetch.py`

All request-path Zenodo downloads use `zenodo_fetch.download()`:

* **Progress** — bytes done/total, EMA MB/s, ETA, attempt, status
  (`queued|connecting|downloading|verifying|done|error`); a per-thread hook
  (`set_hook`) lets async tasks surface it in their progress file.
* **Single-flight** — in-process waiters share one `FetchState`; across
  gunicorn workers an `fcntl` lock on `<dest>.lock` makes the sibling relay
  the sidecar progress (`<cache>/.fetch/<file>.json`) instead of
  downloading a second copy.
* **Robust** — 60 s per-read stall timeout, 4 attempts with HTTP `Range`
  resume from the partial `.tmp`, 429/503 `Retry-After` honoured, 4xx fail
  fast, size + md5 verified before the atomic rename; ≤ `MAX_PARALLEL=4`
  downloads per process.
* **Bounded blocking** — `download(wait=S)` raises `FetchInProgress` after
  S seconds while the download continues. `GET /api/v1/kg/<code>/{segments,layers}`
  use this (`?wait=`, default 25 s) to answer `202 Accepted` +
  `Retry-After` while cold and 200 once the file is local.
* **Parallel prefetch** — `prefetch_many(items, hook)` folds several
  downloads into one aggregate; `trees_v3.prefetch_gpkgs` uses it.
* **Observability** — `GET /api/v1/zenodo/fetches` and the
  `zenodo_fetch:` line in `/process.txt`.

```bash
curl -s localhost:8000/api/v1/zenodo/fetches | jq .text
curl -s -i 'localhost:8000/api/v1/kg/<code>/layers?wait=0' | head -20   # 202 while cold
ls -la data/gpkg_cache/.fetch/
```

## Where to look

```bash
grep -n 'CREATE TABLE\|CREATE VIRTUAL\|CREATE INDEX' search_index.py
grep -n '    def query_\|    def aggregate_' search_index.py
sqlite3 data/search_index.db '.schema kg'
sqlite3 data/search_index.db 'select product_version, count(*) from kg group by 1;'
```
