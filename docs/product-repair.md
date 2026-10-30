# Repairable layer gaps — `product_repair.py`

Load when a KG product is missing a *cache-fillable* layer (NDVI
harmonics, Copernicus NDVI / WorldCover / SAR, Hansen) and you want it
fixed without a from-scratch rerun, or when adding a new layer that comes
from the shared Zenodo tile cache.

## Why

A product can be baked while the tile cache lacks a cell — openEO
read-timeouts that produce zero NDVI months (no harmonics), or a v2
upgrade from the full GPKG whose cache-only fetch (`v2_source._cache_only_fetch`,
never openEO) finds no harmonics cell. Nothing else about the product is
wrong, so **no tombstone, no requeue, no product deletion**: the partial
product stays live and served until a clean one replaces it in place, by
(1) getting the cell into the cache and (2) re-upgrading from the KG's own
full GPKG.

## Mechanism (4 pieces)

| piece | where | what |
|---|---|---|
| **Label** | producer: `austria_processor._product_upload_version(fk, defects)`; `result["product_defects"]` set right after `compute_data_quality` | JSON manifest entries (`_json`, `_json_v2`) get `version = "<ver>-partial"` (`v2.4-partial`). GPKGs keep the plain label. `v21_products.v2_products_complete` compares `== MANIFEST_VERSION`, so a partial pair reads as *not current* → the normal re-upgrade machinery owns the fix. `base_version()` strips the suffix wherever the plain label is needed (`trees_v3.product_entry`, `search_index._version_key`). |
| **Registry** | `data/austria_processor/product_repair.json` `{code: {defects, tiles, bbox, fp, product_ts, version, attempts, state}}` | Fed by `observe()` at the two ingest points — fresh JSON (`search_index.update_kg` → `_repair_observe`) and `_json_v2` (`v2_ingest`) — plus the one-shot corpus scan. Idempotent per `product_ts`. A clean product → `done`. |
| **Fixable gate** | `product_repair.fixable(code)` / `missing_cells(entry)`; used by the director's upgrade-candidate ranking | Asks the Zenodo ZIP / tile-store index (`CopernicusTileCache.has_cached(local_ok=False)`, `HansenTileCache.has_cached`) whether **every** missing product is cached for **every** 0.1° cell of the **tile footprint** (`fetch_extent(entry)`: the `fp` union of `data_quality.tiles[].bbox_wgs`, else the bbox padded by one tile east/north + 0.01°). A partial code becomes an upgrade candidate only when fixable (else `n_partial_wait`). |
| **Cell fill** | director `_product_repair_plan` → `prewarm.repair_cells` + `prewarm.repair_codes` in `cache_manifest.json`; processor `_repair_cells_fill` (from `prewarm_cell_tiles` at every frontier KG end) | `sweep()` every 15 min (`PRODUCT_REPAIR_SWEEP_S`) refreshes states and emits up to 40 `(cell, products)` requests (`PRODUCT_REPAIR_MAX_CELLS`), oldest defects first. Cells are published only while Copernicus health is good **and** `app._openeo_health()` saw no read-timeouts in the last hour. Each frontier fetches ≤ `PREWARM_REPAIR_CELLS_PER_KG` (4) per KG end, only the listed products, then flushes to Zenodo. |

**Footprint, not bbox.** `_compute_tile_grid` lets the last tile column /
row overshoot the KG bbox by up to a full 1.5 km tile, and
`get_harmonics()` returns `None` for the whole tile when *any* cell is
missing — so a code judged on the bare bbox could be promoted to
`fixable`, dispatched, and come back `-partial` with the same
`CacheMissError`. `observe()` records `fp`; `python3 product_repair.py
footprint [codes…]` back-fills it for open entries. Test:
`python3 tests/test_product_repair_footprint.py`.

## States and loop guard

`pending` (cells missing) → `fixable` (all cached; director event
`repair <code>: all missing cells cached — eligible for re-upgrade`) →
`done` (a clean product was ingested) or `stuck`. A re-upgrade that ran
while the code was `fixable` and still came back partial bumps
`attempts`; `MAX_ATTEMPTS = 2` → `stuck` (parked, shown in
`/process.txt`, never re-dispatched). Cells that vanish again (tombstoned
ZIP) drop the code back to `pending`. `done` entries are pruned after
7 days.

## `/process.txt`

```
repair:   pending=N fixable=F stuck=S done=D · by_defect[harmonics=… sar=…] · <code>@<state>[<products>/<tiles>t/<missing cells>c/a<attempts>] … · plan: cells=C codes=K
```

`products:` / `v2:` lines count `v2.4-partial` as its own version bucket
(`stale_v2`, "re-upgrade pending"). Processor log lines: `repair: filled
cell 14.00,47.50 harmonics`. Director events: `?q=repair`.

## Ops

```bash
python3 product_repair.py                       # repair: line + JSON summary
python3 product_repair.py scan --list           # dry run over json/ + kg_v2_store
python3 product_repair.py scan --apply          # record + relabel manifest entries '<ver>-partial'
python3 product_repair.py sweep                 # one director sweep now (states + cell plan)
python3 product_repair.py reset 12029 45311-south   # stuck → pending, attempts=0
python3 product_repair.py footprint [codes…]    # back-fill tile footprint (fp) on open entries
curl -s 'localhost:8000/process.txt?q=repair&log=200&hours=48'
```

Run `scan --apply` again only when a new defect class is added to
`REPAIRABLE_LAYERS` or after a fix that changes what counts as a gap;
restart the web service afterwards so every gunicorn worker reloads the
relabelled manifest.

## Adding a repairable layer

1. The layer must be a tile-cache product with a `has_cached` predicate
   and a `get_*` fetcher on `CopernicusTileCache` / `HansenTileCache`.
2. Add `tile_avail_key → product` to `REPAIRABLE_LAYERS`; teach
   `missing_cells()` / `_repair_cells_fill()` the fetch call if it is not
   one of the existing five.
3. `python3 product_repair.py scan --apply`, restart the web service.

Layers that do **not** belong here: anything from BEV (DTM/DSM/ortho) —
those are `upstream_fail` tiles handled by the coverage oracle, and a
re-upgrade from the full GPKG cannot invent them.

## Invariants

* `-partial` is only ever appended to a JSON product's `version`; never to
  GPKG entries, never to `_json` semantics as the completion marker.
* The fixable gate is evidence-only (remote index, `local_ok=False`).
* Cell-fill requests are gated on openEO health.
* The registry is director-side state; a director elsewhere simply
  publishes an empty plan.
