# AGENTS.md — navigation hub

Read this first, then the one doc that matches your task. Project summary
and setup: `README.md`. Security model: `SECURITY.md`.

## Where to look

| task | doc | code |
|---|---|---|
| operator API, async tasks, re-queueing | `docs/app.md` | `app.py` |
| public read API, resource limits | `docs/public-api.md` | `public_api.py`, `public_docs.py`, `service_guard.py`, `mem_lru.py` |
| NE cells (H3) — records, container, endpoints | `docs/ne-v24-design.md`, `docs/ne-public-api.md` | `ne_h3.py`, `cells_api.py`, `ne_ingest.py`, `cells_national.py` |
| public "this looks wrong" flags | `docs/public-flags.md` | `public_flags.py` |
| the Austria-wide processor, tiles, checkpoints, splitting | `docs/austria-processor.md` | `austria_processor.py`, `kg_splitter.py`, `tile_checkpoint_registry.py` |
| director (single machine / `SRTM_PEERS`) | `docs/peer-director.md` | `peer_director.py` |
| product layout v2.4, upgrades, ingest | `docs/v2.1-product-spec.md` | `v21_products.py`, `v2_verify.py`, `v2_ingest.py`, `v24_fast.py`, `kg_v2_store.py`, `kg_docs.py` |
| tree inventory (v2 live, v3 product-backed) | `docs/tree-inventory-v3.md` | `tree_inventory.py`, `trees_v3.py` |
| classifier v1 (RF) / v2 (LightGBM) | `docs/rf-training.md`, `segv2/README.md` | `learned_classifier.py`, `segv2/`, `safe_load.py` |
| search index | `docs/search-index.md` | `search_index.py` |
| Zenodo tile cache / products / lock | `docs/zenodo-cache.md` | `zenodo_cache.py`, `zenodo_tiles.py`, `zenodo_client.py`, `zenodo_lock.py`, `tile_cache.py` |
| Copernicus throttle + credentials | `docs/copernicus-throttle.md` | `copernicus.py` |
| partial products (missing cache layers) | `docs/product-repair.md` | `product_repair.py` |
| label-id collision guard | `docs/label-mix.md` | `label_mix.py` |
| sliver sweep (bbox-edge cells) | `docs/sliver-sweep.md` | `sliver_sweep.py` |
| BEV vector-tile context (`SRTM_BEV_VECTOR_CONTEXT`) | `docs/vct-pilot.md` | `vct_pilot.py`, `ne_build_tiled.py` |
| licences / attribution strings | `docs/attributions.md` | `attributions.py` |
| exceptions, logging, caches, conventions | `docs/cross-cutting-concerns.md` | — |
| algorithm background reading | `docs/reference_algorithms_summary.md` | `object_segmentation.py` |
| operator API reference (routes) | `llm.txt`, `static/docs.html` | `app.py` |

## Critical invariants (break one → wrong products or a dead machine)

1. **Type ↔ colour sync.** The object-type list and its colours exist in
   three places and must match exactly: `app.SEGMENT_COLORS`,
   `austria_processor.SEGMENT_COLORS`, `static/index.html`
   (`TYPE_COLORS`). Types: tree shrub grass hedge | water | roof greenhouse
   solar_panel | fence wall mast wind_turbine substation | road path
   parking bridge | crop orchard vineyard garden | bare_soil rock |
   excavation fill tree_loss construction earthwork | rail glacier wetland.
2. **Feature order.** `learned_classifier.FEATURE_KEYS` (v1) and the
   `feature_keys` in `data/segv2/models/*.meta.json` (v2) define the
   column order; `object_segmentation.extract_object_features` must emit
   exactly those keys. Never reorder, only append with a model retrain.
3. **`safe_load` fence.** Every pickle / `np.load(allow_pickle)` goes
   through `safe_load.load` (whitelist unpickler); model joblibs through
   `safe_load.load_model` (checksum vs `data/models_manifest.json`).
   No bare `pickle.load` anywhere.
4. **Operator gate.** Mutating routes on `app.py` are loopback-only or
   `X-Operator-Token` (`app._enforce_operator_gate`); the public tier
   relays only `public_segmentation.ROUTES`. Never add a catch-all relay.
5. **Zenodo writes are serialised** through `zenodo_lock` (`kg_upload*`
   pool, `cache` class); `_json` is the completion marker and is
   uploaded last; never rename it.
6. **Upstream credential exceptions are never swallowed**:
   `CredentialRotatedError`, `CreditsExhaustedError`, `IPThrottledError`
   propagate to the processor's top level (pause file / circuit
   breaker). Never probe Copernicus credentials from a request path.
7. **One director** per deployment (`director.lock` fcntl + `is_director`
   flag); background loops in `app.py` are fcntl single-flight across
   gunicorn workers.
8. **No property-shaped data** in products, logs or tests; the sub-KG
   unit is an H3 cell (`tests/test_cells_vocab.py`).

## Quick ops (single machine)

```bash
make check                                                   # tests + grep gates   <!-- VERIFY -->
systemctl status srv srv_public austria_processor            # the three services
curl -s localhost:8000/api/v1/ping; curl -s localhost:8100/api/v1/health
curl -s 'localhost:8000/process.txt?log=50'                  # text dashboard
curl -s localhost:8000/api/v1/processing/status | python3 -m json.tool | head
tail -f data/austria_processor/logs/processor.log
curl -s -XPOST -H 'Content-Type: application/json' localhost:8000/api/v1/processing/queue \
  -d '{"kgs":["63330"],"position":0}'                         # queue a KG (re-run trap: docs/app.md)
python3 austria_processor.py --kg 63330 --no-upload          # one KG by hand → config.OUT_DIR/<kg>/ (also when ZENODO_TOKEN unset; --dry-run = no writes to Zenodo)
python3 v2_ingest.py status; python3 ne_ingest.py status     # serving-side ingest
```

### Quick GPKG validity check

`PRAGMA integrity_check` = ok, `PRAGMA foreign_key_check` empty,
`application_id` = 0x47504B47. `_full`: ~22 `gpkg_contents` rows incl.
`segments` (EPSG:3035, 55 properties) and `segment_points` (EPSG:4326,
54). `_light_v2`: `segments`, `segment_points`, `segment_type`,
`segment_height`, `new_buildings`, `infrastructure`, `tree_apices`
(+ R-tree), `tree_crowns`, `terrain_coarse_*`; `layer_styles` rows ==
layers. Details: `docs/v2.1-product-spec.md`.

## File layout

| path | role |
|---|---|
| `config.py`, `.env.example` | the environment contract (ports, dirs, secrets, flags) |
| `app.py` | operator API, analysis engine, director host (`:8000`) |
| `public_api.py`, `public_docs.py`, `public_segmentation.py`, `cells_api.py`, `cells_bins.py`, `cells_national.py`, `landscape_query.py`, `stats_unit.py`, `hillshade_tiles.py`, `public_flags.py`, `product_segments.py`, `llm_api.py` | public tier (`:8100`) |
| `object_segmentation.py`, `learned_classifier.py`, `segv2/`, `texture_features.py`, `ndvi_harmonics.py`, `terrain_analysis.py`, `temporal_analysis.py`, `tree_inventory.py`, `trees_v3.py`, `infrastructure_lookup.py`, `power_infrastructure.py`, `osm_features.py` | pipeline |
| `raster_io.py`, `ortho_io.py`, `copernicus.py`, `hansen.py`, `hansen_coverage.py`, `bev_retry.py`, `bev_proxy.py` (direct-only stub), `http_pool.py`, `tile_cache.py`, `tile_index.py`, `zenodo_cache.py`, `zenodo_tiles.py`, `zenodo_client.py`, `zenodo_fetch.py`, `zenodo_lock.py`, `zenodo_meta_refresh.py` | data access, caches, Zenodo |
| `austria_processor.py`, `peer_director.py`, `kg_splitter.py`, `kg_universe.py`, `tile_checkpoint_registry.py`, `tile_raster_sidecar.py`, `gpkg_streamed.py`, `v21_products.py`, `v2_source.py`, `v2_verify.py`, `v2_ingest.py`, `v24_fast.py`, `kg_json_v2.py`, `kg_v2_store.py`, `kg_docs.py`, `kg_log_harvest.py`, `ne_h3.py`, `ne_ingest.py`, `ne_rollout.py`, `ne_green_fix.py`, `ne_build_tiled.py`, `vct_pilot.py`, `als_acquisition.py`, `als_years.py`, `product_repair.py`, `product_origin.py`, `label_mix.py`, `sliver_sweep.py`, `stale_blocks.py`, `json_heal.py`, `json_cache.py`, `quality_flags.py`, `feedback_db.py`, `search_index.py`, `grid25_sample.py` | processor, products, serving-side stores |
| `safe_load.py`, `service_guard.py`, `mem_lru.py`, `net_meter.py`, `host_telemetry.py`, `inbound_budget.py`, `geo_parse.py`, `attributions.py` | safety, limits, helpers |
| `static/index.html`, `process.html`, `docs.html`, `flag.js` | UI and dashboards |
| `systemd/` (`srv`, `srv_public`, `austria_processor`, `srv-watchdog`, `cells-national`, gunicorn confs) | service templates |
| `tests/`, `tools/` (`fetch_models.py`, `bench_cells_api.py`, `scan_partial_kgs.py`, `ca_check.sh`) | tests and scripts |
| `data/` shipped: `austria_processor/kg_list.json` (the only processor runtime file in git), `best_model/rf_meta.json`, `segv2/models/model_G.meta.json`, `als_acquisition/*.geojson`, `models_manifest.json`; root `kg_bbox_r1.json`. Fetched: `best_model/rf_model.joblib(.xz)`, `segv2/models/model_G.joblib`. Generated: `austria_processor/*` state, `search_index.db`, `kg_v2_store.db`, `ne/`, `tile_cache/`, `shares/`, `feedback.sqlite`, `public_flags.sqlite` | data |

Runtime state files of the processor are listed in
`docs/austria-processor.md` → *Runtime files*.
