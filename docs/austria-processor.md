# `austria_processor.py` — the Austria-wide KG processor

Walks every Katastralgemeinde (KG, `data/austria_processor/kg_list.json`,
bboxes frozen in `kg_bbox_r1.json`), runs the segmentation pipeline on
1.5 km tiles, and ships the v2.4 product set per code
(`docs/v2.1-product-spec.md`) to Zenodo — or to `SRTM_OUT_DIR` when
uploads are disabled (`config.uploads_enabled()` is false without a
`ZENODO_TOKEN`, `SRTM_DRY_RUN=1`, or `--no-upload`).

```
python3 austria_processor.py [--kg CODE | --state ST] [--retry-failed] [--no-copernicus]
                             [--dry-run] [--no-upload] [--mark-uncertain]
                             [--cache-only] [--v2-upgrade]
```

`austria_processor.service` runs `--mark-uncertain` as the user from the
repo directory (never as root); the director in `app.py` starts / stops
it (`docs/peer-director.md`).

## Process model

`main()` (parent) iterates the queue. Each KG runs in a fresh
`multiprocessing` child (`_OneShotPool`: `spawn` + `Pipe`) for memory
isolation — an OOM-killed child is an EOF on the pipe →
`PoolWorkerDied` within 5 s, the KG is retried once with the same grid.
The child writes `current_step.json` every step; the parent polls it
every 2 s (`_monitor_step_file` → `ProgressTracker` → `progress.json`,
which `GET /api/v1/processing/status` serves).

## `process_one_kg` — the pipeline

1. **bbox → tile grid**: 1.5 km tiles with 100 m overlap (~90 MB per
   tile); a tile's *core* is the tile shrunk by 50 m, objects are owned
   by the tile containing their centroid (dedup across overlaps).
2. **per tile**: read DTM/DSM (all BEV epochs), ortho RGBI, Copernicus
   NDVI harmonics / SAR / WorldCover, Hansen (`raster_io`, `ortho_io`,
   `copernicus`, `hansen`; every upstream read goes through the shared
   tile cache keyed by 0.1° cells, `docs/zenodo-cache.md`) → segment
   (`object_segmentation`: fused gradient → Felzenszwalb scale 150 → RAG
   merge 0.12 → features → RF / LightGBM → grouping) → vectorise new
   buildings + infrastructure → `tile_coarse` evidence → **checkpoint**.
3. **merge** boundary objects across tiles (`resolve edge-clipped`).
4. **full GPKG** (`build_full_gpkg_tiled`) → **light GPKG**
   (`build_light_gpkg_tiled`: segments, segment rasters, tree apices /
   crowns, terrain_coarse, NE cells → `<code>_ne.h3b`) → **JSON summary**
   (`build_json_summary_tiled`, grid25 indexes, `ne`, top-N) →
   `compute_data_quality` → `validate_kg_outputs` / `v2_verify` →
   `upload_kg_to_zenodo` (order full → light_v2 → ne_h3b → json_v2 →
   json).

**Tile checkpoints** `tile_checkpoints/<kg>/tile_N.pkl` (metadata
pickles, restored only through `safe_load.py`'s whitelist unpickler) let
an aborted KG resume; `_remap_restored_tile_ids` keeps label ids
consistent (`docs/label-mix.md`). Raster sidecars
`tile_N/raster/*.npy` (local only, written while
`SIDECAR_MIN_FREE_GB` ≥ 1 GB free) let the full-GPKG step mmap instead
of re-reading BEV. With uploads enabled, checkpoints of an aborted KG are
also archived to the Zenodo cache deposit (`tile_checkpoint_registry.py`,
`chkpt_<kg>.tar.gz`, `cache_manifest` entries, size 0 = tombstone) so a
rerun on another machine resumes too.

**Retry policy**: a tile timeout retries once with the same grid; a
Copernicus failure splits the request 2×2 then 4×4 (quadrants); the tile
grid itself is never subdivided (products must be reproducible).
Transient 5xx → the KG is deferred 5 positions, twice (`deferred_kgs.json`).

## KG block splitting

A KG with > 28 tiles is split into directional blocks
(`kg_splitter.maybe_split_kg`: `49006-south`, `49006-north`,
`80110-southwest-1`, …); each block is a product code with its own
product set, the parent is complete when the blocks cover it
(`app._kg_coverage_complete`). A runtime oversize guard
(`MAX_TILES_PER_BLOCK × RUNTIME_SPLIT_FACTOR` = 22 × 1.3 → 29 tiles)
forces a split before any tile runs. `search_index._enrich_kg_from_blocks`
aggregates blocks into the parent row; `ne_ingest` merges block h3bs.
Strikes per code live in `kg_strikes.json`.

## Modes

| flag / env | effect |
|---|---|
| `--cache-only` / `COPERNICUS_FORBIDDEN=1` | never call Copernicus; a tile not in the tile cache raises `CacheMissError` and the KG is re-queued (`cache_miss_kgs.json`) |
| `--v2-upgrade` | process upgrade units from existing full GPKGs (`docs/v2.1-product-spec.md` § 6) |
| `--no-copernicus` | skip Copernicus layers entirely (features nullable, product labelled partial) |
| `SRTM_BEV_VECTOR_CONTEXT=1` | build NE declared layer + footprint context from BEV vector tiles (`docs/vct-pilot.md`) |
| `--no-upload` | write products to `config.OUT_DIR/<kg>/` (default `out/`); automatic when no `ZENODO_TOKEN` is configured |
| `--dry-run` / `SRTM_DRY_RUN=1` | never touch Zenodo (no uploads, no manifest edits); products under `config.OUT_DIR/<kg>/` |

Upstream hosts are limited to `config.OUTBOUND_ALLOWED_HOSTS`
(`http_pool.py`); there are no proxies (`bev_proxy.py` is a direct-only
stub kept for import compatibility, `PROXIES_REMOVED = True`).

## Zenodo upload watchdog

`zenodo_client.ProgressFileWrapper` / `_check_stall` watch every PUT
≥ 32 MB: 5 min grace, *dead* = 0 bytes for 10 min, *slow* = a 30-min
window below the floor (`max(20 KB/s, 0.25 × median of recent PUTs)`),
skipped once ≥ 90 % is sent; at most one slow abort per file. Test:
`tests/test_put_watchdog.py`.

## Failure modes

| symptom | cause / handling |
|---|---|
| `IPThrottledError` | Copernicus throttled this IP → `copernicus_paused` file, 15-min polls (`docs/copernicus-throttle.md`) |
| `CreditsExhaustedError` / `CredentialRotatedError` | credential problem — never swallowed; processor pauses until `/api/v1/credentials` changes |
| `PoolWorkerDied` | child OOM; check `journalctl -u austria_processor`, `MemoryMax` in the unit |
| sync download timeout | openEO 240 s cap → batch job fallback |
| Hansen missing (west Vorarlberg) | non-fatal, layer nullable |
| 0-byte cache files | deleted on read, re-fetched |
| disk < 3 GB | `check_disk_space` LRU-evicts the local tile cache, pauses below the floor |
| stale `progress.json` | processor not running; `systemctl status austria_processor` |

## Processing API (`app.py`, loopback)

| route | |
|---|---|
| `GET /api/v1/processing/status` | progress.json + current step + queue summary |
| `POST …/start` `{cache_only?, v2_upgrade?}` / `…/pause` / `…/resume` / `…/stop` | lifecycle |
| `POST …/single?kg=` / `…/retry?kg=` | one KG now / retry a failed one |
| `GET/POST …/queue` | priority queue (`retry_queue.json`); re-queue recipe in `docs/app.md` |
| `GET …/log?n=` | processor log tail |
| `GET …/manifest` / `…/cache_manifest` | Zenodo manifests |
| `GET …/throttle` | Copernicus throttle state |
| `GET …/partial_kgs` / `…/kg_strikes` / `…/cache_misses` | diagnostics |
| `POST /api/v1/admin/clear_tile_checkpoints?kg=` | drop a KG's checkpoints (local + registry) |

```bash
tail -f data/austria_processor/logs/processor.log
curl -s localhost:8000/api/v1/processing/status | python3 -m json.tool | head -30
# restart at a specific KG: edit retry_queue.json, rm in_progress_kg.txt, restart the unit
rm -rf data/austria_processor/tile_checkpoints/<kg>/      # clear local checkpoints
```

## Runtime files (`data/austria_processor/`)

Shipped: `kg_list.json`; generated: `progress.json`, `current_step.json`,
`in_progress_kg.txt`, `subprocess_warnings.jsonl`, `logs/`,
`tile_checkpoints/`, `checkpoint_registry.json`, `zenodo_manifest.json`
(`{entries: {key: {depo_id, size, md5, version, status, uploaded_at}}}`),
`cache_manifest.json`, `zenodo_zip_index/`, `failed_kgs.json`,
`retry_queue.json`, `deferred_kgs.json`, `kg_strikes.json`,
`cache_miss_kgs.json`, `copernicus_credential_usage.json`,
`copernicus_paused`, `openeo_circuit.json`, `product_repair.json`,
`label_mix.json`, `sliver_sweep.json`, `vct_pilot.jsonl`, `v2_source/`,
`gpkg/`, `json/`.

## Type letters (log lines / `segment_type` legend)

t=tree s=shrub g=grass h=hedge w=water R=roof G=greenhouse P=solar_panel
F=fence W=wall M=mast T=wind_turbine X=substation r=road p=path k=parking
b=bridge c=crop o=orchard v=vineyard a=garden B=bare_soil K=rock
E=excavation L=fill l=tree_loss C=construction e=earthwork u=unclassified
i=rail I=glacier m=wetland.
