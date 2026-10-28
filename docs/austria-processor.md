# Austria Processor — Mental Model

**Read this section first when working on the processor. It will save you hours.**

The Austria Processor (`austria_processor.py`, ~14.6k lines) processes all
7 850 Austrian Katastralgemeinden (KGs) into landscape analysis products
(v2.4: cadastre-free, see `docs/v2.4-rollout.md`) and uploads them to Zenodo.
It runs for weeks as a background systemd service.

### The Big Picture (5-second version)

```
main()                           ← parent process, iterates KGs
  └─ for each KG:
       └─ _OneShotPool (spawn Process + Pipe)  ← child process (memory isolation)
            └─ process_one_kg()
                 ├── fetch cadastre (run-time model context only, never a product)
                 │   + NE declared layer (ne_h3.fetch_declared) → compute bbox
                 ├── tile grid (1.5km, 100m overlap)
                 ├── for each tile:
                 │     ├── read LiDAR (DTM/DSM, 3 dates)
                 │     ├── read ortho (RGBI)
                 │     ├── fetch Copernicus (NDVI/SAR/WorldCover)
                 │     ├── fetch Hansen (forest change)
                 │     ├── segment (Felzenszwalb + RF classify)
                 │     ├── vectorise new buildings + infrastructure
                 │     └── checkpoint tile to disk
                 ├── merge tile results (dedup at boundaries)
                 ├── build full GPKG (all raster layers + vectors)
                 ├── build light GPKG (segments, tree_apices/crowns, terrain_coarse_*,
                 │   NE observed cells → <code>_ne.h3b)   [v2: _light_v2.gpkg]
                 ├── build JSON summary (KG-level stats, grid25, ne section)
                 ├── validate outputs
                 └── upload to Zenodo
```

### Architecture Decisions (why it's built this way)

1. **Subprocess per KG**: Each KG runs in `_OneShotPool` (one spawn
   `Process` + one `Pipe`; a `Pool(1)` look-alike: `apply_async().get(timeout)`,
   `close/join/terminate`, `_pool`) so memory is fully reclaimed between KGs.
   The parent only does orchestration + Zenodo upload.
   **Why not `multiprocessing.Pool(1)`** (used until Oct 2026): a SIGKILLed
   worker (cgroup OOM) was silently replaced and its task lost (16 h wait,
   fixed by `_pool_worker_died`), and then `Pool.terminate()` deadlocked in
   `_help_stuff_finish` because the respawned worker — created by the Pool's
   handler *thread* — got `PR_SET_PDEATHSIG`-killed when that thread exited,
   while holding `inqueue._rlock`. Zombie child + frozen parent until the
   director's 12 h stuck-KG watchdog. Details: `docs/peer-director.md →
   Stuck-KG watchdog`. A dead `_OneShotPool` child is an EOF on the pipe →
   `PoolWorkerDied` within one 5 s slice.

2. **Tiling**: KGs vary from <1km to 27km across. Everything is processed as
   overlapping 1.5km tiles to cap memory at ~90MB/tile. Tiles overlap by 100m
   so edge objects aren't truncated. Dedup uses centroid-ownership (core zone =
   tile shrunk by 50m on overlap sides).

3. **Tile checkpoints** (two tiers, deployed dd08d74):

   **Metadata tier** — each completed tile is pickled to
   `data/austria_processor/tile_checkpoints/<kg>/tile_N.pkl`. Carries the
   full per-tile result (segmentation, classified objects, vector cadastre,
   NDVI/SAR/Hansen extracts — *everything except raw rasters*). KB–MB per
   tile. Restored on same-peer crash/restart.

   **Cross-peer registry** — when a KG is aborted/deferred mid-flight
   (BEV exhaustion, cred rotation, timeout, postpone, role eviction), the
   parent gzip+tars all tile pickles and uploads to the shared Zenodo
   cache deposit as `chkpt_<kg>.tar.gz`. The next peer that picks up the
   KG downloads + unpacks them inside `process_one_kg()` before the tile
   loop runs. So BEV-expensive aborts are not wasted across peers. See
   `tile_checkpoint_registry.py`. Fleet visibility: cache_manifest.json
   carries `chkpt_*` entries (size>0 = present, size=0 = tombstone
   = deleted on completion) and propagates fleet-wide every 5 min via the
   existing `_sync_peer_data` cycle — no new traffic.

   **Raster sidecars (Phase 1)** — per-peer local only, never uploaded.
   After each tile reads DTM/DSM/nDSM + multi-date DTM/DSM + ortho RGB+NIR,
   `tile_raster_sidecar.persist_dtm_dsm/persist_ortho` writes `.npy` files
   under `tile_checkpoints/<kg>/tile_N/raster/`. `build_full_gpkg_tiled`
   mmaps these instead of re-reading BEV, eliminating 6+ BEV passes per
   tile during the `gpkg_full` step (was the longest-running step
   pre-deploy at 5–7 h/peer). Gated on free disk ≥ `SIDECAR_MIN_FREE_GB`
   (env-overridable, default 8 GB) — silent no-op below that, with the
   BEV re-read fallback. Bulk-released at the existing tile-checkpoint
   free point after `gpkg_full` succeeds.

   All three tiers are deleted after successful KG completion (raster
   sidecars also released on Zenodo-upload-failure rebuild path). Audit
   trail in the merged 24h log via `?q=chkpt`.

4. **Parent-child communication**: The subprocess writes step progress to
   `data/austria_processor/current_step.json` (atomic temp+rename). A parent
   thread (`_monitor_step_file`) polls this every 2s and feeds it into
   `ProgressTracker` → `progress.json` → dashboard API → `process.html`.

5. **Retry**: On timeout (90min), the parent retries once with the same
   tile grid — checkpoints restore completed tiles, only the interrupted
   tile is re-processed. Copernicus handles its own failures internally
   via `_quadrant_split()` (2×2 then 4×4). Tile grid is **never** subdivided.

6. **Grid-snapped caches**: Remote data (Copernicus, Hansen) is cached in a
   regular grid so adjacent KGs share cached tiles. See `tile_cache.py`.

### The Two Processes

| | Parent (`main()`) | Child (`process_one_kg()`) |
|---|---|---|
| **Runs in** | `austria_processor.service` | `_OneShotPool` (spawn `Process`) |
| **PID** | Long-lived | New per KG |
| **Responsible for** | KG iteration, retry logic, Zenodo upload, progress tracking, signal handling | All data I/O, segmentation, GPKG/JSON building |
| **Communicates via** | `current_step.json`, `subprocess_warnings.jsonl`, return dict | Same files (writes them) |
| **Memory** | ~100MB | Up to 3GB (MemoryMax enforced by systemd) |
| **Timeout** | Enforces 30/90min via `async_result.get(timeout=)` | No awareness of timeout |

### Code Map of austria_processor.py

All sections are marked with `# === SECTION: ... ===` comments. Use `grep -n '# ===' austria_processor.py` to orient yourself.

| Marker | Key contents |
|--------|---------------|
| `Config` | `DATA_DIR`, `ZENODO_TOKEN`, `KG_TIMEOUT_SECONDS`, tile cache init |
| `Disk cache management` | `check_disk_space()`, `_lru_delete()` |
| `Logging` | File + stderr handlers |
| `ProgressTracker` | JSON-backed state class: `set_step()`, `add_log()`, `record_success()`, `update_rates()` |
| `Circuit breaker` | `_read_circuit_breaker()` — openEO rate-limit protection |
| `Geometry helpers` | `transform_to_3035/wgs()` |
| `KG list` | `get_all_kgs()` — one GET umfeld `/api/v1/kgs` (ETag-cached, universe-hash asserted), 7850 KGs |
| `Cadastre data fetching` | `fetch_cadastre_data()` — umfeld-at `/k` export (`umfeld.py`), or `fetch_cadastre_data_bevdirect()` in bev mode; ≤24 h `cadastre_cache/`; **model context only** (`CADASTRE_PRODUCTS = False` since 2.4) |
| `Height enrichment` | `enrich_parcels_with_heights()`, `enrich_buildings_with_heights()` — dead code since 2.4 (no callers) |
| `Vectorise unmatched segments` | `vectorise_unmatched_buildings()`, `vectorise_infrastructure()` |
| `Resolve edge-clipped features` | `resolve_edge_clipped_features()` — tile boundary fixup |
| `GPKG style + vector writers` | `_write_segment_vectors()`, `_write_segment_points()`, `_write_gpkg_all_styles()` |
| `Tiled GPKG + JSON builders` | **`build_full_gpkg_tiled()`**, **`build_light_gpkg_tiled()`**, **`build_json_summary_tiled()`** |
| `Data quality scoring` | `compute_data_quality()` |
| `process_one_kg()` | **The main per-KG pipeline** (runs in subprocess, see flow above) |
| `Output validation` | `validate_kg_outputs()` |
| `Zenodo upload helpers` | `upload_kg_to_zenodo()` |
| `JSON dir cleanup` | 4GB cap, LRU eviction |
| `main()` | **KG iteration, retry ladder, subprocess management** |

### Key Modules Called by the Processor

| Module | Size | What it does for the processor |
|--------|------|-------------------------------|
| `tile_cache.py` | ~950L | Grid-snapped 0.1° caches for Copernicus + Hansen. `CopernicusTileCache`, `HansenTileCache`, `order_kgs_nearest_neighbor()`. Zenodo fallback on miss. Re-raises `IPThrottledError`/`CreditsExhaustedError` (never swallows them). |
| `zenodo_cache.py` | ~1530L | Persistent cache on Zenodo. Uploads tiles as ZIP archives (one per product × lat strip). Downloads via HTTP range reads (2-3 requests per tile). Separate `cache_manifest.json`. |
| `object_segmentation.py` | 2218L | `segment_objects_in_area()` — Felzenszwalb + RAG + RF classify. Called once per tile. |
| `copernicus.py` | ~1300L | openEO client: NDVI, WorldCover, SAR, harmonics. 4-credential rotation, sync→batch fallback, `IPThrottledError`/`CreditsExhaustedError` propagation. See **Copernicus Throttle & Retry** section. |
| `ortho_io.py` | 992L | BEV orthophoto reader (RGBI, 47 Operates, DOP fallback). |
| `raster_io.py` | 359L | Windowed reads from BEV GeoTIFFs via `/vsicurl/`. |
| `hansen.py` | 453L | Hansen Global Forest Change data reader. |
| `cadastre.py` | ~580L | Building footprints + parcel boundaries as segmentation ground truth / `building_mask` — run-time context only, never written to a product. |
| `terrain_analysis.py` | 157L | Slope, aspect, TRI, curvature from DTM. |
| `learned_classifier.py` | 559L | RF model loading + 44-feature classification. |
| `zenodo_client.py` | 841L | Zenodo deposit creation, file upload, publish. `Manifest` class for tracking. |
| `bev_retry.py` | 252L | Exponential backoff + proxy rotation for `rasterio.open()`. Proxy pool is free HTTPS proxies from GitHub lists — works for BEV GeoTIFFs, **not useful for Copernicus** (openEO auth is per-credential, not per-IP). |

### Data Flow Through a Single Tile

```
BEV servers ──→ raster_io / bev_retry ──→ DTM, DSM (1m)
                                           ↓
BEV servers ──→ ortho_io ──→ RGBI (0.2m)  │
                               ↓           ↓
openEO ──→ tile_cache ──→ NDVI/SAR/LC    terrain_analysis
                            ↓                ↓
UMD ──→ tile_cache ──→ Hansen           object_segmentation
                            ↓           ↙ ↓ ↘
cadastre API ──→ cadastre ──→ ground truth  features  labels
                                              ↓
                                    learned_classifier (RF)
                                              ↓
                                    classified objects
                                    ↙          ↓          ↘
                        vectorise_*     tile_seg_result    terrain_stats
```

### Persistence & Recovery

| File | Written by | Read by | Purpose |
|------|-----------|---------|--------|
| `progress.json` | Parent (ProgressTracker) | Dashboard API | Live state: current KG, step, rates, log |
| `current_step.json` | Child (`_report_step()`) | Parent (`_monitor_step_file` thread) | IPC: step name + detail + tile index |
| `subprocess_warnings.jsonl` | Child (`_WarningRelayHandler`) | Parent (`_monitor_step_file`) | WARNING/ERROR log relay |
| `in_progress_kg.txt` | Parent | Parent (on restart) | Crash recovery: re-process interrupted KG |
| `tile_checkpoints/<kg>/tile_N.pkl` | Child | Child (on retry); other peers (via Zenodo chkpt registry) | Resume from last completed tile |
| `tile_checkpoints/<kg>/tile_N/raster/*.npy` | Child (per-tile loop) | Child (`gpkg_full` step, mmap) | Skip BEV DTM/DSM/ortho re-reads in `gpkg_full` (Phase 1, dd08d74) |
| `checkpoint_registry.json` | Parent (uploading peer only) | Primary `/process.txt` (read via `cache_manifest.json` mirror) | LRU registry of tile-tars uploaded to Zenodo — `{kg: {ts, n_tiles, bytes, name}}`, cap `MAX_REGISTRY_KGS=200` |
| `zenodo_manifest.json` | Parent (`Manifest`) | Parent + Dashboard | Upload tracking (success/error per KG) |
| `failed_kgs.json` | Parent | Parent (on restart) | Permanently-failed KGs to skip |
| `retry_queue.json` | API / transient handler | Parent (each iteration) | KG codes to insert next in queue (read + cleared) |
| `peer_urls.txt` | deploy.sh / director | app.py (`_sync_peer_data`), director | Peer URLs for data sync + director bandwidth polling |
| `peers.json` | Director API / manual | `peer_director.py` | Peer config: IDs, URLs, enabled, not_before, budget |
| `director_state.json` | Director loop | Director loop | Runtime: active peer, bandwidth per peer, mode |
| `director.lock` | Director loop | Director loop | fcntl lock — single director across gunicorn workers |
| `is_director` | Manual (primary only) | `app.py` (startup) | Flag: director loop only runs if this file exists |
| `kg_list.json` | Parent | Parent | Cached list of 7850 KGs (+ `kg_list.etag`; refreshed ≤ hourly via conditional GET) |

### Operations

```bash
# --- Logs ---
tail -f data/austria_processor/logs/processor.log
grep -i "warning\|error\|failed" data/austria_processor/logs/processor.log | tail -20

# --- Status ---
curl -s http://localhost:8000/api/v1/processing/status | python3 -m json.tool

# --- Restart ---
sudo systemctl stop austria_processor && sudo systemctl start austria_processor

# --- Dashboard ---
# https://example.invalid:8000/process.html
```

### Restarting at a Different KG

To stop the processor and restart it beginning with a specific KG (e.g. to
prioritise a particular area):

```bash
# 1. Stop the processor
sudo systemctl stop austria_processor

# 2. Clean up stale in-progress marker (prevents re-processing the interrupted KG)
rm -f data/austria_processor/in_progress_kg.txt

# 3. Edit the retry queue — items here are processed before the normal NN order
#    Put desired KG code(s) at the front of the list.
python3 -c "
import json
q = json.load(open('data/austria_processor/retry_queue.json'))
# Remove target KG if already in queue, then prepend
for code in ['TARGET_CODE']:
    if code in q: q.remove(code)
q = ['TARGET_CODE'] + q
json.dump(q, open('data/austria_processor/retry_queue.json', 'w'))
print('Queue now:', len(q), 'items, starts with', q[:3])
"

# 4. (Optional) Remove tile checkpoints if the KG was mid-processing
#    This forces a clean restart for that KG. Skip if the KG wasn't in progress.
#    NOTE: this drops the metadata pickles AND the local raster sidecars
#    (Phase 1). If the same KG has a chkpt_*.tar.gz on Zenodo it will be
#    re-downloaded into the dir before the tile loop runs — if you want a
#    truly clean restart, also evict the Zenodo bundle (single-peer admin
#    op, normally only needed on schema bumps):
#      python3 -c "import tile_checkpoint_registry as r; r.delete_kg('TARGET_CODE')"
rm -rf data/austria_processor/tile_checkpoints/TARGET_CODE/

# 5. Start the processor — it reads retry_queue.json first
sudo systemctl start austria_processor

# 6. Verify
tail -5 data/austria_processor/logs/processor.log
```

Replace `TARGET_CODE` with the KG code (e.g. `91109`). The retry queue is
consumed in order and prepended to the normal nearest-neighbor traversal.
Multiple KGs can be queued — just put them all at the front of the list.

### Re-processing a KG (after bad output or code fix)

```bash
# 1. Stop processor
sudo systemctl stop austria_processor

# 2. Delete Zenodo draft depositions (get depo_ids from manifest)
python3 -c "
import json, requests
TOKEN = '...'  # from austria_processor.py line 48
m = json.load(open('data/austria_processor/zenodo_manifest.json'))
entries = m.get('entries', m)
for k in list(entries.keys()):
    if 'KGCODE' in k:
        requests.delete(f'https://zenodo.org/api/deposit/depositions/{entries[k][\"depo_id\"]}',
                       params={'access_token': TOKEN})
        del entries[k]
json.dump(m, open('data/austria_processor/zenodo_manifest.json', 'w'), indent=2)
"

# 3. Remove local JSON + tile history
rm -f data/austria_processor/json/KGCODE.json
python3 -c "
import json
f = 'data/austria_processor/tile_history.json'
d = json.load(open(f))
d.pop('KGCODE', None)
json.dump(d, open(f, 'w'))
"

# 4. Clear search index entry
python3 -c "
import sqlite3
db = sqlite3.connect('data/search_index.db')
db.execute('UPDATE kg SET processed=0, zenodo_json_url=NULL, zenodo_json_size=NULL, zenodo_light_gpkg_url=NULL, zenodo_light_gpkg_size=NULL, zenodo_full_gpkg_url=NULL, zenodo_full_gpkg_size=NULL, zenodo_depo_id=NULL WHERE kg_code=?', ('KGCODE',))
db.execute('DELETE FROM kg_landcover WHERE kg_code=?', ('KGCODE',))
db.execute('DELETE FROM kg_hansen WHERE kg_code=?', ('KGCODE',))
db.commit()
"

# 5. Remove tile checkpoints if they exist
rm -rf data/austria_processor/tile_checkpoints/KGCODE/

# 6. Restart — KG will appear in pending queue
sudo systemctl start austria_processor
```

Replace `KGCODE` with the KG code (e.g. `91109`). Depositions must be
unpublished (draft) to delete via API. The KG will be re-queued via
nearest-neighbor ordering relative to the last completed KG.

### Common Failure Modes & Fixes

| Symptom | Cause | Fix |
|---------|-------|-----|
| `IPThrottledError` / all 4 creds 402 | Copernicus rate-limiting (transient, all probes pass) | **Automatic**: aborts KG, writes `copernicus_paused`, polls every 15 min, re-queues KG. See **Copernicus Throttle & Retry**. |
| `CreditsExhaustedError` | Genuine credit exhaustion (probe fails) | Update `_CREDENTIALS` in `copernicus.py`, delete `data/austria_processor/copernicus_paused` |
| Timeout on large KG | Copernicus slow (NDVI downloads) | Automatic: retry once with checkpoints, then deferred retry 5 KGs later if transient |
| OOM kill | KG worker exceeds the `systemd-run` scope (`MemoryMax=8G`, `MemoryHigh=7G`) | Automatic: parent detects the dead Pool worker within 5 s (`PoolWorkerDied`, message carries `cgroup oom_kill=… peak=…`) → "worker killed (OOM?) at <step>" failure path. Before `c8a3952` the lost task hung until `KG_TIMEOUT_SECONDS` (16 h) and the director's 12 h stuck-KG watchdog did the clean-up. Known OOM-sized steps fixed there: `ne_h3.build_observed` (bbox-clipped cells), `_merge_boundary_segments` (vectorised). |
| `Synchronous download timed out` | Normal Copernicus behavior | No action — falls back to batch job automatically |
| `Hansen resample failed` | No Hansen data (western Vorarlberg) | Non-fatal, skipped. Hansen data sparse at AT borders |
| 0-byte cache files | Interrupted before atomic-write fix | Delete the 0-byte `.tif`/`.npz` files manually |
| Disk critically low | Caches filling disk | Automatic: `check_disk_space()` does LRU cleanup, pauses if <3GB |
| Stale `progress.json` | Service died without cleanup | Restart clears stale state in `main()` init block |
| Transient server error (500/503/timeout) | Remote service hiccup | Automatic: deferred retry 5 KGs later (up to 2×), then permanent fail |

### Type letters (`object_segmentation.TYPE_LETTER`)

Compact single-char type codes (lowercase = natural, uppercase = man-made);
moved here from the deleted `parcel_compact.py` (Kataster purge 2026-10-13).
Only referenced today by the never-executed per-parcel block in
`build_json_summary_tiled` (`CADASTRE_PRODUCTS = False`) — kept as the
canonical letter table:
```
t=tree s=shrub g=grass h=hedge w=water R=roof G=greenhouse P=solar_panel
F=fence W=wall M=mast T=wind_turbine X=substation r=road p=path k=parking
b=bridge c=crop o=orchard v=vineyard a=garden B=bare_soil K=rock
E=excavation L=fill l=tree_loss C=construction e=earthwork u=unclassified
i=rail I=glacier m=wetland   (v2 classes)
```
The per-parcel compact layout (`parcels.details[i]`, frav/top_objs/
top_trees per parcel), `backfill_parcel_top10.py` and the index-time
per-parcel auto-classification are gone — `docs/kataster-purge.md`.

### Per-KG Outputs

Manifest keys per product code (parent `61047` or block `04016-southwest`); details in `docs/v2-upgrade.md → Products` and `docs/v2.4-rollout.md`:

1. **Full GPKG** `<code>_full.gpkg` (`_full_gpkg`): DTM/DSM/nDSM + multi-date, ortho/CIR/NDVI/SAR/Hansen/WorldCover, segment_type/height rasters, `segments` + `segment_points` vectors. Fresh runs only; upgrade input.
2. **Light GPKG v2** `<code>_light_v2.gpkg` (`_light_gpkg_v2`): `segments`, `segment_points`, `segment_type`, `segment_height`, `new_buildings`, `infrastructure`, `tree_apices` (+R-tree), `tree_crowns`, `terrain_coarse_*` 5 m. **No `parcels` / `buildings` / `parcel_outline_z`** (`v2_verify.FORBIDDEN_LIGHT_LAYERS`).
3. **NE cells** `<code>_ne.h3b` (`_ne_h3b`): H3 observed layer NEO1/NET1/NEB1, `ne_h3.py`. May be absent when the declared layer wasn't available → `-partial` label, repaired later.
4. **JSON v2** `<code>_v2.json.gz` (`_json_v2`): `kgjson/2` compact doc, primary ingests it (`v2_ingest`). Contains the `ne` section, `cadastre_free: true`.
5. **JSON v1-shaped** `<code>.json` (`_json`): cadastre-stripped summary — still the fleet-wide **completion marker** (`_kg_coverage_complete`, `completed_codes`, zen_stall, dashboard).

Upload order: full → light_v2 → ne_h3b → json_v2 → json (`_v2_upload_and_finish`). Legacy `_light_gpkg` records survive with `status=removed` (size 0); v1 `_json` written before 2.4 is re-PUT `stripped`.

### Zenodo PUT throughput watchdog (`zenodo_client.ProgressFileWrapper`)

Added fb4b31b (2026-10-28) as a fixed 200 KB/s floor; made **fleet-relative**
the same day after Zenodo drained 50–150 KB/s for *every* peer for hours
(full-GPKG uploads median 5.2 h vs ~1 h) and a re-PUT landed at the same rate
— a fixed floor would have restarted 2 GB PUTs from byte 0 forever. Zenodo is
flaky; we work around it, we don't fight it. Only PUTs ≥ `_LARGE_PUT_BYTES`
(32 MB) are watched.

**Rules** (`_check_stall`, evaluated on every body read ~1/s):

| rule | fires when | notes |
|---|---|---|
| grace | — | nothing fires in the first 5 min (`_STALL_GRACE_S`) |
| **dead** | 0 bytes drained for ≥ 10 min (`_STALL_DEAD_S`) | always — ignores floor, 90 % skip, steady and the abort cap; that is a dead connection, not slowness |
| **slow** | trailing 30 min (`_STALL_WINDOW_S`) averaged `< floor` | skipped at ≥ 90 % sent; **never** when the window is *steady* (every ~60 s bucket progressed and the window averaged ≥ ABS_MIN) or when `remaining / rate < time already sunk` (restart would cost more than finishing) |
| cap | — | at most **one** slow-abort per file (`_STALL_MAX_SLOW_ABORTS`); the next ladder attempt runs with only the dead rule, so a file can't loop |

`floor = max(ABS_MIN, FRAC × fleet_median)`; ABS_MIN 20 KB/s, FRAC 0.25 —
director knobs `zenodo_put_floor_abs_kbps` / `zenodo_put_floor_frac`
(`POST /api/v1/director/knobs`). No fleet figure → ABS_MIN only.

**Fleet median, bandwidth-free**: every uploading peer already pushes
`current_kg.upload_progress {file,sent,total}` on its 30 s status push.
`peer_director.note_put_progress` turns consecutive pushes into a per-upload
span rate (flock'd `/tmp/srtm_peer_push/_put_rates.json`, cross-worker);
`fleet_put_rate()` takes the median over uploads active in the last 5 min
(span ≥ 60 s), smooths it as the median of the last 15 min of per-tick
medians, and the result rides back in the **peer_status ack** (`zput:
{med,n,floor,abs,frac}`, ~30 B — no new poll, no new push). The peer
mirrors it to `data/austria_processor/zenodo_fleet_put.json`
(`app._store_fleet_put`, rewritten only on change / every 5 min); the
primary writes its own mirror in `fleet_put_rate()`.
`zenodo_client.fleet_put_floor()` reads it (stale > 30 min → ABS_MIN).

**Surfacing**: `/process.txt` → `zenodo_uploads: … watchdog: floor=<KB/s>
fleet_med=<KB/s> (n=<active uploads>) aborts_1h=N`; abort log line
`PUT <file> aborted by throughput watchdog (attempt k/6, slow|dead,
slow_aborts=1/1): … floor=… fleet_med=… abs_min=…` (grep `?q=watchdog`).
Cross-check the fleet median against the committed GB/h on the same line.

Trade-off to know: the steady protection means a monotone connection at
≥ ABS_MIN is never cut even when it is an outlier against a fast fleet — the
finishing-cheaper rule plus the 90 % skip are what bound the cost; raise
`zenodo_put_floor_abs_kbps` if a slow-but-steady outlier class reappears.
Tests: `python3 tests/test_put_watchdog.py` (synthetic timelines).

### API Endpoints (served by app.py, read processor state)

| Method | Path | Purpose |
|--------|------|----------|
| GET | `/api/v1/processing/status` | Processor progress (polled by process.html) |
| POST | `/api/v1/processing/start` | Start processor (optional: state=, kg=) |
| POST | `/api/v1/processing/pause` | Pause processor (SIGSTOP) |
| POST | `/api/v1/processing/resume` | Resume processor (SIGCONT) |
| POST | `/api/v1/processing/stop` | Stop processor (SIGTERM) |
| POST | `/api/v1/processing/single?kg=X` | Process single KG |
| POST | `/api/v1/processing/retry?kg=X` | Retry failed KG |
| GET\|POST | `/api/v1/processing/throttle` | Get/toggle bandwidth throttle (skip GPKG uploads) |
| GET | `/api/v1/processing/peers` | Peer coordination state (completed, current, priority, failed, manifest) |
| GET | `/api/v1/processing/peers/status` | Combined status across all peers (instances, combined_completed, rates) |
| GET | `/api/v1/processing/log` | Recent processor log lines |
| GET | `/api/v1/processing/manifest` | Zenodo manifest entries |
| GET\|PUT | `/api/v1/processing/cache_manifest` | Zenodo tile-cache manifest (shared across peers) |
| GET | `/api/v1/kg/<kg_code>` | KG JSON summary (local or Zenodo link) |
| POST | `/api/v1/processing/queue` | Re-queue with `{kgs, position, skip_processed:false}` — see `docs/app.md → Re-queueing` |

### Files

| File | Size | Purpose |
|------|------|----------|
| `austria_processor.py` | ~14.6kL | Main processor (this section documents it) |
| `zenodo_client.py` | 841L | Zenodo API client + `Manifest` class |
| `zenodo_cache.py` | ~1530L | Zenodo-backed persistent cache for Copernicus+Hansen tiles. See detailed section above. |
| `tile_cache.py` | 900L | Grid-snapped Copernicus + Hansen caching, Zenodo fallback on miss |
| `austria_processor.service` | — | systemd unit (MemoryMax=4G, Restart=on-failure). Disabled on peers. |
| `peer_director.py` | ~15kL | Peer Director — orchestration across VMs (`docs/peer-director.md`) |
| `deploy.sh` | ~160L | One-command deployment for new peer VMs |
| `static/process.html` | ~6300L | Dashboard UI (status, map, log, Zenodo manifest, peer director cards) |
| `data/austria_processor/MONITOR.md` | — | Monitoring checklist + expected timelines |
| `gpkg_streamed.py` | ~500L | Strip-streamed full-GPKG builder for large KGs (auto-used >100 Mpx, skipped >200 Mpx) |
| `kg_splitter.py` | ~300L | KG block splitter — splits large KGs (>28 tiles) into contiguous blocks with directional names |

### KG Block Splitting

Large KGs (>28 tiles ≈ >42 km²) are automatically split into smaller contiguous
blocks for processing. Each block is named with a directional suffix:

```
49006 Innerbreitenau  →  49006-south  (28 tiles)
                         49006-north  (28 tiles)

80110 Sölden          →  80110-southwest-1, 80110-southeast-1, ... (16 blocks)
```

**How it works:**
- `kg_splitter.maybe_split_kg(kg)` checks tile count and splits bbox into grid blocks
- Splitting happens in `main()` when building the pending list — transparent to `process_one_kg`
- Each block processes only its own bbox (tiles); NE cells carry the parent's full cell set with unseen cells = nodata
- Blocks are stored on Zenodo as `49006-south.json`, `49006-south_light.gpkg`, etc.
- The search index aggregates all blocks into one parent KG row (`_enrich_kg_from_blocks`)
- The KG counter counts unique parent codes, not blocks
- Priority queue: putting `49006` in queue processes all its blocks

**Files affected by splitting:**
- `kg_splitter.py` — splitting logic, block code utilities
- `austria_processor.py` — block expansion in `main()`, bbox clipping in `process_one_kg()`
- `search_index.py` — `_enrich_kg_from_blocks()` aggregates block JSONs into parent row
- `app.py` — queue API resolves block codes via parent KG

**Adaptive splits (strikes) & runtime oversize guard (Jul 2026):**

The splitter's tile estimate uses the KG **API bbox**, but
`process_one_kg` expands the bbox to the cadastre union before computing
the real grid — some KGs blow past the estimate only at runtime (91017
Schröcken: 20 estimated → 30 runtime tiles). Such KGs overran peer disk
(`disk_low` handoffs) and wedged `gpkg_full` (stuck-kg hard restarts)
without ever splitting, because strikes only accumulated in each peer's
*local* `kg_strikes.json`. Fixes:

1. **Runtime oversize abort** — after the tile grid is computed (and
   only for non-blocks), if `n_tiles > runtime_tile_limit()`
   (`MAX_TILES_PER_BLOCK × RUNTIME_SPLIT_FACTOR` = 22×1.3 → 29) the
   subprocess returns `needs_split` *before* the tile loop.
   `kg_splitter.force_split()` raises the strike counter straight to
   the split threshold (deterministic evidence — no need to burn two
   failed attempts). The parent handler splits immediately and inserts
   the blocks into the local pending list — no director round-trip.
2. **Director-side strike bump** — the stuck-KG watchdog
   (`peer_director._check_stuck_kg`) bumps the parent's strike on the
   **director's** authoritative `kg_strikes.json` before requeueing.
   That file is HA-snapshotted and max-merge synced to peers at
   dispatch, so after 2 watchdog firings the next dispatch splits.
3. **Peer→director strike push** — `_push_strikes_to_director()`
   (fire-and-forget daemon thread, one tiny PUT per rare event) runs on
   needs_split, disk-low handoff, crash-recovery bump, and
   non-permanent failure, so peer-local strikes reach the fleet.
4. **Split-aware mid-run requeue** — parent codes injected from
   `retry_queue.json` mid-run now go through `maybe_split_kg()` (they
   previously bypassed the startup expansion and always re-ran whole).
5. **Disk-low handoffs count as strikes** — two handoffs (any peers)
   ⇒ adaptive split.
6. **Strike clearing is family-aware** — a completed block clears only
   its own strike (`clear_single_strike`); the parent's strikes (which
   keep the split in force) are cleared only when every sibling block
   is complete. Clearing early would collapse a strike-driven split
   while siblings are still pending.

**No upstream cost is repeated on split**: the Copernicus/Hansen/WC
tile cache (local + Zenodo) is keyed by geographic 0.1° grid cells, not
KG code — blocks re-read exactly the cells any earlier whole-KG attempt
already fetched. What *is* lost on split are the parent-keyed tile
pickles (`tile_checkpoints/<parent>/`) — acceptable, since the split
only triggers on deterministic oversize or repeated failure.

### Where to Look When Debugging

| Problem area | Look at |
|---|---|
| KG fails during segmentation | `process_one_kg()` line ~3700, calls `object_segmentation.segment_objects_in_area()` |
| GPKG output wrong | `build_full_gpkg_tiled()` (~l.4340) or `build_light_gpkg_tiled()` (~l.5150) |
| JSON summary wrong | `build_json_summary_tiled()` (~l.6045); v2 doc `kg_json_v2.py` |
| Tile boundary artifacts | Centroid-ownership dedup in `process_one_kg()` ~line 3830, `_segment_touches_edge()` |
| Copernicus data missing | `tile_cache.py` → `copernicus.py` → credential rotation + `@_retry_on_rotation` decorator. See **Copernicus Throttle & Retry**. |
| BEV data read failure | `raster_io.py` → `bev_retry.py` (exponential backoff + proxy rotation) |
| Zenodo upload failure | `main()` ~line 4900, calls `zenodo_client.py` |
| Dashboard not updating | `app.py` `/api/v1/processing/status` reads `progress.json`; check parent thread alive |
| Copernicus quadrant fallback | `_quadrant_split()` in `_fetch_copernicus_for_tile()`. Skipped entirely on `IPThrottledError`. |
| Copernicus 402 / throttle | `copernicus.py` `_check_credits_error()` → `@_retry_on_rotation` (all 4 creds) → `IPThrottledError` → tile_cache re-raises → austria_processor writes pause file + aborts KG |



---

*See `AGENTS.md` for the project map.*

### Tile-checkpoint registry fields in `/process.txt`

(Moved from `AGENTS.md`, 2026-10-13.) The `chkpt_registry:` line (when
present) summarises the cross-peer tile-tar registry used by the dd08d74
BEV-cost-recovery mechanism: when a peer aborts a KG mid-flight (BEV
exhaustion / cred rotation / role eviction) it uploads its tile metadata
pickles as a gzipped tarball to the shared Zenodo cache deposit; the next
peer that picks up the KG pulls them and skips re-doing those tiles.

```
chkpt_registry: kgs=N tiles=T bytes=X.XMB oldest=Yh
```

Fleet visibility piggybacks on the existing `cache_manifest.json` sync
(`chkpt_*` filenames, size=0 = tombstone), so primary sees every peer's
upload/delete within one 5-min sync tick — no new traffic or endpoints.
Every upload / restore / delete also emits an INFO line into the merged
log so the operator can audit eviction fleet-wide:

```bash
curl -s 'https://example.invalid:8000/process.txt?hours=24&log=500&q=chkpt'
```

Look for `chkpt: uploaded <kg>` (parent, post-defer), `chkpt: restored N
tile pickle(s) for <kg>` (subprocess, init step), and `chkpt: deleted
<kg>` (parent, post-completion). Warning variants surface on failure.

Local raster sidecars (Phase 1, dd08d74) under `tile_checkpoints/<kg>/
tile_N/raster/*.npy` are *not* part of the cross-peer registry — they're
per-peer-disk only (gated on `SIDECAR_MIN_FREE_GB` free, default 1 GB
since be7d155 — was 4 GB; lowered after at23/at21 lost DTM tiles in
`gpkg_full` when transient direct-egress flaps coincided with missing
sidecars) and let the `gpkg_full` step mmap DTM/DSM/ortho instead of
re-reading BEV. The `gpkg_full` stitcher also has a deferred-retry pass
(also be7d155): tiles that fail their initial DTM read are queued, then
`bev_proxy._refresh_pool()` is forced once and the queue re-painted, so
a 21h `direct`-slot cooldown can't cause a NaN hole.

#### Surgical eviction of a single KG's checkpoints

Two steps — stop the peer first so it can't re-upload while you delete:

```bash
TOKEN=$(cat data/admin_token)
# 1) stop the peer that holds the (possibly tainted) checkpoints
curl -s -X POST -H "X-Admin-Token: $TOKEN" \
  "http://localhost:8000/api/v1/processing/stop?peer_id=at23"
# 2) clear local pickles + raster sidecars on that peer
curl -s -X POST -H "X-Admin-Token: $TOKEN" \
  "https://example.invalid:8000/api/v1/admin/clear_tile_checkpoints?kg=62133"
# 3) drop the cross-peer Zenodo bundle + tombstone cache_manifest
python3 -c 'import tile_checkpoint_registry as r; print(r.delete_kg("62133"))'
```

Step 3 writes a size=0 entry into `cache_manifest.json` so every peer
drops its mirror view on the next 5-min sync tick; safe to run from
the primary even if no local registry entry exists.
