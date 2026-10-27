# AGENTS.md — srtm-lidar-at

> **Token discipline**: this file is a navigation hub. Deep docs live in `docs/`.
> Don't load a `docs/*.md` unless your task touches that subsystem.

## ⚡ Fast read for agents: `/process.txt`

**Always start here when investigating cluster state.** Token-cheap,
text-only mirror of `/process.html` — full director / peer / log / Zenodo /
bandwidth snapshot. Per-worker render cache (10 s TTL, X-Cache header) +
director status cache (30 s, cross-worker via disk) so concurrent agent
polls cost almost nothing. Add to your context with one curl:

```bash
curl -s https://example.invalid:8000/process.txt          # default 60 log lines, last 24h
curl -s 'https://example.invalid:8000/process.txt?warn=1' # warnings + errors only
curl -s 'https://example.invalid:8000/process.txt?peer=at3&log=200'
curl -s 'https://example.invalid:8000/process.txt?q=cred&log=300'
curl -s 'https://example.invalid:8000/process.txt?hidden=1' # also list stopped/idle peers
curl -s 'https://example.invalid:8000/process.txt?hours=168&log=500&q=Diendorf' # 7d back, archive
curl -s 'https://example.invalid:8000/process.txt?roster=0&log=0' # banner + fleet lines only (cheapest)
curl -s 'https://example.invalid:8000/process.txt?attn=1&warn=1' # triage: attention peers + warn log
curl -s 'https://example.invalid:8000/process.txt?stall=40'      # all stalled/partial Zenodo product triples
```

**First line is a `health:` banner** — `done/7850 (pct%) @rate/h eta` plus
loud flags (`CPU-STARVED`, `BW-OVER-NOMINAL`, `KG-FAILURES`, `FLEET-DORMANT`,
`ZENODO-LOSSY` / `ZENODO-DEGRADED`, `PRIMARY-NET-HIGH`, `HANSEN-GAPS`; else `OK`).
`ZENODO-LOSSY` = circuit tripped on peer PUT failures but the manifest
still shows ≥5 files / ≥1 GB committed in the last hour (retry ladder
absorbing it); `ZENODO-DEGRADED` only when nothing lands. The
`zenodo_uploads:` and `openeo:` lines are the evidence behind both verdicts
— read them before touching timeouts/retry ladders (see
`docs/copernicus-throttle.md → Correction (2026-09-23)`). `FLEET-DORMANT` =
every remote peer unreachable ≥15 min — `docs/peer-director.md →
Fleet-dormant hold mode`. **Counts come from the search index** (one row
per parent KG, fresh `_json` completions only — split blocks and v2.x
upgrade re-uploads never inflate it; `docs/v2-upgrade.md → Telemetry`).

Lines worth knowing: `products:` (per-version parent histogram, `current=v2.4`),
`v2:` (upgrade progress, store, fast path, strikes), `v2_regen:`, `repair:`
(`docs/product-repair.md`), `legacy:` (2.4 clean-up: suspect/cf/stripped/removed
+ `local_json:`), `ne:` / `ne_ingest:` (NE cells, `docs/ne-public-api.md`),
`vct_pilot:` (`docs/vct-pilot.md`), `sweep:` (`docs/sliver-sweep.md`),
`ne_ingest:` → `green_fix(...)` (read-time bau/alpen→gruen override on S2-green cells,
`ne_green_fix.py`, `docs/ne-public-api.md → Green-ground override`), `chkpt_registry:` (`docs/austria-processor.md`),
`fleet_bw:` / `bw_learn:` / `throttle:` (`docs/fleet-bandwidth.md`),
`fleet_proxy:` (`docs/bev-proxy.md`), `zenodo_lock:` (`docs/app.md`),
`hansen_cover:` (missing 0.5° Hansen tiles on Zenodo + `HANSEN-GAPS` flag,
`docs/zenodo-cache.md → Hansen gaps`; `python3 hansen_coverage.py --peers|--flush`).

Query params:
- `log=N` (default 60, max 500) — merged-log line count, newest first.
  **`log=0` suppresses the merged-log block entirely** (skips the disk
  read too — use when you only want fleet/peer state).
- `warn=1` — restrict log to warnings + errors
- `peer=<substr>` — filter peer roster + log lines by id substring
- `q=<substr>` — substring filter on log message body
- `hidden=1` — also include stopped/idle/complete peers in roster
  (default: hidden; attention-state peers are always shown)
- **`attn=1`** — collapse the roster to attention-state peers only
  (offline / needs-update / stale / paused / parked-mid-KG, plus the
  active frontier + owners). The interesting rows are otherwise buried
  under dozens of near-identical CACHE peers; everything else folds into
  the trailing hidden count.
- **`roster=0`** — drop the peer table entirely (director / health /
  fleet lines / log only). Pairs well with `log=0` for the cheapest
  possible top-of-dashboard snapshot.
- **`hours=H`** — look back H hours into the merged log. Default 24
  (live ring only). Higher values transparently dip into the
  **long-term archive** at `data/log_archive/YYYY-MM-DD.jsonl.gz`
  (per-UTC-day gzipped JSONL, written when the live ring prunes).
  Use this to mine the full ~200-day forensic record — e.g.
  `?hours=2400&q=cred` for credential-rotation history.

For structured access, pair with `/api/v1/director/status` and
`/api/v1/director/log/history?hours=H` (both documented in
`/api/v1/docs/llm.txt`). The history endpoint also reads from the
per-day archive when `hours` exceeds the live ring — returns the
*most-recent* `limit` matches in range (sets `truncated:true` if more
exist), avoiding the prior bug that truncated the tail of the day.
Director orchestration events — peer auto-updates, Copernicus credential
revalidation/add/remove, frontier credential & cache-cell plan changes,
**bandwidth-wall auto-park / park-until-renewal**, **stuck-KG hard
restarts** (`stuck-kg <pid>: …` — peer wedged inside a KG with a frozen
progress fingerprint for 12 h; see `docs/peer-director.md → Stuck-KG
watchdog`), **duplicate-KG reconciliation** (`dup-kg <loser>: KG <code>
also running on <winner> …` — two peers ran the same KG outside the
150 s dispatch-claim window; director keeps the furthest-along peer and
hard-stops the laggards; see `docs/peer-director.md → Duplicate-KG
reconciler`) events — are emitted via `app.director_event(…)` and appear
inline in the merged 24h log (`peer=director` for fleet-wide events,
otherwise the affected peer id). Grep `?q=stuck-kg` / `?q=dup-kg` to
audit watchdog firings.

### Stalled / partial Zenodo triples (`zen_stall:` line)

Mirrors the done/partial/stalled chips on `/process.html`'s Zenodo card. A
product code is *done* when `_json` + `_full_gpkg` + (`_light_gpkg` **or**
`_light_gpkg_v2`) are committed (`status=removed` records count as absent;
`_json_v2` / `_ne_h3b` are add-ons, never part of the triple); *partial*
while missing something and first upload < 48 h old; *stalled* past 48 h.
Rows (default 10, `?stall=N` ≤100, `stall=0` summary-only) carry a pickup
verdict from the coverage oracle: `hole+queued` (in retry_queue), `hole`
(parent bbox uncovered → auto-repicked by the pending sweep), `orphan`
(coverage complete — old-split residue, never auto-repicked; use the
re-queue recipe below if wanted), `v2heal` (own `_json` + `_full_gpkg`,
light missing → healed automatically by the v2-upgrade fill, no operator
action). Failed/partial *event-rate* history lives in the `kg_outcome:`
field of the `throttle:` block.

Director events (`peer=director` in the merged log): peer auto-updates,
credential changes, bandwidth-wall parks, `stuck-kg <pid>` hard restarts,
`dup-kg <loser>` reconciliation — grep `?q=stuck-kg` / `?q=dup-kg`
(`docs/peer-director.md`). Structured: `/api/v1/director/status`,
`/api/v1/director/log/history?hours=H` (reads the per-day archive too).

## ⚠ Two API tiers since Oct 2026 — read `docs/api-tiers.md` before exposing anything

* **Public** `https://example.invalid/` = `public_api.py` (gunicorn
  `srv_public`, port 8100 = the exe.dev default port): parcel-free,
  bbox/point/KG-code keyed, index + grid25 + cells (`cells_api.py`) only,
  QA-filtered trees. Contract: `public_docs.LLM_TXT` (`/api/v1/docs/llm.txt`).
  Invariant: `product_version "v2.4"` ⇒ cells served (else `v2.4-pending`).
* **Private** `…/k/<old path>` = the full `app.py`, bearer-token gated
  (`api_tiers.py`), proxied to `127.0.0.1:8000`. Peers reach the director
  via `/k` (canonical primary URL; `http_pool` adds `X-Admin-Token`).
  Dashboards: `https://example.invalid:8000/process.html?token=…`.
* Cadastre upstream = `example.invalid/k` with token (`umfeld.py`,
  `data/umfeld_token`, auto-distributed to peers) — **run-time model context
  + NE declared layer only**; never written to a product
  (`austria_processor.CADASTRE_PRODUCTS = False`). Never put either token in git.
* **Public `/segmentation` UI** = `static/index.html` minus `/*PRIVATE*/`
  fences, relayed to app.py through `public_segmentation.ROUTES` only
  (`X-Srtm-Public`, cadastre off, 5 km², products-first via
  `product_segments.py`, own share namespace). `docs/api-tiers.md →
  Public /segmentation UI`; test `python3 tests/test_public_segmentation.py`.
* Nothing parcel-keyed may be added to any tier. The Kataster purge is
  **done** (2026-10-13, `docs/kataster-purge.md`): parcel routes, `kg_parcels`,
  parcel QA flags and the cadastre modules are deleted; the old `/k`
  landscape paths answer 410 (`api_tiers.landscape_route_retired`).

## TL;DR

Flask + Leaflet app that segments Austrian landscape from BEV LiDAR + BEV ortho
+ Sentinel-2 + ESA WorldCover + Sentinel-1 (+ cadastre as model context). A
background processor (`austria_processor`) runs all 7 850 KGs and uploads
**v2.4 products** to Zenodo; a peer director on the primary VM orchestrates
~50 exe.dev VMs.

- **Live**: https://example.invalid:8000/ (owner) · public https://example.invalid/
- **Stack**: Python 3.12 / Flask / gunicorn / Leaflet
- **Dashboard**: `/process.html`  •  **Query UI**: `/query.html`  •  **API docs**: `/api/v1/docs/llm.txt`

**v2.4 product set** (`PRODUCT_VERSION = "2.4"`, per product code = parent
`61047` or block `04016-southwest`): `<code>_full_gpkg`, `<code>_light_gpkg_v2`
(`_light_v2.gpkg`, no parcels/buildings layers), `<code>_ne_h3b` (NE cells,
H3; may be absent → `-partial`, repaired later), `<code>_json_v2`, and
`<code>_json` (v1-shaped, cadastre-stripped — still the fleet-wide
**completion marker** for `_kg_coverage_complete` / `completed_codes` /
zen_stall / dashboard). Upload order full → light_v2 → ne_h3b → json_v2 →
json. Legacy `_light_gpkg` records stay with `status=removed` (size 0);
pre-2.4 `_json` is re-PUT `status=stripped`. Details: `docs/v2-upgrade.md →
Products`, `docs/v2.4-rollout.md`.

## Deep-dive index (load on demand)

| Topic | File | When to read |
|---|---|---|
| `app.py` mental model | `docs/app.md` | touching `app.py` — section map, async tasks, background threads, auth, Zenodo lock classes, **re-queue / tombstone trap** |
| Austria Processor mental model | `docs/austria-processor.md` | touching `austria_processor.py`, KG pipeline, tile checkpoints + cross-peer registry, GPKG/JSON builders, Zenodo upload, block splitting |
| Peer Director (multi-VM orchestration) | `docs/peer-director.md` | touching `peer_director.py`, `deploy.sh`, parallel frontiers, HA, throttle, admin token, role eviction, BEV outage parks, **cluster bandwidth budget** (`/api/v1/net_stats`, `net_meter.py`, `http_pool.py`) |
| Fleet bandwidth telemetry & parks | `docs/fleet-bandwidth.md` | `fleet_bw:` / `bw_learn:` / `throttle:` lines, canary thresholds, `_peer_bw_depleted`, the five park reasons, unexpected `parked→Xd` |
| BEV proxy pool | `docs/bev-proxy.md` | touching `bev_proxy.py` / `bev_retry.py`, proxy source lists, `fleet_proxy:` line |
| Copernicus throttle & retry | `docs/copernicus-throttle.md` | touching `copernicus.py`, `tile_cache.py`, 402 handling, credential rotation |
| Search index | `docs/search-index.md` | touching `search_index.py`, schema (`kg`, `kg_landcover`, `kg_hansen`, `kg_classification`, `kg_divergence`, `kg_type_top`, `kg_buildings`, `kg_trees`), `GpkgCache`, `zenodo_fetch.py` |
| RF training | `docs/rf-training.md` | touching `train_rf_4000kg.py`, ground-truth filters, retraining triggers |
| Zenodo persistent tile cache (+ **tile store v2** `zenodo_tiles.py`) | `docs/zenodo-cache.md` | touching `zenodo_cache.py`, `zenodo_tiles.py`, tile manifest, ZIP indices, `cache_manifest.json → shards` |
| Cross-cutting concerns | `docs/cross-cutting-concerns.md` | adding object types, RF features, tile grid, credential pool, navigation cheatsheet, SAR dB quirk |
| Planned refactor + speed optimisation | `docs/planned-refactor.md` | next maintenance window work |
| **v2.1 / 2.2 / 2.3 product layout** (grid25 in JSON, tree_apices/tree_crowns/terrain_coarse in light GPKG, robust top-N, disk rule; forestry write-ups `docs/FEEDBACK-4-REPLY.md`, `-5-`) | `docs/v2.1-product-spec.md` | touching `v21_products.py`, the `v21` tile-loop / light-GPKG / JSON plumbing, `v2_verify.check_v21_*`, `kg_v2_store.grid25`, `/kg/<code>/heightfield|landcover`, `/api/v3/trees` |
| **v2.4 product + rollout** (cadastre-free, NE cells `.h3b`, manifest status `cf`/`stripped`/`removed`, legacy clean-up, `legacy:`/`ne:` lines, director 2.4-first wave) | `docs/v2.4-rollout.md` (+ `docs/ne-v24-design.md`, `docs/v2.4-fast-path-handoff.md`, `docs/v2.4-single-peer-check.md`) | touching `ne_h3.py`, `ne_rollout.py`, `v24_fast.py`, `CADASTRE_PRODUCTS`, `remove_legacy_products`, `_v2_upload_and_finish` |
| v2 rollout mechanics (segv2 LightGBM, upgrade-from-GPKG, primary blob store, ingest, strikes/deferrals, **v2_regen**, PUT-then-DELETE replace) | `docs/v2-upgrade.md` | touching `v2_ingest.py`, `kg_v2_store.py`, `kg_docs.py`, `kg_log_harvest.py`, `v2_verify.py`, `--v2-upgrade`, `_v2_upgrade_fill`, `_check_v2_regen`, `zenodo_client._replace_in_bucket`; reading the `v2:` / `v2_regen:` lines |
| **Repairable layer gaps** (`-partial` label, registry, fixable gate, `repair:` line) | `docs/product-repair.md` | touching `product_repair.py`, `_product_upload_version`, `prewarm.repair_cells`, a product missing harmonics / NDVI / SAR / WorldCover / Hansen / ne_cells |
| **Public cells API** (H3 res-12 observed/trees/structures from `data/ne/*.h3b`, `cells.sqlite`, vocabulary rule; `python3 tests/test_cells_vocab.py` before every edit, `python3 cells_api.py --rebuild` if `/status.cells` disagrees with `ls data/ne/*.h3b`) | `docs/ne-public-api.md` (+ `docs/ne-primary-api.md` for the ingest corpus) | touching `cells_api.py`, `ne_ingest.py`, `public_api.py` cells routes, `public_docs` cells section |
| **Public flags** (tokenless "looks wrong" reports per cell / point / KG for every public layer; `data/public_flags.sqlite`, fold/snapshot/superseded, `/api/v1/flags*`, `/cells/<h3>?flags=1`; `python3 tests/test_public_flags.py`) | `docs/public-flags.md` | touching `public_flags.py`, `public_docs.FLAGS_TXT`, umfeld-at glyph / siedler flag integration |
| **Public-tier reliability** (Oct 2026 outage post-mortem; `mem_lru.ByteLRU`, `service_guard`, tile PNG disk cache, `/api/v1/health`, `?guard=1`) | `docs/public-reliability.md` | `example.invalid` slow/down, `srv_public` memory, adding a cache or heavy endpoint to `public_api.py` |
| **VTC pilot / bev mode** (vtcseamless cadastre built locally per peer, `/contrib` reports to umfeld-at, NE change protocol, the `vct_pilot:` block, tiled NEC1 build for huge parents) | `docs/vct-pilot.md` | touching `vct_pilot.py`, `vct_fleet.py`, `ne_build_tiled.py`, `vct_pilot_*` knobs, `/api/v1/admin/vct_pilot`, bevdirect-serve flags in `ne_h3.py` |
| **API tiers** (public `public_api.py` / `landscape_query.py`, hidden `/k` tier `api_tiers.py`, `umfeld.py` upstream token, retired-route 410s, cutover checklist) | `docs/api-tiers.md` | touching `public_api.py`, `landscape_query.py`, `public_docs.py`, `api_tiers.py`, `umfeld.py`, tokens, the exe.dev port switch |
| **Label-mix repair** (restored-checkpoint label-id collision: flat pasture as `roof`, glued polygons; `label_mix:` line, auto-requeue, `LabelMixError` stitch guard) | `docs/label-mix.md` | touching `label_mix.py`, `_remap_restored_tile_ids`, `_assert_tile_labels_consistent`, `v2_verify.segments_label_consistency`; a KG with `geb` cells at `h_max=0` |
| **Security model** (per-peer tokens + scopes, owner channel for the master, HA off, proxy pool removed, rotation, security events) | `docs/security-tokens.md` | touching `peer_tokens.py`, `_enforce_admin_token`, `install_token`, `director_ha.py`, `deploy.sh`, "+ Add Peer"; `/process.txt?q=security` shows events |
| **Sliver sweep** (umfeld bbox revision 2, 2026-10-27: processing bbox pinned to `kg_bbox_r1.json`, `bbox_r2` = NE viewport only; `sliver_sweep.py` sweeps unseen edge cells in small units on a few cache-only peers, merges into `data/ne/<kg>.h3b`, re-POSTs to umfeld; `sweep:` line, `?q=sweep`) | `docs/sliver-sweep.md` | touching `sliver_sweep.py`, `_normalise_umfeld_kg`, `_parent_bbox_wgs`, `/api/v1/director/sweep*`, `/processing/sweep_spec`; a KG with nodata cells along its bbox edge |
| Kataster purge (historical, done 2026-10-13) | `docs/kataster-purge.md` | what was deleted, what remains on purpose, golden test `tools/golden_public.py` |
| Licensing & attribution (BEV CC BY 4.0, Copernicus, OSM ODbL) | `docs/attributions.md` | touching `attributions.py`, Zenodo metadata, GPKG metadata, adding a data source |
| Reference algorithms summary | `docs/reference_algorithms_summary.md` | segmentation/RF internals |

## Bandwidth (primary is metered — keep it < 200 MB/h out+in)

`curl -s 'http://localhost:8000/api/v1/net_stats?fmt=txt'` shows per-endpoint
MB/h in/out (all workers merged). Every periodic fanout must be
single-flight across gunicorn workers, delta-based and gzipped — see
`docs/peer-director.md → Cluster bandwidth accounting`.

## Quick ops

```bash
sudo systemctl restart srv                # web app (gunicorn + director thread)
sudo systemctl restart rf_train           # RF training job
sudo systemctl kill -s SIGKILL austria_processor && sleep 2 && sudo systemctl start austria_processor
journalctl -u srv -f
tail -f data/austria_processor/logs/processor.log
tail -f /tmp/rf_train_4000kg.log
```

## Services

| Unit | Role | Notes |
|---|---|---|
| `srv.service` | gunicorn (2w × 4t, :8000) + director thread — the hidden `/k` tier | MemoryMax=4.2G (fleet traffic only since Oct 2026), on-failure, `srv-watchdog.timer` probes `/api/v1/ping` |
| `srv_public.service` | gunicorn (2w × 24t, :8100) `public_api:app` — public tier + `/k` reverse proxy | MemoryHigh/Max 2.4/2.8G; **`docs/public-reliability.md`** (byte-bounded caches, tile gate/rate-limit/disk cache, RSS watchdog, `srv-watchdog.timer` probes `/api/v1/health`) |
| `austria_processor.service` | KG pipeline | **Disabled** on primary (director manages it) and on peers (director starts via API) |
| `rf_train.service` | background RF training (4000 KGs) | on-failure |

## File layout

### Core
| File | ~Lines | Purpose |
|---|---:|---|
| `app.py` | 22500 | Flask API (hidden `/k` tier), async tasks, processor/director API, `/process.txt`, share storage |
| `austria_processor.py` | 14600 | KG pipeline (parent + subprocess). See `docs/austria-processor.md` |
| `peer_director.py` | 15300 | Multi-VM orchestration. See `docs/peer-director.md` |
| `public_api.py` / `landscape_query.py` / `public_docs.py` / `cells_api.py` | — | Public tier (`:8100`). See `docs/api-tiers.md`, `docs/ne-public-api.md` |
| `object_segmentation.py` | 2400 | Felzenszwalb+RAG → per-object classify; `TYPE_LETTER` |
| `learned_classifier.py` | 590 | RF classifier (`FEATURE_KEYS`, cadastre-trained) |
| `static/index.html` | 3200 | Single-file Leaflet UI |
| `static/process.html` | 6350 | Processor + director dashboard (compact peer strip, Live Log range chip → `/api/v1/director/log/history?hours=N`) |
| `static/query.html` | 725 | Query Explorer over the surviving index routes, `/feedback` |
| `static/flag.js` | 620 | Flag widget (text-selection chip → `/api/v1/flags/match`) |

### Search & products
| File | Purpose |
|---|---|
| `public_flags.py` | public "looks wrong" reports (`/api/v1/flags*`), own SQLite. `docs/public-flags.md` |
| `search_index.py` | SQLite FTS5 + R-tree over 7850 KGs. Auto-rebuild on new JSONs. `GpkgCache` = lazy Zenodo GPKG cache. `docs/search-index.md` |
| `zenodo_fetch.py` | Progress-aware / resumable / single-flight Zenodo product downloads (`/kg/<code>/segments|layers`, trees v3). `/api/v1/zenodo/fetches`; `zenodo_fetch:` line |
| `v21_products.py`, `kg_json_v2.py`, `kg_v2_store.py`, `v2_ingest.py`, `v2_verify.py`, `v24_fast.py` | v2.x product writers / codec / primary blob store / ingest / pre-upload checks / 2.4 fast path |
| `ne_h3.py`, `ne_ingest.py`, `ne_rollout.py`, `cells_api.py` | NE cells: builder + `.h3b` I/O, primary corpus ingest, 2.4 legacy audit lines, public cells API |
| `product_repair.py`, `product_origin.py` | `-partial` defects registry, upload-version label |
| `llm_api.py` | `/api/v1/prewarm`, `/api/v1/trees/bbox` only (parcel-free; the `/llm/*` dossiers are gone) |

### Data I/O
| File | Purpose |
|---|---|
| `raster_io.py` | Windowed reads from BEV via `/vsicurl/` |
| `ortho_io.py` | BEV orthophoto (RGBI, 47 operates + DOP fallback) |
| `copernicus.py` | openEO: NDVI / WC / SAR / harmonics (4-cred rotation). See `docs/copernicus-throttle.md` |
| `cadastre.py` | Building footprints + parcels as segmentation ground truth / `building_mask` — run-time model context only (`CADASTRE_PRODUCTS=False`, never in a product since 2.4) |
| `hansen.py` | Hansen GFC (forest change) |
| `osm_features.py` | OSM via Overpass |
| `bev_retry.py` | Backoff + proxy rotation for `rasterio.open()` |
| `attributions.py` | Licence/attribution single source of truth → `/api/v1/attribution`, Zenodo, GPKG `gpkg_metadata`, KG JSON. See `docs/attributions.md` |
| `tile_index.py` | 55-tile grid, EPSG 4326 ↔ 3035 |
| `tile_cache.py` | Grid-snapped 0.1° caches, Zenodo fallback |
| `zenodo_cache.py` | Persistent tile cache on Zenodo. See `docs/zenodo-cache.md` |
| `zenodo_client.py` | Zenodo API + `Manifest` |
| `gpkg_streamed.py` | Strip-streamed full-GPKG for >100 Mpx KGs |
| `kg_splitter.py` | Splits KGs >28 tiles into directional blocks |
| `geo_parse.py` | KML/GeoJSON/Shapefile/GPX/WKT |

### Feature extraction & training
`terrain_analysis.py`, `temporal_analysis.py`, `texture_features.py`,
`ndvi_harmonics.py`, `train_rf_4000kg.py`, `calibrate.py`.

### Deprecated (kept for reference)
`landscape_classifier.py`, `object_classifier.py`, `scene_adaptive_classifier_patches.py`.

## API surface (high level)

Canonical machine-readable list: `GET /api/v1/docs/llm.txt` (public tier
contract in `public_docs.LLM_TXT`; private-tier reference in repo `llm.txt`).
Groupings (search `# === SECTION:` in `app.py`):

- **Analysis**: `/api/v1/segment` (async), `/elevation`, `/terrain`, `/changes`, `/changes/trees`, `/api/v2|v3/trees*`
- **Async tasks**: `/segment/progress|result|abort`, auto-saved as `auto-<task>` share
- **Overlays/exports**: `/segment/overlay`, `/{dtm,lidar,ortho,cir,hansen}/overlay`, `/export/{geopackage,kml,mbtiles}`, `/{lidar,ortho}/geotiff`
- **Shares**: `/shares`, `/share`, `/share/<id>`, `/share/<id>/rename` (1 GB cap, LRU, `data/shares/`)
- **One-stop**: `/api/v1/onestop?bbox=&format=`
- **Search index**: `/kg/<code>` (+ `history|segments|landcover|heightfield|layers`), `/index/{status,rebuild}`, `/insights`, `/flags*`, `/feedback*`
- **Retired (410)**: `/query*`, `/parcel*`, `/parcels/*`, `/lookup`, `/cadastre/*`, `/kg/<code>/profile|buildings|new_buildings|infrastructure|parcels`, `/llm/kg|kgs|manifest.json` — deleted 2026-10-13; public cell-keyed equivalent is `/api/v1/cells` (`docs/ne-public-api.md`)
- **Processor (proxied)**: `/processing/{status,start,stop,pause,resume,single,retry,queue,throttle,peers,peers/status,log,manifest,cache_manifest,cache_misses,kg_search,…}`
- **Director (admin-token)**: `/director/{status,mode,activate,stop,peers,peers/add,peers/<id>,throttle,proxy/status,proxy/log,update_peers,heartbeat,snapshot,announce,step_down,takeover,handover,identity,v2/priority,…}`
- **Admin**: `/admin/{update,restart_processor,disable_autostart,diskstat,role_evict,proc_env,clear_tile_checkpoints,mem_top,vct_pilot,ne_tooling,set_private_only,…}`
- **Credentials**: `/credentials` (POST add, list usage), `/credentials/validate`
- **Misc**: `/layers?bbox=`, `/info`, `/parse-geometry`, `/zenodo/lock` (mutex broker), `/ne/declared/<kg>` (bev-mode relay), `/net_stats`

## Frontend cheatsheet

- `getPostArgs()` → `{ct, body}` for fetch
- `showResultOnMap(data)` → renders features + legend + overlay
- `restoreShareResult(data)` → restore full state incl. overlays
- `clearEverything()` → reset to blank
- `checkLayerAvailability()` → debounced, hides unavailable layers
- State: `lastResult`, `allFeatureData`, `overlays`, `drawnItems`,
  `currentShareId/Name`, `_activeTaskId/_aborted`, `hiddenTypes`, `selectedTypes`
- `localStorage`: active `taskId` (resume after refresh), `srtm_admin_token`
  (admin-token for dashboard mutations, reset via `srtmResetAdminToken()`)

## Analysis pipeline (one-liner)

1. read DTM+DSM (`raster_io`) → 2. ortho/NDVI/SAR/Hansen/cadastre context →
3. fused gradient → 4. Felzenszwalb (scale=150) + RAG merge (0.12) →
5. extract features per segment → 6. RF classify (or rule-based) →
7. group adjacent compatible segments (tree→forest, roof→building) →
8. GeoJSON features. Details in `object_segmentation.py`.

## Object types (4 letters = man-made when capital)

```
Vegetation:    tree shrub grass hedge
Water:         water
Buildings:     roof greenhouse solar_panel
Infrastructure: fence wall mast (+ wind_turbine, substation)
Transport:     road path parking bridge
Agricultural:  crop orchard vineyard garden
Terrain:       bare_soil rock
Disturbance:   excavation fill tree_loss construction earthwork
v2 extras:     rail glacier wetland
```

Letter mapping (lowercase = natural, uppercase = man-made) is
`object_segmentation.TYPE_LETTER` / `LETTER_TYPE` (table in
`docs/austria-processor.md → Type letters`).

## External data sources

| Source | Resolution | Access |
|---|---|---|
| BEV ALS DTM/DSM | 1m, 3 dates (2022/23/24) | HTTP range on remote GeoTIFF |
| BEV DOP RGBI | 0.2m, 47 operates | HTTP range on remote GeoTIFF |
| Sentinel-2 NDVI / WorldCover / S1 SAR | 10m | openEO (CDSE credential pool) |
| Hansen GFC | 30m | `/vsicurl/` UMD |
| Austrian cadastre (run-time context only) | mm | umfeld-at `/k` REST API, or local bevdirect in bev mode |
| umfeld-at NE declared layer (NEC1) | H3 | umfeld `/k/api/v1/ne/*` (director relay for bev-mode peers) |
| OSM | varies | Overpass |

Caches: `/tmp/copernicus_cache/`, `/tmp/hansen_cache/`. Persistent: Zenodo
depo 19650075 (see `docs/zenodo-cache.md`).

## Where to look (debug fast-path)

```bash
# Section markers
grep -n '# === SECTION' app.py austria_processor.py peer_director.py
# Project-wide section index
grep -rn '# ===' *.py | sed 's/# === SECTION: //' | sed 's/ ===//' | column -t -s:
# Color/type sync (must match across files)
grep -rl 'SEGMENT_COLORS' *.py static/*.html
# RF feature list
grep -A60 'FEATURE_KEYS = \[' learned_classifier.py
# Live processor step
cat data/austria_processor/current_step.json | python3 -m json.tool
```

### Quick GPKG validity check (download from Zenodo + verify)

After changes to the GPKG writers (`_write_segment_vectors`,
`_write_segment_points`, `_write_gpkg_features_chunked`, `v21_products.py`,
`gpkg_streamed.py`), grab a couple of fresh uploads and verify they're
structurally sound (first time new code reaches `gpkg_full` / `gpkg_light`
on the fleet takes several hours). Token: `zenodo_client.DEFAULT_TOKEN`.

```bash
TOKEN=$(python3 -c 'import zenodo_client; print(zenodo_client.DEFAULT_TOKEN)')
# 1) Pick recent fleet GPKG completions:
curl -s 'http://localhost:8000/process.txt?hours=4&log=500&q=gpkg_full' | grep -E '^\s+[0-9]{2}-' | head -10
# 2) Resolve filename -> Zenodo bucket from the manifest:
python3 -c "
import json
m=json.load(open('data/austria_processor/zenodo_manifest.json'))
for k in ('66330_full_gpkg','62028_light_gpkg_v2'):
    e=m['entries'][k]
    print(k, e.get('status'), e['size']//1024//1024,'MB', e['bucket_url']+'/'+e['filename'])
"
# 3) Download (bucket URLs are private; bearer token required):
mkdir -p /tmp/gpkg_check && cd /tmp/gpkg_check
curl -sH "Authorization: Bearer $TOKEN" -o 66330_full.gpkg "https://zenodo.org/api/files/<bucket-uuid>/66330_full.gpkg"
# 4) Validate: SQLite integrity + GPKG layer inventory + readback:
python3 - <<'PY'
import sqlite3, fiona
for f in ['66330_full.gpkg','62028_light_v2.gpkg']:
    c=sqlite3.connect(f)
    print(f, 'integrity:', c.execute('PRAGMA integrity_check').fetchone()[0],
          'fk:', len(c.execute('PRAGMA foreign_key_check').fetchall()),
          'app_id:', hex(c.execute('PRAGMA application_id').fetchone()[0]))
    print('  gpkg_contents:', c.execute('SELECT table_name FROM gpkg_contents ORDER BY 1').fetchall())
    print('  layer_styles:', c.execute('SELECT COUNT(DISTINCT f_table_name) FROM layer_styles').fetchone()[0])
    for l in fiona.listlayers(f):
        if l in ('segments','segment_points','new_buildings','tree_apices'):
            with fiona.open(f, layer=l) as src:
                print(f'  {l}: {sum(1 for _ in src)} features, {len(src.schema["properties"])} props')
PY
```

What to expect on a healthy GPKG (`v2_verify.verify_before_upload` runs the
full check list on peers before upload):
- `integrity_check` = `ok`, `foreign_key_check` = 0 rows, `app_id` = `0x47504B47`.
- `_full.gpkg`: ~22 entries in `gpkg_contents` (DTM/DSM/nDSM + multi-date,
  Ortho/CIR/NDVI/SAR/Hansen/WorldCover, segment_type/height tiles, plus
  vector `segments` (EPSG:3035) + `segment_points` (EPSG:4326)).
- `_light_v2.gpkg` (2.4): `segments`, `segment_points`, `segment_type`,
  `segment_height`, `new_buildings`, `infrastructure`, `tree_apices` (+R-tree),
  `tree_crowns`, `terrain_coarse_*`; **no** `parcels` / `buildings` /
  `parcel_outline_z` (`v2_verify.FORBIDDEN_LIGHT_LAYERS`).
- `layer_styles` row count == number of layers (every layer styled).
- `segments` schema = 55 properties, `segment_points` = 54 (chunked
  writer must keep schema identical across all chunks — drift would
  show as fewer props on later features).
- Layers > 10k features are written as multiple bounded transactions but
  readback must report ONE contiguous feature count matching the
  `gpkg_full — N tiles, M objects` log line.

More in `docs/cross-cutting-concerns.md`.

## Re-queueing already-processed KGs (the tombstone trap)

`POST /api/v1/processing/queue {kgs:[…], position:0, skip_processed:false}`
is the **only** correct path to reprocess a KG that already has manifest
products (`/processing/single|retry` just append and get pruned again).
`GET /processing/queue` (polled by the dashboard) rewrites `retry_queue.json`,
pruning any code the coverage oracle (`_kg_coverage_complete`, keyed on
`_json`) calls complete unless it carries a live `<code>_requeue` tombstone
— and a POST that finds an *existing* tombstone on a complete KG drops it
instead of deleting `_json`. Recipe: make sure no stale `<code>_requeue`
tombstone exists (re-POST once, or delete the `<code>_json` manifest entry),
POST again, then verify the code survives a GET. Full explanation + the
"which queued parents will be pruned" one-liner: `docs/app.md → Re-queueing`.

## Critical invariants (read before editing)

- **Object types & colors** are duplicated across `app.py`, `austria_processor.py`,
  `static/index.html` — must stay in sync. See `docs/cross-cutting-concerns.md`.
- **RF feature order** in `learned_classifier.py:FEATURE_KEYS` must match
  `object_segmentation.extract_object_features()`. Changing count invalidates
  the saved `.joblib`.
- **Only one director** runs at a time. Gated by `data/austria_processor/is_director`.
  Single-flight via `director.lock` (fcntl). HA failover in `director_ha.py`.
- **Frontiers are pinned to disjoint planning cells** (`FRONTIER_CELL_LAT×LON`,
  0.5°×1° since tile store v2; cap `FRONTIER_MAX_PARALLEL`). Many
  cache-only peers in parallel are fine. See `docs/peer-director.md →
  Frontier planning cells`.
- **All Zenodo writes** serialise through `/api/v1/zenodo/lock` on the primary.
- **`CredentialRotatedError` / `CreditsExhaustedError` / `IPThrottledError`**
  must NEVER be swallowed by generic `except Exception`. See `docs/copernicus-throttle.md`.
- **Tokens are per peer** (`docs/security-tokens.md`): the primary's
  `data/admin_token` is the owner-only *master* (accepted only on the
  private `:8000` channel with the owner's exe.dev login, never via `/k`);
  each peer has its own token (map: `data/peer_tokens.json`, primary
  only) scoped to `peer_tokens.PEER_SCOPE` on the primary; `data/mesh_token`
  is peer→peer cache-manifest sync only. Outbound calls are rewritten per
  destination by `peer_tokens.install_outbound_hook` — never paste the
  master into a peer, a deploy command or a snapshot.
- **HA / director takeover is OFF** (`director_ha.ha_enabled()`; only the
  file `data/ha_enabled` on the primary re-enables it). No token can move
  the director role.
- **No third-party proxies, ever** (`bev_proxy.py` is a direct-only stub
  since the 2026-10-23 C2 alert).
- **Process uid**: the processor MUST run as `exedev` (uid 1000), not root.
  See `docs/peer-director.md` (Processor uid section).
- **Don't probe Copernicus credentials from the request path.** Only
  `peer_director._refresh_credentials_if_due()` may. See `docs/copernicus-throttle.md`.

## Persistent state files (all `data/austria_processor/` unless noted)

| File | Purpose |
|---|---|
| `progress.json` | Live processor state (dashboard reads this) |
| `current_step.json` | Subprocess→parent IPC |
| `subprocess_warnings.jsonl` | Warning relay |
| `in_progress_kg.txt` | Crash-recovery marker |
| `tile_checkpoints/<kg>/tile_N.pkl` | Per-tile metadata checkpoints (resume-on-retry; cross-peer via Zenodo chkpt registry — see `docs/austria-processor.md`) |
| `tile_checkpoints/<kg>/tile_N/raster/*.npy` | Per-tile local raster sidecars (DTM/DSM/nDSM + multi-date + ortho RGB+NIR) so `gpkg_full` skips BEV re-reads (Phase 1, dd08d74) |
| `checkpoint_registry.json` | Local view of *which* tile-tars this peer uploaded to Zenodo (only the uploading peer writes it; the fleet-wide view lives in `cache_manifest.json` under `chkpt_*` keys, synced primary↔peers every 5 min) |
| `zenodo_manifest.json` | KG product uploads (schema: `{entries: {key: {depo_id, size, ...}}}` — *not* `{kgs: …}`. `*_error` keys are retry markers; exclude them from totals.) |
| `cache_manifest.json` | Zenodo tile-cache deposit (schema: `{depo_id, files: {fname: {size, tile_count, updated_at, ...}}}`). Doubles as the fleet-wide transport for chkpt registry deltas (`chkpt_*` filenames, size=0 = tombstone). Synced via existing `_sync_peer_data` GET+PUT cycle — no new traffic. |
| `zenodo_zip_index/*.json` | Cached central directories of remote ZIPs |
| `failed_kgs.json` / `retry_queue.json` / `deferred_kgs.json` | Queue mgmt |
| `peers.json` / `director_state.json` / `director.lock` / `is_director` | Director |
| `peer_urls.txt` | Peer list for data-sync thread |
| `self.json` | Identity (id, url, director_url) |
| `copernicus_credentials.json` / `copernicus_credential_usage.json` | Creds + telemetry |
| `copernicus_paused` / `openeo_circuit.json` | Throttle state |
| `admin_token` (in `data/`) | master (primary) / own token (peer); `peer_tokens.json`, `mesh_token`, `owner_emails`, `ha_enabled` — `docs/security-tokens.md` |
| `api_tokens` / `umfeld_token` / `private_only` (in `data/`) | extra `/k` client tokens (one per line) / cadastre-upstream bearer token / flag: app.py 404s without token. All gitignored. See `docs/api-tiers.md` |
| `shadow/` + `shadow/meta.json` | HA: staged state for shadow promotion |
| `zenodo_lock_url.txt` | Peer→broker pointer |
| `kg_strikes.json` / `cache_miss_kgs.json` | Reliability tracking |
| `data/search_index.db` | SQLite FTS5+R-tree (~0.9 GB, auto-rebuild; `docs/search-index.md`) |
| `data/kg_v2_store.db` | primary blob store of ingested `_json_v2` docs + grid25 (`kg_v2_store.py`) |
| `data/ne/*.h3b` + `data/ne/cells.sqlite` | NE cells corpus + chunk index for the public cells API (primary-only) |
| `data/feedback.sqlite` | QA flags + feedback (`quality_flags.py`, `feedback_db.py`) — private tier, object-ref keyed |
| `data/public_flags.sqlite` + `data/public_flags.salt` | public-tier flags (`public_flags.py`, `docs/public-flags.md`); salt is gitignored, 0600 |
| `data/k_landscape_off` | flag: retired `/k` landscape routes answer 410 (permanent since the Kataster purge) |
| `legacy_audit.json` / `legacy_local_strip.json` / `json_heal_state.json` | v2.4 legacy clean-up audit, local v1-JSON strip record, `json_heal` sweep state (`ne_rollout.py`, `json_heal.py`) |
| `data/combined_log_24h.jsonl` | Live 24h merged log ring (pruned every ~10 min). NOT replicated to shadow — rebuilt from peers' own `recent_log` after takeover via `_combined_log_bootstrap_once`. |
| `data/log_archive/YYYY-MM-DD.jsonl.gz` | Long-term per-day archive (full 200-day run). Replicated to shadow via `PUT /api/v1/director/log_archive` on the shadow loop — today-only every hour, full sweep on shadow change, sha256-cached so steady-state traffic is ~0. |
| `data/shares/` | Share storage (1 GB cap, LRU) |

## Conventions for editing

- **Wake-ups / "check back later" = fresh conversation, never in-conversation
  sleep.** Fleet convergence (rollout waves, tombstone/drop propagation,
  a KG finishing) takes 10 min–2 h. Do NOT `sleep 600` and re-poll inside
  the current conversation — every resume re-sends the whole context
  (~$1 per wake-up). Commit + push + restart your change, tell the user what
  to expect, and if verification is genuinely needed later schedule a
  **one-shot systemd timer** that starts a *new* `shelley client chat`
  (see the `schedule` skill; unit name `shelley-<name>`, prompt carries
  the check to perform + the originating conversation id; the unit must
  clean itself up). The new agent reads `/process.txt` cold for a few
  cents instead of dragging this conversation along.

- **Read the relevant `docs/*.md` first** when touching a subsystem. They
  contain hard-won invariants, failure modes, and recovery procedures that
  aren't obvious from the code.
- **Section markers**: every long file (`app.py`, `austria_processor.py`,
  `peer_director.py`) uses `# === SECTION: name ===`. Add markers when
  introducing new logical groupings.
- **Keep this file short.** New deep content → new file in `docs/` and link
  it here. Aim: AGENTS.md fits in a single screen of indexed content.
- **Dashboard bandwidth budget** (`static/process.html`): the page polls
  every 5–15 s from possibly several open tabs, so every polled endpoint
  must stay small. `app._compress_and_etag` gzips JSON ≥1 KiB and answers
  `If-None-Match` with 304, but *don't rely on it* — use the slim views:
  `director/status?slim=1` (no `ip_pools_history`, sparse credential
  buckets), `director/proxy/status` (no embedded `_director` unless
  `?director=1`), `processing/manifest?summary=1` (full 5 MB body only
  while the Zenodo panel is open). Sep 2026 baseline: ~15 MB/h per tab
  (was ~3 GB/h). Before adding a new poll, measure with
  `curl -s -H 'Accept-Encoding: gzip' -o /dev/null -w '%{size_download}' localhost:8000/<ep>`.
- **Restart discipline**: changes to `austria_processor.py` need a processor
  restart at the next KG boundary (or kill it). Changes to `app.py` /
  `peer_director.py` need `sudo systemctl restart srv` (the director thread
  reloads automatically — singleton replaced).
- **Push → restart → rollout ordering**. `_LOCAL_GIT_COMMIT` is frozen
  at `srv` import time and is what the director sends to peers as the
  rollout target (see `versions:` / `(target=...)` line in `/process.txt`).
  Commit *before* restarting srv, then `git push origin main`, then
  `sudo systemctl restart srv`. If you restart first and commit after,
  the director keeps pushing the old commit as target until the next
  restart and the fleet never picks up your change. (`_ensure_origin_synced`
  pushes local main to origin every tick — so a forgotten `git push` is
  recoverable, but a forgotten restart is not.)
- **Let the director roll out the fleet — do NOT hand-drive updates.**
  After commit→push→`restart srv`, the new commit becomes the rollout
  `target=` and the director's **wave-based auto-rollout** propagates it
  on its own: idle peers get a hard restart, mid-KG peers a graceful
  restart at the next KG boundary, capped at `STALE_UPDATE_HARD_PER_TICK=3`
  + `STALE_UPDATE_GRACEFUL_PER_TICK=5` peers/tick (≈5 min for the whole
  fleet). This pacing is load-bearing: it exists *because* a thundering
  herd of simultaneous restarts has repeatedly broken the cluster.
  **Do not** blast `POST /api/v1/director/update_peers` across the fleet
  (especially not `graceful:false`), and do not loop `processing/stop` +
  `clear_tile_checkpoints` over many peers to force a wave — that is the
  thundering herd the wave logic prevents. On **2026-05-06** an all-at-once
  `update_peers` exhausted gunicorn fds and tripped circuit breakers
  fleet-wide; on **2026-06-05** a manual multi-peer force-update +
  stop/clear churn during a 503 incident bounced the director off the
  primary onto at106 (it auto-handed-back, but it was avoidable churn).
  Just commit, push, restart srv, and watch `versions:`/`rollout:` in
  `/process.txt` converge.
- **Correct behaviour for a *single* stale/degraded peer**: if you must
  intervene on one peer (e.g. it swallowed a transient upstream failure
  with old code and is baking bad data into the current KG), the minimal
  safe sequence is — (1) `POST …/api/v1/director/update_peers`
  `{"peer_id":"<id>","graceful":false,"skip_push":true}` so *that one*
  peer pulls + restarts onto the target commit; (2) `POST
  …/<peer>/api/v1/processing/stop` and confirm the **processor** (not
  just srv) is actually stopped — the web app restart alone leaves the
  old processor running in RAM, already past the cadastre/upstream step;
  (3) `POST …/<peer>/api/v1/admin/clear_tile_checkpoints?kg=<code>` to
  drop the tainted tile pickles (otherwise the new code restores them
  and re-bakes the bad data); then **leave starting to the director** —
  it restarts stopped peers on the next tick (~30 s) onto a fresh KG.
  One peer at a time; never script this across the fleet.
