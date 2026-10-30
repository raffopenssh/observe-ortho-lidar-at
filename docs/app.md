# `app.py` — operator API, analysis engine and director host (`:8000`)

`srv.service` runs `gunicorn -w 2 --threads 4 app:app` bound to
`127.0.0.1:${SRTM_APP_PORT}` (8000). It hosts three things:

1. **Analysis endpoints** (`/api/v1/segment`, `/elevation`, `/terrain`,
   overlays, exports, trees v2/v3, changes, shares, `/onestop`) — used by
   the bundled UI (`static/index.html`) and relayed for the public tier
   by `public_segmentation.ROUTES` (`docs/public-api.md`).
2. **Operator endpoints** (`/api/v1/processing/*`, `/director/*`,
   `/admin/*`, `/credentials`, `/zenodo/lock`, `/index/rebuild`,
   `/classifier/train`, …) — control plane of the Austria-wide processor.
3. **Background loops**: director (`peer_director`, `docs/peer-director.md`),
   `v2_ingest`, `ne_ingest`, audits (`label_mix`, `product_repair`,
   `sliver_sweep`), Zenodo lock heartbeat. Each is single-flight across
   the gunicorn workers via an `fcntl` lock file under
   `data/austria_processor/`.

## Operator gate (one rule)

`_enforce_operator_gate` (`before_request`): every route under
`_OPERATOR_PREFIXES` and **every** non-GET request outside the public
analysis allow-list is allowed only from loopback (127.0.0.1 / ::1
without `X-Forwarded-For`) or with `X-Operator-Token ==
config.operator_token()` (`SRTM_OPERATOR_TOKEN` or
`${SRTM_SECRETS_DIR}/operator_token`); otherwise 403. Requests arriving
through the public relay always carry `X-Forwarded-For` and get 404 on
operator paths (defence in depth). `_OPERATOR_GET_OPEN` lists the
read-only inspection GETs (`/director/status`, `/processing/status`,
`/processing/queue`, …) the dashboard may read. On a single machine you
need no secret at all: `curl localhost:8000/...`.

## Async tasks

`/api/v1/segment`, `/onestop`, `/export/geopackage|mbtiles` return a
`task_id` immediately; progress lives in `/tmp/segment_progress/<id>.json`
and results in `/tmp/segment_results/<id>.json.gz` (gzip, 4 h TTL),
polled via `/segment/progress?task_id=` / `/segment/result?task_id=`,
aborted via `/segment/abort`. Every finished segmentation is also saved
as the auto-share `auto-<task8>` (`/api/v1/share/<id>`). At most
`_TASK_SEMAPHORE` = 2 heavy tasks run at once; a third gets 429.
Long loops inside a task must call `_check_aborted()`.

## Section map (`# === SECTION:` markers in `app.py`)

| section | what |
|---|---|
| public-tier relay | `_public_request()` detection for the relayed routes |
| response compression + conditional GET | `_compress_and_etag`: gzip ≥ 1 KiB, strong ETag → 304 |
| Copernicus credentials API | `GET/POST/DELETE /api/v1/credentials` → `copernicus.py` credential store (`docs/copernicus-throttle.md`) |
| Status push to director | this VM's processor state pushed to the director (no-op single-VM) |
| Processing queue / async task system | semaphore, file-backed progress + results |
| Austria Processor endpoints | `/api/v1/processing/*` (table in `docs/austria-processor.md`) |
| KG coverage completion oracle | `_kg_coverage_complete(code)`: a parent is complete when its blocks' rects cover it and every block has a live `_json` |
| split-layout-drift wedge detection | `stale_blocks.py` — retire residue of an old split layout |
| Bandwidth & Peer Director | director loop host, `/api/v1/director/*` |
| Zenodo upload mutex / lock broker | `/api/v1/zenodo/lock` (`zenodo_lock.py`): classes `kg_upload*` (shared pool `ZENODO_LOCK_KG_SLOTS`) and `cache` (one at a time); TTL 120 s, heartbeat 30 s, orphan reap 100 s; `ZENODO_LOCK_URL` points peers at one broker |
| sliver sweep | `/api/v1/director/sweep*` (`docs/sliver-sweep.md`) |
| Search index API | `/api/v1/kg/<code>[/history\|segments\|landcover\|heightfield\|layers]`, `/index/status`, `/index/rebuild`, `/insights` (`docs/search-index.md`) |
| Zenodo fetch progress | sync GPKG endpoints answer 202 while a product downloads |
| Geometry + parameter helpers | `_get_geometry`, `_get_params`, `_validate_area`, `_clean_polygon` — every analysis endpoint goes through them |
| /elevation, /terrain, /segment, /segment/overlay | the live pipeline (`object_segmentation.py`, `_segment_core` / `_segment_worker`) |
| exports | `/export/geopackage`, `/export/kml`, `/export/mbtiles`, `/{lidar,ortho}/geotiff` |
| changes / trees | `/changes`, `/changes/trees`, `/api/v2/trees*`, `/api/v3/trees*` (`docs/tree-inventory-v3.md`) |
| overlays | `/{dtm,lidar,ortho,cir,hansen}/overlay` |
| RF classifier training | `/classifier/train`, `/classifier/status` (`docs/rf-training.md`) |
| docs / share / attribution / layers / onestop | `/docs` (`static/docs.html`), `/api/v1/share*`, `/attribution`, `/layers?bbox=`, `/onestop` |
| Quality flags + feedback | operator-tier `/api/v1/flags*`, `/feedback*` (`quality_flags.py`, `feedback_db.py`) |
| parse-geometry | `/api/v1/parse-geometry` (`geo_parse.py`: GeoJSON, WKT, KML, GPX, GPKG, zipped SHP, coordinates) |

## Re-queueing a KG (and the tombstone trap)

```bash
curl -s -XPOST -H 'Content-Type: application/json' localhost:8000/api/v1/processing/queue \
  -d '{"kgs":["63330"],"position":0,"skip_processed":false}'
```

`skip_processed:false` writes a `<code>_requeue` tombstone into the
manifest and invalidates **every** product pointer of the code (incl.
`_full_gpkg`) — the KG becomes a from-scratch rerun. `GET
/processing/queue` prunes codes the coverage oracle calls complete
unless a live tombstone exists; a second POST on a complete KG with an
existing tombstone *drops* the tombstone. To force a rerun of a complete
KG: POST once (tombstone), wait for the next GET-prune, POST again — or
delete the `<code>_json` manifest entry. To upgrade an old product to the
current version instead, see `docs/v2.1-product-spec.md` § 6 (never use
this path for that).

Diagnose: `python3 -c "import app; print(app._kg_coverage_complete('63330'))"`
(set `SRTM_APP_IMPORT_ONLY=1` so no director thread starts).

## Ops

```bash
curl -s localhost:8000/api/v1/ping
curl -s 'localhost:8000/process.txt?log=50'              # one-page dashboard (text)
curl -s localhost:8000/api/v1/processing/status | python3 -m json.tool | head
curl -s localhost:8000/api/v1/director/status | python3 -m json.tool | head
curl -s 'localhost:8000/api/v1/net_stats?fmt=txt'        # per-endpoint bandwidth
journalctl -u srv -n 100
```
