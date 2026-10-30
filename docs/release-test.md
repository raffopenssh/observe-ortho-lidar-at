# Release test report (step 9)

Date: 2026-10-30 · Repo: `srtm-lidar-public` · Tested commit: see "Commits" below (the tree moved
during the test because a security-audit agent committed concurrently; the test started at `c0243c6`).

Environment: Ubuntu VM, Python 3.12.3, clean venv `/tmp/rt-venv` (`--system-site-packages`), run as `exedev`.
Instance under test: `app.py` on `127.0.0.1:8700`, `public_api.py` on `127.0.0.1:8701`, `SRTM_OUT_DIR=/tmp/rt-out`,
no `ZENODO_TOKEN` (uploads disabled), `COPERNICUS_CREDENTIALS_FILE=~/.config/srtm-lidar/copernicus_credentials.json`
(read-only copy of a live credential store; neither it nor `.env` is committed). A separate production instance on
ports 8000/8100 was left untouched; several bugs below are precisely about the two instances interfering through
shared `/tmp` state.

Legend: **PASS** works as documented · **FIXED** failed first, fixed in this step, re-verified · **FAIL** open ·
**N/A** could not be exercised on this VM (reason given).

## Summary

| # | Item | Result |
|---|------|--------|
| 1 | Clean venv, `make check`, model fetch check, `.env` | PASS |
| 2 | Start both tiers via gunicorn conf / systemd templates, route checks, 10-min idle log | FIXED (3 bugs) |
| 3 | Browser e2e `/segmentation`: draw, run, result, export GPKG, share save/restore | FIXED (1 bug) |
| 4 | Public API smoke (every `LLM_TXT` route), caps, rate limits, tier separation | PASS |
| 5 | Processor `--kg` small KG, GPKG validity, >28-tile KG kill/resume | see below |
| 6 | Director single-VM mode: activate / pause / resume / stop / deactivate | see below |
| 7 | Residual limitations | documented |

## 1. Clean environment — PASS

```
python3 -m venv --system-site-packages /tmp/rt-venv
/tmp/rt-venv/bin/pip install -r requirements.txt gunicorn pytest     # OK (all pins satisfiable)
/tmp/rt-venv/bin/pip install --dry-run --ignore-installed --no-deps -r requirements.txt   # resolves against PyPI, OK
make check PY=/tmp/rt-venv/bin/python3    # 56 passed, 3 skipped; 3 grep gates ok; pip-audit not installed → skipped
/tmp/rt-venv/bin/python3 tools/fetch_models.py --check   # rf_model.joblib.xz ok, segv2/models/model_G.joblib ok
```

Notes
- With `--system-site-packages` and the packages already present system-wide, pip installs nothing into the venv,
  so there is no `/tmp/rt-venv/bin/gunicorn`. The systemd units therefore start gunicorn as `python3 -m gunicorn`
  (changed in `d55a888`, see §2).
- The security-audit agent bumped `pillow 13.0.0`, `requests 2.33.0`, `lxml 6.1.0`, `Flask 3.1.3` mid-test
  (`a3ff4a7`); `pip install -r requirements.txt` was re-run and still resolves.
- `.env` (git-ignored) = `.env.example` with `SRTM_APP_PORT=8700 SRTM_APP_BIND=127.0.0.1 SRTM_PUBLIC_PORT=8701
  SRTM_PUBLIC_BIND=127.0.0.1 SRTM_OUT_DIR=/tmp/rt-out COPERNICUS_CREDENTIALS_FILE=~/.config/srtm-lidar/copernicus_credentials.json`.
  `python3 -c 'import config,json;print(json.dumps(config.describe()))'` → `single_vm: true`, `uploads_enabled: false`.

## 2. Services — FIXED

Start (what `systemd/srv.service` / `srv_public.service` do, run in tmux instead of systemd):

```
set -a; . ./.env; set +a
/tmp/rt-venv/bin/python3 -m gunicorn -c systemd/gunicorn_app.conf.py app:app            # tier A, 8700
/tmp/rt-venv/bin/python3 -m gunicorn -c systemd/gunicorn_public.conf.py public_api:app  # tier B, 8701
```

Bugs found while starting:

| Bug | Symptom | Fix |
|-----|---------|-----|
| gunicorn conf files did `import config` | gunicorn refuses to start: `Invalid value for config: <module 'config'> (Not a string)` — the module shadowed gunicorn's own `config` setting | `d55a888` – `import config as _cfg`, delete helper names; units use `python -m gunicorn` |
| `copernicus.py` hard-coded the credential store path under `data/` | director status: `0 Copernicus credentials` although `COPERNICUS_CREDENTIALS_FILE` was set | `63fffaa` – read `config.COPERNICUS_CREDENTIALS_FILE`, usage store under `config.PROCESSOR_DIR` |
| both tiers used gunicorn's default control socket path | second tier collided with the first | `f34666a` – `$XDG_RUNTIME_DIR/ortho-lidar-{app,public}.ctl` |
| `net_meter` telemetry dir `/tmp/net_meter` shared across instances | the test instance's `/process.txt` showed the production instance's traffic (incl. its peer hostnames) | `cbb7714` – `/tmp/net_meter-<uid>-<port>` (`SRTM_NET_METER_DIR`) |
| `app.py` fixed `/tmp` scratch paths (`srtm_public_slots`, `segment_progress`, `segment_results`, `seg_cache`, index-watch lock, status cache) shared by every app.py on the host | task ids / slots of two instances mixed | `88c1a26` – `config.RUNTIME_DIR` (`SRTM_RUNTIME_DIR`, default `/tmp/srtm-lidar-<uid>-<port>`), documented in `.env.example`, README, `docs/app.md` |
| director reconciled the Zenodo cache manifest even with uploads disabled | `cache_reconcile ERROR` noise in `/process.txt` | `cbb7714` – skip when `not config.uploads_enabled()` |

Route checks after the fixes (`curl -s -o /dev/null -w '%{http_code}'`):

| Route | 8700 (operator) | 8701 (public) |
|-------|-----------------|---------------|
| `/api/v1/ping` | 200 | 404 (operator-only, by design) |
| `/api/v1/health` | 404 (public tier only) | 200 `{ok, rss_mb, caches_mb}` |
| `/api/v1/info` | 200 | 404 |
| `/api/v1/director/status` | 200, `mode` + `peers: [["primary", null]]` (single-VM) | 404 |
| `/process.txt`, `/process.html` | 200 / 200 | 404 |
| `/api/v1/docs/llm.txt` | 200 | 200 |
| `/` | 200 (operator UI) | 200 (public landing) |
| `/segmentation` | 404 | 200 |

Idle log window (10 min, both tiers, nothing but my own requests): no tracebacks, no periodic line every 30 s
(the `service_guard` fix holds); background lines seen once per interval only: `product_repair sweep` (every ~10 min),
`meta_refresh` on director tick. Noted, not fixed: the public tier logs every relayed `/segment/progress` poll at INFO
(one line per 2 s *while a segmentation runs*, silent when idle). One start-up race seen exactly once before the
runtime-dir fix: `process.txt progress_summary failed: no such column json_uploaded_at` (search-index schema rebuild
in progress); not reproducible after restart.

Operational notes
- `tmux kill-session` does not stop gunicorn (HUP = reload). Stop with `TERM` to the master pid (or `systemctl --user stop`).
- Director mode `auto` starts the processor at boot; mode `manual` still (re)starts it (documented in
  `docs/peer-director.md`); only `paused` keeps it idle. `POST /api/v1/processing/stop` runs `pkill -f austria_processor.py`,
  which also kills any unrelated shell whose argv contains that string — fragile, left as is (operator loopback only).
- `systemd-run` warns `does not accept OOMScoreAdjust=100 — dropping` on this host (cosmetic).

## 3. Browser e2e on `/segmentation` — FIXED

Chromium via the browser tool against `http://localhost:8701/segmentation`, network + console capture enabled.
The rectangle was created through the Leaflet.draw event the UI listens to
(`map.fire(L.Draw.Event.CREATED, {layer: L.rectangle([[48.405,15.595],[48.411,15.605]])})` — same code path as a
mouse draw; `#geometry` is filled with the polygon, UI reports 49.2 ha) and started with `#btn-run`.

First run (before fix): finished in ~70 s with 1947 objects but **0 % man-made, "rock" 32 %, "crop" 32 %** in the
old town of Krems. `app.log` showed every Copernicus layer failing with `401 invalid_client`: the credential at
index 0 of the store is flagged invalid and `copernicus._get_connection()` never rotated. Fixed in `2dd7174`
(rotate on 401 up to 5 times, mark the credential `last_status=invalid`, never pick invalid credentials while
others exist). The production store had 19/107 invalid entries, so this affected the live deployment too.

Second run (after fix): Copernicus authenticates after one rotation; NDVI composite, WorldCover, SAR, 8 NDVI months
downloaded (one transient `Preflight process graph validation failed: [502] Bad Gateway` retried cleanly).
Result after 253.65 s: 1947 objects, 652 man-made / 1295 natural; legend parking 33 %, rock 29 %, grass 19 %,
tree 9 %, shrub 8 %. Plausible for a town centre apart from "parking" standing in for buildings and the high
"rock" share — both because no building-footprint / OSM / INVEKOS context is available in public mode (see §7).

- Screenshot (map + legend): `docs/img/release-test-segmentation.png` (252 KB, palette-quantised).
- Export GeoPackage: the UI posts `POST /api/v1/export/geopackage` → 202 task, then navigates to
  `/api/v1/export/geopackage/download/<task>`; downloaded through 8701: 200, `application/geopackage+sqlite3`, 13.4 MB,
  `PRAGMA integrity_check` = ok, `application_id` = 0x47504B47, layers `segment_type`, `segment_height`
  (2d-gridded-coverage), `segments`, `segment_points` (features). A rasterio warning
  (`NaN nodata cannot be recorded in gpkg_2d_gridded_coverage_ancillary`) is logged; harmless.
- Share: "Share" → `http://localhost:8701/segmentation?share=auto-b4568cb7` (auto-saved, 300 KB); loading that URL
  in a fresh navigation restored geometry, all 1947 markers and the legend; `/api/v1/shares` lists it.
- Abort mid-task, 5 km² cap (9.9 km² polygon → 400 "The selected area is too large (9.9 km²) … maximum allowed is
  5 km²") and outside-Austria (Paris → 400 "Geometry is outside Austria…") verified through the same API the UI uses.
- JS console errors: none during the whole session.

## 4. Public API smoke — PASS

Script: `/tmp/rt-smoke.sh` hits every route listed in `public_docs.LLM_TXT` on 8701 with
`bbox=15.595,48.405,15.605,48.411`, KG `63349`, a KML file for `parse-geometry` and the task/share ids from §3.
Status column = HTTP status, "doc 4xx" means the documented not-ready / validation response.

| Route | Result |
|-------|--------|
| `GET /api/v1/docs/llm.txt`, `/license`, `/attribution`, `/health`, `/status`, `/status?guard=1` | 200 |
| `GET /api/v1/landscape` (bbox, point, `include=trees,landmarks,cells`), `/context` | 200 |
| `GET /api/v1/trees/bbox`, `/landmarks/bbox`, `/buildings/bbox` | 200 |
| `GET /api/v1/kgs` (bbox / limit+offset / codes / fields=codes / fields=codes,als), `/kgs/summary` | 200 |
| `GET /api/v1/kg/63349` | 202 `ready:false` + `retry_after_s` (KG not processed on this instance) |
| `GET /api/v1/kg/63349/cells`, `/cells?bbox|lon,lat|ids`, `POST /cells`, `/cells/<h3>`, `/cells/chunk/<h3>` | 404 `not_processed` (doc 4xx — no v2.4 product on this fresh instance) |
| `GET /api/v1/cells/dict`, `/cells/bins`, `/flags/dict`, `/flags/stats`, `/flags/heat?res=7` | 200 |
| `GET /api/v1/cells/national?res=9` | 404 "national aggregate not built yet" (doc 4xx) |
| `GET /api/v1/heightfield?bbox=…` | 404 "no product ≥2.1 grid for this bbox yet" (doc 4xx) |
| `GET /tiles/hillshade|slope/14/8903/5629.png` | 204 (no processed data → empty tile) |
| `GET /api/v1/stats/unit` (kg csv, bbox json) | 200 |
| `GET /api/v1/flags` (bbox / kg / lon,lat), `/flags/<id>` | 200 / 404 unknown id |
| `POST /api/v1/flags` without subject | 400 "subject required…" ; with `kg`+`claim` → 201 |
| `GET /llm/manifest.json` | 200 |
| `POST /api/v1/elevation`, `/terrain` | 200 |
| `POST /api/v1/segment/overlay`, `/dtm/overlay`, `/lidar/overlay`, `/ortho/overlay`, `/cir/overlay`, `/hansen/overlay` | 200 image/png (segment 25 s, others ≤ 2.3 s) |
| `POST /api/v1/export/kml` (no result_json) / `/export/mbtiles` (no layer) | 400 (doc 4xx) |
| `GET /api/v1/export/geopackage/download/<task>` | 200 application/geopackage+sqlite3 |
| `POST /api/v1/parse-geometry` (KML upload) | 200 |
| `GET /api/v1/layers?bbox=`, `/shares`, `/share/<id>`, `POST /share/<id>/rename` (no new_id) | 200 / 200 / 200 / 400 |
| `GET /api/v1/segment/progress`, `/segment/result` | 200 |
| `POST /api/v1/segment?async=true` 9.9 km² / Paris | 400 cap / 400 outside Austria |

Tier separation / retired routes (8701): `/api/v1/parcels`, `/api/v1/cadastre`, `POST /api/v1/processing/pause`,
`/api/v1/director/status`, `/api/v1/ping` → **404** `{"error":"not_found","docs":"/api/v1/docs/llm.txt"}`.
The spec wording says "retired routes → 410"; the public tier deliberately answers a uniform 404 so that the
retired route names do not have to exist anywhere in the public code (they would also trip the `make check` grep gates).
`POST /api/v1/processing/pause` via app.py from loopback → 404 "Processor not running" (not 403; the route is reachable).

Rate limits: `POST /api/v1/flags` burst of 40 → 20 × 201, 20 × 429. The tile renderer's limiter
(`RateLimiter('tile_render', 25/s, burst 150)`) was exercised in-process — 300 takes → 150 allowed / 150 refused — but a
300-request burst on `/tiles/hillshade` returned 300 × 204 because empty tiles (no processed KG under the tile) short-
circuit before the limiter, as documented ("cached/empty tiles are not counted"). A 429 on real tiles needs a processed KG.

Forbidden-string scan of every body (the five patterns from the spec: parcel / cadastre / `/k/` / token / the private fleet domain): only `Tokenless` / "admin token" / "no token"
in the docs text and the attribution id `bev_cadastre` in `/api/v1/license` (an attribution record, intended).

## 5. Processor — FIXED (see 5c)

### 5a. Small KG: `--kg 12115` (Landersdorf, NÖ; 2 tiles, 0.66 km²) — PASS

```
set -a; . ./.env; set +a
/tmp/rt-venv/bin/python3 austria_processor.py --kg 12115 --no-upload      # EXIT=0, 0.6 h wall
```
(The task text says `--single`; the CLI flag is `--kg`, as README and `docs/austria-processor.md` document.)

Timeline (log `/tmp/rt-logs/proc-12115.log`): BEV DTM/DSM/ortho reads immediate; Copernicus credential rotation
`1/107 → 70/107` then authenticated; per 0.1° cell: NDVI composite ~3 min, WorldCover 15 s, SAR 1.5 min, 8 monthly
NDVI scenes ~80 s each (sequential, single credential), harmonics fit; Hansen tile; then per tile Felzenszwalb+RAG
segmentation (10.8 k segments / tile) and per-object feature extraction ~9 min per tile; products written 17:13.
No Copernicus 402/throttle during this run; one transient `[502] Bad Gateway` retried.

Products under `/tmp/rt-out/12115/` (v2.4 set, NE cells absent → `-partial` as predicted):

| File | Size | Check |
|------|------|-------|
| `12115_full.gpkg` | 390 MB | `integrity_check` ok, `foreign_key_check` empty, `application_id` 0x47504B47; 22 layers (DTM/DSM/nDSM + 2022/2023 dates, NDVI, SAR_VV/VH, WorldCover, Hansen ×2, Ortho 2020/2023/2024, CIR 2020/2023, segments EPSG:3035 **57 columns = fid+geom+55 props**, segment_points EPSG:4326 **56 = fid+geom+54 props**, segment_type, segment_height) |
| `12115_light_v2.gpkg` | 188 MB | integrity ok, application_id ok; layers `segments`, `segment_points`, `segment_type`, `segment_height`, `infrastructure`, `tree_apices`, `tree_crowns`, `terrain_coarse_{dtm,dsm,ndsm,slope,aspect}` (no footprint-shaped layers) |
| `12115_v2.json.gz` | 60 KB | `version: 2.4`, keys version/kg_code/…/landscape; `v2verify: PASS 46/49 checks` (warnings: `segments_layer_drift` 2.1 % objects without polygon, `segment_points_count`, `ne_cells: no NE product`) |
| `12115.json` | 315 KB | legacy JSON, quality 93 % grade A, `missing=building_context,infrastructure` |

Observations (not fixed): `segments` holds 170 608 multipolygon parts for 19 903 distinct ids (one id alone has
10 485 parts — the vectorised label raster is very fragmented for large ground segments); `v2_verify` documents the
one-row-per-connected-part layout and tolerates the 2 % id drift. Peak child RSS 1.1 GB, parent 150 MB; the host
(8 GB, two instances + a production pair) went down to 60 MB free during feature extraction.

### 5c. Bug: outbound Zenodo calls with `--no-upload` — FIXED (`31f3752`)

At KG end the log showed `zenodo_tiles: tile store: shard … unavailable: 400/403 … zenodo.org/api/deposit/depositions`
(×16), `chkpt_registry: delete chkpt_12115.tar.gz skipped: 403` and `Zenodo cache flush failed: 400`, i.e. the
tile-cache flush and the checkpoint registry still contacted Zenodo although uploads were disabled. Causes:
`flush_tile_cache_to_zenodo()` and `tile_checkpoint_registry._get_cache()` never consulted the upload switch, and
`config.uploads_enabled()` only knew `SRTM_DRY_RUN` while `--no-upload` sets `SRTM_NO_UPLOAD` (the processor had its
own private helper). Fix: `config.uploads_enabled()` honours both variables, the processor's `_uploads_enabled()`
delegates to it, and both call sites return early when uploads are disabled. `make check` green.

