# observe-ortho-lidar-at

Object-level landscape segmentation for Austria from open data: BEV
airborne LiDAR (1 m DTM/DSM, three epochs) and 20 cm RGBI orthophotos,
Copernicus Sentinel-2 / Sentinel-1, ESA WorldCover and Hansen Global
Forest Change. A Flask + Leaflet app segments any polygon you draw into
up to 28 object types (trees, roofs, roads, crops, water, rock, glacier,
…) with per-object height, terrain, spectral, temporal and SAR
attributes; a background processor applies the same pipeline to all
~8 440 Austrian Katastralgemeinden (KGs) and publishes the products
(GeoPackages, compact JSON indexes, H3 cell records) on Zenodo. A
separate read-only public API serves those products: per-KG summaries,
25 m terrain / land-cover grids, a product-backed tree inventory and
H3 "NE cells".

![UI screenshot placeholder](docs/img/ui.png) <!-- VERIFY: add screenshots (UI, /segmentation, cells map) -->

## How it works

```
DTM + DSM (BEV, 3 epochs) ─┐
ortho RGBI → NDVI, texture ─┤   fused gradient → Felzenszwalb (scale 150) + RAG merge (0.12)
Sentinel-2 NDVI harmonics  ─┤ → 117 features per segment → LightGBM (v2) / Random Forest (v1)
Sentinel-1 SAR, WorldCover ─┤ → rule-based fallback → hierarchical grouping (forest, building, …)
Hansen GFC                 ─┘ → GeoJSON / GPKG / KML / MBTiles, overlays, tree inventory
```

* `object_segmentation.py` — the pipeline; `learned_classifier.py` (RF
  v1), `segv2/` (LightGBM v2); `tree_inventory.py`, `trees_v3.py` —
  apex / crown inventory; `terrain_analysis.py`, `temporal_analysis.py`.
* `app.py` — operator API + analysis engine (`:8000`, loopback);
  `public_api.py` — public read tier (`:8100`); `austria_processor.py`
  — the Austria-wide batch processor; `peer_director.py` — keeps it fed.
* Products: `docs/v2.1-product-spec.md`; NE cells: `docs/ne-v24-design.md`,
  `docs/ne-public-api.md`. Full map of the docs: `AGENTS.md`.

## Setup (one machine)

Requirements: Linux, Python 3.12, GDAL ≥ 3.6 with the HTTP driver
(`libgdal-dev`), ~20 GB disk for caches, outbound HTTPS to the hosts in
`config.OUTBOUND_ALLOWED_HOSTS`.

```bash
git clone https://github.com/raffopenssh/observe-ortho-lidar-at.git && cd observe-ortho-lidar-at
python3.12 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python3 tools/fetch_models.py        # RF + LightGBM weights (data/models_manifest.json; MODELS_BASE_URL or Zenodo)
cp .env.example .env                 # edit as needed; everything is optional
make check                           # unit tests + release gates
```
<!-- VERIFY: Makefile targets (make check) -->

Run it by hand:

```bash
gunicorn -w 2 --threads 4 -b 127.0.0.1:8000 --timeout 600 app:app      # operator API + UI  → http://localhost:8000/
gunicorn -w 2 --threads 24 -b 0.0.0.0:8100 public_api:app              # public API         → http://localhost:8100/
python3 austria_processor.py --kg 63330 --no-upload                     # one KG into ./out/
```

Or install the systemd templates in `systemd/` (`srv.service`,
`srv_public.service`, `austria_processor.service`, `srv-watchdog.timer`,
`cells-national.timer`, `rf_train.service`; they reference `${HOME}` and
the repo directory — adjust `User=` / `WorkingDirectory=`):
<!-- VERIFY: units moved to systemd/ and parametrised -->

```bash
sudo cp systemd/*.service systemd/*.timer /etc/systemd/system/ && sudo systemctl daemon-reload
sudo systemctl enable --now srv srv_public austria_processor srv-watchdog.timer cells-national.timer
```

Secrets (all optional) go into `SRTM_SECRETS_DIR` (default
`~/.config/srtm-lidar/`): `zenodo_token` (only to **publish** products),
`operator_token` (only if `:8000` is exposed beyond loopback),
`copernicus_credentials.json` (openEO client credentials for the
Copernicus layers; without them the Copernicus features are null and
products are labelled partial). Register at
<https://dataspace.copernicus.eu/> for free Copernicus credentials.

### Environment variables (`.env.example`)

| variable | default | meaning |
|---|---|---|
| `SRTM_APP_PORT` / `SRTM_APP_BIND` | `8000` / `127.0.0.1` | operator API + UI (`app.py`); keep loopback-only |
| `SRTM_PUBLIC_PORT` / `SRTM_PUBLIC_BIND` | `8100` / `0.0.0.0` | public read API (`public_api.py`) |
| `SRTM_APP_URL` | `http://127.0.0.1:8000` | where the public tier relays analysis requests |
| `SRTM_SELF_URL` | — | external URL used in share links only |
| `SRTM_DATA_DIR` | `./data` | corpus, caches, state |
| `SRTM_SECRETS_DIR` | `~/.config/srtm-lidar` | `zenodo_token`, `operator_token`, `copernicus_credentials.json` |
| `SRTM_OUT_DIR` | `./out` | products when uploads are disabled |
| `SRTM_PRIMARY_URL`, `SRTM_PEERS`, `SRTM_PEER_ID` | empty | multi-machine mode (`docs/peer-director.md`); empty = single machine |
| `SRTM_OPERATOR_TOKEN` | empty | `X-Operator-Token` for mutating routes from non-loopback clients |
| `ZENODO_TOKEN` | empty | enables uploads; empty → products to `SRTM_OUT_DIR` |
| `ZENODO_CACHE_DEPOSIT` | `19650075` | shared tile-cache deposit (read-only use without a secret) |
| `SRTM_DRY_RUN` | `0` | never upload |
| `COPERNICUS_CREDENTIALS_FILE` | `$SRTM_SECRETS_DIR/copernicus_credentials.json` | openEO credentials |
| `POWER_API_URL`, `UMFELD_URL`, `INVEKOS_API_URL`, `ZENODO_LOCK_URL` | empty | optional upstream services, disabled when empty |
| `SRTM_BEV_VECTOR_CONTEXT` | `0` | in-RAM building-footprint context from public BEV vector tiles (`docs/vct-pilot.md`) |
| `PUBLIC_MAX_AREA_KM` | `5` | area cap for public-tier analysis requests (km²) |
| `SRTM_SHARE_CAP_MB` | `1024` | disk cap for saved shares |
| `SRTM_EXTRA_ALLOWED_HOSTS` | empty | extra outbound hosts |

## Data sources and licences

| source | licence | credit |
|---|---|---|
| BEV ALS DTM/DSM 1 m, DOP RGBI 0.2 m, vector tiles (`data.bev.gv.at`) | CC BY 4.0 | "Datenquelle: BEV – Bundesamt für Eich- und Vermessungswesen, Stichtag …, bearbeitet" |
| Copernicus Sentinel-2 L2A, Sentinel-1 IW GRD (openEO, Copernicus Data Space) | free, full & open | "Contains modified Copernicus Sentinel data [year]" |
| ESA WorldCover 2021 v200 | CC BY 4.0 | Zanaga et al. 2022, doi:10.5281/zenodo.7254221 |
| Hansen Global Forest Change 2000–2024 v1.12 | CC BY 4.0 | Hansen et al. 2013, Science 342:850–853 |
| OpenStreetMap (Overpass) | ODbL 1.0 | "© OpenStreetMap contributors" — the `infrastructure` layer is OSM-derived and share-alike |

Our products are published CC BY 4.0 (OSM-derived layers ODbL);
`GET /api/v1/attribution` returns the machine-readable text, and every
GPKG carries it in `gpkg_metadata`. Details: `docs/attributions.md`,
`attributions.py`. No product contains property- or ownership-level
data; the sub-KG unit is an H3 cell.

**Zenodo.** Finished KG products are open-access Zenodo records (one per
KG). The shared tile cache (Copernicus / Hansen cells, tile checkpoints)
is deposit `19650075`; a fresh install reads it anonymously
(`docs/zenodo-cache.md`) so a cache-only run (`--cache-only`) needs no
Copernicus credentials for already-covered cells. Writing to it needs the
deposit owner's secret.

## Using the APIs

* UI: `http://localhost:8000/` (draw a polygon), processor dashboard
  `/process.html`, API docs `/docs` (`static/docs.html`), full operator
  reference `llm.txt` in this repo.
* Public tier: `http://localhost:8100/` — contract at
  `GET /api/v1/docs/llm.txt`; `docs/public-api.md`, `docs/ne-public-api.md`.

```bash
curl -s -XPOST localhost:8000/api/v1/segment?async=true -H 'Content-Type: application/json' \
  -d '{"geometry":{"type":"Polygon","coordinates":[[[15.40,47.07],[15.405,47.07],[15.405,47.076],[15.40,47.076],[15.40,47.07]]]}}'
curl -s 'localhost:8100/api/v1/cells?bbox=15.40,47.07,15.41,47.08' | head -c 400
```

## Repository layout

| path | what |
|---|---|
| `app.py`, `public_api.py`, `public_docs.py`, `public_segmentation.py` | web services |
| `object_segmentation.py`, `learned_classifier.py`, `segv2/`, `tree_inventory.py`, `trees_v3.py`, `terrain_analysis.py`, `temporal_analysis.py`, `texture_features.py`, `ndvi_harmonics.py` | pipeline |
| `raster_io.py`, `ortho_io.py`, `copernicus.py`, `hansen.py`, `osm_features.py`, `tile_cache.py`, `zenodo_*.py`, `http_pool.py` | data access + caches |
| `austria_processor.py`, `peer_director.py`, `kg_splitter.py`, `v2_*.py`, `v21_products.py`, `v24_fast.py`, `ne_h3.py`, `ne_ingest.py`, `product_repair.py`, `label_mix.py`, `sliver_sweep.py` | batch processor + products |
| `search_index.py`, `kg_v2_store.py`, `kg_docs.py`, `cells_api.py`, `landscape_query.py`, `hillshade_tiles.py`, `public_flags.py`, `quality_flags.py` | serving-side stores and query modules |
| `safe_load.py`, `service_guard.py`, `mem_lru.py`, `config.py` | safety and limits |
| `static/` | UI (`index.html`), dashboards, `docs.html` |
| `data/` | shipped: `austria_processor/kg_list.json`, `kg_bbox_r1.json` (root), `best_model/rf_meta.json`, `segv2/models/*.meta.json`, `als_acquisition/*.geojson`, `models_manifest.json`; fetched: model weights; generated: everything else |
| `tests/`, `tools/`, `systemd/`, `docs/` | tests, scripts, unit templates, documentation |

## Limits

Analysis requests: Austria only, ≤ 25 km² on the operator port, ≤ 5 km²
on the public tier; two heavy tasks at a time. The public tier is
rate-limited and memory-guarded (`docs/public-api.md`). A full-Austria
run needs Copernicus credentials, a Zenodo account for publishing, and
weeks of compute on one machine; `--cache-only` reuses the shared tile
cache instead.

## Security

See `SECURITY.md` — the operator API is loopback-only by design, inputs
are validated, outbound hosts are allow-listed, and every pickle goes
through a whitelist unpickler.

## Licence

Code: MIT (`LICENSE`). Data and products: see above. Contributions:
`CONTRIBUTING.md`.
