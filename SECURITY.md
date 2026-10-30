# Security policy

## Reporting a vulnerability

Please report security issues privately through **GitHub security
advisories** of `raffopenssh/observe-ortho-lidar-at`
(*Security → Report a vulnerability*). Do not open a public issue. You
should hear back within a week; fixes are released as ordinary commits
with a note in the advisory.

## Scope and deployment model

One machine runs three services (see README → *Setup*):

| service | bind | exposure |
|---|---|---|
| `srv_public.service` — `public_api.py` | `0.0.0.0:8100` | **public**, read-only product corpus + an allow-listed relay of analysis routes |
| `srv.service` — `app.py` | `127.0.0.1:8000` | **operator API**: analysis engine, processor / director control, credentials |
| `austria_processor.service` | — | batch pipeline, outbound only |

Anything that lets a remote client reach an operator route, read a
secret, execute code, or exhaust the machine from the public port is in
scope. The threat model assumes `:8100` is reachable by anyone and
`:8000` only by the operator (loopback or `X-Operator-Token`).

## Trust boundaries and controls

* **Operator gate** (`app._enforce_operator_gate`): every state-mutating
  route and every `/api/v1/{admin,director,processing,credentials,…}`
  path requires a loopback source address without `X-Forwarded-For`, or
  the operator secret (`SRTM_OPERATOR_TOKEN` /
  `${SRTM_SECRETS_DIR}/operator_token`, compared with
  `hmac.compare_digest`). The public relay always sets
  `X-Forwarded-For`, so a relayed request can never pass as loopback;
  operator paths answer 404 through the relay.
* **Public relay allow-list** (`public_segmentation.ROUTES`): only the
  listed (method, path-regex) pairs reach `app.py`; area capped by
  `PUBLIC_MAX_AREA_KM` (5 km²), body ≤ 32 MiB, no catch-all.
* **Untrusted geometry input** (`geo_parse.py`, `_get_geometry`):
  GeoJSON / WKT / KML / GPX / GeoPackage / zipped Shapefile uploads are
  parsed with fiona / shapely only, size- and feature-capped, polygons
  repaired server-side, and validated against the Austrian bbox and the
  area limits before any raster is touched. Shapefile ZIPs are extracted
  with path checks into a temp dir.
* **Outbound fetchers** go through `http_pool.py` with the host
  allow-list `config.OUTBOUND_ALLOWED_HOSTS` (BEV, Copernicus Data
  Space, Zenodo, Hansen on `storage.googleapis.com`, Overpass) plus
  `SRTM_EXTRA_ALLOWED_HOSTS`; no URL supplied by a client is ever
  fetched. GDAL `/vsicurl/` reads use the same hosts.
* **Pickle / checkpoint loading**: tile checkpoints
  (`tile_checkpoints/<kg>/tile_N.pkl`), restored checkpoint archives
  from the Zenodo cache deposit, and the RF / LightGBM model files are
  loaded only through `safe_load.py`, a whitelist unpickler (numpy,
  shapely, scikit-learn / lightgbm estimator classes, builtins); any
  other global raises. `tests/test_safe_load.py` covers it. A tampered
  Zenodo record can therefore break a run but not execute code.
* **Secrets** live outside the repo in `SRTM_SECRETS_DIR`
  (default `~/.config/srtm-lidar`, files `zenodo_token`,
  `operator_token`, `copernicus_credentials.json`) or in the service
  environment; nothing secret is written under `data/` (the
  `public_flags.salt` HMAC salt is the one random file, 0600).
  Stored error strings scrub `access_token=` query parameters.
* **Resource limits** on the public tier (`docs/public-api.md`):
  byte-bounded caches, a concurrency gate and token-bucket rate limiter
  on every render, single-flight per tile, an RSS watchdog that sheds
  caches and finally restarts the worker, and systemd `MemoryMax` on
  every unit. Public flags store an HMAC of reporter id | IP, never the
  raw values.
* The processor runs as the service user, never root; child processes
  are spawned per KG and killed by cgroup limits on OOM.

## Out of scope

* Vulnerabilities that require operator access to the machine or to
  `:8000`.
* Denial of service by legitimately heavy analysis requests on the
  operator port (it is loopback-only by design).
* Issues in upstream data providers (BEV, Copernicus, Zenodo, OSM) or
  in third-party dependencies without a demonstrated impact here —
  please report those upstream; dependency bumps are welcome as PRs.
* Deployments that change `SRTM_APP_BIND` to a non-loopback address
  without configuring `SRTM_OPERATOR_TOKEN`.
