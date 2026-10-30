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
  parsed with fiona / shapely / stdlib `xml.etree` only (no external
  entity resolution), capped at 5 000 features and 500 000 vertices per
  request, polygons repaired server-side, and validated against the
  Austrian bbox and the area limits before any raster is touched.
  Request bodies are capped (`SRTM_APP_MAX_BODY_MB`, 256 MiB on the
  operator port; 32 MiB through the public relay); gzip / ZIP uploads
  may not inflate beyond the same cap (≤ 200 ZIP members); Shapefile
  ZIPs are extracted with Python's `zipfile` (absolute and `..` members
  are stripped) into a private temp dir that is deleted afterwards.
  KG codes in paths and `?kg=` parameters are validated
  (`NNNNN` / `NNNNN-block`) before any file path is built from them.
* **Outbound fetchers** go through `http_pool.py` with the host
  allow-list `config.OUTBOUND_ALLOWED_HOSTS` (BEV, Copernicus Data
  Space, Zenodo, Hansen on `storage.googleapis.com`, Overpass) plus
  `SRTM_EXTRA_ALLOWED_HOSTS`; no URL supplied by a client is ever
  fetched. GDAL `/vsicurl/` reads use the same hosts. The one
  operator-supplied URL (`POST /api/v1/director/peers/add`, multi-VM only)
  must be `https://`, resolve to a non-loopback address and is checked by
  `app._outbound_url_ok`. Zenodo manifest entries (`link`, `bucket_url`)
  can originate from another instance and are treated as untrusted:
  `zenodo_client.is_zenodo_url` restricts every fetch built from them to
  the configured Zenodo host, and the Zenodo token travels only as an
  `Authorization` header to that host (never in a query string).
* **Pickle / checkpoint loading**: tile checkpoints
  (`tile_checkpoints/<kg>/tile_N.pkl`), restored checkpoint archives
  from the Zenodo cache deposit and every other pickle the pipeline
  reads go through `safe_load.py`, a whitelist unpickler (numpy
  reconstruction helpers and scalars, `affine.Affine`, `rasterio.crs.CRS`,
  `object_segmentation.SegmentedObject`, stdlib containers / datetime) —
  any other global raises `UnsafePickle`. `np.load(allow_pickle=True)` is
  routed through the same whitelist process-wide by `safe_load.install()`.
  Checkpoint bundles restored from Zenodo are untarred member-by-member
  (basename only, `tile_*.pkl` only, never `extractall`). Model files (scikit-learn / LightGBM joblib) cannot be
  whitelisted, so `safe_load.load_model` only loads them from the repo's
  `data/` tree after a checksum match against `data/models_manifest.json`
  (`SRTM_REQUIRE_MODEL_CHECKSUM=1` makes a missing checksum fatal).
  `tests/test_safe_load.py` covers both. A tampered Zenodo record can
  therefore break a run but not execute code.
* **Secrets** live outside the repo in `SRTM_SECRETS_DIR`
  (default `~/.config/srtm-lidar`, files `zenodo_token`,
  `operator_token`, `copernicus_credentials.json`) or in the service
  environment; nothing secret is written under `data/` (the
  `public_flags.salt` HMAC salt is the one random file, 0600).
  Stored error strings scrub `access_token=` query parameters.
* **Resource limits** on the public tier (`docs/public-api.md`):
  byte-bounded caches, a concurrency gate and a leaky-bucket rate limiter
  on every render, single-flight per tile, an RSS watchdog that sheds
  caches and finally restarts the worker, and systemd `MemoryMax` on
  every unit. Rate-limit buckets key on `X-Forwarded-For` only when the
  request arrived from a loopback / private proxy address, otherwise on
  the socket address. Public flags store an HMAC of reporter id | IP,
  never the raw values.
* **Dependencies** are pinned in `requirements.txt`; `pip-audit -r
  requirements.txt` is clean as of the last audit
  (`docs/security-audit.md`). Re-run it before each release.
* **Telemetry egress**: the operator dashboard geolocates the host once
  via `ipinfo.io` / `api.ipify.org` (region label only, no payload); the
  `SRTM_BEV_VECTOR_CONTEXT` feature, when enabled, installs a pinned Go
  binary and a pinned pip package from GitHub at first use — both off by
  default and operator-triggered.
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
