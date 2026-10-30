# Security audit — public tree (Step 6)

Date: 2026-10-30. Scope: the public fork at this repository (all tracked
files + full git history), reviewed against the step-6 checklist of the
release spec. `make gates` and `make test` pass after every fix below.

Severity: **H** high, **M** medium, **L** low, **I** informational.
Status: **fixed** (in this audit), **accepted** (documented, no change),
**open** (must be handled before/at publication).

## Findings

| id | sev | location | finding | status |
|---|---|---|---|---|
| S-1 | H | git history (53 commits since Step 0) | Author/committer identity of every post-rewrite commit is the VM service account at the former hosting domain (username + hostname leak); the pre-Step-0 history still contains the maintainer's personal e-mail address in the former OSM/Overpass `USER_AGENT` string (4 diff lines, `osm_features.py` / `power_infrastructure.py`). The spec explicitly wanted personal e-mails out of the kept history. | **open** — needs one `git filter-repo --mailmap … --replace-text …` pass right before the push (this audit must not rewrite history); the exact mailmap / replacement lines were handed to the release manager out of band. |
| S-2 | I | git history | ~585 diff lines mention former fleet hostnames at the previous hosting domain and two NAT-pool /24 prefixes (`peer_director.py` docstring, still in the tree). Not secrets; the hostnames are already dead. | accepted (hostnames are not credentials; a full purge would need a second filter-repo pass — optional) |
| S-3 | I | history + tree | Token-pattern scan (`2dnLSA`, `ghp_`, `AKIA`, PEM headers, `sk-…`, JWT, `password=`, `api_key=`, `client_secret=` with values) over `git log -p --all`: 0 hits; the 10 occurrences of `REDACTED_ROTATED_ZENODO_TOKEN` are the filter-repo placeholder. `detect-secrets scan --all-files` on the tree: only sha256 pins (`ne_h3.py`, `segv2/model_v2.py`, `data/models_manifest.json`, `kg_universe.py`) and H3 cell ids (`public_docs.py`, `docs/ne-public-api.md`) — all false positives. `.env` exists locally but is git-ignored and untracked. | accepted |
| S-4 | L | `copernicus.py:402` | Hard-coded absolute cache path under the former user's home (username + private path; `mkdir` at import). | fixed → `config.DATA_DIR/copernicus_cache` (`COPERNICUS_CACHE_DIR` override, added to `.env.example`); comments naming the former service uid in `app.py` / `austria_processor.py` reworded. |
| D-1 | I | `safe_load.py`, `tile_cache.py`, `zenodo_cache.py`, `hansen.py`, `app.py:426`, `austria_processor.py:46` | Every `np.load(allow_pickle=True)` (11 sites) runs after `safe_load.install()` in the importing module; explicit `pickle.load` sites (`app.py` ×3 seg-cache, `austria_processor._load_tile_checkpoint`) call `safe_load.load`; joblib models (`learned_classifier`, `segv2/model_v2`, `segv2/train`) go through `safe_load.load_model` (checksum fence). No `yaml.load`, `marshal`, `dill`, `torch.load`, `shelve` in the tree. `tile_checkpoint_registry` untars bundles member-by-member (basename, `tile_*.pkl` only). `tests/test_safe_load.py` passes. | accepted (no gaps) |
| N-1 | H | `app.py` peer-sync + `backfill_jsons_from_manifest`, `v2_ingest._link`, `search_index._zenodo_url`, `zenodo_fetch._auth_headers` | Manifest entries (`link`, `bucket_url`) are merged from peers in multi-VM mode and were fetched verbatim **with the Zenodo bearer token attached** → token exfiltration / SSRF to any host a malicious peer names. | fixed: `zenodo_client.is_zenodo_url()` (https + host ∈ {ZENODO_BASE_URL host, zenodo.org, sandbox.zenodo.org}); every manifest-derived fetch is skipped unless it passes; `auth_headers(url)` and `zenodo_fetch._auth_headers` drop `Authorization` for non-Zenodo URLs. Single-VM deployments were not exposed (no peer manifests). |
| N-2 | M | `zenodo_cache.py` (`_request`, bucket PUT, `_authed_session`), `austria_processor.py:10760` | Zenodo token sent as `?access_token=` query parameter (ends up in proxy / server access logs; the Step-3 commit claimed this was gone). | fixed → `Authorization: Bearer` header. |
| N-3 | I | `app.py:/api/v1/director/peers/add` | Only client-supplied outbound URL. `https://` required, `_outbound_url_ok(allow_private=True)` rejects loopback / link-local / unresolvable hosts; route is operator-gated. | accepted |
| N-4 | I | all `requests.*` / `urlopen` sites (AST scan, 114 call sites) | Every outbound call passes an explicit `timeout`; `http_pool.install()` additionally pools module-level calls. No `verify=False`, no `/vsicurl/` URL built from request input (`ortho_io`, `tile_index`, `hansen` build them from fixed BEV / UMD bases). `/parse-geometry` and the overlay routes take file/geometry bodies only, never a URL. | accepted |
| N-5 | L | `app.py:_detect_region`, `host_telemetry.py:279` | Dashboard geolocation calls `ipinfo.io` / `api.ipify.org` (fixed URLs, no payload). Documented in SECURITY.md. | accepted |
| N-6 | L | `ne_h3.py` (`ensure_ne_cells_package`, `go install`) | With `SRTM_BEV_VECTOR_CONTEXT=1` the server pip-installs a git-pinned package and `go install`s a sha256-pinned binary at first use (supply-chain surface). Off by default, operator-triggered, pins + checksums present. Documented. | accepted |
| P-1 | M | `app.py` routes `/api/v1/kg/<kg_code>*`, `/api/v1/flags?kg=`, `/api/v1/processing/*?kg=` | KG codes were interpolated into `data/austria_processor/json/<kg>.json` paths without validation (route converters block `/`, but `?kg=../x` reached `Path.glob`/`exists`/`scan_json` on an open GET). | fixed: `before_request` validator — view args `kg`/`kg_code`/`code` must match `^[0-9]{5}(-[a-z]+[0-9]*)?$`, `?kg=` must match `^[A-Za-z0-9_-]{0,40}$` (partial filter allowed on the log route). Public tier already enforced 5 digits. |
| P-2 | I | `app.py` send_file sites (16) | Shares / exports / product download go through `_safe_child` (secure_filename + resolved-prefix) or `_safe_send_file`; task ids are regex-checked (`^[a-f0-9\-]+$`); mbtiles layer name passes `secure_filename`; `cells_api` h3b paths come from DB rows / 5-digit codes; the remaining `send_file`s serve server-generated temp files or fixed paths. | accepted |
| P-3 | M | `app.py:/api/v1/parse-geometry` | gzip and ZIP uploads were inflated without a bound (decompression bomb → disk/RAM). | fixed: inflated bytes ≤ `MAX_CONTENT_LENGTH`, ≤ 200 ZIP members. (`zipfile.extractall` already strips absolute / `..` members.) |
| P-4 | L | `peer_director.get_peer_log`, `_peer_commit_contains` | `tail -<lines>` from an unbounded int; peer-reported commit string passed to `git merge-base` argv (an option-looking value could change git's behaviour). | fixed: `lines` clamped 1–5000; commit must be hex (4–64). (Committed via the concurrent Step 9 commit `cbb7714`.) |
| A-1 | I | `app._enforce_operator_gate`, `public_segmentation.ROUTES` | Enumerated `app.url_map`: 96 non-GET rules — all gated (non-GET is operator-only unless the request carries the relay header **and** matches the explicit allow-list of 16 POST analysis routes). 13 GETs gated under `_OPERATOR_PREFIXES`, 77 open (read-only). `public_api.py` exposes 3 POSTs of its own (`/cells`, `/flags`, `/flags/<id>/status` — the latter needs the operator token) and relays only via the allow-list (anchored regexes, no catch-all); mutating relays never carry `X-Operator-Token`/`Authorization`/cookies (`_STRIP_IN`). | accepted |
| A-2 | I | `app._request_is_loopback` | No `ProxyFix`; loopback bypass needs `remote_addr ∈ {127.0.0.1, ::1}` **and** no `X-Forwarded-For`; the relay always sets XFF. A remote client cannot forge `remote_addr`. Claiming `X-Srtm-Public: 1` directly only *reduces* rights. Operator token compared with `hmac.compare_digest`. | accepted |
| A-3 | L | `service_guard.client_key` | Rate-limit bucket keyed on the first `X-Forwarded-For` hop unconditionally → a client hitting `:8100` directly could forge its bucket. | fixed: XFF trusted only when `remote_addr` is loopback/private (i.e. behind a proxy). |
| R-1 | I | `app.py:64`, `public_api.py:38` | `MAX_CONTENT_LENGTH` 256 MiB (operator) / 32 MiB (public); area cap `PUBLIC_MAX_AREA_KM2`=5 km² (25 km² operator); share cap `SHARE_CAP_BYTES`; tile renders gated (3 concurrent) + rate-limited (25/s, burst 150); relay heavy gate 2 + 0.5/s; flags POST 1/s; cells bbox ≤ 0.05°, ≤ 16 384 cells; landscape bbox ≤ 0.03 deg². | accepted |
| R-2 | L | `app._get_geometry` | No feature / vertex cap before `make_valid` + `unary_union` on untrusted geometry (CPU DoS within the body cap). | fixed: ≤ 5 000 features, ≤ 500 000 vertices. |
| R-3 | I | subprocess (19 sites) | No `shell=True`; argv lists only; user input reaches argv only in P-4 (now validated). No `eval`/`exec`. | accepted |
| C-1 | I | binds / debug / CORS | `app.run(debug=False)`; `APP_BIND` defaults to `127.0.0.1`, `PUBLIC_BIND` to `0.0.0.0` (read-only tier); gunicorn confs read the same config; CORS `*` only on read-only public GET paths (`public_api` and `llm_api.cors_path`); no `verify=False`; `tools/ca_check.sh` kept. No token/secret appears in log calls (grep `log.*token`): only "token re-read" / "no token" notices. | accepted |
| V-1 | M | `requirements.txt` | `pip-audit`: 74 advisories in pillow 12.2.0, requests 2.31.0, Flask 3.0.2, lxml 6.0.2. | fixed: pins bumped to pillow 13.0.0, requests 2.33.0, Flask 3.1.3, lxml 6.1.0 → `pip-audit` clean; `pip install --dry-run -r requirements.txt` resolves. **Not import-tested**: the system interpreter still has the old versions (not upgraded during the concurrent functional test); `make venv` on a fresh machine will exercise the new pins. |
| X-1 | I | `SECURITY.md` | Claimed shapely in the unpickle whitelist (it is not; `rasterio.crs.CRS`/`affine` are), omitted the manifest-URL / token-header rules, the upload inflate caps, the KG validation, the proxy-only XFF rule, the dependency policy and the two telemetry/auto-install egress paths. | fixed (SECURITY.md updated) |

## What a release manager still has to do

1. **S-1** — rewrite author e-mails and the one personal address with
   `git filter-repo` immediately before the first push (see the row).
2. Optionally purge the dead fleet hostnames from history (S-2).
3. Re-run `pip-audit -r requirements.txt` and `make check` in a fresh
   `make venv` to exercise the bumped pins (V-1).

## Method

* Secrets: regex scan over `git log -p --all` (900 MB), `detect-secrets`
  on the tree, author/committer e-mail listing, grep for e-mails, former hosting-domain
  hostnames, RFC1918 / public IPs and the former VM user / owner names.
* Deserialisation / network / subprocess: grep + an AST walk over every
  `requests`-style call checking for a `timeout` keyword.
* Routes: `app.url_map` enumeration against `_is_operator_path` and
  `public_segmentation.route_kind`; Flask test-client probes for the KG
  validator (`/api/v1/kg/../etc` → 404, `?kg=../x` → 400, `?kg=633` → 200).
* Dependencies: `pip-audit` in a scratch venv.
