# API tiers — public parcel-free API vs hidden `/k/` full app (Oct 2026)

Why: the cadastre upstream (`example.invalid` → now
`example.invalid`) stopped publishing BEV Kataster data for copyright
reasons and moved its full API behind a token at `/k/…`. We mirror that:
**nothing parcel-keyed is public any more**, siedler-oesterreich fetches
parcels itself (bevdirect) and asks us about *bboxes*.

| tier | URL | what | code |
|---|---|---|---|
| PUBLIC | `https://example.invalid/` (port **8100** → exe.dev default port) | bbox / point / KG-code keyed landscape data, no cadastre-derived fields, index + grid25 fast path only, no Zenodo | `public_api.py`, `landscape_query.py`, `public_docs.py`, `srv_public.service` |
| PRIVATE | `https://example.invalid/k/<old path>` | everything `app.py` has, 1:1, bearer-token gated; 404 without token | `api_tiers.py`; proxied by `public_api.py` to `127.0.0.1:8000`; `app.py` strips `/k` itself (`StripPrefixMiddleware`) |
| dashboards | `https://example.invalid:8000/process.html?token=<admin token>` | user-authenticated exe.dev port; `?token=` once sets the `srtm_k` cookie | `app.py` `_enforce_private_only` / `_tier_cookie` |

## Public surface (`public_docs.LLM_TXT` is the contract)

`/api/v1/landscape?bbox=|lon/lat` (alias `/context`) — terrain (25 m
grid25, area-weighted cells), land-cover shares + `dom_terrain`, QA-filtered
trees, LiDAR building heights, forest loss, KG coverage/readiness.
`/api/v1/trees/bbox`, `/landmarks/bbox`, `/buildings/bbox`, `/heightfield`,
`/tiles/hillshade|slope`, `/api/v1/kgs`, `/api/v1/kg/<code>`
(+ `/llm/kg/<code>`, `/llm/kgs`, `/llm/manifest.json` aliases), `/status`,
`/license`, `/llm.txt`.

**Rules.** A public handler may read `kg`, `kg_landcover`, `kg_hansen`,
`kg_trees`, `kg_type_top`, `kg_buildings`, `kg_v2_store.grid25`,
`feedback.sqlite` flags, `data/ne/*.h3b` (cells). It must NEVER echo a
parcel id / parcel area / footprint id or accept a `parcel=` parameter
(the parcel-keyed index tables no longer exist — `docs/kataster-purge.md`).
Nothing may touch Zenodo / BEV / GPKG in the request path.

**Realistic trees** (`landscape_query.trees_bbox`): `kg_trees` rows minus
(a) QA flags `kind='top_tree'` severity high/critical (matched by rounded
lon/lat, per-KG 1 h cache; was `parcel_top_tree` before the purge),
(b) `rf_conf < 0.5`, (c) h outside 3–60 m. Landmarks likewise from `kg_type_top` + `kind='top_by_type'` flags
with per-type height windows. Siedler no longer needs `/api/v1/flags`.

## Tokens

* `data/admin_token` — cluster token; every peer has it, so peers reach the
  director through `/k` without code changes: `http_pool._inject_tier_auth`
  adds `X-Admin-Token` to every `requests.*` call to
  `example.invalid/k/…`. `director_ha.canonical_peer_url('primary')`
  = `https://example.invalid/k`.
* `data/api_tokens` — one line per external client (ops); ≥16 chars;
  rotate one without touching the others. Valid for `/k/` but **not** for
  admin endpoints. (Does not exist on the primary today — the admin token
  is the only `/k` token; no external `/k` client exists.)
* Transport: `Authorization: Bearer`, `X-API-Key`, `X-Admin-Token`,
  `?token=`, cookie `srtm_k`.
* `data/private_only` (flag file) → `app.py` answers the generic 404 to
  every non-loopback request without a valid token. Create it once the
  fleet has converged on the `/k` URL (`versions:` line) and the exe.dev
  default port has been switched.
* **Retired landscape routes** (`api_tiers.PRIVATE_ROUTES_LANDSCAPE` +
  `PRIVATE_KG_SUBROUTES_LANDSCAPE`: `/parcel*`, `/parcels/*`, `/query*`,
  `/lookup`, `/cadastre/*`, `/flags`, `/llm/kg|kgs|manifest.json`,
  `/kg/<c>/profile|buildings|new_buildings|infrastructure|parcels`): the
  handlers were deleted on 2026-10-13 (Kataster purge); while
  `data/k_landscape_off` exists (it does) `public_api.private_tier` answers
  410 `{"gone":true,"see":"/api/v1/docs/llm.txt#cells"}` even with a token,
  so stale clients get a pointer instead of a 404.

## Cadastre upstream (`umfeld.py`)

`CADASTRE_BASE = https://example.invalid/k/api/v1`; token in
`data/umfeld_token` (0600, gitignored — the repo is public) or
`$UMFELD_TOKEN`. `http_pool._inject_umfeld_auth` adds the bearer header to
every `requests.*` call to that host and raises `ConnectionError` when the
token is missing (so the processor retries instead of baking an empty
cadastre). The tier's token-refusal 404 body is treated as transient in
`austria_processor.fetch_cadastre_data`. Fleet distribution: peers send
`umfeld_token_sha` in the status push; the director POSTs its token to
`/api/v1/admin/install_umfeld_token` on mismatch (≤1/5 min/peer). Check:
`curl -s 'localhost:8000/process.txt?q=umfeld&log=50'`.

## Rollout / cutover checklist

1. (done) commit → push → `restart srv` + `srv_public` on the primary.
2. Watch `versions:` in `/process.txt` converge; peers on the new commit
   call `…/k/…` with the token (works before and after the port switch).
3. exe.dev: switch the VM's default port 8000 → **8100**. Peers keep
   working (same `/k` URL, now via the proxy). `:8000` stays reachable to
   the owner only.
4. `touch data/private_only` on the primary. **Peers get it automatically**:
   each status push carries `private_only: bool`; while the primary has the
   flag, the director POSTs `/api/v1/admin/set_private_only {enabled:true}`
   to any peer reporting `false` (≤1/5 min/peer, `_maybe_push_private_only`),
   so newly added peers are guarded on their first push. `deploy.sh` also
   touches the flag on a fresh install. Peers' `:8000` otherwise serves the
   full dashboards + the private-tier `/api/v1/docs/llm.txt` (repo `llm.txt`).
   Director→peer traffic is unaffected (peer URLs are `:8000` →
   `_request_via_owner_port`, plus admin headers). Audit:
   `journalctl -u srv | grep 'private_only guard pushed'`.
5. (done) siedler uses the public `llm.txt` (`/api/v1/docs/llm.txt`); the
   parcel-keyed routes are gone everywhere (`docs/kataster-purge.md`).

## Verifying

```bash
curl -s localhost:8100/api/v1/landscape?bbox=15.084,47.064,15.088,47.068 | jq .landcover
curl -s -o /dev/null -w '%{http_code}\n' localhost:8100/k/api/v1/info            # 404
TOK=$(cat data/admin_token); curl -s -H "Authorization: Bearer $TOK" localhost:8100/k/process.txt?log=0 | head -3
curl -s -o /dev/null -w '%{http_code}\n' localhost:8100/api/v1/query/parcels   # 404 (not public)
curl -s -H "Authorization: Bearer $TOK" localhost:8100/k/api/v1/query/parcels   # 410 gone (retired landscape route)
```

## Peers on `:8000` (since a9b8b94)

Non-primary VMs never serve the legacy app regardless of `data/private_only`:
a tokenless browser hitting any path on `https://srtm-lidar-atN.exe.xyz:8000/`
gets `api_tiers` "processing peer of srtm-lidar-at" note that redirects to the
public landing page after `PEER_NOTE_REDIRECT_S` (15 s); tokenless API calls
(incl. `/api/v1/docs/llm.txt`, `/llm.txt`, `/process.html`) get the
unadvertised 404; only `/api/v1/ping` stays open. Director / peer / HA traffic
is unaffected because `http_pool._inject_tier_auth` adds `X-Admin-Token` to
every call to a `srtm-lidar-*.exe.xyz` host. Peers still on an older commit
serve the legacy app until the wave rollout reaches them (`versions:` line).

```bash
curl -s -o /dev/null -w '%{http_code}\n' https://example.invalid:8000/api/v1/docs/llm.txt   # 404
curl -s -o /dev/null -w '%{http_code}\n' -H "X-Admin-Token: $TOK" https://example.invalid:8000/api/v1/processing/status  # 200
```

## Public `/segmentation` UI (Oct 2026)

`public_segmentation.py` (blueprint registered by `public_api.py`) serves
`static/index.html` at `https://example.invalid/segmentation` with
every `/*PRIVATE*/ … /*/PRIVATE*/` block stripped (cadastre toggle +
evaluation, dashboard links, `training/status` + `processing/status`
pollers) and `window.SRTM_PUBLIC=true` injected. The *same* file keeps
serving the owner UI on `:8000` unchanged — mark anything owner-only with
those comment fences (HTML: `<!--/*PRIVATE*/-->…<!--/*/PRIVATE*/-->`).

Relay: `public_api`'s 404 handler hands unmatched `/api/v1/*` to
`public_segmentation.relay()`, which forwards **only** the
`ROUTES` allow-list (segment/elevation/terrain, progress/result/abort,
overlays, exports + downloads, parse-geometry, layers, attribution,
shares) to `127.0.0.1:8000` with `X-Srtm-Public: 1` and all
auth/XFF headers stripped. app.py (`# === SECTION: public-tier relay`)
then:

* forces `include_cadastre=false`, drops `cadastre_evaluation`, pins
  `source` to `auto|product|bev` (default auto);
* caps area at `PUBLIC_MAX_AREA_KM2` (5 — a 12 km² product build cost ~1.2 GB RSS and wedged srv on 2026-10-19);
* stores shares in `data/shares/public/` and tags async tasks
  (`<task>.public`), so public callers only ever see their own;
* answers 404 for any admin route (defence in depth).

Every JSON body on the way back is passed through `scrub()` (keys /
strings carrying a `cells_api.FORBIDDEN_WORDS` token are dropped /
masked). Primary VM only (`is_primary()`, peers answer 404). Guards: 30 heavy POSTs/min per client, gate of 2 concurrent
heavy relays per worker (`SEG_MAX_CONCURRENT`, `SEG_RATE_PER_S`).

`source=auto` → `product_segments.build()`: if every KG under the area has
a finished v2.x product (`trees_v3.resolve_coverage`, ≥50 % covered, no
live parents) the light GPKG `segments` layer is read (GpkgCache) and
rasterised at 1 m, so results come back in seconds with
`meta.source="product"`, `product_codes`, `product_version`,
`product_obs_year`. **Cost gate** (Oct 2026): light GPKGs are whole-KG
files (0.1–1 GB each), so `auto` only downloads when the not-yet-local
bytes (`trees_v3.pending_download`, `.tmp` partials credited) are within
`product_segments.download_budget(area_ha)` =
`max(SEG_AUTO_DL_FLOOR_MB=128, SEG_AUTO_DL_PER_HA_MB=2 × ha)`; a 60 ha AOI
spanning three ~500 MB products therefore segments live (seconds of BEV
range reads + cached 0.1° Copernicus tiles instead of a 15 min / 2 GB
pull), with `meta.auto_reason` / a `warnings` entry saying why.
`source=product` always downloads. Otherwise the live BEV pipeline runs
(`source="bev"`).
Overlays + exports use the same `_seg_cache` entry (`…_product` key).

Test before touching any of this: `python3 tests/test_public_segmentation.py`
(rendered page / module / docs section word ban, scrub, allow-list).
