# Public-repo release — agent prompt (copy below the line)

Context for maintainers (review 2026-10-30): old Zenodo token in git history
(`8912a9f`; already rotated after the eMudhra fix — verified the live token
differs), ~20 files with hardcoded exe.xyz hosts, `/k` tier plumbing in ~15
files, cadastre/parcel code in ~25 files, no public-tier access log. Decision:
squash to ONE commit. Prompt below is self-contained.

---

You are preparing a clean PUBLIC fork of `/home/exedev/srtm-lidar` (Austrian
landscape segmentation: BEV LiDAR/ortho + Sentinel + NE H3 cells, products on
Zenodo). Work in a NEW directory `/home/exedev/srtm-lidar-public`, fresh git
history: exactly ONE commit, author `srtm-lidar maintainers <noreply@…>`
(the old history contains a since-rotated Zenodo token and personal emails).
`paxl` must keep working against the public tree: <FILL IN what paxl is / how
to test it> — treat its interface as frozen. Never
touch the live repo or services. Read `AGENTS.md` and `docs/api-tiers.md`,
`docs/vct-pilot.md`, `docs/security-tokens.md` first; load other `docs/*.md`
only when editing that subsystem.

Hard requirements:
0. Repo hygiene — only shippable files survive. DELETE: the ~27 root-level
   analysis/prompt/scratch files (`BATCH_*.md`, `CLASSIFIER_REDESIGN_PATCHES.md`,
   `CODEBASE_SUMMARY.md`, `DATA_FLOW_SUMMARY.md`, `ENRICHMENT_PIPELINE_ANALYSIS.md`,
   `EXTRACTED_CODE_SECTIONS.md`, `GAP_ANALYSIS.md`, `IMPLEMENTATION_*.md`,
   `MAIN_LOOP_ANALYSIS.md`, `QUICK_REFERENCE*`, `README_REVIEW.md`,
   `REDESIGN_SUMMARY.txt`, `REVIEW_SUMMARY.md`, `TECHNICAL_*.md`,
   `app_code_extraction.md`, `aspect_addition_plan.md`,
   `austria_processor_sections.txt`, `bev_operate_check_*.txt`,
   `conversation-summary-*.md`, `enhanced_queue_design.md`,
   `national-park-kg-api-calls.md`, `prompt-rf-cleanup.md`); tracked runtime
   junk under `data/` (`MONITOR.md`, `monitor_state.json`, `next-prompt*.md`,
   `feedback-system-prompt.md`, `oob_*`, `segv2/*.jsonl|*.log`,
   `best_model/best.log`) and `calibration_results/` (102 files);
   `static/insights/`; `docs/` files that describe private ops only
   (`security-tokens.md`, `api-tiers.md`, `kataster-purge.md`,
   `v2.4-ne-proposal.md`, `forestry-vm-prompt.md`, `*-handoff.md`,
   `*-single-peer-check.md`, `public-release-prompt.md`, `FEEDBACK-*-REPLY.md`
   unless genuinely methodological). KEEP + review: `README.md`, `AGENTS.md`,
   `llm.txt`, `requirements.txt`, `*.service`/`*.timer` (templated),
   `kg_bbox_r1.json`, `data/als_acquisition/*.geojson`,
   `data/best_model/rf_model.joblib.xz` + `rf_meta.json`,
   `data/segv2/models/*` (check size/licence; >50 MB → fetch from Zenodo at
   first run instead), remaining `docs/*.md`, `tests/`, `tools/`.
   Models: `data/best_model/rf_model.joblib.xz` (99 MB) and
   `data/segv2/models/model_G.joblib` (36 MB, `model_D` is NOT tracked but
   referenced) → do not commit; ship `rf_meta.json` + `*.meta.json` +
   `zenodo_models_manifest.json` and a `tools/fetch_models.py` that downloads
   them from the public Zenodo record on first run (md5-checked).
   `data/austria_processor/kg_list.json` (7850 KGs, 4.6 MB) is untracked today
   but REQUIRED by the processor — commit it (or a generator). Final tree:
   `git ls-files | wc -l` well under 250, no file > 20 MB.
1. NO secrets, NO private-tier: delete `api_tiers.py`, `peer_tokens.py`,
   `director_ha.py`, the `/k` reverse proxy in `public_api.py`, the
   `X-Admin-Token`/outbound token hook in `http_pool.py`, all `/k` paths,
   `data/admin_token|umfeld_token|api_tokens|mesh_token|peer_tokens.json`
   reads. Grep gate: `grep -rnE '/k/|/k\b|admin_token|umfeld_token|X-Admin-Token|Bearer' --include=*.py --include=*.html --include=*.sh .` → 0 hits outside tests you rewrote.
2. NO hardcoded hosts: every `*.exe.xyz` (srtm-lidar-at*, umfeld-at,
   cadastre-process-api, austria-power, pid, forestry) and peer-id list in
   `deploy.sh`, `add_peer.sh`, `peer_director.py`, `http_pool.py`,
   `attributions.py`, `infrastructure_lookup.py`, `power_infrastructure.py`,
   `static/*.html` becomes config: `config.py` reading env
   (`SRTM_PRIMARY_URL`, `SRTM_PEERS` list, `POWER_API_URL`, `UMFELD_URL`),
   each optional with a "feature disabled" default. Ship `.env.example`.
   Grep gate: `grep -rn 'exe.xyz' .` → only `.env.example` + README.
3. NO parcels / cadastre persistence: delete `cadastre.py`, `live_cadastre.py`,
   `umfeld.py`, `segv2/parcel_elevation.py`, `bulk_export.py`,
   `fix_frav_inflation.py`, `reupload_fixed_jsons.py`, `backfill_detail.py`,
   `train_rf_100kg.py`, deprecated `landscape_classifier.py`,
   `object_classifier.py`, `scene_adaptive_classifier_patches.py`,
   `calibration_results/`. Strip parcel/cadastre branches from
   `austria_processor.py`, `app.py`, `v2_verify.py`, `search_index.py`,
   `zenodo_client.py`, `learned_classifier.py`, `object_segmentation.py`,
   `segv2/labels.py`, `train_rf_4000kg.py`, `calibrate.py`, `static/*.html`.
   ONLY allowed cadastre path: `vct_pilot.py` + `ne_h3.py` bevdirect/vtcseamless
   mode — in-RAM, from public BEV vector tiles, never written to disk, product
   or log. Make that the sole `building_mask` source, default off.
   Grep gate: `grep -rniE 'parcel|cadastre|kataster|grundst|umfeld' --include=*.py --include=*.html .` → hits only in `vct_pilot.py`/`ne_h3.py` bevdirect block + attribution text.
4. RF model: check `learned_classifier.FEATURE_KEYS` vs
   `object_segmentation.extract_object_features()`; if cadastre-derived
   features remain, either make them nullable (document the degradation) or
   retrain. Model `.joblib.xz` must load and `python3 -c 'import app, public_api, austria_processor, peer_director'` must succeed.
5. Keep working single-VM: `public_api.py` on :8100 + `app.py` on :8000
   (same-host, loopback, no token), processor + director in single-peer mode
   (`SRTM_PEERS` empty). Provide `systemd/*.service` templates with `${HOME}`
   paths, a `README.md` (setup, data sources + licences from
   `attributions.py`, Zenodo deposit 19650075 read-only usage, env vars) and
   `make check` running the surviving `tests/` + the three grep gates.
6. Security audit of the final tree (report as a checklist, fix what you can):
   - secret scan: `pip install detect-secrets && detect-secrets scan` (or
     gitleaks if available) on the tree AND the single commit; also grep
     for `@gmail`, `exedev@`, private IPs, `data/*_token` paths.
   - unsafe deserialisation: all `pickle`/`np.load(allow_pickle)`/`joblib`
     must go through `safe_load.py`; tile checkpoints pulled from Zenodo are
     untrusted input — keep the fence, document it.
   - network: no `verify=False`; keep `tools/ca_check.sh`; every outbound URL
     built from user params (`/parse-geometry`, overlay/url fetchers, ~28
     `requests.*(url…)` sites) gets an allow-list (BEV, Copernicus, Zenodo,
     UMD, Overpass) → no SSRF to loopback/metadata.
   - files: `send_file`/`send_from_directory` (16 sites) and share/export
     paths must be `secure_filename` + prefix-checked; no path traversal.
   - admin/processor/director routes: anything mutating state
     (`/processing/*`, `/director/*`, `/admin/*`, `/credentials`) must bind
     loopback-only or require a locally generated token read from a file
     outside the repo; never open on 0.0.0.0 by default. CORS `*` only on
     read-only public GETs.
   - resource limits: keep `service_guard`, `mem_lru`, tile rate-limit, 5 km²
     public segmentation cap, 1 GB share cap; confirm body-size limits.
   - deps: pin `requirements.txt`, run `pip-audit`; no `debug=True`.
   - add `SECURITY.md` (report channel, scope, threat model: public read API +
     single-VM operator).
7. Docs clean-up — EVERY surviving `.md`/`.txt`/`.html` (AGENTS.md, llm.txt,
   README, `docs/*.md`, `segv2/README.md`, `static/docs.html`, docstrings in
   module headers) is rewritten, not just grepped: remove live URLs, `/k`,
   tokens, peer ids (`at3`, `at106`…), umfeld/cadastre/parcel/Kataster
   history, incident dates and "Correction (2026-…)" notes that only make
   sense with the private fleet; drop sections about HA, admin tokens,
   owner channel, bandwidth-wall parks for 50 VMs; keep architecture,
   product spec (v2.4 layout), algorithms, data sources + licences, single-VM
   ops. AGENTS.md ≤ 150 lines, pointing only at docs that exist.
   Grep gate (docs): `grep -rniE 'exe\.xyz|/k/|/k\b|token|umfeld|parcel|cadastre|kataster|paxl|@gmail|exedev' --include=*.md --include=*.txt --include=*.html .` → 0 hits except the vtcseamless paragraph in `docs/vct-pilot.md` and licence text in README.

8. Austria-processor dependency closure. `austria_processor.py` imports these
   local modules — each must exist after the cuts or be stubbed with the same
   call signature: attributions bev_proxy copernicus gpkg_streamed
   host_telemetry http_pool infrastructure_lookup kg_json_v2 kg_splitter
   kg_universe learned_classifier ne_h3 object_segmentation ortho_io
   product_repair quality_flags raster_io safe_load sliver_sweep
   terrain_analysis tile_cache tile_checkpoint_registry tile_index
   tile_raster_sidecar tree_inventory v21_products v24_fast v2_source
   v2_verify vct_pilot zenodo_cache zenodo_client zenodo_lock. REMOVE its
   imports of `cadastre`, `umfeld`, `peer_tokens`, `fleet_secrets` (replace
   with `config.py` lookups). Also check `segv2/labels.py` (INVEKOS GPKG path,
   `/k` refs) and every module the above import transitively:
   `python3 -c "import austria_processor, app, public_api, peer_director, public_segmentation, cells_api, search_index"` on a clean venv from `requirements.txt`
   must succeed. Runtime files it needs under `data/austria_processor/`
   (`kg_list.json`, `kg_bbox_r1.json` at root, `progress.json` etc. created
   on demand) — document which are shipped vs generated.

9. Thorough functional test — run on the public tree against a fresh venv,
   services started from the templated units on :8000/:8100 (test VM or
   different ports; never touch the live `srv`/`srv_public`). Record every
   result in `docs/release-test.md` (command, expected, actual). Must pass:
   a. `make check` (surviving `tests/*.py` — delete `test_peer_tokens.py`,
      `test_fleet_secrets.py`; keep `test_public_segmentation.py`,
      `test_safe_load.py`, `test_cells_*`, `test_trees_v3.py`,
      `test_v2*`, `test_label_mix.py`, `test_product_repair_footprint.py`,
      `test_sweep_products.py`, `test_zenodo_cache_reconcile.py`,
      `test_public_flags.py`, `test_als_years.py`, `test_grid_anchor.py`,
      `test_stats_unit.py`) + the grep gates + `pip-audit`.
   b. `/segmentation` end-to-end in a real browser (use the browser tool):
      page loads with no console errors/404s; draw a ~0.5 km² polygon in a
      v2.4-covered KG (e.g. around Krems 48.41,15.60) → `POST /api/v1/segment`
      → poll `/segment/progress` → `/segment/result` returns features with
      types/colours matching `TYPE_LETTER`; legend, type toggles, overlay
      (`/segment/overlay`), `/elevation`, `/terrain`, `/layers?bbox=`,
      `/parse-geometry` (KML + GeoJSON + WKT upload), share create → reload by
      share URL → `/shares` list; abort mid-task; 5 km² cap rejects a 6 km²
      polygon with a clear message; polygon outside Austria → clean error.
      Screenshot the result and verify visually that it looks right.
   c. Public API smoke (every route in `public_docs.LLM_TXT`): `/api/v1/health`,
      `/api/v1/docs/llm.txt`, `/api/v1/kg/<code>` for a parent + a split
      block, `/kg/<code>/segments|layers|landcover|heightfield`,
      `/api/v3/trees?bbox=`, `/api/v1/trees/bbox`, `/api/v1/cells?bbox=`,
      `/cells/<h3>`, `/api/v1/onestop?bbox=&format=geojson|gpkg`,
      `/index/status`, `/insights`, `/api/v1/flags` GET+POST, `/attribution`,
      hillshade/tile PNGs for 3 zoom levels, `/api/v1/prewarm`. Assert 200 +
      schema + `product_version "v2.4"` ⇒ cells present; retired routes → 410;
      no response body contains `parcel`, `cadastre`, `/k/`, a token or an
      `exe.xyz` host.
   d. Processor single-KG run: `python3 austria_processor.py --single <small KG>`
      (pick one ≤ 6 tiles, e.g. from `kg_list.json` by tile count) with
      Zenodo upload disabled (`--dry-run`/`ZENODO_TOKEN` unset → write products
      to `out/`), with and without `vct_pilot` bevdirect mode. Verify: all
      tiles complete, `_full.gpkg`, `_light_v2.gpkg`, `.h3b`, `_json_v2`,
      `_json` written; `v2_verify.verify_before_upload` passes; the GPKG
      check from AGENTS.md (integrity ok, no `parcels`/`buildings` layers,
      55/54 props); no `parcel|cadastre` string in any output file or log;
      no outbound connection to any `exe.xyz` host (`ss -tnp`/strace or a
      `http_pool` log) — only BEV, Copernicus, UMD, Overpass, Zenodo.
      Then `--single` on a >28-tile KG to exercise `kg_splitter` blocks, and
      the resume path (kill mid-KG, restart, checkpoints picked up).
   e. Director in single-VM mode: start `app.py` with `SRTM_PEERS` empty,
      `GET /api/v1/director/status` and `/process.txt` work, no crash loop,
      no attempt to contact peers; `/api/v1/processing/queue` POST/GET,
      `start/stop/pause/resume`; dashboard `/process.html` renders in the
      browser with no 401/403/404 and no token prompt.
   f. Memory/limits: `service_guard` watchdog doesn't spam (fix the current
      bug where it logs every 30 s with all caches already at 0 — back off or
      only log on actual shedding); hit the tile endpoint 200× and confirm
      rate limiting + disk cache; `/api/v1/health` stays `ok`.
   Anything failing in a–f blocks the release; do not mark done with a known
   red.

Process: inventory first (`git ls-files`, the grep gates), post the plan as a
table (file → keep/delete/rewrite), then execute in order 0→9, re-running the
gates after each step. Report remaining hits you could not remove and why, plus the final `git ls-files` list.
Do not add features. Commit once at the end.
