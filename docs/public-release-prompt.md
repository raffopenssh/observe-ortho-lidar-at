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
   Final tree: `git ls-files | wc -l` well under 250, no file > 50 MB,
   `du -sh` of tracked files < 60 MB.
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

Process: inventory first (`git ls-files`, the grep gates), post the plan as a
table (file → keep/delete/rewrite), then execute in order 0→7, re-running the
gates after each step. Report remaining hits you could not remove and why, plus the final `git ls-files` list.
Do not add features. Commit once at the end.
