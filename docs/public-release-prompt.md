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
7. Rewrite `AGENTS.md`/`docs/` for the public layout: drop
   `security-tokens.md`, `api-tiers.md`, `kataster-purge.md`, umfeld sections;
   remove every live URL/token mention.

Process: inventory first (`git ls-files`, the grep gates), post the plan as a
table (file → keep/delete/rewrite), then execute in order 1→7, re-running the
gates after each step. Report remaining hits you could not remove and why.
Do not add features. Commit once at the end.
