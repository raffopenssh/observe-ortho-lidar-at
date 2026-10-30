# Cross-cutting concerns (changes that span multiple files)

These are the dangerous changes — they touch many files and are easy to
break.

## Adding / changing an object type

1. `object_segmentation.py` → `OBJECT_TYPES` dict (canonical type → code).
2. `object_segmentation.py` → `GROUP_TYPES` (if it belongs to a group).
3. `object_segmentation.py` → `classify_object()` (rule-based logic).
4. `object_segmentation.py` → `TYPE_LETTER` / `LETTER_TYPE` (compact
   single-letter codes; table in `docs/austria-processor.md`).
5. `app.py` → `SEGMENT_COLORS` (RGBA for overlays / exports).
6. `austria_processor.py` → `SEGMENT_COLORS` (**duplicate** of app.py's —
   must match).
7. `static/index.html` → JS `TYPE_COLORS` (frontend legend — must match).
8. `segv2/labels.py` if the LightGBM v2 model should learn it
   (`segv2/README.md`).

`grep -rl 'SEGMENT_COLORS' *.py static/*.html` lists every copy.

## Adding / changing an RF feature

1. `learned_classifier.py` → `FEATURE_KEYS` (canonical feature order).
2. `object_segmentation.py` → `extract_object_features()` must populate
   the new key.
3. Retrain (`docs/rf-training.md`): the saved `rf_model.joblib.xz` becomes
   incompatible when the feature count or order changes;
   `data/best_model/rf_meta.json` records the feature list the model was
   trained with.

## Changing the Copernicus credential set

1. `POST /api/v1/credentials {client_id, client_secret, label?, validate}`
   on the operator API (or the dashboard's Credentials panel). New
   credentials are probed (OIDC client-credentials) and persisted to the
   credentials file (`config.COPERNICUS_CREDENTIALS_FILE`); the next KG
   picks them up, no restart needed.
2. `rm -f data/austria_processor/copernicus_paused` (clears a throttle
   pause), `rm -f data/austria_processor/openeo_circuit.json` (resets the
   circuit breaker).
3. `_exhausted_cred_indices` is mirrored into the credentials file
   (`exhausted: true`) so a fresh subprocess inherits the state.
   `POST /api/v1/credentials/validate` re-probes everything (e.g. after
   monthly credit renewal).
4. `@_retry_on_rotation` tries `len(credentials)+1` attempts, so adding or
   removing credentials changes retry depth automatically.
5. Usage telemetry per credential (`copernicus_credential_usage.json`,
   per-hour buckets, 7-day window) is written by `_run_datacube` and
   `_download_month_sequential`; new code paths that hit Copernicus must
   call `copernicus.record_credential_usage(cred_index, kind, product)`.

The processor honours `COPERNICUS_CRED_INDICES="0,2"` (restrict to a
subset) and `KG_CELL_FILTER="[[s,n,w,e],…]"` (restrict to planning cells)
in its environment — the director sets both when several machines run
frontier work in parallel (`docs/peer-director.md`).

## Changing the tile grid / overlap

1. `austria_processor.py` → `_compute_tile_grid()` (`tile_km`, `overlap_km`).
2. `tile_cache.py` → grid sizes per source (0.1° Copernicus, 0.5° Hansen).
3. Invalidate tile checkpoints:
   `rm -rf data/austria_processor/tile_checkpoints/` (drops metadata
   pickles + raster sidecars). If a `chkpt_<kg>.tar.gz` exists for the KG
   in the Zenodo cache deposit it is downloaded back at the top of
   `process_one_kg()` — evict it too via
   `tile_checkpoint_registry.delete_kg(<kg>)` on schema bumps.

## Cache-only cache-miss avoidance

When a cache-only processor hits a missing tile it records the KG in
`data/austria_processor/cache_miss_kgs.json` (via
`POST /api/v1/processing/cache_misses`) together with a fingerprint of the
relevant Zenodo cache strip (`updated_at` of the strip ZIPs in
`cache_manifest.json`). The director excludes the KG from cache-only
whitelists until the strip changes, so a known-incomplete KG is not
re-tried for nothing.

## KG runtime limits

`KG_TIMEOUT_SECONDS` (first attempt, 16 h) / `KG_RETRY_TIMEOUT_SECONDS`
(retry, 12 h). Long KGs (`gpkg_full` at 11 h) finish well inside the
wall; the dominant warning pattern is transient openEO 503 / NDVI read
timeouts, which the month-skip logic already handles. Revisit only if
`Maximum processing time … exceeded` warnings appear in
`/process.txt?warn=1`.

## Known quirk: `sar.*_mean_db` is linear σ⁰

`copernicus.fetch_sar` requests `sigma0-ellipsoid` (linear) and nothing
converts to dB, yet the KG JSON summary (`sar_info["<band>_mean_db"]`),
the search-index columns `sar_vv_mean_db` / `sar_vh_mean_db`, the
dashboard card and the full-GPKG `SAR_VV`/`SAR_VH` QML styles (stretched
−25…0 dB) all say dB. Typical values are VV ≈ 0.09–0.12, VH ≈ 0.02–0.03
**linear** (≈ −10 / −16 dB). Per-segment `sar_vv` / `sar_vh` features and
the rule thresholds in `object_segmentation.py` are linear too, so
classification is self-consistent.

**Decision: not fixed in the product** — every published JSON carries the
same semantics, and converting would create a mixed corpus. Treat the key
name as a misnomer: `x_dB = 10·log10(value)`. If you ever change it,
convert at ingest in `search_index` (values > 0 ⇒ linear) rather than in
the producer, and fix the QML stretch (~0–0.5 / 0–0.1) at the same time.

## Navigation cheatsheet

```bash
grep -rn '# === SECTION' *.py                    # every section in every long file
grep -n '# ===' austria_processor.py             # sections of one file
grep -rl 'SEGMENT_COLORS' *.py static/*.html     # every copy of the colour table
grep -n 'def process_one_kg' *.py
grep -A60 'FEATURE_KEYS = \[' learned_classifier.py
cat data/austria_processor/current_step.json | python3 -m json.tool   # current processor step
grep -rn '# ===' *.py | sed 's/# === SECTION: //' | sed 's/ ===//' | column -t -s:
```
