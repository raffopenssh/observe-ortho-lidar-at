# Classifier models (RF v1 / LightGBM v2)

The segmentation pipeline classifies each segment with a trained model:
the v1 **Random Forest** (`learned_classifier.py`) or, when available,
the v2 **LightGBM** model (`segv2/`, see `segv2/README.md`). Without any
model the rule-based classifier in `object_segmentation.py` takes over,
so a missing model degrades quality but never breaks a request.

**Retraining tooling is not part of the public release.** Labelling the
training segments needs reference data that is not public; only the
fitted estimators ship. This doc covers what ships, how it is loaded,
and the contract you must keep when touching features.

## The shipped models

| model | file (not in git) | metadata (in git) | facts |
|---|---|---|---|
| v1 RF | `data/best_model/rf_model.joblib.xz` (≈ 100 MB) | `data/best_model/rf_meta.json` | 16 classes (`bare_soil crop earthwork garden grass orchard parking path road rock roof shrub tree tree_loss vineyard water`), 87 496 training segments, OOB ≈ 0.70, 200 trees / depth 20 / min leaf 5 / balanced weights |
| v2 LightGBM | `data/segv2/models/model_G.joblib` | `data/segv2/models/model_G.meta.json` | 117 features, 14 classes, CV macro-F1 0.78 |

Download: `python3 tools/fetch_models.py` reads `data/models_manifest.json`
and fetches each entry from `MODELS_BASE_URL` (any HTTP directory) or the
Zenodo record named in the manifest, checking the checksum.

Provenance: segments were labelled from land-use polygons, building
footprints and OpenStreetMap with height / NDVI sanity checks per label
(ground-surface codes rejected under canopy or when NDVI > 0.25), plus
synthetic `tree_loss` labels from Hansen loss with the circular Hansen
features zeroed. `rf_meta.json` records per-class OOB accuracy and
feature importances so you can tell which model you are running.

## How a model is loaded

`LearnedClassifier.load()` prefers `data/best_model/` and calls
`safe_load.load_model(path)`:

* the file must sit under the repo's `data/` tree;
* its checksum must match the `data/models_manifest.json` entry (or an
  explicit `sha1=` / `md5=`); a mismatch raises `ModelChecksumError`;
  `SRTM_REQUIRE_MODEL_CHECKSUM=1` makes a missing checksum fatal;
* `.xz` archives are decompressed next to the file on first use
  (`safe_load.ensure_decompressed`);
* only then is `joblib.load` called. Every other pickle in the project
  goes through `safe_load.load` (whitelist unpickler) — never bare
  `pickle.load`.

v2 is used iff `segv2.model_v2.available()` (`lightgbm` importable and
the model file present); `MODEL_VERSION` in the products records which
one produced them.

## Feature contract

`learned_classifier.FEATURE_KEYS` (v1) and `feature_keys` in the v2
`.meta.json` define the column order; `object_segmentation.extract_object_features`
must emit exactly those keys. Changing the feature count or order
silently breaks inference (wrong columns, no error) — without the
retraining tooling, **do not add, remove or reorder features**; new
features belong in a new model version with its own meta file.

All keys derive from rasters (DTM/DSM/ortho/Sentinel-2/-1/WorldCover/
Hansen) and segment shape. Two distance features depend on vector
context that is optional in the public release:

* `dist_parcel_edge` — no source in the public release; it is fed as the
  constant 500 m cap (`segv2/features.py`: missing distance → 500 m).
  Documented degradation: field-boundary cues are lost, so the
  crop / orchard / vineyard / garden split and the `garden`-vs-`grass`
  decision rely on spectral and height features alone. Expect a few
  points lower accuracy on those classes compared with `rf_meta.json`.
* `dist_building` — from OSM buildings (optional OSM geometry service / Overpass when
  configured) or the in-RAM BEV vector context (`SRTM_BEV_VECTOR_CONTEXT=1`,
  `docs/vct-pilot.md`); otherwise the same 500 m cap.

## Operations

```bash
python3 tools/fetch_models.py                   # fetch / verify models
python3 -c "import learned_classifier as l; c=l.LearnedClassifier.load(); print(c.model_hash, len(c.classes), c.oob_score)"
python3 -c "import segv2.model_v2 as m; print(m.available())"
python3 tests/test_safe_load.py
```
