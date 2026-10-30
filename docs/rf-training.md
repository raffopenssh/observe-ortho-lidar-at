# RF training (`train_rf_4000kg.py`)

Background job that trains the Random Forest classifier used by the
segmentation pipeline (`learned_classifier.py`). Runs as
`rf_train.service` (template in `systemd/`) or by hand:
`python3 train_rf_4000kg.py`. Log: `/tmp/rf_train_4000kg.log`. Status:
`GET /api/v1/training/status` on the operator API.

## The shipped model

`data/best_model/rf_model.joblib.xz` (≈ 100 MB, **not** in git —
`python3 tools/fetch_models.py` downloads it from the public Zenodo
models record and checks the md5) with its metadata in
`data/best_model/rf_meta.json` (committed): 16 classes (`bare_soil crop
earthwork garden grass orchard parking path road rock roof shrub tree
tree_loss vineyard water`), ≈ 87 k training segments, OOB accuracy ≈ 0.70
(per-class OOB in the file). The model is loaded through `safe_load.py`
(allow-listed unpickler).

The shipped model was trained on segments labelled from **land-use and
building-footprint ground truth plus OpenStreetMap**. Only the trained
estimator ships; none of the label sources are part of a product.
The public training script labels from OpenStreetMap (and, when
`SRTM_BEV_VECTOR_CONTEXT=1`, from in-RAM building footprints derived from
public BEV vector tiles — `docs/vct-pilot.md`), so a retrained model will
have a somewhat different label distribution; `rf_meta.json` tells you
which model you are running. <!-- VERIFY: label sources left in
train_rf_4000kg.py after the code pass -->

## What it does (in 5 lines)

1. Sample N random KGs across Austria (`kg_list.json`).
2. For each KG: fetch the ground-truth layers, fetch all raster data
   (DTM/DSM/ortho/NDVI/SAR/Hansen), run the segmentation, label each
   segment from ground truth.
3. Save labelled features as a checkpoint (`.npz`) to disk.
4. Every 10 KGs: load all checkpoints, train the RF, save the model to
   `/tmp/learned_classifier/`.
5. KGs that crash (OOM, bad data) are recorded in `bad_kgs.json` so
   retries don't loop.

Checkpoints are durable: restarting resumes where it stopped and
retrains from the full checkpoint corpus.

## Storage layout (`rf_training_data/`)

```
rf_training_data/
  checkpoints/kg_<code>.npz    ← X, y, group (one row per segment)
  bad_kgs.json                 ← permanently skipped KGs
  copernicus_cache/ hansen_cache/ osm_cache/   ← reused across runs
```

## Ground-truth pipeline

Labelling segments is the hard part. Every label source goes through
height and spectral sanity checks:

```
For each segment:
    ├─ 1. polygon ground truth rasterised at 1 m, height-aware
    │     └─ reject if the code is a ground surface (road / parking / bare
    │        soil) but nDSM > per-code ceiling (= tree canopy above it)
    │     └─ reject if the code says ground surface but NDVI > 0.25
    │        (= vegetation, label stale)
    ├─ 2. OSM ground-level types (roads, paths, water, landcover)
    │     └─ reject if h_mean is above the per-type ceiling
    └─ 3. no confident match → unlabelled, excluded from training
```

Key functions: `rasterize_*_labels()` (polygons → 1 m label raster,
height-masked), `match_segments_via_raster()` (majority pixel vote per
segment), `match_segment_to_osm()`.

Constants at the top of the file: `_GROUND_SURFACE_CODES`, per-code
nDSM ceilings, `_NDVI_MAX_*` (0.25), `_OSM_HEIGHT_CEILINGS`. Retuning
them changes training-set quality directly — bump conservatively and
retrain.

## Circuit breaker

openEO 503s / timeouts are common. `_read_circuit_breaker()` /
`_write_circuit_breaker()` share a file-backed cooldown across
subprocesses; while tripped, NDVI/SAR are skipped instead of burning
3 × 180 s timeouts per KG.

## Model artefact & feature contract

`/tmp/learned_classifier/model.joblib` is saved every 10 KGs and loaded
by `learned_classifier.py` on the next request. Feature order MUST match
`learned_classifier.FEATURE_KEYS` and
`object_segmentation.extract_object_features()`. Changing the feature
count silently breaks inference — retrain after any feature change
(`docs/cross-cutting-concerns.md`).

## Operations

```bash
sudo systemctl status rf_train
tail -f /tmp/rf_train_4000kg.log
curl -s localhost:8000/api/v1/training/status | python3 -m json.tool

# fresh training run on existing checkpoints
rm -f /tmp/learned_classifier/model.joblib && sudo systemctl restart rf_train

# inspect a checkpoint
python3 -c "
import numpy as np
d = np.load('rf_training_data/checkpoints/kg_91109.npz', allow_pickle=False)
print(d.files); print('X', d['X'].shape, 'y', d['y'].shape)"
```

## When to retrain

* added / renamed an object type (label space changed);
* added / removed an RF feature (feature dimension changed);
* tuned the ground-truth filters (label distribution changed);
* upstream label data changed significantly.

Don't retrain casually — 4 000 KGs take days even with cached rasters.
