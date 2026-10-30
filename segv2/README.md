# segv2 — segmentation / classification model v2 (LightGBM)

`segv2/` holds the training harness and the inference wrapper of the v2
object classifier. At runtime the processor uses v2 when
`segv2.model_v2.available()` is true (lightgbm importable and
`data/segv2/models/model_G.joblib` present — fetched by
`python3 tools/fetch_models.py`; the metadata
`data/segv2/models/model_G.meta.json` is shipped in git); otherwise the
v1 random forest (`data/best_model/`, `docs/rf-training.md`) is used.
`MODEL_VERSION` is recorded in every product (`methods.classifier`).

## Shipped model `G`

LightGBM over 117 object features (`feature_keys` in the meta — the order
is authoritative, never the caller's), 14 classes (`crop garden glacier
grass orchard parking rail road rock roof shrub tree vineyard water`),
merges `excavation`/`fill → earthwork`, `hedge → shrub`,
`bare_soil → rock`; `class_weight = sqrt`; 1.23 M training objects from
~4 000 KGs; grouped-by-KG spatial CV macro-F1 0.78. NaN stays "missing"
for LightGBM (`nan_to_zero: false`). Train-time NDVI vetoes
(`min_ndvi_seg` / `max_ndvi_seg`) are applied at inference as class masks
on the probability matrix (`ModelV2.predict`); rows without real NIR are
exempt. Rule-only types (`wind_turbine`, `substation`, `solar_panel`,
`mast`, `fence`, `wall`, disturbance classes) stay rule-based in
`object_segmentation.py`.

## Why v2 over the v1 RF

1. **Better labels.** v1 learnt from a coarse land-use table. v2 labels
   come from a priority stack of open sources (`segv2/labels.py`): OSM
   road / rail centrelines buffered by class, OSM water areas and lines,
   INVEKOS field polygons with crop type (AMA, CC BY-AT 4.0 — optional,
   `config.INVEKOS_API_URL`), and building footprints for `roof` (needs
   nDSM ≥ 2 m). In the public release `LabelContext` paints **no**
   land-use layer: the OSM geometry comes from the optional OSM geometry
   service (`/api/v1/osm/geometry`, see `.env.example`) or Overpass, the
   INVEKOS polygons from `config.INVEKOS_API_URL`; both optional — with
   neither configured, only footprint / raster-derived labels remain and
   the fitted `model_G` is what you use.
2. **Flight dates vary.** DSM mosaics are stitched from flights of
   different years; v2 features carry the effective DSM year gap,
   per-year-normalised height change and the ortho ↔ LiDAR year gap
   (`segv2/acquisition.py`, `als_acquisition.py`).
3. **Training data from our own products.** Features are extracted from
   the full GPKGs on Zenodo (all 1 m rasters, multi-date DTM/DSM, ortho
   RGBI, NDVI, WorldCover, SAR, Hansen) plus the tile cache for
   harmonics — no BEV / openEO traffic for training. Every GPKG pulled is
   audited (md5 vs manifest, `integrity_check`, layer inventory) into
   `data/segv2/audit.jsonl`.

## Pipeline

```
segv2/gpkg_fetch.py      Zenodo full GPKG → /tmp (parallel range requests), audit
segv2/gpkg_raster.py     read GPKG raster layers as arrays
segv2/labels.py          open-source label stack → 1 m label raster
segv2/features.py        GPKG rasters → tiles → object_segmentation(features_only)
                         + v2 extras (OSM distances, neighbour context, ALS dates)
segv2/build_dataset.py   per-KG parquet: features, label, label_source, purity, xy
segv2/train.py           harness: v1 baseline / RF-relabelled / LightGBM(+context);
                         grouped-by-KG spatial CV, macro-F1, per-class F1, ECE
segv2/model_v2.py        ModelV2.load() / predict() / report()  (runtime wrapper)
segv2/inference.py       batch inference over a dataset
segv2/seg_live.py        run the v2 classifier on a live AOI for QA
segv2/retry_failed.sh    re-run failed per-KG dataset builds
```

```bash
python3 -m segv2.build_dataset --kgs 63330 61047        # needs the full GPKGs reachable
python3 -m segv2.train --model G                        # writes data/segv2/models/model_<X>.joblib + meta
python3 -c "from segv2.model_v2 import ModelV2; ModelV2.load().report()"   # data/segv2/report_model_v2.md
```
<!-- VERIFY: CLI flags of build_dataset / train after the label-source cut -->

## Label taxonomy

The 25 v1 types plus `rail`, `wetland`, `glacier` (`docs/cross-cutting-concerns.md`
lists the colours; keep `object_segmentation`, `austria_processor` and
`static/index.html` in sync).

## Promotion criteria (for a new model)

Grouped-KG CV on the same relabelled test folds: a candidate must beat
the shipped model by ≥ +0.05 macro-F1 **and** not lose > 0.02 F1 on any
of tree / roof / grass / crop / water / road, with calibration (ECE) not
worse; then side-by-side visual QA on 5 KGs and a product-version bump
(`docs/v2.1-product-spec.md`). Performance rule: every per-tile step
masks to the KG footprint first; tiles with < 2 % KG coverage are
skipped.
