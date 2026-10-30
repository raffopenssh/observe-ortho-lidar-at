# Tree inventory v3 — product-backed `/api/v3/trees*`

Design notes for the v3 tree service (`trees_v3.py`, `tree_inventory.py`;
contract in `/api/v1/docs/llm.txt` → "v3 tree service"; product columns
in `docs/v2.1-product-spec.md`).

## Why a v3

`/api/v2/trees` re-detects every apex from live BEV rasters on every call
(tens of seconds per km², BEV egress, Copernicus credits for ortho/NIR).
Since product 2.1 the Austria-wide processor runs exactly that detector
once per KG and ships the result:

* `tree_apices` point layer in every `_light_v2.gpkg` — `ndsm_only`
  seeding on the stitched 1 m nDSM; per apex `h_m, crown_r_m,
  crown_area_m2, surface_class, tree_likelihood, stand_context,
  segment_type(+conf), dh_per_year_m, als_year, ndvi, leaf_type_hint
  (+conf), dbh_est_cm, volume_m3_est`, plus (2.2+) per-crown spectra
  `ndvi_mean, ndvi_p10, nir_mean, brightness_mean, green_ratio_mean`,
  `vitality` (+conf), `species_hint` (+conf);
* `tree_crowns` polygon layer (2.2+): watershed crown outline per apex
  (EPSG:3035, simplified 0.5 m, `tree_id` only);
* `landcover.grid25` in the KG JSON — dominant segment class per 25 m
  cell plus `canopy_frac`, `tree_frac`, `woody_frac` (2.2+).

v3 reads those instead of recomputing. Same tree ids, same allometry,
same surface engine — a v3 answer over a product KG is the frozen
`ndsm_only` answer.

| endpoint | in | out |
|---|---|---|
| `POST /api/v3/trees` | AOI (any accepted geometry format) | apex Features + explicit-denominator summary |
| `POST /api/v3/trees/by-polygons` | FeatureCollection of stands (`key_property`) | per-stand summaries (+`include_trees`) |
| `POST /api/v3/changes/trees` | AOI, `date_b` | product apices (epoch a) matched against a live nDSM (epoch b) |

## Stable tree ids — the grid anchor

All 1 m grids (live v2, live v3 fallback, fresh products, the v2-upgrade
shim `v2_source._Gpkg`) are the integer-metre EPSG:3035 BEV grid:
`raster_io.read_window_bbox` snaps the bbox outwards to whole metres, an
apex is the pixel centre (`x.5`), and `tree_id = t_<E_dm>_<N_dm>` of that
centre — AOI-independent. Named in `meta.grid_anchor` (`apex_anchor:
pixel_center`, origin, id rule). Pre-2.2 products are re-anchored on read
(`trees_v3.canonicalize_rows`: the per-product δ is the median fractional
offset); `tree_id_product` returns the id stored in the file.
`legacy_ids=1` adds `tree_id_v2` (`tree_inventory.legacy_tree_id`) for
ids captured from older live calls over the same AOI. Products built by
the v2-upgrade path (from v1 full GPKGs) can carry ≤ 1 px tile
misregistration inside the mosaic. Test: `tests/test_grid_anchor.py`.

## Summary fields

* `stems_per_ha_forest` — stems whose apex sits on a `tree` segment,
  over the `tree`-class grid25 area (`area_ha_forest`);
  `stems_per_ha_canopy` keeps the wider woody-stand denominator
  (tree / orchard / vineyard / hedge / garden / shrub);
  `stems_per_ha_total` the AOI.
* `by_stand_context` — forest vs orchard vs hedge vs garden trees;
  filter server-side with `stand_context=tree`.
* `dh_per_year_m_median` / per-tree `dh_per_year_m` — apex height change
  between the oldest and newest BEV flight over the KG, normalised by the
  real flight-year gap (null for a single flight year); `als_year` per
  tree is the flight-block year, not the mosaic date.
* `by_source` / `meta.source_mix` — product vs live-fallback trees.
* `meta.acquisition` — flight blocks per product (`als_year_min/max`,
  `years_gap_median`, `flight_dates`, `n_tiles`).

## Coverage and honesty rules

* A product's footprint is its classified grid25 cells (≈ the KG
  polygon). Apices are read inside footprint ∩ AOI; where two products
  overlap the same `tree_id` is counted once.
* The rest of the AOI (KGs without a product) runs the live v2 detector
  once (`fallback_live=true`, default), `ndsm_only` so both halves use
  the same seeding; live apices within 1.5 m of a product apex are
  dropped. `fallback_live=false` gives product-only answers and reports
  `meta.coverage.live_area_ha`.
* `detection_mode=fused` runs the live fused (ortho + nDSM) detector over
  the whole AOI, joins product rows by `tree_id` and carries the product
  enrichment onto matched trees (`source=fused_product`); ortho-only
  stems come back as `live_fused`.
* **Canopy area** (2.2+): `CanopyGrid.canopy` = Σ (cell ∩ AOI area) ×
  `canopy_frac` — no centre rule, no dominance rule, no rim loss.
  `area_ha_canopy` ← `canopy_frac`, `area_ha_forest` ← `tree_frac`,
  `area_ha_stand` ← `woody_frac`; `summary.canopy_method` names the
  method; `canopy_denominator_note` states what was counted.
* **Vitality**: `dead` is absolute (NDVI < 0.15, p10 < 0.10, h ≥ 5 m);
  `stressed` / `vital` and `ndvi_percentile_in_aoi` are re-ranked over
  the submitted AOI on every call (`rank_vitality_in_aoi`); the anomaly
  rule is bottom decile AND below the population median.
* **Edges**: `trees_v3.mark_edge` — crown disc crosses the AOI boundary →
  `is_edge` (product and live alike; per stand in `by-polygons`).
  Densities use non-edge stems; `include_edge=false` drops them.
* `crown_geometry=polygon` returns `tree_crowns` outlines where present,
  else the apex point; `meta.crown_polygons` counts both.
* `meta.product_versions` / `meta.product_codes_failed_reasons` per code.

## Changes against the frozen inventory

`/api/v3/changes/trees`: epoch a is the product (no re-detection, no
drift), epoch b is live at `date_b`. Epoch a is a crown-footprint label
raster (`tree_crowns` polygons, or mean-radius discs for 2.1), so
`match_trees` runs both passes (apex, then crown overlap).
`felling_patches` are crown-level: a crown is felled when
`h_m − p90(live nDSM inside its footprint) ≥ felling_min_drop_m`;
adjacent felled footprints merge into patches with `area_sqm`,
`drop_mean_m`, `drop_max_m`, `height_before_mean_m`, `n_crowns`. Growth
rates are only emitted when `date_b`'s flight year is later than the
product `als_year`; `epoch_dates.same_flight_epoch` flags republished
data. `GET /api/v3/*` → 405 with a hint.
