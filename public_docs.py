"""public_docs — llm.txt + landing page + licence block for public_api.py.

All licence / attribution wording is pulled from ``attributions.py`` (single
source of truth) — never hand-edit credits here.
"""
from __future__ import annotations

from kg_universe import KG_TOTAL as _KG_TOTAL
import datetime as _dt

import attributions as _attr
import config as _config

# Public base URL of this deployment (SRTM_PUBLIC_URL, else the local public tier).
BASE = _config.env('SRTM_PUBLIC_URL') or f'http://127.0.0.1:{_config.PUBLIC_PORT}'
# Optional sibling service that serves the *statistical* (declared) land-use
# layer per H3 cell — same cell ids (SRTM_NE_STATS_URL).  Empty => the
# paragraphs are omitted.
NE_STATS_URL = _config.env('SRTM_NE_STATS_URL')
NE_STATS_NOTE = (f" The statistical counterpart (same cell ids) is served at {NE_STATS_URL}/api/v1/ne."
                 if NE_STATS_URL else '')
NE_STATS_HTML = (f' The statistical counterpart (same cell ids): <code>GET {NE_STATS_URL}/api/v1/ne</code>.'
                 if NE_STATS_URL else '')
# One-line credit used in headers, meta.attribution, llm.txt and the landing footer.
ATTRIBUTION_SHORT = _attr.attribution_short()
ALPHA_NOTE = 'alpha — field names and limits may still move between epochs'


TREES_META = {
    'note': 'Sampled tree apices from the LiDAR nDSM: the tallest few per ~0.1 ha, index keeps '
            'h ≥ 20 m nationwide (so min_height < 20 returns no extra trees). QA-filtered here: '
            'mast-like / implausible / low-confidence apices removed — this is the realistic set.',
    'complete': False,
}

CELLS_TXT = """## Cells (H3 res 12, ~300 m² hexagons) — what we measured, per cell       #cells

Every processed KG is also published as a grid of H3 res-12 cells (~307 m², ~52 000 per KG) with
one 41-byte observation record each, plus every tree apex (h ≥ 3 m) and every segmented structure
with LiDAR heights. Cell ids and query shapes follow the shared NE cells contract (ne-cells-2), so a
client can zip this observed layer with a statistical land-use layer by cell id.{{NE_STATS_NOTE}}
Served from pre-built chunk files on this host (1–80 ms). Not on
disk yet → 404 {ready:false, status:not_processed|pending|partial, retry_after_s, kgs_missing,
kgs_pending, kgs_not_processed} + Retry-After.
A bbox that is only partly covered answers 200 with meta.partial:true, meta.cells_missing,
meta.kgs_missing (= kgs_pending ∪ kgs_not_processed). kgs_pending = the KG list already
lists a v2.4 product but its cells file is not published on this host yet (ingest lag,
hours) → poll after meta.retry_after_s; kgs_not_processed = no cells product generated
(permanent until the KG is processed). Field layout, units, code tables, limits: GET /api/v1/cells/dict.

Publication invariant (since 2026-10-07): a KG-list row (/api/v1/kgs, /api/v1/kg/{{code}},
/llm/manifest.json) shows product_version "v2.4" ONLY once /api/v1/cells serves that KG
completely. Until then the row shows product_version "v2.4-pending" + retry_after_s
(product_version_registry carries the raw KG-list value). Every row also carries
ne_ready (bool), ne_status (served|partial|pending|not_processed) and
ne_cells_published_at (UTC, when the cells went live here). updated_at is bumped to
ne_cells_published_at when cells go live, so the /kgs ETag (and /status
cells_generation / manifest_generation) changes on cells publication, not only on KG
generation — revalidate on ETag and re-baseline from manifest_generation.

  GET  /api/v1/cells?bbox=w,s,e,n                 cells in a box (span ≤ 0.05°, ≤ 16 384 cells, else 413)
  GET  /api/v1/cells?lon=&lat=[&k=0..3]           the cell under a point + k rings
  GET  /api/v1/cells?ids=h3,h3,…                  ≤ 512 ids (res 12; res 8–11 ids expand to their res-12 children)
  POST /api/v1/cells {"ids":[…]}                  ≤ 16 384 ids
  GET  /api/v1/cells/<h3>                         one res-12 cell, all layers (obs + trees + structures), JSON
  GET  /api/v1/cells/chunk/<h3>                   one res-10 (or res-9 = 7 chunks) chunk for bulk clients; format=h3b|bin|json
  GET  /api/v1/cells/dict                         fields, units, code tables, group labels, limits, epoch, bins
  GET  /api/v1/cells/bins                         every 0.025° bin with a published product + its generation (see format=bin)
  GET  /api/v1/cells/national?res=9[&fields=…]    Austria-wide NEX1 table, one row per res-9 (or res-8) H3 parent (see below)
  GET  /api/v1/cells/blocks[?fields=…&epoch=…]    manifest of the prebuilt id-free zstd 0.1° blocks (bi, bj, gen, etag, bytes)
  GET  /api/v1/cells/sample?res=10&format=bin     national sample: one row per res-10 cell = its res-12 centre child, every cell with DTM
  GET  /api/v1/kg/<code>/cells                    whole-KG container: format=h3b (default, downloadable) | json (summary)
  GET  /api/v1/kgs/summary?codes=a,b,…             merged observed summary over a KG SET (≤ 500 codes, else 400);
       …?gemeinde=61630 | district=616 | state=6    or by Statistik Austria GKZ prefix (5 / 3 / 1 digits).
                                                  Wien Gemeindebezirke (901–923 / 9xx01) → 400 {{error:"unsupported_filter"}},
                                                  fall back to codes= (Wien as a whole: district=900, gemeinde=90001).
                                                  → {{scope{{type,code|n}}, kgs, kgs_processed, incomplete,
                                                  kgs_pending[_n], kgs_not_processed[_n], kgs_partial[_n], kgs_unknown?,
                                                  summary{{…same keys as /kg/<code>/cells?format=json summary…}}, meta}}.
                                                  summary: cover_ha / consistency_n / consistency_ha / cells_observed /
                                                  trees_n / structures_n / terrain.cells_with_terrain / hansen_loss_cells
                                                  are SUMS over the processed KGs; terrain.elev_min_m / elev_max_m =
                                                  min / max; terrain.slope_mean_deg = cell-weighted mean; epoch = newest
                                                  (+ epochs[]). KGs without published cells are skipped and counted in
                                                  kgs − kgs_processed; incomplete:true when any KG is missing or partial.
                                                  Header-only read (no raster), ETag + max-age=600, CORS *.
  GET  /api/v1/landscape?…&include=cells          adds a per-bbox roll-up block (cover, heights, canopy, change, consistency)
  GET  /api/v1/status                             cells{kgs_ready, cells_total, index_rebuilt_at, epoch}
                                                  als{kgs_with_als_year, year_hist, newest_min/max, oldest_min}

  ALS acquisition year (real BEV Flugjahr, not the mosaic date): als_years in the row is a bitmask
    (bit i = 2020+i) and is 0 for everything last flown ≤ 2019. The read-time fields als_year_newest /
    als_year_oldest (u16, 65535 = null; json/columns/csv, not bin/h3b) carry the flight year of the
    20240915 mosaic block (= the year every observed value was flown) and of the 20220915 block
    (= the "oldest" side of dh_m / dtm_m; equal years ⇒ dh_m is noise). Roll-up: max / min.
    Per KG: /api/v1/kg/<code> → als{{years, newest, oldest, dominant, coverage (share of cells under
    newest), reflown_share, source "data.gv.at 5ce253fc-…", mosaics}}. Source: BEV flight-block
    polygons ("Aktualität DGM - ALS", Flugjahr per block, DSM year), evaluated at the cell centre.

  params   layers=obs,trees,structures (default obs; trees/structures only at res 12; /cells/<h3> always all)
           res=12|11|10 (server-side roll-up: means for cover / ndvi / terrain / height, max for h_max, sums
           for counts, mode + consistency_hist for categories, union for als_years; returns n_children)
           fields=a,b,c (subset of the obs field names from /dict)   centres=1 (adds lon/lat of the cell centre)
           format=json|columns|bin|csv|geojson (geojson ≤ 4 096 cells; h3b only on /chunk and /kg/<code>/cells)
           pretty=1
  headers  ETag (changes when a product is re-built), If-None-Match → 304, Cache-Control max-age 86400,
           X-Cells-Source: disk, X-Cells-Partial: 1, X-Cells-Epoch, X-Cells-Kgs, CORS *, gzip ≥ 1 KiB.
  rate     ≤ 5 r/s sustained, burst 20.
  borders  a cell present in several KG products is served once (observed copy first, then the larger
           chunk, then the lowest kg); meta.kgs lists the products touched. trees.n saturates at 255
           per cell (meta.trees_truncated_cells counts those). /api/v1/kg/<code> carries links.cells.

  One cell (format=json, res 12):
  {{"cell": "8c1e108cc922dff", "res": 12, "kg": "63330",
    "cover": {{"geb": 0.02, "bau": 0, "acker": 0, "gruen": 0.31, "wald": 0.61, "wasser": 0, "verkehr": 0.04, "alpen": 0, "sonst": 0.02}},
    "structures_cover": 0.02, "structures_n": 1,
    "height": {{"mean": 9.5, "p90": 21.0, "max": 27.5}},
    "trees": {{"n": 14, "h_max": 27.5, "canopy": 0.58}},            (layer trees: trees = [{{dx, dy, lon, lat, h_m, crown_m2, species, vitality, ndsm_only}}, …])
    "structures": [{{dx, dy, lon, lat, type, h_max_m, h_robust_m, area_m2, conf, dh_m}}, …]   (layer structures)
    "ndvi": {{"mean": 0.71, "amp": 0.22}}, "phenology": "forest",
    "change": {{"dh_m": -0.25, "dtm_m": 0.0, "forest_loss_year": null, "als_years": [2022, 2024],
               "als_year_newest": 2022, "als_year_oldest": 2010}},
    "terrain": {{"elev_m": 612, "slope": 14, "slope_p90": 23, "aspect": "SE", "roughness": 1.3, "tri": 0.8, "tpi": -0.5}},
    "consistency": "consistent"}}
  cover groups (shared NE cells codes): geb=buildings, bau=construction & parking, acker=cropland,
    gruen=grass/garden/orchard/vineyard/shrub/hedge, wald=trees, wasser=water, verkehr=roads & paths,
    alpen=rock/bare/glacier, sonst=other.  consistency = our observation vs. the statistical land-use layer
    for the same cell (frozen epoch), computed when the product was built:
    consistent | forest_loss | forest_gain | sealed_new | structure_new | green_new | unknown.
  format=columns  {{cells:[hex], kg:[…], cover:[[9 × u8]], h_mean:[u8], …, units, codes}} — raw quantised
    integers at res 12 (~20 B/cell gzipped), the format for map heat layers; res 11/10 → aggregated floats.
  format=bin      cells_n × 41 B NEO1 rows (little-endian, sorted by cell id), X-Cells-Record: neo1/41,
    X-Cells-Layout: neo1=N[,net1=T][,neb1=B] with trees (u32) / structures (u64) rows appended.
  format=bin + fields= on a 0.025°-aligned bbox (w=i·0.025, s=j·0.025, e=w+0.025, n=s+0.025, half-open)
    → the columnar NEX1 BIN: every local product joined per cell — observation rows (all fields),
    tree apices → canopy_h_p90/p50, man-made objects → bld_n / bld_h_max / bld_h_p50 / bld_area_sum /
    stories_max / struct_h_max, product roof hints → roof_dom (0 unknown 1 flat 2 pitched), 25 m DTM /
    landcover grid → elev_mean / slope_mean / slope_p90 / aspect_deg (u8 ×2°, 255 flat, 254 unknown) for
    EVERY cell under a product raster (terrain_src 1 = row, 2 = 25 m grid), lc25 / canopy25 / woody25.
    Container: 'NEX1' | u32 header_len | header JSON {{n, gen, epoch, kgs[], fields[], columns[{{name,
    dtype, bytes, shape?, nodata?}}], ids: delta_u64|u64, complete, lc25_legend}} | column bytes in header
    order, rows sorted by cell id (ids delta-coded: first absolute, then differences; ids=u64 for raw).
    u8 half-metre columns: 0 = none in this observed cell, 1..254 ×0.5 m, 255 = cell never observed.
    fields=* for everything, ~17 k cells per bin, ≤ ~60 KB gz for the 9-field terrain+building set.
    Headers: ETag "b-<gen>-<fields>" (strong; If-None-Match → 304 without touching the blob),
    X-Cells-Gen = the bin generation — KEY YOUR COPY ON THIS (changes iff a contributing product changed;
    X-Cells-Epoch is the product epoch, informational only), X-Cells-Partial 1 when header.kgs_missing.
    Discovery: GET /api/v1/cells/bins → columns {{i, j, gen, epoch, complete, kgs_n, stored}} for every
    bin touching a published product (ETag "bins-<generation>"); diff gen against your copy, pull only
    changed bins, ≤ 4 in flight. Field table + semantics: /api/v1/cells/dict → bins.
  block=1 on a 0.1°-aligned bbox (w=bi·0.1, s=bj·0.1) → NEXB v2 (since 2026-11-03): the 16 bins in one body WITHOUT
    ids — the cell set of each bin is the shared enumeration "all res-12 cells whose centre lies in the half-open
    bin [w,w+0.025)×[s,s+0.025), ascending H3" (h3 polygonToCells centre rule); header.bins[k] = {{i, j, n, row0,
    ids_xor (xor of the u64 ids, hex), gen, complete}}, body = present bitmap (ceil(n/8) B, bit r = byte r>>3 bit
    r&7) | planar columns (n rows each, rows = the 16 bin enumerations concatenated in row-major bin order).
    Check n / ids_xor against your enumeration; on mismatch re-request with ids=1 (NEXB v1 frames with ids, unchanged).
    Prebuilt on disk and served zstd (send Accept-Encoding: zstd; else inflated + gzip): ≈ 0.7–1.1 MB per block
    for the 9 glyph fields, < 100 ms warm. ETag "B-<gen>-<hash>", X-Cells-Gen, X-Cells-Epoch, X-Cells-Ids: bitmap.
    Manifest: GET /api/v1/cells/blocks?fields=… → columns {{bi, bj, gen, etag, bytes, stored, current, n, bins_n,
    complete}} (bytes null until prebuilt for that field set); epoch= must equal X-Cells-Epoch (else 409).
  /api/v1/cells/sample?res=10&format=bin[&fields=glyph|default|*|a,b,…]  → the NATIONAL SAMPLE for the glyph
    filter: one NEX1 row per res-10 cell of Austria = its res-12 centre child (cellToCenterChild(parent,12);
    parent = cellToParent(id,10)), ascending H3, every cell with DTM (terrain_src != 0), ≈ 5.6 M rows, ids
    delta-coded then planar columns with the bin dtypes / units / nodata (255 = unknown). fields=glyph =
    bld_h_max bld_h_p50 stories_max roof_dom canopy_h_p90 aspect_deg slope_mean slope_p90 elev_mean (≈ 26 MB zstd);
    default adds obs terrain_src bld_n bld_area_sum struct_h_max canopy_h_p50 lc25 tree_n dh_m consistency
    forest_loss_year phenology als_year_newest (≈ +12 MB); * = every bin column except cover. Rebuilt by the hourly
    builder only when the bin corpus changed. Headers: ETag "S-r10-<gen>-<fields>" (strong; If-None-Match → 304),
    X-Cells-Gen (key your copy on it), X-Cells-Epoch (the ne-observe product epoch), X-Cells-Built, Content-Encoding
    zstd when accepted. format=json / meta=1 → build meta. 404 + Retry-After before the first build. One pull per
    generation from one client — never per tile.
  /api/v1/cells/national?res=9|8[&fields=a,b,…]  → the NATIONAL aggregate for overview zooms: one NEX1
    row per res-9 parent (~1.05 M rows, ~20 MB gz; res 8 ≈ 150 k rows), Austria-wide, built daily from every
    published cells product. Same container / dtypes / units as the bins (header "res": 9, no ij / bbox); per
    field the MEAN over the res-12 children where the field is non-null (bld_* / canopy_h_* over object-holding
    children; bld_h_top / canopy_h_top = max), verdict = u16 counts per consistency class (header.verdicts),
    n_cells / n_obs = children with a row / observed. Border cells deduplicated across products. Fields:
    n_cells n_obs verdict elev_mean slope_mean slope_p90 roughness tri tpi h_mean h_p90 h_max tree_h_max canopy
    ndvi_mean ndvi_amp dh_m dtm_m structures_cover cover[9] tree_n structures_n forest_loss_n bld_n bld_cells
    bld_h_max bld_h_p50 bld_h_top bld_area_sum struct_h_max canopy_h_p90 canopy_h_p50 canopy_h_top (u32 = count/
    area sums). No fields= → the whole table (pre-gzipped); fields= → a cached slice. Headers: ETag
    "n-r<res>-<gen>-<fields>", X-Cells-Gen (key your copy on it; rebuilt only when the corpus changed),
    X-Cells-Epoch, X-Cells-Built, Cache-Control max-age 86400. format=json (or meta=1) → build meta {gen, epoch,
    built_at, rows, bytes_gz}. 404 + Retry-After until the first build. Fetch once per generation, never per tile.
    DAILY UPDATE LOGIC: the table is rebuilt once a day at 10:00 Europe/Vienna (~15 min), and only when the
    cells corpus changed since the last build (gen = hash of the product set); unchanged corpus ⇒ same gen,
    same ETag ⇒ 304. New observations keep arriving while the fleet processes KGs, so expect a new gen most
    days until the run completes, then rarely. Client schedule that is FINE: once a day after ~11:00 CET
    GET ?format=json (≈1 KB), compare gen with your copy, and only on change GET the table with your
    fields= list (~20 MB gz for 5 fields at res 9; always send If-None-Match + Accept-Encoding: gzip).
    NOT fine: polling the table itself (no fields= ⇒ all 33 fields ≈ 65 MB gz), fetching more than once per
    gen, many distinct fields= combinations (each one is a server-side slice cached per gen; pick one list
    and keep it), or deriving the overview from per-bin / per-bbox cells calls instead of this table.
  format=h3b      H3B1 container: magic | ver u8 | hdr_len u32 | header JSON | zlib chunks of
    shuffle(NEO1)|shuffle(NET1)|shuffle(NEB1) per res-10 cell (header.chunks[] has off/len/cells/trees/bld).
    The whole-KG file is the public product download (CC BY 4.0, attribution in the header).

  Recipe (map client): per viewport tile (~0.02°) one /api/v1/cells?bbox&format=columns call for a
  heat layer (canopy, structure heights, change, consistency); on click /api/v1/cells/<h3> for the full
  dossier incl. every tree and structure; lon/lat&k=1 for "around this point". Join to your own
  geometry client-side by cell id (h3.latlng_to_cell(lat, lon, 12)).

"""

STATS_TXT = """## Stats envelope — one flat JSON per spatial unit          #stats
  GET /api/v1/stats/unit?h3=<id> | kg=<code>[,<code>…≤50] | bbox=w,s,e,n   [&format=json|flat|csv] [&keys=a,b]
  Schema "stats-envelope/1": the same envelope a statistical NE service (land-use + diff) and
  farm-subsidies-austria emit, so a spreadsheet or an agent can zip the three by unit key without walking cells.
  unit:   h3   res 5–12; the unit = every res-12 cell that is a descendant of the id (exact set, not bbox)
          kg   5-digit code; several codes → one envelope per code in "units":[…] (unknown / unprocessed → 200,
               coverage.complete=false, empty figures — never 404, spreadsheet rows stay intact)
          bbox WGS84, span ≤ 0.1°; cells whose centre lies inside
  body:   {{schema, service:"srtm-lidar-at", unit{{kind, key, bbox, res:12, cells_n, area_ha, kgs[]}}, source
          ("srtm-lidar-at@2.4"), epoch ("2026-10"), coverage{{complete, cells_known, cells_total, note?}},
          figures{{…flat…}}, attribution, license:"CC-BY-4.0", computed_at, age_s}}
  figures (flat, dotted, scalar; unit in the suffix _ha _n _m _deg _pct _year):
          cells_n, area_ha, cells_observed_n, trees_n, structures_n, structures.cover_pct,
          cover_ha.<geb|bau|acker|gruen|wald|wasser|verkehr|alpen|sonst>
          consistency_ha.<consistent|forest_loss|forest_gain|sealed_new|structure_new|green_new|unknown>,
          consistency_n.<same>, terrain.elev_min_m / elev_max_m / elev_mean_m / slope_mean_deg,
          height.mean_m / p90_mean_m / max_m, canopy.cover_pct, ndvi.mean, change.dh_mean_m, hansen_loss_ha,
          als.newest_year / oldest_year / coverage_pct   (BEV flight year, chunk precision ≈ 300 m)
          kg units: area_ha / cover_ha / consistency_ha are the product-header figures (border cells weighted
          by the KG's share, identical to /kgs/summary); counts and means are over every cell of the product.
          h3 / bbox units: whole cells, de-duplicated across KG products, ha = cells × cell area.
  format=flat → figures only (array of flat objects with "unit" for several codes);
  format=csv  → header row + one row per unit (text/csv UTF-8, no BOM; first columns unit, coverage_complete)
          → Google Sheets =IMPORTDATA("{BASE}/api/v1/stats/unit?kg=63330&format=csv"), Excel Power Query.
  keys=cover_ha,als → only those figure keys (prefix match).
  Ops: ETag + Cache-Control max-age=600, CORS *, gzip; warm answers < 10 ms, first touch of a KG / res-7 cell
  ~0.1–0.5 s; a cold res-5 cell (823 k cells) may answer 202 {{status:"computing", retry_after_s}} once —
  retry after Retry-After, the result is then cached. Consistency counts apply the read-time green-ground
  override (see cells), so consistency_n can differ by a few cells from the frozen product header.

"""

FLAGS_TXT = """## Flags — tell us when something we serve looks wrong                       #flags

Tokenless, for every public layer: an NE cell whose observed cover group looks off (a glyph under
v=obs), a giant tree that is a mast, a structure height that cannot be, a tile artefact, a stale KG. Flags are
QA evidence, not edits: nothing you send changes what we serve; they drive re-checks and re-processing
(`superseded` tells you the product was rebuilt since). Subjects are H3 res-12 cells or KG codes only —
nothing object-, lot- or identifier-keyed exists here. Please flag with a point or a cell, say what you saw and
what you expect, and send `looks_right` too: a confirmation is as useful as a complaint.

  POST /api/v1/flags                              one report, or {{"reports":[…]}} ≤ 50 → 201
    {{"cell": "8c1e108cc922dff"                      subject: res-12 id (res 8–11 snap to their centre cell)
     | "lon": 15.0857, "lat": 47.0665               … or a point (→ its res-12 cell)   | "kg": "63349" (whole KG)
     "layer": "cells.cover",                        cells.cover | cells.height | cells.trees | cells.structures |
                                                    cells.change | cells.consistency | cells.terrain | cells.ndvi |
                                                    trees | landmarks | buildings | landscape | heightfield | tiles |
                                                    kg | docs | other
     "claim": "wrong_type",                         wrong_type | wrong_height | too_tall | too_short | missing |
                                                    spurious | misplaced | stale | artifact | looks_right | other
     "observed": {{"group": "wald", "share": 0.61}},  what you saw from us (≤ 512 B). OMIT IT and we snapshot the
                                                    cell as served right now: {{group, share, h_max, trees_n,
                                                    structures_n, consistency[, nearest{{h_m|h_max_m, type, dist_m}}]}}
                                                    (nearest apex / structure for the *trees* / *structures* layers)
     "expected": {{"group": "gruen"}},                what it should be (≤ 256 B): {{group}}, {{type}}, {{h_m}}, {{absent:true}} …
     "note": "mown meadow, no trees",               ≤ 280 chars, shown publicly (no personal data please)
     "source": "my-client/glyph",                   your client label ≤ 40 (stats are grouped by it)
     "reporter": "opaque-id",                       optional; hashed with a server salt, never stored or returned
     "epoch": "…"}}                                  optional: meta.epoch / X-Cells-Epoch you read (else ours)
    → {{id, folded, n, status, layer, claim, subject{{cell, kg, lon, lat}}, epoch, served, snapshot?, tally{{n,
       reporters, open, claims{{…}}}}, warnings?}}. Same subject + layer + claim + reporter folds into ONE row
    (n += 1) — repeat freely, you never create duplicates. Unknown fields are ignored and named in warnings.
    Limits: body ≤ 32 KB; ≤ 1 r/s sustained (burst 10), ≤ 500 reports / client / day → 429 + Retry-After.

  GET  /api/v1/flags?bbox=w,s,e,n                  span ≤ 0.05° (viewport tile) — grouped: one row per subject
  GET  /api/v1/flags?cell=<h3>                     res 12 exact; res 6–11 = everything inside that hexagon
  GET  /api/v1/flags?ids=a,b,…                     ≤ 512 res-12 ids       GET /api/v1/flags?lon=&lat=[&k=0..3]
  GET  /api/v1/flags?kg=63349                      everything flagged in a KG (cells and the KG itself)
       filters layer=a,b  claim=a,b  status=open (default) | confirmed | rejected | fixed | stale | all
               since=<unix s | ISO>  limit≤2000  group=subject (default) | report (raw rows, newest first)
               notes=0 (drop notes)  format=json | columns ({{cells[], kg[], layer[], n[], open[], last[], top_claim[]}})
       subject row: {{cell, kg, lon, lat, layer, n, reporters, open, claims{{claim: n}}, expected{{claim: …}}, last,
                     epoch, superseded?}}        superseded = the KG product was rebuilt after the report
  GET  /api/v1/flags/<id>                          one report + tally   (status changes are ours: admin token)
  GET  /api/v1/flags/heat?res=7[&status=all][&kg=][&layer=]   national overview rolled up to H3 res 6–12:
       {{res, cells[hex], n[], open[], reporters[]}} — a few KB, 5-min cache: paint it on a map glyph
  GET  /api/v1/flags/stats                         totals, by layer / claim / source / status, 24h / 7d, top KGs
  GET  /api/v1/flags/dict                          the enums + limits above, machine-readable
  GET  /api/v1/cells/<h3>?flags=1                  the cell dossier with its flag tally inline (max-age 60)
  All reads: ETag + If-None-Match → 304, Cache-Control max-age 30 (heat/stats 300), gzip, CORS *, ≤ 5 r/s.

  Recipe (map glyph / cell card): on the card, one tap = POST {{cell, layer:"cells.cover", claim:"wrong_type",
  expected:{{group}}, source, reporter}} (we snapshot what we served); paint /flags/heat?res=7 as a "disputed"
  tint; per viewport tile GET /flags?bbox=&format=columns to mark disputed cells; show tally.claims on the card
  from /cells/<h3>?flags=1. Giant trees / heights (game-client pattern): POST {{lon, lat, layer:"trees",
  claim:"too_tall", observed:{{h_m}}}} straight from the tree symbol.

"""

LLM_TXT = f"""# srtm-lidar-at — what the LiDAR + satellite segmentation learned about any place in Austria (public API)
base: {BASE}      docs: /api/v1/docs/llm.txt (this file)      licence: /api/v1/license      stage: alpha
JSON, CORS *, gzip, ETag + Cache-Control. No key, no login. Please keep ≤ 5 req/s sustained per client.
alpha = field names and limits may still move between epochs; read meta.* and /api/v1/status rather than hard-coding.
@@COVERAGE@@

## What this is — and is not

We segment the whole of Austria (7 850 Katastralgemeinden, KGs — see §KG universe) from BEV airborne-laser
DTM/DSM 1 m, BEV orthophotos 0.2 m, Sentinel-2 NDVI/phenology, Sentinel-1 SAR, ESA WorldCover
and Hansen forest change into 25 object types (tree, shrub, grass, hedge, water, roof, road,
parking, crop, orchard, vineyard, garden, bare_soil, rock, glacier, mast, wind_turbine, …).
This API answers "what is the landscape like HERE?" for a bbox, a point or a KG code:
  · terrain: elevation, slope, aspect, terrain class (25 m DTM grid)
  · land cover: class shares, dominant type, natural / impervious / woody shares, canopy cover
  · trees: QA-filtered apex positions + heights (realistic giants only), tall man-made landmarks
  · built-up: LiDAR-measured structure heights / storeys / roof form at centroid points
  · forest loss by year (Hansen GFC), KG coverage + product readiness
  · cells: H3 res-12 (~300 m²) grid of everything above + every tree apex and structure (#cells)
  · hillshade / slope XYZ tiles for a map background
  · flags: tell us when any of the above looks wrong, per cell / point / KG (#flags)

Everything is keyed by bbox, point or KG code only — no property-register identifiers
of any kind, no vector outlines of lots or buildings. Clients that hold their own vector
geometry ask us about the bbox / representative point of each feature.

Everything is served from a pre-built index + 25 m grids on this host (5–150 ms). There is no
on-demand raster access: a KG that the fleet has not processed yet answers ready:false /
HTTP 202 with retry_after_s — repeat later, don't poll in a loop. Coverage: /api/v1/status.

## Keys and conventions

  bbox            west,south,east,north in WGS84 degrees (lon first). Also accepted:
                  west=&south=&east=&north= (aliases minlon/minlat/maxlon/maxlat) or
                  lon=&lat=[&radius_m=15] for a point. Max 0.03 deg² (~25 × 10 km) — tile bigger
                  viewports. Austria only (lon 9–17.6, lat 46–49.2).
  kg_code         5-digit Katastralgemeinde (KG) code ("63349") — the unit we process by.
  heights         metres above ground (nDSM) unless *_elev_* (metres a.s.l., DTM).
  fractions       0..1 shares of the bbox area; *_pixel_share are 1 m-pixel shares inside the
                  25 m cells (finer than the dominant-class shares).
  ready           false → nothing processed here yet (fleet is working through all KGs);
                  resolution: "25m" (product ≥ 2.1 grid) | "kg" (older product: KG averages).
  Every response: meta{{service, license, attribution, docs, duration_ms}} + headers
                  X-Data-Attribution and Link: <…>; rel="license". Keep them with the data.
  ?pretty=1       indents JSON. Errors: {{"error": "…"}} with 4xx/5xx.

## The one call: GET /api/v1/landscape   (alias /api/v1/context)

  GET /api/v1/landscape?bbox=15.084,47.064,15.088,47.068
  GET /api/v1/landscape?lon=15.0857&lat=47.0665&radius_m=20
  GET /api/v1/landscape?bbox=…&include=trees,landmarks&tree_limit=50

  → {{bbox, ready, resolution, fine_grid_complete, product_versions,
      kgs[{{kg_code, kg_name, gemeinde_code, gemeinde_name, processed, product_version,
            generated_at, updated_at, quality_grade, grid25}}],
      coverage{{kgs_total, kgs_processed, kgs_grid25, cells_25m, cells_valid, point_sampled}},
      terrain{{elevation_min_m, elevation_max_m, elevation_mean_m, elevation_range_m,
               slope_mean_deg, slope_max_deg, aspect_deg, aspect (N…NW),
               terrain_class flat|gentle|moderate|steep|very_steep, resolution_m, source}},
      landcover{{fractions{{type: share}}, dominant_type, dom_terrain, natural_fraction,
                 impervious_fraction, woody_fraction, woody_pixel_share, tree_pixel_share,
                 canopy_pixel_share, source}},
      trees{{count_sampled, height_max_m, height_mean_m, tallest[{{lon, lat, height_m,
             crown_d_m, confidence}}], dropped_qa, note}},
      buildings{{count, height_max_m, stories_max, roof_pitched_share, note}},
      landmarks[…]            (only with include=landmarks)
      forest_loss{{by_year{{"2015": px, …}}, since_2020_px, resolution:"kg", unit}},
      missing{{block: reason}}, retry_after_s (when not ready), meta}}

  dom_terrain = the dominant NATURAL cover (skips roof/road/parking/…) — use it as the ground
  colour of a lot; dominant_type may be "roof" for a built lot. Cells are area-weighted, so a
  30 m lot straddling four cells gets a proper mix. Blocks fail independently (missing{{}}).
  include= defaults to trees,buildings; landmarks must be asked for.

## Trees, landmarks, buildings (points in a bbox)

GET /api/v1/trees/bbox?bbox=…[&min_height=20][&limit=2000][&format=geojson]
    QA-filtered tree apices: {{lon, lat, height_m, crown_d_m, confidence, kg_code}}, tallest first.
    Realistic set: mast-like / implausible (> 60 m) / low-confidence apices are removed by our
    QA rules before you see them (dropped_qa tells how many). The index holds the tallest few
    apices per ~0.1 ha with h ≥ 20 m — it is a SAMPLE for map decoration and giant-tree finding,
    not a full inventory (meta.complete=false). crown_d_m only for single-crown segments.
GET /api/v1/landmarks/bbox?bbox=…
    Tall man-made verticals worth a map symbol: roof 18–60 m (church towers, silos, high-rises),
    mast 15–120 m, wind_turbine 40–200 m, bridge 20–120 m. {{type, lon, lat, height_m, area_sqm,
    confidence, kg_code}}. QA-filtered like trees.
GET /api/v1/buildings/bbox?bbox=…[&limit=3000]
    LiDAR-measured heights of built-up objects: {{lon, lat, height_max_m, height_mean_m,
    stories_est, roof pitched|flat, ground_elev_m, kg_code}}, tallest first. These are
    centroids of OUR segmented roofs — no outline ids / geometry / areas. Join to your own
    building outlines by point-in-polygon.

## Terrain grids and tiles

GET /api/v1/heightfield?bbox=…[&cell=25][&landcover=1]
    Regular lon/lat grid for 2.5D relief: {{cols, rows, cell_m, origin:"nw",
    rows_major_north_to_south:true, z:[[m a.s.l. | null]], elev_min, elev_max,
    landcover:[[class code]] + landcover_legend when asked}}. cell ≥ 25 (the native grid),
    ≤ 250 000 cells per call. 404 + ready:false where no product ≥ 2.1 grid exists yet.
GET /tiles/hillshade/{{z}}/{{x}}/{{y}}.png  ·  GET /tiles/slope/{{z}}/{{x}}/{{y}}.png
    256 px XYZ tiles, z 10–17, rendered from the 25 m DTM grid (X-Resolution-M: 25).
    204 = no data in this tile. Cache-Control 1 year, immutable, strong ETag (send
    If-None-Match → 304) — cache them. X-Tile-Source: hit|render.
    Fair use: rendering is budgeted. Cache hits are free and unlimited; cache MISSES
    are limited to 25 renders/s per client (burst 150) → 429 + Retry-After, and the
    renderer itself answers 503 + Retry-After when saturated. Clients MUST honour
    Retry-After (exponential back-off from it), keep ≤ 8 tile requests in flight,
    and never pre-fetch whole pyramids — fetch what a viewport needs, when it needs it.
    Low zooms (z10–11) span many KGs and are the most expensive renders.
GET /api/v1/health
    Liveness probe ({{ok, rss_mb}}; 503 while a worker is recycling). /api/v1/status?guard=1
    adds per-worker cache / gate / rate-limit telemetry.

## KG coverage (register keys only)

GET /api/v1/kgs?bbox=…                      KGs intersecting a bbox (processed_only=1 default)
GET /api/v1/kgs?limit=1000&offset=0         all processed KGs, paginated ({{kgs, total, …}})
GET /api/v1/kgs?codes=63349,63332           specific codes
GET /api/v1/kgs?fields=codes&processed_only=0 all 7 850 codes + universe_hash (see §KG universe)
GET /api/v1/kgs?fields=codes,als&processed_only=0 bulk ALS flight years, one row per KG:
    {{kg_code, als:{{newest, oldest, dominant, dominant_share, precision, reflown_share}}}} (als=null
    where no cells product yet); no histograms — those stay on /api/v1/kg/<code>. ~400 KB gz, max-age 1800.
    Strong ETag + Last-Modified (moves only when a block is (re)computed or the universe changes):
    send If-None-Match → 304 and skip the body. dominant = flight year covering the largest cell
    share of the KG (dominant_share); newest = max year present and may be a sliver (its share =
    coverage on /api/v1/kg/<code>). Label by dominant, treat dominant_share<0.9 as mixed.
    rows: {{kg_code, kg_name, gemeinde_code, gemeinde_name, processed, product_version,
           product_version_registry, ne_ready, ne_status, ne_cells_published_at,
           generated_at, updated_at, quality_grade, bbox, centroid, url}}
    product_version "v2.4" ⇒ cells served (see #cells publication invariant); "v2.4-pending"
    ⇒ generated, cells not yet published here (retry_after_s). Response carries
    manifest_generation + cells_generation (change whenever cells go live) and kgs_ne_served.
GET /api/v1/kg/{{kg_code}}                  one KG's landscape dossier:
    {{terrain, landcover{{fractions, by_type[{{type, fraction, n_objects, height_mean_m,
    height_p90_m}}], …}}, vegetation{{ndvi_mean, phenology_dominant, tree_count,
    tree_canopy_sqm, tree_mean_height_m, tree_stem_volume_m3}}, built{{building_count,
    …heights/storeys/pitched…}}, change{{net_volume_change_m3, …}}, forest_loss, quality,
    grid25, links}}. 202 + retry_after_s while the KG is unprocessed.
    Aliases for sibling services: /llm/kg/{{code}}, /llm/kgs, /llm/manifest.json, /llm.txt.
GET /api/v1/status                           kgs_processed / kgs_total, universe_hash, product versions, grid25 count, cells{{…}}

## KG universe — the contract: kgs_total = 7850
The set of Katastralgemeinden (BEV, 7 850 codes) is pinned per epoch:
  universe_hash = sha256(join(sorted(kg_code), "\n")) hex — fingerprint of the SET of codes,
    currently 057334c3faa38fc0443df14a34444f2b273f42cf27065fb1845c1c49b1fe6f05. Exposed in
    /api/v1/status and every /api/v1/kgs response (field universe_hash + header
    X-KG-Universe-Hash). Equal hash on both services ⇒ identical universe — compare the hash,
    don't download both lists.
  GET /api/v1/kgs?fields=codes&processed_only=0   {{codes:[7850], count, universe_hash}} (~60 KB)
    — enough to recompute the hash yourself. Cache-Control max-age 3600, revalidate with ETag.
  kgs_total ≠ 7850 or a changed universe_hash means the universe changed → treat as a new
  epoch; we bump this contract. Status also carries
  universe_agrees_contract (live index vs the pinned hash) so a drift is visible at once.

{CELLS_TXT.replace("{{", "{").replace("}}", "}").replace("{NE_STATS_NOTE}", NE_STATS_NOTE)}{STATS_TXT.replace("{{", "{").replace("}}", "}")}{FLAGS_TXT.replace("{{", "{").replace("}}", "}")}
## Segmentation UI — the interactive map at /segmentation                                #segmentation
  {BASE}/segmentation — draw or upload an area (≤ {_config.PUBLIC_MAX_AREA_KM2:g} km²), run
  Segment / Elevation / Terrain, toggle the LiDAR / ortho / CIR / forest-change overlays, export
  GeoPackage / KML / MBTiles, save & share a result (?share=<id>; shares made here are kept in
  their own namespace). The same endpoints are callable directly, no token:
    POST /api/v1/segment?async=true&source=auto[&min_object_size=30&types=…]   body: GeoJSON geometry
      → {{task_id}}; poll GET /api/v1/segment/progress?task_id=…, fetch GET /api/v1/segment/result
    POST /api/v1/elevation | /api/v1/terrain                 synchronous, same geometry body
    POST /api/v1/segment/overlay | /dtm|lidar|ortho|cir|hansen/overlay   PNG + X-Bounds header
    POST /api/v1/export/geopackage|kml|mbtiles, GET /api/v1/export/<kind>/download/<task_id>
    POST /api/v1/parse-geometry (KML/GPX/SHP/… → GeoJSON), GET /api/v1/layers?bbox=, /api/v1/attribution
    GET /api/v1/shares, POST /api/v1/share, GET /api/v1/share/<id>, POST /api/v1/share/<id>/rename
  source= chooses where the segmentation comes from:
    auto (default)  cheapest path. Every KG under the area has a finished v2.x product AND
                    those products are already cached here (or cost ≤ max(128 MB, 2 MB/ha)
                    to fetch) → the frozen product segments are served (seconds;
                    meta.source="product", meta.product_codes, meta.product_version).
                    Otherwise the live pipeline runs on the BEV rasters + Zenodo tile caches
                    (meta.source="bev"; meta.auto_reason explains a skipped product download)
    product         products only; 400 when nothing is finished there yet
    bev             always live (BEV DTM/DSM/ortho + Copernicus context; minutes for 1 km²)
  model=v2 (default, LightGBM — the same classifier the fleet products use) | v1 (legacy RF).
  Limits: 5 km² per request, 30 heavy requests/min per client (429 + Retry-After), 2 concurrent
  analyses per worker (503 + Retry-After when the gate is full). Results carry the same object
  vocabulary as the rest of this API; no property-boundary geometry is served or stored.

## Recipe: a map client with its own vector geometry

  1. Draw your lot / building outlines from your own source.
  2. Per feature (on click or when it enters the viewport), send ITS bbox:
     GET /api/v1/landscape?bbox=w,s,e,n   → dom_terrain for the ground colour, fractions for
     the composition chips, terrain for elevation/slope/aspect, trees.tallest for giants,
     buildings for storeys. Cache per feature for days (data changes only on KG reprocessing).
  3. Per viewport: /api/v1/trees/bbox (tree symbols), /api/v1/landmarks/bbox (towers,
     turbines), /api/v1/buildings/bbox (height → extrusion, matched to your outlines by
     point-in-polygon), /tiles/hillshade for the background.
  4. Registry / "which KGs are playable": /api/v1/kgs?limit=1000&offset=… and
     /api/v1/kg/{{code}} for per-KG stats; compare updated_at to detect reprocessing.
  If this API is down only these overlays degrade — your map keeps working.

## Attribution (short form; full per-source text + obligations in /api/v1/license)
  {ATTRIBUTION_SHORT}
  Derived segmentation and all statistics here: srtm-lidar-at, {_attr.OUTPUT_LICENSE} — credit
  "srtm-lidar-at landscape segmentation" when you redistribute. Counts derived from OSM /
  austria-power (infrastructure_count, landmark cross-checks) remain ODbL. Keep meta.attribution,
  the X-Data-Attribution and Link rel="license" headers with every stored copy. Not operated by or
  affiliated with BEV, ESA or the Copernicus programme.
"""

# Sources that reach the public tier (subset of attributions.KG_PRODUCT_SOURCES;
# basemap_at is display-only in the private UI and never served here).
PUBLIC_SOURCES = ('bev_als', 'bev_dop', 'bev_cadastre', 'copernicus_s2', 'copernicus_s1',
                  'esa_worldcover', 'hansen_gfc', 'osm', 'austria_power')
_USED_FOR = {
    'bev_als': 'terrain, heights (trees, structures, landmarks), hillshade / slope tiles, heightfield, cells h_*',
    'bev_dop': 'segmentation features (RGBI), tree / structure delineation',
    'bev_cadastre': 'per-cell statistical land-use comparison only (cells.consistency); no geometry, no identifiers',
    'copernicus_s2': 'NDVI mean / amplitude, phenology class, vegetation shares',
    'copernicus_s1': 'SAR backscatter features of the segmentation',
    'esa_worldcover': 'land-cover prior',
    'hansen_gfc': 'forest_loss by year, cells.hansen_loss_year',
    'osm': 'aggregate counts only (infrastructure_count); no OSM geometry is served',
    'austria_power': 'landmark cross-check (masts, wind turbines); aggregate counts only',
}


def license_block() -> dict:
    """/api/v1/license — per-source obligations, headers, not_served,
    personal_data, caching; wording from attributions.py."""
    d = _attr.attribution_dict(PUBLIC_SOURCES)
    sources = []
    obligations = []
    for k in PUBLIC_SOURCES:
        src = dict(d['sources'][k])
        src['id'] = k
        src['used_for'] = _USED_FOR.get(k, '')
        sources.append(src)
        ob = f"{src['provider']}: {src['license']} — '{src['attribution']}'"
        if src.get('share_alike'):
            ob += ' — share-alike: a redistributed DATABASE that merges these fields becomes ODbL'
        obligations.append(ob)
    return {
        'service': 'srtm-lidar-at',
        'stage': 'alpha',
        'license': _attr.OUTPUT_LICENSE,
        'license_id': _attr.OUTPUT_LICENSE_ZENODO_ID,
        'license_url': _attr.OUTPUT_LICENSE_URL,
        'attribution': ATTRIBUTION_SHORT,
        'modification_note': d['modification_note'],
        'osm_derived': {'fields': ['infrastructure_count'], 'license': d['osm_derived_license'],
                        'note': 'aggregate counts; no OSM geometry or OSM-keyed record is served'},
        'sources': sources,
        'per_source_obligations': obligations,
        'derived_work': 'Segmentation classes, statistics, QA filters and the H3 cell layer are our own work, '
                        f'{_attr.OUTPUT_LICENSE}: credit "srtm-lidar-at landscape segmentation".',
        'not_served': 'No property-register data (lot or building outlines, lot identifiers, land-use '
                      'register codes) and nothing derived from it beyond per-cell statistics is served by '
                      'this public API.',
        'personal_data': 'None. No owner data, no lot identifiers are stored or returned.',
        'response_headers': ['X-Data-Attribution', 'Link: <' + _attr.OUTPUT_LICENSE_URL + '>; rel="license"'],
        'caching': 'Allowed. Keep meta.attribution with every stored copy. Never cache ready:false / '
                   '202 / error bodies (they mean UNKNOWN, not empty).',
        'generated_at': _dt.datetime.now(_dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    }


# Shared look of every HTML page this cluster serves (public landing page,
# peer note in api_tiers.peer_note_html). Plain str, embedded by the
# INDEX_HTML f-string below.
PAGE_STYLE = """<style>
:root{--bg:#fbfbf8;--fg:#1f2328;--mut:#6b7280;--rule:#d9d9d2;--acc:#0b5fa5;--code:#f1f1ec}
@media(prefers-color-scheme:dark){:root{--bg:#111312;--fg:#d7d9d4;--mut:#8a918c;--rule:#2c302d;--acc:#7ab4ea;--code:#1b1e1c}}
*{box-sizing:border-box}
body{font:14px/1.6 ui-monospace,SFMono-Regular,Menlo,Consolas,"Liberation Mono",monospace;background:var(--bg);color:var(--fg);
  max-width:84ch;margin:3rem auto;padding:0 1.25rem;-webkit-font-smoothing:antialiased}
h1{font-size:1.1rem;font-weight:700;letter-spacing:.02em;margin:0 0 1rem;padding-bottom:.6rem;border-bottom:1px solid var(--rule)}
h1::before{content:"# ";color:var(--mut)}
h2{font-size:.95rem;font-weight:700;margin:2.4rem 0 .8rem;text-transform:uppercase;letter-spacing:.08em;color:var(--fg)}
h2::before{content:"## ";color:var(--mut)}
p{margin:.7rem 0}
a{color:var(--acc);text-decoration:none;border-bottom:1px solid transparent}a:hover{border-bottom-color:var(--acc)}
code{background:var(--code);padding:1px 5px;border-radius:3px;font-size:.95em}
pre{background:var(--code);border-left:3px solid var(--rule);padding:.8rem 1rem;margin:1rem 0;overflow:auto;line-height:1.5;font-size:.95em}
table{border-collapse:collapse;width:100%;margin:.8rem 0}
th{text-align:left;font-weight:700;color:var(--mut);text-transform:uppercase;font-size:.78em;letter-spacing:.08em;padding:4px 10px 6px 0;border-bottom:1px solid var(--rule)}
td{padding:6px 10px 6px 0;border-bottom:1px dashed var(--rule);vertical-align:top}
td:first-child{color:var(--mut);white-space:nowrap;width:36%}
td code{background:none;padding:0}
strong{font-weight:700}
.muted{color:var(--mut);font-size:.9em;margin-top:2.5rem;padding-top:.8rem;border-top:1px solid var(--rule)}
.stage{font-size:.65rem;font-weight:400;letter-spacing:.12em;text-transform:uppercase;color:var(--mut);border:1px solid var(--rule);border-radius:3px;padding:0 .4em;margin-left:.6em;vertical-align:middle}
.cov{color:var(--mut)}
</style>"""


INDEX_HTML = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<title>srtm-lidar-at — landscape from LiDAR + satellites, for any place in Austria</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
{PAGE_STYLE}</head><body>
<h1>srtm-lidar-at — landscape from LiDAR + satellites <span class="stage" title="{ALPHA_NOTE}">alpha</span></h1>
<p>A bbox or a point in, what the segmentation learned out: terrain (25 m DTM), land-cover shares,
realistic tree apices, structure heights, landmarks, forest loss. Austria-wide, 7 850 Katastralgemeinden,
served from a pre-built index. JSON, CORS, no key.</p>
@@COVERAGE@@
<p><strong>Landscape only.</strong> Everything is keyed by bbox, point or KG code; no property-register
identifiers or vector outlines are served. Clients hold their own geometry and ask us about the bbox
of each feature.</p>
<h2>Start here</h2>
<pre>GET /api/v1/landscape?bbox=15.084,47.064,15.088,47.068
GET /api/v1/landscape?lon=15.0857&amp;lat=47.0665&amp;radius_m=20
GET /api/v1/trees/bbox?bbox=15.08,47.06,15.09,47.07&amp;min_height=25
GET /api/v1/cells?lon=15.0857&amp;lat=47.0665&amp;k=1</pre>
<table><tr><th>Need</th><th>Call</th></tr>
<tr><td>Everything about a bbox / point</td><td><code>/api/v1/landscape?bbox=</code> (alias <code>/context</code>)</td></tr>
<tr><td>Realistic tall trees</td><td><code>/api/v1/trees/bbox?bbox=</code></td></tr>
<tr><td>Towers, masts, wind turbines</td><td><code>/api/v1/landmarks/bbox?bbox=</code></td></tr>
<tr><td>Measured building heights</td><td><code>/api/v1/buildings/bbox?bbox=</code></td></tr>
<tr><td>Per-cell observations (H3 res 12, ~300 m²)</td><td><code>/api/v1/cells?bbox=</code> · <code>/api/v1/cells?lon=&amp;lat=&amp;k=1</code> · <code>/api/v1/cells/{{h3}}</code></td></tr>
<tr><td>Whole-KG cell file (download)</td><td><code>/api/v1/kg/{{code}}/cells</code> (<code>format=h3b</code> | <code>json</code> summary)</td></tr>
<tr><td>Relief grid for 2.5D</td><td><code>/api/v1/heightfield?bbox=&amp;cell=50</code></td></tr>
<tr><td>Hillshade / slope map tiles</td><td><code>/tiles/hillshade/{{z}}/{{x}}/{{y}}.png</code> · <code>/tiles/slope/…</code></td></tr>
<tr><td>Which KGs are processed</td><td><code>/api/v1/kgs?bbox=</code> · <code>/api/v1/kgs?limit=1000&amp;offset=0</code> · <code>/api/v1/kg/{{code}}</code></td></tr>
<tr><td>Coverage</td><td><code>/api/v1/status</code></td></tr></table>
<h2>Cells — what we measured, hexagon by hexagon</h2>
<p>Every processed KG is also published as a grid of <a href="https://h3geo.org">H3</a> res-12 cells
(~307 m², ~52 000 per KG). Each cell carries one 41-byte observation record — land-cover shares in nine
groups (<code>geb bau acker gruen wald wasser verkehr alpen sonst</code>), structure cover and count, LiDAR
heights (mean / p90 / max), tree count, tallest tree and canopy share, NDVI mean + seasonal amplitude and a
phenology class, height and ground change between ALS years, Hansen forest-loss year, terrain (elevation,
slope, aspect, roughness, TRI, TPI) and a consistency verdict against the statistical
land-use layer for the same cell. On top of that: every tree apex
≥ 3 m and every segmented structure with its LiDAR height, positioned inside the cell.
Cell ids follow the shared NE cells contract (ne-cells-2), so an observed and a statistical layer
zip together client-side by cell id.{NE_STATS_HTML}</p>
<pre>GET /api/v1/cells?bbox=16.354,48.187,16.357,48.189
    # cells in a box (span &le; 0.05&deg;, &le; 16 384 cells)
GET /api/v1/cells?lon=16.355&amp;lat=48.188&amp;k=1
    # the cell under a point + 1 ring
GET /api/v1/cells?bbox=&hellip;&amp;format=columns&amp;fields=cover,h_max
    # raw quantised columns for a map heat layer
GET /api/v1/cells?bbox=&hellip;&amp;res=10
    # server-side roll-up to res 11 / 10
GET /api/v1/cells/8c1e108cc922dff
    # one cell, all layers (obs + trees + structures)
GET /api/v1/cells/dict
    # fields, units, code tables, limits
GET /api/v1/kg/01008/cells
    # whole-KG H3B container (CC BY 4.0 product download)
GET /api/v1/landscape?bbox=&hellip;&amp;include=cells
    # per-bbox roll-up block inside /landscape</pre>
<p>Formats <code>json | columns | bin | csv | geojson</code> (+ <code>h3b</code> for chunk and whole-KG
downloads); layers <code>obs,trees,structures</code>. Not processed yet → <code>404 {{ready:false, retry_after_s}}</code>;
a bbox that is only partly covered answers 200 with <code>meta.partial:true</code>. Readiness:
<code>/api/v1/status</code> → <code>cells{{kgs_ready, cells_total, epoch}}</code>. Full field layout and the binary record
specs are in the <a href="/api/v1/docs/llm.txt#cells">reference</a>.</p>
<h2>Flags — when something looks wrong</h2>
<p>Every public layer can be flagged, tokenless: a cell whose observed cover group looks off, a giant tree that is a
mast, a structure height that cannot be, a tile artefact. Subjects are H3 res-12 cells (or a point → its cell) and
KG codes only. Repeats fold into one row; when you omit <code>observed</code> we snapshot what we serve for that
cell at that moment, so the flag can be judged after a rebuild. Send <code>looks_right</code> too.</p>
<pre>POST /api/v1/flags  {{"lon": 15.0857, "lat": 47.0665, "layer": "cells.cover", "claim": "wrong_type",
                     "expected": {{"group": "gruen"}}, "note": "mown meadow", "source": "my-map"}}
GET  /api/v1/flags?bbox=&hellip;&amp;format=columns      # disputed cells in a viewport tile
GET  /api/v1/flags/heat?res=7                      # national overview for a map glyph
GET  /api/v1/cells/8c1e108cc922dff?flags=1         # cell dossier with its flag tally
GET  /api/v1/flags/dict                            # layers, claims, limits</pre>
<h2>Segmentation map</h2>
<p><a href="/segmentation">/segmentation</a> — draw an area, see the object segmentation
(precomputed products first, live LiDAR where none exists yet), overlay DTM / ortho / CIR / forest
change, export GeoPackage / KML / MBTiles, share a result. Details: <a href="/api/v1/docs/llm.txt#segmentation">llm.txt → Segmentation UI</a>.</p>

<h2>Documentation</h2>
<p><a href="/api/v1/docs/llm.txt">Full reference (plain text, for humans and LLM agents)</a> ·
<a href="/api/v1/license">Licences &amp; attribution</a></p>
<p class="muted">{ATTRIBUTION_SHORT}. Derived segmentation, statistics and the cell layer © srtm-lidar-at,
{_attr.OUTPUT_LICENSE} (<a href="{_attr.OUTPUT_LICENSE_URL}">licence</a>). Per-source obligations: <a href="/api/v1/license">/api/v1/license</a>.
Not operated by or affiliated with BEV, ESA or the Copernicus programme. Stage: {ALPHA_NOTE}.</p>
</body></html>"""


# --------------------------------------------------------------------------
# Live coverage block.  Both LLM_TXT and INDEX_HTML carry a literal
# ``@@COVERAGE@@`` marker that public_api.render_docs() substitutes with the
# current /api/v1/status numbers (cached, so the docs stay a static-cost
# response).  Kept here so the wording lives next to the rest of the prose.
# --------------------------------------------------------------------------
COVERAGE_MARK = '@@COVERAGE@@'


def _fmt_n(n) -> str:
    try:
        return f'{int(n):,}'.replace(',', '\u202f')
    except (TypeError, ValueError):
        return '?'


def _current_version() -> str:
    try:
        from v21_products import MANIFEST_VERSION
        return str(MANIFEST_VERSION)
    except Exception:  # noqa: BLE001
        return 'v2.4'


def _current_count(st: dict) -> tuple[int, int]:
    """``(complete, pending)`` for the current product version.

    *complete* = KGs whose product is the FULL current version (``vX.Y``
    exactly) AND whose cells file is served completely here — the same
    number reported as "observed layer live".
    *pending* = register says ``vX.Y`` but cells not yet published
    (``vX.Y-pending`` in ``/api/v1/kgs``).  ``-partial`` variants, split
    blocks and older versions are deliberately NOT counted: clients only
    care about complete KGs."""
    if st.get('kgs_current_complete') is not None:
        return int(st['kgs_current_complete']), int(st.get('kgs_current_pending') or 0)
    cur = _current_version()
    pv = st.get('product_versions') or {}
    return int((pv.get(cur) or 0)), int((pv.get(cur + '-pending') or 0))


def coverage_txt(st: dict) -> str:
    tot = st.get('kgs_total') or _KG_TOTAL
    cur = _current_version()
    n, pend = _current_count(st)
    pct = 100.0 * n / tot if tot else 0
    return (f'coverage (live, refreshed ~10 min): {_fmt_n(n)} / {_fmt_n(tot)} complete KGs at product {cur} ({pct:.1f} %)'
            f' — complete = whole KG (no split blocks), all layers, cells served'
            + (f'; {_fmt_n(pend)} more at {cur} awaiting cells publication ({cur}-pending — poll, not a gap)' if pend else '') + '\n'
            f'  {_fmt_n(tot)} = all Austrian Katastralgemeinden. '
            f'Index updated {st.get("index_updated_at") or "?"} — exact numbers: GET /api/v1/status')


def coverage_html(st: dict) -> str:
    tot = st.get('kgs_total') or _KG_TOTAL
    cur = _current_version()
    n, pend = _current_count(st)
    pct = 100.0 * n / tot if tot else 0
    return (f'<p class="cov"><strong>Coverage right now:</strong> {_fmt_n(n)} / {_fmt_n(tot)} complete KGs at product {cur} '
            f'({pct:.1f}&nbsp;%) — complete = whole KG (no split blocks), all layers, cells served'
            + (f'; {_fmt_n(pend)} more at {cur} awaiting cells publication (<code>{cur}-pending</code> — poll, not a gap)' if pend else '')
            + f'. {_fmt_n(tot)} = all Austrian Katastralgemeinden. '
            f'Index updated {st.get("index_updated_at") or "?"}; '
            f'live numbers at <a href="/api/v1/status">/api/v1/status</a>. Unprocessed areas answer '
            f'<code>202</code> / <code>404 ready:false</code> — processing runs around the clock.</p>')
