# Licensing & attribution (`attributions.py`)

Single source of truth for licence/attribution wording. Consumers:

| Where | How |
|---|---|
| `GET /api/v1/attribution` (+ `?format=text`) | `attribution_dict()` / `attribution_text()` |
| `GET /api/v1/info` → `license`, `attribution`, `attribution_short` | same |
| `POST /api/v1/segment` → `meta.attribution` / `meta.license` | `attribution_short()` |
| KG JSON summary → `attribution` key | `attribution_dict(year=obs_year)` (`build_json_summary_tiled`) |
| GPKG (`_full`, `_light`, streamed) → `gpkg_metadata` + `gpkg_metadata_reference` | `write_gpkg_metadata(conn, layers=…)` from `_write_gpkg_all_styles`; dataset-scope text + JSON rows, plus a per-table ODbL row for `OSM_DERIVED_LAYERS` |
| Zenodo deposits (KG products, mirror, tile cache) → `description` footer, `notes`, `license`, `keywords` | `zenodo_description_footer()`, `attribution_text()`, `OUTPUT_LICENSE_ZENODO_ID`, `zenodo_keywords()` |
| `public_docs.py` (llm.txt attribution section, landing footer, `license_block()` → `/api/v1/license`) | generated from `attribution_short()` / `attribution_dict()` — no hand edits |
| `static/index.html` Leaflet attribution control | inline string, links to `/api/v1/attribution` |

## Assessment

**BEV** (ALS DTM/DSM, DOP orthophoto, vector tiles) is Open Government Data
under **CC BY 4.0** (data.bev.gv.at metadata: "Für dieses Produkt gilt die
Standardlizenz CC-BY-4.0"; the same licence applies to the public BEV
vector-tile service). CC BY 4.0 permits copying, transformation
(segmentation, APIs, derived products) and commercial use.
Obligations:

1. Attribution: "Datenquelle: BEV – Bundesamt für Eich- und
   Vermessungswesen" (+ Stichtag where known).
2. Licence link: https://creativecommons.org/licenses/by/4.0/
3. Indicate changes → "bearbeitet / modified" (`MODIFICATION_NOTE`).
4. No implication of BEV endorsement.

Caveats:
- BEV holds copyright + sui generis database rights (§76c ff UrhG); the CC
  BY grant licenses exactly those, no separate agreement needed.
- No personal data (ownership etc.) is in the open datasets nor in our
  products → no GDPR issue.
- Stichtag: ALS epoch folders (20220915 …) are mosaic snapshot dates; real
  flight years per block come from `als_acquisition.py` (`meta.acquisition`).

**Copernicus Sentinel-1/-2** — free, full and open (Reg. (EU) 377/2014,
Del. Reg. (EU) 1159/2013). Modified products must state "Contains modified
Copernicus Sentinel data [year]". No share-alike.

**ESA WorldCover 2021 v200** — CC BY 4.0; cite Zanaga et al. 2022,
doi:10.5281/zenodo.7254221.

**Hansen GFC** — CC BY 4.0; cite Hansen et al. 2013, Science 342:850–853.

**OpenStreetMap** — **ODbL 1.0**, not CC BY. "© OpenStreetMap contributors".
OSM-derived fields merged into a redistributed database trigger
share-alike for that database → the `infrastructure` layer/key (OSM power
polygons via `power_infrastructure.py`) is listed in `OSM_DERIVED_LAYERS`,
flagged per-table in GPKG metadata and called out in the Zenodo footer.
OSM as an RF *training label* source yields a produced work / model, not
a derivative database (ODbL Community Guidelines).

**Optional power-infrastructure service** (`POWER_API_URL`, off by
default) — mixed upstream terms; treated like OSM (share-alike flag).

**basemap.at** — CC BY 4.0, display-only map background in the UI.

Our Zenodo derivatives are published CC BY 4.0 — compatible with all
sources above except OSM-derived layers, which stay ODbL.

## Adding a new data source

1. Add an entry to `attributions.SOURCES` (name, provider, license,
   license_url, attribution, url; `citation` / `share_alike` if applicable).
2. Add its id to `KG_PRODUCT_SOURCES` if it lands in KG products.
3. If it is share-alike, add the layer/JSON key to `OSM_DERIVED_LAYERS`
   (rename if it stops being OSM-only).
4. Update the table in `llm.txt` → "Licence & Attribution".
