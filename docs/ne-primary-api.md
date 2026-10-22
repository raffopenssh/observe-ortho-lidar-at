# NE cells on the primary — ingest corpus + (planned) fast NE API

Status 2026-10-04: **ingest implemented** (`ne_ingest.py`, thread in `app.py`,
primary only); **API endpoint not yet built** — design below.

## Corpus (`data/ne/`)

* `data/ne/<parent>.h3b` — unsplit KGs (pulled as-is) and split parents
  (merged from blocks, header `merged_from`, `partial_cells` until complete).
* `data/ne/blocks/<block>.h3b` — raw block products (kept; re-merge source).
* `data/ne/index.json` — `files{code→uploaded_at,size,md5,src,peer,neo1_digest…}`,
  `parents{parent→blocks,digest,complete,…}`, `umfeld{parent→confirmed_digest,
  post_attempts,…}`, `failed{}`, `stats{pulled_peer,pulled_zenodo,merges,posts}`.
* Readers: `ne_h3.read_h3b_header(path)` → header with `chunks[]` (one per
  res-10 parent: `r10, off, len, cells, trees, bld`), `read_h3b_chunk(path,
  hdr, chunk)` → `{lu, obs, trees, bld}` raw rows; `ne_ingest.read_rows` for a
  whole file.  NEO1 layout: `ne_h3.OBS_REC` / `OBS_FIELDS` (41 B).

## Load caveats baked into the ingest

* **Primary inbound is metered** (AGENTS.md: < 50 MB/h total).  All bulk pulls
  go through `inbound_budget.allow/add` (rolling hour, persisted):
  channel `ne` budget knob `ne_ingest_mb_per_h` (default 15), channel
  `v2json` (`v2_ingest`) knob `v2_ingest_mb_per_h` (default 25).  Backlogs
  drain over days; umfeld is the live NE source meanwhile.
* **One shared Zenodo token (~5000 req/h)** is the fleet-wide bottleneck of
  the v2.4 wave, so both ingests pull **peer-first**: peers keep uploaded
  `<code>_ne.h3b` under `data/austria_processor/ne_products/` (GC
  `NE_KEEP_DAYS`=7 / 2 GB LRU) and serve them + `<code>_v2.json.gz` via
  `GET /api/v1/processing/product?key=<code>_ne_h3b|<code>_json_v2`
  (admin token).  Who holds what: `product_origin.json`, filled by
  `POST /api/v1/manifest/push` (peer adds `peer` to the payload).  Zenodo is
  the fallback (md5 vs manifest checksum either way).
* **Split parents**: blocks carry the parent's full cell set with unseen cells
  as nodata; merge per cell = the block that observed it (overlap → larger
  land-use mass).  Byte-exact on identity / 10 %-overlap synthetic splits.
  Complete ⇔ every live block (`<parent>-*_json_v2` committed) ingested AND
  `app._kg_coverage_complete(parent)`.  Only complete merges are POSTed.
* **umfeld confirmation** every tick: umfeld's `/ne/manifest` `kgs[kg].obs.digest`
  must equal our NEO1 digest for every parent on disk; missing/mismatch →
  re-POST from the h3b (`neo1_section_from_h3b`), ≤5 posts/tick, 6 attempts,
  30 min × 2^n backoff; KGs umfeld hasn't built yet are counted `unbuilt`.
* Knobs: `POST /api/v1/director/config {"ne_ingest_enabled": false}` stops
  pulls (merge/confirm keep running).  Line `ne_ingest:` in `/process.txt`,
  structured at `director/status.v2.ne_ingest`.

## Public endpoint — superseded by `docs/ne-public-api.md` (cells_api.py); the sketch below is historical

* Queries: `bbox=w,s,e,n` | `point=lon,lat&radius_m=` | `kg=<code>`;
  `layers=obs|lu|trees|bld` (default obs), `format=json|csv|h3b`.
* Resolution: bbox → H3 res-10 cover (`h3.polygon_to_cells`) → KG codes via
  the search index R-tree → for each KG on disk read only the chunks whose
  `r10` is in the cover (header directory, no full inflate); decode NEO1 via
  `OBS_REC` → dicts with `OBS_UNITS` applied.  Hard caps: ≤ 20 000 cells /
  response, ≤ 8 KGs; 413 beyond.
* **Fallback** when a KG is not on disk: `ne_h3.fetch_declared_umfeld(kg,
  layer="obs")` with the umfeld token — rate-limited (≤ 30/min, token bucket)
  and NOT written to `data/ne/` (the ingest owns the corpus); answers carry
  `X-NE-Source: disk|umfeld` and `X-NE-Partial` when the merge is partial.
* Keep it off the public tier until parcel-free review (cells are 1-ha
  aggregates — fine in principle; see docs/api-tiers.md).
