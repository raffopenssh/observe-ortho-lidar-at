# BEV vector-tile context — `vct_pilot.py` (`SRTM_BEV_VECTOR_CONTEXT`)

Optional, **off by default**. When `SRTM_BEV_VECTOR_CONTEXT=1`
(`config.BEV_VECTOR_CONTEXT`), the processor's NE step builds each KG's
*declared* NE layer (NEC1, k-anonymous per-cell land-use statistics,
`docs/ne-v24-design.md`) locally from the public BEV vector tiles, and
the same documents give the segmentation model the building-footprint
context it uses as `building_mask`. Without the flag the processor has no
declared layer (every NE verdict is `unknown`, product labelled
`v2.4-partial` with defect `ne_cells`) and no footprint context.

Modules: `vct_pilot.py` (helper + telemetry), `ne_h3.py` (tooling,
`fetch_declared`, `bevdirect-serve` launch), `ne_build_tiled.py` (tiled
NEC1 build for huge parents), hooks in `austria_processor.py` (step 1c /
5c) and `app.py` (`GET/POST /api/v1/admin/vct_pilot`).

## What is fetched and where it lives

The pipeline consumes public BEV vector tiles (CC BY 4.0,
`gis.bev.gv.at`) through a local [vtcseamless](https://github.com/raffopenssh/vtcseamless)
`bevdirect-serve` (pinned `ne_h3.BEVDIRECT_TAG`, sha256-pinned release
tarball, `data/bin/bevdirect-serve`, `127.0.0.1:8787`) and the pinned
[vtcseamless-py](https://github.com/raffopenssh/vtcseamless-py)
`ne_cells` package (`ne_h3.NE_CELLS_PIN`, `NE_CELLS_MIN_VERSION`). The
server assembles footprints + land use for one `/viewport` per aligned
0.02° cell (`ix = floor(lon / 0.02)`) intersecting the parent bbox
(+0.004°). Property-shaped data is **only ever in memory**: the server
keeps raw tiles in an in-process RAM LRU (`BEVDIRECT_TILE_CACHE_MB` 512,
`BEVDIRECT_TILE_TTL` 24h, `GOMEMLIMIT` `BEVDIRECT_GOMEMLIMIT`; no `.pbf`
is written) and the assembled cells in RAM (`BEVDIRECT_CELL_TTL` 6h); the
viewport documents are parsed in-process into aggregated NEC1 cells and
dropped; the footprints reach the KG subprocess over an anonymous pipe
and live only in that process. Nothing property-shaped is written to
disk or into any product — only counts / digests / bytes land in
`data/austria_processor/vct_pilot.jsonl` (ring of 200 rows). The light
GPKG forbids footprint-shaped layers (`v2_verify.FORBIDDEN_LIGHT_LAYERS`)
and the JSON carries no outlines.

| rule | implementation |
|---|---|
| in memory only, ≤ 24 h | RAM LRUs with TTL in `bevdirect-serve`; `ne_source=bevdirect` work files on tmpfs (`BEVDIRECT_WORK`, `/dev/shm`) unlinked after the build |
| one server | `ne_h3._ensure_bevdirect_server` launches one long-lived server (pidfile `data/bevdirect-serve.pid`), restarted only when `/health` reports a version ≠ `BEVDIRECT_TAG` |
| one request per cell, never a free viewport | `vct_pilot._grid` → aligned 0.02° cells; `-prefetch 0`; blocks of a split parent re-use the parent's run for 24 h (`recent_for`) |
| never interrupts the pipeline | bounded helper subprocess (`VCT_PILOT_TIMEOUT_S`, 25 min) in a thread; a failure is one WARNING + a jsonl row, the KG continues without NE cells / footprint context |
| one tile generation per build | every cell document must be assembled after the last BEV regeneration window and < `VCT_DOC_MAX_AGE_S` ago, no tile `(z,x,y)` with two sha256 across documents; a violation restarts the server and refetches once (`INCOMPLETE_REFETCH`) |

## Tooling

`ne_h3.bevdirect_ready()` → `{binary, binary_version, package, server,
tag, pin}`; `binary: true` only when `bevdirect-serve -version` equals
`BEVDIRECT_TAG`. `ne_h3.ensure_bevdirect_tooling()` installs the release
tarball (`BEVDIRECT_RELEASE_URL`) or falls back to `go install`;
`ne_h3.ensure_ne_cells_package()` pip-installs the pinned package
(`--user`, `sys.executable`; pip tail kept in `data/ne_tooling_last.json`).
Both run from `app._ensure_v2_deps` at web-service start.
`GET /api/v1/admin/ne_tooling` reports the state.
<!-- VERIFY: ensure_bevdirect_tooling / ensure_ne_cells_package still run at startup after the app.py cut; otherwise document the manual install. -->

## Build

`vct_pilot.build_declared(kg, bbox, context=…)` fetches the cells, builds
NEC1 with `ne_cells` (`ne-cells-2`, source `bevdirect@<tag>`, epoch =
current month) and keeps it under `data/ne/<kg>.nec` (reused < 48 h by
sibling blocks: the helper then runs in context-only mode (`fetch_context_only`) and only streams
the footprints over the pipe). Above `VCT_TILE_CELLS` (300 000) estimated
cells the global input domain is cut into an nx×ny grid of padded cores
(`ne_build_tiled.build_cells_tiled`, 0.006° pad, "homed" cells only) —
byte-identical to the monolithic build at a fraction of the RSS (the
dense `ne_cells.algo.build_cells` needs ≈ 3.4 KB per product cell). Rows
carry `rss=<peak MB>` and `tiled=N`. Step 5c retries the declared layer
once right before `gpkg_light` (`result.ne_status = ok_late`).

## Operating it

```bash
# .env: SRTM_BEV_VECTOR_CONTEXT=1 ; restart austria_processor.service
curl -s 'localhost:8000/api/v1/admin/vct_pilot?rows=5'         # summary + last rows
curl -s 'localhost:8000/process.txt?log=0' | grep -A3 '^vct_pilot:'
python3 vct_pilot.py summary                                      # same from the CLI
```

`/process.txt` block: `vct_pilot: on · kgs_24h=N ok=M · bw(vtcseamless):
net~MB/24h tiles=MB req=N`, then one row per KG (`<kg> <time>
bevdirect@<tag> cells=N req=N net~MB lru=used/cap rss=MB tiled=N`).
`net~` is the estimated bytes fetched from the BEV tile host
(`bytes × (1 − hits / tiles)`); `req=` the number of viewport requests.
