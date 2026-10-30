# Zenodo persistent tile cache (`zenodo_cache.py`, `zenodo_tiles.py`)

Local Copernicus / Hansen tile caches (`tile_cache.py`, grid-snapped 0.1°
cells under `/tmp/copernicus_cache/`, `/tmp/hansen_cache/`) are evicted
when the disk gets tight. The Zenodo cache persists those tiles in a
Zenodo deposit so that they survive eviction and can be shared between
machines: a tile that was fetched from openEO once (and cost credits) is
never fetched again.

## Read-only vs. read-write

* `config.ZENODO_CACHE_DEPOSIT` (default **19650075**, the project's
  public tile-cache deposit) is where the fetchers look on a local cache
  miss. Reading needs no credentials when the deposit's files are
  publicly accessible. <!-- VERIFY: confirm anonymous range reads against
  the deposit files work; Zenodo draft buckets need the owner secret -->
* Writing (uploading new tiles, creating shards, reconciling) needs
  `ZENODO_TOKEN` (or `$SRTM_SECRETS_DIR/zenodo_token`). Without it
  `config.uploads_enabled()` is false and every upload path is a no-op —
  the processor still works, it just does not persist its tiles beyond the
  local disk. Point `ZENODO_CACHE_DEPOSIT` at your own deposit if you want
  to write.
* All Zenodo writes serialise through `zenodo_lock.zenodo_upload_lock()`
  — a local lock in single-VM mode, the broker at `ZENODO_LOCK_URL`
  otherwise (`docs/app.md`).

## Two manifests (don't confuse them)

| File | Contents |
|---|---|
| `data/austria_processor/cache_manifest.json` | the tile-cache deposit: `files{name → size, tile_count, updated_at, depo_id, kind …}`, `shards{}` (tile store v2), tile-checkpoint entries (`chkpt_*`) |
| `data/austria_processor/zenodo_manifest.json` | KG **product** uploads (`<code>_full_gpkg`, `_light_gpkg_v2`, `_ne_h3b`, `_json_v2`, `_json`) — `zenodo_client.Manifest` |

Every writer of `cache_manifest.json` holds
`zenodo_cache.cache_manifest_write_lock()` (fcntl on `cache_manifest.lock`,
30 s bounded wait) across read → modify → `os.replace` and **re-reads inside
the lock**. Atomic replace protects against torn files, not against lost
updates. `CacheManifest` additionally tracks dirty entries
(`_dirty_files` / `_dirty_shards` / `_dirty_top`) so a reload from disk
never drops an unsaved mutation (newest `updated_at` wins).

## v1 layout — ZIP per product × latitude strip (Hansen, frozen Copernicus)

Each local `.npz` tile has a `.meta.json` sidecar (written by
`tile_cache._write_tile_meta()`) recording product and grid coordinates.
**Every `.npz` MUST have a sidecar** — orphans are invisible to the
uploader; `cleanup_orphan_tiles()` deletes them at processor start.

`ZenodoCache.upload_all()`:

1. `_build_reverse_index()` reads the sidecars (fallback
   `tile_bbox_index.json`);
2. groups tiles by (product, 0.5° lat strip);
3. compares local entry names with the cached remote ZIP central
   directory;
4. local ⊆ remote → skip; otherwise merge local + remote-only into a new
   ZIP and upload. If the remote directory cannot be listed for a non-404
   reason the ZIP is **deferred**, never rebuilt local-only (that would
   lose every remote-only tile);
5. invalidates the cached ZIP index.

**Download**: on a local miss `tile_cache` calls
`ZenodoCache.fetch_copernicus()` / `fetch_hansen()`, which read individual
NPZ members with 2–3 HTTP range requests using the cached central
directory (`data/austria_processor/zenodo_zip_index/*.json`, keyed by MD5
of the URL). Stale indices cause false "local ⊆ remote" skips — delete the
directory to force a re-fetch.

**When uploads happen**: after each completed tile in the KG subprocess
(throttled to 30 min), after KG completion (forced), before disk eviction
of expensive tiles (forced), before each KG in the parent (throttled).

### Upload failure semantics

`_upload_file` is **PUT-in-place → verify via deposit listing**. Zenodo's
bucket API replaces a same-name object atomically, so a failed PUT leaves
the previous copy and its manifest entry intact (a delete-then-PUT cycle
would turn every transient 5xx into permanent cache loss). After any PUT
failure the deposit is re-listed once and `_handle_upload_failure`
decides:

| listing says | error class | action |
|---|---|---|
| target present, size == sent | anything | **success** (timeout after landing) — manifest updated from the listing |
| target present, other size | anything | keep entry untouched, WARN |
| absent / listing failed | 5xx, 429, timeout, SSL, connection | mark `unverified: true` (+ `unverified_since`, `last_error`); readers still serve it, local tiles kept |
| absent | definitive 4xx, first attempt | `unverified` + `upload_4xx_history` |
| absent | definitive 4xx, second attempt ≥ 10 min later | **tombstone** (`prev_size/prev_checksum/prev_tile_count` preserved) |
| any | Zenodo-degraded circuit set | never tombstone → `unverified` |

A read-side 404 on a ZIP index (`_confirm_404_then_tombstone`) also
re-lists first. `CacheManifest.get_file` hides tombstones (`size == 0`)
but returns `unverified` entries — a stale-but-existing ZIP beats a cache
miss. Any `access_token=` is scrubbed from stored error reasons.

## Tile store v2 (`zenodo_tiles.py`) — Copernicus products

Second-generation layout for `ndvi`, `sar`, `worldcover`, `harmonics`
(Hansen stays on v1). v1 had to rebuild and re-upload a whole ZIP on every
flush; a transient read error during the merge could silently drop remote
tiles.

* One tile = one immutable object
  `<product>_<S>_<W>_<N>_<E>[_<year>].npz`, PUT once (idempotent). Nothing
  is rebuilt or merged.
* Sharded draft deposits keyed `(product, 0.5° lat, 1° lon)` (≤ 50 tiles
  each — Zenodo caps deposits at 100 files). Registry in
  `cache_manifest.json → shards` (`{key: {depo_id, bucket_url,
  checked_at, files}}`); every tile entry also carries its own `depo_id`
  so readers never depend on the registry.
* Parallel PUTs (`ZENODO_TILE_UPLOAD_WORKERS`, default 3); the write lock
  is only taken around shard *creation*.
* Read-through: `fetch_copernicus` → `tiles.fetch` first, then the frozen
  v1 ZIP indices. `tile_cache.has_cached` checks `tiles.has_tile` first.
* `reconcile_manifest` also runs `tiles.reconcile` (≤ 20 shard deposits
  per pass, round-robin by `checked_at`): live-but-absent → tombstone,
  tombstoned-but-present → restore, unknown deposit file → **adopt**
  (safety net for entries lost between a PUT and a manifest save).

## Reconcile manifest ↔ deposit

```bash
python3 zenodo_cache.py reconcile           # dry run, prints the diff
python3 zenodo_cache.py reconcile --apply   # takes the Zenodo write lock, writes atomically
```

Ignoring `chkpt_*` and non-`.zip` keys: live entry absent from the
deposit → tombstone; tombstoned entry present → `_probe_zip_usable`
(fresh central directory, count members, range-read the last member's
local header) → restore or keep tombstoned as corrupt; `unverified`
entries present → flag cleared and size/md5 adopted; unknown deposit
files → logged. Summary in
`data/austria_processor/cache_reconcile_last.json` (`cache_reconcile:`
line in `/process.txt`). The director runs it hourly when uploads are
enabled.

## Zenodo-degraded circuit

`zenodo_degraded()` reads `cache_manifest.json → zenodo_circuit.degraded`
(written by the director after repeated PUT failures). While set,
`upload_all` skips *replacement* uploads of ZIPs that already have a live
entry, still performs first-time uploads, and no code path tombstones.
Small ZIPs (≤ 128 MB) bypass the deferral so Hansen strips keep flushing.
KG product uploads (`zenodo_client`) are unaffected.

## Hansen coverage (`hansen_coverage.py`)

Hansen tiles are 0.5° NPZs bundled into `hansen_*.zip`. `hansen_coverage.py`
computes **needed** (every 0.5° cell intersected by a KG bbox in the
search index, with `kgs=` touching and `todo=` unprocessed) versus
**have** (cells contained by a live ZIP member) and writes
`data/austria_processor/hansen_coverage.json` (refreshed hourly,
`first_missing` persists). `/process.txt` shows the `hansen_cover:` line
and raises `HANSEN-GAPS` once a cell has been missing ≥ 24 h.

```
hansen_cover: zenodo=60/68 cells zips=11 missing=8 (oldest 3m)
    missing lon/lat(SW corner, 0.5°): 15.5/46.5(kgs=298/todo=251,3m) …
```

`todo>0` → the processor will request the tile when it reaches those KGs
(a gap lasting days means a UMD egress problem); `todo=0` → only
`product_repair` asks for it; `on-primary-disk` → the tile is local but
not flushed: `python3 hansen_coverage.py --flush`. `HansenTileCache`
writes a `.fail` sidecar after an exhausted UMD retry ladder and skips
remote retries for `HANSEN_REMOTE_FAIL_COOLDOWN` (2 h).

## Why not BEV / ortho?

BEV DTM/DSM/ortho are already COGs with efficient HTTP range reads, and
all of Austria at 1 m is ~4 TB — not something to mirror.

## Operations

```bash
python3 zenodo_cache.py status       # local + Zenodo tile counts
python3 zenodo_cache.py dry-run      # build ZIPs without uploading
python3 zenodo_cache.py upload       # upload local tiles (needs ZENODO_TOKEN)
python3 -c 'import zenodo_cache as z; print(z.ZenodoCache().tiles.status())'
python3 -c 'import zenodo_cache as z; print(z.ZenodoCache().tiles.reconcile(dry_run=True))'
# tiles in deposits no shard row knows about (should be empty):
python3 -c "
import json;m=json.load(open('data/austria_processor/cache_manifest.json'))
d={v['depo_id'] for v in m.get('shards',{}).values()}
print([k for k,v in m['files'].items() if isinstance(v,dict) and v.get('kind')=='tile' and v.get('size') and v['depo_id'] not in d])"
```

Troubleshooting: `Upload complete: 0 ZIPs, 0 tiles` is normal when the
same cells are reused across KGs or everything is already remote; tiles
not uploading → check `.meta.json` sidecars; stale indices →
`rm -rf data/austria_processor/zenodo_zip_index/`.
