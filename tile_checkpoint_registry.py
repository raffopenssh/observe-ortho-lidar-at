"""Cross-peer registry of tile-checkpoint metadata pickles.

When a peer aborts a KG mid-flight (BEV throttle, Copernicus rotation,
bandwidth wall, role eviction, …) the partially-completed tile pickles
(``data/austria_processor/tile_checkpoints/<kg>/tile_*.pkl``) carry
the full per-tile result (segmentation, classified objects, vector
building context, NDVI/SAR/Hansen extracts — *everything except the raw
DTM/DSM/ortho rasters*). They are small (KB–MB) and reusable by any
other peer picking up the same KG. Today they live only on the
aborting peer's local disk and are lost when it moves on.

This module wraps them as one gzipped tarball per KG, parks the tarball
in the shared Zenodo cache deposit, and exposes minimal upload /
download / delete operations. The same deposit already serves
``zenodo_cache`` tile bundles — we just live under a different prefix.

Raster sidecars: the default DTM/DSM (+nDSM) of each tile ride along,
losslessly packed, as far as ``MAX_BUNDLE_BYTES`` allows after the pickles
(``SHIP_RASTERS``); every tile ships at least its ``meta.json`` with the BEV
source identity so a re-read by the next peer is checked against it.
Multi-date and ortho sidecars do not travel.

Local manifest at ``data/austria_processor/checkpoint_registry.json``
tracks ``{kg_code: {ts, n_tiles, bytes, name}}``. The manifest is added
to ``director_ha.SNAPSHOT_FILES`` so the shadow director sees the same
view after takeover.

Lifecycle hooks (see austria_processor.py):

* Upload — ``main()`` after a KG is deferred / re-queued.
* Download — ``process_one_kg()`` right after the checkpoint dir is
  created and BEFORE the tile loop runs.
* Delete — right after ``_clear_tile_checkpoints()`` on successful
  completion.

Bounded eviction: at most ``MAX_REGISTRY_KGS`` entries are kept (LRU
by ``ts``). Older entries are deleted from Zenodo when a new one is
added. The fleet rarely has > ~50 KGs in-flight, so the 200-entry
default is generous headroom.
"""
from __future__ import annotations

import gzip
import io
import json
import logging
import os
import tarfile
import tempfile
import time
from pathlib import Path
from typing import Dict, Optional

_log = logging.getLogger("chkpt_registry")

DATA_DIR = Path("data/austria_processor")
REGISTRY_MANIFEST = DATA_DIR / "checkpoint_registry.json"
CKPT_ROOT = DATA_DIR / "tile_checkpoints"

# Cross-fleet visibility: mirror chkpt entries into cache_manifest.json
# (the same deposit hosts both tile-cache ZIPs and chkpt tarballs, and
# cache_manifest.json is already synced primary<->all peers every 5 min
# by app._sync_peer_data). Entries are tagged with the CHKPT_PREFIX
# filename so the primary's /process.txt + dashboard can derive a
# fleet-wide chkpt registry view without any extra network round-trips.
CACHE_MANIFEST = DATA_DIR / "cache_manifest.json"

# Zenodo-side prefix (lives in the same deposit as the tile cache).
ZENODO_PREFIX = "chkpt"

# Hard cap on number of KGs in registry (LRU on ts). The Zenodo deposit
# has a HARD 100-file limit shared with the tile-cache ZIPs (~45 files
# and growing as the cell layout fills in). Letting chkpt bundles crowd
# the deposit caused the Jul 2026 incident where a tile-cache ZIP was
# deleted-then-400'd on re-upload (deposit full) and its tiles were
# lost. Keep chkpt well under half the deposit.
MAX_REGISTRY_KGS = 30

# Never upload a NEW chkpt bundle when the deposit is already this full
# (Zenodo hard limit = 100 files). Leaves headroom for tile-cache ZIPs,
# whose delete-then-PUT re-upload cycle is NOT safe against a full
# deposit (the delete succeeds, the PUT 400s, the data is gone).
DEPOSIT_FILE_HEADROOM = 90

# Fleet-wide cap on live chkpt bundles (counted from the synced
# cache_manifest, so every peer sees the same number). MAX_REGISTRY_KGS
# above is per-peer and did NOT bound the fleet: 30 peers x 30 = the
# whole deposit. The reconciler GC (zenodo_cache.ZenodoCache.
# _gc_chkpt_bundles, CHKPT_FLEET_MAX) enforces the same number
# server-side and evicts completed / >14 d bundles.
FLEET_MAX_BUNDLES = 20


def _last_deposit_count(max_age_s: float) -> Optional[int]:
    """Deposit file count from the reconciler's last listing
    (``data/austria_processor/deposit_file_count.json``), or None if
    absent / stale."""
    try:
        p = DATA_DIR / "deposit_file_count.json"
        d = json.loads(p.read_text())
        if time.time() - float(d.get("ts", 0)) > max_age_s:
            return None
        return int(d.get("deposit_files"))
    except Exception:
        return None


# Cap per-KG bundle size. Metadata pickles should never exceed this;
# if they do, the KG has anomalous tile counts and we skip uploading.
MAX_BUNDLE_BYTES = 200 * 1024 * 1024  # 200 MB


# ----------------------------------------------------------------------
# Local manifest helpers
# ----------------------------------------------------------------------

def _load_manifest() -> Dict:
    if not REGISTRY_MANIFEST.exists():
        return {}
    try:
        return json.loads(REGISTRY_MANIFEST.read_text() or "{}")
    except Exception:
        return {}


def _save_manifest(m: Dict) -> None:
    REGISTRY_MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    tmp = REGISTRY_MANIFEST.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(m, sort_keys=True))
    tmp.rename(REGISTRY_MANIFEST)


def _mirror_to_cache_manifest(kg_code: str, name: str, size: int,
                              n_tiles: int, deleted: bool = False) -> None:
    """Mirror a chkpt entry into cache_manifest.json.

    Hooks into the existing primary<->peers cache_manifest sync (5-min
    cadence in app._sync_peer_data). Deletes are propagated as
    size=0/tile_count=0 tombstones — the sync's last-writer-wins merge
    on updated_at handles eviction naturally. The dashboard /process.txt
    filters tombstones (size==0).
    """
    try:
        from zenodo_cache import CacheManifest
        cm = CacheManifest()
        now_iso = (
            __import__("datetime").datetime.now(
                __import__("datetime").timezone.utc).isoformat())
        cm.set_file(
            zip_name=name,
            url="",  # chkpt entries are not consumed via URL on peers
            size=0 if deleted else int(size),
            checksum="",
            tile_count=0 if deleted else int(n_tiles),
            updated_at=now_iso,
        )
        cm.save()
    except Exception as e:  # never block upload/download on mirror failure
        _log.debug("chkpt: mirror to cache_manifest skipped for %s: %s",
                   kg_code, e)


def _bundle_name(kg_code: str) -> str:
    safe = "".join(c for c in kg_code if c.isalnum() or c in "-_")
    return f"{ZENODO_PREFIX}_{safe}.tar.gz"


# ----------------------------------------------------------------------
# Bundling
# ----------------------------------------------------------------------

# Raster sidecars in the bundle (docs/qc-lessons.md → pos_shift): the default
# DTM/DSM of a tile ride along so the peer that picks the KG up stitches the
# SAME rasters the tiles were segmented from instead of re-reading BEV (a
# second read is where the 5 m shift lived, and BEV may serve other data by
# then).  Lossless (uint32 row delta + byte shuffle + zlib, ~3-4x on terrain).
# Pickles have priority: rasters only fill what the pickles leave of
# MAX_BUNDLE_BYTES, tile by tile; a tile that does not fit still ships its
# ``meta.json`` (transform + BEV source identity) so the re-read is checked.
# Multi-date and ortho sidecars never travel (re-read on the tile grid).
SHIP_RASTERS = os.environ.get("CHKPT_SHIP_RASTERS", "1") != "0"
RASTER_MARGIN_BYTES = 4 * 1024 * 1024
_F32Z = b"F32Z1"


def _f32z_encode(a) -> bytes:
    import zlib
    import numpy as np
    u = np.ascontiguousarray(a, dtype="<f4").view("<u4")
    d = u.copy()
    d[:, 1:] -= u[:, :-1]
    body = np.ascontiguousarray(d.view(np.uint8).reshape(-1, 4).T).tobytes()
    return _F32Z + np.array(u.shape, "<u4").tobytes() + zlib.compress(body, 6)


def _f32z_decode(blob: bytes):
    import zlib
    import numpy as np
    if blob[:5] != _F32Z:
        raise ValueError("not an F32Z1 raster")
    rows, cols = (int(x) for x in np.frombuffer(blob[5:13], "<u4"))
    b = np.frombuffer(zlib.decompress(blob[13:]), np.uint8).reshape(4, rows * cols)
    d = np.ascontiguousarray(b.T).view("<u4").reshape(rows, cols)
    return np.cumsum(d, axis=1, dtype=np.uint32).view("<f4")


def _derive_ndsm(dtm, dsm, nd_valid):
    import numpy as np
    with np.errstate(invalid="ignore"):
        nd = np.where(nd_valid, dsm - dtm, np.nan).astype(np.float32)
        return np.where((nd < 0) & nd_valid, np.float32(0.0), nd).astype(np.float32)


def _raster_members(kg_dir: Path, tile_no: str, with_rasters: bool) -> Optional[Dict[str, bytes]]:
    """Bundle members of one tile's raster sidecar: always ``meta.json``;
    with *with_rasters* also dtm / dsm (+ the nDSM validity bits, or the nDSM
    itself when it is not exactly max(dsm - dtm, 0))."""
    import zlib
    import numpy as np
    rdir = kg_dir / f"tile_{tile_no}" / "raster"
    try:
        meta = json.loads((rdir / "meta.json").read_text())
    except Exception:
        return None
    out_meta = {k: meta[k] for k in ("transform", "shape", "bev_source") if meta.get(k)}
    out_meta["layers"] = {}
    pre = f"tile_{tile_no}/raster/"
    out: Dict[str, bytes] = {}
    if with_rasters and (meta.get("layers") or {}).get("default"):
        try:
            dtm = np.load(str(rdir / "dtm.npy"), mmap_mode="r")
            dsm = np.load(str(rdir / "dsm.npy"), mmap_mode="r")
            ndsm = np.load(str(rdir / "ndsm.npy"), mmap_mode="r")
            if dtm.ndim == 2 and dtm.shape == dsm.shape == ndsm.shape:
                ndv = np.isfinite(ndsm)
                out[pre + "dtm.f32z"] = _f32z_encode(dtm)
                out[pre + "dsm.f32z"] = _f32z_encode(dsm)
                if np.array_equal(_derive_ndsm(np.asarray(dtm), np.asarray(dsm), ndv), ndsm, equal_nan=True):
                    out[pre + "ndsm_valid.bitz"] = zlib.compress(np.packbits(ndv).tobytes(), 6)
                else:
                    out[pre + "ndsm.f32z"] = _f32z_encode(ndsm)
                out_meta["layers"] = {"default": True}
        except Exception as e:  # noqa: BLE001
            _log.info("chkpt raster pack tile_%s failed: %s", tile_no, e)
            out = {}
            out_meta["layers"] = {}
    out[pre + "meta.json"] = json.dumps(out_meta).encode()
    return out


def _tar_gz(members) -> bytes:
    """members: iterable of (arcname, Path | bytes)."""
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        with tarfile.open(fileobj=gz, mode="w") as tar:
            for name, src in members:
                if isinstance(src, (bytes, bytearray)):
                    ti_ = tarfile.TarInfo(name)
                    ti_.size = len(src)
                    tar.addfile(ti_, io.BytesIO(src))
                else:
                    tar.add(str(src), arcname=name)
    return buf.getvalue()


def _bundle_tile_pickles(kg_code: str) -> Optional[bytes]:
    """Tar+gzip all tile_*.pkl files for kg_code into an in-memory blob,
    plus the tiles' default raster sidecars as far as the size cap allows.

    Returns None if there are no pickles to bundle or the pickles alone
    exceed ``MAX_BUNDLE_BYTES``.
    """
    kg_dir = CKPT_ROOT / kg_code
    if not kg_dir.exists():
        return None
    pkls = sorted(p for p in kg_dir.iterdir()
                  if p.is_file() and p.name.startswith("tile_")
                  and p.name.endswith(".pkl"))
    if not pkls:
        return None
    base = [(p.name, p) for p in pkls]
    data = _tar_gz(base)
    if len(data) > MAX_BUNDLE_BYTES:
        # Routine on big v2.x parents (a third of the fleet's KGs, Oct 2026):
        # the metadata pickles of a 20-tile KG gzip to 200–450 MB.  Info,
        # not warning — at warning it tagged every such KG's gpkg_light
        # step "completed with warnings" and drowned the real signals.
        _log.info("chkpt bundle for %s exceeds %d MB (%d MB) — skipping upload (local checkpoints still serve same-peer retries)",
                     kg_code, MAX_BUNDLE_BYTES // (1024*1024), len(data) // (1024*1024))
        return None
    if not SHIP_RASTERS:
        return data
    try:
        budget = MAX_BUNDLE_BYTES - len(data) - RASTER_MARGIN_BYTES
        extra, n_ras, n_meta = [], 0, 0
        for p in pkls:
            tno = p.name[len("tile_"):-len(".pkl")]
            mem = _raster_members(kg_dir, tno, with_rasters=budget > 0)
            if mem is None:
                continue
            size = sum(len(v) for v in mem.values())
            if size > budget and len(mem) > 1:
                mem = _raster_members(kg_dir, tno, with_rasters=False) or {}
                size = sum(len(v) for v in mem.values())
            if len(mem) > 1:
                n_ras += 1
            else:
                n_meta += 1
            budget -= size
            extra.extend(sorted(mem.items()))
        if not extra:
            return data
        full = _tar_gz(base + extra)
        if len(full) > MAX_BUNDLE_BYTES:
            return data
        _log.info("chkpt bundle for %s: %d tile raster sidecar(s) shipped, %d identity-only (%.1f MB pickles → %.1f MB)",
                  kg_code, n_ras, n_meta, len(data) / 1e6, len(full) / 1e6)
        return full
    except Exception as e:  # noqa: BLE001 — rasters are an add-on, never lose the pickles
        _log.warning("chkpt bundle for %s: raster sidecars skipped (%s)", kg_code, e)
        return data


def _install_raster(kg_dir: Path, tile_no: str, files: Dict[str, bytes]) -> bool:
    """Write one tile's shipped sidecar into ``tile_<N>/raster/`` (never over
    an existing local one).  True when dtm/dsm/ndsm were installed."""
    import zlib
    import numpy as np
    import tile_raster_sidecar as trs
    rdir = kg_dir / f"tile_{tile_no}" / "raster"
    if (rdir / "meta.json").exists() or "meta.json" not in files:
        return False
    meta = json.loads(files["meta.json"])
    has = bool((meta.get("layers") or {}).get("default")) and "dtm.f32z" in files and "dsm.f32z" in files
    if has and not trs._enough_disk():
        has = False
    rdir.mkdir(parents=True, exist_ok=True)
    if has:
        dtm = _f32z_decode(files["dtm.f32z"]); dsm = _f32z_decode(files["dsm.f32z"])
        if "ndsm.f32z" in files:
            ndsm = _f32z_decode(files["ndsm.f32z"])
        else:
            ndv = np.unpackbits(np.frombuffer(zlib.decompress(files["ndsm_valid.bitz"]), np.uint8),
                                count=dtm.size).reshape(dtm.shape).astype(bool)
            ndsm = _derive_ndsm(dtm, dsm, ndv)
        trs._atomic_save_npy(rdir / "dtm.npy", dtm)
        trs._atomic_save_npy(rdir / "dsm.npy", dsm)
        trs._atomic_save_npy(rdir / "ndsm.npy", ndsm)
    meta["layers"] = {"default": True} if has else {}
    meta["shipped"] = True
    tmp = rdir / "meta.json.tmp"
    tmp.write_text(json.dumps(meta))
    tmp.rename(rdir / "meta.json")
    return has


LAST_UNPACK: Dict = {}       # {"rasters": n, "identity": n} of the last _unpack_bundle


def _unpack_bundle(blob: bytes, kg_code: str) -> int:
    """Extract tile_*.pkl entries (and shipped raster sidecars) into
    tile_checkpoints/<kg>/.

    Skips any tile pickle already present locally (don't clobber a
    freshly-written checkpoint). Returns count of pickles installed.
    """
    kg_dir = CKPT_ROOT / kg_code
    kg_dir.mkdir(parents=True, exist_ok=True)
    installed = 0
    LAST_UNPACK.clear(); LAST_UNPACK.update(rasters=0, identity=0)
    try:
        buf = io.BytesIO(blob)
        new_tiles = set()
        with gzip.GzipFile(fileobj=buf, mode="rb") as gz:
            with tarfile.open(fileobj=gz, mode="r") as tar:
                cur_no, cur = None, {}

                def _flush():
                    if cur_no is not None and cur and cur_no in new_tiles:
                        try:
                            if _install_raster(kg_dir, cur_no, cur):
                                LAST_UNPACK["rasters"] += 1
                            else:
                                LAST_UNPACK["identity"] += 1
                        except Exception as e:  # noqa: BLE001
                            _log.info("chkpt raster install tile_%s failed: %s", cur_no, e)

                for member in tar:
                    parts = member.name.split("/")
                    name = parts[-1]
                    if len(parts) == 3 and parts[1] == "raster" and parts[0].startswith("tile_") \
                            and parts[0][5:].isdigit():
                        # members of one tile are contiguous and follow all pickles
                        if parts[0][5:] != cur_no:
                            _flush()
                            cur_no, cur = parts[0][5:], {}
                        fobj = tar.extractfile(member)
                        if fobj is not None and name in ("meta.json", "dtm.f32z", "dsm.f32z",
                                                         "ndsm.f32z", "ndsm_valid.bitz"):
                            cur[name] = fobj.read()
                        continue
                    if len(parts) != 1 or not (name.startswith("tile_") and name.endswith(".pkl")):
                        continue
                    dst = kg_dir / name
                    if dst.exists():
                        continue
                    fobj = tar.extractfile(member)
                    if fobj is None:
                        continue
                    tmp = kg_dir / (name + ".tmp")
                    with open(tmp, "wb") as f:
                        f.write(fobj.read())
                    tmp.rename(dst)
                    installed += 1
                    new_tiles.add(name[5:-4])
                _flush()
    except Exception as e:
        _log.warning("unpack chkpt bundle for %s failed: %s", kg_code, e)
    return installed


# ----------------------------------------------------------------------
# Zenodo upload/download (uses the cache deposit)
# ----------------------------------------------------------------------

def _get_cache() -> "object | None":
    try:
        import config as _config
        if not _config.uploads_enabled():
            # No Zenodo token / --no-upload: the registry is local-only;
            # never contact Zenodo (every caller treats None as "skip").
            return None
        from zenodo_cache import ZenodoCache
        return ZenodoCache()
    except Exception as e:
        _log.warning("chkpt_registry: cannot init ZenodoCache: %s", e)
        return None


def _upload_blob_to_deposit(cache, name: str, blob: bytes) -> bool:
    """Write blob to a temp file and upload via the cache deposit's bucket."""
    try:
        depo_id = cache._ensure_deposit()
    except Exception as e:
        _log.warning("chkpt_registry: ensure_deposit failed: %s", e)
        return False
    with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tf:
        tf.write(blob)
        tmp_path = Path(tf.name)
    try:
        cache._upload_file(depo_id, tmp_path, name)
        return True
    except Exception as e:
        _log.warning("chkpt_registry: upload %s failed: %s", name, e)
        return False
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass


def _delete_from_deposit(cache, name: str) -> bool:
    try:
        depo_id = cache._ensure_deposit()
        cache._delete_file(depo_id, name)
        return True
    except Exception as e:
        # 404 is common (already evicted) — quiet at debug.
        _log.info("chkpt_registry: delete %s skipped: %s", name, e)
        return False


def _download_from_deposit(cache, name: str) -> Optional[bytes]:
    """Stream a file out of the cache deposit. Returns bytes or None."""
    try:
        depo_id = cache._ensure_deposit()
        # Use deposition files API — returns the bucket download URL.
        r = cache._api("GET", f"/api/deposit/depositions/{depo_id}/files")
        for f in r.json():
            if f.get("filename") == name:
                links = f.get("links") or {}
                url = links.get("download") or links.get("self")
                if not url:
                    return None
                # Re-use cache's _session for keep-alive
                sess = cache._session
                rr = sess.get(url, params={"access_token": cache.token},
                              timeout=120)
                rr.raise_for_status()
                return rr.content
        return None
    except Exception as e:
        _log.info("chkpt_registry: download %s failed: %s", name, e)
        return None


# ----------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------

def upload_kg(kg_code: str) -> Optional[Dict]:
    """Bundle this KG's tile pickles and upload to Zenodo registry.

    Idempotent: if the manifest already records the KG and the local
    pickles haven't changed in count, skip the re-upload. Acquires the
    fleet zenodo upload lock for the duration of the PUT only.

    Returns ``{ok, n_tiles, bytes, name, skipped}`` on success/skip,
    or ``None`` if nothing was bundled / upload failed.
    """
    blob = _bundle_tile_pickles(kg_code)
    if blob is None:
        return None
    n_tiles = sum(1 for p in (CKPT_ROOT / kg_code).iterdir()
                  if p.is_file() and p.name.startswith("tile_")
                  and p.name.endswith(".pkl"))
    m = _load_manifest()
    existing = m.get(kg_code)
    name = _bundle_name(kg_code)
    if existing and existing.get("n_tiles") == n_tiles \
            and existing.get("bytes") == len(blob):
        # Already on Zenodo; refresh cache_manifest mirror so the
        # primary sees it even if its sync missed our last upload tick.
        _mirror_to_cache_manifest(kg_code, name, len(blob), n_tiles)
        return {"ok": True, "n_tiles": n_tiles, "bytes": len(blob),
                "name": name, "skipped": True}
    cache = _get_cache()
    if cache is None:
        return None
    # Deposit-fullness guard: the shared Zenodo deposit has a hard
    # 100-file cap. If the (fleet-synced) cache manifest shows we're
    # near it, refuse NEW bundles — a full deposit breaks the tile
    # cache's delete-then-PUT re-upload cycle (data loss, Jul 2026).
    # Re-uploads of an existing bundle are fine (net file count 0).
    if not existing:
        try:
            from zenodo_cache import CacheManifest
            cm = CacheManifest()
            all_files = cm.all_files() or {}
            # Main-deposit files only (store-v2 shard objects carry a
            # depo_id and don't count against this deposit's 100 cap).
            live = cm.main_deposit_live_count()
            live_chkpt = sum(1 for _n, _e in all_files.items()
                             if _n.startswith(ZENODO_PREFIX + "_")
                             and (_e or {}).get("size", 0) > 0
                             and not (_e or {}).get("depo_id"))
            # The manifest's live count UNDERCOUNTS the deposit: orphaned
            # chkpt tars (delete failed / other peer finished the KG) stay
            # in the bucket while tombstoned here. Sep 2026: manifest said
            # 84 live, the deposit had 100 files and every new cell
            # upload 400'd. Prefer the reconciler's last real listing
            # (synced to peers via cache_manifest? no — it's primary-local,
            # so peers fall back to the manifest count) when fresh.
            observed = _last_deposit_count(max_age_s=4 * 3600)
            if observed is not None:
                live = max(live, observed)
            if live >= DEPOSIT_FILE_HEADROOM:
                _log.warning(
                    "chkpt_registry: deposit near file cap (%d >= %d); "
                    "skipping upload of %s", live, DEPOSIT_FILE_HEADROOM,
                    name)
                return None
            if live_chkpt >= FLEET_MAX_BUNDLES:
                _log.warning(
                    "chkpt_registry: fleet already holds %d chkpt bundles "
                    "(cap %d); skipping upload of %s", live_chkpt,
                    FLEET_MAX_BUNDLES, name)
                return None
        except Exception:
            pass
    # Per-bundle fleet lock — keep critical section minimal.
    try:
        from zenodo_lock import zenodo_upload_lock
        with zenodo_upload_lock(purpose=f"chkpt_upload:{kg_code}"):
            ok = _upload_blob_to_deposit(cache, name, blob)
    except Exception as e:
        _log.warning("chkpt_registry: lock acquire for %s failed: %s", kg_code, e)
        return None
    if not ok:
        return None
    m[kg_code] = {
        "ts": time.time(),
        "n_tiles": n_tiles,
        "bytes": len(blob),
        "name": name,
    }
    # LRU eviction: keep at most MAX_REGISTRY_KGS entries.
    if len(m) > MAX_REGISTRY_KGS:
        ordered = sorted(m.items(), key=lambda kv: kv[1].get("ts", 0))
        n_drop = len(m) - MAX_REGISTRY_KGS
        for kg, _info in ordered[:n_drop]:
            try:
                _delete_from_deposit(cache, _bundle_name(kg))
            except Exception:
                pass
            m.pop(kg, None)
    _save_manifest(m)
    _mirror_to_cache_manifest(kg_code, name, len(blob), n_tiles)
    _log.info("chkpt_registry: uploaded %s (%d tiles, %.1f MB)",
              kg_code, n_tiles, len(blob) / 1e6)
    return {"ok": True, "n_tiles": n_tiles, "bytes": len(blob),
            "name": name, "skipped": False}


def download_kg(kg_code: str) -> Dict:
    """If the registry has this KG, download + extract pickles locally.

    Returns ``{n, bytes}`` (n=tile pickles installed, bytes=bundle size).
    Falls back to the fleet-wide cache_manifest mirror when the local
    chkpt manifest is empty — a fresh peer never authored an upload
    so its local registry is empty, but the cache_manifest sync gives
    it the full fleet view of available bundles.
    """
    m = _load_manifest()
    name = _bundle_name(kg_code)
    have_locally = kg_code in m
    have_in_mirror = False
    if not have_locally:
        try:
            from zenodo_cache import CacheManifest
            mirror = CacheManifest().get_file(name)
            have_in_mirror = bool(mirror and mirror.get("size"))
        except Exception:
            have_in_mirror = False
    if not (have_locally or have_in_mirror):
        return {"n": 0, "bytes": 0}
    cache = _get_cache()
    if cache is None:
        return {"n": 0, "bytes": 0}
    blob = _download_from_deposit(cache, name)
    if not blob:
        return {"n": 0, "bytes": 0}
    n = _unpack_bundle(blob, kg_code)
    if n:
        _log.info("chkpt_registry: restored %d tile pickles for %s from Zenodo",
                  n, kg_code)
    return {"n": n, "bytes": len(blob), "rasters": LAST_UNPACK.get("rasters", 0),
            "identity": LAST_UNPACK.get("identity", 0)}


def delete_kg(kg_code: str) -> bool:
    """Drop a KG from the registry (call on successful completion).

    Drops both the local registry entry and the Zenodo bundle, and
    writes a size=0 tombstone into cache_manifest.json so other peers
    drop their mirror view on the next sync tick.
    """
    m = _load_manifest()
    entry = m.pop(kg_code, None)
    # Always tombstone the cache_manifest mirror — a fresh peer that
    # consumed the bundle via the mirror (no local registry entry)
    # still owes the fleet a deletion signal.
    name = (entry.get("name") if entry else None) or _bundle_name(kg_code)
    _mirror_to_cache_manifest(kg_code, name, 0, 0, deleted=True)
    if entry is not None:
        _save_manifest(m)
    cache = _get_cache()
    if cache is None:
        return False
    try:
        from zenodo_lock import zenodo_upload_lock
        with zenodo_upload_lock(purpose=f"chkpt_delete:{kg_code}"):
            ok = _delete_from_deposit(cache, name)
        if ok:
            _log.info("chkpt_registry: deleted Zenodo bundle %s", name)
        return ok
    except Exception as e:
        _log.info("chkpt_registry: delete lock failed for %s: %s", kg_code, e)
        return False


def stats() -> Dict:
    """Return small dict for /process.txt: kgs, bytes_total, oldest_age_s."""
    m = _load_manifest()
    # Drop local entries whose fleet mirror is tombstoned (reconciler GC /
    # another peer deleted the bundle) — the local file is only ever
    # written by upload/delete on THIS peer, so it goes stale otherwise.
    try:
        from zenodo_cache import CacheManifest
        cm_files = CacheManifest().all_files() or {}
        stale = [kg for kg, e in m.items()
                 if (cm_files.get(e.get("name") or _bundle_name(kg)) or {}).get("size", 1) == 0]
        if stale:
            for kg in stale:
                m.pop(kg, None)
            _save_manifest(m)
    except Exception:
        pass
    if not m:
        return {"kgs": 0, "bytes": 0, "oldest_age_s": 0}
    now = time.time()
    return {
        "kgs": len(m),
        "bytes": sum(int(e.get("bytes", 0)) for e in m.values()),
        "oldest_age_s": now - min(float(e.get("ts", now)) for e in m.values()),
    }
