"""ne_h3 — NE cells (H3) for the v2.4 product.  See docs/ne-v24-design.md and
the umfeld-at contract (``/k/api/v1/ne/draft/ne-cells.md``, algo ``ne-cells-2``).

What lives here
---------------
* Container I/O byte-compatible with umfeld-at's ``ne_cells/pack.py``
  (``NEC1`` declared layer, ``NEO1`` observed layer; ``NET1``/``NEB1`` are ours only).
* ``fetch_declared(kg)`` — GET the frozen declared layer from umfeld-at
  (``?layers=lu``), or build it locally from a bevdirect-serve tile assembly
  with umfeld's pinned ``ne_cells`` package (``NE_SOURCE=bevdirect``).
* ``build_observed(...)`` — NEO1 (29 B/cell), NET1 (4 B/tree), NEB1 (8 B/object)
  from the stitched 1 m ``segment_type`` / nDSM arrays, the segment objects and
  the v2.1 apex inventory.  Verdict per cell = declared vs observed.
* ``write_h3b`` / ``read_h3b`` — the on-disk + Zenodo product ``<kg>_ne.h3b``:
  JSON header + independently deflated **res-10 chunks** (NEC1 | NEO1 | NET1 |
  NEB1 rows of that chunk, all sorted by cell id → concatenating the chunks
  reproduces every layer's digest).
* ``post_observed(kg, neo1_section)`` — round-trip to umfeld-at (``/obs``).

Nothing in this module touches a parcel, a footprint, a GNR or an EZ.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import struct
import subprocess
import time
import zlib
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

log = logging.getLogger("ne_h3")

# === SECTION: constants / record layouts ===

FMT_VER = 1
MAGIC_LU = b"NEC1"
MAGIC_OBS = b"NEO1"
MAGIC_TREES = b"NET1"
MAGIC_BLD = b"NEB1"
H3B_MAGIC = b"H3B1"

ALGO_OBS = "obs-cells-1"
ALGO_TREES = "obs-trees-1"
ALGO_BLD = "obs-bld-1"
ALGO_DECLARED = "ne-cells-2"          # the frozen umfeld algo we accept
OBS_SOURCE = "srtm-lidar-at@2.4"
OBS_SOURCE_FIXED = "srtm-lidar-at@2.4.1"   # NEO1 export with the green-ground override applied (ne_green_fix; epoch unchanged)
H3_RES = 12
CHUNK_RES = 10

GROUPS = ("geb", "bau", "acker", "gruen", "wald", "wasser", "verkehr", "alpen", "sonst")

# umfeld layouts (pack.py) — verified sizes 30 / 29
LU_REC = struct.Struct("<QBBBB9sBHBBHBB")
K_REC = struct.Struct("<QHIIIHBHH")
# ours
# NEO1 row, 40 B.  umfeld's spec text lists the first 14 fields after the id
# (its "29 B" is a miscount: that is 30 B); the server only requires fixed-size
# rows 9..64 B with the cell id first and passes them through, so we append the
# terrain / change / phenology fields that used to live per parcel (v2.3
# ``parcels.details``) — nothing of that insight is lost at the cell level.
OBS_REC = struct.Struct("<Q9sBBBBBBBBBBbBB" "HBBBBBbbBB")
OBS_FIELDS = ["cell_id", "obs_lu[9]", "obs_bld_cover", "obs_bld_n", "h_mean", "h_p90", "h_max",
              "tree_n", "tree_h_max", "crown_frac", "ndvi_mean", "ndvi_amp", "dh_mean",
              "flight_years", "verdict",
              # 2.4 terrain + change + phenology (offsets 30..39)
              "elev_mean", "slope_mean", "slope_p90", "aspect_dom", "roughness", "tri", "tpi",
              "dtm_change", "hansen_loss_year", "phenology"]
OBS_UNITS = {"h_mean/h_p90/h_max/tree_h_max": "0.5 m (255 nodata)", "crown_frac": "/254",
             "ndvi_mean": "(v+1)/2*254 (255 nodata)", "ndvi_amp": "*254 (255 nodata)",
             "dh_mean": "nDSM change newest-oldest, 0.25 m i8 (-128 nodata)",
             "flight_years": f"bit i = ALS {2020}+i",
             "elev_mean": "m a.s.l. u16 (65535 nodata)", "slope_mean/slope_p90": "deg (255 nodata)",
             "aspect_dom": "0=N 1=NE 2=E 3=SE 4=S 5=SW 6=W 7=NW 8=flat (255 nodata)",
             "roughness": "DSM local std *10 (255 nodata)",
             "tri": "terrain ruggedness index (mean |dz| to neighbours) m *10 (255 nodata)",
             "tpi": "topographic position index (r=5) m *4 i8 (-128 nodata)",
             "dtm_change": "terrain change newest-oldest, 0.25 m i8 (-128 nodata)",
             "hansen_loss_year": "0 none, else year-2000 (255 nodata)",
             "phenology": "index into phenology[] (255 nodata)"}
PHENOLOGY_CODES = ("unknown", "forest", "crop", "pasture", "seasonal_vegetation", "road_or_bare")
_PHENO_IDX = {c: i for i, c in enumerate(PHENOLOGY_CODES)}
ASPECT_FLAT = 8
TREE_REC_BYTES = 4
BLD_REC_BYTES = 8
assert LU_REC.size == 30 and K_REC.size == 29 and OBS_REC.size == 41

VERDICTS = ("consistent", "forest_loss", "forest_gain", "sealed_undeclared",
            "building_undeclared", "green_undeclared", "unknown")
V_CONSISTENT, V_FOREST_LOSS, V_FOREST_GAIN, V_SEALED, V_BUILDING, V_GREEN, V_UNKNOWN = range(7)

# segment type code → NE group index (frozen with ALGO_OBS)
_TYPE_GROUP = {
    "roof": "geb", "greenhouse": "geb", "solar_panel": "geb",
    "construction": "bau", "earthwork": "bau", "excavation": "bau", "fill": "bau", "parking": "bau",
    "crop": "acker",
    "grass": "gruen", "garden": "gruen", "orchard": "gruen", "vineyard": "gruen",
    "shrub": "gruen", "hedge": "gruen",
    "tree": "wald", "tree_loss": "wald",
    "water": "wasser", "wetland": "wasser",
    "road": "verkehr", "path": "verkehr", "bridge": "verkehr", "rail": "verkehr",
    "rock": "alpen", "bare_soil": "alpen", "glacier": "alpen",
}
# NEB1 ``type`` nibble
BLD_TYPES = ("roof", "greenhouse", "solar_panel", "mast", "wind_turbine", "substation",
             "bridge", "wall", "fence")
_BLD_TYPE_IDX = {t: i for i, t in enumerate(BLD_TYPES)}
SPECIES_CODES = ("unknown", "spruce", "fir", "pine", "larch", "beech", "oak", "maple",
                 "ash", "birch", "alder", "poplar", "willow", "fruit", "conifer", "broadleaf")
_SPECIES_IDX = {s: i for i, s in enumerate(SPECIES_CODES)}
VITALITY_CODES = ("unknown", "vital", "normal", "stressed", "declining", "dead")
_VITALITY_IDX = {s: i for i, s in enumerate(VITALITY_CODES)}

NODATA_U8 = 255
NODATA_I8 = -128
FLIGHT_YEAR0 = 2020          # flight_years bit i ⇔ ALS year 2020+i

DATA_DIR = Path(os.environ.get("NE_DATA_DIR") or (Path(__file__).resolve().parent / "data"))
NE_DIR = DATA_DIR / "ne"


class NEError(Exception):
    pass


class NENotBuilt(NEError):
    """umfeld-at has not built the declared layer for this KG yet (404).
    ``retry_after_s`` honours the server's Retry-After; ``status`` is one of
    queued / stalled / failed / not_in_index."""

    def __init__(self, kg: str, status: str, retry_after_s: int | None, body: dict | None = None):
        super().__init__(f"NE declared layer for {kg} not built: {status} (retry_after={retry_after_s})")
        self.kg, self.status, self.retry_after_s, self.body = kg, status, retry_after_s, body or {}


# === SECTION: container I/O (umfeld-compatible) ===

def canonical_json(obj) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def digest_of(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()[:16]


class Section:
    __slots__ = ("magic", "header", "raw")

    def __init__(self, magic: bytes, header: dict, raw: bytes):
        self.magic, self.header, self.raw = magic, header, raw

    @property
    def digest(self) -> str:
        return self.header.get("digest", "")

    def cell_ids(self) -> np.ndarray:
        """uint64 cell ids of the fixed-size rows (NEC1: the LU rows only)."""
        if self.magic == MAGIC_LU:
            n = int(self.header["cells_n"])
            rec = LU_REC.size
        elif self.magic == MAGIC_OBS:
            n = int(self.header["cells_n"])
            rec = OBS_REC.size
        else:
            raise ValueError("cell_ids: only NEC1/NEO1 carry one row per cell")
        buf = np.frombuffer(self.raw, dtype=np.uint8, count=n * rec).reshape(n, rec)
        return buf[:, :8].copy().view("<u8").reshape(n)

    def lu_table(self) -> dict:
        """NEC1 → columnar numpy view of the LU rows."""
        if self.magic != MAGIC_LU:
            raise ValueError("lu_table: NEC1 only")
        n = int(self.header["cells_n"])
        b = np.frombuffer(self.raw, dtype=np.uint8, count=n * LU_REC.size).reshape(n, LU_REC.size)
        return {
            "cell_id": b[:, :8].copy().view("<u8").reshape(n),
            "kg_share": b[:, 8], "land": b[:, 9], "cov": b[:, 10], "flags": b[:, 11],
            "lu": b[:, 12:21], "bld_cover": b[:, 21],
            "bld_n": b[:, 22:24].copy().view("<u2").reshape(n),
            "gk": b[:, 24], "n_parc": b[:, 25],
            "line_m_ha": b[:, 26:28].copy().view("<u2").reshape(n),
            "orient_coh": b[:, 28], "orient_axis": b[:, 29],
        }


def pack_section(magic: bytes, header: dict, raw: bytes, level: int = 9) -> bytes:
    hdr = dict(header)
    hdr["digest"] = digest_of(raw)
    hdr["fmt_ver"] = FMT_VER
    hdr["magic"] = magic.decode()
    hj = canonical_json(hdr)
    body = zlib.compress(raw, level)
    return magic + bytes([FMT_VER]) + struct.pack("<I", len(hj)) + hj + struct.pack("<I", len(body)) + body


def unpack_sections(buf: bytes) -> list[Section]:
    out, off = [], 0
    while off < len(buf):
        if len(buf) - off < 9:
            raise ValueError("truncated section header")
        magic = buf[off:off + 4]
        if buf[off + 4] != FMT_VER:
            raise ValueError(f"unsupported fmt_ver {buf[off + 4]}")
        (hl,) = struct.unpack_from("<I", buf, off + 5)
        off += 9
        header = json.loads(buf[off:off + hl].decode("utf-8"))
        off += hl
        (bl,) = struct.unpack_from("<I", buf, off)
        off += 4
        raw = zlib.decompress(buf[off:off + bl])
        off += bl
        if header.get("digest") != digest_of(raw):
            raise ValueError(f"{magic!r}: digest mismatch")
        out.append(Section(magic, header, raw))
    return out


def h3_parent(cid: int, res: int) -> int:
    """Pure-int H3 parent (h3 v4 bit layout) — identical to umfeld's pack.h3_parent."""
    cur = (cid >> 52) & 0xF
    if res >= cur:
        return cid
    out = (cid & ~(0xF << 52)) | (res << 52)
    for d in range(res + 1, 16):
        out |= 0x7 << ((15 - d) * 3)
    return out


def h3_parent_vec(cids: np.ndarray, res: int) -> np.ndarray:
    c = cids.astype(np.uint64)
    cur = (c >> np.uint64(52)) & np.uint64(0xF)
    out = (c & ~np.uint64(0xF << 52)) | np.uint64(res << 52)
    fill = np.uint64(0)
    for d in range(res + 1, 16):
        fill |= np.uint64(0x7 << ((15 - d) * 3))
    out = out | fill
    return np.where(cur <= res, c, out)


def h3hex(cid: int) -> str:
    return format(int(cid), "x")


# === SECTION: declared layer — umfeld-at or local bevdirect ===

def ne_source() -> str:
    """``umfeld`` (default: GET the frozen index epoch from umfeld-at) or
    ``bevdirect`` (compute from a local bevdirect-serve tile assembly).  The
    director pushes this per start payload → env ``NE_SOURCE``; a sidecar
    file ``data/austria_processor/ne_source`` wins over the env so a running
    fleet can be switched without a restart."""
    try:
        p = DATA_DIR / "austria_processor" / "ne_source"
        if p.exists():
            v = p.read_text().strip().lower()
            if v in ("umfeld", "bevdirect"):
                return v
    except Exception:
        pass
    v = (os.environ.get("NE_SOURCE") or "umfeld").strip().lower()
    return v if v in ("umfeld", "bevdirect") else "umfeld"


def _umfeld_base() -> str:
    """Public (tokenless) NE read tier — ``/ne/manifest``, ``/ne/{kg}?layers=``.
    Our token is a contributor token since 2026-10-23: every ``/k/…`` path
    answers 404 for it (umfeld.py)."""
    import umfeld
    return umfeld.NE_PUBLIC_BASE


def _umfeld_contrib_base() -> str:
    """``/contrib/api/v1/ne`` — the write tier (``/obs``, ``/report``, ``/chunks``)."""
    import umfeld
    return umfeld.NE_CONTRIB_BASE


def _umfeld_headers(auth: bool = True) -> dict:
    import umfeld
    h = umfeld.headers() if auth else {}
    h["User-Agent"] = "srtm-lidar-at/2.4 ne_h3"
    return h


def fetch_manifest(timeout: float = 60.0, etag: str | None = None,
                   since: str | None = None) -> tuple[dict | None, str | None]:
    """GET /ne/manifest (gzip + ETag).  Returns (manifest or None on 304, etag).

    With *since* (= ``generated_at`` of the previous answer) umfeld returns a
    DELTA — ``{delta:true, kgs:{only kgs with updated_at >= since}, built,
    kgs_total, obs_posted, generated_at, kgs_changed}`` (no queue/frozen);
    a cursor older than 7 d / in the future yields the full body with
    ``delta:false``.  Callers must merge/replace accordingly (see
    ``ne_rollout.refresh_ne_manifest``).  400 = unparseable cursor."""
    import requests
    h = _umfeld_headers(auth=False)
    h["Accept-Encoding"] = "gzip"
    if etag:
        h["If-None-Match"] = etag
    params = {"since": since} if since else None
    r = requests.get(_umfeld_base() + "/manifest", params=params, headers=h, timeout=timeout)
    if r.status_code == 304:
        return None, etag
    r.raise_for_status()
    return r.json(), r.headers.get("ETag")


def fetch_declared_umfeld(kg: str, timeout: float = 120.0, layer: str = "lu") -> Section:
    """GET /ne/<kg>?layers=lu → verified NEC1 section.  404 → NENotBuilt."""
    import requests
    kg = str(kg).zfill(5)
    url = f"{_umfeld_base()}/{kg}"
    last = None
    for attempt in range(3):
        try:
            r = requests.get(url, params={"layers": layer}, headers=_umfeld_headers(auth=False), timeout=timeout)
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 + 3 * attempt)
            continue
        if r.status_code == 404:
            body = {}
            try:
                body = r.json()
            except Exception:
                pass
            ra = r.headers.get("Retry-After") or body.get("retry_after_s")
            try:
                ra = int(float(ra)) if ra is not None else None
            except Exception:
                ra = None
            raise NENotBuilt(kg, str(body.get("status") or "missing"), ra, body)
        if r.status_code in (429, 500, 502, 503, 504):
            last = NEError(f"umfeld /ne/{kg}: HTTP {r.status_code}")
            time.sleep(5 + 10 * attempt)
            continue
        if r.status_code in (401, 403):
            raise NEError(f"umfeld /ne/{kg}: HTTP {r.status_code} — token refused")
        r.raise_for_status()
        secs = [s for s in unpack_sections(r.content) if s.magic == MAGIC_LU]
        if not secs:
            raise NEError(f"umfeld /ne/{kg}: no NEC1 section in answer")
        sec = secs[0]
        _check_declared_header(sec, kg)
        return sec
    raise NEError(f"umfeld /ne/{kg}: {last}")


def _check_declared_header(sec: Section, kg: str) -> None:
    h = sec.header
    if str(h.get("kg", "")).zfill(5) != str(kg).zfill(5):
        raise NEError(f"NEC1 kg mismatch: {h.get('kg')} != {kg}")
    if list(h.get("groups") or []) != list(GROUPS):
        raise NEError(f"NEC1 group order {h.get('groups')} != {GROUPS}")
    if int(h.get("h3_res", 0)) != H3_RES:
        raise NEError(f"NEC1 h3_res {h.get('h3_res')} != {H3_RES}")
    n = int(h.get("cells_n", 0)); kn = int(h.get("kcells_n", 0))
    if n * LU_REC.size + kn * K_REC.size != len(sec.raw):
        raise NEError("NEC1 body length != cells_n*30 + kcells_n*29")
    if n == 0:
        raise NEError("NEC1 has zero cells")
    ids = sec.cell_ids()
    if not np.all(ids[1:] > ids[:-1]):
        raise NEError("NEC1 cell ids not strictly sorted")


# --- bevdirect: local tile assembly + umfeld's pinned ne_cells package ---------

# bevdirect-serve is public since v0.3.0 (github.com/raffopenssh/vtcseamless);
# output is bit-identical to the pre-public v0.2.0/v0.2.1 builds, so the
# pinned 4-cell baseline stays valid.  v0.3.1 (2026-10-06) moved the raw
# BEV tile cache from disk to an in-RAM LRU (``-tile-cache-mb``; ``-cache``
# is accepted but ignored) — cell output unchanged, registered as
# cell-equivalent to the bev baseline on the NE server
# (``manifest.bev_equivalent_tags``), so no re-baselining.  Documents report
# ``bevdirect@<tag>`` from the binary's ``-X main.version`` ldflag — ``go
# install`` alone would stamp "dev", hence the explicit ldflags below.
# v0.3.2 (2026-10-07, commit 5ffbe23) fixes duplicate footprint fragments in
# multi-cell /viewport documents (a building crossing a z16 tile edge inside
# a neighbour cell's 0.004° pad was emitted whole in one cell and clipped
# under another per-tile id by the other; compose() deduped on that id).
# Aligned 0.02° single-cell documents — our request pattern — were already
# correct, so no digest change is expected; v0.3.2 is in the NE server's
# ``frozen.bev_equivalent_tags`` and compared against the v0.2.0 baseline.
# Pieces now carry ``complete`` and merged footprints ``members`` (ne_cells
# canon ignores both).  Any aligned-cell digest change on upgrade = upstream
# regression → report to umfeld.
# v0.3.3 (2026-10-08): empty layers are ``[]`` instead of ``null`` (the
# frozen ne_cells canon crashed on ``null`` footprints); nothing changes for
# the cells, v0.3.3 stays in the comparable tag class.  Requested by umfeld
# together with the aligned-cell ``input_bbox`` rule (vct_pilot.run).
# v0.3.5 (2026-10-23): BEV TLS outage fix — kataster.bev.gv.at stopped sending
# the GEANT TLS RSA 1 intermediate (~13:00Z); every tile fetch failed with
# "x509: certificate signed by unknown authority", /viewport 502/pending,
# zero NE builds.  v0.3.5 ships the intermediate + AIA fallback (extras via
# -ca-bundle / BEVDIRECT_CA_BUNDLE).  No data/assembly change, same
# equivalence class at umfeld.  /health gains ``upstream`` {state ∈
# unknown|ok|degraded|down, tls_errors, http_5xx, last_error}; 502 and
# pending bodies carry the same object (vct_pilot._viewport honours it).
# Never work around with InsecureSkipVerify / curl -k.
# v0.3.6 (2026-10-24): aligned-cell lookup fix (our report, see
# docs/vct-pilot.md → v0.3.6).  ``cellOf`` floored ``lon/0.02`` without an
# epsilon, so an exactly aligned single-cell /viewport (15.54/0.02 =
# 776.999…) resolved to two cells for 1 214/18 000 columns (and rows) and
# compose() kept the NEIGHBOUR's truncated copy of any parcel wider than
# cell+pad — border rows differed per KG / observer.  Now
# ``floor(lon/0.02 + 1e-9)``.  Documents + ne_cells digests of affected
# cells change ONCE (legitimate, not land-use change); all other cells are
# byte-identical.  umfeld lists v0.3.6 in ``bev_equivalent_tags`` → same
# source class, no re-baseline: boundary chunks flip on our first v0.3.6
# pass (candidate), the second pass confirms.
BEVDIRECT_TAG = os.environ.get("BEVDIRECT_TAG", "v0.3.6")
BEVDIRECT_MODULE = "github.com/raffopenssh/vtcseamless"
# Static release tarball (GitHub egress — NOT the metered primary); the
# primary-served copy and ``go install`` are fallbacks.
BEVDIRECT_RELEASE_URL = ("https://github.com/raffopenssh/vtcseamless/releases/download/"
                         "{tag}/bevdirect-serve_{tag}_linux_{arch}.tar.gz")
# sha256 of the release tarballs (from the upstream release notes); a tag
# missing here is installed unverified (``-version`` check only).
BEVDIRECT_RELEASE_SHA256 = {
    ("v0.3.2", "amd64"): "5a776c3c44d3fef52724229672e386c7908faeb60bc513fd306fd1322ef2a40f",
    ("v0.3.2", "arm64"): "cfe1972a058fd7cf11c578a7d34b5561dc2636bfd492661db609e98db3de4054",
    ("v0.3.3", "amd64"): "8a1f0ad073e41b7268c802b44df84414d95be79ccbed9ba5eabbcd63ca362bc4",
    ("v0.3.3", "arm64"): "a313d439dd7179c441006043ddcbaa15f358e25d07e5d41e4e7b336de90c402c",
    # v0.3.4 (2026-10-15): /viewport carries tiles[{z,x,y,sha256}] + stats.tiles_expected/fetched
    # (QC provenance); assembly unchanged, cell-equivalent to v0.3.0-v0.3.3
    ("v0.3.4", "amd64"): "ad1fb0166139dfb1a85b0da4bd88be5df28b4dafb0cd96d7ce198559e16cacd5",
    ("v0.3.4", "arm64"): "9076ea05da6c35e86331a4bf1a6898293bed812037221b262d02b38d24188d96",
    # v0.3.5 (2026-10-23): BEV TLS intermediate + AIA fallback; assembly unchanged
    ("v0.3.5", "amd64"): "70ad46bbaa454d9a97a61c4291d8d6bf6e3db83c5c45e5fc30e6c8ed819a8c52",
    ("v0.3.5", "arm64"): "d20835815fbd937edc704c59549cd79167243d07304a85994e0e11dbedd4d5b2",
    # v0.3.6 (2026-10-24): aligned-cell lookup epsilon; one-time digest flip on affected cell columns/rows
    ("v0.3.6", "amd64"): "d564589300332540630b9bcb192eea2de409e40d35eacdc63d60fcf3c2ab5f2e",
    ("v0.3.6", "arm64"): "b9ad0fe7cfa099a07999dfd1711bf0519d2ab935b38a7641ea10d07736ca564f",
}
# umfeld's pinned ``ne_cells`` package ships in the PUBLIC vtcseamless-py repo
# (pip project ``vtcseamless``, modules ``vtcseamless`` + ``ne_cells``).  It
# used to be pulled from cadastre-process-api, which is private — peers
# could never clone it, so the first pilot election stalled at 1/49 ready
# (Oct 2026).  algo/pack/canon/ns_groups are byte-identical to umfeld's.
NE_CELLS_REPO = "https://github.com/raffopenssh/vtcseamless-py.git"
NE_CELLS_MIN_VERSION = "2.1.0"     # first vtcseamless-py release of the package
VTCSEAMLESS_MIN_VERSION = "0.3.2"  # client dist: pins bevdirect@v0.3.6 + cells_for epsilon (0.3.1: /health.upstream contract; 0.3.0: QC provenance)
BEVDIRECT_BIN = DATA_DIR / "bin" / "bevdirect-serve"
BEVDIRECT_PID = DATA_DIR / "bevdirect-serve.pid"
BEVDIRECT_ADDR = os.environ.get("BEVDIRECT_ADDR", "127.0.0.1:8787")
# VTC posture (docs/vct-pilot.md): since v0.3.1 raw tiles live ONLY in the
# server's RAM LRU (``-tile-cache-mb``, 24 h TTL, gone with the process);
# assembled cells stay in RAM 6 h; no background prefetch ring (one request
# per KG, nothing speculative).  Peers are 8 GB VMs shared with the KG
# processor, so the LRU is sized below the upstream 1024 MiB default —
# over budget just means a refetch.  GOMEMLIMIT gives the Go runtime a
# soft ceiling above the LRU (upstream unit: 1024 MiB LRU / 2560 MiB limit).
BEVDIRECT_TILE_CACHE_MB = int(os.environ.get("BEVDIRECT_TILE_CACHE_MB", "512"))
BEVDIRECT_GOMEMLIMIT = os.environ.get("BEVDIRECT_GOMEMLIMIT", "1280MiB")
BEVDIRECT_TILE_TTL = os.environ.get("BEVDIRECT_TILE_TTL", "24h")
BEVDIRECT_CELL_TTL = os.environ.get("BEVDIRECT_CELL_TTL", "6h")
# Scratch dir for ``ne_source=bevdirect``'s cell documents (tmpfs; unlinked
# right after the build).  Pre-v0.3.1 the on-disk tile cache lived next to
# it — those dirs are purged on every server launch / tooling install so
# no raw .pbf survives anywhere (``_purge_legacy_tile_dirs``).
BEVDIRECT_WORK = Path(os.environ.get("BEVDIRECT_WORK") or (
    "/dev/shm/srtm_ne_work" if Path("/dev/shm").is_dir() else str(DATA_DIR / "ne_work")))
_LEGACY_TILE_DIRS = [Path("/dev/shm/srtm_bevcache"), DATA_DIR / "bevcache"]
if os.environ.get("BEVDIRECT_CACHE"):
    _LEGACY_TILE_DIRS.append(Path(os.environ["BEVDIRECT_CACHE"]))
NE_CELLS_PIN = os.environ.get("NE_CELLS_PIN", "ca485b5")   # vtcseamless-py 0.3.2 (bevdirect v0.3.6 pin, cell_of epsilon; ne_cells/ untouched)
TOOLING_LAST = DATA_DIR / "ne_tooling_last.json"   # last install attempt (rc + pip tail), for /admin/ne_tooling
CELL_DEG = 0.02


def bevdirect_ready(probe_server: bool = True) -> dict:
    """Capability probe for the director: binary present, package importable,
    server answering (skipped with ``probe_server=False`` — used from the
    peer-status push, where a 3 s connect attempt per push is too much).
    Never raises."""
    bv = _binary_version()
    # ``binary`` means *the pinned version is installed*: a stale v0.3.0 on
    # disk reads as missing so the director's stager (app._maybe_stage_ne_tooling)
    # and ensure_bevdirect_tooling() replace it after a tag bump.
    out = {"binary": bool(bv) and bv == BEVDIRECT_TAG, "binary_version": bv,
           "package": False, "server": False, "tag": BEVDIRECT_TAG, "pin": NE_CELLS_PIN}
    try:
        # the package may have been pip-installed after this interpreter
        # scanned site-packages: refresh the finder caches first
        import importlib
        importlib.invalidate_caches()
        import ne_cells  # noqa: F401
        out["package"] = True
        out["package_version"] = getattr(ne_cells, "__version__", None)
        # the client half of the dist (vtcseamless) moves while ne_cells is
        # frozen at 2.1.0: ``package`` means BOTH halves are at the pin
        try:
            from importlib.metadata import version as _dv
            out["client_version"] = _dv("vtcseamless")
            if (tuple(int(x) for x in out["client_version"].split(".")[:3])
                    < tuple(int(x) for x in VTCSEAMLESS_MIN_VERSION.split("."))):
                out["package"] = False
                out["package_stale"] = f"vtcseamless {out['client_version']} < {VTCSEAMLESS_MIN_VERSION}"
        except Exception:
            out["package"] = False
            out["package_stale"] = "vtcseamless dist missing"
    except Exception:
        pass
    if not probe_server:
        return out
    try:
        import requests
        r = requests.get(f"http://{BEVDIRECT_ADDR}/health", timeout=3)
        out["server"] = r.ok
        if r.ok:
            j = r.json() or {}
            out["server_version"] = j.get("bevdirect_version") or j.get("version")
            if isinstance(j.get("tile_cache"), dict):   # v0.3.1+: {tiles,bytes,max_bytes,hits,misses}
                out["tile_cache"] = j["tile_cache"]
            if isinstance(j.get("upstream"), dict):     # v0.3.5+: {state,tls_errors,http_5xx,last_error}
                out["upstream"] = j["upstream"]
    except Exception:
        pass
    return out


_BIN_VERSION_CACHE: dict = {}


def _binary_version() -> str | None:
    """``bevdirect-serve -version`` → ``v0.3.1`` (first token after the
    name), cached per (mtime, size).  None when the binary is missing."""
    try:
        st = BEVDIRECT_BIN.stat()
    except FileNotFoundError:
        return None
    key = (st.st_mtime_ns, st.st_size)
    if _BIN_VERSION_CACHE.get("key") == key:
        return _BIN_VERSION_CACHE.get("v")
    v = None
    try:
        r = subprocess.run([str(BEVDIRECT_BIN), "-version"], capture_output=True, text=True, timeout=20)
        toks = (r.stdout or r.stderr or "").split()
        if len(toks) >= 2 and toks[0] == "bevdirect-serve":
            v = toks[1]
    except Exception:
        v = None
    _BIN_VERSION_CACHE.update(key=key, v=v)
    return v


def _purge_legacy_tile_dirs() -> list[str]:
    """Remove pre-v0.3.1 on-disk tile caches (raw BEV .pbf — must not persist
    anywhere).  Idempotent, never raises."""
    import shutil
    gone = []
    for d in _LEGACY_TILE_DIRS:
        try:
            if d.is_dir():
                shutil.rmtree(d, ignore_errors=True)
                gone.append(str(d))
        except Exception:
            pass
    if gone:
        log.info("ne_h3: purged legacy bevdirect tile dir(s) %s", gone)
    return gone


def _fetch_release_binary(tag: str = BEVDIRECT_TAG) -> bool:
    """Step 0 of the binary install: the static release tarball from GitHub
    (public, un-metered) → ``BEVDIRECT_BIN``.  Verifies ``-version`` == tag."""
    import platform
    import tarfile
    import tempfile
    import requests
    arch = {"x86_64": "amd64", "aarch64": "arm64", "arm64": "arm64"}.get(platform.machine(), platform.machine())
    url = BEVDIRECT_RELEASE_URL.format(tag=tag, arch=arch)
    with tempfile.TemporaryDirectory(prefix="bdrel_") as td:
        tgz = Path(td) / "rel.tgz"
        h = hashlib.sha256()
        with requests.get(url, timeout=(10, 120), stream=True) as r:
            r.raise_for_status()
            with open(tgz, "wb") as fh:
                for ch in r.iter_content(1 << 20):
                    fh.write(ch)
                    h.update(ch)
        want = BEVDIRECT_RELEASE_SHA256.get((tag, arch))
        if want and h.hexdigest() != want:
            raise NEError(f"release tarball {url} sha256 {h.hexdigest()[:16]}… != pinned {want[:16]}…")
        with tarfile.open(tgz) as tf:
            member = next((m for m in tf.getmembers() if m.isfile() and m.name.endswith("/bevdirect-serve")), None)
            if member is None:
                raise NEError(f"release tarball {url} has no bevdirect-serve")
            src = tf.extractfile(member)
            tmp = BEVDIRECT_BIN.with_suffix(".tmp")
            with open(tmp, "wb") as fh:
                fh.write(src.read())
        tmp.chmod(0o755)
        r2 = subprocess.run([str(tmp), "-version"], capture_output=True, text=True, timeout=20)
        toks = (r2.stdout or "").split()
        if len(toks) < 2 or toks[1] != tag:
            tmp.unlink(missing_ok=True)
            raise NEError(f"release binary reports {toks[:2]}, expected {tag}")
        tmp.replace(BEVDIRECT_BIN)
    _BIN_VERSION_CACHE.clear()
    log.info("ne_h3: installed bevdirect-serve %s from GitHub release", tag)
    return True


def _package_stale(st: dict) -> bool:
    """True when ``ne_cells`` imports but predates the public vtcseamless-py
    release (legacy install from the private repo) — re-install so the whole
    fleet runs one dist."""
    if not st.get("package"):
        return True
    v = str(st.get("package_version") or "0")
    try:
        if tuple(int(x) for x in v.split(".")[:3]) < tuple(int(x) for x in NE_CELLS_MIN_VERSION.split(".")):
            return True
    except Exception:
        return True
    return False


_NE_CELLS_FRESH = [None]   # version string once this process installed/verified the package


def ensure_ne_cells_package(timeout: int = 900) -> dict:
    """pip-install the pinned ``ne_cells`` package from the public
    vtcseamless-py repo (as the srv user, ``--user``).  Idempotent; safe to
    run on every peer at srv start (GitHub egress only — nothing from the
    metered primary).  Returns the install record (also written to
    ``TOOLING_LAST``)."""
    import sys as _sys
    last = {"at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    st = bevdirect_ready(probe_server=False)
    if _NE_CELLS_FRESH[0]:
        # installed earlier in this process; the in-process import still
        # shows the legacy module (already imported) until srv restarts
        return {"present": True, "package_version": _NE_CELLS_FRESH[0]}
    if not _package_stale(st):
        return {"present": True, "package_version": st.get("package_version")}
    if st.get("package") or st.get("package_stale"):
        # legacy dist (pip project ``ne_cells`` from cadastre-process-api)
        # owns the same module path; drop it so RECORDs don't tangle
        try:
            subprocess.run([_sys.executable, "-m", "pip", "uninstall", "-y", "-q", "--break-system-packages", "ne_cells"],
                           check=False, timeout=120, capture_output=True, text=True)
            last["uninstalled_legacy"] = st.get("package_version")
        except Exception as e:  # noqa: BLE001
            last["uninstall_error"] = str(e)[:200]
    # ``sys.executable`` (not a bare ``python3``): gunicorn's interpreter
    # is the one that must import the package.  Output is kept so the
    # peer's ``/api/v1/admin/ne_tooling`` can show WHY an install failed
    # (Oct 2026: 49/49 peers had the binary, 1/49 the package, and the
    # only trace was in each peer's srv journal).
    cmd = [_sys.executable, "-m", "pip", "install", "--quiet", "--user", "--break-system-packages",
           "--no-warn-script-location", f"git+{NE_CELLS_REPO}@{NE_CELLS_PIN}"]
    log.info("ne_h3: installing ne_cells (%s)", " ".join(cmd))
    try:
        r = subprocess.run(cmd, check=False, timeout=timeout, capture_output=True, text=True)
        last["pip_rc"] = r.returncode
        last["pip_tail"] = ((r.stderr or "") + (r.stdout or ""))[-1500:]
        if r.returncode != 0:
            log.warning("ne_h3: ne_cells pip install rc=%s: %s", r.returncode, last["pip_tail"][-400:])
    except Exception as e:  # noqa: BLE001
        last["pip_rc"] = -1
        last["pip_tail"] = f"{type(e).__name__}: {e}"[:500]
        log.warning("ne_h3: ne_cells pip install failed: %s", e)
    # a fresh interpreter sees the new package; this process may not
    # (import caches) — probe in a subprocess so the verdict is honest.
    # Probe BOTH halves of the dist: ``_NE_CELLS_FRESH`` short-circuits every
    # later call in this process, so it must only be set when the pin really
    # landed.  (Oct 2026, at205: pip failed once, the legacy ne_cells 2.1.0
    # still imported, FRESH got set from that and the peer sat for 8 h on
    # ``package_stale: vtcseamless 0.3.0 < 0.3.2`` with the director
    # deferring every start — ×580 ``tooling installing`` events.)
    try:
        pr = subprocess.run([_sys.executable, "-c",
                             "import ne_cells;from importlib.metadata import version as v;"
                             "print(getattr(ne_cells,'__version__',''));print(v('vtcseamless'))"],
                            capture_output=True, text=True, timeout=60)
        last["import_rc"] = pr.returncode
        last["import_tail"] = (pr.stderr or pr.stdout or "")[-300:]
        lines = [x.strip() for x in (pr.stdout or "").splitlines() if x.strip()]
        if pr.returncode == 0 and len(lines) >= 2 and last.get("pip_rc") == 0:
            try:
                cv = tuple(int(x) for x in lines[1].split(".")[:3])
                if cv >= tuple(int(x) for x in VTCSEAMLESS_MIN_VERSION.split(".")):
                    _NE_CELLS_FRESH[0] = lines[0]
                    last["client_version"] = lines[1]
                else:
                    last["client_stale"] = lines[1]
            except Exception:
                pass
    except Exception as e:  # noqa: BLE001
        last["import_tail"] = str(e)[:200]
    out = bevdirect_ready(probe_server=False)
    last["ready"] = {k: out.get(k) for k in ("binary", "package", "package_version")}
    if _NE_CELLS_FRESH[0]:
        last["ready"]["package"] = True
        last["ready"]["package_version"] = _NE_CELLS_FRESH[0]
    try:
        TOOLING_LAST.parent.mkdir(parents=True, exist_ok=True)
        TOOLING_LAST.write_text(json.dumps(last))
    except Exception:
        pass
    return last


def ensure_bevdirect_tooling(install: bool = True, binary: bool = True,
                             primary_fallback: bool = True) -> dict:
    """Install the pinned ``ne_cells`` package (pip, from the public
    vtcseamless-py repo) and fetch / build the bevdirect-serve binary when
    it is missing OR not the pinned version (``binary=False`` skips it).
    Binary sources in order: GitHub release tarball (un-metered), the
    primary's copy (``primary_fallback`` — only the paced director stager
    should allow this, it is ~9 MB of metered egress), ``go install``.
    A stale running server is stopped so the next build relaunches the new
    binary; legacy on-disk tile dirs are purged.  Idempotent; used by
    ``/api/v1/admin/ne_tooling`` and the srv startup deps thread."""
    st = bevdirect_ready()
    if not install:
        return st
    last = {"at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    if _package_stale(st):
        last.update(ensure_ne_cells_package())
    if binary and not st["binary"]:
        BEVDIRECT_BIN.parent.mkdir(parents=True, exist_ok=True)
        got = False
        # 0) static release tarball from GitHub
        try:
            got = _fetch_release_binary()
        except Exception as e:  # noqa: BLE001
            log.warning("ne_h3: GitHub release fetch failed: %s", e)
            last["release_error"] = str(e)[:300]
        # 1) prebuilt binary from the primary (served by app.py /api/v1/admin/bevdirect_binary)
        try:
            if got or not primary_fallback:
                raise StopIteration
            self_json = DATA_DIR / "austria_processor" / "self.json"
            d = json.loads(self_json.read_text()) if self_json.exists() else {}
            durl = (d.get("director_url") or "").rstrip("/")
            if durl:
                import requests
                tok = (DATA_DIR / "admin_token").read_text().strip()
                r = requests.get(durl + "/api/v1/admin/bevdirect_binary", timeout=120,
                                 headers={"X-Admin-Token": tok}, stream=True)
                if r.ok and r.headers.get("X-Bevdirect-Tag", BEVDIRECT_TAG) == BEVDIRECT_TAG:
                    tmp = BEVDIRECT_BIN.with_suffix(".tmp")
                    with open(tmp, "wb") as fh:
                        for ch in r.iter_content(1 << 20):
                            fh.write(ch)
                    tmp.chmod(0o755)
                    tmp.replace(BEVDIRECT_BIN)
                    _BIN_VERSION_CACHE.clear()
                    got = True
        except StopIteration:
            pass
        except Exception as e:  # noqa: BLE001
            log.warning("ne_h3: prebuilt bevdirect-serve download failed: %s", e)
        # 2) go install from the public module (needs go), version stamped
        if not got:
            try:
                env = dict(os.environ, GOBIN=str(BEVDIRECT_BIN.parent), CGO_ENABLED="0",
                           GOFLAGS="-trimpath")
                env.setdefault("GOPATH", "/tmp/gopath")
                env.setdefault("GOCACHE", "/tmp/gocache")
                subprocess.run(["go", "install",
                                "-ldflags", f"-s -w -X main.version={BEVDIRECT_TAG}",
                                f"{BEVDIRECT_MODULE}/cmd/bevdirect-serve@{BEVDIRECT_TAG}"],
                               check=True, timeout=900, env=env)
                _BIN_VERSION_CACHE.clear()
                log.info("ne_h3: built %s", _binary_version())
            except Exception as e:  # noqa: BLE001
                log.warning("ne_h3: bevdirect-serve go install failed: %s", e)
                last["go_error"] = str(e)[:300]
        # a server still running the previous binary would tag documents
        # with the old version — stop it; the next build relaunches.
        sv = st.get("server_version")
        if st.get("server") and sv and sv != BEVDIRECT_TAG:
            _stop_bevdirect_server()
    _purge_legacy_tile_dirs()
    out = bevdirect_ready()
    last["ready"] = {k: out.get(k) for k in ("binary", "binary_version", "package", "package_version")}
    try:
        TOOLING_LAST.parent.mkdir(parents=True, exist_ok=True)
        TOOLING_LAST.write_text(json.dumps(last))
    except Exception:
        pass
    return out


def tooling_last() -> dict | None:
    try:
        return json.loads(TOOLING_LAST.read_text())
    except Exception:
        return None


def _stop_bevdirect_server(timeout_s: float = 15.0) -> bool:
    """Stop the bevdirect-serve we launched (pidfile; ``pkill -f`` on our
    listen address as fallback).  Its cache is RAM-only, so nothing to
    clean.  Returns True when the port stopped answering."""
    import signal
    pid = None
    try:
        pid = int(BEVDIRECT_PID.read_text().strip())
        cmd = Path(f"/proc/{pid}/cmdline").read_bytes()
        if b"bevdirect-serve" not in cmd:
            pid = None
    except Exception:
        pid = None
    try:
        if pid:
            os.kill(pid, signal.SIGTERM)
        else:
            subprocess.run(["pkill", "-f", f"bevdirect-serve -addr {BEVDIRECT_ADDR}"],
                           capture_output=True, timeout=10)
    except Exception as e:  # noqa: BLE001
        log.warning("ne_h3: stopping bevdirect-serve failed: %s", e)
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        if not bevdirect_ready()["server"]:
            BEVDIRECT_PID.unlink(missing_ok=True)
            log.info("ne_h3: stopped bevdirect-serve (pid %s)", pid)
            return True
        time.sleep(0.5)
    if pid:
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:
            pass
    return not bevdirect_ready()["server"]


# BEV regenerates the Katastralmappe vector tiles nightly at ~00:54Z.  A
# build whose tiles straddle that moment mixes two tile epochs (at210's
# 73008 one-column deviation, umfeld 2026-10-08), so: no fetch inside the
# regeneration window, and the server's RAM tile/cell cache is dropped
# (restart) after every regeneration — a build can then only ever see one
# epoch.  ``vct_pilot.run`` additionally re-fetches a build that crossed
# the window.
BEV_REGEN_WINDOW_UTC = (os.environ.get("BEV_REGEN_WINDOW_UTC") or "00:45-01:15")


def _regen_window_secs() -> tuple[int, int]:
    a, b = BEV_REGEN_WINDOW_UTC.split("-")
    ha, ma = (int(x) for x in a.split(":")); hb, mb = (int(x) for x in b.split(":"))
    return ha * 3600 + ma * 60, hb * 3600 + mb * 60


def bev_regen_window(now: float | None = None) -> tuple[float, float]:
    """``(start, end)`` of the regeneration window that is current or next."""
    now = time.time() if now is None else now
    a, b = _regen_window_secs()
    day = now - (now % 86400)
    if now >= day + b:
        day += 86400
    return day + a, day + b


def bev_regen_last_end(now: float | None = None) -> float:
    """End of the most recent regeneration window — tiles fetched before it
    belong to the previous BEV epoch."""
    now = time.time() if now is None else now
    return bev_regen_window(now)[1] - 86400


def bev_epoch_day(ts: float | None = None) -> str:
    """UTC date of the tile epoch a fetch at *ts* belongs to."""
    return time.strftime("%Y-%m-%d", time.gmtime(bev_regen_last_end(ts)))


def wait_out_bev_regen(deadline: float | None = None) -> float:
    """Sleep while inside the regeneration window (bounded by *deadline*).
    Returns the seconds slept."""
    a, b = bev_regen_window()
    now = time.time()
    if not (a <= now < b):
        return 0.0
    wait = b - now
    if deadline is not None:
        wait = min(wait, max(0.0, deadline - now))
    log.info("ne_h3: inside the BEV tile regeneration window (%s UTC) — waiting %.0fs", BEV_REGEN_WINDOW_UTC, wait)
    time.sleep(wait)
    return wait


def _server_started_at() -> float | None:
    try:
        return BEVDIRECT_PID.stat().st_mtime
    except Exception:
        return None


def _ensure_bevdirect_server() -> None:
    st = bevdirect_ready()
    if st["server"]:
        sv = st.get("server_version")
        started = _server_started_at()
        if started is not None and started < bev_regen_last_end():
            # RAM caches may hold tiles of the previous BEV epoch: drop them
            log.info("ne_h3: bevdirect-serve started %s, before the last BEV regeneration — restarting for a fresh tile epoch",
                     time.strftime("%m-%d %H:%MZ", time.gmtime(started)))
            if not _stop_bevdirect_server():
                raise NEError("bevdirect-serve would not stop for the BEV epoch restart")
            st = bevdirect_ready()
        elif not sv or sv == BEVDIRECT_TAG:
            return
        else:
            # Documents would be tagged bevdirect@<sv> and compared against the
            # wrong baseline — restart onto the pinned binary instead.
            log.warning("ne_h3: bevdirect-serve on %s is %s, pinned %s — restarting", BEVDIRECT_ADDR, sv, BEVDIRECT_TAG)
            if not _stop_bevdirect_server():
                raise NEError(f"bevdirect-serve on {BEVDIRECT_ADDR} is {sv}, pinned {BEVDIRECT_TAG}, and would not stop")
            st = bevdirect_ready()
    if not st["binary"]:
        # missing or stale binary: the GitHub release fetch takes seconds
        st = ensure_bevdirect_tooling(install=True, binary=True, primary_fallback=False)
        if not st["binary"]:
            raise NEError(f"bevdirect-serve binary missing/stale ({st.get('binary_version')}, pinned {BEVDIRECT_TAG}) "
                          "— run ensure_bevdirect_tooling()")
    _purge_legacy_tile_dirs()
    logf = open(DATA_DIR / "bevdirect-serve.log", "ab")
    # v0.3.1: no ``-cache`` (tiles are RAM-only), LRU sized by ``-tile-cache-mb``.
    env = dict(os.environ, GOMEMLIMIT=BEVDIRECT_GOMEMLIMIT)
    proc = subprocess.Popen([str(BEVDIRECT_BIN), "-addr", BEVDIRECT_ADDR,
                             "-tile-cache-mb", str(BEVDIRECT_TILE_CACHE_MB),
                             "-prefetch", "0", "-cells", "64", "-tile-ttl", BEVDIRECT_TILE_TTL,
                             "-ttl", BEVDIRECT_CELL_TTL, "-workers", "8"],
                            stdout=logf, stderr=logf, start_new_session=True, env=env)
    try:
        BEVDIRECT_PID.write_text(str(proc.pid))
    except Exception:
        pass
    for _ in range(30):
        time.sleep(1)
        if bevdirect_ready()["server"]:
            log.info("ne_h3: bevdirect-serve %s up on %s (tile LRU %d MiB, GOMEMLIMIT %s)",
                     BEVDIRECT_TAG, BEVDIRECT_ADDR, BEVDIRECT_TILE_CACHE_MB, BEVDIRECT_GOMEMLIMIT)
            return
    raise NEError("bevdirect-serve did not come up")


# Static floor of bev-equivalent tags (mirrors umfeld ``frozen.bev_equivalent_tags``
# as of 2026-10-23).  ``bev_sources_ok()`` extends it with the current binary pin
# and whatever the cached umfeld manifest lists, so a ``BEVDIRECT_TAG`` bump can
# never again reject every local build fleet-wide (v0.3.5 incident, 2026-10-23).
BEV_SOURCES_OK = frozenset({"bevdirect@v0.2.0", "bevdirect@v0.2.1", "bevdirect@v0.3.0", "bevdirect@v0.3.1",
                            "bevdirect@v0.3.2", "bevdirect@v0.3.3", "bevdirect@v0.3.4", "bevdirect@v0.3.5",
                            "bevdirect@v0.3.6"})


def bev_sources_ok() -> frozenset:
    """Accepted NEC1 ``source`` tags: static floor ∪ ``bevdirect@<pin>`` ∪
    umfeld's ``frozen.bev_equivalent_tags`` from the on-disk manifest cache
    (``ne_rollout.NE_MANIFEST_CACHE``; no network call)."""
    tags = set(BEV_SOURCES_OK)
    tags.add(f"bevdirect@{BEVDIRECT_TAG}")
    try:
        import ne_rollout
        if ne_rollout.NE_MANIFEST_CACHE.exists():
            mf = (json.loads(ne_rollout.NE_MANIFEST_CACHE.read_text()) or {}).get("manifest") or {}
            for t in ((mf.get("frozen") or {}).get("bev_equivalent_tags") or []):
                if str(t).startswith("bevdirect@"):
                    tags.add(str(t))
    except Exception:  # noqa: BLE001 — cache absent/corrupt → static floor
        pass
    return frozenset(tags)


def is_bev_declared(h) -> bool:
    """True when a NEC1 header / Section / h3b ``nec1`` dict is a bevdirect
    (local) build rather than umfeld's frozen ``index-2026-03`` layer."""
    if isinstance(h, Section):
        h = h.header
    return str((h or {}).get("source") or "").startswith("bevdirect@")


def nec1_bytes(sec: Section) -> bytes:
    return pack_section(MAGIC_LU, sec.header, sec.raw)


def build_declared_bevdirect(kg: str, bbox_wgs: tuple, is_block: bool = False) -> Section:
    """Compute the declared layer locally from the public BEV vector tiles:
    the VTC pilot run (``vct_pilot.build_declared`` — one ``/viewport`` per
    parent, cells aggregated in memory with umfeld's pinned ``ne_cells``,
    digests reported to umfeld-at ``/contrib``) writes the NEC1 section
    (statistics only) and we validate it here.  Source must be one of the
    bev-equivalent tags umfeld's ``/obs`` accepts (``BEV_SOURCES_OK``)."""
    import vct_pilot
    kg = str(kg).zfill(5)
    blob = vct_pilot.build_declared(kg, bbox_wgs, is_block=is_block)
    secs = [x for x in unpack_sections(blob) if x.magic == MAGIC_LU]
    if not secs:
        raise NEError("vct declared build produced no NEC1 section")
    sec = secs[0]
    _check_declared_header(sec, kg)
    src = str(sec.header.get("source") or "")
    ok = bev_sources_ok()
    if src not in ok:
        raise NEError(f"NEC1 source {src!r} is not a bev-equivalent tag umfeld accepts ({sorted(ok)})")
    if src != f"bevdirect@{BEVDIRECT_TAG}":
        log.warning("ne_h3: declared layer %s built with %s (binary pin is %s)", kg, src, BEVDIRECT_TAG)
    if sec.header.get("algo") != "ne-cells-2":
        raise NEError(f"NEC1 algo {sec.header.get('algo')!r} != ne-cells-2")
    return sec


def cadastre_source() -> str:
    """``bevdirect`` iff ``ne_source()`` is — one knob: a bev-mode peer takes
    its parcels/footprints from the same viewport documents as its declared
    layer and never calls umfeld's ``/k`` tier (docs/vct-pilot.md → bev mode)."""
    return ne_source()


def fetch_declared_director(kg: str, timeout: float = 120.0) -> Section:
    """umfeld's frozen ``lu`` NEC1 for *kg*, relayed by the cluster director
    (``GET /api/v1/ne/declared/<kg>``, admin token; the director holds the
    umfeld token and caches the ~265 KB section on disk).  Same 404 contract
    as umfeld → ``NENotBuilt``."""
    import requests
    import director_ha
    kg = str(kg).zfill(5)
    base = director_ha.director_url() or "http://127.0.0.1:8000"
    h = {"User-Agent": "srtm-lidar-at/2.4.1 ne_h3"}
    try:
        h["X-Admin-Token"] = (DATA_DIR / "admin_token").read_text().strip()
    except Exception:
        pass
    last = None
    for attempt in range(3):
        try:
            r = requests.get(f"{base.rstrip('/')}/api/v1/ne/declared/{kg}", headers=h, timeout=timeout)
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 + 3 * attempt)
            continue
        if r.status_code == 404:
            body = {}
            try:
                body = r.json()
            except Exception:
                pass
            ra = r.headers.get("Retry-After") or body.get("retry_after_s")
            try:
                ra = int(float(ra)) if ra is not None else None
            except Exception:
                ra = None
            raise NENotBuilt(kg, str(body.get("status") or "missing"), ra, body)
        if r.status_code in (429, 500, 502, 503, 504):
            last = NEError(f"director /ne/declared/{kg}: HTTP {r.status_code}")
            time.sleep(5 + 10 * attempt)
            continue
        r.raise_for_status()
        secs = [s for s in unpack_sections(r.content) if s.magic == MAGIC_LU]
        if not secs:
            raise NEError(f"director /ne/declared/{kg}: no NEC1 section in answer")
        sec = secs[0]
        _check_declared_header(sec, kg)
        return sec
    raise NEError(f"director /ne/declared/{kg}: {last}")


def fetch_declared(kg: str, bbox_wgs: tuple | None = None, code: str | None = None) -> Section:
    """Declared layer for parent *kg* from the configured source.

    ``ne_source=bevdirect`` builds it locally (``build_declared_bevdirect``)
    — for split blocks too since Oct 2026: every block fetches the parent's
    viewport grid anyway (it is the pipeline's cadastre), the build is
    deterministic across hosts (63101 primary == at68, 75110
    ``identical_kg_digest``), and the primary's ``ne_ingest`` merge re-keys
    by cell id when blocks disagree after all (BEV tile update between
    blocks).  The product's NEO1 is then posted as NEC1+NEO1 by the primary.

    Two fallbacks keep a KG from shipping ``-partial`` because of
    vtcseamless, both WITHOUT touching umfeld's ``/k`` from the peer:

    * ``DeclaredDeficient`` (own parcels clipped by the cell grid → cells
      lost; alpine parents) and any local failure → umfeld's ``lu`` relayed
      by the director (``fetch_declared_director``);
    * only when there is no director (standalone / the primary itself) →
      umfeld directly.
    """
    import director_ha
    if ne_source() == "bevdirect":
        if bbox_wgs is None:
            raise NEError("bevdirect source needs the KG bbox")
        try:
            return build_declared_bevdirect(kg, bbox_wgs, is_block="-" in str(code or ""))
        except Exception as e:  # noqa: BLE001
            import vct_pilot
            if isinstance(e, vct_pilot.DeclaredDeficient):
                log.info("ne_h3: %s — declared layer from umfeld lu via director", e)
            else:
                log.warning("ne_h3: local (bevdirect) declared layer for %s failed — falling back to umfeld lu: %s",
                            kg, str(e)[:200])
        if director_ha.director_url():
            return fetch_declared_director(kg)
    return fetch_declared_umfeld(kg)


OBS_BODY_CAP = 32 * 1024 * 1024


def post_observed(kg: str, neo1_section: bytes, timeout: float = 120.0,
                  nec1_section: bytes | None = None) -> dict:
    """POST the observed layer to umfeld-at ``/obs``.  Returns the server's
    JSON (``unchanged`` / ``replaced`` …).

    * bare NEO1 (``nec1_section=None``): the cell set must equal umfeld's
      frozen ``lu``; 409 on mismatch → NEError.
    * NEC1 + NEO1 (``nec1_section`` = our bevdirect build, since 2026-10-06):
      validated against THAT NEC1 (409 only when the two disagree = a local
      bug); umfeld stores the NEC1 as the KG's ``bev`` section and answers
      ``declared_layer:"bev"``, ``bev{digest,cells_n,source}`` and, if its
      ``lu`` exists, ``lu_diff{lu_cells_n,bev_cells_n,missing_n,extra_n}``
      (border-cell drift, informational).  400 = bad NEC1 header/source.
    """
    import requests
    kg = str(kg).zfill(5)
    h = _umfeld_headers()
    h["Content-Type"] = "application/vnd.ne-cells"
    body = (nec1_section + neo1_section) if nec1_section else neo1_section
    if len(body) > OBS_BODY_CAP:
        # umfeld's /obs cap (16 MB until 2026-10-27, 32 MB since — raised for
        # 80110 Sölden, 1.55 M cells = 17.5 MB NEO1 / 21.5 MB NEC1+NEO1)
        raise NEError(f"umfeld /ne/{kg}/obs body {len(body)} B > {OBS_BODY_CAP // (1024 * 1024)} MB (over-cap)")
    r = requests.post(f"{_umfeld_contrib_base()}/{kg}/obs", data=body, headers=h, timeout=timeout)
    if r.status_code in (400, 409):
        raise NEError(f"umfeld /ne/{kg}/obs {r.status_code}{' (NEC1+NEO1)' if nec1_section else ''}: {r.text[:700]}")
    r.raise_for_status()
    try:
        out = r.json()
    except Exception:
        out = {"status": r.status_code}
    if nec1_section and isinstance(out, dict):
        ld = out.get("lu_diff") or {}
        log.info("ne_h3: /obs %s on %s declared layer (bev %s cells=%s) lu_diff: lu=%s bev=%s missing=%s extra=%s",
                 kg, out.get("declared_layer"), (out.get("bev") or {}).get("digest"),
                 (out.get("bev") or {}).get("cells_n"), ld.get("lu_cells_n"), ld.get("bev_cells_n"),
                 ld.get("missing_n"), ld.get("extra_n"))
    return out


# === SECTION: geometry helpers ===

def _tf_3035_to_4326():
    from pyproj import Transformer
    return Transformer.from_crs("EPSG:3035", "EPSG:4326", always_xy=True)


def _tf_4326_to_3035():
    from pyproj import Transformer
    return Transformer.from_crs("EPSG:4326", "EPSG:3035", always_xy=True)


def cells_for_points(es: np.ndarray, ns: np.ndarray, res: int = H3_RES) -> np.ndarray:
    """EPSG:3035 points → uint64 H3 cells."""
    import h3.api.numpy_int as h3n
    if es.size == 0:
        return np.zeros(0, np.uint64)
    lon, lat = _tf_3035_to_4326().transform(es.astype(float), ns.astype(float))
    f = h3n.latlng_to_cell
    return np.fromiter((f(float(a), float(b), res) for a, b in zip(lat, lon)), dtype=np.uint64,
                       count=len(lat))


def cell_centres_3035(cids: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Metre-snapped EPSG:3035 centres (the dx/dy origin of NET1/NEB1)."""
    import h3.api.numpy_int as h3n
    if cids.size == 0:
        return np.zeros(0), np.zeros(0)
    ll = [h3n.cell_to_latlng(int(c)) for c in cids]
    lat = np.array([p[0] for p in ll]); lon = np.array([p[1] for p in ll])
    e, n = _tf_4326_to_3035().transform(lon, lat)
    return np.round(e), np.round(n)


def cell_area_m2(cids: np.ndarray) -> np.ndarray:
    import h3.api.numpy_int as h3n
    return np.array([h3n.cell_area(int(c), unit="m^2") for c in cids], dtype=np.float64)


# === SECTION: observed layer ===

def _q_u8(x, scale, nodata=NODATA_U8, lo=0, hi=254):
    """round-half-up quantiser to u8 with *nodata* for NaN."""
    x = np.asarray(x, dtype=np.float64)
    out = np.full(x.shape, nodata, np.uint8)
    ok = np.isfinite(x)
    v = np.floor(x[ok] * scale + 0.5)
    out[ok] = np.clip(v, lo, hi).astype(np.uint8)
    return out


def _bits(val: int, width: int) -> int:
    return int(val) & ((1 << width) - 1)


def tile_flight_years(tr: dict) -> set:
    """ALS flight years of one tile for the NEO1 ``flight_years`` bitmask.

    Reads ``tr["v21"]["acq"]`` as written by ``v21_products.tile_coarse``
    (``dtm_year`` / ``dsm_year``; the ortho year is a different sensor and
    is NOT a flight year) plus optional ``tr["flight_years"]`` (fast path:
    years recovered from the JSON ``acquisition`` block).  Before 2026-10-04
    this looked for ``acq.years`` / ``acq.flight_years`` — keys that never
    existed — so every product carried ``flight_years = 0``."""
    years = set()
    v = tr.get("v21") or {}
    acq = v.get("acq") or {}
    for k in ("dtm_year", "dsm_year"):
        try:
            if acq.get(k):
                years.add(int(acq[k]))
        except Exception:
            pass
    for src in (v.get("als_year"), *(tr.get("flight_years") or [])):
        try:
            if src:
                years.add(int(src))
        except Exception:
            pass
    return years


def build_observed(declared: Section, seg_type_full: np.ndarray, ndsm_full: np.ndarray,
                   labels_full: np.ndarray, all_objects: list, full_tf,
                   type_names: dict, apex_rows: list | None = None,
                   tile_seg_results: list | None = None, kg: str = "",
                   strip_rows: int = 1000, coarse: dict | None = None,
                   heartbeat=None) -> dict:
    """NEO1 + NET1 + NEB1 for the cell set of *declared* (NEC1).

    Arrays are the stitched 1 m EPSG:3035 rasters of ``build_light_gpkg_tiled``
    (``full_tf`` their affine); ``type_names`` = ``object_segmentation.ALL_TYPE_NAMES``
    (code → name).  Pixels are assigned to cells per 5 m block (block centre →
    H3), then aggregated per strip of *strip_rows* rows so peak memory stays
    ~ one strip.  ``coarse`` = ``v21_products.stitch_coarse`` output (5 m
    dtm/slope/aspect) → per-cell terrain; tile ``hansen_loss_year`` rasters →
    per-cell loss year.  Returns {"neo1": bytes(section), "net1": …, "neb1": …,
    "summary": dict, per-cell columns}.  ``heartbeat(detail: str)`` (optional)
    is called once per strip / phase so the caller can prove liveness to
    the director's stuck-KG watchdog.
    """
    t0 = time.time()
    _hb = heartbeat if callable(heartbeat) else (lambda _d: None)
    cids_all = declared.cell_ids()                  # sorted uint64 (the PARENT's set)
    n_all = len(cids_all)
    lu_all = declared.lu_table()
    res = abs(float(full_tf.a))
    H, W = seg_type_full.shape
    left, top = float(full_tf.c), float(full_tf.f)
    right, bottom = left + W * res, top - H * res

    # --- clip the declared set to this raster's bbox ---------------------
    # A split block carries the parent's full NEC1 (Sölden 80110: 1.5 M
    # res-12 cells for a 19 Mpx block).  Every accumulator below is sized
    # per cell and the height histogram is (n, 256) with an int64 bincount
    # temp per strip → ~8 GB for 1.5 M cells → cgroup OOM, and the Pool
    # lost the worker silently (stuck-kg 12 h restarts, Oct 2026).  Only
    # cells whose centre lies within one cell radius of the raster can
    # receive a pixel / apex / object, so compute everything on that
    # subset and scatter back into the parent's cell order at the end
    # (unselected cells = unseen = nodata, byte-identical to before).
    ce_all, cn_all = cell_centres_3035(cids_all)
    _M = 50.0                                       # res-12 circumradius ~9.4 m
    sel_idx = np.nonzero((ce_all >= left - _M) & (ce_all <= right + _M)
                         & (cn_all >= bottom - _M) & (cn_all <= top + _M))[0]
    if sel_idx.size == 0 and n_all > 0:
        # Block grid cells of a huge parent can lie entirely outside the KG
        # polygon (Sölden 80110-northwest-*, Oct 2026): nothing declared in
        # this raster.  Keep one (off-raster) cell so the searchsorted /
        # ``cids[min(i, n-1)]`` guards below stay well-defined; it receives
        # no pixel/apex/object → every parent cell ends up unseen.
        log.info("ne_h3: declared %d cells, none inside raster bbox — all unseen", n_all)
        sel_idx = np.array([0], dtype=np.int64)
    cids = cids_all[sel_idx]                        # still sorted (subset of sorted)
    n_cells = len(cids)
    lu_tab = {k: v[sel_idx] for k, v in lu_all.items()}
    ce, cn = ce_all[sel_idx], cn_all[sel_idx]
    cell_area_all = cell_area_m2(cids_all)         # m² per cell
    cell_area = cell_area_all[sel_idx]
    if n_cells < n_all:
        log.info("ne_h3: declared %d cells, %d inside raster bbox (%d x %d px)", n_all, n_cells, W, H)

    # --- type LUTs (256 codes) -------------------------------------------
    grp_lut = np.full(256, 255, np.uint8)          # 255 = no group (unsegmented / unknown)
    for code, name in type_names.items():
        if 0 <= int(code) < 256:
            g = _TYPE_GROUP.get(name)
            grp_lut[int(code)] = GROUPS.index(g) if g else GROUPS.index("sonst")
    grp_lut[0] = 255

    # --- per-object LUTs (label → value) ----------------------------------
    max_lab = int(labels_full.max()) if labels_full.size else 0
    obj_by_id = {o.obj_id: o for o in all_objects}
    ndvi_lut = np.full(max_lab + 1, np.nan, np.float32)
    amp_lut = np.full(max_lab + 1, np.nan, np.float32)
    dh_lut = np.full(max_lab + 1, np.nan, np.float32)
    rough_lut = np.full(max_lab + 1, np.nan, np.float32)
    dtmc_lut = np.full(max_lab + 1, np.nan, np.float32)
    tri_lut = np.full(max_lab + 1, np.nan, np.float32)
    tpi_lut = np.full(max_lab + 1, np.nan, np.float32)
    pheno_lut = np.full(max_lab + 1, 255, np.uint8)
    for oid, o in obj_by_id.items():
        if 0 < oid <= max_lab:
            nd = getattr(o, "ndvi_fused", 0.0) or getattr(o, "ndvi_mean", 0.0)
            ndvi_lut[oid] = nd
            amp_lut[oid] = getattr(o, "harm_amplitude", 0.0) or np.nan
            dh_lut[oid] = getattr(o, "height_change", 0.0)
            rough_lut[oid] = getattr(o, "roughness", 0.0)
            dtmc_lut[oid] = getattr(o, "dtm_change", 0.0)
            tri_lut[oid] = getattr(o, "tri_mean", 0.0)
            tpi_lut[oid] = getattr(o, "tpi_mean", 0.0)
            pheno_lut[oid] = _PHENO_IDX.get(str(getattr(o, "phenology_class", "") or "unknown"), 0)

    # --- accumulators ------------------------------------------------------
    px_total = np.zeros(n_cells, np.int64)          # pixels inside any tile (segmented or not)
    px_seg = np.zeros(n_cells, np.int64)
    grp_px = np.zeros((n_cells, 9), np.int64)
    h_hist = np.zeros((n_cells, 256), np.int32)     # nDSM 0.5 m bins (sat 127 m)
    h_sum = np.zeros(n_cells, np.float64); h_n = np.zeros(n_cells, np.int64)
    ndvi_sum = np.zeros(n_cells, np.float64); ndvi_n = np.zeros(n_cells, np.int64)
    amp_sum = np.zeros(n_cells, np.float64); amp_n = np.zeros(n_cells, np.int64)
    dh_sum = np.zeros(n_cells, np.float64); dh_n = np.zeros(n_cells, np.int64)
    rough_sum = np.zeros(n_cells, np.float64); rough_n = np.zeros(n_cells, np.int64)
    dtmc_sum = np.zeros(n_cells, np.float64); dtmc_n = np.zeros(n_cells, np.int64)
    tri_sum = np.zeros(n_cells, np.float64); tri_n = np.zeros(n_cells, np.int64)
    tpi_sum = np.zeros(n_cells, np.float64); tpi_n = np.zeros(n_cells, np.int64)
    pheno_px = np.zeros((n_cells, len(PHENOLOGY_CODES)), np.int64)

    blk = max(1, int(round(5.0 / res)))             # 5 m blocks
    bw = (W + blk - 1) // blk
    bx = left + (np.arange(bw) * blk + blk / 2.0) * res
    for r0 in range(0, H, strip_rows):
        r1 = min(H, r0 + strip_rows)
        _hb(f"strip {r1}/{H} rows, {n_cells} cells")
        # block cells for this strip
        brows = np.arange(r0, r1, blk)
        by = top - (brows + blk / 2.0) * res
        BE, BN = np.meshgrid(bx, by)
        bc = cells_for_points(BE.ravel(), BN.ravel()).reshape(BE.shape)
        bidx = np.searchsorted(cids, bc)
        bidx = np.where((bidx < n_cells) & (cids[np.minimum(bidx, n_cells - 1)] == bc), bidx, -1).astype(np.int32)
        # upsample to pixels of the strip
        idx = np.repeat(np.repeat(bidx, blk, axis=0), blk, axis=1)[: r1 - r0, :W]
        st = seg_type_full[r0:r1]
        nd = ndsm_full[r0:r1]
        lb = labels_full[r0:r1]
        inside = idx >= 0
        if not inside.any():
            continue
        ii = idx[inside]
        stv = st[inside]
        valid_px = np.isfinite(nd[inside])          # tile coverage proxy: nDSM finite
        np.add.at(px_total, ii[valid_px], 1)
        g = grp_lut[stv]
        segm = g < 255
        np.add.at(px_seg, ii[segm], 1)
        gi = ii[segm] * 9 + g[segm].astype(np.int64)
        grp_px += np.bincount(gi, minlength=n_cells * 9).reshape(n_cells, 9)
        # heights (nDSM, 0.5 m bins)
        hv = nd[inside]
        hok = np.isfinite(hv)
        if hok.any():
            hb = np.clip(np.floor(np.maximum(hv[hok], 0) * 2 + 0.5), 0, 254).astype(np.int64)
            # sparse accumulate (unique+counts) — a dense bincount(minlength=n*256)
            # int64 temp is 2 KB per cell per strip
            _hk, _hc = np.unique(ii[hok] * 256 + hb, return_counts=True)
            h_hist.reshape(-1)[_hk] += _hc.astype(np.int32)
            np.add.at(h_sum, ii[hok], hv[hok]); np.add.at(h_n, ii[hok], 1)
        # per-object spectra / change
        lv = lb[inside]
        lok = (lv > 0) & (lv <= max_lab)
        if lok.any():
            li = lv[lok]; ci = ii[lok]
            v = ndvi_lut[li]; m = np.isfinite(v)
            np.add.at(ndvi_sum, ci[m], v[m]); np.add.at(ndvi_n, ci[m], 1)
            v = amp_lut[li]; m = np.isfinite(v)
            np.add.at(amp_sum, ci[m], v[m]); np.add.at(amp_n, ci[m], 1)
            v = dh_lut[li]; m = np.isfinite(v)
            np.add.at(dh_sum, ci[m], v[m]); np.add.at(dh_n, ci[m], 1)
            v = rough_lut[li]; m = np.isfinite(v)
            np.add.at(rough_sum, ci[m], v[m]); np.add.at(rough_n, ci[m], 1)
            v = dtmc_lut[li]; m = np.isfinite(v)
            np.add.at(dtmc_sum, ci[m], v[m]); np.add.at(dtmc_n, ci[m], 1)
            v = tri_lut[li]; m = np.isfinite(v)
            np.add.at(tri_sum, ci[m], v[m]); np.add.at(tri_n, ci[m], 1)
            v = tpi_lut[li]; m = np.isfinite(v)
            np.add.at(tpi_sum, ci[m], v[m]); np.add.at(tpi_n, ci[m], 1)
            pv = pheno_lut[li]; m = pv < 255
            pheno_px += np.bincount(ci[m] * len(PHENOLOGY_CODES) + pv[m].astype(np.int64),
                                    minlength=n_cells * len(PHENOLOGY_CODES)).reshape(n_cells, -1)

    # --- per-cell derived columns -----------------------------------------
    with np.errstate(invalid="ignore", divide="ignore"):
        frac = grp_px / cell_area[:, None]            # fraction of CELL area (1 m² px)
        lu_u8 = np.floor(frac * 255 + 0.5)
        s = lu_u8.sum(axis=1)
        over = s > 255
        lu_u8[over] = np.floor(lu_u8[over] * (255.0 / s[over, None]))
        lu_u8 = np.clip(lu_u8, 0, 255).astype(np.uint8)
        obs_bld_cover = np.clip(np.floor(grp_px[:, 0] / cell_area * 255 + 0.5), 0, 255).astype(np.uint8)
        h_mean = np.where(h_n > 0, h_sum / np.maximum(h_n, 1), np.nan)
        cum = np.cumsum(h_hist, axis=1, dtype=np.int32)
        tot = cum[:, -1]
        p90_bin = (cum < np.ceil(0.9 * tot)[:, None]).sum(axis=1)
        max_bin = 255 - (h_hist[:, ::-1] > 0).argmax(axis=1)
        h_p90 = np.where(tot > 0, p90_bin / 2.0, np.nan)
        h_max = np.where(tot > 0, max_bin / 2.0, np.nan)
        ndvi_mean = np.where(ndvi_n > 0, ndvi_sum / np.maximum(ndvi_n, 1), np.nan)
        ndvi_amp = np.where(amp_n > 0, amp_sum / np.maximum(amp_n, 1), np.nan)
        dh_mean = np.where(dh_n > 0, dh_sum / np.maximum(dh_n, 1), np.nan)
        rough_mean = np.where(rough_n > 0, rough_sum / np.maximum(rough_n, 1), np.nan)
        dtmc_mean = np.where(dtmc_n > 0, dtmc_sum / np.maximum(dtmc_n, 1), np.nan)
        tri_mean = np.where(tri_n > 0, tri_sum / np.maximum(tri_n, 1), np.nan)
        tpi_mean = np.where(tpi_n > 0, tpi_sum / np.maximum(tpi_n, 1), np.nan)
    pheno_dom = np.where(pheno_px.sum(axis=1) > 0, pheno_px.argmax(axis=1), 255).astype(np.uint8)
    observed = px_total > 0.25 * cell_area           # ≥ 25 % of the cell inside our tiles

    # --- terrain from the 5 m coarse grids (v21_products.stitch_coarse) -------
    elev_mean = np.full(n_cells, np.nan); slope_mean = np.full(n_cells, np.nan)
    slope_p90 = np.full(n_cells, np.nan); aspect_dom = np.full(n_cells, 255, np.uint8)
    if coarse and coarse.get("dtm") is not None:
        try:
            cres = float(coarse["res"]); cl, ct = float(coarse["left"]), float(coarse["top"])
            dtm5 = coarse["dtm"]; slope5 = coarse.get("slope"); asp5 = coarse.get("aspect")
            ch, cw = dtm5.shape
            es5 = cl + (np.arange(cw) + 0.5) * cres
            ns5 = ct - (np.arange(ch) + 0.5) * cres
            EE, NN = np.meshgrid(es5, ns5)
            cc = cells_for_points(EE.ravel(), NN.ravel())
            ci5 = np.searchsorted(cids, cc)
            ok5 = (ci5 < n_cells) & (cids[np.minimum(ci5, n_cells - 1)] == cc)
            ci5 = ci5[ok5]
            d = dtm5.ravel()[ok5]; m = np.isfinite(d)
            e_sum = np.bincount(ci5[m], weights=d[m], minlength=n_cells); e_n = np.bincount(ci5[m], minlength=n_cells)
            with np.errstate(invalid="ignore", divide="ignore"):
                elev_mean = np.where(e_n > 0, e_sum / np.maximum(e_n, 1), np.nan)
            if slope5 is not None:
                sl = slope5.ravel()[ok5]; m = np.isfinite(sl)
                s_sum = np.bincount(ci5[m], weights=sl[m], minlength=n_cells); s_n = np.bincount(ci5[m], minlength=n_cells)
                with np.errstate(invalid="ignore", divide="ignore"):
                    slope_mean = np.where(s_n > 0, s_sum / np.maximum(s_n, 1), np.nan)
                sb = np.clip(np.floor(sl[m] + 0.5), 0, 90).astype(np.int64)
                sh = np.bincount(ci5[m] * 91 + sb, minlength=n_cells * 91).reshape(n_cells, 91)
                scum = np.cumsum(sh, axis=1); stot = scum[:, -1]
                p90 = (scum < np.ceil(0.9 * stot)[:, None]).sum(axis=1)
                slope_p90 = np.where(stot > 0, p90, np.nan)
            if asp5 is not None:
                av = asp5.ravel()[ok5]; m = np.isfinite(av)
                sec = np.where(av[m] < 0, ASPECT_FLAT, np.floor(((av[m] + 22.5) % 360) / 45.0)).astype(np.int64)
                sec = np.clip(sec, 0, 8)
                ah = np.bincount(ci5[m] * 9 + sec, minlength=n_cells * 9).reshape(n_cells, 9)
                aspect_dom = np.where(ah.sum(axis=1) > 0, ah.argmax(axis=1), 255).astype(np.uint8)
        except Exception as e:  # noqa: BLE001
            log.warning("ne_h3: terrain aggregation failed: %s", e)

    # --- Hansen loss year (per-tile 1 m rasters, sparse) ------------------------
    hansen_year = np.zeros(n_cells, np.uint8)      # 0 = no loss
    hansen_seen = False
    if tile_seg_results:
        for tr in tile_seg_results:
            ly = tr.get("hansen_loss_year")
            if ly is None:
                continue
            hansen_seen = True
            try:
                ly = np.asarray(ly)
                rr, cc_ = np.nonzero(ly > 0)
                if rr.size == 0:
                    continue
                th_, tw_ = ly.shape
                l_, b_, r_, t_ = tr["bounds_3035"]
                tres_x = (r_ - l_) / tw_; tres_y = (t_ - b_) / th_
                es = l_ + (cc_ + 0.5) * tres_x; ns = t_ - (rr + 0.5) * tres_y
                hc = cells_for_points(es, ns)
                hi = np.searchsorted(cids, hc)
                okh = (hi < n_cells) & (cids[np.minimum(hi, n_cells - 1)] == hc)
                yrs = ly[rr, cc_].astype(np.int64)
                yrs = np.where(yrs > 2000, yrs - 2000, yrs)          # accept 2001.. or 1..
                yrs = np.clip(yrs, 1, 254)
                np.maximum.at(hansen_year, hi[okh], yrs[okh].astype(np.uint8))
            except Exception as e:  # noqa: BLE001
                log.debug("ne_h3: hansen tile skipped: %s", e)

    _hb(f"trees/objects ({n_cells} cells)")
    # --- trees (NET1) ---------------------------------------------------------
    # (ce, cn = metre-snapped cell centres, computed with the bbox clip above)
    tree_n = np.zeros(n_cells, np.int64)
    tree_h_max = np.full(n_cells, np.nan)
    crown_m2 = np.zeros(n_cells, np.float64)
    net_rows: list[list[int]] = [[] for _ in range(n_cells)]
    n_trees_total = 0
    if apex_rows:
        te = np.array([r["e"] for r in apex_rows], float)
        tn = np.array([r["n"] for r in apex_rows], float)
        th = np.array([r.get("h_m") or 0.0 for r in apex_rows], float)
        keep = th >= 3.0
        tc = cells_for_points(te[keep], tn[keep])
        ti = np.searchsorted(cids, tc)
        ok = (ti < n_cells) & (cids[np.minimum(ti, n_cells - 1)] == tc)
        rows_k = [r for r, k in zip(apex_rows, keep) if k]
        for j in np.nonzero(ok)[0]:
            c = int(ti[j]); r = rows_k[j]
            if tree_n[c] >= 255:
                continue
            tree_n[c] += 1; n_trees_total += 1
            h = float(r.get("h_m") or 0.0)
            tree_h_max[c] = h if not np.isfinite(tree_h_max[c]) else max(tree_h_max[c], h)
            ca = float(r.get("crown_area_m2") or 0.0)
            crown_m2[c] += ca
            dx = int(np.clip(round(float(r["e"]) - ce[c]), -16, 15))
            dy = int(np.clip(round(float(r["n"]) - cn[c]), -16, 15))
            hq = int(np.clip(math.floor(h * 4 + 0.5), 0, 255))
            cq = int(np.clip(math.floor(math.log2(max(ca, 1.0)) * 5 + 0.5), 0, 63))  # log2 steps /5 → 1..~6000 m²
            sp = _SPECIES_IDX.get(str(r.get("species_hint") or "unknown"), 0)
            vt = _VITALITY_IDX.get(str(r.get("vitality") or "unknown"), 0)
            fl = 1 if (r.get("detection_source") or "") == "ndsm_only" else 0
            w = (_bits(dx, 5) | (_bits(dy, 5) << 5) | (hq << 10) | (cq << 18) | (sp << 24)
                 | (vt << 28) | (fl << 31))
            net_rows[c].append(w)
    crown_frac = np.clip(crown_m2 / cell_area, 0, 1)

    # --- man-made objects (NEB1) ----------------------------------------------
    bld_n = np.zeros(n_cells, np.int64)
    neb_rows: list[list[int]] = [[] for _ in range(n_cells)]
    cand = [o for o in all_objects if o.obj_type in _BLD_TYPE_IDX
            and (o.obj_type not in ("wall", "fence") or (o.height_max or 0) >= 1.0)]
    n_bld_total = 0
    if cand:
        oe = np.array([o.centroid_e for o in cand]); on = np.array([o.centroid_n for o in cand])
        oc = cells_for_points(oe, on)
        oi = np.searchsorted(cids, oc)
        ok = (oi < n_cells) & (cids[np.minimum(oi, n_cells - 1)] == oc)
        for j in np.nonzero(ok)[0]:
            c = int(oi[j]); o = cand[j]
            if bld_n[c] >= 255:
                continue
            bld_n[c] += 1; n_bld_total += 1
            dx = int(np.clip(round(o.centroid_e - ce[c]), -16, 15))
            dy = int(np.clip(round(o.centroid_n - cn[c]), -16, 15))
            hm = int(np.clip(math.floor((o.height_max or 0) * 4 + 0.5), 0, 255))
            hr = int(np.clip(math.floor((getattr(o, "height_p90", 0) or 0) * 4 + 0.5), 0, 255))
            ar = int(np.clip(round(o.area_sqm or 0), 0, 65535))
            ty = _BLD_TYPE_IDX[o.obj_type]
            cf = int(np.clip(math.floor((o.confidence or 0) * 15 + 0.5), 0, 15))
            dhq = int(np.clip(round((getattr(o, "height_change", 0.0) or 0.0) * 4), -127, 127)) & 0xFF
            w = (_bits(dx, 5) | (_bits(dy, 5) << 5) | (hm << 10) | (hr << 18) | (ar << 26)
                 | (ty << 42) | (cf << 46) | (dhq << 50))
            neb_rows[c].append(w)

    # --- flight years bitmask --------------------------------------------------
    fy = np.zeros(n_cells, np.uint8)
    if tile_seg_results:
        for tr in tile_seg_results:
            years = tile_flight_years(tr)
            if not years:
                continue
            bits = 0
            for y in years:
                i = y - FLIGHT_YEAR0
                if 0 <= i < 8:
                    bits |= 1 << i
            l_, b_, r_, t_ = tr["bounds_3035"]
            sel = (ce >= l_) & (ce < r_) & (cn > b_) & (cn <= t_)
            fy[sel] |= np.uint8(bits)

    # --- scatter the per-cell results back into the parent's cell order ------
    # Everything above ran on the bbox subset; from here on arrays are n_all
    # long so NEO1 rows / verdicts / summaries cover the whole declared set
    # (unselected cells: unseen → nodata, exactly what the dense path produced).
    def _expand(a, fill):
        if n_cells == n_all:
            return a
        out = np.full((n_all,) + a.shape[1:], fill, dtype=a.dtype)
        out[sel_idx] = a
        return out
    lu_u8 = _expand(lu_u8, 0); obs_bld_cover = _expand(obs_bld_cover, 0)
    observed = _expand(observed, False)
    h_mean = _expand(h_mean, np.nan); h_p90 = _expand(h_p90, np.nan); h_max = _expand(h_max, np.nan)
    ndvi_mean = _expand(ndvi_mean, np.nan); ndvi_amp = _expand(ndvi_amp, np.nan)
    dh_mean = _expand(dh_mean, np.nan); rough_mean = _expand(rough_mean, np.nan)
    dtmc_mean = _expand(dtmc_mean, np.nan); tri_mean = _expand(tri_mean, np.nan)
    tpi_mean = _expand(tpi_mean, np.nan); pheno_dom = _expand(pheno_dom, 255)
    elev_mean = _expand(elev_mean, np.nan); slope_mean = _expand(slope_mean, np.nan)
    slope_p90 = _expand(slope_p90, np.nan); aspect_dom = _expand(aspect_dom, 255)
    hansen_year = _expand(hansen_year, 0)
    tree_n = _expand(tree_n, 0); tree_h_max = _expand(tree_h_max, np.nan)
    crown_frac = _expand(crown_frac, 0.0); bld_n = _expand(bld_n, 0); fy = _expand(fy, 0)
    if n_cells < n_all:
        # row lists: unselected cells have no rows; keep global cell order
        _nr = [[] for _ in range(n_all)]; _br = [[] for _ in range(n_all)]
        for j, i in enumerate(sel_idx):
            _nr[i] = net_rows[j]; _br[i] = neb_rows[j]
        net_rows, neb_rows = _nr, _br
    cids, lu_tab, n_cells, cell_area = cids_all, lu_all, n_all, cell_area_all

    # --- verdict --------------------------------------------------------------
    decl_lu = lu_tab["lu"].astype(np.float64) / 255.0
    decl_unknown = (lu_tab["flags"] & 1).astype(bool) | (lu_tab["cov"] < int(0.9 * 255))
    obs_lu = lu_u8.astype(np.float64) / 255.0
    gi = {g: i for i, g in enumerate(GROUPS)}
    d_wald = decl_lu[:, gi["wald"]]; o_wald = obs_lu[:, gi["wald"]]
    d_geb = decl_lu[:, gi["geb"]] + decl_lu[:, gi["bau"]]
    d_green = decl_lu[:, gi["gruen"]] + decl_lu[:, gi["acker"]] + decl_lu[:, gi["wald"]]
    o_sealed = obs_lu[:, gi["verkehr"]] + obs_lu[:, gi["geb"]]
    o_green = obs_lu[:, gi["gruen"]] + obs_lu[:, gi["acker"]] + obs_lu[:, gi["wald"]]
    d_sealed = decl_lu[:, gi["verkehr"]] + decl_lu[:, gi["geb"]] + decl_lu[:, gi["bau"]]
    o_bld = obs_bld_cover.astype(np.float64) / 255.0
    verdict = np.full(n_cells, V_CONSISTENT, np.uint8)
    verdict[(d_wald >= 0.6) & (o_wald < 0.25)] = V_FOREST_LOSS
    verdict[(d_wald < 0.15) & (o_wald >= 0.6) & (d_green >= 0.5)] = V_FOREST_GAIN
    verdict[(o_sealed >= 0.5) & (d_green >= 0.7)] = V_SEALED
    verdict[(o_bld >= 0.15) & (d_geb < 0.03)] = V_BUILDING
    verdict[(o_green >= 0.7) & (d_sealed >= 0.6)] = V_GREEN
    verdict[decl_unknown | ~observed] = V_UNKNOWN

    # --- pack NEO1 -----------------------------------------------------------------
    _hb(f"pack NEO1 ({n_cells} cells)")
    h_mean_q = _q_u8(h_mean, 2.0); h_p90_q = _q_u8(h_p90, 2.0); h_max_q = _q_u8(h_max, 2.0)
    tree_h_q = _q_u8(tree_h_max, 2.0)
    crown_q = _q_u8(crown_frac, 254.0)
    ndvi_q = _q_u8((ndvi_mean + 1.0) / 2.0, 254.0)
    amp_q = _q_u8(ndvi_amp, 254.0)
    dh_q = np.full(n_cells, NODATA_I8, np.int8)
    m = np.isfinite(dh_mean)
    dh_q[m] = np.clip(np.round(dh_mean[m] * 4), -127, 127).astype(np.int8)
    dtmc_q = np.full(n_cells, NODATA_I8, np.int8)
    m = np.isfinite(dtmc_mean)
    dtmc_q[m] = np.clip(np.round(dtmc_mean[m] * 4), -127, 127).astype(np.int8)
    elev_q = np.full(n_cells, 65535, np.uint16)
    m = np.isfinite(elev_mean)
    elev_q[m] = np.clip(np.round(elev_mean[m]), 0, 65534).astype(np.uint16)
    slope_q = _q_u8(slope_mean, 1.0, hi=254); slope90_q = _q_u8(slope_p90, 1.0, hi=254)
    rough_q = _q_u8(rough_mean, 10.0, hi=254)
    tri_q = _q_u8(tri_mean, 10.0, hi=254)
    tpi_q = np.full(n_cells, NODATA_I8, np.int8)
    m = np.isfinite(tpi_mean)
    tpi_q[m] = np.clip(np.round(tpi_mean[m] * 4), -127, 127).astype(np.int8)
    hansen_q = hansen_year.copy()
    if not hansen_seen:
        hansen_q[:] = NODATA_U8
    # cells we never saw: everything nodata
    unseen = ~observed
    lu_u8[unseen] = 0; obs_bld_cover[unseen] = 0
    for arr in (h_mean_q, h_p90_q, h_max_q, tree_h_q, crown_q, ndvi_q, amp_q, rough_q, tri_q, hansen_q, pheno_dom):
        arr[unseen] = NODATA_U8
    dh_q[unseen] = NODATA_I8; dtmc_q[unseen] = NODATA_I8; tpi_q[unseen] = NODATA_I8
    tree_n_u8 = np.minimum(tree_n, 255).astype(np.uint8)
    bld_n_u8 = np.minimum(bld_n, 255).astype(np.uint8)
    neo = bytearray()
    for i in range(n_cells):
        neo += OBS_REC.pack(int(cids[i]), lu_u8[i].tobytes(), int(obs_bld_cover[i]), int(bld_n_u8[i]),
                            int(h_mean_q[i]), int(h_p90_q[i]), int(h_max_q[i]), int(tree_n_u8[i]),
                            int(tree_h_q[i]), int(crown_q[i]), int(ndvi_q[i]), int(amp_q[i]),
                            int(dh_q[i]), int(fy[i]), int(verdict[i]),
                            int(elev_q[i]), int(slope_q[i]), int(slope90_q[i]), int(aspect_dom[i]),
                            int(rough_q[i]), int(tri_q[i]), int(tpi_q[i]), int(dtmc_q[i]), int(hansen_q[i]), int(pheno_dom[i]))
    neo = bytes(neo)
    net = b"".join(struct.pack("<I", w) for c in range(n_cells) for w in net_rows[c])
    neb = b"".join(struct.pack("<Q", w) for c in range(n_cells) for w in neb_rows[c])

    epoch = datetime.now(timezone.utc).strftime("%Y-%m")
    kgz = str(kg or declared.header.get("kg", "")).zfill(5)
    common = dict(epoch=epoch, source=OBS_SOURCE, kg=kgz, h3_res=H3_RES, groups=list(GROUPS),
                  declared_digest=declared.digest, declared_epoch=declared.header.get("epoch"),
                  declared_source=declared.header.get("source"), declared_algo=declared.header.get("algo"))
    # real ALS acquisition years (header only, digest-neutral — als_years.py;
    # the row's flight_years bitmask is unchanged).  Centres already computed above.
    try:
        import als_years as _aly
        als_hdr = _aly.header_block_for_ids(cids_all, ce_all, cn_all)
    except Exception as _e:  # noqa: BLE001
        log.debug("ne_h3: als_years header skipped: %s", _e)
        als_hdr = None
    neo_hdr = dict(common, algo=ALGO_OBS, cells_n=n_cells, record=OBS_REC.format, record_bytes=OBS_REC.size,
                   fields=list(OBS_FIELDS), units=dict(OBS_UNITS), phenology=list(PHENOLOGY_CODES),
                   verdicts=list(VERDICTS), cells_observed=int(observed.sum()),
                   **({"als_years": als_hdr} if als_hdr else {}))
    net_hdr = dict(common, algo=ALGO_TREES, rows_n=n_trees_total, record="u32: dx:5 dy:5 h:8(0.25m) crown:6(log2*5) species:4 vitality:3 flag:1",
                   species=list(SPECIES_CODES), vitality=list(VITALITY_CODES), min_h_m=3.0,
                   origin="metre-snapped EPSG:3035 cell centre; rows in NEO1 cell order, count = tree_n")
    neb_hdr = dict(common, algo=ALGO_BLD, rows_n=n_bld_total,
                   record="u64: dx:5 dy:5 h_max:8(0.25m) h_robust:8(0.25m) area:16(m2) type:4 conf:4 dh:8(i8 0.25m)",
                   types=list(BLD_TYPES), origin="metre-snapped EPSG:3035 cell centre; rows in NEO1 cell order, count = obs_bld_n")

    # --- summary (for the KG JSON) ----------------------------------------------------
    share = lu_tab["kg_share"].astype(np.float64) / 255.0
    w_area = cell_area * share / 10_000.0            # ha, weighted by this KG's share
    decl_ha = {g: round(float((decl_lu[:, i] * w_area).sum()), 2) for i, g in enumerate(GROUPS)}
    obs_ha = {g: round(float((obs_lu[:, i] * w_area).sum()), 2) for i, g in enumerate(GROUPS)}
    verdict_ha = {VERDICTS[v]: round(float(w_area[verdict == v].sum()), 2) for v in range(len(VERDICTS))}
    verdict_n = {VERDICTS[v]: int((verdict == v).sum()) for v in range(len(VERDICTS))}
    _t_ok = np.isfinite(elev_mean)
    terrain_sum = {
        "cells_with_terrain": int(_t_ok.sum()),
        "elev_min_m": (round(float(np.nanmin(elev_mean)), 1) if _t_ok.any() else None),
        "elev_max_m": (round(float(np.nanmax(elev_mean)), 1) if _t_ok.any() else None),
        "slope_mean_deg": (round(float(np.nanmean(slope_mean)), 1) if np.isfinite(slope_mean).any() else None),
        "hansen_loss_cells": int((hansen_year > 0).sum()) if hansen_seen else None,
    }
    summary = {
        "algo": {"declared": declared.header.get("algo"), "observed": ALGO_OBS, "trees": ALGO_TREES,
                 "buildings": ALGO_BLD},
        "record_bytes": {"nec1": LU_REC.size, "neo1": OBS_REC.size, "net1": TREE_REC_BYTES, "neb1": BLD_REC_BYTES},
        "terrain": terrain_sum,
        "declared": {"digest": declared.digest, "epoch": declared.header.get("epoch"),
                     "source": declared.header.get("source"), "cells_n": n_cells,
                     "kcells_n": int(declared.header.get("kcells_n", 0)),
                     "declared_unknown_n": int(decl_unknown.sum())},
        "observed": {"digest": digest_of(neo), "epoch": epoch, "source": OBS_SOURCE,
                     "cells_observed": int(observed.sum()), "trees_n": n_trees_total,
                     "manmade_n": n_bld_total},
        "h3_res": H3_RES, "chunk_res": CHUNK_RES, "groups": list(GROUPS),
        "als": als_hdr,
        "declared_ha": decl_ha, "observed_ha": obs_ha,
        "verdict_ha": verdict_ha, "verdict_n": verdict_n,
        "attribution": declared.header.get("attribution"), "notice": declared.header.get("notice"),
        "seconds": round(time.time() - t0, 1),
    }
    log.info("ne_h3: %s observed %d/%d cells, %d trees, %d man-made, %.1fs",
             kgz, int(observed.sum()), n_cells, n_trees_total, n_bld_total, time.time() - t0)
    return {
        "neo1": pack_section(MAGIC_OBS, neo_hdr, neo),
        "net1": pack_section(MAGIC_TREES, net_hdr, net),
        "neb1": pack_section(MAGIC_BLD, neb_hdr, neb),
        "neo1_raw": neo, "net1_raw": net, "neb1_raw": neb,
        "tree_n": tree_n_u8, "bld_n": bld_n_u8, "verdict": verdict, "cell_ids": cids,
        "summary": summary,
    }


# === SECTION: .h3b product ===

def _shuffle(raw: bytes, rec: int) -> bytes:
    """Row-major fixed-size records → column-major bytes (byte shuffle): the
    deflater then sees runs of equal high bytes / nodata columns.  25–45 %
    smaller chunks on real NEO1/NEC1 data; exact inverse ``_unshuffle``."""
    if not raw or rec <= 1:
        return raw
    n = len(raw) // rec
    return np.frombuffer(raw, np.uint8, n * rec).reshape(n, rec).T.tobytes() + raw[n * rec:]


def _unshuffle(buf: bytes, rec: int, n: int) -> bytes:
    if n == 0 or rec <= 1:
        return buf[: n * rec]
    return np.frombuffer(buf, np.uint8, n * rec).reshape(rec, n).T.tobytes()


def write_h3b(path: str | Path, declared: Section, obs: dict, kg: str, product_version: str,
              extra_header: dict | None = None, level: int = 9) -> dict:
    """``<kg>_ne.h3b`` = H3B1 | ver u8 | hdr_len u32 | header JSON | chunks.

    Chunk = zlib(NEC1 LU rows | NEO1 rows | NET1 rows | NEB1 rows) of one res-10
    parent, in cell-id order.  The header's ``chunks[]`` directory carries
    offsets + row counts so a reader (or a Range request) inflates one chunk.
    NEC1 K records (res-10 statistics) are stored whole in ``k_records``.
    Returns the header."""
    cids = obs["cell_ids"]
    n = len(cids)
    lu_raw = declared.raw[: n * LU_REC.size]
    k_raw = declared.raw[n * LU_REC.size:]
    neo_raw, net_raw, neb_raw = obs["neo1_raw"], obs["net1_raw"], obs["neb1_raw"]
    tree_n, bld_n = obs["tree_n"].astype(np.int64), obs["bld_n"].astype(np.int64)
    tree_off = np.concatenate([[0], np.cumsum(tree_n)]); bld_off = np.concatenate([[0], np.cumsum(bld_n)])
    parents = h3_parent_vec(cids, CHUNK_RES)
    # cells are globally sorted → equal parents are contiguous
    bounds = np.flatnonzero(np.concatenate([[True], parents[1:] != parents[:-1], [True]]))
    body = bytearray()
    chunks = []
    for a, b in zip(bounds[:-1], bounds[1:]):
        a, b = int(a), int(b)
        part = (_shuffle(lu_raw[a * LU_REC.size:b * LU_REC.size], LU_REC.size)
                + _shuffle(neo_raw[a * OBS_REC.size:b * OBS_REC.size], OBS_REC.size)
                + _shuffle(net_raw[tree_off[a] * 4:tree_off[b] * 4], 4)
                + _shuffle(neb_raw[bld_off[a] * 8:bld_off[b] * 8], 8))
        z = zlib.compress(part, level)
        chunks.append({"r10": h3hex(parents[a]), "off": len(body), "len": len(z), "raw_len": len(part),
                       "cells": b - a, "trees": int(tree_off[b] - tree_off[a]),
                       "bld": int(bld_off[b] - bld_off[a]), "first": h3hex(cids[a])})
        body += z
    s = obs["summary"]
    hdr = {
        "magic": H3B_MAGIC.decode(), "fmt_ver": FMT_VER, "kg": str(kg).zfill(5),
        "product_version": product_version, "created_at": datetime.now(timezone.utc).isoformat(),
        "h3_res": H3_RES, "chunk_res": CHUNK_RES, "cells_n": n, "groups": list(GROUPS),
        "chunk_layout": "zlib(shuffle(NEC1 rows) | shuffle(NEO1 rows) | shuffle(NET1 rows) | shuffle(NEB1 rows)); "
                        "shuffle = byte-column-major transpose of the fixed-size records",
        "record_bytes": {"nec1": LU_REC.size, "neo1": OBS_REC.size, "net1": 4, "neb1": 8},
        # the FULL NEC1 header (since 2026-10-06; was a 13-key subset) so the
        # section can be re-created byte-for-byte for a NEC1+NEO1 /obs
        # re-post (``nec1_section_from_h3b``) — needed for bevdirect builds
        "nec1": dict(declared.header),
        "neo1": {"algo": ALGO_OBS, "digest": digest_of(neo_raw), "record": OBS_REC.format,
                 "epoch": s["observed"]["epoch"], "source": OBS_SOURCE},
        "net1": {"algo": ALGO_TREES, "digest": digest_of(net_raw), "rows_n": len(net_raw) // 4},
        "neb1": {"algo": ALGO_BLD, "digest": digest_of(neb_raw), "rows_n": len(neb_raw) // 8},
        "k_records": {"n": len(k_raw) // K_REC.size, "zlib_b64": __import__("base64").b64encode(zlib.compress(k_raw, 9)).decode()},
        "summary": s, "chunks": chunks,
    }
    if extra_header:
        hdr.update(extra_header)
    hdr["body_offset"] = 0
    for _ in range(6):   # fixed point: the offset's own digits change the header length
        hj = canonical_json(hdr)
        hdr_bytes = H3B_MAGIC + bytes([FMT_VER]) + struct.pack("<I", len(hj)) + hj
        if len(hdr_bytes) == hdr["body_offset"]:
            break
        hdr["body_offset"] = len(hdr_bytes)
    assert len(hdr_bytes) == hdr["body_offset"]
    p = Path(path)
    tmp = p.with_suffix(p.suffix + ".tmp")
    with open(tmp, "wb") as fh:
        fh.write(hdr_bytes)
        fh.write(body)
    tmp.replace(p)
    return hdr


def read_h3b_header(path: str | Path) -> dict:
    with open(path, "rb") as fh:
        head = fh.read(9)
        if head[:4] != H3B_MAGIC or head[4] != FMT_VER:
            raise ValueError("not an H3B1 file")
        (hl,) = struct.unpack_from("<I", head, 5)
        return json.loads(fh.read(hl).decode("utf-8"))


def read_h3b_chunk(path: str | Path, hdr: dict, chunk: dict) -> dict:
    """Inflate one chunk → {"lu": bytes, "obs": bytes, "trees": bytes, "bld": bytes}."""
    with open(path, "rb") as fh:
        fh.seek(hdr["body_offset"] + chunk["off"])
        raw = zlib.decompress(fh.read(chunk["len"]))
    n, t, b = chunk["cells"], chunk["trees"], chunk["bld"]
    rb = hdr.get("record_bytes") or {"nec1": LU_REC.size, "neo1": OBS_REC.size, "net1": 4, "neb1": 8}
    o1 = n * rb["nec1"]; o2 = o1 + n * rb["neo1"]; o3 = o2 + t * rb["net1"]
    shuffled = "shuffle" in str(hdr.get("chunk_layout", ""))
    if not shuffled:
        return {"lu": raw[:o1], "obs": raw[o1:o2], "trees": raw[o2:o3], "bld": raw[o3:o3 + b * rb["neb1"]]}
    return {"lu": _unshuffle(raw[:o1], rb["nec1"], n), "obs": _unshuffle(raw[o1:o2], rb["neo1"], n),
            "trees": _unshuffle(raw[o2:o3], rb["net1"], t), "bld": _unshuffle(raw[o3:o3 + b * rb["neb1"]], rb["neb1"], b)}


def verify_h3b(path: str | Path, expected_declared_digest: str | None = None) -> dict:
    """Structural check of a product file: every chunk inflates, concatenated
    layers reproduce the header digests, cell ids sorted, NEO1 cell set ==
    NEC1 cell set, row counts == tree_n / obs_bld_n sums.  Returns a report
    dict with ``ok`` and ``errors``."""
    errs = []
    hdr = read_h3b_header(path)
    lu = bytearray(); obs = bytearray(); trees = bytearray(); bld = bytearray()
    for ch in hdr["chunks"]:
        try:
            part = read_h3b_chunk(path, hdr, ch)
        except Exception as e:  # noqa: BLE001
            errs.append(f"chunk {ch.get('r10')}: {e}")
            continue
        lu += part["lu"]; obs += part["obs"]; trees += part["trees"]; bld += part["bld"]
    n = hdr["cells_n"]
    if len(lu) != n * LU_REC.size:
        errs.append(f"lu rows {len(lu) // LU_REC.size} != cells_n {n}")
    if len(obs) != n * OBS_REC.size:
        errs.append(f"obs rows {len(obs) // OBS_REC.size} != cells_n {n}")
    if not errs:
        import base64
        k_raw = zlib.decompress(base64.b64decode(hdr["k_records"]["zlib_b64"]))
        d_lu = digest_of(bytes(lu) + k_raw)
        if d_lu != hdr["nec1"]["digest"]:
            errs.append(f"NEC1 digest {d_lu} != header {hdr['nec1']['digest']}")
        if expected_declared_digest and d_lu != expected_declared_digest:
            errs.append(f"NEC1 digest {d_lu} != expected {expected_declared_digest}")
        if digest_of(bytes(obs)) != hdr["neo1"]["digest"]:
            errs.append("NEO1 digest mismatch")
        if digest_of(bytes(trees)) != hdr["net1"]["digest"]:
            errs.append("NET1 digest mismatch")
        if digest_of(bytes(bld)) != hdr["neb1"]["digest"]:
            errs.append("NEB1 digest mismatch")
        a = np.frombuffer(bytes(lu), np.uint8).reshape(n, LU_REC.size)[:, :8].copy().view("<u8").reshape(n)
        b = np.frombuffer(bytes(obs), np.uint8).reshape(n, OBS_REC.size)[:, :8].copy().view("<u8").reshape(n)
        if not np.array_equal(a, b):
            errs.append("NEO1 cell set != NEC1 cell set")
        if n > 1 and not np.all(a[1:] > a[:-1]):
            errs.append("cell ids not sorted")
        ob = np.frombuffer(bytes(obs), np.uint8).reshape(n, OBS_REC.size)
        if int(ob[:, 18].astype(np.int64).sum()) != len(bld) // 8:
            errs.append("NEB1 rows != Σ obs_bld_n")
        if int(ob[:, 22].astype(np.int64).sum()) != len(trees) // 4:
            errs.append("NET1 rows != Σ tree_n")
        if int((ob[:, 29] > 6).sum()):
            errs.append("verdict code out of range")
    return {"ok": not errs, "errors": errs, "cells_n": n, "chunks": len(hdr["chunks"]),
            "trees_n": len(trees) // 4, "bld_n": len(bld) // 8, "header": hdr}


def neo1_rows_from_h3b(path: str | Path, green_fix: bool = True) -> tuple[dict, bytes, int]:
    """``(header, neo1_rows, n_fixed)`` of a product file.  With *green_fix*
    (default) the read-time green-ground override (``ne_green_fix``) is
    applied per chunk — the file itself is never modified."""
    hdr = read_h3b_header(path)
    obs = bytearray()
    n_fix = 0
    if green_fix:
        import ne_green_fix as _gf
    for ch in hdr["chunks"]:
        part = read_h3b_chunk(path, hdr, ch)
        o = part["obs"]
        if green_fix:
            o, k = _gf.fix_rows(o, part["lu"])
            n_fix += k
        obs += o
    return hdr, bytes(obs), n_fix


def neo1_section_from_h3b(path: str | Path, green_fix: bool = True) -> bytes:
    """Re-create the NEO1 section (for POST /obs) from a product file.
    ``green_fix`` applies the read-time override (``ne_green_fix``; header
    keys ``green_fix*`` document it)."""
    hdr, obs, n_fix = neo1_rows_from_h3b(path, green_fix=green_fix)
    h = dict(algo=ALGO_OBS, epoch=hdr["neo1"]["epoch"], source=OBS_SOURCE, kg=hdr["kg"], h3_res=H3_RES,
             cells_n=hdr["cells_n"], groups=list(GROUPS), record=OBS_REC.format, record_bytes=OBS_REC.size,
             fields=list(OBS_FIELDS), units=dict(OBS_UNITS), phenology=list(PHENOLOGY_CODES),
             verdicts=list(VERDICTS), declared_digest=hdr["nec1"]["digest"],
             declared_source=hdr["nec1"].get("source"))
    if green_fix:
        import ne_green_fix as _gf
        h["source"] = OBS_SOURCE_FIXED      # umfeld's manifest shows which KGs carry the corrected export
        h.update(_gf.header_note(n_fix, hdr["cells_n"]))
    als = _als_header_for(hdr, obs)
    if als:
        h["als_years"] = als                # header only — digest covers the rows, no re-post needed
    return pack_section(MAGIC_OBS, h, obs)


def _als_header_for(hdr: dict, obs: bytes) -> dict | None:
    """``als_years`` header block for a re-POST: primary cache → product
    header summary → compute from the row ids (last resort, ~1 µs/cell)."""
    try:
        import als_years as _aly
        kg = str(hdr.get("kg", "")).zfill(5)
        b = _aly.cached(kg)
        if b and b.get("neo1_digest") == (hdr.get("neo1") or {}).get("digest"):
            return {k: b.get(k) for k in ("newest", "oldest", "source", "coverage", "dominant", "years", "mosaics", "algo")} | {
                "precision": b.get("precision")}
        s_als = (hdr.get("summary") or {}).get("als")
        if s_als:
            return s_als
        n = int(hdr["cells_n"])
        ids = np.frombuffer(obs, dtype=np.uint8, count=n * OBS_REC.size).reshape(n, OBS_REC.size)[:, :8].copy().view("<u8").reshape(n)
        return _aly.header_block_for_ids(ids)
    except Exception as e:  # noqa: BLE001
        log.debug("ne_h3: als_years header: %s", e)
        return None


# === SECTION: declared view (umfeld chunk-log view, 2026-10-27) ===

def fetch_declared_view(kg: str, timeout: float = 120.0) -> "Section | None":
    """``GET /contrib/api/v1/ne/{kg}/declared`` → the NEC1 section umfeld
    assembles from its chunk log (``source: chunks@<state>``, header
    ``view{state,hidden,…}``, register bytes zero).  This is the cell set a
    bare NEO1 is validated against since 2026-10-27 (ne-change.md §4b).
    ``None`` when the KG holds nothing (404).  ~0.2 MB per KG, never cached
    on disk — call it only right before a POST."""
    import requests
    kg = str(kg).zfill(5)
    url = f"{_umfeld_contrib_base()}/{kg}/declared"
    last = None
    for attempt in range(3):
        try:
            r = requests.get(url, headers=_umfeld_headers(), timeout=timeout)
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 + 3 * attempt)
            continue
        if r.status_code == 404:
            return None
        if r.status_code in (429, 500, 502, 503, 504):
            last = NEError(f"umfeld {kg}/declared: HTTP {r.status_code}")
            time.sleep(5 + 10 * attempt)
            continue
        if r.status_code in (401, 403):
            raise NEError(f"umfeld {kg}/declared: HTTP {r.status_code} — token refused")
        r.raise_for_status()
        secs = [s for s in unpack_sections(r.content) if s.magic == MAGIC_LU]
        if not secs:
            raise NEError(f"umfeld {kg}/declared: no NEC1 section in answer")
        sec = secs[0]
        sec.header.setdefault("_etag", r.headers.get("ETag"))
        return sec
    raise NEError(f"umfeld {kg}/declared: {last}")


def unseen_obs_row(cid: int) -> bytes:
    """An all-nodata NEO1 row (what the builder writes for a declared cell
    no raster covered — ``ne_ingest.row_observed`` → False)."""
    return OBS_REC.pack(int(cid), bytes(9), 0, 0, NODATA_U8, NODATA_U8, NODATA_U8, 0, NODATA_U8, NODATA_U8,
                        NODATA_U8, NODATA_U8, NODATA_I8, 0, V_CONSISTENT,
                        65535, NODATA_U8, NODATA_U8, NODATA_U8, NODATA_U8, NODATA_U8, NODATA_I8, NODATA_I8,
                        NODATA_U8, NODATA_U8)


def neo1_section_on_view(path: str | Path, view: "Section", green_fix: bool = True) -> tuple[bytes, dict]:
    """NEO1 section for POST /obs whose cell set is exactly *view*'s (the
    ``/declared`` NEC1): our rows re-keyed onto the view's ids in the view's
    order; cells the view lacks are dropped (they would 409 — superseded or
    moved to a neighbour KG), view cells we never observed get an unseen
    (all-nodata) row.  Returns ``(section, {cells_n, dropped, filled, digest,
    state})``.  Header ``declared_digest/source`` name the view."""
    hdr, obs, n_fix = neo1_rows_from_h3b(path, green_fix=green_fix)
    n = len(obs) // OBS_REC.size
    ob = np.frombuffer(obs, np.uint8).reshape(n, OBS_REC.size)
    ours = ob[:, :8].copy().view("<u8").reshape(n)
    vn = int(view.header["cells_n"])
    vids = np.frombuffer(view.raw[:vn * LU_REC.size], np.uint8).reshape(vn, LU_REC.size)[:, :8].copy().view("<u8").reshape(vn)
    order = np.argsort(ours, kind="stable")
    pos = np.searchsorted(ours[order], vids)
    pos = np.minimum(pos, max(n - 1, 0))
    hit = (n > 0) & (ours[order][pos] == vids)
    out = np.empty((vn, OBS_REC.size), np.uint8)
    if n:
        out[hit] = ob[order[pos[hit]]]
    miss = np.flatnonzero(~hit)
    for i in miss:
        out[i] = np.frombuffer(unseen_obs_row(int(vids[i])), np.uint8)
    raw = out.tobytes()
    h = dict(algo=ALGO_OBS, epoch=hdr["neo1"]["epoch"], source=OBS_SOURCE, kg=hdr["kg"], h3_res=H3_RES,
             cells_n=vn, groups=list(GROUPS), record=OBS_REC.format, record_bytes=OBS_REC.size,
             fields=list(OBS_FIELDS), units=dict(OBS_UNITS), phenology=list(PHENOLOGY_CODES),
             verdicts=list(VERDICTS), declared_digest=view.header.get("digest"),
             declared_source=view.header.get("source"),
             view_rekey={"dropped": int(n - int(hit.sum())), "filled": int(len(miss)),
                         "product_cells_n": int(n), "product_declared_digest": hdr["nec1"]["digest"]})
    if green_fix:
        import ne_green_fix as _gf
        h["source"] = OBS_SOURCE_FIXED
        h.update(_gf.header_note(n_fix, n))
    st = (view.header.get("view") or {}).get("state")
    stats = {"cells_n": vn, "dropped": int(n - int(hit.sum())), "filled": int(len(miss)), "digest": digest_of(raw),
             "state": st, "view_digest": view.header.get("digest"), "hidden": (view.header.get("view") or {}).get("hidden")}
    return pack_section(MAGIC_OBS, h, raw), stats


def neo1_fixed_digest(path: str | Path) -> tuple[str, int]:
    """``(digest of the green-fixed NEO1 rows, n cells touched)``."""
    _, obs, n_fix = neo1_rows_from_h3b(path, green_fix=True)
    return digest_of(obs), n_fix


def nec1_section_from_h3b(path: str | Path) -> bytes:
    """Re-create the declared NEC1 section from a product file (LU rows from
    the chunks + K records from the header).  Needs the full NEC1 header
    (files written since 2026-10-06); older files raise NEError."""
    import base64
    hdr = read_h3b_header(path)
    nh = dict(hdr["nec1"])
    if not all(k in nh for k in ("kg", "groups", "h3_res", "digest", "cells_n")):
        raise NEError("h3b carries only the legacy NEC1 header subset — cannot rebuild the section")
    lu = bytearray()
    for ch in hdr["chunks"]:
        lu += read_h3b_chunk(path, hdr, ch)["lu"]
    k_raw = zlib.decompress(base64.b64decode(hdr["k_records"]["zlib_b64"])) if hdr["k_records"]["n"] else b""
    sec = Section(MAGIC_LU, nh, bytes(lu) + k_raw)
    if sec.digest != nh["digest"]:
        raise NEError(f"rebuilt NEC1 digest {sec.digest} != header {nh['digest']}")
    return pack_section(MAGIC_LU, nh, sec.raw)


def json_section(obs: dict, h3b_hdr: dict | None = None) -> dict:
    """The ``ne`` section of the KG JSON (summary only; cells live in the .h3b)."""
    s = dict(obs["summary"])
    s["product"] = "<kg>_ne.h3b (Zenodo product key _ne_h3b): res-10 chunks of NEC1|NEO1|NET1|NEB1"
    if h3b_hdr:
        s["h3b"] = {"chunks": len(h3b_hdr.get("chunks") or []), "net1_rows": h3b_hdr["net1"]["rows_n"],
                    "neb1_rows": h3b_hdr["neb1"]["rows_n"]}
    return s


if __name__ == "__main__":  # small CLI: fetch / verify
    import argparse
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch"); f.add_argument("kg"); f.add_argument("-o", default=None)
    v = sub.add_parser("verify"); v.add_argument("path")
    m = sub.add_parser("manifest")
    t = sub.add_parser("tooling"); t.add_argument("--install", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO)
    if a.cmd == "fetch":
        sec = fetch_declared(a.kg)
        print(json.dumps({k: sec.header.get(k) for k in ("kg", "algo", "epoch", "source", "digest", "cells_n", "kcells_n")}))
        if a.o:
            Path(a.o).write_bytes(pack_section(MAGIC_LU, sec.header, sec.raw))
    elif a.cmd == "verify":
        r = verify_h3b(a.path); r.pop("header", None); print(json.dumps(r, indent=1))
    elif a.cmd == "manifest":
        mf, et = fetch_manifest(); print(json.dumps({"built": mf.get("built"), "total": mf.get("kgs_total"), "queue": mf.get("queue"), "etag": et}))
    elif a.cmd == "tooling":
        print(json.dumps(ensure_bevdirect_tooling(install=a.install), indent=1))
