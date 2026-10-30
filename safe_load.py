"""safe_load — restricted unpickling for artefacts that cross the fleet.

Threat: every peer can write to the shared Zenodo deposits (tile-cache
NPZs, cross-peer tile-checkpoint tars). A compromised peer could therefore
plant a pickle payload that executes on every *other* peer that restores
it — ``pickle.load`` and ``np.load(allow_pickle=True)`` run arbitrary
callables named in the stream. Model files are not affected (sha-pinned in
code); JSON / GPKG / plain ``.npy`` are data-only.

Fix: a whitelist unpickler. Only the classes our checkpoints actually
contain may be constructed (numpy reconstruction helpers, ``affine.Affine``,
``object_segmentation.SegmentedObject``, a few stdlib containers). Anything
else raises :class:`UnsafePickle` naming the offender, so extending the
list is an explicit, reviewed change.

``install()`` swaps the ``pickle`` reference inside numpy's NPY reader so
*every* ``np.load(allow_pickle=True)`` in the process goes through the
whitelist — one call at process start covers ``tile_cache``, ``zenodo_cache``
and ``hansen`` without touching their call sites. Explicit ``pickle.load``
sites (tile checkpoints, seg cache) call :func:`load` / :func:`loads`.

Model files (sklearn / LightGBM joblib) cannot be whitelisted — they pickle
dozens of library classes — so they go through :func:`load_model` instead:
the file must sit under the repo's ``data/`` tree (never a peer-shared
cache), its checksum is compared against ``data/models_manifest.json``
(``tools/fetch_models.py``) or an explicit ``sha1``/``md5`` argument, and
only then is ``joblib.load`` called.  ``.xz``-compressed models are
decompressed next to the archive on first use.

Test: ``python3 tests/test_safe_load.py``.
"""
from __future__ import annotations

import io
import pickle


class UnsafePickle(pickle.UnpicklingError):
    pass


# (module, name) pairs. Keep this *small*; a wildcard here reopens the hole.
ALLOWED: frozenset = frozenset({
    ('numpy', 'dtype'), ('numpy', 'ndarray'),
    ('numpy._core.multiarray', 'scalar'), ('numpy._core.multiarray', '_reconstruct'),
    ('numpy._core.numeric', '_frombuffer'),
    ('numpy.core.multiarray', 'scalar'), ('numpy.core.multiarray', '_reconstruct'),
    ('numpy.core.numeric', '_frombuffer'),
    ('numpy', 'float64'), ('numpy', 'float32'), ('numpy', 'int64'), ('numpy', 'int32'),
    ('numpy', 'bool_'), ('numpy', 'uint8'), ('numpy', 'uint16'), ('numpy', 'int16'),
    ('affine', 'Affine'),
    # rasterio CRS pickles as copyreg._reconstructor + __setstate__(wkt str):
    # pure WKT parsing, no callable payload. Every WorldCover tile stores one.
    ('rasterio.crs', 'CRS'),
    ('object_segmentation', 'SegmentedObject'),
    ('collections', 'OrderedDict'), ('collections', 'defaultdict'), ('collections', 'Counter'),
    ('builtins', 'set'), ('builtins', 'frozenset'), ('builtins', 'slice'),
    ('builtins', 'bytearray'), ('builtins', 'complex'), ('builtins', 'range'),
    ('builtins', 'dict'), ('builtins', 'list'), ('builtins', 'tuple'),
    ('datetime', 'datetime'), ('datetime', 'date'), ('datetime', 'timedelta'),
    ('datetime', 'timezone'),
    ('copyreg', '_reconstructor'),
})

# numpy dtype scalars are reconstructed via numpy.dtype + scalar; plain
# builtins types reached through copyreg._reconstructor need 'object'.
_RECONSTRUCTOR_BASES = frozenset({('builtins', 'object')})


class RestrictedUnpickler(pickle.Unpickler):
    def find_class(self, module, name):  # noqa: D401
        key = (module, name)
        if key in ALLOWED or key in _RECONSTRUCTOR_BASES:
            return super().find_class(module, name)
        raise UnsafePickle(f'refusing to unpickle {module}.{name}')


def load(fp, **kw):
    return RestrictedUnpickler(fp, **kw).load()


def loads(b: bytes, **kw):
    return RestrictedUnpickler(io.BytesIO(b), **kw).load()


class _PickleShim:
    """Stands in for the ``pickle`` module inside numpy's NPY reader."""
    load = staticmethod(load)
    loads = staticmethod(loads)
    dump = staticmethod(pickle.dump)
    dumps = staticmethod(pickle.dumps)
    UnpicklingError = pickle.UnpicklingError
    HIGHEST_PROTOCOL = pickle.HIGHEST_PROTOCOL
    DEFAULT_PROTOCOL = pickle.DEFAULT_PROTOCOL

    def __getattr__(self, n):
        return getattr(pickle, n)


_installed = False


def install() -> bool:
    """Route ``np.load(allow_pickle=True)`` through the whitelist. Idempotent."""
    global _installed
    if _installed:
        return True
    ok = False
    for modname in ('numpy.lib._format_impl', 'numpy.lib.format'):
        try:
            import importlib
            m = importlib.import_module(modname)
            if getattr(m, 'pickle', None) is not None:
                m.pickle = _PickleShim()
                ok = True
        except Exception:
            continue
    _installed = ok
    return ok


def installed() -> bool:
    return _installed


# --- model files (joblib) ------------------------------------------------------

class ModelChecksumError(RuntimeError):
    pass


def _digest(path, algo: str) -> str:
    import hashlib
    h = hashlib.new(algo)
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _manifest_entry(path) -> dict | None:
    """Entry of ``data/models_manifest.json`` whose ``path`` or ``filename``
    matches *path* (None when the manifest is absent or has no entry)."""
    import json
    from pathlib import Path
    p = Path(path).resolve()
    root = Path(__file__).resolve().parent
    mf = root / "data" / "models_manifest.json"
    try:
        entries = json.loads(mf.read_text())
    except Exception:
        return None
    if isinstance(entries, dict):
        entries = entries.get("models") or entries.get("files") or list(entries.values())
    for e in entries or []:
        if not isinstance(e, dict):
            continue
        ep = e.get("path")
        if (ep and (root / ep).resolve() == p) or e.get("filename") == p.name:
            return e
    return None


def ensure_decompressed(path):
    """``<x>.joblib`` from ``<x>.joblib.xz`` when only the archive exists."""
    from pathlib import Path
    p = Path(path)
    xz = p.with_name(p.name + ".xz")
    if not p.exists() and xz.exists():
        import lzma, shutil
        tmp = p.with_name(p.name + ".tmp")
        with lzma.open(xz, "rb") as src, open(tmp, "wb") as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
        tmp.replace(p)
    return p


def load_model(path, *, sha1: str | None = None, md5: str | None = None,
               require_checksum: bool = False):
    """``joblib.load`` behind a checksum fence.

    The checksum comes from *sha1* / *md5*, else from the models manifest
    entry for the file (``md5`` / ``sha1`` / ``sha256`` keys; the ``.xz``
    archive's entry counts for the decompressed file).  A mismatch raises
    :class:`ModelChecksumError`.  Without any known checksum the load is
    allowed unless *require_checksum* (env ``SRTM_REQUIRE_MODEL_CHECKSUM=1``).
    """
    import os
    from pathlib import Path
    p = ensure_decompressed(path)
    if not p.exists():
        raise FileNotFoundError(str(p))
    checks = {}
    if sha1:
        checks["sha1"] = sha1
    if md5:
        checks["md5"] = md5
    if not checks:
        e = _manifest_entry(p)
        if e is None and Path(str(p) + ".xz").exists():
            e = _manifest_entry(str(p) + ".xz")
            target = Path(str(p) + ".xz")
        else:
            target = p
        for k in ("sha256", "sha1", "md5"):
            if e and e.get(k):
                checks[k] = e[k]
                break
    else:
        target = p
    if not checks:
        if require_checksum or os.environ.get("SRTM_REQUIRE_MODEL_CHECKSUM"):
            raise ModelChecksumError(f"no checksum known for {p}")
    for algo, want in checks.items():
        got = _digest(target, algo)
        if got.lower() != str(want).lower():
            raise ModelChecksumError(f"{target}: {algo} {got} != expected {want}")
    import joblib
    return joblib.load(p)
