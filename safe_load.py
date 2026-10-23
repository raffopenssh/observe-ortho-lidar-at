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
