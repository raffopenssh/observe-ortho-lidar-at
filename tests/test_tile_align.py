"""Regression: `_read_dtm_for_tile` must return rasters on the tile's own
pixel grid.  The old padded re-read started 5 m further NW and was pasted at
the tile offset → full-GPKG DTM 5 m off → grid25 of every upgrade product
misregistered → hillshade seams (docs/hillshade-seams.md)."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
from rasterio.transform import from_origin, array_bounds
import austria_processor as ap

L, T, th, tw = 4290000.0, 2680000.0, 60, 80
tr = {"bounds_3035": array_bounds(th, tw, from_origin(L, T, 1, 1)), "shape": (th, tw)}
def truth(left, top, h, w):
    yy, xx = np.mgrid[0:h, 0:w]
    return ((left + xx + 0.5) * 0.01 + (top - yy - 0.5) * 0.02).astype(np.float32)
want = truth(L, T, th, tw)
# 1) padded read (origin 5 m NW, +10 px) → cropped onto the tile grid
pad = truth(L - 5, T + 5, th + 10, tw + 10)
td = {"dtm": pad, "dsm": pad + 1, "ndsm": pad * 0 + 1, "mask": np.ones(pad.shape, bool),
      "transform": from_origin(L - 5, T + 5, 1, 1), "shape": pad.shape}
out = ap._align_tdata_to_tile(td, tr)
assert out["dtm"].shape == (th, tw) and np.allclose(out["dtm"], want), "padded read not aligned"
assert (out["transform"].c, out["transform"].f) == (L, T) and out["mask"].all()
# 2) aligned input is returned untouched
ok = {"dtm": want, "transform": from_origin(L, T, 1, 1), "shape": (th, tw)}
assert ap._align_tdata_to_tile(ok, tr) is ok
# 3) transform-less multi-date sidecar from a padded read → centre crop
out = ap._align_tdata_to_tile({"dtm": pad, "dsm": pad}, tr)
assert np.allclose(out["dtm"], want)
# 4) short read → NaN-padded, still registered
sh = truth(L + 3, T - 2, th - 2, tw - 3)
out = ap._align_tdata_to_tile({"dtm": sh, "transform": from_origin(L + 3, T - 2, 1, 1)}, tr)
assert out["dtm"].shape == (th, tw) and np.allclose(out["dtm"][2:, 3:], want[2:, 3:]) and np.isnan(out["dtm"][0, 0])
print("ok")
