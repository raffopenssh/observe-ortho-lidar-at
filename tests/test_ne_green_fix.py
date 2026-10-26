"""ne_green_fix: bau/alpen on S2-green cells → gruen, verdict recomputed, idempotent, files untouched."""
import os, sys, struct
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
import ne_green_fix as g


def _row(cid, lu, ndvi=0.7, pheno=3, verdict=0, bld_cover=0):
    r = np.zeros(41, np.uint8)
    r[:8] = np.frombuffer(struct.pack("<Q", cid), np.uint8)
    r[8:17] = lu
    r[17] = bld_cover
    r[25] = 255 if ndvi is None else int(round((ndvi + 1) / 2 * 254))
    r[29] = verdict; r[40] = pheno
    return r


def _lu(cid, decl, flags=0, cov=255):
    r = np.zeros(30, np.uint8)
    r[:8] = np.frombuffer(struct.pack("<Q", cid), np.uint8)
    r[8] = 255; r[10] = cov; r[11] = flags; r[12:21] = decl
    return r


def main():
    gi = g._GI
    rows = [
        _row(1, [0, 206, 0, 48, 0, 0, 0, 0, 0], ndvi=0.67),             # the 63330 case → gruen 254
        _row(2, [0, 200, 0, 0, 0, 0, 0, 0, 0], ndvi=0.2),               # genuine car park, dull → untouched
        _row(3, [0, 0, 0, 0, 0, 0, 0, 255, 0], ndvi=0.6),               # rock on green → gruen
        _row(4, [0, 100, 0, 0, 0, 0, 0, 0, 0], ndvi=0.8, pheno=5),       # road_or_bare phenology → untouched
        _row(5, [0, 100, 0, 0, 0, 0, 0, 0, 0], ndvi=None),              # ndvi nodata → untouched
        _row(6, [0, 0, 0, 255, 0, 0, 0, 0, 0], ndvi=0.9),               # already green → untouched
        _row(7, [0, 255, 0, 0, 0, 0, 0, 0, 0], ndvi=0.7, verdict=6),    # unknown verdict stays unknown
    ]
    obs = np.stack(rows).tobytes()
    decl = [0] * 9; decl[gi["bau"]] = 255                               # declared Bauland everywhere
    lu = np.stack([_lu(i, decl) for i in range(1, 8)]).tobytes()
    fixed, n = g.fix_rows(obs, lu)
    assert n == 3, n
    f = np.frombuffer(fixed, np.uint8).reshape(-1, 41)
    o = np.frombuffer(obs, np.uint8).reshape(-1, 41)
    assert list(f[0, 8:17]) == [0, 0, 0, 254, 0, 0, 0, 0, 0]
    assert f[0, 29] == g._V_GREEN, f[0, 29]                              # observed green, declared sealed
    assert (f[1] == o[1]).all() and (f[3] == o[3]).all() and (f[4] == o[4]).all() and (f[5] == o[5]).all()
    assert f[2, 8 + gi["gruen"]] == 255 and f[2, 8 + gi["alpen"]] == 0
    assert f[6, 29] == 6 and f[6, 8 + gi["gruen"]] == 255
    assert (f[:, :8] == o[:, :8]).all() and (f[:, 17:29] == o[:, 17:29]).all() and (f[:, 30:] == o[:, 30:]).all()
    # idempotent + no-op returns the same object
    again, n2 = g.fix_rows(fixed, lu)
    assert n2 == 0 and again is fixed
    noop, n3 = g.fix_rows(np.stack(rows[1:2]).tobytes())
    assert n3 == 0
    # without lu: mix moves, verdict untouched
    f2, _ = g.fix_rows(obs)
    f2 = np.frombuffer(f2, np.uint8).reshape(-1, 41)
    assert f2[0, 29] == 0 and list(f2[0, 8:17]) == [0, 0, 0, 254, 0, 0, 0, 0, 0]
    st = g.stats(obs)
    assert st["touched"] == 3 and st["bau_green_cells"] == 3 and st["alpen_green_cells"] == 1, st
    print("ne_green_fix OK")


if __name__ == "__main__":
    main()
