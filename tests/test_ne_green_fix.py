"""ne_green_fix: bau/alpen on S2-green cells → gruen, verdict recomputed, idempotent, files untouched."""
import os, sys, struct
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
import ne_green_fix as g


def _row(cid, lu, ndvi=0.7, pheno=3, verdict=0, bld_cover=0, crown=0.0, tree_h=None, h_max=None, h_p90=None):
    r = np.zeros(41, np.uint8)
    r[:8] = np.frombuffer(struct.pack("<Q", cid), np.uint8)
    r[8:17] = lu
    r[17] = bld_cover
    r[20] = 255 if h_p90 is None else int(round(h_p90 * 2))
    r[21] = 255 if h_max is None else int(round(h_max * 2))
    r[23] = 255 if tree_h is None else int(round(tree_h * 2))
    r[24] = 255 if crown is None else int(round(crown * 254))
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
    tree_rule()
    print("ne_green_fix OK")


def tree_rule():
    """LiDAR tree rule: closed canopy labelled rock/parking/meadow → wald, before the green rule."""
    gi = g._GI
    W = [0] * 9; W[gi["wald"]] = 255
    rows = [
        _row(1, [0, 0, 0, 4, 0, 0, 0, 250, 0], ndvi=0.575, pheno=1, verdict=1, crown=84 / 254, h_p90=10.5, h_max=13.5),  # 80007 case (raw)
        _row(2, [0, 0, 0, 0, 0, 0, 0, 255, 0], ndvi=0.1, crown=0.0, h_p90=0.5, h_max=1.0),     # real rock → untouched
        _row(3, [0, 0, 0, 0, 0, 0, 0, 255, 0], ndvi=0.6, crown=0.0, h_p90=0.0, h_max=0.5),     # rock on green, no canopy → green rule → gruen
        _row(4, [0, 0, 0, 200, 0, 55, 0, 0, 0], ndvi=0.7, crown=0.6, h_p90=9.0, h_max=12.0),   # meadow under canopy → wald (wasser kept)
        _row(5, [0, 0, 0, 255, 0, 0, 0, 0, 0], ndvi=0.7, crown=0.7, h_p90=3.0, h_max=4.5),     # low shrub canopy → untouched
        _row(6, [0, 0, 0, 0, 0, 0, 0, 255, 0], ndvi=0.7, crown=0.9, h_p90=20.0, pheno=5),      # road_or_bare phenology → untouched
        _row(7, [0, 100, 0, 0, 100, 0, 0, 0, 0], ndvi=0.7, crown=0.29, h_p90=20.0),            # crown just below → green rule only (bau→gruen)
        _row(8, [0, 0, 0, 0, 0, 0, 0, 255, 0], ndvi=0.7, crown=0.5, h_p90=7.5, h_max=30.0),    # one tall tree, p90 < 8 → green rule
        _row(9, [0, 0, 0, 0, 0, 0, 0, 255, 0], ndvi=0.3, verdict=6, crown=0.8, h_p90=15.0),    # unknown verdict stays
    ]
    obs = np.stack(rows).tobytes()
    lu = np.stack([_lu(i, W) for i in range(1, 10)]).tobytes()     # declared Wald everywhere
    fixed, nt, ng = g.fix_rows_detail(obs, lu)
    f = np.frombuffer(fixed, np.uint8).reshape(-1, 41)
    o = np.frombuffer(obs, np.uint8).reshape(-1, 41)
    assert (nt, ng) == (3, 3), (nt, ng)
    assert list(f[0, 8:17]) == [0, 0, 0, 0, 254, 0, 0, 0, 0] and f[0, 29] == g._V_CONSISTENT, f[0]
    assert (f[1] == o[1]).all()
    assert f[2, 8 + gi["gruen"]] == 255 and f[2, 8 + gi["wald"]] == 0 and f[2, 29] == g._V_FOREST_LOSS
    assert list(f[3, 8:17]) == [0, 0, 0, 0, 200, 55, 0, 0, 0] and f[3, 29] == g._V_CONSISTENT
    assert (f[4] == o[4]).all() and (f[5] == o[5]).all()
    assert list(f[6, 8:17]) == [0, 0, 0, 100, 100, 0, 0, 0, 0]
    assert f[7, 8 + gi["gruen"]] == 255
    assert f[8, 8 + gi["wald"]] == 255 and f[8, 29] == 6
    assert (f[:, :8] == o[:, :8]).all() and (f[:, 17:29] == o[:, 17:29]).all() and (f[:, 30:] == o[:, 30:]).all()
    again, n2 = g.fix_rows(fixed, lu)
    assert n2 == 0 and again is fixed                                   # idempotent
    st = g.stats(obs)
    assert st["tree_touched"] == 3 and st["touched"] == 6, st
    hn = g.header_note(6, 9, 3)
    assert hn["tree_fix_cells"] == 3 and "tree_fix_rule" in hn and hn["green_fix"] == g.FIX_ID
    # the real 80007 cell, if the corpus is on this box
    import os
    p = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "ne", "80007.h3b")
    if os.path.exists(p):
        import ne_h3
        cid = int("8c1f88472a0b3ff", 16)
        hdr, rows_fixed, n = ne_h3.neo1_rows_from_h3b(p, green_fix=True)
        fr = np.frombuffer(rows_fixed, np.uint8).reshape(-1, 41)
        ids = fr[:, :8].copy().view("<u8").reshape(-1)
        k = np.nonzero(ids == cid)[0]
        assert len(k) == 1, "80007 cell not in product"
        r = fr[k[0]]
        assert r[8 + gi["wald"]] >= 250 and r[8 + gi["alpen"]] == 0 and r[8 + gi["gruen"]] == 0, list(r[8:17])
        assert r[29] != g._V_FOREST_LOSS, r[29]
        print("  80007/8c1f88472a0b3ff:", list(r[8:17]), "verdict", r[29])


if __name__ == "__main__":
    main()
