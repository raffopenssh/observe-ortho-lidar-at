"""tree_rule — the static apex-height rule, ONE implementation for every caller.

An apex is dropped when it is saturated (>= ``H_SATURATED`` m: the quantisation ceiling, ~90 %
masts / turbines / cranes) or a spike: >= ``SPIKE_H`` m and either h / p90 > ``SPIKE_RATIO`` or
more than ``SPIKE_GAP`` m above the second tallest apex of its res-10 chunk (derivation: see
``ne_trees``).  Heights are the stored NET1 heights (0.25 m steps, as ``cells_api.decode_trees``).

Callers: ``ne_h3.write_h3b`` (every writer: builder on peers, 2.4 fast path, block merge)
evaluates it per chunk and records the hidden NET1 row indices in the h3b header
(``net1.tree_rule`` = :func:`marker`) — the file keeps EVERY apex, so ``realistic=0`` stays the
raw inventory; ``ne_trees._decode_chunk_uncached`` (/trees/bbox realistic=1, and through
``ne_trees.chunk_keep`` the ``tree_qa`` store → cell products) takes :func:`chunk_keep`: the
header's verdict, or the rule itself for files written without a marker.  Pure numpy.

``RULE_ID`` is THE id to bump when a constant below changes: ``tree_qa.RULE_ID`` is this value
(every KG is re-evaluated) and a file whose header carries another id is evaluated at read
time like a file without a marker (:func:`built_with`).
"""
from __future__ import annotations

import numpy as np

RULE_ID = 'tq-1'
CELL_RULE_ID = 'rb1'    # apex → cell assignment rule (ne_rebin.py, docs/tree-cell-assignment.md): the res-12
                        # cell of the SERVED lon / lat.  Bump when that rule (or what moves a served
                        # position without changing the file digest / frame tag) changes.
H_SATURATED = 60.0      # >= this: quantisation ceiling, treated as non-tree
SPIKE_H = 45.0          # spike rules apply from here
SPIKE_RATIO = 1.6       # h / p90(chunk)
SPIKE_GAP = 15.0        # h - second tallest in chunk (m)


def _chunk(hs: np.ndarray) -> np.ndarray:
    ok = hs < H_SATURATED
    if hs.size > 1 and hs.max() >= SPIKE_H:
        srt = np.sort(hs)
        p90 = float(np.percentile(hs, 90)); sec = float(srt[-2])
        spike = (hs >= SPIKE_H) & ((hs / max(p90, 1.0) > SPIKE_RATIO) | (hs - sec > SPIKE_GAP))
        ok &= ~spike
    return ok


def keep_mask(h_m, chunk=None) -> np.ndarray:
    """Keep-mask over apex heights (m).  ``chunk`` None: *h_m* is one res-10 chunk; else an
    equally long array of chunk ids and the rule is evaluated per chunk."""
    hs = np.asarray(h_m, np.float64)
    if chunk is None or hs.size == 0:
        return _chunk(hs)
    g = np.asarray(chunk)
    order = np.argsort(g, kind='stable')
    gs = g[order]; hsrt = hs[order]
    cut = np.flatnonzero(np.concatenate([[True], gs[1:] != gs[:-1], [True]]))
    ok = hs < H_SATURATED
    for k in np.flatnonzero(np.maximum.reduceat(hsrt, cut[:-1]) >= SPIKE_H):
        idx = order[cut[k]:cut[k + 1]]
        ok[idx] = _chunk(hs[idx])
    return ok


def quant_h(h_m: np.ndarray) -> np.ndarray:
    """Apex height → u8 ×0.5 m (1..254) — the scale of tree_h / tree_h_max."""
    return np.clip(np.round(h_m * 2.0), 1, 254).astype(np.uint8)


def marker(rows: dict) -> dict:
    """The h3b header record (``net1.tree_rule``): *rows* = {r10 hex: [NET1 row index within that
    chunk, ...]} of the apices the rule hides.  The file itself keeps every apex."""
    return {'id': RULE_ID, 'mode': 'mark', 'hidden': int(sum(len(v) for v in rows.values())),
            'rows': {k: [int(i) for i in v] for k, v in sorted(rows.items())}}


def _marker(hdr: dict | None):
    m = ((hdr or {}).get('net1') or {}).get('tree_rule')
    return m if isinstance(m, dict) and m.get('id') == RULE_ID else None


def built_with(hdr: dict | None) -> bool:
    """True when the file's header carries the verdict of the CURRENT rule."""
    return _marker(hdr) is not None


_P: dict = {}           # NET1 digest → {r10 int: row idx}   (header dicts stay plain JSON)


def _parsed(m: dict, hdr: dict) -> dict:
    key = (hdr.get('net1') or {}).get('digest') or id(m)
    p = _P.get(key)
    if p is None:       # a marker without ``rows`` (the short-lived drop-at-build format of
        if len(_P) > 4096:      # fdff4ff) = nothing left to hide
            _P.clear()
        p = _P[key] = {int(k, 16): np.asarray(v, np.int64) for k, v in (m.get('rows') or {}).items()}
    return p


_NONE = np.zeros(0, np.int64)


def hidden_rows(hdr: dict | None, r10: int):
    """NET1 row indices of chunk *r10* the rule hides, read from the header → array (often
    empty); None = file without a current marker (caller evaluates :func:`keep_mask`)."""
    m = _marker(hdr)
    return None if m is None else _parsed(m, hdr).get(int(r10), _NONE)


def marked_chunks(hdr: dict | None):
    """r10 ids of the chunks with a hidden apex per the header; None = no current marker."""
    m = _marker(hdr)
    return None if m is None else set(_parsed(m, hdr))


def chunk_keep(hdr: dict | None, r10: int, h_m) -> np.ndarray:
    """Keep-mask of one stored chunk: the header's verdict when the file has one, else the rule."""
    hr = hidden_rows(hdr, r10)
    if hr is None:
        return keep_mask(h_m)
    ok = np.ones(np.asarray(h_m).size, bool)
    ok[hr[hr < ok.size]] = False
    return ok
