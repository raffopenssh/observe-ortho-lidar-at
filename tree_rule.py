"""tree_rule — the static apex-height rule, ONE implementation for every caller.

An apex is dropped when it is saturated (>= ``H_SATURATED`` m: the quantisation ceiling, ~90 %
masts / turbines / cranes) or a spike: >= ``SPIKE_H`` m and either h / p90 > ``SPIKE_RATIO`` or
more than ``SPIKE_GAP`` m above the second tallest apex of its res-10 chunk (derivation: see
``ne_trees``).  Heights are the stored NET1 heights (0.25 m steps, as ``cells_api.decode_trees``).

Callers: ``ne_trees._decode_chunk_uncached`` (/trees/bbox, and through ``chunk_keep`` the
``tree_qa`` store) for files built without the rule; ``ne_h3.build_observed`` (the builder, on
peers) which drops those apices before NET1 is written and records ``RULE_ID`` in the h3b header
(``net1.tree_rule``).  Pure numpy — no primary-only imports.

``RULE_ID`` is THE id to bump when a constant below changes: ``tree_qa.RULE_ID`` is this value
(every KG is re-evaluated) and a file whose header carries another id is treated as built
without the current rule (:func:`built_with`).
"""
from __future__ import annotations

import numpy as np

RULE_ID = 'tq-1'
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


def marker(dropped: int, **extra) -> dict:
    """The h3b header record (``net1.tree_rule``) of a file whose NET1 rows already passed the rule."""
    return dict(id=RULE_ID, dropped=int(dropped), **extra)


def built_with(hdr: dict | None) -> bool:
    """True when the file's NET1 rows already passed the CURRENT rule (header marker)."""
    m = ((hdr or {}).get('net1') or {}).get('tree_rule')
    return isinstance(m, dict) and m.get('id') == RULE_ID
