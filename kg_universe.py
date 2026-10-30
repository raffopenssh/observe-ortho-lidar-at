"""Canonical KG universe — single source of truth for "how many KGs exist".

The authority is the shipped ``data/austria_processor/kg_list.json``
(7850 Katastralgemeinden across 94 districts / 9 states, from the BEV
KG register).

History (2026-10-04): we carried ``8440`` for ~200 days.  That number was
an artefact of ``get_all_kgs()`` querying an upstream ``/search/kg?district=<name>``
whose ``district`` filter was ``LIKE '%name%'`` ("Wien" also matched
Wiener Neustadt, "Feldkirch" matched Feldkirchen), the merged district
"Waidhofen an der Thaya" appears twice in ``/search/district`` (codes
322 + 325), and ``limit=500`` without ``offset`` truncated that merged
district (526 KGs) by 26.  Net: 8440 rows = 7824 unique + 616 dupes,
and 26 KGs (24377–24400, 24501, 24502) were never scheduled.

Everything that needs a denominator imports :data:`KG_TOTAL` /
:func:`expected_total` from here instead of hard-coding a number.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

#: Authoritative count (BEV KG register), 2026-10-04.
KG_TOTAL = 7850

#: Fingerprint contract (shared with the NE server, ``/llm.txt §"KG universe"``):
#: ``sha256("\n".join(sorted(kg_code)))`` hex.  Both services expose it as
#: ``universe_hash`` + header ``X-KG-Universe-Hash``; equal hash ⇒ same set.
#: If either side changes the set, bump this AND tell the other side first.
UNIVERSE_HASH_RECIPE = 'sha256(join(sorted(kg_code), "\\n")) hex'
UNIVERSE_HASH_CONTRACT = '057334c3faa38fc0443df14a34444f2b273f42cf27065fb1845c1c49b1fe6f05'

#: Accept a freshly fetched list as "complete" only above this fraction.
MIN_COMPLETE_FRAC = 0.98

_HERE = Path(__file__).resolve().parent
KG_LIST_FILE = _HERE / 'data' / 'austria_processor' / 'kg_list.json'


def dedupe(kgs: list[dict]) -> list[dict]:
    """Unique by ``kg_code``, first occurrence wins, order preserved."""
    seen: set[str] = set()
    out: list[dict] = []
    for kg in kgs or []:
        code = str(kg.get('kg_code') or '')
        if code and code not in seen:
            seen.add(code)
            out.append(kg)
    return out


def load_codes(path: os.PathLike | str | None = None) -> set[str]:
    p = Path(path) if path else KG_LIST_FILE
    try:
        import json_cache
        st = p.stat()
        sig = (st.st_mtime_ns, st.st_size)
        hit = _codes_cache.get(str(p))
        if hit and hit[0] == sig:
            return hit[1]
        codes = {str(k['kg_code']) for k in (json_cache.load(p) or []) if k.get('kg_code')}
        _codes_cache[str(p)] = (sig, codes)
        return codes
    except Exception:
        return set()


_codes_cache: dict = {}


def expected_total(path: os.PathLike | str | None = None) -> int:
    """Unique count of the local ``kg_list.json`` if it looks complete,
    else :data:`KG_TOTAL`.  Never returns a duplicate-inflated number."""
    n = len(load_codes(path))
    return n if n >= KG_TOTAL * MIN_COMPLETE_FRAC else KG_TOTAL


def universe_hash(codes) -> str:
    """Fingerprint of a SET of KG codes per :data:`UNIVERSE_HASH_RECIPE`."""
    return hashlib.sha256('\n'.join(sorted({str(c) for c in codes})).encode()).hexdigest()
