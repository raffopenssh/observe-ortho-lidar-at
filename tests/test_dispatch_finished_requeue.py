#!/usr/bin/env python3
"""Duplicate re-run guard (2026-11-23): a force-requeued code a peer has already
re-completed must not be dispatched again while the primary's manifest lags.

    python3 tests/test_dispatch_finished_requeue.py
"""
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import peer_director as pd  # noqa: E402


def iso(t, naive=False):
    d = datetime.fromtimestamp(t, timezone.utc)
    return d.replace(tzinfo=None).isoformat() if naive else d.isoformat()


def reset():
    pd._FINISHED_KG.clear()
    pd._RECENT_DISPATCH.clear()


def test_hold_and_release():
    now = time.time()
    tomb = {'47011-north_requeue': iso(now - 7200, naive=True), '47011-north_fresh': iso(now - 7200, naive=True),
            '67106-southwest-1_fresh': iso(now - 9000, naive=True),      # marker only (requeue stamp already dropped)
            '84005-northeast_requeue': iso(now - 600, naive=True)}
    reset()
    codes = ['47011-north', '67106-southwest-1', '84005-northeast', '12345']
    assert pd._finished_requeues(codes, tomb) == set()                   # nobody finished anything
    pd._note_finished_kg({'state': 'idle', 'last_kg_code': '47011-north', 'last_kg_uploaded_at': iso(now - 60)})
    pd._note_finished_kg({'state': 'running', 'last_kg_code': '67106-southwest-1', 'last_kg_uploaded_at': iso(now - 30)})
    pd._note_finished_kg({'last_kg_code': '84005-northeast', 'last_kg_uploaded_at': iso(now - 86400)})  # an OLD run, before the requeue
    pd._note_finished_kg({'last_kg_code': '12345', 'last_kg_uploaded_at': iso(now - 10)})               # not tombstoned: normal flow
    pd._note_finished_kg({'last_kg_code': None}); pd._note_finished_kg({'current_kg': 'repair-probe'})
    assert pd._finished_requeues(codes, tomb) == {'47011-north', '67106-southwest-1'}, pd._finished_requeues(codes, tomb)
    # an older report never lowers the stamp
    pd._note_finished_kg({'last_kg_code': '47011-north', 'last_kg_uploaded_at': iso(now - 90000)})
    assert '47011-north' in pd._finished_requeues(codes, tomb)
    # the operator requeues again later → newer tombstone → dispatchable
    tomb2 = dict(tomb, **{'47011-north_requeue': iso(now - 5, naive=True)})
    assert '47011-north' not in pd._finished_requeues(codes, tomb2)
    # manifest caught up, tombstones dropped → nothing held (the queue prune drains the code)
    assert pd._finished_requeues(codes, {}) == set()
    # safety valve: the upload record never arrived → hold lapses
    assert pd._finished_requeues(codes, tomb, now=now + pd._FINISHED_HOLD_S + 60) == set()


def test_claim_dispatch_filters(tmp):
    import json
    import pathlib
    now = time.time()
    old = pd.DATA_DIR
    pd.DATA_DIR = pathlib.Path(tmp)
    try:
        (pd.DATA_DIR / 'manifest_tombstones.json').write_text(json.dumps(
            {'47011-north_requeue': iso(now - 7200, naive=True), '73310-northeast_requeue': iso(now - 7200, naive=True)}))
        reset()
        assert pd._claim_dispatch(['47011-north', '73310-northeast', '30102'], 'http://a') == ['47011-north', '73310-northeast', '30102']
        reset()
        pd._note_finished_kg({'last_kg_code': '47011-north', 'last_kg_uploaded_at': iso(now - 20)})
        # neither another peer (at300) nor the finishing peer itself (at301 case) gets it again
        assert pd._claim_dispatch(['47011-north', '73310-northeast', '30102'], 'http://b') == ['73310-northeast', '30102']
        assert pd._claim_dispatch(['47011-north'], 'http://a') == []
        assert '47011-north' not in pd._RECENT_DISPATCH
    finally:
        pd.DATA_DIR = old
        reset()


if __name__ == '__main__':
    import tempfile
    test_hold_and_release()
    with tempfile.TemporaryDirectory() as t:
        test_claim_dispatch_filters(t)
    print('ALL OK')
