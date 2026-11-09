"""ne_h3.declared_view — stored copy + ETag-conditional GET (the NE server bandwidth, 2026-11-09)."""
import json
import numpy as np
import pytest

import ne_h3


def _body(n=3):
    rows = b"".join(ne_h3.LU_REC.pack(int(1000 + i), 0, 0, 0, 0, bytes(9), 0, 0, 0, 0, 0, 0, 0) for i in range(n))
    return ne_h3.pack_section(ne_h3.MAGIC_LU, {"source": "chunks@abc", "n": n}, rows)


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setattr(ne_h3, "DECLARED_DIR", tmp_path / "declared")
    for k in ne_h3.DECLARED_NET:
        ne_h3.DECLARED_NET[k] = 0
    calls = []

    def fake(kg, timeout, etag):
        calls.append(etag)
        return fake.answer(etag)
    monkeypatch.setattr(ne_h3, "_declared_fetch", fake)
    fake.calls = calls
    return fake


def test_first_fetch_stores_then_expect_etag_serves_without_get(store):
    body = _body()
    store.answer = lambda etag: (200, body, '"e1"')
    v = ne_h3.declared_view("00001")
    assert v.header["_from"] == "200" and v.header["_etag"] == '"e1"'
    assert ne_h3.declared_stored_etag("00001") == '"e1"'
    assert store.calls == [None]
    v2 = ne_h3.declared_view("00001", expect_etag='"e1"')
    assert v2.header["_from"] == "stored" and store.calls == [None]       # no request
    assert ne_h3.DECLARED_NET["stored"] == 1 and ne_h3.DECLARED_NET["gets"] == 0


def test_conditional_get_304_serves_stored_200_replaces(store):
    store.answer = lambda etag: (200, _body(2), '"e1"')
    ne_h3.declared_view("00002")
    store.answer = lambda etag: (304, b"", etag) if etag == '"e1"' else (200, _body(2), '"e1"')
    v = ne_h3.declared_view("00002", expect_etag='"e2"')        # row says it moved → conditional GET
    assert store.calls[-1] == '"e1"' and v.header["_from"] == "304"
    store.answer = lambda etag: (200, _body(4), '"e3"')
    v = ne_h3.declared_view("00002", expect_etag='"e3"')
    assert v.header["_from"] == "200" and ne_h3.declared_stored_etag("00002") == '"e3"'
    assert json.loads((ne_h3.DECLARED_DIR / "00002.json").read_text())["etag"] == '"e3"'
    assert v.header["n"] == 4


def test_404_drops_copy_and_refresh_skips_store(store):
    store.answer = lambda etag: (200, _body(1), '"e1"')
    ne_h3.declared_view("00003")
    ne_h3.declared_view("00003", expect_etag='"e1"', refresh=True)      # 409 retry: must ask the server
    assert store.calls == [None, '"e1"']
    store.answer = lambda etag: (404, b"", None)
    assert ne_h3.declared_view("00003") is None
    assert ne_h3.declared_stored_etag("00003") is None


def test_net_delta():
    before = ne_h3.declared_net_snapshot()
    ne_h3.DECLARED_NET["gets"] += 4; ne_h3.DECLARED_NET["n304"] += 3; ne_h3.DECLARED_NET["bytes"] += 200_000
    d = ne_h3.declared_net_delta(before)
    assert d["gets"] == 4 and d["ratio_304"] == 0.75 and d["mb"] == 0.2
