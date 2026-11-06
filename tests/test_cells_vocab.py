"""Vocabulary rule for the public cells API (docs/ne-public-api.md §0.1).

``cells_api.py`` (minus its own FORBIDDEN_WORDS block) and the cells section
of ``public_docs.LLM_TXT`` must not contain any forbidden word as a whole
word, case-insensitively.  Also checks the live JSON surface of /cells/dict
and one cell record, when a product is on disk.
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import cells_api  # noqa: E402
import public_docs  # noqa: E402


def _strip_forbidden_block(src: str) -> str:
    a = src.index('# --- forbidden-words-begin')
    b = src.index('# --- forbidden-words-end')
    return src[:a] + src[b:]


def _hits(text: str):
    out = []
    for w in cells_api.FORBIDDEN_WORDS:
        for m in re.finditer(r'(?<![\w])' + re.escape(w) + r'(?![\w])', text, flags=re.IGNORECASE):
            line = text.count('\n', 0, m.start()) + 1
            out.append((w, line, text.splitlines()[line - 1].strip()[:100]))
    return out


def test_source_vocabulary():
    src = _strip_forbidden_block((ROOT / 'cells_api.py').read_text())
    hits = _hits(src)
    assert not hits, f'forbidden words in cells_api.py: {hits}'


def test_bins_source_vocabulary():
    src = (ROOT / 'cells_bins.py').read_text()
    hits = _hits(src)
    assert not hits, f'forbidden words in cells_bins.py: {hits}'


def test_stats_unit_vocabulary():
    hits = _hits((ROOT / 'stats_unit.py').read_text())
    assert not hits, f'forbidden words in stats_unit.py: {hits}'
    hits = _hits(public_docs.STATS_TXT)
    assert not hits, f'forbidden words in public_docs.STATS_TXT: {hits}'


def test_docs_vocabulary():
    hits = _hits(public_docs.CELLS_TXT)
    assert not hits, f'forbidden words in public_docs.CELLS_TXT: {hits}'
    a = public_docs.INDEX_HTML.find('<h2>Cells')
    b = public_docs.INDEX_HTML.find('<h2>', a + 1)
    hits = _hits(public_docs.INDEX_HTML[a:b])
    assert not hits, f'forbidden words in public_docs.INDEX_HTML cells section: {hits}'


def test_dict_and_record_vocabulary():
    from flask import Flask
    app = Flask(__name__)
    app.register_blueprint(cells_api.cells_bp)
    c = app.test_client()
    body = c.get('/api/v1/cells/dict').get_data(as_text=True)
    assert not _hits(body), 'forbidden words in /api/v1/cells/dict'
    files = sorted(p for p in cells_api.NE_DIR.glob('*.h3b') if not p.name.startswith('.'))
    if not files or not cells_api.INDEX_DB.exists():
        return
    kg = files[0].stem
    r = c.get(f'/api/v1/kg/{kg}/cells?format=json')
    assert r.status_code == 200, r.data
    assert not _hits(r.get_data(as_text=True)), 'forbidden words in /kg/<code>/cells json'
    hdr = cells_api.header(kg, with_chunks=True)
    first = hdr['chunks'][0]['first']
    r = c.get(f'/api/v1/cells/{first}')
    assert r.status_code == 200, r.data
    assert not _hits(r.get_data(as_text=True)), 'forbidden words in a cell record'
    # the public h3b container carries no first-slice bytes: record_bytes.nec1 == 0
    r = c.get(f'/api/v1/cells/chunk/{hdr["chunks"][0]["r10"]}?format=h3b')
    assert r.status_code == 200
    import json, struct
    blob = r.data
    hl = struct.unpack_from('<I', blob, 5)[0]
    h = json.loads(blob[9:9 + hl])
    assert h['record_bytes']['nec1'] == 0
    assert not _hits(json.dumps(h)), 'forbidden words in public h3b header'


if __name__ == '__main__':
    test_source_vocabulary(); test_docs_vocabulary(); test_dict_and_record_vocabulary()
    print('ok')
