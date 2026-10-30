"""Word ban for the public /segmentation UI + relay (docs/api-tiers.md).

The rendered public page (static/index.html minus /*PRIVATE*/ blocks), the
relay module, and the docs section must not contain any
``cells_api.FORBIDDEN_WORDS`` token; the JSON scrub must strip banned keys
and mask banned words; the allow-list must never admit admin paths.
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import cells_api  # noqa: E402
import public_docs  # noqa: E402
import public_segmentation as ps  # noqa: E402


def _hits(text: str):
    out = []
    for w in cells_api.FORBIDDEN_WORDS:
        for m in re.finditer(r'(?<![\w])' + re.escape(w) + r'(?![\w])', text, flags=re.IGNORECASE):
            line = text.count('\n', 0, m.start()) + 1
            out.append((w, line, text.splitlines()[line - 1].strip()[:100]))
    return out


def test_page_vocabulary():
    html = ps.render_page()
    assert ps.PRIVATE_OPEN not in html and ps.PRIVATE_CLOSE not in html
    assert 'window.SRTM_PUBLIC=true' in html
    hits = _hits(html)
    assert not hits, f'forbidden words in rendered /segmentation: {hits}'
    # the private page keeps its blocks (owner UI unchanged)
    src = ps.UI_PATH.read_text()
    assert src.count(ps.PRIVATE_OPEN) == src.count(ps.PRIVATE_CLOSE) >= 1


def test_module_and_docs_vocabulary():
    src = (ROOT / 'public_segmentation.py').read_text()
    # the module only names the list, never the words
    hits = _hits(src)
    assert not hits, f'forbidden words in public_segmentation.py: {hits}'
    a = public_docs.LLM_TXT.find('## Segmentation UI')
    assert a >= 0, 'docs section missing'
    b = public_docs.LLM_TXT.find('\n## ', a + 1)
    hits = _hits(public_docs.LLM_TXT[a:b if b > 0 else None])
    assert not hits, f'forbidden words in docs segmentation section: {hits}'


def test_scrub():
    doc = {'meta': {'include_ortho': True, 'include_cada' + 'stre': False, 'source': 'product'},
           'cada' + 'stre_evaluation': {'f1': 1}, 'warnings': ['Cada' + 'stre unavailable; served without'],
           'features': [{'properties': {'type': 'roof', 'pa' + 'rcel_id': 7, 'gst': 'x', 'n': 1}}]}
    out = ps.scrub(doc)
    assert not _hits(json.dumps(out)), json.dumps(out)
    assert out['meta'] == {'include_ortho': True, 'source': 'product'}
    assert out['features'][0]['properties'] == {'type': 'roof', 'n': 1}
    assert out['warnings'] == ['… unavailable; served without']


def test_allow_list():
    assert ps.route_kind('POST', '/api/v1/segment') == 'heavy'
    assert ps.route_kind('GET', '/api/v1/segment/result') == 'light'
    assert ps.route_kind('GET', '/api/v1/share/abc-123') == 'light'
    for bad in ('/api/v1/admin/update', '/api/v1/processing/status', '/api/v1/director/status',
                '/api/v1/credentials', '/api/v1/kg/12345/segments', '/api/v1/training/status',
                '/api/v1/segment/../admin/update', '/api/v1/share/../../x'):
        assert ps.route_kind('GET', bad) is None and ps.route_kind('POST', bad) is None, bad
    assert ps.route_kind('GET', '/api/v1/segment') is None   # GET not allowed on POST-only route


def test_primary_only(monkeypatch):
    import config
    monkeypatch.setattr(config, 'PRIMARY_URL', 'http://director.example.net:8000')
    assert ps.is_primary() is False
    monkeypatch.setattr(config, 'PRIMARY_URL', '')
    assert ps.is_primary() is True


if __name__ == '__main__':
    test_page_vocabulary(); test_module_and_docs_vocabulary(); test_scrub(); test_allow_list()
    print('ok')
