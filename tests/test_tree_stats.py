"""tree_stats: the national count is ONE number, consistent with its own bands and with the rebin store."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import tree_stats


def test_bands():
    b = tree_stats._bands(['3-5', '5-10', '55+'], __import__('numpy').array([1, 2, 1]))
    assert [x['min_m'] for x in b] == [3.0, 5.0, 55.0] and b[-1]['max_m'] is None
    assert abs(sum(x['share'] for x in b) - 1) < 1e-6
    print('ok test_bands')


def test_live():
    n = tree_stats.national()
    if n is None:
        print('skip test_live (no national table)'); return
    assert n['apices'] == sum(b['apices'] for b in n['height_bands']) == n['at_least'][0]['apices']
    assert n['apices'] == sum(e['apices'] for e in n['by_elevation']), 'elevation bands must partition'
    assert [a['apices'] for a in n['at_least']] == sorted((a['apices'] for a in n['at_least']), reverse=True)
    if n.get('by_state'):
        s = sum(x['apices'] for x in n['by_state'])
        assert abs(s - n['apices']) / n['apices'] < 0.01, (s, n['apices'])   # two independent counts
    import public_api
    c = public_api.app.test_client()
    r = c.get('/api/v1/trees/stats')
    assert r.status_code == 200 and r.get_json()['apices'] == n['apices'] and r.headers.get('ETag')
    assert c.get('/api/v1/trees/stats', headers={'If-None-Match': r.headers['ETag']}).status_code == 304
    v = c.get('/api/v1/kg/92124').get_json()['vegetation']
    assert v['tree_segments'] == v['tree_count'] and (v['tree_apices'] is None or v['tree_apices'] > 0)
    print('ok test_live', n['apices'])


if __name__ == '__main__':
    test_bands(); test_live(); print('all ok')
