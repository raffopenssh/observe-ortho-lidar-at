"""tree_taxa: the species dictionary is collision-free after normalisation,
ranked densely, uses only the 15 model classes, keeps every taxon of the first
(92-row) list, and the resolver answers the contract cases.  No DB, no corpus."""
import gzip
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import tree_taxa as tt  # noqa: E402

# taxon=class of dictionary rev 1 — stored reports carry these strings, they must stay resolvable
REV1 = """Picea abies=spruce;Picea=spruce;Picea pungens=spruce;Picea omorika=spruce;Abies alba=fir;Abies=fir;Abies nordmanniana=fir;Pinus sylvestris=pine;Pinus nigra=pine;Pinus cembra=pine;Pinus mugo=pine;Pinus strobus=pine;Pinus=pine;Larix decidua=larch;Larix=larch;Pseudotsuga menziesii=conifer;Taxus baccata=conifer;Thuja=conifer;Juniperus communis=conifer;Sequoiadendron giganteum=conifer;Metasequoia glyptostroboides=conifer;Ginkgo biloba=broadleaf;Fagus sylvatica=beech;Quercus robur=oak;Quercus petraea=oak;Quercus cerris=oak;Quercus pubescens=oak;Quercus rubra=oak;Quercus=oak;Acer pseudoplatanus=maple;Acer platanoides=maple;Acer campestre=maple;Acer=maple;Fraxinus excelsior=ash;Fraxinus ornus=ash;Betula pendula=birch;Betula pubescens=birch;Betula=birch;Alnus glutinosa=alder;Alnus incana=alder;Alnus alnobetula=alder;Alnus=alder;Populus nigra=poplar;Populus alba=poplar;Populus tremula=poplar;Populus x canadensis=poplar;Populus=poplar;Salix alba=willow;Salix caprea=willow;Salix babylonica=willow;Salix=willow;Malus domestica=fruit;Pyrus communis=fruit;Prunus avium=fruit;Prunus domestica=fruit;Prunus armeniaca=fruit;Prunus persica=fruit;Prunus padus=broadleaf;Prunus=fruit;Juglans regia=fruit;Cydonia oblonga=fruit;Morus=fruit;Castanea sativa=broadleaf;Aesculus hippocastanum=broadleaf;Tilia cordata=broadleaf;Tilia platyphyllos=broadleaf;Tilia=broadleaf;Carpinus betulus=broadleaf;Ostrya carpinifolia=broadleaf;Ulmus glabra=broadleaf;Ulmus minor=broadleaf;Ulmus laevis=broadleaf;Ulmus=broadleaf;Robinia pseudoacacia=broadleaf;Ailanthus altissima=broadleaf;Platanus x hispanica=broadleaf;Sorbus aucuparia=broadleaf;Sorbus aria=broadleaf;Sorbus torminalis=broadleaf;Sorbus domestica=broadleaf;Corylus colurna=broadleaf;Corylus avellana=broadleaf;Sambucus nigra=broadleaf;Crataegus=broadleaf;Liriodendron tulipifera=broadleaf;Catalpa bignonioides=broadleaf;Paulownia tomentosa=broadleaf;Gleditsia triacanthos=broadleaf;Celtis australis=broadleaf;Magnolia=broadleaf;Coniferae=conifer;Magnoliopsida=broadleaf"""

CASES = {  # input → taxon
    'Fi': 'Picea abies', 'PiAb': 'Picea abies', 'PIEAB': 'Picea abies', 'P. abies': 'Picea abies', 'p abies': 'Picea abies',
    'picea abies': 'Picea abies', 'Fichte': 'Picea abies', 'Rottanne': 'Picea abies', 'norway spruce': 'Picea abies',
    'PicAbi': 'Picea abies', 'PiceAbie': 'Picea abies', 'Pic abi': 'Picea abies', 'Pic. abi.': 'Picea abies',
    'Picea excelsa': 'Picea abies', 'Weisskiefer': 'Pinus sylvestris', 'Weißkiefer': 'Pinus sylvestris', 'Kiefer': 'Pinus sylvestris',
    'Föhre': 'Pinus sylvestris', 'Foehre': 'Pinus sylvestris', 'Fohre': 'Pinus sylvestris', 'Ki': 'Pinus sylvestris',
    'Kir': 'Prunus avium', 'Zwetschge': 'Prunus domestica', 'Zwetschke': 'Prunus domestica', 'Akazie': 'Robinia pseudoacacia',
    'FaSy': 'Fagus sylvatica', 'BAh': 'Acer pseudoplatanus', 'Lae': 'Larix decidua', 'Lä': 'Larix decidua', 'LÄRCHE': 'Larix decidua',
    'Berg-Ahorn': 'Acer pseudoplatanus', 'Kastanie': 'Aesculus hippocastanum', 'Maroni': 'Castanea sativa', 'EKa': 'Castanea sativa',
    'Quercus': 'Quercus', 'quercus sp.': 'Quercus', 'Eiche': 'Quercus', 'Ei': 'Quercus', '1QUEG': 'Quercus', 'SEi': 'Quercus robur',
    'Platanus × acerifolia': 'Platanus x hispanica', 'platanus x hispanica': 'Platanus x hispanica',
    'Platanus hispanica': 'Platanus x hispanica', 'Alnus viridis': 'Alnus alnobetula', 'Torminalis glaberrima': 'Sorbus torminalis',
    'Cormus domestica': 'Sorbus domestica', 'Arve': 'Pinus cembra', 'Legföhre': 'Pinus mugo', 'Vogelbeere': 'Sorbus aucuparia',
    'Espe': 'Populus tremula', 'Nussbaum': 'Juglans regia', 'Holler': 'Sambucus nigra', 'Marille': 'Prunus armeniaca',
    'Ginkgo': 'Ginkgo biloba', 'Thuje': 'Thuja', 'Dgl': 'Pseudotsuga menziesii', 'Nadelbaum': 'Coniferae', 'Laubholz': 'Magnoliopsida',
}


def main() -> int:
    T = tt.TABLE
    assert 250 <= len(T) <= 350, len(T)
    assert tt.digest() == tt.DIGEST, f'table changed: bump tree_taxa.REV (now {tt.REV}) and set DIGEST = {tt.digest()!r}'
    # shape, classes, rank
    assert [t[4] for t in T] == list(range(1, len(T) + 1)), 'rank must be dense 1..N in table order'
    assert len({t[0] for t in T}) == len(T)
    assert {t[3] for t in T} <= set(tt.CLASSES) and len(tt.CLASSES) == 15
    assert [t[0] for t in T[-2:]] == list(tt.GROUPS) and T[0][0] == 'Picea abies'
    for t in T:
        assert all(isinstance(x, str) and x for x in t[:4]) and isinstance(t[5], list) and isinstance(t[6], list), t
        assert len(t[5]) == len(set(t[5])) and len(t[6]) == len(set(t[6])), t
    # one genus row per genus, right after its most frequent species
    pos = {t[0]: i for i, t in enumerate(T)}
    first: dict = {}
    for t in T:
        if ' ' in t[0]:
            first.setdefault(t[0].split()[0], t[0])
    for g, sp in first.items():
        assert pos.get(g) == pos[sp] + 1, (g, sp)
    assert {t[0] for t in T if ' ' not in t[0]} == set(first) | set(tt.GROUPS)
    # collision-free after normalisation; every name of a row resolves to that row
    assert not tt.CLASHES, tt.CLASHES
    for t in T:
        for j, nm in enumerate(tt.names(t)):
            got = tt.resolve(nm)
            if j == 0 and tt.IS_GENUS[t[0]] and got[0] != t[0]:
                assert got[0].split()[0] == t[0], (nm, got)        # "Ginkgo" → Ginkgo biloba
                continue
            assert got == (t[0], True), (nm, got, t[0])
    # rev 1 taxa keep their name and class
    for kv in REV1.split(';'):
        tx, cl = kv.split('=')
        assert tt.CLASS.get(tx) == cl and tt.taxon_class(tx) == cl, kv
    # resolver
    for q, want in CASES.items():
        assert tt.resolve(q) == (want, True), (q, tt.resolve(q))
    assert tt.resolve('Zelkova abelicea') == ('Zelkova abelicea', False)          # unlisted binomial: kept as written
    assert tt.resolve('quercus × turneri') == ('Quercus x turneri', False)
    for q, want, amb in (('Fichtte', 'Picea abies', False), ('Picea abis', 'Picea abies', False), ('Kifer', 'Pinus sylvestris', False),
                         ('Rosskastanei', 'Aesculus hippocastanum', False), ('Acer p', 'Acer', False),
                         ('Pr se', 'Prunus serrulata', True), ('xyzzy', None, False)):
        try:
            tt.resolve(q); raise SystemExit(f'expected Unknown for {q!r}')
        except tt.Unknown as e:
            assert len(e.did_you_mean) <= 5 and e.ambiguous == amb, (q, e.did_you_mean, e.ambiguous)
            assert (want is None and not e.did_you_mean) or e.did_you_mean[0] == want, (q, e.did_you_mean)
            assert all(x in tt.RANK for x in e.did_you_mean)
    # n+n codes: unique ones resolve, shared ones are ambiguous unless listed as an explicit code
    shared = {k: v for k, v in tt._NN.items() if len(v) > 1 and k not in tt.INDEX}
    for k, v in list(shared.items())[:40]:
        try:
            tt.resolve(k); raise SystemExit(f'expected ambiguous {k}')
        except tt.Unknown as e:
            assert e.ambiguous and e.did_you_mean == sorted(v, key=tt.RANK.get)[:5], (k, e.did_you_mean)
    # EPPO codes only on the rows they were fetched for; size budget
    assert all(tx in tt.ROW for tx in tt._EPPO)
    gz = len(gzip.compress(json.dumps(tt.as_dict(), ensure_ascii=False, separators=(',', ':')).encode()))
    assert gz < 25000, gz
    print(f'ok ({len(T)} rows, rev {tt.REV}, {sum(len(t[5]) for t in T)} codes, {sum(len(t[6]) for t in T)} alt names, '
          f'{len(tt.INDEX)} keys, {len(shared)} ambiguous n+n codes, gzip {gz} B)')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
