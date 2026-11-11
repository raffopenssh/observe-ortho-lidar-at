"""tree_taxa — the tree species dictionary of ``/api/v1/trees/taxa/dict`` and
the resolver behind the ``species`` field of a tree report (tree_user.py).

One row per taxon: ``taxon|de|en|class|codes|alt`` (codes ","-separated, alt
";"-separated).  Species rows are written **in rank order** (1 = most frequent
tree in Austria); genus rows live in ``_G`` and are ranked right after the
most frequent species of the genus; the two group rows come last.

Rank sources (docs/tree-user.md → Dictionary): forest species follow the
published ÖWI 2016/21 shares (Fichte, Buche, Lärche, Weißkiefer, Tanne, Eiche,
then the hardwood / softwood groups); street and park trees are interleaved by
their counts in the Vienna Baumkataster (233 k trees, data.wien.gv.at, tallied
2026-11); orchard trees by the usual Streuobst order (Apfel, Birne, Zwetschke,
Kirsche, Walnuss).  It is an ordering for a suggestion list, not a census.

Codes: Austrian / German forestry abbreviations first, then the EPPO code
(``_EPPO``, taken from Wikidata P3031 and checked against the taxon name —
regenerate with ``tools/taxa_eppo.py``).  No numeric ÖWI codes: we have no
citable table for them.

Rules the table obeys (``python3 tests/test_tree_taxa.py``):
  * after normalisation no name, code or synonym belongs to two rows
    (the one exception is built in: a bare genus word that is also listed as a
    common name of a species of that genus — "Ginkgo", "Catalpa" — means the
    species); a genus row's de is shown as "<Name> (Gattung)", and the plain
    name is added to its alt when no other row claims it;
  * ``class`` is one of the 15 model classes; rank is dense 1..N;
  * ``REV`` must be bumped whenever the table changes (the test pins a digest).

Decided clashes: Ki / Kie = Kiefer (Pinus sylvestris), Kir = Kirsche; GEr =
Grauerle, GrEr = Grünerle; SPa = Schwarzpappel, WPa = Silberpappel; SWei is
left out (Sal- vs Silberweide → SaWei / SiWei); SAh = Spitzahorn; bare
Eiche / Ahorn / Birke / Erle / Pappel / Weide / Linde / Ulme / Ei / Ah / Bi / Er
/ Pa / Wei / Li / Ul mean the genus, while Fichte / Tanne / Buche / Lärche /
Esche / Kiefer mean the one species foresters mean by them; Kastanie =
Rosskastanie (Edelkastanie / Maroni is the other); Akazie = Robinie.
"""
from __future__ import annotations

import hashlib
import re
import unicodedata

REV = 2                       # 1 = the 92-row list (no rev field); bump on ANY change of _S / _G / _EPPO
DIGEST = 'f8f83d60b8af4757'                   # pinned by tests/test_tree_taxa.py (sha256[:16] of the built table)
FIELDS = ('taxon', 'de', 'en', 'class', 'rank', 'codes', 'alt')
CLASSES = ('alder', 'ash', 'beech', 'birch', 'broadleaf', 'conifer', 'fir', 'fruit', 'larch', 'maple', 'oak',
           'pine', 'poplar', 'spruce', 'willow')
GROUPS = ('Coniferae', 'Magnoliopsida')

# species, most frequent first: taxon|de|en|class|codes|alt
_S = """Picea abies|Fichte|Norway spruce|spruce|Fi|Rottanne;Gemeine Fichte;Gewöhnliche Fichte;Rotfichte;European spruce;Picea excelsa
Fagus sylvatica|Rotbuche|beech|beech|Bu,RBu|Buche;Blutbuche;European beech;common beech
Larix decidua|Lärche|European larch|larch|Lä,ELä|Europäische Lärche;Lärchenbaum;Larix europaea
Pinus sylvestris|Weißkiefer|Scots pine|pine|WKi,Kie,Ki,Fö|Rotföhre;Föhre;Kiefer;Waldkiefer;Rotkiefer;Weißföhre;Gemeine Kiefer;Forche;Scotch pine
Abies alba|Weißtanne|silver fir|fir|Ta,WTa|Tanne;Edeltanne;Silbertanne;European silver fir;Abies pectinata
Quercus robur|Stieleiche|pedunculate oak|oak|SEi,StEi|Sommereiche;Deutsche Eiche;English oak;common oak;Quercus pedunculata
Quercus petraea|Traubeneiche|sessile oak|oak|TEi,TrEi|Wintereiche;Steineiche;durmast oak;Quercus sessiliflora;Quercus sessilis
Fraxinus excelsior|Esche|ash|ash|Es|Gemeine Esche;Gewöhnliche Esche;European ash;common ash
Acer pseudoplatanus|Bergahorn|sycamore maple|maple|BAh|sycamore;Weißahorn
Carpinus betulus|Hainbuche|hornbeam|broadleaf|HBu,Hbu,HaBu|Weißbuche;Hagebuche;European hornbeam
Betula pendula|Hängebirke|silver birch|birch|SaBi|Sandbirke;Weißbirke;Warzenbirke;Betula verrucosa
Alnus incana|Grauerle|grey alder|alder|GEr,WEr|Weißerle;gray alder
Alnus glutinosa|Schwarzerle|black alder|alder|SEr,RErl|Roterle;common alder
Pinus nigra|Schwarzkiefer|black pine|pine|SKi,SFö|Schwarzföhre;Parapluiebaum;Austrian pine
Pinus cembra|Zirbe|Swiss stone pine|pine|Zi,Zir|Zirbelkiefer;Zirm;Arve;Arolla pine
Pinus mugo|Latsche|mountain pine|pine|Lat|Legföhre;Latschenkiefer;Bergkiefer;Krummholzkiefer;dwarf mountain pine;Pinus montana;Pinus mugho
Populus tremula|Zitterpappel|aspen|poplar|As,Asp,ZPa|Aspe;Espe;European aspen
Salix caprea|Salweide|goat willow|willow|SaWei|Palmweide;Palmkätzchen;pussy willow
Sorbus aucuparia|Eberesche|rowan|broadleaf|Vb,VoBe,Eb|Vogelbeere;Vogelbeerbaum;mountain ash
Malus domestica|Apfel|apple|fruit|Apf|Apfelbaum;Kulturapfel;apple tree;Malus pumila
Acer platanoides|Spitzahorn|Norway maple|maple|SAh,SpAh|
Tilia cordata|Winterlinde|small-leaved lime|broadleaf|WLi|Steinlinde;littleleaf linden;small-leaved linden;Tilia parvifolia
Prunus avium|Vogelkirsche|cherry|fruit|Kir,VKir|Kirsche;Kirschbaum;Süßkirsche;Wildkirsche;wild cherry;sweet cherry;gean;Cerasus avium
Robinia pseudoacacia|Robinie|black locust|broadleaf|Rob|Akazie;Scheinakazie;Falsche Akazie;false acacia;Robinia pseudacacia
Pyrus communis|Birne|pear|fruit||Birnbaum;Kulturbirne;Mostbirne;pear tree
Prunus domestica|Zwetschke|plum|fruit||Zwetschge;Zwetschkenbaum;Pflaume;Kriecherl;Ringlotte;Mirabelle;plum tree;damson
Juglans regia|Walnuss|walnut|fruit|Nu,WNu|Nussbaum;Walnussbaum;Echte Walnuss;Nuss;English walnut;Persian walnut
Acer campestre|Feldahorn|field maple|maple|FAh|Maßholder;hedge maple
Alnus alnobetula|Grünerle|green alder|alder|GrEr|Alpenerle;Laublatsche;Alnus viridis
Corylus avellana|Hasel|hazel|broadleaf|Has|Haselnuss;Haselstrauch;Haselnussstrauch;common hazel
Tilia platyphyllos|Sommerlinde|large-leaved lime|broadleaf|SLi|large-leaved linden;bigleaf linden;Tilia grandifolia
Ulmus glabra|Bergulme|wych elm|broadleaf|BUl|Bergrüster;Scots elm;Ulmus montana;Ulmus scabra
Quercus cerris|Zerreiche|Turkey oak|oak|ZEi|
Pseudotsuga menziesii|Douglasie|Douglas fir|conifer|Dgl,Dg,Dou|Douglastanne;Küstendouglasie;Douglasfichte;Oregon pine;Pseudotsuga taxifolia;Pseudotsuga douglasii
Salix alba|Silberweide|white willow|willow|SiWei|Weißweide
Populus nigra|Schwarzpappel|black poplar|poplar|SPa|Pyramidenpappel;Säulenpappel;Lombardy poplar
Populus x canadensis|Hybridpappel|hybrid poplar|poplar|HPa|Kanadapappel;Kanadische Pappel;Kanadische Holzpappel;Canadian poplar;Populus x euramericana
Aesculus hippocastanum|Rosskastanie|horse chestnut|broadleaf|RKa|Kastanie;Kastanienbaum;Gemeine Rosskastanie;Weiße Rosskastanie;conker tree
Sambucus nigra|Holunder|elder|broadleaf||Holler;Hollerbusch;Schwarzer Holunder;elderberry
Castanea sativa|Edelkastanie|sweet chestnut|broadleaf|EKa|Maroni;Marone;Esskastanie;Käste;Kestenbaum;Spanish chestnut
Quercus rubra|Roteiche|red oak|oak|REi|Amerikanische Roteiche;northern red oak;Quercus borealis
Populus alba|Silberpappel|white poplar|poplar|WPa|Weißpappel
Quercus pubescens|Flaumeiche|downy oak|oak|FlEi|pubescent oak;Quercus lanuginosa
Ostrya carpinifolia|Hopfenbuche|hop hornbeam|broadleaf|HoBu|Europäische Hopfenbuche
Fraxinus ornus|Blumenesche|manna ash|ash|BlEs|Mannaesche;flowering ash
Prunus padus|Traubenkirsche|bird cherry|broadleaf|TKir|Gewöhnliche Traubenkirsche;Ahlkirsche;Elexen;Padus avium
Ulmus minor|Feldulme|field elm|broadleaf|FUl|Feldrüster;smooth-leaved elm;Ulmus campestris;Ulmus carpinifolia
Sorbus aria|Mehlbeere|whitebeam|broadleaf|Mehlb,MBe|Echte Mehlbeere;Mehlbeerbaum;Aria edulis
Taxus baccata|Eibe|yew|conifer|Eib|Europäische Eibe;Heimische Eibe;common yew
Betula pubescens|Moorbirke|downy birch|birch|MoBi|Haarbirke
Thuja occidentalis|Abendländischer Lebensbaum|northern white cedar|conifer||Thujenhecke;Abendländische Thuje;eastern arborvitae
Prunus armeniaca|Marille|apricot|fruit||Aprikose;Marillenbaum;Armeniaca vulgaris
Prunus cerasus|Weichsel|sour cherry|fruit||Sauerkirsche;Weichselbaum;Weichselkirsche;tart cherry
Salix fragilis|Bruchweide|crack willow|willow|BrWei|Knackweide;Fahlweide;Salix x rubens;Salix euxina
Celtis australis|Zürgelbaum|hackberry|broadleaf||Südlicher Zürgelbaum;European nettle tree
Platanus x hispanica|Platane|plane|broadleaf|Pla|Ahornblättrige Platane;London plane;Platanus x acerifolia;Platanus hybrida
Sorbus torminalis|Elsbeere|wild service tree|broadleaf|Elsb,Els|Adlitzbeere;Odlatzbia;Torminalis glaberrima
Ulmus laevis|Flatterulme|white elm|broadleaf|FlUl|Flatterrüster;European white elm;Ulmus effusa
Ailanthus altissima|Götterbaum|tree of heaven|broadleaf||Ailanthus glandulosa
Gleditsia triacanthos|Gleditschie|honey locust|broadleaf||Lederhülsenbaum;Christusdorn;Gleditschia triacanthos
Pyrus calleryana|Zierbirne|Callery pear|broadleaf||Chinesische Wildbirne;Stadtbirne
Styphnolobium japonicum|Schnurbaum|pagoda tree|broadleaf||Japanischer Schnurbaum;Sophora;Sophora japonica
Tilia tomentosa|Silberlinde|silver lime|broadleaf|SiLi|silver linden
Prunus persica|Pfirsich|peach|fruit||Pfirsichbaum;Nektarine;Weingartenpfirsich;Persica vulgaris
Corylus colurna|Baumhasel|Turkish hazel|broadleaf||Türkische Hasel
Prunus cerasifera|Kirschpflaume|cherry plum|fruit||Myrobalane;Blutpflaume
Pyrus pyraster|Wildbirne|wild pear|fruit||Holzbirne
Malus sylvestris|Holzapfel|crab apple|fruit||Wildapfel;European crab apple
Koelreuteria paniculata|Blasenbaum|golden rain tree|broadleaf||Blasenesche
Prunus serrulata|Japanische Blütenkirsche|Japanese cherry|broadleaf||Zierkirsche;Japanische Zierkirsche;Nelkenkirsche
Aesculus x carnea|Rote Rosskastanie|red horse chestnut|broadleaf||Rotblühende Rosskastanie;Fleischrote Rosskastanie;Rotblühende Kastanie
Fraxinus pennsylvanica|Rotesche|green ash|ash||Pennsylvanische Esche;Grünesche
Ginkgo biloba|Ginkgo|maidenhair tree|broadleaf||Fächerblattbaum;Ginko
Acer negundo|Eschenahorn|box elder|maple||Eschenblättriger Ahorn
Juglans nigra|Schwarznuss|black walnut|fruit|SNu|Schwarznussbaum
Fraxinus angustifolia|Schmalblättrige Esche|narrow-leaved ash|ash||Quirlesche;Fraxinus oxycarpa
Populus x canescens|Graupappel|grey poplar|poplar|GPa|gray poplar
Sorbus domestica|Speierling|service tree|broadleaf|Spei|Sperberbaum;Spierling;Cormus domestica
Cydonia oblonga|Quitte|quince|fruit||Quittenbaum
Celtis occidentalis|Amerikanischer Zürgelbaum|common hackberry|broadleaf||Westlicher Zürgelbaum
Catalpa bignonioides|Trompetenbaum|southern catalpa|broadleaf||Catalpa;Indian bean tree
Picea pungens|Stechfichte|blue spruce|spruce||Blaufichte;Blautanne;Colorado spruce
Thuja plicata|Riesenlebensbaum|western red cedar|conifer||Riesenthuje
Larix kaempferi|Japanische Lärche|Japanese larch|larch|JLä|Japanlärche;Larix leptolepis
Abies grandis|Küstentanne|grand fir|fir|KTa|Riesentanne;Große Küstentanne
Pinus strobus|Weymouthskiefer|white pine|pine|Stro|Strobe;Weymouthsföhre;Seidenkiefer;eastern white pine
Pinus uncinata|Spirke|mountain pine (upright)|pine|Spi|Hakenkiefer;Moorspirke;Aufrechte Bergkiefer;Bergspirke;Pinus mugo subsp. uncinata;Pinus rotundata
Platanus orientalis|Morgenländische Platane|oriental plane|broadleaf||Orientalische Platane
Tilia x europaea|Holländische Linde|common lime|broadleaf||Kaiserlinde;common linden;Tilia x vulgaris;Tilia x intermedia
Paulownia tomentosa|Blauglockenbaum|princess tree|broadleaf||Paulownia;Kaiserbaum;Kiribaum;foxglove tree;empress tree
Morus alba|Weiße Maulbeere|white mulberry|fruit||Weißer Maulbeerbaum
Salix babylonica|Trauerweide|weeping willow|willow||Korkenzieherweide;Hängeweide;Salix x sepulcralis;Salix matsudana
Salix eleagnos|Lavendelweide|olive willow|willow||Grauweide;Salix elaeagnos;Salix incana
Salix daphnoides|Reifweide|violet willow|willow||
Salix appendiculata|Großblattweide|large-leaved willow|willow||Schluchtweide
Salix purpurea|Purpurweide|purple willow|willow||
Salix viminalis|Korbweide|osier|willow||Hanfweide;common osier
Salix pentandra|Lorbeerweide|bay willow|willow||
Prunus mahaleb|Steinweichsel|mahaleb cherry|broadleaf||Felsenkirsche;Weichselkirsche (Stein)
Prunus serotina|Spätblühende Traubenkirsche|black cherry|broadleaf||Späte Traubenkirsche
Tilia x euchlora|Krimlinde|Crimean lime|broadleaf||Crimean linden
Tilia americana|Amerikanische Linde|basswood|broadleaf||American linden
Ulmus pumila|Sibirische Ulme|Siberian elm|broadleaf||
Ulmus x hollandica|Holländische Ulme|Dutch elm|broadleaf||Bastardulme
Zelkova serrata|Japanische Zelkove|Japanese zelkova|broadleaf||Zelkove;keaki
Liriodendron tulipifera|Tulpenbaum|tulip tree|broadleaf||tulip poplar
Rhus typhina|Essigbaum|staghorn sumac|broadleaf||Hirschkolbensumach
Cornus mas|Dirndl|cornelian cherry|fruit||Dirndlstrauch;Kornelkirsche;Gelber Hartriegel
Populus simonii|Simons Pappel|Simon poplar|poplar||Birkenpappel
Populus balsamifera|Balsampappel|balsam poplar|poplar|BPa|
Gymnocladus dioicus|Geweihbaum|Kentucky coffeetree|broadleaf||
Acer x freemanii|Freemans Ahorn|Freeman maple|maple||
Fraxinus americana|Weißesche|white ash|ash||
Cercis siliquastrum|Judasbaum|Judas tree|broadleaf||
Acer saccharinum|Silberahorn|silver maple|maple||Acer dasycarpum
Crataegus monogyna|Eingriffeliger Weißdorn|common hawthorn|broadleaf||Hagedorn
Crataegus laevigata|Zweigriffeliger Weißdorn|midland hawthorn|broadleaf||Rotdorn
Chamaecyparis lawsoniana|Lawsons Scheinzypresse|Lawson cypress|conifer||Scheinzypresse;Port Orford cedar
Acer monspessulanum|Französischer Ahorn|Montpellier maple|maple||Burgenahorn;Felsenahorn
Elaeagnus angustifolia|Ölweide|Russian olive|broadleaf||Schmalblättrige Ölweide
Acer tataricum|Tatarischer Ahorn|Tatar maple|maple||Feuerahorn;Steppenahorn;Amur maple;Acer ginnala
Picea omorika|Serbische Fichte|Serbian spruce|spruce||Omorikafichte
Aesculus flava|Gelbe Rosskastanie|yellow buckeye|broadleaf||Gelbe Kastanie
Platycladus orientalis|Morgenländischer Lebensbaum|oriental arborvitae|conifer||Thuja orientalis
Pterocarya fraxinifolia|Flügelnuss|Caucasian wingnut|broadleaf||Kaukasische Flügelnuss
Morus nigra|Schwarze Maulbeere|black mulberry|fruit||Schwarzer Maulbeerbaum
Sorbus intermedia|Schwedische Mehlbeere|Swedish whitebeam|broadleaf||Scandosorbus intermedia
Cercis canadensis|Kanadischer Judasbaum|eastern redbud|broadleaf||
Parrotia persica|Eisenholzbaum|Persian ironwood|broadleaf||Parrotie
Liquidambar styraciflua|Amberbaum|sweetgum|broadleaf||Amerikanischer Amberbaum
Prunus subhirtella|Higan-Kirsche|Higan cherry|broadleaf||Schneekirsche;Winterkirsche
Cupressus x leylandii|Leyland-Zypresse|Leyland cypress|conifer||Bastardzypresse;Cupressocyparis leylandii;Cuprocyparis leylandii
Amelanchier lamarckii|Kupfer-Felsenbirne|juneberry|broadleaf||snowy mespilus
Acer truncatum|Chinesischer Spitzahorn|Shantung maple|maple||
Metasequoia glyptostroboides|Urweltmammutbaum|dawn redwood|conifer||Chinesisches Rotholz
Quercus frainetto|Ungarische Eiche|Hungarian oak|oak||
Quercus palustris|Sumpfeiche|pin oak|oak||
Cedrus atlantica|Atlaszeder|Atlas cedar|conifer||Blaue Atlaszeder
Cedrus deodara|Himalayazeder|deodar cedar|conifer||
Cedrus libani|Libanonzeder|cedar of Lebanon|conifer||
Magnolia x soulangeana|Tulpenmagnolie|saucer magnolia|broadleaf||Magnolia x soulangiana
Magnolia kobus|Kobushi-Magnolie|kobus magnolia|broadleaf||Baummagnolie
Acer opalus|Schneeballahorn|Italian maple|maple||Italienischer Ahorn
Pinus heldreichii|Schlangenhautkiefer|Bosnian pine|pine||Panzerkiefer;Pinus leucodermis
Cercidiphyllum japonicum|Katsurabaum|katsura|broadleaf||Kuchenbaum;Lebkuchenbaum
Crataegus x lavalleei|Lederblättriger Weißdorn|Lavallée hawthorn|broadleaf||Apfeldorn;Hahnendorn;Crataegus x lavallei
Juniperus communis|Wacholder|juniper|conifer||Gemeiner Wacholder;Kranewitt;Kranawitt;common juniper
Juniperus virginiana|Virginischer Wacholder|eastern red cedar|conifer||Bleistiftzeder
Ilex aquifolium|Stechpalme|holly|broadleaf||Schradl;Hülse
Prunus dulcis|Mandel|almond|fruit||Mandelbaum;Amygdalus communis
Ficus carica|Feige|fig|fruit||Feigenbaum
Mespilus germanica|Mispel|medlar|fruit||Asperl;Echte Mispel
Acer rubrum|Rotahorn|red maple|maple||
Amelanchier ovalis|Gewöhnliche Felsenbirne|snowy mespilus (European)|broadleaf||Edelweißstrauch
Acer griseum|Zimtahorn|paperbark maple|maple||
Acer palmatum|Fächerahorn|Japanese maple|maple||
Broussonetia papyrifera|Papiermaulbeere|paper mulberry|broadleaf||
Magnolia grandiflora|Immergrüne Magnolie|southern magnolia|broadleaf||
Pinus wallichiana|Tränenkiefer|Himalayan pine|pine||Bhutan pine
Sequoiadendron giganteum|Mammutbaum|giant sequoia|conifer||Riesenmammutbaum;Bergmammutbaum;Wellingtonie;giant redwood;Wellingtonia gigantea
Abies nordmanniana|Nordmanntanne|Nordmann fir|fir||Nordmannstanne;Christbaum;Caucasian fir
Abies concolor|Koloradotanne|white fir|fir||Coloradotanne;Grautanne
Abies koreana|Koreatanne|Korean fir|fir||
Abies procera|Edle Tanne|noble fir|fir||Pazifische Edeltanne;Abies nobilis
Abies pinsapo|Spanische Tanne|Spanish fir|fir||
Abies cephalonica|Griechische Tanne|Greek fir|fir||
Picea orientalis|Kaukasusfichte|oriental spruce|spruce||Orientalische Fichte
Picea sitchensis|Sitkafichte|Sitka spruce|spruce||
Picea glauca|Zuckerhutfichte|white spruce|spruce||Weißfichte;Schimmelfichte
Larix x marschlinsii|Hybridlärche|hybrid larch|larch||Dunkeld larch;Larix x eurolepis
Albizia julibrissin|Seidenbaum|silk tree|broadleaf||Seidenakazie;Schlafbaum
Betula utilis|Himalayabirke|Himalayan birch|birch||
Betula papyrifera|Papierbirke|paper birch|birch||
Quercus coccinea|Scharlacheiche|scarlet oak|oak||
Laburnum anagyroides|Goldregen|laburnum|broadleaf||Gemeiner Goldregen;golden chain
Prunus x yedoensis|Tokyo-Kirsche|Yoshino cherry|broadleaf||Yoshino-Kirsche
Prunus sargentii|Scharlachkirsche|Sargent cherry|broadleaf||
Cladrastis kentukea|Gelbholz|yellowwood|broadleaf||Cladrastis lutea
Tetradium daniellii|Bienenbaum|bee-bee tree|broadleaf||Stinkesche;Euodia hupehensis;Evodia hupehensis
Maclura pomifera|Osagedorn|Osage orange|broadleaf||Milchorange
Taxodium distichum|Sumpfzypresse|bald cypress|conifer||swamp cypress
Pyrus salicifolia|Weidenblättrige Birne|willow-leaved pear|broadleaf||
Nyssa sylvatica|Tupelobaum|black tupelo|broadleaf||
Acer buergerianum|Dreispitzahorn|trident maple|maple||
Davidia involucrata|Taschentuchbaum|handkerchief tree|broadleaf||Taubenbaum;dove tree
Alnus cordata|Herzblättrige Erle|Italian alder|alder||Italienische Erle
Alnus x spaethii|Purpurerle|Spaeth alder|alder||
Cupressus nootkatensis|Nootka-Scheinzypresse|Nootka cypress|conifer||Chamaecyparis nootkatensis;Xanthocyparis nootkatensis;Callitropsis nootkatensis
Cupressus sempervirens|Mittelmeerzypresse|Italian cypress|conifer||Säulenzypresse;Zypresse
Acer cappadocicum|Kolchischer Ahorn|Cappadocian maple|maple||
Tsuga canadensis|Hemlocktanne|eastern hemlock|conifer||Kanadische Hemlocktanne;Schierlingstanne
Acer saccharum|Zuckerahorn|sugar maple|maple||
Calocedrus decurrens|Weihrauchzeder|incense cedar|conifer||Flusszeder
Juniperus chinensis|Chinesischer Wacholder|Chinese juniper|conifer||
Pinus ponderosa|Gelbkiefer|ponderosa pine|pine||
Cryptomeria japonica|Sicheltanne|Japanese cedar|conifer||sugi
Araucaria araucana|Andentanne|monkey puzzle|conifer||Araukarie;Chilenische Araukarie;Schmucktanne
Diospyros kaki|Kaki|persimmon|fruit||Kakibaum"""

# genus rows: taxon|de (shown with " (Gattung)")|en|class|codes|alt
_G = """Picea|Fichte|spruce|spruce||Fichten
Fagus|Buche|beech (genus)|beech||Buchen
Larix|Lärche|larch|larch||Lärchen
Pinus|Kiefer|pine|pine||Kiefern;Föhren
Abies|Tanne|fir|fir||Tannen
Quercus|Eiche|oak|oak|Ei|Eiche;Eichen
Fraxinus|Esche|ash (genus)|ash||Eschen
Acer|Ahorn|maple|maple|Ah|Ahorn;Ahorne
Carpinus|Hainbuche|hornbeam (genus)|broadleaf||
Betula|Birke|birch|birch|Bi|Birke;Birken
Alnus|Erle|alder|alder|Er,Erl|Erle;Erlen
Populus|Pappel|poplar|poplar|Pa|Pappel;Pappeln
Salix|Weide|willow|willow|Wei|Weide;Weiden;Weidenbaum
Sorbus|Mehlbeeren|rowans and whitebeams|broadleaf||
Malus|Apfel|apple (genus)|fruit||Zierapfel
Tilia|Linde|lime|broadleaf|Li|Linde;Linden;Lindenbaum;linden
Prunus|Steinobst|stone fruit|fruit||Obstbaum;fruit;fruit tree
Robinia|Robinie|locust|broadleaf||
Pyrus|Birne|pear (genus)|fruit||
Juglans|Walnuss|walnut (genus)|fruit||
Corylus|Hasel|hazel (genus)|broadleaf||
Ulmus|Ulme|elm|broadleaf|Ul|Ulme;Ulmen;Rüster
Pseudotsuga|Douglasie|Douglas fir (genus)|conifer||
Aesculus|Rosskastanie|buckeye|broadleaf||
Sambucus|Holunder|elder (genus)|broadleaf||
Castanea|Kastanie|chestnut|broadleaf||
Ostrya|Hopfenbuche|hop hornbeam (genus)|broadleaf||
Taxus|Eibe|yew (genus)|conifer||
Thuja|Thuje|arborvitae|conifer||Thujen;Lebensbaum
Celtis|Zürgelbaum|hackberry (genus)|broadleaf||
Platanus|Platane|plane (genus)|broadleaf||Platanen
Ailanthus|Götterbaum|ailanthus|broadleaf||
Gleditsia|Gleditschie|honey locust (genus)|broadleaf||
Styphnolobium|Schnurbaum|pagoda tree (genus)|broadleaf||
Koelreuteria|Blasenbaum|golden rain tree (genus)|broadleaf||
Ginkgo|Ginkgo|ginkgo (genus)|broadleaf||
Cydonia|Quitte|quince (genus)|fruit||
Catalpa|Trompetenbaum|catalpa (genus)|broadleaf||
Paulownia|Blauglockenbaum|paulownia (genus)|broadleaf||
Morus|Maulbeere|mulberry|fruit||Maulbeerbaum
Zelkova|Zelkove|zelkova|broadleaf||
Liriodendron|Tulpenbaum|tulip tree (genus)|broadleaf||
Rhus|Sumach|sumac|broadleaf||
Cornus|Hartriegel|dogwood|broadleaf||
Gymnocladus|Geweihbaum|coffeetree|broadleaf||
Cercis|Judasbaum|redbud|broadleaf||
Crataegus|Weißdorn|hawthorn|broadleaf||
Chamaecyparis|Scheinzypresse|false cypress|conifer||
Elaeagnus|Ölweide|oleaster|broadleaf||
Platycladus|Morgenländischer Lebensbaum|platycladus|conifer||
Pterocarya|Flügelnuss|wingnut|broadleaf||
Parrotia|Eisenholzbaum|ironwood|broadleaf||
Liquidambar|Amberbaum|sweetgum (genus)|broadleaf||
Cupressus|Zypresse|cypress|conifer||Zypressen
Amelanchier|Felsenbirne|serviceberry|broadleaf||
Metasequoia|Urweltmammutbaum|dawn redwood (genus)|conifer||
Cedrus|Zeder|cedar|conifer||Zedern
Magnolia|Magnolie|magnolia|broadleaf||Magnolien;Magnolienbaum
Cercidiphyllum|Katsurabaum|katsura (genus)|broadleaf||
Juniperus|Wacholder|juniper (genus)|conifer||
Ilex|Stechpalme|holly (genus)|broadleaf||
Ficus|Feige|fig (genus)|fruit||
Mespilus|Mispel|medlar (genus)|fruit||
Broussonetia|Papiermaulbeere|paper mulberry (genus)|broadleaf||
Sequoiadendron|Mammutbaum|giant sequoia (genus)|conifer||
Albizia|Seidenbaum|albizia|broadleaf||
Laburnum|Goldregen|laburnum (genus)|broadleaf||
Cladrastis|Gelbholz|yellowwood (genus)|broadleaf||
Tetradium|Bienenbaum|tetradium|broadleaf||Euodia;Evodia
Maclura|Osagedorn|maclura|broadleaf||
Taxodium|Sumpfzypresse|bald cypress (genus)|conifer||
Nyssa|Tupelobaum|tupelo|broadleaf||
Davidia|Taschentuchbaum|davidia|broadleaf||
Tsuga|Hemlocktanne|hemlock|conifer||
Calocedrus|Weihrauchzeder|incense cedar (genus)|conifer||
Cryptomeria|Sicheltanne|cryptomeria|conifer||
Araucaria|Araukarie|araucaria|conifer||
Diospyros|Kaki|persimmon (genus)|fruit||"""

_GROUP_ROWS = (('Coniferae', 'Nadelbaum', 'conifer', 'conifer', ['Ndh', 'NH'],
                ['Nadelholz', 'Nadelbäume', 'Nadelgehölz', 'conifers', 'softwood', 'Pinopsida']),
               ('Magnoliopsida', 'Laubbaum', 'broadleaf', 'broadleaf', ['Lbh', 'LH'],
                ['Laubholz', 'Laubbäume', 'Laubgehölz', 'broadleaved tree', 'deciduous tree', 'hardwood']))

# EPPO codes by taxon (Wikidata P3031; tools/taxa_eppo.py regenerates this block)
# --- EPPO BEGIN ---
_EPPO: dict = {
    'Abies': ('1ABIG',),
    'Abies alba': ('ABIAL',),
    'Abies cephalonica': ('ABICE',),
    'Abies concolor': ('ABICO',),
    'Abies grandis': ('ABIGR',),
    'Abies koreana': ('ABIKO',),
    'Abies nordmanniana': ('ABINO',),
    'Abies pinsapo': ('ABIPN',),
    'Abies procera': ('ABIPR',),
    'Acer': ('1ACRG',),
    'Acer buergerianum': ('ACRBU',),
    'Acer campestre': ('ACRCA',),
    'Acer cappadocicum': ('ACRCP',),
    'Acer griseum': ('ACRGS',),
    'Acer monspessulanum': ('ACRMS',),
    'Acer negundo': ('ACRNE',),
    'Acer opalus': ('ACROP',),
    'Acer palmatum': ('ACRPA',),
    'Acer platanoides': ('ACRPL',),
    'Acer pseudoplatanus': ('ACRPP',),
    'Acer rubrum': ('ACRRB',),
    'Acer saccharinum': ('ACRSA',),
    'Acer saccharum': ('ACRSC',),
    'Acer tataricum': ('ACRTA',),
    'Acer truncatum': ('ACRTU',),
    'Acer x freemanii': ('ACRFE',),
    'Aesculus': ('1AECG',),
    'Aesculus flava': ('AECOC',),
    'Aesculus hippocastanum': ('AECHI',),
    'Aesculus x carnea': ('AECCR',),
    'Ailanthus': ('1AILG',),
    'Ailanthus altissima': ('AILAL',),
    'Albizia': ('1ALBG',),
    'Albizia julibrissin': ('ALBJU',),
    'Alnus': ('1ALUG',),
    'Alnus alnobetula': ('ALUVI',),
    'Alnus cordata': ('ALUCO',),
    'Alnus glutinosa': ('ALUGL',),
    'Alnus incana': ('ALUIN',),
    'Amelanchier': ('1AMEG',),
    'Amelanchier ovalis': ('AMEOV',),
    'Araucaria': ('1ARUG',),
    'Araucaria araucana': ('ARUAR',),
    'Betula': ('1BETG',),
    'Betula papyrifera': ('BETPA',),
    'Betula pendula': ('BETPE',),
    'Betula pubescens': ('BETPB',),
    'Betula utilis': ('BETUT',),
    'Broussonetia': ('1BRNG',),
    'Broussonetia papyrifera': ('BRNPA',),
    'Calocedrus': ('1CCDG',),
    'Calocedrus decurrens': ('CCDDE',),
    'Carpinus': ('1CIPG',),
    'Carpinus betulus': ('CIPBE',),
    'Castanea': ('1CSNG',),
    'Castanea sativa': ('CSNSA',),
    'Catalpa': ('1CTLG',),
    'Catalpa bignonioides': ('CTLBI',),
    'Cedrus': ('1CEUG',),
    'Cedrus atlantica': ('CEUAT',),
    'Cedrus deodara': ('CEUDE',),
    'Cedrus libani': ('CEULI',),
    'Celtis': ('1CETG',),
    'Celtis australis': ('CETAU',),
    'Celtis occidentalis': ('CETOC',),
    'Cercidiphyllum': ('1CCPG',),
    'Cercidiphyllum japonicum': ('CCPJA',),
    'Cercis': ('1CCSG',),
    'Cercis canadensis': ('CCSCA',),
    'Cercis siliquastrum': ('CCSSI',),
    'Chamaecyparis': ('1CHCG',),
    'Chamaecyparis lawsoniana': ('CHCLA',),
    'Cladrastis': ('1CLQG',),
    'Cladrastis kentukea': ('CLQLU',),
    'Cornus': ('1CRWG',),
    'Cornus mas': ('CRWMS',),
    'Corylus': ('1CYLG',),
    'Corylus avellana': ('CYLAV',),
    'Corylus colurna': ('CYLCO',),
    'Crataegus': ('1CSCG',),
    'Crataegus laevigata': ('CSCOX',),
    'Crataegus monogyna': ('CSCMO',),
    'Cryptomeria': ('1CMYG',),
    'Cryptomeria japonica': ('CMYJA',),
    'Cupressus': ('1CVBG',),
    'Cupressus nootkatensis': ('CHCNO',),
    'Cupressus sempervirens': ('CVBSE',),
    'Cydonia': ('1CYDG',),
    'Cydonia oblonga': ('CYDOB',),
    'Davidia': ('1DAVG',),
    'Davidia involucrata': ('DAVIN',),
    'Diospyros': ('1DOSG',),
    'Diospyros kaki': ('DOSKA',),
    'Elaeagnus': ('1ELGG',),
    'Elaeagnus angustifolia': ('ELGAN',),
    'Fagus': ('1FAUG',),
    'Fagus sylvatica': ('FAUSY',),
    'Ficus': ('1FIUG',),
    'Ficus carica': ('FIUCA',),
    'Fraxinus': ('1FRXG',),
    'Fraxinus americana': ('FRXAM',),
    'Fraxinus angustifolia': ('FRXAN',),
    'Fraxinus excelsior': ('FRXEX',),
    'Fraxinus ornus': ('FRXOR',),
    'Fraxinus pennsylvanica': ('FRXPE',),
    'Ginkgo': ('1GIKG',),
    'Ginkgo biloba': ('GIKBI',),
    'Gleditsia': ('1GLIG',),
    'Gleditsia triacanthos': ('GLITR',),
    'Gymnocladus': ('1GYMG',),
    'Gymnocladus dioicus': ('GYMDI',),
    'Ilex': ('1ILEG',),
    'Ilex aquifolium': ('ILEAQ',),
    'Juglans': ('1IUGG',),
    'Juglans nigra': ('IUGNI',),
    'Juglans regia': ('IUGRE',),
    'Juniperus': ('1IUPG',),
    'Juniperus chinensis': ('IUPCH',),
    'Juniperus communis': ('IUPCO',),
    'Juniperus virginiana': ('IUPVI',),
    'Koelreuteria': ('1KOTG',),
    'Koelreuteria paniculata': ('KOTPA',),
    'Laburnum': ('1LABG',),
    'Laburnum anagyroides': ('LABAN',),
    'Larix': ('1LAXG',),
    'Larix decidua': ('LAXDE',),
    'Larix kaempferi': ('LAXLE',),
    'Liquidambar': ('1LIQG',),
    'Liquidambar styraciflua': ('LIQST',),
    'Liriodendron': ('1LIRG',),
    'Liriodendron tulipifera': ('LIRTU',),
    'Maclura': ('1MACG',),
    'Maclura pomifera': ('MACPO',),
    'Magnolia': ('1MAGG',),
    'Magnolia grandiflora': ('MAGGR',),
    'Magnolia kobus': ('MAGKO',),
    'Malus': ('1MABG',),
    'Malus domestica': ('MABSD',),
    'Malus sylvestris': ('MABSY',),
    'Mespilus': ('1MSPG',),
    'Mespilus germanica': ('MSPGE',),
    'Metasequoia': ('1MEQG',),
    'Metasequoia glyptostroboides': ('MEQGL',),
    'Morus': ('1MORUG',),
    'Morus alba': ('MORAL',),
    'Morus nigra': ('MORNI',),
    'Nyssa': ('1NYSG',),
    'Nyssa sylvatica': ('NYSSY',),
    'Ostrya': ('1OSTG',),
    'Ostrya carpinifolia': ('OSTCA',),
    'Parrotia': ('1PTTG',),
    'Parrotia persica': ('PTTPE',),
    'Paulownia': ('1PAZG',),
    'Paulownia tomentosa': ('PAZTO',),
    'Picea': ('1PIEG',),
    'Picea abies': ('PIEAB',),
    'Picea glauca': ('PIEGA',),
    'Picea omorika': ('PIEOM',),
    'Picea orientalis': ('PIEOR',),
    'Picea pungens': ('PIEPU',),
    'Picea sitchensis': ('PIESI',),
    'Pinus': ('1PIUG',),
    'Pinus cembra': ('PIUCE',),
    'Pinus heldreichii': ('PIUHE',),
    'Pinus mugo': ('PIUMU',),
    'Pinus nigra': ('PIUNI',),
    'Pinus ponderosa': ('PIUPO',),
    'Pinus strobus': ('PIUST',),
    'Pinus sylvestris': ('PIUSI',),
    'Pinus uncinata': ('PIUUN',),
    'Pinus wallichiana': ('PIUWA',),
    'Platanus': ('1PLTG',),
    'Platanus orientalis': ('PLTOR',),
    'Platanus x hispanica': ('PLTHY',),
    'Platycladus': ('1PTZG',),
    'Platycladus orientalis': ('THUOR',),
    'Populus': ('1POPG',),
    'Populus alba': ('POPAL',),
    'Populus balsamifera': ('POPBA',),
    'Populus nigra': ('POPNI',),
    'Populus simonii': ('POPSM',),
    'Populus tremula': ('POPTR',),
    'Populus x canadensis': ('POPCA',),
    'Prunus': ('1PRNG',),
    'Prunus armeniaca': ('PRNAR',),
    'Prunus avium': ('PRNAV',),
    'Prunus cerasifera': ('PRNCF',),
    'Prunus cerasus': ('PRNCE',),
    'Prunus domestica': ('PRNDO',),
    'Prunus dulcis': ('PRNDU',),
    'Prunus mahaleb': ('PRNMH',),
    'Prunus padus': ('PRNPA',),
    'Prunus persica': ('PRNPS',),
    'Prunus sargentii': ('PRNSG',),
    'Prunus serotina': ('PRNSO',),
    'Prunus serrulata': ('PRNSL',),
    'Prunus x yedoensis': ('PRNYE',),
    'Pseudotsuga': ('1PSTG',),
    'Pseudotsuga menziesii': ('PSTME',),
    'Pterocarya': ('1PTFG',),
    'Pterocarya fraxinifolia': ('PTFFR',),
    'Pyrus': ('1PYUG',),
    'Pyrus calleryana': ('PYUCA',),
    'Pyrus communis': ('PYUCO',),
    'Pyrus pyraster': ('PYUPS',),
    'Pyrus salicifolia': ('PYUSA',),
    'Quercus': ('1QUEG',),
    'Quercus cerris': ('QUECE',),
    'Quercus coccinea': ('QUECO',),
    'Quercus frainetto': ('QUEFR',),
    'Quercus palustris': ('QUEPA',),
    'Quercus petraea': ('QUEPE',),
    'Quercus pubescens': ('QUEPU',),
    'Quercus robur': ('QUERO',),
    'Quercus rubra': ('QUERU',),
    'Rhus': ('1RHUG',),
    'Rhus typhina': ('RHUTY',),
    'Robinia': ('1ROBG',),
    'Robinia pseudoacacia': ('ROBPS',),
    'Salix': ('1SAXG',),
    'Salix alba': ('SAXAL',),
    'Salix appendiculata': ('SAXAQ',),
    'Salix babylonica': ('SAXBA',),
    'Salix caprea': ('SAXCP',),
    'Salix daphnoides': ('SAXDA',),
    'Salix eleagnos': ('SAXEL',),
    'Salix fragilis': ('SAXFR',),
    'Salix pentandra': ('SAXPE',),
    'Salix purpurea': ('SAXPU',),
    'Salix viminalis': ('SAXVI',),
    'Sambucus': ('1SAMG',),
    'Sambucus nigra': ('SAMNI',),
    'Sequoiadendron': ('1SQUG',),
    'Sequoiadendron giganteum': ('SQUGI',),
    'Sorbus': ('1SOUG',),
    'Sorbus aria': ('SOUAI',),
    'Sorbus aucuparia': ('SOUAU',),
    'Sorbus domestica': ('SOUDO',),
    'Sorbus intermedia': ('SOUIT',),
    'Sorbus torminalis': ('SOUTO',),
    'Styphnolobium japonicum': ('SOBJA',),
    'Taxodium': ('1TADG',),
    'Taxodium distichum': ('TADDI',),
    'Taxus': ('1TAXG',),
    'Taxus baccata': ('TAXBA',),
    'Tetradium': ('1TTDG',),
    'Tetradium daniellii': ('EVDDA',),
    'Thuja': ('1THUG',),
    'Thuja occidentalis': ('THUOC',),
    'Thuja plicata': ('THUPL',),
    'Tilia': ('1TILG',),
    'Tilia americana': ('TILAM',),
    'Tilia cordata': ('TILCO',),
    'Tilia platyphyllos': ('TILPL',),
    'Tilia tomentosa': ('TILTO',),
    'Tilia x europaea': ('TILVU',),
    'Tsuga': ('1TSUG',),
    'Tsuga canadensis': ('TSUCA',),
    'Ulmus': ('1ULMG',),
    'Ulmus glabra': ('ULMGL',),
    'Ulmus laevis': ('ULMLV',),
    'Ulmus minor': ('ULMMI',),
    'Ulmus pumila': ('ULMPU',),
    'Zelkova': ('1ZELG',),
    'Zelkova serrata': ('ZELSE',),
}
# --- EPPO END ---


# === SECTION: normalisation ===

_UML = (('ä', 'ae'), ('ö', 'oe'), ('ü', 'ue'))
_SUFFIX = re.compile(r'\s+(sp|spp|spec)\.?$')


def _clean(s: str) -> str:
    """lower-case, ß→ss, × → ' x ', separators (hyphen, dot, underscore, comma, quotes) → space."""
    s = unicodedata.normalize('NFC', s).strip().lower().replace('ß', 'ss').replace('×', ' x ')
    s = re.sub(r"[\-‐-―._,'’`()/]+", ' ', s)
    return re.sub(r'\s+', ' ', s).strip()


def _strip_marks(s: str) -> str:
    return ''.join(ch for ch in unicodedata.normalize('NFD', s) if not unicodedata.combining(ch))


def _compact(s: str) -> str:
    return ''.join(t for t in s.split(' ') if t != 'x')


def keys(raw: str) -> list:
    """Lookup keys of a name: without diacritics, and with ä/ö/ü as ae/oe/ue.
    Spaces, hyphens, dots and the hybrid sign do not matter."""
    c = _clean(raw)
    a = _compact(_strip_marks(c))
    u = c
    for x, y in _UML:
        u = u.replace(x, y)
    b = _compact(_strip_marks(u))
    return [a] if a == b else [a, b]


def tokens(raw: str) -> list:
    return [t for t in _strip_marks(_clean(raw)).split(' ') if t and t != 'x']


# === SECTION: table ===

def _rows(text):
    for line in text.splitlines():
        p = line.split('|')
        assert len(p) == 6, line
        yield p[0], p[1], p[2], p[3], [c for c in p[4].split(',') if c], [a for a in p[5].split(';') if a]


def _build():
    genus = {g[0]: g for g in _rows(_G)}
    # a genus' plain German name ("Thuje", "Magnolie") means the genus unless a row claims it ("Fichte" = Picea abies)
    claimed = {k for r in (*_rows(_S), *_rows(_G)) for nm in (*(r[1:3] if ' ' in r[0] else r[2:3]), *r[4], *r[5])
               for k in keys(nm)}
    for g in genus.values():
        if not set(keys(g[1])) & claimed:
            g[5].insert(0, g[1])
            claimed.update(keys(g[1]))
    out, seen = [], set()
    for sp in _rows(_S):
        out.append(sp)
        g = sp[0].split()[0]
        if g not in seen:
            seen.add(g)
            gr = genus[g]
            out.append((gr[0], gr[1] + ' (Gattung)', gr[2], gr[3], gr[4], gr[5]))
    assert seen == set(genus), sorted(set(genus) ^ seen)
    out.extend(_GROUP_ROWS)
    table = []
    for i, (tx, de, en, cl, codes, alt) in enumerate(out, 1):
        codes = list(codes) + [c for c in _EPPO.get(tx, ()) if c not in codes]
        table.append((tx, de, en, cl, i, codes, list(alt)))
    return tuple(table)


TABLE = _build()                                   # rows in FIELDS order, rank ascending
TAXA = tuple(t[:4] for t in TABLE)                 # the old 4-column view
RANK = {t[0]: t[4] for t in TABLE}
CLASS = {t[0]: t[3] for t in TABLE}
ROW = {t[0]: t for t in TABLE}
IS_GENUS = {t[0]: (' ' not in t[0] and t[0] not in GROUPS) for t in TABLE}
GENUS_CLASS: dict = {}
for _t in TABLE:
    if ' ' in _t[0]:
        GENUS_CLASS.setdefault(_t[0].split()[0], _t[3])
for _t in TABLE:
    if IS_GENUS[_t[0]]:
        GENUS_CLASS.setdefault(_t[0], _t[3])


def digest() -> str:
    return hashlib.sha256(repr(TABLE).encode()).hexdigest()[:16]


def names(row) -> list:
    """Every string that must resolve to this row (taxon first)."""
    tx, de, en, _cl, _rk, codes, alt = row
    return [tx, de, en, *codes, *alt]


def build_index() -> tuple[dict, list]:
    """→ (key → taxon, collisions).  A genus word that is also the common name
    of a species of that genus ("Ginkgo") goes to the species."""
    idx: dict = {}
    clashes = []
    late = []
    for row in TABLE:
        for j, nm in enumerate(names(row)):
            if j == 0 and IS_GENUS[row[0]]:
                late.append(row[0])
                continue
            for k in keys(nm):
                o = idx.setdefault(k, row[0])
                if o != row[0]:
                    clashes.append((nm, o, row[0]))
    for g in late:
        for k in keys(g):
            o = idx.setdefault(k, g)
            if o != g and o.split()[0] != g:
                clashes.append((g, o, g))
    return idx, clashes


INDEX, CLASHES = build_index()


def _epithet(tx: str):
    p = [w for w in tx.lower().split() if w != 'x']
    return (p[0], p[1]) if len(p) == 2 else None


_SPECIES = [(t[0],) + _epithet(t[0]) for t in TABLE if _epithet(t[0])]
_NN: dict = {}
for _tx, _g, _e in _SPECIES:
    for _n in (2, 3, 4):
        if len(_g) >= _n and len(_e) >= _n:
            _NN.setdefault(_g[:_n] + _e[:_n], []).append(_tx)
_KEYLIST = sorted(INDEX)
_LATIN = re.compile(r'^[a-z]{3,24}( (x )?[a-z][a-z-]{2,30})?$')


# === SECTION: resolver ===

class Unknown(ValueError):
    def __init__(self, msg: str, did_you_mean: list, ambiguous: bool = False):
        super().__init__(msg)
        self.did_you_mean = did_you_mean
        self.ambiguous = ambiguous


def _by_rank(taxa) -> list:
    return sorted(set(taxa), key=lambda t: RANK.get(t, 10 ** 6))


def _abbrev(raw: str) -> list:
    """Rule-based Latin abbreviations → matching species (rank order)."""
    tk = tokens(raw)
    if not tk or not all(t.isalpha() and t.isascii() for t in tk):
        return []
    hits = []
    if len(tk) == 2 and len(tk[1]) >= 2:           # "p abies", "pic abi", "pice abie", "pic abies"
        hits = [tx for tx, g, e in _SPECIES if g.startswith(tk[0]) and e.startswith(tk[1])]
    if not hits:
        c = ''.join(tk)
        if len(c) in (4, 6, 8) and len(tk) <= 2:   # PiAb / PicAbi / PiceAbie
            hits = list(_NN.get(c, ()))
    return _by_rank(hits)


def _lev(a: str, b: str, cap: int) -> int:
    if abs(len(a) - len(b)) > cap:
        return cap + 1
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        lo = i
        for j, cb in enumerate(b, 1):
            v = min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb))
            cur.append(v)
            lo = min(lo, v)
        if lo > cap:
            return cap + 1
        prev = cur
    return prev[-1]


def _near(k: str) -> list:
    """Keys within edit distance 1 (2 from 7 characters) → [(distance, taxon)]."""
    cap = 2 if len(k) >= 7 else 1
    out = []
    for key in _KEYLIST:
        d = _lev(k, key, cap)
        if d <= cap:
            out.append((d, INDEX[key]))
    return out


def suggest(raw: str, n: int = 5) -> list:
    """≤ n taxa, best match first, ties by rank: near-misses by edit distance,
    then names containing / starting with the input, then fuzzy matches."""
    import difflib
    best: dict = {}

    def put(tx, score):
        if tx not in best or score < best[tx]:
            best[tx] = score
    for k in keys(raw):
        if not k:
            continue
        for d, tx in _near(k):
            put(tx, d)
        if len(k) >= 3:
            for key in _KEYLIST:
                if key.startswith(k):
                    put(INDEX[key], 3)
                elif len(k) >= 4 and k in key:
                    put(INDEX[key], 4)
        for m in difflib.get_close_matches(k, _KEYLIST, n=6, cutoff=0.72):
            put(INDEX[m], 5 + (1 - difflib.SequenceMatcher(None, k, m).ratio()))
    return [t for t, _ in sorted(best.items(), key=lambda kv: (kv[1], RANK[kv[0]]))][:n]


def resolve(raw: str, max_chars: int = 60) -> tuple[str, bool]:
    """→ (canonical taxon, listed).  Order: exact name / code → rule-based Latin
    abbreviation → unlisted well-formed binomial (listed False) → Unknown."""
    s = _SUFFIX.sub('', _clean(raw)[:max_chars])
    ks = keys(s)
    for k in ks:
        hit = INDEX.get(k)
        if hit:
            return hit, True
    ab = _abbrev(s)
    if len(ab) == 1:
        return ab[0], True
    if len(ab) > 1:
        raise Unknown(f'ambiguous abbreviation {raw!r}: write more letters or use a code from the dictionary',
                      ab[:5], ambiguous=True)
    free = ' '.join(t for t in _strip_marks(s).split(' ') if t)
    free = re.sub(r'^([a-z]+) x ', r'\1 x ', free)
    if _LATIN.match(free) and ' ' in free:
        # one slip away from a listed name is a typo, not a new species
        close = [tx for k in ks for d, tx in _near(k) if d <= 1]
        if not close:
            return free[0].upper() + free[1:], False
        raise Unknown(f'unknown species {raw!r} — nearly a listed name', _by_rank(close)[:5])
    raise Unknown(f'unknown species {raw!r}: send a scientific name ("Quercus robur", genus alone is fine), a German '
                  'or English name, a forestry or EPPO code ("Fi", "PIEAB") or an abbreviation ("P. abies", "PiAb")',
                  suggest(s))


def taxon_class(latin: str | None) -> str | None:
    if not latin:
        return None
    return CLASS.get(latin) or GENUS_CLASS.get(latin.split()[0])


def as_dict() -> dict:
    return {'rev': REV, 'fields': list(FIELDS), 'taxa': [list(t) for t in TABLE], 'classes': list(CLASSES)}
