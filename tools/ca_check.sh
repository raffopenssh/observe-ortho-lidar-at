#!/bin/bash
# Verify this host does NOT trust the compromised eMudhra intermediates
# (https://emudhra.we-have-a-problem.com/, Oct 2026) across every client
# stack the project uses: system curl, Python ssl/requests (openeo, zenodo,
# peers), GDAL /vsicurl (BEV rasters), Go, Node. Exit 1 if anything accepts.
# Usage: bash tools/ca_check.sh      (run on primary or any peer)
set -u
bad=0
H=dv-g3b-emudhra.we-have-a-problem.com
hosts="dv-g3b dv-g3b-aaa dv-g3b-noct dv-g3b-aaa-noct"
echo "ca-certificates: $(dpkg-query -W -f='${Version}' ca-certificates 2>/dev/null)"
for h in $hosts; do
  if curl -s -m 10 -o /dev/null "https://$h-emudhra.we-have-a-problem.com/"; then echo "curl $h: ACCEPTED (BAD)"; bad=1; else echo "curl $h: rejected"; fi
done
python3 - "$H" <<'PY' || bad=1
import sys, ssl, urllib.request
H='https://'+sys.argv[1]+'/'; ok=True
def t(name, fn):
    global ok
    try: fn(); print(f"python {name}: ACCEPTED (BAD)"); ok=False
    except Exception as e: print(f"python {name}: rejected ({type(e).__name__})")
t('ssl-default', lambda: urllib.request.urlopen(H, timeout=10))
try:
    import requests; t('requests', lambda: requests.get(H, timeout=10))
except ImportError: pass
try:
    import rasterio
    def g():
        try: rasterio.open('/vsicurl/'+H+'x.tif')
        except Exception as e:
            if 'SSL' in str(e) or 'certificate' in str(e): raise
            print('python gdal: no SSL error ->', str(e)[:120]); ok=False
    t('gdal-vsicurl', g)
except ImportError: pass
sys.exit(0 if ok else 1)
PY
command -v node >/dev/null && { node -e 'require("https").get("https://'"$H"'/",r=>{console.log("node: ACCEPTED (BAD)");process.exit(1)}).on("error",e=>console.log("node: rejected ("+e.code+")"))' || bad=1; }
[ $bad = 0 ] && echo "RESULT: OK — compromised intermediates not trusted" || { echo "RESULT: VULNERABLE — run: sudo apt-get install --only-upgrade ca-certificates"; exit 1; }
