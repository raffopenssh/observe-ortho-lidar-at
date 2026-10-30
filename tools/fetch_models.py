#!/usr/bin/env python3
"""Download the trained model artefacts that are too large for git.

Reads ``data/models_manifest.json`` (shipped) and fetches every entry
into its ``path``, verifying the MD5 checksum.  Sources, in order:

1. ``MODELS_BASE_URL`` env — any HTTP(S) directory containing the files
   (e.g. a GitHub release or a mirror).
2. The Zenodo record(s) in the manifest.  Published records need no
   credentials.  While a record is still an unpublished deposit, set
   ``ZENODO_TOKEN`` (owner token) and the file is fetched from the
   deposit bucket instead.

Usage::

    python3 tools/fetch_models.py            # fetch what is missing
    python3 tools/fetch_models.py --force    # re-download everything
    python3 tools/fetch_models.py --check    # verify checksums only
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import config  # noqa: E402

MANIFEST = ROOT / "data" / "models_manifest.json"
UA = {"User-Agent": "observe-ortho-lidar-at/1.0 (+https://github.com/raffopenssh/observe-ortho-lidar-at)"}


def md5_of(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _stream(url: str, dest: Path, headers: dict, expect_md5: str) -> None:
    tmp = dest.with_suffix(dest.suffix + ".part")
    with requests.get(url, headers=headers, stream=True, timeout=(30, 600)) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length") or 0)
        done = 0
        with tmp.open("wb") as fh:
            for chunk in r.iter_content(1 << 20):
                fh.write(chunk)
                done += len(chunk)
                if total:
                    print(f"\r  {dest.name}: {done / 1e6:7.1f} / {total / 1e6:.1f} MB", end="", flush=True)
    print()
    got = md5_of(tmp)
    if got != expect_md5:
        tmp.unlink(missing_ok=True)
        raise SystemExit(f"checksum mismatch for {dest.name}: {got} != {expect_md5}")
    tmp.replace(dest)


def candidate_urls(entry: dict) -> list[tuple[str, dict]]:
    out: list[tuple[str, dict]] = []
    base = (os.environ.get("MODELS_BASE_URL") or "").rstrip("/")
    if base:
        out.append((f"{base}/{entry['filename']}", dict(UA)))
    rec = entry.get("zenodo_record")
    if rec:
        out.append((f"{config.ZENODO_BASE_URL}/records/{rec}/files/{entry['filename']}?download=1", dict(UA)))
    tok = config.zenodo_token()
    if tok and entry.get("bucket_url"):
        out.append((f"{entry['bucket_url']}/{entry['filename']}",
                    {**UA, "Authorization": f"Bearer {tok}"}))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()

    manifest = json.loads(MANIFEST.read_text())
    rc = 0
    for key, e in manifest["entries"].items():
        dest = ROOT / e["path"]
        want = e["md5"]
        if dest.exists() and md5_of(dest) == want and not args.force:
            print(f"ok       {key}  ({dest.relative_to(ROOT)})")
            continue
        if args.check:
            print(f"MISSING  {key}  ({dest.relative_to(ROOT)})")
            rc = 1
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        last_err: Exception | None = None
        for url, headers in candidate_urls(e):
            try:
                print(f"fetch    {key} <- {url.split('?')[0]}")
                _stream(url, dest, headers, want)
                last_err = None
                break
            except Exception as exc:  # try the next source
                last_err = exc
                print(f"  failed: {exc}")
        if last_err is not None or not dest.exists():
            print(f"FAILED   {key}: no source worked (set MODELS_BASE_URL or ZENODO_TOKEN)")
            rc = 1
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
