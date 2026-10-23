"""git_auth — read-only GitHub token for the (private) project repo.

The repo went private in Oct 2026. Peers update with ``git fetch origin``
(``app._safe_git_sync``), so every peer needs credentials. They come from
``data/github_token`` (0600, gitignored — GitHub revokes any token it finds
in a commit) via the credential helper ``scripts/git_credential_helper.sh``,
which :func:`ensure_helper` registers in the repo's *local* git config.
The primary's own global ``credential.helper=store`` (write token) is
consulted first, so pushes from the primary keep working unchanged.

Fleet distribution mirrors ``umfeld.py``: peers report ``git_token_sha`` in
the status push, the director POSTs its token to
``/api/v1/admin/install_git_token`` on mismatch (app.py).

Token: a GitHub fine-grained PAT, *Contents: read-only*, single repo.
"""
from __future__ import annotations

import hashlib
import os
import subprocess
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent
TOKEN_PATH = Path(os.environ.get('GITHUB_TOKEN_PATH') or 'data/github_token')
HELPER = REPO / 'scripts' / 'git_credential_helper.sh'

_lock = threading.Lock()
_cache: dict = {'tok': None, 'mtime': None, 'checked': 0.0}
_helper_ok = {'t': 0.0}


def token() -> str:
    env = (os.environ.get('GITHUB_TOKEN') or '').strip()
    if env:
        return env
    try:  # leased from the primary, memory-only (fleet_secrets.py); file = legacy fallback
        import fleet_secrets
        v = fleet_secrets.token('github')
        if v:
            return v
    except Exception:
        pass
    now = time.time()
    with _lock:
        if now - _cache['checked'] < 5 and _cache['tok'] is not None:
            return _cache['tok']
        _cache['checked'] = now
        try:
            st = TOKEN_PATH.stat()
            if st.st_mtime != _cache['mtime'] or _cache['tok'] is None:
                _cache['tok'] = TOKEN_PATH.read_text().strip()
                _cache['mtime'] = st.st_mtime
        except FileNotFoundError:
            _cache['tok'] = ''
            _cache['mtime'] = None
        except Exception:
            if _cache['tok'] is None:
                _cache['tok'] = ''
        return _cache['tok']


def token_sha() -> str | None:
    t = token()
    return hashlib.sha256(t.encode()).hexdigest()[:16] if t else None


def install_token(tok: str) -> bool:
    """Write the token (0600) + register the helper. True when changed."""
    tok = (tok or '').strip()
    if len(tok) < 16:
        raise ValueError('github token must be >= 16 chars')
    changed = token() != tok
    if changed:
        TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = TOKEN_PATH.with_suffix('.tmp')
        tmp.write_text(tok + '\n')
        os.chmod(tmp, 0o600)
        os.replace(tmp, TOKEN_PATH)
        with _lock:
            _cache.update(tok=tok, mtime=None, checked=0.0)
    ensure_helper()
    return changed


def ensure_helper(repo: Path | str = REPO) -> bool:
    """Register ``scripts/git_credential_helper.sh`` as a *local* credential
    helper (idempotent, at most once per 10 min). Returns True if set."""
    now = time.time()
    if now - _helper_ok['t'] < 600:
        return True
    try:
        want = '!' + str(HELPER)
        cur = subprocess.run(['git', 'config', '--local', '--get-all', 'credential.helper'],
                             capture_output=True, text=True, timeout=5, cwd=str(repo)).stdout.split('\n')
        if want not in [c.strip() for c in cur]:
            subprocess.run(['git', 'config', '--local', '--add', 'credential.helper', want],
                           capture_output=True, text=True, timeout=5, cwd=str(repo))
        try:
            os.chmod(HELPER, 0o755)
        except Exception:
            pass
        _helper_ok['t'] = now
        return True
    except Exception:
        return False


def git_env(base: dict | None = None) -> dict:
    """Environment for git subprocesses: never block on a terminal prompt."""
    env = dict(base if base is not None else os.environ)
    env['GIT_TERMINAL_PROMPT'] = '0'
    t = token()
    if t:
        env['GITHUB_TOKEN'] = t   # read by scripts/git_credential_helper.sh (never on disk)
    return env
