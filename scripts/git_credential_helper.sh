#!/bin/sh
# git credential helper: answers `get` with the read-only GitHub token.
# Source order: $GITHUB_TOKEN (set by git_auth.git_env() from the primary's
# in-memory lease, fleet_secrets.py) -> legacy data/github_token (primary
# only; peers no longer keep a copy on disk).
# Installed into the repo's local config by git_auth.ensure_helper().
[ "$1" = "get" ] || exit 0
tok="$GITHUB_TOKEN"
if [ -z "$tok" ]; then
  f="$(dirname "$0")/../data/github_token"
  [ -s "$f" ] && tok="$(head -n1 "$f" | tr -d '\r\n ')"
fi
[ -n "$tok" ] || exit 0
echo "username=x-access-token"
echo "password=$tok"
