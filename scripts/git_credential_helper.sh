#!/bin/sh
# git credential helper: answers `get` with the read-only GitHub token from
# data/github_token (distributed by the director, never in git).
# Installed into the repo's local config by git_auth.ensure_helper().
[ "$1" = "get" ] || exit 0
f="$(dirname "$0")/../data/github_token"
[ -s "$f" ] || exit 0
echo "username=x-access-token"
echo "password=$(head -n1 "$f" | tr -d '\r\n ')"
