#!/usr/bin/env bash
# Deploy the current origin/master to the live server.
#
# The host, key and paths live in .deploy.env, which is gitignored: this file
# is public and they are not. See .deploy.env.example.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ ! -f .deploy.env ]; then
  echo "No .deploy.env — copy .deploy.env.example and fill it in." >&2
  exit 1
fi
# shellcheck disable=SC1091
. ./.deploy.env

: "${DEPLOY_HOST:?set DEPLOY_HOST in .deploy.env}"
: "${DEPLOY_PATH:?set DEPLOY_PATH in .deploy.env}"
: "${DEPLOY_SERVICE:?set DEPLOY_SERVICE in .deploy.env}"
DEPLOY_URL="${DEPLOY_URL:-}"
# Expanded here, not on the server: the remote shell has no .deploy.env, so an
# escaped default silently checked the wrong port.
DEPLOY_PORT="${DEPLOY_PORT:-3000}"
ssh_args=(-o BatchMode=yes -o ConnectTimeout=10)
[ -n "${DEPLOY_KEY:-}" ] && ssh_args+=(-i "$DEPLOY_KEY")

# The server pulls from origin, so anything still sitting in the working tree
# or unpushed would deploy as whatever master last had instead — silently.
git fetch -q origin master
local_sha=$(git rev-parse @)
remote_sha=$(git rev-parse origin/master)
if [ "$local_sha" != "$remote_sha" ]; then
  echo "warning: local HEAD and origin/master differ — deploying origin/master" >&2
  echo "  local  $local_sha" >&2
  echo "  origin $remote_sha" >&2
fi
if [ -n "$(git status --porcelain)" ]; then
  echo "warning: uncommitted changes are not part of this deploy" >&2
fi

echo "==> deploying ${remote_sha:0:7} to $DEPLOY_HOST:$DEPLOY_PATH"
ssh "${ssh_args[@]}" "$DEPLOY_HOST" bash -s <<REMOTE
set -euo pipefail
cd "$DEPLOY_PATH"
git fetch -q origin master
git merge --ff-only origin/master
pnpm install --frozen-lockfile --silent
# A restart, not stop-then-start: it re-binds within the same second and so
# wins the race for the port against anything else on the box that wants it.
sudo systemctl restart "$DEPLOY_SERVICE"
sleep 2
systemctl is-active --quiet "$DEPLOY_SERVICE" || {
  echo "service is not active after restart:" >&2
  journalctl -u "$DEPLOY_SERVICE" -n 20 --no-pager >&2
  exit 1
}
echo -n "==> deployed \$(git rev-parse --short HEAD): "
curl -sf localhost:$DEPLOY_PORT/healthz || { echo "healthz on port $DEPLOY_PORT failed" >&2; exit 1; }
echo
REMOTE

if [ -n "$DEPLOY_URL" ]; then
  echo -n "==> public: "
  curl -sf --max-time 10 "$DEPLOY_URL/healthz" || { echo "public healthz failed" >&2; exit 1; }
  echo
fi
