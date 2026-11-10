#!/usr/bin/env bash
# Deploy llm-bridge-devin: build it from this clone's default branch and
# install it to ~/bin, where llm-bridge-server finds it on PATH.
#
# No restart. The server spawns this binary per session and per one-shot
# call, so the next of either runs the new one. A devin session already
# running keeps the binary it started with until it is respawned.
#
# ~/bin/deploy-gate refuses a side branch, a dirty tree or a commit that is
# not on origin, and records the deploy in repo-store's ledger.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
BIN_NAME="llm-bridge-devin"
USER_BIN="$HOME/bin/$BIN_NAME"
cd "$REPO_DIR"
export PATH="$HOME/.local/share/mise/shims:$PATH"

"$HOME/bin/deploy-gate" check

echo "==> Testing..."
go test ./...

echo "==> Building..."
BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "$BUILD_DIR"' EXIT
go build -o "$BUILD_DIR/$BIN_NAME" .

echo "==> Installing to $USER_BIN..."
if [ -e "$USER_BIN" ]; then
  cp -p "$USER_BIN" "$USER_BIN.backup-$(date -u +%Y%m%dT%H%M%S)"
fi
# install writes a new file rather than rewriting the old one in place, so a
# running process keeps the binary it loaded.
install -m 0755 "$BUILD_DIR/$BIN_NAME" "$USER_BIN"

echo "==> Smoke-checking the installed binary..."
# An empty prompt must be refused in the one-shot contract's own error shape:
# proves the installed binary has -oneshot and answers on stdout, without a
# model call.
SMOKE="$(echo '{"prompt":""}' | "$USER_BIN" -oneshot || true)"
if [ "$SMOKE" != '{"error":"prompt required"}' ]; then
  echo "ERROR: $USER_BIN -oneshot answered an empty prompt with: $SMOKE" >&2
  exit 1
fi
echo "    -oneshot answers in its contract"

# Runner machines download this wrapper from the bridge too; the shared script
# in llm-bridge-runner cross-builds and publishes it (todo 919fa8b7).
( cd "$REPO_DIR" && "$REPO_DIR/../llm-bridge-runner/scripts/publish-harness-wrapper.sh" "$BIN_NAME" )

"$HOME/bin/deploy-gate" record
echo "==> Done. Rollback: the newest $USER_BIN.backup-* next to it."
