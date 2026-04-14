#!/usr/bin/env bash
# scripts/lint-protos.sh — Lint all .proto files with buf
# Usage: ./scripts/lint-protos.sh
# Requires: buf (https://buf.build/docs/installation)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "==> Linting Protobuf files with buf..."
buf lint

echo "==> Checking breaking changes against master..."
if git rev-parse --verify origin/master > /dev/null 2>&1; then
  buf breaking --against ".git#branch=origin/master"
else
  echo "    (skipping breaking check — origin/master not available)"
fi

echo "==> Done. No lint errors."
