#!/usr/bin/env bash
# scripts/build-protos.sh — Generate code from .proto files with buf
# Usage: ./scripts/build-protos.sh [--go] [--java]
# Requires: buf (https://buf.build/docs/installation)
#           protoc-gen-go + protoc-gen-go-grpc (for Go)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "==> Validating buf configuration..."
buf build --error-format text

echo "==> Generating code with buf..."
buf generate

echo "==> Generation complete."
echo "    Go output  : gen/go/"
echo "    Java output: gen/java/"
