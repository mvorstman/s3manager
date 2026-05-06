#!/usr/bin/env bash
set -euo pipefail

export GOCACHE="${GOCACHE:-/tmp/s3manager-go-build}"

echo "==> Running go build ./..."
go build ./...

echo "==> Running go test ./..."
go test ./...

echo "==> Validation successful"
