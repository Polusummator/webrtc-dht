#!/usr/bin/env zsh
set -euo pipefail

cd "$(dirname "$0")/.."

GOOS=linux GOARCH=amd64 go build -o dist/bench_transport     ./cmd/bench_transport
GOOS=linux GOARCH=amd64 go build -o dist/bench_signaling     ./cmd/bench_signaling
GOOS=linux GOARCH=amd64 go build -o dist/signal_server       ./cmd/signal_server

echo "binaries in dist/"
ls -lh dist/

