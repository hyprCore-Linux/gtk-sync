#!/usr/bin/env bash
# Thin wrapper — prefer ../install.sh from the repo root.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
exec "$ROOT/install.sh" --server "$@"
