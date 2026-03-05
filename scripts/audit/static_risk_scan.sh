#!/usr/bin/env bash
set -euo pipefail

if ! command -v rg >/dev/null 2>&1; then
  echo "warning: rg not found; skipping static risk scan" >&2
  exit 0
fi

# Heuristic scan for panic-prone or unsafe patterns in executable surfaces.
rg -n "unwrap\\(|expect\\(|panic!\\(|unsafe\\s*\\{" \
  src tests sidecars scripts examples \
  --glob '*.rs' --glob '*.py' --glob '*.sh' || true
