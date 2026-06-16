#!/usr/bin/env bash
# Ensure DuckDB shared library is available.
# Uses libduckdb-sys's built-in download mechanism — no manual setup needed.
# Just set DUCKDB_DOWNLOAD_LIB=1 and cargo handles the rest.
set -euo pipefail

echo "→ DuckDB library: automatic download via DUCKDB_DOWNLOAD_LIB=1"
echo "  (controlled by .cargo/config.toml — no manual steps needed)"
echo ""
echo "  To verify: cargo check --lib"
