#!/usr/bin/env bash
# Rebuild local Rust DuckDB extensions against the DuckDB version swanflow embeds (v1.5.6).
# Usage: bash scripts/rebuild_extensions_156.sh <repo1> [repo2 ...]
# After each successful build, copies the binary into ~/.duckdb/extensions/v1.5.6/linux_amd64/.
set -euo pipefail

TARGET=v1.5.6
DEST="$HOME/.duckdb/extensions/$TARGET/linux_amd64"
mkdir -p "$DEST"

for repo in "$@"; do
    name=$(basename "$repo")
    if [ ! -f "$repo/Makefile" ]; then
        echo "SKIP $name (no Makefile — likely not a quack-rs extension repo)"
        continue
    fi
    echo "=== $name ==="
    # Point the quack-rs build at the target DuckDB version
    sed -i "s/^TARGET_DUCKDB_VERSION=.*/TARGET_DUCKDB_VERSION=$TARGET/" "$repo/Makefile"
    if [ -x "$repo/configure" ]; then
        (cd "$repo" && ./configure) || { echo "CONFIGURE FAILED $name"; continue; }
    fi
    (cd "$repo" && make release) || { echo "BUILD FAILED $name"; continue; }
    # Locate the freshest binary
    bin=$(find "$repo/build" -name '*.duckdb_extension' -newer "$repo/Makefile" 2>/dev/null | head -1)
    if [ -z "$bin" ]; then
        echo "NO BINARY FOUND for $name (build may have skipped the copy step)"
        continue
    fi
    cp -f "$bin" "$DEST/"
    echo "OK $name -> $DEST/$(basename "$bin")"
done
echo "done."
