#!/usr/bin/env bash
# Rebuild local Rust DuckDB extensions against the DuckDB version swanflow embeds (v1.5.6).
# Usage: bash scripts/rebuild_extensions_156.sh /path/to/ext-repo1 [/path/to/ext-repo2 ...]
#
# Each repo must be a quack-rs style extension (Makefile with TARGET_DUCKDB_VERSION,
# extension-ci-tools submodule providing configure/release targets). The script:
#   1. rewrites TARGET_DUCKDB_VERSION to v1.5.6
#   2. wipes and re-runs `make configure` (a stale configure/venv from another
#      python version breaks the build - we saw venv/bin vanish)
#   3. runs `make release`
#   4. installs the built binary into ~/.duckdb/extensions/v1.5.6/linux_amd64/
set -u

DEST="$HOME/.duckdb/extensions/v1.5.6/linux_amd64"
TARGET_VER="v1.5.6"

for repo in "$@"; do
    name=$(basename "$repo")
    echo "=== $name ==="
    if [ ! -f "$repo/Makefile" ]; then
        echo "SKIP $name: no Makefile (not a quack-rs repo?)"
        continue
    fi
    if ! cd "$repo"; then
        echo "SKIP $name: cannot cd"
        continue
    fi

    # Point the build at the version swanflow embeds.
    if grep -q '^TARGET_DUCKDB_VERSION=' Makefile; then
        sed -i "s|^TARGET_DUCKDB_VERSION=.*|TARGET_DUCKDB_VERSION=$TARGET_VER|" Makefile
    else
        echo "SKIP $name: no TARGET_DUCKDB_VERSION in Makefile"
        continue
    fi

    # A stale configure/venv (e.g. python upgraded underneath it) produces
    # cryptic failures; a clean configure is cheap compared to debugging that.
    rm -rf configure
    if ! make configure 2>&1 | tail -2; then
        echo "CONFIGURE FAILED $name"
        continue
    fi

    if ! make release 2>&1 | tail -3; then
        echo "RELEASE FAILED $name"
        continue
    fi

    # quack-rs lays out build/release/<name>/<name>.duckdb_extension OR
    # build/release/extension/<name>/<name>.duckdb_extension depending on vintage.
    # Pick the NEWEST binary by mtime — a `-newer Makefile` filter is unreliable
    # because sed refreshes the Makefile timestamp (this exact bug skipped ml).
    ext=$(find build/release -name '*.duckdb_extension' -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | cut -d' ' -f2-)
    if [ -z "$ext" ]; then
        echo "NO BINARY FOUND $name (build dir layout unknown?)"
        find build -name '*.duckdb_extension' 2>/dev/null | head -3
        continue
    fi
    mkdir -p "$DEST"
    cp -f "$ext" "$DEST/"
    echo "INSTALLED $name -> $DEST/$(basename "$ext")"
done
echo "done."
