#!/bin/bash
set -e

DUCKDB_VERSION="1.4.4" # Updated to 1.4.4 as requested
# Or if we want to stay with 1.4.4:
# DUCKDB_VERSION="1.4.4"

echo "🦆 Setting up DuckDB v$DUCKDB_VERSION..."

# Download shared library
URL="https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/libduckdb-linux-amd64.zip"
wget -q $URL -O libduckdb.zip
unzip -o libduckdb.zip
rm libduckdb.zip

# Move to standard paths (requires sudo or local path management)
# For this environment, we'll try to move to /usr/local/lib if possible, or keep local
mkdir -p ./libs
mv libduckdb.so ./libs/
mv duckdb.h duckdb.hpp ./libs/

echo "✅ DuckDB binaries downloaded to ./libs/"
echo "💡 To build with this library, run:"
echo "   export DUCKDB_LIB_DIR=\$(pwd)/libs"
echo "   export DUCKDB_INCLUDE_DIR=\$(pwd)/libs"
echo "   export LD_LIBRARY_PATH=\$(pwd)/libs:\$LD_LIBRARY_PATH"
