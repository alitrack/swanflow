#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LIB_DIR="${ROOT_DIR}/libs"

if [[ ! -f "${LIB_DIR}/libduckdb.so" ]]; then
  (cd "${ROOT_DIR}" && bash setup_duckdb.sh)
fi

export DUCKDB_LIB_DIR="${LIB_DIR}"
export DUCKDB_INCLUDE_DIR="${LIB_DIR}"
export LD_LIBRARY_PATH="${LIB_DIR}:${LD_LIBRARY_PATH:-}"

echo "DUCKDB_LIB_DIR=${DUCKDB_LIB_DIR}"
