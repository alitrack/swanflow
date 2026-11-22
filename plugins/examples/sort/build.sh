#!/usr/bin/env bash
# Build the example sort plugin into a loadable .wasm module.
#
#   ./build.sh                 -> plugins/examples/sort/sort.wasm
#
# Requires the wasm32-unknown-unknown target:
#   rustup target add wasm32-unknown-unknown
set -euo pipefail

cd "$(dirname "$0")"

if ! rustup target list --installed 2>/dev/null | grep -qx wasm32-unknown-unknown; then
  echo "missing target: rustup target add wasm32-unknown-unknown" >&2
  exit 1
fi

# Keep build artefacts off any /mnt/d mount (slow and sometimes wedged there).
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/swanflow-plugin-target}"

cargo build --release --target wasm32-unknown-unknown

built="$CARGO_TARGET_DIR/wasm32-unknown-unknown/release/swanflow_plugin_sort.wasm"
if [ ! -f "$built" ]; then
  echo "build produced no module at $built" >&2
  exit 1
fi

cp "$built" ./sort.wasm
echo "built ./sort.wasm ($(wc -c < ./sort.wasm) bytes)"
echo
echo "verify the host can drive it:"
echo "  cargo test -p swanflow --features wasm --lib plugin_wasm  # ABI + sandbox nails"
echo "  cargo test -p swanflow --features wasm --test plugin_sort_equivalence"
