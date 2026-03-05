#!/usr/bin/env bash
set -euo pipefail

TS="$(date +%Y%m%d-%H%M%S)"
OUT="artifacts/audit/${TS}"
mkdir -p "$OUT"

# shellcheck disable=SC1090
source "$HOME/.cargo/env"

cargo fmt --check >"$OUT/fmt.log" 2>&1 || true
cargo clippy --all-targets --all-features -- -D warnings >"$OUT/clippy.log" 2>&1 || true
cargo test --all-targets --all-features >"$OUT/test.log" 2>&1 || true
bash scripts/audit/static_risk_scan.sh >"$OUT/risk.log" 2>&1 || true

echo "$OUT"
