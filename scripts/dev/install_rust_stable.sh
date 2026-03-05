#!/usr/bin/env bash
set -euo pipefail

if ! command -v curl >/dev/null 2>&1; then
  echo "error: curl is required to install rustup" >&2
  exit 1
fi

if ! command -v rustup >/dev/null 2>&1; then
  curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable
fi

# shellcheck disable=SC1090
source "$HOME/.cargo/env"

rustup toolchain install stable
rustup default stable
rustup component add rustfmt clippy

rustc --version
cargo --version
rustfmt --version
cargo clippy -V
