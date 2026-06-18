# SwanFlow build targets
# Usage:
#   make release          — optimized single binary
#   make release-fast     — fast build for CI
#   make check            — cargo check
#   make test             — run tests

.PHONY: release release-fast check test clean

DUCKDB_ENV = DUCKDB_DOWNLOAD_LIB=1

release:
	$(DUCKDB_ENV) cargo build --release -p swanflow
	@echo "Binary: target/release/swanflow"
	@ls -lh target/release/swanflow

release-fast:
	$(DUCKDB_ENV) cargo build --profile release-fast -p swanflow
	@echo "Binary: target/release-fast/swanflow"

check:
	$(DUCKDB_ENV) cargo check --workspace

test:
	$(DUCKDB_ENV) cargo test --workspace

clean:
	cargo clean
