## 1. Dependency Alignment

- [x] 1.1 Update `Cargo.toml` with `datafusion = "52.1.0"`, `arrow = "57.0.0"`, `pyo3 = "0.26.0"`, and `pyo3-arrow = "0.10"`.
- [x] 1.2 Run `cargo update` and verify that no duplicate major versions of `arrow-schema` or `arrow-array` exist in the lockfile.

## 2. Global Refactoring (Type Standardization)

- [x] 2.1 Replace all `use arrow::...` with `use datafusion::arrow::...` in `src/core/*.rs`.
- [x] 2.2 Replace all `use arrow::...` with `use datafusion::arrow::...` in `src/nodes/**/*.rs`.
- [x] 2.3 Update `src/core/compiler.rs` to handle `Expr::Literal` with `None` metadata.

## 3. Physical Plan Refactoring

- [x] 3.1 Update `ExecutionPlan::children` signature in `src/core/datasource.rs` to return `Vec<Arc<dyn ExecutionPlan>>`.
- [x] 3.2 Update `ExecutionPlan::children` signature in `src/core/observer.rs` to return `Vec<Arc<dyn ExecutionPlan>>`.
- [x] 3.3 Update `ExecutionPlan::children` signature in `src/nodes/script/python.rs`.

## 4. Verification and Fixes

- [x] 4.1 Fix any remaining compilation errors (e.g., `DFSchema::try_from` trait bounds).
- [x] 4.2 Verify that `cargo build` completes successfully.
- [x] 4.3 Run all tests with `cargo test` and ensure zero failures on DataFusion 52.1.0.
