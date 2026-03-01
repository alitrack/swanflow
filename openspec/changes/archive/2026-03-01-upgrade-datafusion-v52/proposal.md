## Why

Upgrading to DataFusion 52.1.0 is essential to leverage the latest performance optimizations, bug fixes, and new features of the Apache DataFusion ecosystem. This ensures `rustflow` remains compatible with the cutting edge of the Rust data ecosystem and provides a modern foundation for future analytical features.

## What Changes

- **Dependency Upgrade**: Bump `datafusion` to 52.1.0 and align `arrow`, `parquet`, and `pyo3` versions to resolve conflicts.
- **API Refactoring (BREAKING)**: Update all `ExecutionPlan` implementations to return `Vec<Arc<dyn ExecutionPlan>>` from the `children()` method.
- **Literal Value Refactoring**: Update `Expr::Literal` and `ScalarValue` usage to support the new `FieldMetadata` requirement.
- **Namespace Standardization**: Transition all internal `arrow` imports to use `datafusion::arrow` to ensure type consistency.

## Capabilities

### New Capabilities
- `modern-datafusion-engine`: Support for DataFusion 52.1.0 features and optimized physical planning.

### Modified Capabilities
- `swanflow-system`: Requirements for physical plan construction and logical-to-physical translation are changing to match DataFusion 52.x standards.

## Impact

- **Affected Code**: `src/core/datasource.rs`, `src/core/observer.rs`, `src/core/compiler.rs`, `src/core/engine.rs`, `src/nodes/io.rs`, `src/nodes/script/python.rs`.
- **Dependencies**: `Cargo.toml` updates for `datafusion`, `arrow`, `parquet`, `pyo3`, and `pyo3-arrow`.
