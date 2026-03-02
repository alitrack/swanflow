## Context

The current `SqlxScanExec` uses a simplified approach where every database value is fetched as a `String` and appended to a `StringBuilder`. While functional for basic data display, it loses the type information necessary for DataFusion's query engine to perform analytics (e.g., you cannot SUM a string column). SQLx's `AnyRow` provides runtime type information via `TypeInfo`, which we must use to build proper Arrow arrays.

## Goals / Non-Goals

**Goals:**
- Dynamically detect database column types at runtime using `sqlx::any`.
- Map common database types (Integer, Float, Boolean, Date/Time) to standard Arrow `DataType`s.
- Use specialized Arrow builders (`Int64Builder`, `Float64Builder`, etc.) for performance and correctness.
- Implement a robust fallback to `StringBuilder` for unknown or complex types.

**Non-Goals:**
- Supporting every esoteric database type (e.g., geometry, network types).
- Implementing full-blown ORM logic.
- Performance optimization beyond basic vectorized batch building.

## Decisions

### 1. Dynamic Builder Dispatch
We will use an Enum-based dispatcher or a trait-based approach to manage different Arrow builders. Since the schema is known at the start of the `execute` call, we will create a `Vec<Box<dyn ArrayBuilder>>` where each builder corresponds to a column.

### 2. Type Mapping Strategy
- **Integer types (TINYINT, INT, BIGINT)** → `DataType::Int64`.
- **Floating point (FLOAT, DOUBLE, DECIMAL)** → `DataType::Float64`.
- **Boolean** → `DataType::Boolean`.
- **DateTime/Timestamp** → `DataType::Timestamp(Nanosecond, None)`.
- **Everything else** → `DataType::Utf8`.

### 3. Error Handling
We will use `try_get` with type-specific fallback. If a value fails to be fetched as its inferred type, we will attempt to stringify it before failing the entire batch.

## Risks / Trade-offs

- **[Risk] SQLx Any Type Resolution** → Some databases (like MySQL) return different type info for the same column depending on the protocol version. **Mitigation**: Use broad type categories (e.g., checking if it's "numeric" rather than exact bit-width).
- **[Risk] Memory Overhead** → Managing many specialized builders. **Mitigation**: Since we batch every 1000 rows, memory usage remains bounded and predictable.
