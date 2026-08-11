# ADR-011: Schema-Driven Config Contract

**Status**: Accepted
**Date**: 2026-08-11
**Deciders**: SwanFlow maintainers

## Context

Config validation in SwanFlow was hand-written per node: each `NodeBehavior`
implementation provided its own `validate()`, while `NodeSpec.config_schema`
(declarative JSON Schema) existed for every node but was not consumed for
validation. This created a drift risk — the declared schema and the enforced
validation could disagree, and nothing outside the engine could trust
`config_schema` to describe actual behavior.

**Research** (task #2026081102) analyzed Forge (github.com/nihalashetty/Forge,
MIT, by Nihal A Shetty), a self-hosted AI workflow builder. Its defining
architecture is a **shared schema contract triangle**: one JSON Schema in
`packages/schemas/` drives (1) backend validation, (2) the compilation
engine, and (3) frontend `<SchemaForm>` generation. The frontend and backend
cannot drift because they consume the same artifact.

SwanFlow has no GUI, so Forge's third corner maps to the **adapter protocol**
(`Inspect` message): external editors render config forms from the same
schema the engine validates against. We already had the schema (corner of the
triangle) but neither of the other two.

## Decision

Adopt the schema-driven contract with three changes:

1. **`swanflow-core` gains `validation.rs`** — `SchemaValidator` compiles each
   node's `config_schema` (via the `jsonschema` crate, MIT) and validates
   instances. Errors are `CONFIG_ERROR`/Semantic and carry the JSON Pointer
   path for field-level highlighting. Unknown node types fail loudly (no
   silent pass).

2. **`NodeBehavior::validate` gets a schema-driven default** — both the core
   and main-crate trait definitions. Existing nodes keep their hand-written
   overrides (zero behavior change); new nodes get schema validation for
   free. Nodes whose semantics exceed JSON Schema (e.g. non-empty array)
   continue to override and document why.

3. **Adapter `Inspect` exposes `config_schema`** — `AdapterSchemaSummary`
   gains `config_schema: Option<JsonValue>` (`#[serde(default)]` for backward
   compatibility). The Inspect handler resolves each node's schema from the
   registry. This is the "form generation" corner for external editors.

## Consequences

**Positive:**
- `config_schema` becomes the single source of truth for config checks;
  declared schema and enforced validation cannot silently diverge
- External editors (and future GUI) can render config forms without
  duplicating schemas
- New nodes get validation by default; the `validate()` override remains
  for genuinely non-schema-expressible semantics
- Backward compatible: old adapter clients see no `config_schema` field
  (deserializes to `None`)

**Negative:**
- Adds `jsonschema` dependency tree to `swanflow-core` (~9 transitive deps:
  strum, uuid-simd, etc.)
- Two trait definitions (core + main crate) both carry the default; the
  long-standing duplication is noted but not refactored here (out of scope)
- Schema errors from jsonschema 0.49 name missing-required fields in the
  message but point the instance path at the whole object; type errors carry
  the pointer — error consumers must handle both shapes

**Evidence:**
- `cargo test -p swanflow-core`: 84/84 green (7 new validation tests +
  default-validate test)
- `cargo test -p swanflow --lib adapter`: 4/4 green (serialize,
  old-format deserialize, roundtrip ×2)
- `cargo check -p swanflow-core -p swanflow`: zero errors

## Alternatives Considered

- **Reuse `schemars` derive** (already a dead dependency in main crate):
  deferred — hand-written `config_schema` and schemars output are both
  standard JSON Schema, so `jsonschema` validation composes with either;
  migrating 69 nodes to derive is a separate change.
- **Replace all hand-written `validate()` with the default**: rejected —
  several nodes have semantics JSON Schema cannot express (e.g.
  `contracts.rs` requires non-empty arrays); forcing schema-only would
  silently loosen those checks.
