# ADR-001: Node Capability Model (Read-XOR-Write)

- **Status**: Accepted
- **Date**: 2026-06-16
- **Inspired by**: Nika (supernovae-st/nika) read-XOR-write capability model

## Context

SwanFlow's DAG engine runs arbitrary nodes that may have side effects:
database writes, API calls, file system modifications. Without a capability
model, the AI Agent (and any automated executor) can accidentally write to
external systems when it should only read.

Nika's breakthrough is the read-XOR-write model: every step explicitly
declares whether it reads, writes, or both. A read step cannot silently write.

## Decision

Add a `capability()` method to the `NodeBehavior` trait with three variants:

```rust
pub enum NodeCapability {
    ReadOnly,   // In-memory transform, zero side effects
    ReadWrite,  // May read/write external systems (default)
    WriteOnly,  // Sends data out (email, webhook, etc.)
}
```

### Scope

- Every node type gets a default of `ReadWrite`
- Pure transform nodes (Filter, Project, Aggregate, SQL) override to `ReadOnly`
- Sink nodes (DbWrite, HttpPost, Email) override to `WriteOnly`
- The AI Agent node respects capability gating at runtime

### Enforcement Path (Phase 2)

1. Workflow compiler checks: if a workflow runs in `read-only` mode,
   any node not marked `ReadOnly` causes a compile error
2. AI Agent's action space is filtered by capability: a read-only agent
   cannot call tools marked as writes
3. Workflow YAML gains optional `mode: read-only` declaration at the top level

## Consequences

- **Positive**: Accidental writes are caught at compile time, not runtime
- **Positive**: Workflow YAML becomes self-documenting about side effects
- **Positive**: Auditability — a `read-only` workflow is provably side-effect-free
- **Neutral**: Requires updating each node type's capability (mechanical work)
- **Negative**: Slight API surface increase on the NodeBehavior trait

## Related

- Nika read-XOR-write model: https://github.com/supernovae-st/nika
- SwanFlow issue: #capability-model
