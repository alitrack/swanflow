# AI Safety Gates

SwanFlow is an AI-orchestrated data workflow engine. The LLM proposes
workflows and can issue SQL / scripts / tool calls; **it must not be able to
perform destructive or exfiltrating side effects on its own**. This document
lists the structural (non-prompt) safety gates that enforce that, where they
live in code, and the known limits of each.

Design principle: **every gate is structural, not advisory.** A prompt, a
model output, or a tool-call argument cannot override these checks — they run
in the engine regardless of what the model asked for.

---

## 1. MCP `execute_sql` — SELECT-only guard

**What:** The MCP server's `execute_sql` tool rejects any statement containing
a write/DDL keyword. Only reads reach the engine from a raw SQL call;
mutations must go through `run_workflow` with an explicit data-sink node.

**Where:** `crates/swanflow/src/mcp/server.rs` → `McpServer::guard_sql`.
Forbidden tokens (matched at statement start or after space / newline / tab):
`INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, REPLACE, MERGE`.

**Tests:** `guard_sql_tests` (same file) — 11 cases: SELECT/WHERE/JOIN/CTE
pass; INSERT/DELETE/DROP/UPDATE (incl. leading whitespace, multi-statement,
newline-separated) are rejected.

**Known limit (conservative bias):** the guard is a keyword match on
whitespace boundaries, not a SQL parser. A `SELECT updated_at ...` is
over-blocked because `updated_at` contains `UPDATE` at a space boundary. This
is left as-is deliberately: for a SELECT-only surface, **over-blocking is the
safe failure mode** (a legit query is refused; a write never slips through).
A future switch to a real tokenizer is a conscious decision, documented in the
test `note_identifier_substring_is_overblocked_safe`.

---

## 2. Node capability model — least-privilege by declaration

**What:** Every node declares a `NodeCapability` (`ReadOnly` / `ReadWrite` /
`WriteOnly`). Read-only compute nodes (filter, aggregate, window, profiler,
pivot, …) cannot touch external systems; only nodes that explicitly opt into
`ReadWrite`/`WriteOnly` (DB connectors, writers, webhook/email) have side
effects. This bounds the blast radius of any single mis-specified node.

**Where:** `crates/swanflow-core/src/nodes.rs` → `enum NodeCapability`
(lines 70-74: `ReadWrite / ReadOnly / WriteOnly`) and the
`NodeBehavior::capability()` method. Layering discipline is enforced by CI
(`layering.yml`) so the main crate cannot quietly add cross-layer writes.

---

## 3. Python node — sandboxed by default

**What:** `core.script.python` runs in a **restricted-import sandbox by
default**. The `sandboxed` flag defaults to `true`; an opt-in
`sandboxed:false` script prefix (documented for KNIME-bridge scripts that
need sklearn/tensorflow/paramiko) disables it. In sandboxed mode the node
installs a `restricted_import` that blocks non-allowlisted imports.

**Where:** `crates/swanflow/src/nodes/script/python.rs` (sandbox parse at
~line 145, restricted-import shim at ~158).

**Companion:** `core.script.boxed_python` runs in a hardware-isolated
**BoxLite** sandbox with S3 data exchange (`nodes/script/boxed_python.rs`) —
the stronger isolation tier for untrusted code.

---

## 4. LuaJIT self-evolution — trusted sandbox + explicit approval

**What:** The agent's `create_lua_lib` tool writes new Lua libraries, but they
run in the duckdb-luajit extension's **trusted sandbox**. `io` / `os` / `ffi`
operations require **explicit approval** — a freshly generated lib cannot
arbitrarily touch the filesystem or shell.

**Where:** `crates/swanflow/src/nodes/luajit.rs` (approval note at ~line 238);
agent-side tool docs in `crates/swanflow/src/agent/mod.rs`.

---

## 5. PII / privacy gate node

**What:** `PiiDetectorNode` (`core.ai.pii_detector`) detects PII categories in
text so downstream masking can be applied before data leaves the pipeline; the
companion `core.ai.pii_masker` performs the masking/de-masking. This is the
data-exfiltration counterweight — it marks sensitive fields so writers can be
configured to mask them.

**Where:** `crates/swanflow/src/nodes/ai/privacy.rs` (`PiiDetectorNode`,
`PiiMatch`, `PiiDetectionResult`).

---

## 6. What is NOT yet gated (known gaps)

These are tracked in `openspec/changes/*/tasks.md`; listed here so the safety
surface is explicit rather than implied:

- **HOTL (human-on-the-loop) approval for generated workflows** — the engine
  executes what the agent proposes; a pre-execution human-approval gate for
  side-effecting workflows is not yet a first-class feature
  (intelligence-analytics-pack 4.3).
- **`x-semantic-validate` marker** — nodes whose hand-written `validate` is
  stricter than their JSON schema are not yet flagged, so schema-only checks
  can under-represent real constraints (schema-config-contract 2.4).
- **MCP `run_workflow` sink audit** — writes *are* allowed through
  `run_workflow` by design; there is no per-sink allowlist yet that a
  read-only deployment could tighten.

---

## Maintenance

- Add a new structural gate → add a section here **and** a `#[cfg(test)]`
  test that pins its contract (see `guard_sql_tests` as the template).
- If a gate is removed or weakened, update §6 and the corresponding
  `openspec/changes/*/tasks.md` in the same commit.
- Related ADR: `docs/adr/ADR-010-ai-orchestration-enhancement.md`.
