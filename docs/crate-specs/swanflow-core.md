# SwanFlow Crate Specs

> 当前为单 crate (`swanflow`)。
> 随着项目增长，预计拆分为多个 crate。本文档记录 crate 边界和拆分计划。

## 当前结构 (v0.1)

```
swanflow/                     — 单 crate, cdylib+rlib
├── src/
│   ├── core/                 — 引擎核心
│   │   ├── engine/           — 🟢 <1.5K ✅
│   │   │   ├── mod.rs        — SwanFlowEngine + IntelligenceHook
│   │   │   ├── planner.rs    — SwanFlowPlanner + SwanFlowQueryPlanner
│   │   │   └── metadata.rs   — WorkflowMetadata
│   │   ├── datasource.rs     — 🔴 1,933 行 ⏳ 待拆分
│   │   ├── error.rs          — 🟢 SwanFlowError
│   │   ├── kernel/           — 🟢 Kernel traits (17 traits, 3 groups)
│   │   │   ├── mod.rs        — re-export 统一入口
│   │   │   ├── core.rs       — 8 traits: DbRead/DbWrite/FsRead/HttpFetch/KeyStore/Clock/BlobStore/ShellExec/ProcessManager
│   │   │   ├── ai.rs         — 3 traits: LLMInfer/Embedding/Tokenize
│   │   │   ├── runtime.rs    — 3 traits: CancellationToken/Timeout/Progress
│   │   │   └── mock.rs       — 所有 mock 实现 + 27 个测试
│   │   ├── graph.rs          — 🟢 Workflow DAG
│   │   ├── compiler.rs       — 🟢 Workflow compiler
│   │   ├── connection_config.rs — 🟢 DBX-compatible config
│   │   ├── observer.rs       — 🟢 StateProbe, Materializer
│   │   └── templating.rs     — 🟢 Template engine
│   ├── nodes/                — 节点实现
│   │   ├── io/               — 🟢 <1.5K ✅
│   │   ├── connectors/       — 🟢 7 files, all <500 lines
│   │   ├── ai/               — 🟡 AI agent nodes
│   │   └── ...               — 🟢 其他模块
│   ├── agent/                — 🟢 AI Agent + hook
│   ├── mcp/                  — 🟢 MCP server/client
│   ├── adapter/              — 🟢 Structured protocol
│   └── lib.rs                — 🟢 Python bindings
```

## 文件大小纪律

| 阈值 | 动作 |
|---|---|
| ≤1,000 行 | 正常监控 |
| 1,000-1,500 行 | 标记监控 |
| **>1,500 行** | **必须拆分到子目录** |

## 未来 crate 拆分建议

当 `swanflow-core` 超过 5,000 行或项目成熟到 v0.2+ 时：

1. **swanflow-core** — 引擎核心（engine, graph, compiler, observer）
2. **swanflow-connectors** — 数据库连接器（PG/MySQL/ClickHouse/DuckDB/Mongo/Redis/SQLServer）
3. **swanflow-nodes** — 节点实现（io, transform, wasm, ai, script）
4. **swanflow-agent** — AI Agent 循环 + 自愈 hook
5. **swanflow-adapter** — 结构化协议适配器

## 模块 LOC 预算

| 模块 | 当前 | 预算 | 状态 |
|---|---|---|---|
| `core/datasource.rs` | 1,933 | 1,500 | ⏳ 待拆分 |
| `core/observer.rs` | 402 | 1,500 | 🟢 |
| `nodes/transform.rs` | 918 | 1,500 | 🟡 监控中 |
| `agent/mod.rs` | 505 | 1,500 | 🟢 |
