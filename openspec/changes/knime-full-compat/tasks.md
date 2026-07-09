# Tasks: KNIME Full Compatibility Pipeline

## Phase 1: Foundation — Crate + Parser

- [ ] 1.1 创建 `crates/swanflow-knime/` — Cargo.toml（仅依赖 swanflow-core + quick-xml + petgraph）
- [ ] 1.2 实现 `SwanWorkflow` IR 数据结构（SwanNode / SwanEdge / PortDef / FlowVarRef）
- [ ] 1.3 实现 `parse_knwf()` — KNIME XML → SwanWorkflow
  - [ ] 1.3a 流式 XML 解析（quick-xml）
  - [ ] 1.3b 节点提取（factory_id, settings, ports）
  - [ ] 1.3c 边提取（connections）
  - [ ] 1.3d 流变量提取（node_variable, variable_name）
  - [ ] 1.3e 容错解析（未知节点→Unsupported，损坏节点→skip+warning）
- [ ] 1.4 实现 `ParseDiagnostic` — 错误溯源（文件名:行号:列号 + 原始 KNIME 节点 ID）
- [ ] 1.5 Parser 单元测试 — 5 个真实 .knwf 文件（CSV→Filter、GroupBy、Loop、Viz、复杂管线）
- [ ] 1.6 `cargo check` 通过 + 所有 parser 测试通过

## Phase 2: Mapper — 93 个工厂映射

- [ ] 2.1 实现 `KnimeMapper` 结构体 + 映射注册表
- [ ] 2.2 实现 IO 类 22 个映射（CSV/Excel/DB/Parquet/JSON）
- [ ] 2.3 实现 Transform 类 35 个映射（Filter/Sort/Group/Join/Pivot/Window）
- [ ] 2.4 实现 Analysis 类 12 个映射（Stats/Correlation/PCA/K-Means）
- [ ] 2.5 实现 Visualization 类 12 个映射（Bar/Pie/Line/Scatter/Heatmap/Table）
- [ ] 2.6 实现 Flow Control 类 10 个映射（Loop/Switch/Variable）
- [ ] 2.7 实现 Script 类 2 个映射（Python/R）
- [ ] 2.8 映射测试 — 每个类别至少 3 个映射的配置翻译测试
- [ ] 2.9 降级策略测试 — Histogram→BARCHART, BoxPlot→LINECHART, Density→LINECHART
- [ ] 2.10 `cargo check` 通过

## Phase 3: Execution — Variable Flow + Loop

- [ ] 3.1 实现 `WorkflowContext` — 变量读/写/模板替换
- [ ] 3.2 实现 `resolve_flow_vars()` — %%varname%% → JsonValue
- [ ] 3.3 实现 `execute_loop()` — Generic/Chunk/Counting/Recursive 四种循环
- [ ] 3.4 实现拓扑排序 + 层级执行（`execution_levels()`）
- [ ] 3.5 实现终端节点物化（DuckDB `CREATE TABLE AS SELECT`）
- [ ] 3.6 实现 Auto 引擎选择（transform→DataFusion, SQL→DuckDB）
- [ ] 3.7 实现 `--print` 模式（前 5 行预览）
- [ ] 3.8 集成测试 — 完整 KNIME 工作流端到端执行
  - [ ] 3.8a CSV→Filter→GroupBy→Bar 管线
  - [ ] 3.8b Loop（Chunk 循环 3 次迭代）
  - [ ] 3.8c Flow Variables（变量创建→使用→输出）
  - [ ] 3.8d 多引擎混合（DataFusion + DuckDB 在同一管线）
- [ ] 3.9 `cargo check` 通过 + 所有集成测试通过

## Phase 4: Shaper Push

- [ ] 4.1 实现 `push_dashboards()` — DuckDB ATTACH SQLite → INSERT apps
- [ ] 4.2 实现 `build_shaper_sql()` — Viz 节点 → Shaper chart SQL（10 种图表类型）
- [ ] 4.3 实现图表类型回退（BoxPlot→BARCHART, Density→LINECHART）
- [ ] 4.4 实现 Shaper 数据库路径发现（默认 `~/.shaper/shaper_internal.sqlite`）
- [ ] 4.5 集成测试 — 执行管线 → push → 验证 apps 表
- [ ] 4.6 `cargo check` 通过

## Phase 5: CLI Integration

- [ ] 5.1 `swanflow knime parse <file>` — 解析并打印 SwanWorkflow JSON
- [ ] 5.2 `swanflow knime run <file> [--engine auto|datafusion|duckdb] [--print]` — 解析+执行
- [ ] 5.3 `swanflow knime shaper --push <file> [--shaper-db <path>]` — 解析+执行+推送
- [ ] 5.4 `swanflow knime list-mappings` — 列出所有工厂映射及兼容性
- [ ] 5.5 E2E 测试 — `swanflow knime run test.knwf --print --push-shaper`
- [ ] 5.6 `cargo check` 通过

## Phase 6: Documentation + Polish

- [ ] 6.1 README 更新 — KNIME 兼容性章节 + 示例
- [ ] 6.2 错误信息富化 — 所有 parse/map/exec 错误引用原始 KNIME 节点
- [ ] 6.3 覆盖率报告 — `swanflow knime list-mappings` 输出覆盖率 CSV
- [ ] 6.4 提交 + 推送
