# Tasks: KNIME Full Compatibility Pipeline

## Phase 1: Foundation — Crate + Parser

- [x] 1.1 创建 `crates/swanflow-knime/` — Cargo.toml（依赖 swanflow-core + quick-xml + zip）
- [x] 1.2 实现 KnwfArchive IR 数据结构（KnimeNode / KnimeEdge / metanodes）
- [x] 1.3 实现 `KnwfParser` — ZIP-based KNIME XML → KnwfArchive
  - [x] 1.3a ZIP 流式解析（zip crate）
  - [x] 1.3b 节点提取（factory_id, settings.xml → JSON, ports）
  - [x] 1.3c 边提取（connections）
  - [x] 1.3d 流变量提取
  - [x] 1.3e 容错解析（未知节点→Unsupported，损坏节点→skip+warning）
  - [x] 1.3f metanode 递归解析（sub-workflow detection + recursive zip parsing）
- [x] 1.4 实现 KnimeError — 错误溯源
- [x] 1.5 Parser 集成测试（walmart.knwf）
- [x] 1.6 `cargo check` 通过

## Phase 2: Mapper — 120 个工厂映射

- [x] 2.1 实现 `KnimeNodeMapper` 结构体 + macro-based 映射注册表
- [x] 2.2 实现 IO 类 12 个映射（CSV/Excel/DB/Table/Parquet）
- [x] 2.3 实现 Transform 类 41 个映射（Filter/Sort/Group/Join/Pivot/Window/Rank/Normalize/Category）
- [x] 2.4 实现 Analysis 类 14 个映射（ValueCounter, Correlation, KMeans, DecisionTree, LinearReg, PCA, Normalizer, DistanceMatrix, HierarchicalCluster, AssociationRules）
- [x] 2.5 实现 Visualization 类 28 个映射（Bar/Pie/Line/Scatter/Histogram/BoxPlot/Heatmap/Bubble/Sunburst/Parallel/Density/Radar/TagCloud/ECharts）
- [x] 2.6 实现 Flow Control 类 12 个映射（LoopStart/End/Chunk/Counting/Recursive + Variable/Merge）
- [x] 2.7 实现 Script 类 2 个映射（Python/JEP）
- [x] 2.8 Row/Window/Rank 类 8 个映射（RowSplitter, RowSampling, CellReplacer, Rank, Windowing）
- [x] 2.9 `list_all()` + `categorize()` + `compat_level()` 覆盖率报告
- [x] 2.10 `cargo check` 通过

## Phase 3: Execution — Variable Flow + Loop

- [x] 3.1 WorkflowContext 变量注入（已有 step_context + TemplateEngine）
- [x] 3.2 流变量模板替换（`$${Svar}$$` → 引擎 TemplateEngine 处理）
- [x] 3.3 Loop 执行 — metanode 子图递归 + 引擎 StepMode::Loop 正确执行
- [x] 3.4 拓扑排序执行（petgraph::algo::toposort）
- [x] 3.5 终端节点物化（DuckDB `CREATE TABLE AS SELECT` + parquet）
- [x] 3.6 Auto 引擎选择（已有 DataFusion/DuckDB/Auto）
- [x] 3.7 `--print` 模式（前 5 行预览通过 DuckDB read parquet）
- [x] 3.8 集成测试 — CSV→Filter→GroupBy→Bar 管线执行成功
- [x] 3.9 `cargo check` + 编译通过

## Phase 4: Shaper Push

- [x] 4.1 `push_dashboards()` — DuckDB ATTACH SQLite → INSERT apps
- [x] 4.2 `build_shaper_sql()` — Viz 节点 → Shaper chart SQL（10 种图表类型）
- [x] 4.3 图表类型回退（BoxPlot→BARCHART, Density→LINECHART）
- [x] 4.4 Shaper 数据库路径发现（默认 `~/.shaper/shaper_internal.sqlite`）
- [x] 4.5 集成测试 — push → 验证 apps 表记录
- [x] 4.6 `cargo check` 通过

## Phase 5: CLI Integration

- [x] 5.1 `swanflow knime run <file> [--engine auto|datafusion|duckdb] [--print] [--push-shaper]` — 解析+执行
- [x] 5.2 `swanflow knime shaper <file> [--push]` — 推送到 Shaper
- [x] 5.3 `swanflow knime scan <files>` — 扫描工作流节点类型
- [x] 5.4 `swanflow knime list-mappings` — 列出所有工厂映射 CSV
- [x] 5.5 E2E 测试 — `swanflow knime run --print --push-shaper`
- [x] 5.6 `cargo check` 通过

## Phase 6: Documentation + Polish

- [x] 6.1 README 更新 — KNIME 兼容性章节 + 架构图
- [x] 6.2 错误信息 — KnimeError + ParseDiagnostic + tracing::warn
- [x] 6.3 覆盖率报告 — `swanflow knime list-mappings` CSV 输出
- [x] 6.4 提交 + 推送
