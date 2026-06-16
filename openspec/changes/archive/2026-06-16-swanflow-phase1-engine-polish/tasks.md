# Tasks: SwanFlow Phase 1 Engine Polish

总预估：9-13 周（子 spec 可并行）

---

## S1: Core Node Stability （2-3 周）

### T1.1 节点测试框架

- [ ] 1.1.1 实现单节点测试工具（mock input → assert output schema + data）
- [ ] 1.1.2 实现两节点链路测试（A→B，验证数据正确传递）
- [ ] 1.1.3 实现三节点 DAG 测试（分支/合并场景）

### T1.2 DatabaseSource / SQLNode

- [ ] 1.2.1 补齐 Late-Binding 引用：`SELECT * FROM node_uuid`
- [ ] 1.2.2 测试空表、超大列数、特殊字符列名的边界
- [ ] 1.2.3 SQLNode 加入参数化查询（`$var` 注入）

### T1.3 FilterNode / GroupByNode / JoinNode

- [ ] 1.3.1 FilterNode 加入复杂条件测试（AND/OR/NOT/in/NULL）
- [ ] 1.3.2 GroupByNode 加入 HAVING 支持
- [ ] 1.3.3 JoinNode 补齐 LEFT / FULL / ANTI / SEMI / CROSS
- [ ] 1.3.4 JoinNode 测试重复列名、空 join 键的场景

### T1.4 SortNode / FormulaNode

- [ ] 1.4.1 SortNode 多字段升降序混合测试
- [ ] 1.4.2 SortNode NULL FIRST/LAST 行为
- [ ] 1.4.3 FormulaNode 补齐（线性表达式、条件逻辑）
- [ ] 1.4.4 FormulaNode Python UDF fallback

### T1.5 MissingNode / PythonUDFNode

- [ ] 1.5.1 MissingNode 占位逻辑（报错 + 提示候选节点）
- [ ] 1.5.2 PythonUDFNode 错误传递（Python panic → Rust 优雅处理）

---

## S2: AI Analyst Agent 完善 （2-3 周）

> 对接已有的 `analyst-agent-v2` OpenSpec。见 openspec/changes/analyst-agent-v2/tasks.md

### T2.1 Macro Planner

- [ ] 2.1.1 实现 `generate_workflow_yaml` 工具
- [ ] 2.1.2 Agent 根据自然语言描述生成完整 YAML 工作流
- [ ] 2.1.3 生成的 YAML 通过 compiler `infer_metadata` 验证

### T2.2 Atomic Corrector

- [ ] 2.2.1 实现 `patch_node_config` / `add_node` / `connect` 工具
- [ ] 2.2.2 当 `infer_metadata` 发现错误，Agent 只 patch 出错的节点
- [ ] 2.2.3 测试：给 Agent 一个错误 schema，验证自愈循环

### T2.3 Statistical Profiling

- [ ] 2.3.1 实现 ProfilerNode（min/max/nulls/top-k/分布摘要）
- [ ] 2.3.2 更新 WorkflowMetadata 包含列级统计
- [ ] 2.3.3 Agent 在规划时能看到统计信息而非原始数据

### T2.4 IntelligenceHook 扩展

- [ ] 2.4.1 将 IntelligenceHook 扩展到所有核心节点
- [ ] 2.4.2 Hook 在节点执行失败时触发 Agent 自愈
- [ ] 2.4.3 测试端到端：Agent 自动修复一个出错的 5 节点 DAG

---

## S3: Python 包发布 （1 周）

### T3.1 Cargo.toml 改造

- [ ] 3.1.1 添加 cdylib crate-type
- [ ] 3.1.2 添加 pyo3 依赖并配置正确版本

### T3.2 Python API 实现

- [ ] 3.2.1 `swanflow.run("workflow.yaml")` 入口
- [ ] 3.2.2 Python DAG 构建 API（`swanflow.DAG()` + `add_source` + `add_sql` + `connect` + `run`）
- [ ] 3.2.3 `.to_pandas()` zero-copy 转换（via pyo3-arrow）
- [ ] 3.2.4 `.to_arrow()` 直接返回 PyArrow Table

### T3.3 发布流程

- [ ] 3.3.1 编写 setup.py / pyproject.toml
- [ ] 3.3.2 在 PyPI 上注册 `swanflow` 包名
- [ ] 3.3.3 CI 自动构建 wheel（Linux x86_64 + macOS ARM）
- [ ] 3.3.4 编写 Quickstart 文档

---

## S4: 连接器层集成 （2 周）

### T4.1 Connector Trait 定义

- [ ] 4.1.1 定义 `Connector` trait（connect/list_tables/get_schema/scan）
- [ ] 4.1.2 定义 `ConnConfig` 配置结构（DBX 兼容格式）

### T4.2 优先连接器实现

- [ ] 4.2.1 PostgreSQL（已有 SQLx，强化）
- [ ] 4.2.2 MySQL（已有 SQLx）
- [ ] 4.2.3 ClickHouse（HTTP 协议驱动）
- [ ] 4.2.4 SQL Server（TDS 协议）
- [ ] 4.2.5 MongoDB（Arrow Flight？或 datafusion-mongodb）

### T4.3 DBX 配置兼容

- [ ] 4.3.1 分析 DBX 连接配置格式
- [ ] 4.3.2 实现 DBX-to-ConnConfig 转换器
- [ ] 4.3.3 测试：DBX 中配好的连接，SwanFlow 直接复用

### T4.4 Late-Binding 验证

- [ ] 4.4.1 测试：`SELECT * FROM postgres_node` 在连接器层同样工作
- [ ] 4.4.2 测试：运行时切换数据源（Postgres ↔ MySQL）而 YAML 不变

---

## S5: 工作流模式扩展（2 周）

### T5.1 YAML Schema 扩展

- [ ] 5.1.1 在 task 定义中加入 `mode`、`error_mode`、`until`、`max_iterations`、`condition` 字段
- [ ] 5.1.2 编写扩展后的 YAML schema 文档
- [ ] 5.1.3 更新 compiler 解析新字段

### T5.2 fan_out / collect 模式

- [ ] 5.2.1 实现 fan_out 执行（tokio::join_all 并发）
- [ ] 5.2.2 实现 collect 模式（合并 fan-out 输出 → 统一输入）
- [ ] 5.2.3 测试：3 路并发 + collect 聚合

### T5.3 conditional 模式

- [ ] 5.3.1 实现 `when` 条件守卫（类似 Nika）
- [ ] 5.3.2 条件表达式支持：`==` / `contains` / `> <`
- [ ] 5.3.3 测试条件满足时执行、不满足时跳过

### T5.4 loop 模式

- [ ] 5.4.1 实现 loop step（重复执行直到 `until` 条件满足）
- [ ] 5.4.2 加入 `max_iterations` 安全限制
- [ ] 5.4.3 测试：loop 提前终止和达到上限

### T5.5 错误处理模式

- [ ] 5.5.1 实现 `error_mode: fail`（默认）
- [ ] 5.5.2 实现 `error_mode: skip`（静默跳过）
- [ ] 5.5.3 实现 `error_mode: retry`（重试 N 次）
- [ ] 5.5.4 集成 IntelligenceHook：retry 失败后触发 Agent 自愈

### T5.6 简单 trigger 引擎

- [ ] 5.6.1 实现事件总线（EventBus trait）
- [ ] 5.6.2 实现文件系统触发（文件变化 → 启动工作流）
- [ ] 5.6.3 实现定时触发（cron 表达式）
- [ ] 5.6.4 测试：文件写入后自动触发工作流执行

---

## V: 验证任务

- [ ] V.1 全链路测试：Postgres 读入 → 过滤 → 分组 → 聚合 → Excel 导出
- [ ] V.2 全链路测试：Agent 从自然语言"分析 Q3 销售数据，找异常值，导出仪表盘"完全自动化
- [ ] V.3 性能基准：SwanFlow vs dbt vs Pandas，标准 ETL 场景
- [ ] V.4 Python 包安装测试：`pip install swanflow && python -c "import swanflow; swanflow.run('tutorials/basic.yaml')"`
- [ ] V.5 模式测试：fan-out 3 路 + collect + conditional + loop 完整 DAG
