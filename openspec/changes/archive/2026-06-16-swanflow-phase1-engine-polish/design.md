## 架构设计

### Phase 1 的总架构目标

将 SwanFlow 从"Rust 引擎原型"升级为"可发布的 YAML 工作流运行时"。核心变化：

```
Current:          Phase 1 Target:
┌─────────┐      ┌─────────────────┐
│ Engine  │      │ Engine + Modes  │ ← fan-out/loop/conditional
│ CLI     │      │ CLI (swanflow)  │ ← 独立二进制
│ Agent   │      │ Agent (闭环)     │ ← 自愈循环
│ Nodes   │      │ Nodes (稳定)     │ ← 边界测试
│ DuckDB  │      │ Connectors      │ ← 50+ 数据库
└─────────┘      │ Python Package  │  ← pip install
                 └─────────────────┘
```

### 子 Spec 设计

#### S1: core-node-stability

核心节点必须通过以下三个层级的测试：

| 层级 | 描述 | 验证方式 |
|------|------|----------|
| T1 | 单节点正确性 | 给定已知输入，输出符合预期 |
| T2 | 两节点连线 | A→B 链路数据正确传递 |
| T3 | 三节点 DAG | 分支/合并场景正确 |

需要补齐的节点：

| 节点 | 当前状态 | 需要完成 |
|------|----------|----------|
| DatabaseSource | 基本可用 | 加入 Late-Binding 引用：`SELECT * FROM node_uuid` |
| SQLNode | 基本可用 | 同上 |
| FilterNode | 部分可用 | 加入条件表达式的边界测试 |
| GroupByNode | 部分可用 | 加入聚合边界场景 |
| JoinNode | 部分可用 | 加入多种 Join 类型（LEFT/FULL/ANTI/SEMI） |
| SortNode | 基本可用 | 加入多字段排序测试 |
| FormulaNode | 实验性 | 补齐 |
| MissingNode | 待实现 | 占位/报错逻辑 |
| PythonUDFNode | 基本可用 | 加入错误传递测试 |
| ExcelSinkNode | 已有 | 维持 |

#### S2: ai-analyst-agent

融合 00-research-report §2.1（GraSP typed DAG + 局部修复）的设计：

```
Agent 循环:
┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
│ Plan     │ → │ Generate │ → │ Verify   │ → │ Execute  │
│ (Macro)  │   │ YAML     │   │ infer_   │   │ (if pass)│
│          │   │ Workflow │   │ metadata │   │          │
└──────────┘   └──────────┘   └──────────┘   └──────────┘
                                    │ fail
                                    ▼
                               ┌──────────┐
                               │ Fix      │
                               │ (Atomic) │
                               │ patch_   │
                               │ node     │
                               └──────────┘
```

**Macro Plan** → 生成完整 YAML 工作流
**Atomic Fix** → 发现错误后，只 patch 出错的节点/边，不重写整个图
**Integration**：与已有的 analyst-agent-v2 OpenSpec 对接，作为其完善版本

引入 **Statistical Profiling**（来自 00-research-report 的 KG 语义约束思想）：
- 在 `infer_metadata` 阶段计算列级统计
- Agent 凭统计信息做规划，而非凭原始数据

#### S3: python-package

```toml
[lib]
name = "swanflow"
crate-type = ["cdylib", "rlib"]  # Python 用 cdylib
```

Python API 设计：

```python
import swanflow

# 运行 YAML 工作流
result = swanflow.run("workflows/analysis.yaml")

# 或直接用 Python 构建 DAG
dag = swanflow.DAG()
src = dag.add_source("postgres://...")
query = dag.add_sql("SELECT * FROM src WHERE amount > 1000")
dag.connect(src, query)
dag.run()

# 与 Pandas 互通
df = result.to_pandas()  # zero-copy via Arrow
```

#### S4: connector-layer

借鉴 DBX 的驱动模式。不内嵌 DBX 二进制，而是**实现相同驱动接口**：

```
Connector Trait:
  connect(config: ConnConfig) -> Result<Box<dyn TableProvider>>
  list_tables() -> Result<Vec<String>>
  get_schema(table: &str) -> Result<SchemaRef>
  scan(table: &str, projection: &[usize]) -> Result<SendableRecordBatchStream>
```

Phase 1 优先集成（按用户量排序）：
- PostgreSQL, MySQL, SQLite (已有 SQLx)
- ClickHouse (HTTP 协议)
- MongoDB (Arrow Flight?)
- SQL Server (TDS)
- Oracle / TiDB / OceanBase (JDBC bridge 或原生驱动)

匹配 DBX 的连接配置格式，让用户可以在 DBX 中配好的连接在 SwanFlow 中复用。

#### S5: openfang-patterns

从 OpenFang 工作流引擎中提取设计模式，**不做直接代码复用**：

| OpenFang 模式 | SwanFlow 实现 |
|---|---|
| `mode: sequential` | 已有（默认行为） |
| `mode: fan_out` | 并发执行多个 step，join_all |
| `mode: collect` | 合并 fan-out 输出为统一输入 |
| `mode: conditional` | `when` 条件守卫 |
| `mode: loop` | 重复执行直到条件满足 |
| `error_mode: fail/skip/retry` | IntelligenceHook 配合 |
| Trigger 引擎 | 简单事件 → 工作流触发 |

YAML 语法扩展：

```yaml
tasks:
  - id: research
    agent: researcher
    mode: fan_out     # 新增
  - id: technical
    agent: architect
    mode: fan_out     # 同时并行
  - id: combine
    mode: collect     # 收集 fan-out 输出
  - id: review
    mode: loop
    until: APPROVED
    max_iterations: 3
    error_mode: retry
```
