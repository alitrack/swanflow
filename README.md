# SwanFlow

> Rust-native Data Science DAG Engine · AI Agent · WASM UDF · DBX-Compatible Connections

SwanFlow is a high-performance data science engine built on **Apache DataFusion 52** and **Apache Arrow 57**. It combines a declarative DAG pipeline engine, an AI analyst agent with self-healing capability, WASM UDF for user-defined functions, and DBX-compatible database connections.

## Why SwanFlow?

| 能力 | SwanFlow | Pandas | KNIME | Airflow |
|---|---|---|---|---|
| **DAG + AI Agent** | ✅ 融合 | ❌ | ❌ | ❌ |
| **KNIME 兼容** | ✅ .knwf 导入执行 | ❌ | ✅ 原生 | ❌ |
| **WASM UDF** | ✅ 浏览器级沙箱 | ❌ | ❌ | ❌ |
| **MCP 双通道** | ✅ Server + Client | ❌ | ❌ | ❌ |
| **AI 预算路由** | ✅ ModelRouter 5维评分 | ❌ | ❌ | ❌ |
| **智能修复上下文** | ✅ Schema+拓扑感知 PromptBuilder | ❌ | ❌ | ❌ |
| **Shaper 仪表板** | ✅ DuckDB push | ❌ | ❌ | ❌ |
| **单二进制** | ✅ 编译即用 | ❌ | ❌ | ❌ |
| **零拷贝 Python** | ✅ Arrow FFI | ❌ | ❌ | ❌ |
| **Late-Binding SQL** | ✅ DataFusion | ❌ | ❌ | ❌ |

## 快速开始

### 编译

```bash
git clone <your-repo> && cd swanflow
cargo build --release
```

### 运行工作流

```bash
# 最简单示例
cargo run -- run examples/hello-world.yaml

# ETL 管道
cargo run -- run examples/postgres-etl.yaml

# AI Agent 分析
cargo run -- run examples/agent-analyst.yaml
```

### 数据库连接管理

SwanFlow 使用与 [DBX (Beekeeper Studio)](https://github.com/hezhijie0327/DBX) 兼容的连接配置格式——一条配置，两边通用。

```bash
# 从 DBX 配置目录导入
cargo run -- connect import-dbx

# 列出已注册连接
cargo run -- connect list

# 查看连接详情
cargo run -- connect show "My Connection"
```

连接配置存储在 `connections/` 目录下（也可与 DBX 共享 `~/.dbx/config/profiles/`）：

```yaml
# connections/postgres-prod.yaml
name: "Production PostgreSQL"
db_type: postgres
host: db-prod.example.com
port: 5432
user: analyst
password: "${DB_PASSWORD}"
database: analytics
ssl: true
read_only: true
```

### KNIME 工作流兼容

SwanFlow 可直接运行 KNIME 工作流（`.knwf`），120 个节点工厂映射，覆盖 IO/Transform/Analysis/Visualization/FlowControl 五大类。

```bash
# 导入并执行 KNIME 工作流
swanflow knime run sales_analysis.knwf

# 预览输出前 5 行
swanflow knime run pipeline.knwf --print

# 选择执行引擎
swanflow knime run pipeline.knwf --engine duckdb

# 执行后推送到 Shaper 仪表板
swanflow knime run pipeline.knwf --push-shaper

# 查看当前映射覆盖率
swanflow knime list-mappings

# 扫描 KNIME 安装目录，了解节点类型
swanflow knime scan ~/knime-workspace/
```

**支持能力：** CSV/Excel 读写 · 过滤/排序/分组/聚合 · JOIN/Pivot · SQL · 可视化（Bar/Pie/Line/Scatter/Heatmap 等 28 种 VIZ） · 循环（Loop/Chunk/Counting/Recursive） · 流变量 · Python 脚本

**覆盖率：** 120 个工厂映射，61 Full (51%)，41 Emulated (34%)，15 Unsupported (12%)，3 Degraded (3%)

### MCP 集成

SwanFlow 可嵌入任何支持 MCP 的平台（LangFlow · Claude Code · Dify · n8n）：

```bash
# 启动 MCP Server（stdio 模式）
cargo run -- mcp serve

# 连接到外部 MCP Server
cargo run -- mcp tools -- python /path/to/mcp-server.py
cargo run -- mcp call -- python /path/to/mcp-server.py tool_name '{"arg": 42}'
```

## 🔑 核心能力

### DAG 引擎 (Phase 1)
- 5 种执行模式：Sequential / Fan-Out / Collect / Conditional / Loop
- 错误处理：Fail / Skip / Retry(N)
- AI Agent 自愈回环（Plan → Execute → Verify → Fix）
- WASM UDF（wasmtime 28，隔离沙箱执行）
- 模板变量系统 (`${{ var_name }}`)

### MCP 双通道 (Phase 2)
- **MCP Server**: 将 SwanFlow 工作流作为工具暴露给 LLM
- **MCP Client**: 在工作流内部调用外部 MCP 工具
- **5 个内置工具**: RunWorkflow, InspectWorkflow, ListNodeTypes, BakeParquet, ExportExcel

### DBX 配置兼容 (Phase 3)
- 与 DBX 共享数据库连接配置
- 支持 49 种数据库类型的默认端口和 URL 模板
- 支持 SSH 隧道 / SSL / 超时配置
- `swanflow connect import-dbx` 一键导入

## 📊 支持的数据库

| 原生连接器 | 通过 SQLx Any | 其他 |
|---|---|---|
| PostgreSQL ✅ | MySQL | DuckDB |
| ClickHouse | SQLite | Elasticsearch |
| SQL Server 🗄️ | | MongoDB 🗄️ |
| | | Redis 🗄️ |

🗄️ = 代码已就位，tokio 版本兼容待解决

## 🏗 架构

```
┌─────────────────────────────────────────────────────────┐
│          CLI (swanflow run/inspect/knime/...)            │
├─────────────────────────────────────────────────────────┤
│  MCP Server  │  MCP Client  │  AI Agent  │  KNIME Importer │
├─────────────────────────────────────────────────────────┤
│  DAG Engine (petgraph + tokio)                          │
│  StepMode: Sequential / FanOut / Collect / Conditional / Loop
├─────────────────────────────────────────────────────────┤
│  DataFusion + DuckDB Query Engines                      │
├───────┬──────────┬────────────┬─────────────┬──────────┤
│ Arrow │Connectors│   WASM     │   Python    │  Shaper  │
│       │(DBX)     │   UDF      │   Bridge    │  Push    │
└───────┴──────────┴────────────┴─────────────┴──────────┘
```

## 技术栈

| 层 | 技术 |
|---|---|
| 查询引擎 | Apache DataFusion 52.1.0 |
| 内存格式 | Apache Arrow 57.0 |
| 数据库 | sqlx 0.8 / DuckDB 1.4 |
| WASM | wasmtime 28.0.0 |
| Python | PyO3 0.26 / pyo3-arrow 0.14 |
| 序列化 | serde / serde_yaml / serde_json |
| 图引擎 | petgraph 0.7 |
| CLI | clap 4.5 |
| 文档 | rust_xlsxwriter / calamine |

## 📁 项目结构

```
crates/
├── swanflow/          # CLI + 引擎主机
│   └── src/
│       ├── core/      # 引擎核心 (DataFusion + DuckDB)
│       ├── nodes/     # 原生节点 (IO/Transform/AI/Script)
│       ├── mcp/       # MCP Server + Client
│       └── agent/     # AI Agent 引擎
├── swanflow-core/     # 核心抽象 (Workflow, NodeBehavior trait)
├── swanflow-knime/    # KNIME 互操作 (.knwf 解析/映射/执行)
├── swanflow-ai/       # AI 能力 (Analyst Agent)
└── swanflow-plugin-sdk/ # 插件 SDK

examples/              # 示例工作流
connections/           # DBX 兼容连接配置
```
