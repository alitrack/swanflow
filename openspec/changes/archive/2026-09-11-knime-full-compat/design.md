# Design: KNIME Full Compatibility Pipeline

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                     swanflow-knime crate                      │
│                                                               │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐  │
│  │  Parser  │──▶│  Mapper  │──▶│ Execution│──▶│  Shaper  │  │
│  │  .knwf   │   │ factory→ │   │ engine   │   │  push    │  │
│  │  → IR    │   │ NodeInst │   │ run      │   │ SQLite   │  │
│  └──────────┘   └──────────┘   └──────────┘   └──────────┘  │
│        │              │               │               │       │
│        ▼              ▼               ▼               ▼       │
│  SwanWorkflow    NodeInstance[]  SwanFlowEngine   DuckDB     │
│  (internal IR)                   .execute()       ATTACH     │
└──────────────────────────────────────────────────────────────┘
         │                                             │
         ▼                                             ▼
  swanflow-core                               ~/.shaper/
  (Workflow, NodeSpec,                        shaper_internal
   EngineBackend)                             .sqlite
```

## Module Design

### 1. Parser (`parser.rs`)

```
KNIME XML (.knwf) → SwanWorkflow (internal IR)
```

**Internal IR (`SwanWorkflow`):**

```rust
pub struct SwanWorkflow {
    pub name: String,
    pub nodes: Vec<SwanNode>,
    pub edges: Vec<SwanEdge>,
    pub variables: HashMap<String, JsonValue>,  // flow variables
}

pub struct SwanNode {
    pub id: Uuid,
    pub knime_id: i64,            // 原始 KNIME node ID
    pub factory_id: String,       // org.knime.core... 原始工厂名
    pub label: String,
    pub settings: HashMap<String, String>,  // key=value 配置
    pub in_ports: Vec<PortDef>,
    pub out_ports: Vec<PortDef>,
    pub flow_vars_in: Vec<String>,   // 读哪些 flow var
    pub flow_vars_out: Vec<String>,  // 写哪些 flow var
}

pub struct SwanEdge {
    pub source: (i64, u32),   // (knime_node_id, port_index)
    pub target: (i64, u32),
}
```

**解析策略：**

1. `quick-xml` 流式解析 KNIME XML（避免 DOM 膨胀）
2. 提取 `<entry key="...">` → `settings` HashMap（KNIME XML 的配置都是扁平的 key=value）
3. 提取 `<port name="..." index="...">` → `PortDef`
4. 提取 `<connection srcID="..." destID="..." ...>` → `SwanEdge`
5. Flow variables 从 `<config key="node_variable">` 和 `<config key="variable_name">` 提取

**错误处理：**
- 每个 parse error 附带 `SourcePosition { file: String, line: usize, col: usize }`
- 使用 `miette::Diagnostic` trait 提供富文本错误报告
- 解析失败不阻塞其他节点——收集所有错误后统一报告

### 2. Mapper (`mapper.rs`)

```
SwanNode → NodeInstance (SwanFlow 原生节点)
```

**映射表结构：**

```rust
pub struct FactoryMapping {
    pub factory_pattern: &'static str,    // "org.knime.core.node.io.csv*"
    pub swanflow_type: &'static str,      // "io.csv_read"
    pub category: NodeCategory,
    pub compat: CompatLevel,
    pub config_translator: fn(&HashMap<String,String>) -> JsonValue,
}

pub enum NodeCategory {
    IO,
    Transform,
    Analysis,
    Visualization,
    FlowControl,
    Script,
}

pub enum CompatLevel {
    Full,        // 1:1 映射，所有参数转换
    Emulated,    // 用 SQL/DataFusion 组合模拟（如 Histogram → Agg+Bar）
    Passthrough, // 执行直通，Shaper 做渲染（仅 Viz）
    Unsupported, // 明确不支持，给出替代方案建议
}
```

**v1 目标：覆盖率**

| 类别 | KNIME 节点数 | 目标映射数 | 覆盖策略 |
|---|---|---|---|
| IO (CSV/Excel/DB/File) | 28 | 22 | 已有 SwanFlow IO 节点 |
| Transform (Filter/Sort/Group/Agg/Join/Pivot/Window) | 42 | 35 | DataFusion execution plan |
| Analysis (Stats/Mining/Distance) | 18 | 12 | DataFusion UDF + DuckDB |
| Visualization (Chart/Plot/Graph) | 15 | 12 | Passthrough → Shaper SQL |
| Flow Control (Loop/Switch/Try) | 12 | 10 | SwanFlow StepMode |
| Script (Python/R/Java) | 8 | 2 | BoxedPython, 其余标记 Unsupported |
| **总计** | **123** | **93 (76%)** | |

**配置翻译规范：**

```rust
// KNIME 配置 key → SwanFlow 配置 value
fn translate_csv_reader(settings: &HashMap<String, String>) -> JsonValue {
    json!({
        "file_path": settings.get("input_file").unwrap_or(&String::new()),
        "has_header": settings.get("has_column_header").map(|v| v == "true"),
        "delimiter": settings.get("column_separator").map(|v| match v.as_str() {
            "," | "\\t" | ";" | "|" => v.clone(),
            _ => ",".to_string(),
        }),
        "skip_rows": settings.get("skip_lines").and_then(|v| v.parse::<usize>().ok()),
    })
}
```

### 3. Execution (`exec.rs`)

```
SwanWorkflow + NodeInstance[] → SwanFlowEngine.execute_workflow()
```

**执行流程：**

```
1. 拓扑排序（petgraph::algo::toposort）
2. 创建 WorkflowContext { variables: HashMap }
3. For each node:
   a. 注入 flow variables → NodeInstance.config
   b. engine.execute_node(node) → RecordBatch
   c. 提取输出 flow variables → WorkflowContext
   d. 发射 NodeEvent::Completed
4. 最终节点 RecordBatch 作为结果返回
```

**变量流实现：**

```rust
pub struct WorkflowContext {
    variables: HashMap<String, JsonValue>,
}

impl WorkflowContext {
    pub fn resolve_vars(config: &mut JsonValue, vars: &HashMap<String, JsonValue>) {
        // 替换 config 中的 %%varname%% 占位符
    }
}
```

**循环执行：**

```rust
impl SwanFlowEngine {
    pub async fn execute_loop(
        &self,
        node: &NodeInstance,
        ctx: &mut WorkflowContext,
        max_iters: u32,
    ) -> Result<Vec<RecordBatch>> {
        let mut results = Vec::new();
        let mut iteration = 0;
        
        loop {
            if iteration >= max_iters { break; }
            let sub_wf = node.sub_workflow.as_ref()
                .ok_or_else(|| anyhow!("Loop node missing sub_workflow"))?;
            let batch = self.execute_workflow(sub_wf).await?;
            results.push(batch);
            
            // Check loop termination
            if let Some(until) = &node.until {
                if results.last().and_then(|r| {
                    String::from_utf8(r.column(0)?.to_string()).ok()
                }).map_or(false, |s| s.contains(until)) {
                    break;
                }
            }
            iteration += 1;
        }
        Ok(results)
    }
}
```

### 4. Shaper Push (`shaper.rs`)

```
Execution results → Shaper SQLite apps table
```

**实现方式：DuckDB `INSTALL sqlite; LOAD sqlite; ATTACH`**

```rust
pub fn push_to_shaper(db_path: &Path, dashboards: &[DashboardDef]) -> Result<()> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch("INSTALL sqlite; LOAD sqlite;")?;
    conn.execute_batch(&format!(
        "ATTACH '{}' AS shaper (TYPE SQLITE);", 
        db_path.display()
    ))?;
    
    for dash in dashboards {
        conn.execute(
            "INSERT INTO shaper.apps (id, name, content, type) VALUES (?, ?, ?, 'dashboard')",
            params![dash.id, dash.name, dash.sql],
        )?;
    }
    Ok(())
}
```

**支持的图表类型：**
BARCHART, LINECHART, PIECHART, SCATTERPLOT, BUBBLECHART, HEATMAP, SUNBURST, PARALLEL, TABLE, ECHART

**图表 SQL 生成：**

```rust
fn build_shaper_sql(node: &SwanNode, table_name: &str) -> String {
    match node.factory_id.as_str() {
        "org.knime.core.node.visualization.bar" => 
            format!("SELECT {}::XAXIS as x, {}::BARCHART as y FROM \"{}\"", 
                node.settings["x_column"], node.settings["y_column"], table_name),
        // ... 其他图表类型
        _ => format!("SELECT *::TABLE FROM \"{}\"", table_name),
    }
}
```

## Data Flow: End-to-End

```
$ swanflow knime run sales_analysis.knwf --push-shaper --print

1. Parser: .knwf XML → SwanWorkflow { 5 nodes, 4 edges, 2 flow_vars }
2. Mapper:  SwanWorkflow → 5 NodeInstance[] 
   - CSV Reader    → io.csv_read      [Full]
   - Row Filter    → transform.filter [Full] 
   - GroupBy       → transform.agg    [Full]
   - Bar Chart     → viz.bar          [Passthrough]
   - Loop Start/End→ flow.loop        [Full]
3. Execution:
   - EngineMode::Auto (DataFusion for transform, DuckDB for SQL)
   - Loop: 3 iterations, collecting results
   - stdout: ┌───────┬──────────┐
             │ region│ revenue  │
             ├───────┼──────────┤
             │ East  │ 12345.67 │ ...
4. Shaper Push:
   - DuckDB ATTACH shaper_internal.sqlite
   - INSERT INTO apps VALUES (uuid, "Sales Bar", "SELECT region::XAXIS...")
   - ✅ Dashboard ready
```

## Error Traceability

每个来自 KNIME 的错误都携带原始节点信息：

```
Error: Column 'sales_amount' not found
  ┌─ sales_analysis.knwf:142:29 (node ID 47: "Row Filter")
  │
142│   <entry key="include_column" value="sales_amount"/>
  │                               ^^^^^^^^^^^^^^^^^^^^^^
  └─ Original KNIME node: org.knime.core.node.transform.filter
     Mapped to SwanFlow node: transform.filter
```

## Test Strategy

| 层 | 测试方式 |
|---|---|
| Parser | 单元测试：已知 `.knwf` XML snippets → 断言 `SwanWorkflow` 结构 |
| Mapper | 单元测试：每个 factory mapping → 断言 `NodeInstance.config` |
| Execution | 集成测试：真实 `.knwf` 文件 → `execute_workflow` → 断言 RecordBatch |
| Shaper | 集成测试：执行结果 → 断言 SQLite `apps` 表记录 |
| E2E | CLI 测试：`swanflow knime run --print --push-shaper` 全链路 |
