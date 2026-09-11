# Spec: KNIME Execution

## Purpose

将 parser + mapper 输出的 `SwanWorkflow` + `NodeInstance[]` 送入 `SwanFlowEngine` 执行，正确管理变量流（flow variables）和循环（loop）语义。

## Contract

### 输入

- `SwanWorkflow`（含 flow variables 声明）
- `Vec<NodeInstance>`（每个节点已映射为 SwanFlow 原生节点）
- `EngineMode`（DataFusion | DuckDB | Auto）
- `print_mode: bool`（是否在 stdout 打印前 5 行）

### 输出

```rust
pub struct KnimeExecutionResult {
    pub events: Vec<NodeEvent>,
    pub final_table: Option<String>,       // 最终节点物化表名
    pub preview: Option<Vec<RecordBatch>>, // print_mode 时返回前 5 行
    pub flow_vars: HashMap<String, JsonValue>, // 执行后所有变量
    pub dashboards: Vec<DashboardDef>,     // 可视化节点收集
}
```

### 执行语义

```
1. 拓扑排序 → Vec<ExecutionLevel>
2. 初始化 WorkflowContext { variables }
3. For each level:
   a. 解析节点 config 中的 %%varname%% → WorkflowContext 实际值
   b. 执行节点 → RecordBatch
   c. 提取输出 flow_vars → WorkflowContext
   d. Loop 节点：展开子图，迭代执行
   e. Viz 节点：不执行引擎，收集为 DashboardDef
4. 最终节点 RecordBatch → preview
```

### 变量流解析

```rust
pub fn resolve_flow_vars(
    config: &mut JsonValue,
    ctx: &WorkflowContext,
    flow_vars_in: &[String],
) -> Result<()> {
    // 替换所有 %%var_name%% 模板为实际值
    for var in flow_vars_in {
        let value = ctx.get(var)
            .ok_or_else(|| anyhow!("Flow variable '{}' not found", var))?;
        replace_placeholder(config, &format!("%%{}%%", var), value);
    }
    Ok(())
}
```

### 循环执行

```rust
pub enum LoopKind {
    Generic,      // Loop Start/End
    Chunk { size: usize },
    Counting { from: usize, to: usize },
    Recursive { max_depth: usize },
}

pub fn execute_loop(
    engine: &SwanFlowEngine,
    node: &NodeInstance,
    ctx: &mut WorkflowContext,
    kind: LoopKind,
) -> Result<Vec<RecordBatch>> {
    let sub_wf = node.sub_workflow
        .ok_or_else(|| anyhow!("Loop node missing sub_workflow"))?;
    
    let iterations = match kind {
        LoopKind::Chunk { size } => {
            let total = ctx.get("loop_total_rows")?.as_u64()?;
            (total as f64 / size as f64).ceil() as usize
        }
        LoopKind::Counting { from, to } => to - from + 1,
        LoopKind::Recursive { max_depth } => max_depth,
        LoopKind::Generic => node.max_iterations as usize,
    };
    
    let mut results = Vec::with_capacity(iterations);
    for i in 0..iterations {
        ctx.set("loop_iteration", json!(i));
        let batch = engine.execute_workflow_cached(sub_wf).await?;
        results.push(batch);
        
        if let Some(until_expr) = &node.until {
            if evaluate_until(until_expr, &results, ctx)? {
                break;
            }
        }
    }
    
    // Collect: append all iteration results
    Ok(results)
}
```

### 引擎选择（Auto 模式）

```rust
fn select_engine(node_type: &str) -> EngineBackend {
    match node_type {
        // SQL-heavy → DuckDB
        "sql" | "transform.joiner" | "transform.groupby" | "transform.window" 
            => EngineBackend::DuckDB,
        // Statistical → DataFusion 
        "analysis.statistics" | "analysis.correlation" | "analysis.pca"
            => EngineBackend::DataFusion,
        // Default
        _ => EngineBackend::DataFusion,
    }
}
```

### Print Mode

```rust
fn print_preview(batch: &RecordBatch, limit: usize) {
    let rows = min(batch.num_rows(), limit);
    // Pretty-print first `limit` rows as CSV-like table
    for col in batch.columns() {
        print!("{:>12} | ", col_name(col));
    }
    println!();
    for row in 0..rows {
        for col in batch.columns() {
            print!("{:>12} | ", format_value(col, row));
        }
        println!();
    }
}
```

## Details

### 拓扑排序 + 层级执行

使用 `petgraph::algo::toposort` 确定执行顺序，然后按层级并行执行（同一层内节点无依赖关系）：

```rust
fn execution_levels(workflow: &SwanWorkflow, node_map: &HashMap<i64, NodeIndex>) 
    -> Vec<Vec<NodeIndex>> 
{
    // Build petgraph from SwanWorkflow
    // toposort → 寻找最优并行执行层级
}
```

### WorkflowContext 生命周期

```
初始化: flow_variables = HashMap
   ↓
节点执行前: resolve_flow_vars(config, ctx)
   ↓
节点执行后: 
  - extract_output_vars(node, batch) → ctx.insert(name, value)
  - RecordBatch 物化到 DuckDB: ctx.register_table(node_id, batch)
   ↓
下游节点: engine.execute_node(config) 
  - config 中引用前序节点表名: "SELECT * FROM node_uuid"
```

### 终端节点物化

所有非可视化终端节点的输出物化到 DuckDB 临时表：
```rust
engine.duckdb_conn.execute(
    &format!("CREATE TABLE {}_output AS SELECT * FROM df", node.id)
)?;
```

## Boundary Cases

- **空结果集**: 节点输出 0 行 → 返回空 RecordBatch，下游节点可能报 "no data" 或正常处理
- **变量未找到**: `%%varname%%` 引用了未定义的变量 → 报错 `FlowVariableNotFound { name, node }`
- **无限循环**: `Chunk Loop` 数据量为 0 时 → 不执行迭代
- **嵌套循环**: Loop 中的 Loop → 每个层级的 ctx 是独立作用域，外层变量只读
- **可视化节点在管线中间**: Viz 节点接收数据输入并输出数据（KNIME 中 Bar Chart 有 out port） → Passthrough 模式，数据透传到下游
- **超大物化表**: 物化到 DuckDB，使用 `CREATE TABLE AS` 而非内存 `RecordBatch`
