# Spec: KNIME Mapper

## Purpose

将 `SwanNode`（parser 输出）映射为 SwanFlow 原生的 `NodeInstance`，正确翻译 KNIME 配置参数到 SwanFlow 期望的配置格式。未映射的节点标记为 `Unsupported` 并给出用户可理解的替代建议。

## Contract

### 输入

- `SwanNode`（来自 parser）
- `NodeRegistry`（SwanFlow 已注册节点类型）
- 可选的 `mapper_overrides: HashMap<String, String>`（用户自定义映射）

### 输出

```rust
Result<NodeInstance, MapDiagnostic>
```

其中 `MapDiagnostic` 包含：
- `knime_factory: String` — 原始 KNIME 工厂名
- `compat_level: CompatLevel` — 兼容级别
- `swanflow_type: String` — 目标 SwanFlow 节点类型
- `warnings: Vec<String>` — 参数转换中的警告
- `suggestion: Option<String>` — Unsupported 时的替代方案

### 映射表

每条记录：

```rust
pub struct FactoryMapping {
    pub knime_factory: &'static str,
    pub swanflow_type: &'static str,
    pub category: NodeCategory,
    pub compat: CompatLevel,
    pub translate: fn(&HashMap<String, String>) -> JsonValue,
    pub ports: fn() -> (Vec<PortDef>, Vec<PortDef>),
}

pub enum NodeCategory { IO, Transform, Analysis, Visualization, FlowControl, Script }

pub enum CompatLevel {
    Full,         // 1:1 原生映射
    Emulated,     // SQL/DataFusion 组合模拟
    Passthrough,  // 执行直通
    Unsupported,  // 明确不支持
    Degraded,     // 部分功能降级
}
```

### v1 必须覆盖的工厂（93 个，按类别）

**IO 类 (22):**
```
csv_read, csv_write, excel_read, excel_write, table_read, table_write,
arff_read, parquet_read, parquet_write, json_read, json_write,
file_list, url_to_file, zip, unzip, db_reader, db_writer,
db_query, db_connector, temp_dir, file_download, file_upload
```

**Transform 类 (35):**
```
row_filter, column_filter, column_rename, column_splitter,
column_combiner, column_mapper, missing_value, string_replace,
math_formula, rule_engine, row_splitter, row_sampling,
sorter, groupby, pivoting, unpivoting, concatenate, joiner,
cross_joiner, partition, rank, window_loop, one_to_many,
cell_splitter, constant_value, extract_column_header,
transpose, reference_row, java_snippet, python_script, r_snippet,
string_manipulation, category_to_number, number_to_category,
normalizer
```

**Analysis 类 (12):**
```
statistics, correlation_matrix, clustering, decision_tree,
linear_regression, k_means, pca, value_counter, histogram,
box_plot, line_plot, scatter_plot
```

**Visualization 类 (12):**
```
bar_chart, pie_chart, line_chart, scatter_chart, bubble_chart,
heatmap, sunburst, parallel_coordinates, histogram_chart,
box_plot_chart, density_plot, table_view
```

**Flow Control 类 (10):**
```
loop_start, loop_end, chunk_loop_start, counting_loop_start,
recursive_loop_start, try_catch, switch_case, merge_variables,
variable_create, table_to_variable
```

**Script 类 (2):**
```
python_script, r_snippet
```

### 配置翻译规范

每个 `translate` 函数：

1. 读取 KNIME settings（`HashMap<String, String>`）
2. 类型转换（KNIME 的 xboolean/xint/xdouble 在 XML 中存为字符串）
3. 生成 SwanFlow `NodeInstance.config`（`JsonValue`）
4. 报告不能转换的参数为 Warning

### 降级策略

当 KNIME 节点无法原生映射时：

| 场景 | 策略 |
|---|---|
| KNIME `Histogram` → SwanFlow 无原生对应 | Emulated: `Agg(COUNT, GROUP BY bin) + Bar Chart` |
| KNIME `Box Plot` → SwanFlow 无原生对应 | Emulated: `Agg(PERCENTILE_25,50,75) + pass to Shaper` |
| KNIME `Density Plot` → SwanFlow 无原生对应 | Emulated: `Agg(COUNT, bin width) + Line Chart` |
| KNIME `Rule Engine` → SwanFlow 无原生对应 | Emulated: `CASE WHEN ... CASE WHEN ...` SQL |
| KNIME `Java Snippet` | Unsupported — 建议用 Python 重写 |

## Details

### Mapper 注册

```rust
pub struct KnimeMapper {
    mappings: HashMap<String, FactoryMapping>,  // factory_id → mapping
    category_index: HashMap<NodeCategory, Vec<String>>,  // 按类别索引
}

impl KnimeMapper {
    pub fn new() -> Self {
        let mut mapper = Self { /* ... */ };
        mapper.register_all_builtin();  // 注册所有 93 个映射
        mapper
    }
    
    pub fn map(&self, node: &SwanNode) -> Result<NodeInstance, MapDiagnostic> {
        // 1. 精确匹配 factory_id
        // 2. 前缀匹配（处理 KNIME 版本差异，如 org.knime.core.v5.node...）
        // 3. 回退到 "unknown" → Unsupported
    }
}
```

### 端口映射

KNIME 端口到 SwanFlow 端口：

```
KNIME Port Type                 SwanFlow Port Schema
─────────────────────────────────────────────────────
org.knime.core.data.DataTable   → Arrow Table (columns + types)
org.knime.core.node.port.flowvariable.FlowVariablePortObject 
                                → FlowVarRef { name, type }
```

## Boundary Cases

- **未知 factory_id**: 返回 `CompatLevel::Unsupported` + 建议使用类似 SwanFlow 节点
- **KNIME 版本差异**: `org.knime.core.v5.node...` vs `org.knime.core.v4.node...` — 前缀匹配处理
- **参数缺失**: KNIME 节点配置中缺失某个 key → 使用 SwanFlow 节点默认值 + Warning
- **多输入端口**: KNIME 节点有多个 Table 输入端口（如 Joiner） → 按 port_index 映射
- **循环依赖检测**: Mapper 构建后调用 `petgraph::algo::is_cyclic_directed()` 检测
- **配置中的 KNIME 变量引用**: `${}$` 语法 → 替换为 `%%varname%%` 暂存，执行时由 WorkflowContext 解析
