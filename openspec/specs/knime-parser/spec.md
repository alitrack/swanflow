# Spec: KNIME Parser

## Purpose

将 KNIME workflow XML（`.knwf` 文件）解析为内部 `SwanWorkflow` 中间表示（IR），保留所有拓扑、配置和流变量信息。解析必须是容错的——遇到不可识别的节点不应崩溃，而是标记为 `Unsupported` 并继续。

## Contract

### 输入

- `.knwf` 文件路径（KNIME workflow XML 格式）
- KNIME 4.7+ / 5.x 格式兼容

### 输出

```rust
Result<SwanWorkflow, Vec<ParseDiagnostic>>
```

其中 `ParseDiagnostic` 包含：
- `severity: Error | Warning | Info`
- `message: String`
- `source_position: SourcePosition`（文件名、行号、列号）
- `knime_node_id: Option<i64>`（关联的 KNIME 节点 ID）

### 必须提取的元素

1. **Workflow 元数据**: name, knime_version
2. **节点**: 
   - `knime_id`（KNIME 内部 ID）
   - `factory_id`（完整限定工厂名，如 `org.knime.core.node.io.filewriter.csv.CSVWriterNodeFactory`）
   - `label`（用户可见名称）
   - `settings`（所有 `entry` → key=value 映射）
   - `in_ports` / `out_ports`（端口名 + 索引）
   - `flow_vars_in` / `flow_vars_out`（流变量声明）
3. **边**: source (node_id, port_index) → target (node_id, port_index)
4. **Flow Variables**: 工作流级别的变量定义

### 必须处理的 XML 结构

- `<config key="node">` → 节点容器
- `<entry key="..." type="..." value="...">` → 配置项（支持 xboolean/xint/xlong/xdouble/xstring 等类型）
- `<config key="node_variable">` → 流变量输入
- `<config key="variable_name">` → 流变量输出
- `<connection srcID="..." destID="..." srcPort="..." destPort="...">` → 边

### 错误处理规则

- **节点解析失败**: 创建 `UnsupportedNode` 占位，继续解析下游
- **边引用不存在节点**: 报 Warning，跳过该边
- **XML 格式错误**: 立即返回 `Error`，附带位置信息
- **未知工厂类型**: 不报错——标记为 `factory: "unknown"`，由 Mapper 层决定如何处理

## Details

### 实现技术

- `quick-xml` crate：流式/SAX 解析 KNIME XML（比 DOM 方式更高效）
- XML 层级结构：`<workflow> → <config key="nodes"> → <config key="node"> → <config key="...">`
- 每个 `entry` 的 `type` 属性决定了值类型（xboolean → bool, xint → i32, xstring → String）

### SwanWorkflow IR 定义

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwanWorkflow {
    pub name: String,
    pub knime_version: Option<String>,
    pub nodes: Vec<SwanNode>,
    pub edges: Vec<SwanEdge>,
    pub flow_variables: HashMap<String, JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwanNode {
    pub id: Uuid,
    pub knime_id: i64,
    pub factory_id: String,
    pub label: String,
    pub settings: HashMap<String, String>,
    pub in_ports: Vec<PortDef>,
    pub out_ports: Vec<PortDef>,
    pub flow_vars_in: Vec<FlowVarRef>,
    pub flow_vars_out: Vec<FlowVarRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortDef {
    pub name: String,
    pub index: u32,
    pub port_type: PortType,  // Table, FlowVariable, Image, etc.
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwanEdge {
    pub source_node: i64,
    pub source_port: u32,
    pub target_node: i64,
    pub target_port: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowVarRef {
    pub name: String,
    pub var_type: FlowVarType,  // String, Int, Double, Boolean
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PortType { Table, FlowVariable, Image, Model, Other(String) }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlowVarType { String, Int, Double, Boolean, Any }
```

### 解析生命周期

```rust
pub fn parse_knwf(path: &Path) -> Result<SwanWorkflow, Vec<ParseDiagnostic>>
```

1. 打开文件，创建 `quick_xml::Reader`
2. 遍历 XML 事件（`StartElement`, `EndElement`, `Text`）
3. 维护状态机：`Root → Nodes → Node → Config → Entry`
4. 收集所有节点后，解析 `<config key="connections">` 中的边
5. 最后收集工作流级别 `<entry key="variable_name">` → flow_variables

## Boundary Cases

- **空工作流**: 0 个节点，0 条边 → 返回 `SwanWorkflow` with `nodes: vec![]`，不报错
- **孤儿节点**（有节点无边连接）: 解析成功，由执行层决定如何处理（按拓扑序独立执行）
- **循环引用**（DAG 中有环）: Parser 不检测（这是 Mapper/Execution 层的职责），只忠实地解析
- **超大工作流**（1000+ 节点）: 使用流式解析，不应 OOM
- **XML 编码**: 支持 UTF-8，处理 XML 实体（`&amp;`, `&lt;`, `&gt;`）
- **KNIME 5.x 新 XML schema**: 如果遇到未识别的 XML 结构，跳过并报 Warning，不崩溃
