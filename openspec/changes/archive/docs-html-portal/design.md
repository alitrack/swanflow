# Design: `swanflow docs` HTML 血缘门户

## 数据流

```
load_workflow(file)
  → Workflow { name, graph: StableGraph<NodeInstance, EdgeInstance> }
    → toposort(&graph)            // 拓扑序（petgraph::algo::toposort）
    → 每节点 in-degree / out-degree
      → 分组：Sources(in=0) / Sinks(out=0) / Steps(其余)
    → 渲染：节点卡片 + 边表 + (可选) Mermaid
      → 单文件 HTML → 写文件
```

全部**只读**：不碰 executor / planner / registry，`docs` 命令不建 engine、不
`infer_metadata`（那需要跑节点，慢且可能失败）——门户是**结构快照**，不是执行产物。
（`inspect` 才做 schema 推断；`docs` 只做血缘可视化，二者互补。）

## 自包含决策（决定性）

det 的门户用 **CDN Mermaid**（`<script src="https://cdn.jsdelivr.net/npm/mermaid">`）。
swanflow 的卖点是"可 email 附件 / 丢内网 / 离线双击即开"，CDN 会毁掉"内网可用"。
故本 change 的默认形态**不依赖任何外部资源**：

| 形态 | 触发 | 渲染 | 离线 |
|------|------|------|------|
| **默认**（`swanflow docs <f>`） | 无 flag | 纯 HTML/CSS 节点卡片，按拓扑深度分列，CSS 箭头连线；边表 + 节点 config JSON 表 | ✅ 真自包含 |
| **增强**（`--mermaid`） | 显式 | 额外注入 `<pre class="mermaid">` 图定义 + CDN `<script>` | ⚠️ 联网渲染成图；离线时显示原始 mermaid 文本（优雅降级，不报错） |

默认形态用 CSS grid 做"层"（每列 = 一个拓扑深度，节点卡片纵向堆叠），层间用
`→` 伪元素 / 简单 SVG 连线。节点多时（>20）退化为"拓扑序列表 + 邻接表"，避免
CSS 布局爆炸——诚实标注"复杂图建议加 `--mermaid`"。

## HTML 结构（分块 render，不写巨型 format!）

```
render_html(wf, topo, groups) -> String
  ├── render_head()                 // <head>: 内联 <style>, <title>=workflow 名
  ├── render_header(wf)             // workflow 名 / id / 节点数 / 边数 / 生成时间
  ├── render_nav()                  // 锚点: 概览 | 血缘图 | 节点 | 边
  ├── render_overview()             // 统计卡片 (N nodes / M edges / depth)
  ├── render_lineage(groups, topo)  // 默认 CSS 分列 / --mermaid 则 <pre class=mermaid>
  ├── render_nodes(cards)           // 每节点: type_id / mode / error_mode / state / config(JSON 高亮)
  ├── render_edges(edges)           // 表: # | source(type_id) | target(type_id) | port
  └── render_footer()               // 生成器 + swanflow 版本
```

- **HTML 转义**：所有用户数据（`type_id` / `config` JSON / `name`）必须过 `escape_html`
  （`&` `<` `>` `"` `'`），config 是任意 JSON，不转义就是 XSS / 破版。
- **config 展示**：`serde_json::to_string_pretty` 后转义进 `<pre class="json">`。
- **节点稳定 ID**：用 `NodeInstance.id`(Uuid) 前 8 位做 `#node-<id8>` 锚点，避免
  petgraph 的 `NodeIndex`（内部序）做锚点（跨次生成可能不稳定）。

## Mermaid（仅 `--mermaid`）分组规则（抄 det）

- `subgraph Sources`：in-degree 0 的节点。
- `subgraph Steps`：其余。
- `subgraph Sinks`：out-degree 0 的节点。
- 边：`<src_id8> --> <dst_id8>`，多端口加 label `|p{source_port}→p{target_port}|`。
- 节点标签：`<id8>[<type_id>]`（`type_id` 里的 `.` 需转义或放节点 tooltip）。

## 备选与否决

| 方案 | 否决原因 |
|------|----------|
| 纯 CDN Mermaid（照抄 det） | 毁掉"内网 / 离线自包含"卖点；CDN 挂了门户就空 |
| 内嵌 wasm / 前端构建链 | 引入 npm/wasm-pack 构建，CI 复杂化，违背"纯 Rust 生成" |
| 起本地 HTTP server 提供门户 | "双击即开 / email 附件"做不到，违背自包含 |
| 在 `inspect` 里加 `--html` | `inspect` 要跑 `infer_metadata`（执行节点），失败就出不了门户；`docs` 独立、零执行 |

## 风险

- **CSS 布局对超大图（>20 节点）不优雅** → 退化为列表 + 邻接表 + 提示加 `--mermaid`，
  不硬撑。
- **petgraph `toposort` 对非连通图** → `toposort` 返回部分序 + 剩余节点；需处理
  "未入序"节点（孤立节点 / 环）。环在 `Workflow` 构造时已被 `is_cyclic_directed`
  拒绝（`graph.rs:199`），故正常无环；孤立节点单列。
- **config JSON 含敏感字段**（连接串 / 密码）→ 门户是本地文件，风险可控；但 design
  里**显式标注**：门户可能含 config 原文，分享前自查敏感字段（不自动脱敏，因为
  脱敏规则依赖节点类型，超出本 change 范围；列为后续增强）。
