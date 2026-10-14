# Proposal: `swanflow docs` — 自包含 HTML 血缘门户

## 问题描述

SwanFlow 的 workflow（`core/graph.rs` 的 `Workflow` = petgraph `StableGraph<NodeInstance, EdgeInstance>`）目前只能通过 `swanflow inspect`（stdout 文本 schema）查看，**没有任何可视化的血缘视图**。用户想分享一个 pipeline 给同事 / 客户时，要么截图、要么口头解释，没有一个可点开、可离线、自包含的门户页面。

det（本次调研对象）有 `det.docs` 生成**自包含单文件 HTML**：Mermaid 血缘图（按 in/out degree 分 Sources / Steps / Destinations 三组 subgraph）+ 每节点 schema/settings 表 + 锚点导航。这正是 swanflow 缺的那块，也是本次"抄模式不依赖"要抄回的具体模式之一。

## 动机

1. **可分享性**：一个 pipeline 的完整血缘（节点 / 边 / 配置 / 拓扑序）应是可导出的产物，而非只能 stdout 看。
2. **零外部依赖**：单文件 HTML，双击即开、可离线、可 email 附件、可丢内网，无需起 server、无需 node。
3. **复用现有数据**：`load_workflow` + petgraph `toposort` / `node_weight` / `upstream_of` / `downstream_of` 全都现成，门户只是把已有结构渲染出来，**不新增执行逻辑、不动引擎**。

## 影响范围

- 新增 CLI 命令：`swanflow docs <file> [-o out.html]`（默认 `<workflow名>-docs.html`）。
- 受影响能力（新增）：`docs-html-portal`（workflow 血缘的自包含 HTML 导出）。
- 新增代码：`crates/swanflow/src/docs_portal.rs`（渲染模块）+ `main.rs` 的 `Commands::Docs` 分支。
- **不**修改 `graph.rs` / `compiler.rs` / executor（纯只读渲染）。

## 明确不做的事

- **不**做交互式编辑 / 拖拽（那是 GUI/Web IDE 的范畴，swanflow 没有也不该塞进 CLI）。
- **不**做实时刷新 / WebSocket（门户是**快照**，`run` 之后的静态血缘）。
- **不**把 HTML 模板硬编码成几百行的巨型 `format!` 宏（可维护性差）；用结构化的 render 函数分块拼。
- **不**引入前端构建链（无 `npm build` / wasm-pack）——纯 Rust `String` 拼接生成，CI 友好。

## 成功标准

1. `swanflow docs examples/5_1_analyze_export.yaml` 产出一个**单文件** `.html`，浏览器打开能看到：
   - 血缘图（节点按拓扑序连线，Sources/Steps/Destinations 分组）。
   - 每节点的 `type_id` / config（JSON）/ mode / error_mode / state。
   - 边列表（source → target，含 port）。
2. 产物自包含：单文件、无本地资源依赖（CDN 仅用于 Mermaid 渲染库，可离线降级为纯表格）。
3. `cargo test -p swanflow --lib docs_portal` 全绿（对 5_1 示例做节点/边/拓扑序断言）。
4. `cargo build` 通过，`-D warnings` 无新增告警。

## 相关能力

- 新增：`docs-html-portal`
- 关联（只读引用，不修改其 spec）：`standardized-graph-protocol`（workflow 图模型）、`query-engine`（`core.sql` 节点，门户里会渲染其 config）。
