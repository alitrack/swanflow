# Tasks: `swanflow docs` HTML 血缘门户

## 实现

- [ ] **1** 新增 `crates/swanflow/src/docs_portal.rs`：
  - `escape_html(s) -> String`（`& < > " '` 全转义）。
  - `build_doc(workflow: &Workflow) -> DocModel`（toposort + 分组 + 节点/边收集）。
  - `render_html(doc: &DocModel, mermaid: bool) -> String`（分块 render，见 design）。
  - `write_doc(path, html)`。
- [ ] **2** `lib.rs` 加 `pub mod docs_portal;`。
- [ ] **3** `main.rs`：`Commands::Docs { file, output, mermaid }` + 分支
  （`load_workflow` → `build_doc` → `render_html` → 写文件 → 打印路径）。
- [ ] **4** 单测 `docs_portal::tests`：对 `examples/5_1_analyze_export.yaml`
  （或内联 fixture）断言节点数 / 边数 / 拓扑序 / 分组 / 转义正确。

## 验证

- [ ] **5** `cargo build -p swanflow` 通过；`-D warnings` 无新增告警。
- [ ] **6** 实跑 `swanflow docs examples/5_1_analyze_export.yaml`，产物
  浏览器打开：血缘图 + 节点 config + 边表齐全。
- [ ] **7** `swanflow docs <f> --mermaid` 产物含 `<pre class="mermaid">` + CDN
  script；默认产物**不含**任何 `http://`/`https://`（真自包含）。
- [ ] **8** `cargo test -p swanflow --lib` 全绿。
- [ ] **9** `openspec validate docs-html-portal --strict` 通过。

## 提交纪律

- 按 ADR-002：这是 **feature**（新命令 + 新能力），走 OpenSpec，**不**用 `fix(...)`。
- commit 前 `git status` 确认只 stage 本 change 目标文件（**绝不** `git add -A`）。
- 敏感字段：config 原文进 HTML（本地文件，风险可控），design 已标注分享前自查。
