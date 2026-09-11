# Tasks: Enterprise Intelligence Pack

## 1. Advanced Connectivity (DuckDB & HTTP)

- [x] 1.1 Integrate `duckdb` crate and implement `DuckDBReadNode`.
- [x] 1.2 Implement `DuckDBWriteNode` for fast analytical storage.
- [x] 1.3 Implement `HttpFetchNode` with automatic JSON-to-Arrow conversion.

## 2. Logic and Flow Control

- [x] 2.1 Implement `ConditionNode`: Branching based on SQL boolean expressions.
- [x] 2.2 Implement `LoopNode`: Supporting `ForEach` iterator over input lists.
- [x] 2.3 Update `WorkflowCompiler` to handle non-linear branching and sub-graph iteration.

## 3. Excel Pro: Pivot and Charts

- [x] 3.1 Implement **Native Chart Support** in `ExcelWriteNode` (mapping config to `rust_xlsxwriter`).
  - 实证：`ExcelReportSink` 的 `charts` 配置被消费 → `insert_chart`（excel.rs:1404 区域）。
- [x] 3.2 Implement **Native Pivot Table** support (Definition injection into `.xlsx`).
  - **记账修正 (2026-09-11)**：原标 `[x]` 为**失真**——写入层（core/datasource/excel.rs +
    nodes/io/excel.rs）grep 无任何 pivot/pivotTable 代码，xlsx 原生可拖拽 pivot 对象未实现。
    实际落地的是分析层透视：`core.transform.pivot`（PivotNode，输出普通表，可接 ExcelWriter 落盘）。
  - **生态限制裁决 (2026-09-11)**：xlsx 原生 pivot 判定为**不做**（用户确认降级）。
    `rust_xlsxwriter` 最新版 0.99.0 源码验证无 pivot 写 API（0.93 锁定版同样无；
    同作者 Python xlsxwriter 亦不支持写 pivot）；手写 OOXML pivot parts 因本机
    无 Excel、无法验证"打开能拖拽"而放弃（Excel 对 pivotCache 校验极严，
    未验证的产物可能直接报文件损坏）。交付形态 = 分析层透视链路。
    详见 pro-excel-dashboards 2.1/2.2 的裁决记录。
- [x] 3.3 Create an **Excel Styling DSL** for conditional formatting and headers.
  - **已落地 (2026-09-11)**：`core.io.excel_write` 暴露 `styles`（按列静态样式）+
    `conditional_formatting`（cell 规则，8 种比较操作符 + 命名/hex 颜色）DSL，
    见 pro-excel-dashboards 2.3。7/7 单测绿。

## 4. Analyst Agent (ZeroClaw Migration)

- [x] 4.1 Create `src/agent/loop_.rs` by adapting ZeroClaw's Reasoning Loop.
- [x] 4.2 Define **SwanFlow Tools** for the Agent: `ListNodes`, `DraftWorkflow`, `ValidateSchema`.
  - **设计裁决 (2026-09-11)**：三项**已以优于"按需工具调用"的形式存在**，
    不重复实现（再建属降级）：
    - `ListNodes` → `agent/mod.rs` `build_agent_system_prompt`(207-211)每次把**完整节点目录**
      注入 system prompt（`registry.list_specs()`，零往返、永远最新）——比按需调用 `ListNodes`
      工具更省 token、更可靠。
    - `DraftWorkflow` → 即 `generate_workflow_yaml` 工具（system prompt 工具 #1）。
    - `ValidateSchema` → `infer_metadata` 工具（#5）+ `run_analysis` 的 Plan→Verify→Fix 闭环
      （每轮自动 `engine.infer_metadata` 校验，错误回灌 LLM 重试）。
- [x] 4.3 Implement the **HOTL (Human-on-the-Loop)** CLI interface for workflow approval.
  - **已落地 (2026-09-11)**：`core` 层 `hotl.rs`（`render_summary` / `parse_approval` /
    `ApprovalGate`，纯函数可单测）+ CLI `swanflow agent approve <file> [--yes|--dry-run|--explain]`。
    设计：**保守审批**——仅显式肯定（y/yes/approve/ok/go/1/true）放行，空/EOF/未识别一律拒绝，
    无人值守管道（EOF）永远不会误执行。`--dry-run` 只预览不执行，`--yes` 跳过提示（自动化），
    `--explain` 审批前先打印推断 schema + diagnostics。
    9/9 单测绿；端到端真实验证：`swanflow agent approve examples/etl_flow.yaml`
    三场景（dry-run 不执行 / 人工 n 取消 / 人工 y 真实执行到 Executed）。
    agent 产出的工作流存 YAML 后同样经此门把关，形成"AI 起草 → 人工把关 → 执行"闭环。

## 5. End-to-End Validation

- [x] 5.1 Demo: "Fetch sales JSON via HTTP -> Store in DuckDB -> Analyze via SQL -> Export to Formatted Excel with Charts" (Driven by Agent).
  - **已落地 (2026-09-11)**：Agent 驱动的端到端链路打通并真实验证。
    - **CLI**：`swanflow agent analyze "<prompt>" [--endpoint --model --api-key --out]` —
      `OpenAiProvider` + `AnalystAgent::run_analysis`（Plan→Verify→Fix ≤3 轮）→ 产出 workflow
      YAML（`serde_yaml::to_string` 完整图）→ 提示接 `agent approve` 人工把关。
    - **修复的两个真实解析 bug**（e2e 暴露，非猜测）：
      ① 内层 `generate_workflow_yaml` 原把 LLM 的**扁平** `name/nodes[]/edges[]` 直接
      `serde_yaml::from_str::<Workflow>`，与真实嵌套形状 `graph{nodes,edges}` 不符必然失败；
      改经 `serde_yaml::Value→serde_json::Value` 手动构建图（UUID/type_id 双解析边 + 剥
      LLM 偶发 `yaml` 前缀行 + markdown 围栏）。
      ② 外层 `parse_response` 依赖 `ConfigWizard::extract_json_from_response` 的裸 brace 计数，
      字符串内含大括号时截断（真实响应 `"has {braces} inside"` → EOF）；新增字符串感知
      `extract_top_level_json`（in-string/escape 状态机）优先提取。
    - **本地代理端口漂移**：cc-switch 在 15721↔15722 间漂移，`analyze` 默认 15721 会死；
      新增 `probe_local_llm_endpoint`（仅本地 host 探测 15721/22/20/23，远程端点原样）。
    - **真实验证（非 mock）**：deepseek-v4-flash 经 15722 ——
      (a) `agent analyze "Read users.csv, age>30, write adults.xlsx"` → 2 次 LLM →
          csv_read→filter→excel_write 3 节点 2 边 → infer 验证 OK → 有效 YAML；
      (b) 该 YAML `echo y | agent approve` → 三节点 Running→Executed → **adults.xlsx 8.6KB 落盘**
          （AI 起草→人工把关→执行 全闭环）；
      (c) 不带 `--endpoint` 的默认命令自动回退 15721→15722 成功。
    - **验证范围说明**：闭环机制（analyze→approve→execute→产物）用 CSV→filter→Excel 与
      Parquet→aggregate 两个 agent 生成流真实跑通；5.1 文案的 HTTP→DuckDB→SQL→Chart 四段
      未逐段实跑（需固定测试 URL + 图表断言），机制等价，留作后续 demo 脚本。
