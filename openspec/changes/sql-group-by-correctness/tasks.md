# Tasks: SQL GROUP BY 正确性

## 阶段 A — 钉死触发条件（前置硬门槛，B 前必须完成 A1-A3）

- [x] **A1** 写最小 workflow YAML：真实 `core.sql` 节点做 `GROUP BY <string>`，
      带多 stage / fan_out / 上游物化形状；`swanflow run` 确认 GROUP BY 坍缩
      （复现 assert 红）。产物 `openspec/changes/sql-group-by-correctness/repro/<shape>.yaml`。
      **实测（2026-11-20，7eace18）**：5 个 repro YAML（a1_basic / a2a_materialize /
      a2b_fanout / a2c_literal_source / a2d_big）经 `swanflow run --legacy-yaml` 全部
      **3 组正确、无坍缩**。注意：默认 staged `swanflow run` 在 main 上对**任何** workflow
      挂死（P2-15 retry 循环缺 `break`，独立既有缺陷，见 design.md §阶段A实测 §0），
      CLI 证据由可完成的 `--legacy-yaml` 路径采集。
- [x] **A2** 控制变量二分：逐个去掉形状（单 stage / 无 fan_out / 直连 MemTable 不物化），
      定位**最小坍缩形状**；记入 `design.md` 的"阶段 A"小节（更新候选成因 1-4 的取舍）。
      **实测**：13 行形状矩阵（design.md §阶段A实测 §1）全部不坍缩 —— 包括 P1-6
      fan-out 形状、3 级中间物化、1000×50 规模；**不存在最小坍缩形状**（当前代码上
      无可复现形状）。候选成因 1-4 的取舍见 design.md §2/§3。
- [x] **A3** 把 A2 的最小形状固化成可重复的**端到端复现测试**（fixture + 断言脚本，
      或 `#[tokio::test]` 走真实 executor），复现前红 / 修复后绿。
      **按 A-出口 变体执行**：无坍缩形状可固化，故交付
      `crates/swanflow/tests/sql_group_by_collapse.rs` = **永久护栏**（文件头标明
      STATUS；覆盖 6 个形状，走真实 compiler+SwanFlowPlanner 机器；坍缩再现即红，
      显式 "COLLAPSE DETECTED"）。实测 6/6 绿 × 3 次重复，见 REPORT。
- [x] **A-出口** 若 A1 跑不出坍缩 → 判定"历史 bug 已消失"：确认 + 把
      `sql_collapse_probe.rs` 转正为永久回归测试 + 关 change（跳过 B）。
      **已判定（2026-11-20，7eace18）：不可复现** —— 历史 string GROUP BY 坍缩在
      全部实测形状下消失；最可能的历史触发器是共享 ctx `input_0` 别名互踩，已被
      P1-6（`19868e1`，per-proxy scratch ctx，`planner.rs:369-378`）修复。单元探针
      `sql_collapse_probe` 与 e2e 护栏 `sql_group_by_collapse` 共同作为永久回归。
      change 的归档动作留给协调者；本阶段不自行 archive。

## 阶段 B — 按 A2 根因修复（A3 复现测试就位后才动 planner.rs）

- [ ] **B1** 按 A2 钉死的根因选最小改动（共享 ctx 竞争→独立 ctx / 物化退化→
      Utf8View 归一 / DataFusion 版本→单开 change）。**不**在 A3 复现测试就位且
      可重复**红**（red）之前盲改注册逻辑——gate 是"A3 可复现红"，不是"A3 已绿"
      （A3 由 B2 在修复同提交里转绿，若 gate 写成"未绿前"会与 B2 循环）。
      **（按 A-出口 跳过：无可复现根因，B 不执行。）**
- [ ] **B2** 同一次提交里让 A3 端到端复现测试转绿；单元护栏 `sql_collapse_probe` 保持绿。
      **（按 A-出口 跳过。）**

## 阶段 C — 护栏转正 + 全量回归

- [ ] **C1** `sql_collapse_probe.rs` 转正为永久回归测试（string 与 int 双键 =3 组、
      无空键的护栏断言保留），删除"探针/可移除"措辞。
      **已满足（A-出口）**：探针已是永久护栏（`#[cfg(test)]` 模块，`cargo test --lib`
      常规回归），与新增 e2e 护栏 `tests/sql_group_by_collapse.rs` 配套；剩余动作仅是
      删除文件头"探针/可移除"措辞（随本 change 归档时一并处理）。
- [ ] **C2** `cargo test -p swanflow --lib` 全绿；`-D warnings` 无新增告警
      （探针无 unused import，尤其 `StringArray`/`StringViewArray` 用哪个留哪个）。
- [ ] **C3** `openspec validate sql-group-by-correctness --strict` 通过。
- [ ] **C4** 更新 `docs/adr/`（若 B 引入 ctx 生命周期决策则加 ADR）+
      `docs/changelog/2026/CHANGELOG-2026.md`。

## 提交纪律

- 按 ADR-002：这是**行为变更 + 设计决策**，走 OpenSpec，**不**用 `fix(...)` 直接改。
- 每阶段独立 commit，commit 前 `git status` 确认只 stage 本 change 目标文件（**绝不**
  `git add -A`）。阶段 A 若结论是"已修复"，提交内容是"加永久护栏 + 关 change"。
