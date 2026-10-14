# Tasks: SQL GROUP BY 正确性

## 阶段 A — 钉死触发条件（前置硬门槛，B 前必须完成 A1-A3）

- [ ] **A1** 写最小 workflow YAML：真实 `core.sql` 节点做 `GROUP BY <string>`，
      带多 stage / fan_out / 上游物化形状；`swanflow run` 确认 GROUP BY 坍缩
      （复现 assert 红）。产物 `openspec/changes/sql-group-by-correctness/repro/<shape>.yaml`。
- [ ] **A2** 控制变量二分：逐个去掉形状（单 stage / 无 fan_out / 直连 MemTable 不物化），
      定位**最小坍缩形状**；记入 `design.md` 的"阶段 A"小节（更新候选成因 1-4 的取舍）。
- [ ] **A3** 把 A2 的最小形状固化成可重复的**端到端复现测试**（fixture + 断言脚本，
      或 `#[tokio::test]` 走真实 executor），复现前红 / 修复后绿。
- [ ] **A-出口** 若 A1 跑不出坍缩 → 判定"历史 bug 已消失"：确认 + 把
      `sql_collapse_probe.rs` 转正为永久回归测试 + 关 change（跳过 B）。

## 阶段 B — 按 A2 根因修复（A3 复现测试就位后才动 planner.rs）

- [ ] **B1** 按 A2 钉死的根因选最小改动（共享 ctx 竞争→独立 ctx / 物化退化→
      Utf8View 归一 / DataFusion 版本→单开 change）。**不**在 A3 未绿前盲改注册逻辑。
- [ ] **B2** 同一次提交里让 A3 端到端复现测试转绿；单元护栏 `sql_collapse_probe` 保持绿。

## 阶段 C — 护栏转正 + 全量回归

- [ ] **C1** `sql_collapse_probe.rs` 转正为永久回归测试（string 与 int 双键 =3 组、
      无空键的护栏断言保留），删除"探针/可移除"措辞。
- [ ] **C2** `cargo test -p swanflow --lib` 全绿；`-D warnings` 无新增告警
      （探针无 unused import，尤其 `StringArray`/`StringViewArray` 用哪个留哪个）。
- [ ] **C3** `openspec validate sql-group-by-correctness --strict` 通过。
- [ ] **C4** 更新 `docs/adr/`（若 B 引入 ctx 生命周期决策则加 ADR）+
      `docs/changelog/2026/CHANGELOG-2026.md`。

## 提交纪律

- 按 ADR-002：这是**行为变更 + 设计决策**，走 OpenSpec，**不**用 `fix(...)` 直接改。
- 每阶段独立 commit，commit 前 `git status` 确认只 stage 本 change 目标文件（**绝不**
  `git add -A`）。阶段 A 若结论是"已修复"，提交内容是"加永久护栏 + 关 change"。
