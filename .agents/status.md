# status.md — SwanFlow 当前状态、未解问题与被否决的方案

> 这份文件回答三个问题，按顺序：**现在在哪**（哪些测过、哪些没测）、
> **卡在哪**（未解问题）、**哪些路已经试过且不该再试**（否决记录）。
>
> 写作规则（三条硬约束）：
> 1. 每个数字带测量日期和命令。没测的写 `UNMEASURED`，不许写估计值。
> 2. 只描述当前状态。历史演进进 CHANGELOG.md，这里不写"曾经"。
> 3. 任何一条待办都要能被下一个人独立复现，否则它不配写进来。
>
> 上游模式来源：pithings/unkernel 的 `.agents/status.md`（438 行、每条带数字与代价）。
> 本文件是同一模式的落地，第一版按现有仓内可查事实填写，`UNMEASURED` 是诚实标记不是缺口。

最后更新：2026-11-25

---

## 1. 现在在哪

### 1.1 已测量（可直接引用）

测量时间 2026-11-25，命令见每行括号。

| 指标 | 值 | 口径 |
|---|---|---|
| crate 数 | 6 | `ls crates/` |
| Rust 源文件 | 160 | `find crates -name '*.rs' \| wc -l` |
| Rust 代码行 | 81,329 | 同上，`cat \| wc -l`，含测试与注释 |
| `swanflow` crate 体量 | 121 文件 / 64,964 行 | 同上 |
| `swanflow-core` | 16 文件 / 4,982 行 | 同上 |
| `swanflow-knime` | 11 文件 / 8,413 行 | 同上 |
| 注册节点类型 | **81 个唯一名**（`core.*` 命名空间） | `grep -rhoE '"core\.[a-z0-9_.]+"' crates/swanflow/src/nodes/ \| sort -u \| wc -l` |
| 注册调用点 | 87 处 | `grep -cE 'reg(ister)?[_(]' crates/swanflow/src/nodes/registry.rs` |
| 集成测试文件 | 42 个 | `ls tests \| wc -l` |
| ADR | 15 篇（ADR-001 ~ ADR-015） | `ls docs/adr \| wc -l` |
| 缺陷逃逸台账条目 | 6 条（FL-1 ~ FL-6） | `grep -c '^### FL-' docs/findings-log.md` |
| 源码内 TODO/FIXME/HACK | **0** | `grep -rn "TODO\|FIXME\|HACK" --include=*.rs crates/` |

`TODO/FIXME = 0` 是刻意维持的：待办只允许存在两份文件里（本文件、findings-log），
不许散落在代码注释里。散落的注释没人扫，等于没记。

### 1.2 UNMEASURED（本轮没跑，别当已知）

| 指标 | 状态 | 拿到它的命令 |
|---|---|---|
| 单测通过数 | **UNMEASURED** | `DUCKDB_DOWNLOAD_LIB=1 cargo test --lib -- --test-threads=1` |
| 全量测试通过数 | **UNMEASURED** | `DUCKDB_DOWNLOAD_LIB=1 make test` |
| E2E 工作流矩阵通过率 | **UNMEASURED** | `ls examples/` 后逐条 `swanflow run` |
| release 二进制体积 | **UNMEASURED** | `make release` 后 `ls -lh target/release/swanflow` |
| 冷启动到首个节点执行 | **UNMEASURED** | 需先定计时口径（含不含 DuckDB 扩展加载） |
| layering 门当前状态 | **UNMEASURED** | `bash scripts/ci/check-layering.sh` |

写"UNMEASURED"不是偷懒。上一版口头流传过"290 基线"这类数字（见 findings-log FL-1），
它是某次运行的快照，没有命令和日期，现在没人能重跑出同一个数。**没有测量命令的
数字，下一个 agent 拿到就等于拿到谣言。**

---

## 2. 卡在哪

### 2.1 系统性失守层：unit 的节点契约面（来源 FL-1..FL-6 tally）

6 条逃逸缺陷里 4 条本该被单测拦住（FL-1 类型不匹配的聚合、FL-3 零行批次静默不物化、
FL-5 Excel 图表 schema、FL-6 luajit 安装失败返 NULL）。E2E 在替单测干活。

固定动作（下一个人优先做这个，不要新开战役）：
给每个注册节点的 `validate()` / `propose_plan()` 补契约测试，输入是
`(schema, 参数)` → 期望 `Ok | Err(具体错误) `，不测数值正确性（那是 E2E 的活）。

### 2.2 「注册节点 × planner lowering」缺 CI 派生矩阵（FL-2）

`core.ai.mcp_tool` 注册了但没有 lowering 分支，运行时才炸。规则应该是：
注册了 Extension logical node，`core/engine/planner.rs` 必须有对应分支。
**这条规则至今靠人记，没挂 CI。** 挂上去的形态：从 registry 导出节点名清单，
与 planner 的 match 分支取差集，非空即红。

### 2.3 其余未解项

- `swanflow-core`（4,982 行）与 `swanflow`（64,964 行）的边界在 ADR-008 里只约束
  了 import 方向，没约束"什么该下沉"。下沉判据未写，导致每次都是临场判断。
- `findings-log.md` 的 tally 每 10 条更新一次，现在 6 条，**还差 4 条才轮到下一次
  tally**。别为凑数把非缺陷记进去。

### 2.4 已修：脚本断言了被退役的契约（2026-11-25，8bf6823）

`scripts/smoke_provenance.py` 断言 `fingerprint_kind == "aggregate"`，而 `c24645e`
已把新写入的 header 迁到 `row_multiset`（`aggregate` 只保留为「老 header 缺字段」
时的 legacy 假定）。脚本没跟着改，于是在 main 上跑就是红的——**红的是脚本，不是代码**。
已改成断言本 build 实际写入的 kind，并保留原意（「不是逐行哈希」）。

教训（写在这里因为会再犯）：**契约迁移时，断言旧契约的脚本是第二个要改的地方**。
`cargo test` 全绿也发现不了，因为这类脚本不在 `cargo test` 里。

---

## 3. 被否决的方案（试过，别再来一次）

这一节是这份文件里最省时间的部分。每条都写清"为什么不行"，因为下一个人
最可能重新提出来的正是这些。

### 3.1 用闭包做共享 retry helper（ADR-013）

**否决原因（编译器，不是口味）**：Rust 的 `FnMut` 借用规则不允许闭包捕获
`&mut step_context` 之后返回一个活得比调用更久的 future。单节点路径与 fan-out
路径的 retry 循环因此保持两份完全相同的实现。

**如果将来有人想去重**：除非改成 trait object 或者状态机，否则同一个墙会再撞一次。
真要动，先写 PoC 证明 future 能活过调用，再谈重构。

### 3.2 fan-out 全宽错误策略覆盖（ADR-013）

**否决原因**：per-child policy 与既有契约一致，粒度更细。全宽覆盖会把
`error_mode: skip` 的容错能力一起改掉。

### 3.3 保持无条件容错（ADR-013）

**否决原因**：静默失败是最贵的一类 bug。FL-3 和 FL-6 都是同一个病：
失败不抛错，下游拿到 NULL 当正常值继续跑。

### 3.4 用 cargo-deny 做层级门（ADR-008）

**否决原因**：workspace 级白名单太粗，需要的是 per-module 规则。
现役方案是 `scripts/ci/check-layering.sh`（8.0 KB，只查 `use crate::`）。
**已知盲区**：拦不住通过 trait object 或函数签名间接引用（如
`fn foo(n: &dyn NodeBehavior)`），这是为 CI 简单性付的代价，写在这里以免被当成 bug 报。

### 3.5 Nika 的四脚本方案（ADR-008）

**否决原因**：对当前规模（单 crate、57 节点口径）过度设计。注意 ADR-008 写的是
57 节点，现在实测 81 个唯一节点名，**这份旧口径需要在新 ADR 里修正**，别继续引用。

---

## 4. 门禁与回归（照抄执行，不要发明）

```bash
export DUCKDB_DOWNLOAD_LIB=1

# 四道门（CI 同款，全阻塞）
cargo fmt --all -- --check
cargo clippy --lib -- -D warnings
bash scripts/ci/check-no-unwrap.sh      # unwrap/expect 卫生
bash scripts/ci/check-layering.sh       # 层级方向

# 回归
cargo test --lib -- --test-threads=1    # 单测，线程数固定为 1
make test                                # 全量
```

**不要做的事**：
- 不要 `#[allow(...)]` 绕过 clippy。门是红的就修源头。
- 不要在 debug 模式下跑性能对比。ADR 与 AGENTS.md 都要求 release 模式 A/B。
- 不要把测量结论写成没有命令的数字。写完数字顺手把命令抄在旁边。
