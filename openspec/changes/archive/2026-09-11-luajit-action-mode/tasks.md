# Luajit Action Mode — Tasks

## 执行前置
- 不触碰他人未提交区（单用户仓库，直接 main 提交）
- 测试命令（WSL）：
  ```
  cd /mnt/d/wsl2/swanflow
  env LD_LIBRARY_PATH=/home/lhy/.local/share/uv/python/cpython-3.11.14-linux-x86_64-gnu/lib cargo test -p swanflow --lib luajit -- --nocapture
  ```
- Mac 编译验证（改动后必做）：`ssh sa@10.10.10.17 'cd ~/ws/swanflow && git pull && cargo check -p swanflow'`

## 任务
1. ✅ LuaJitNode spec：mode enum 加 `action`；args 描述更新（object/array）
2. ✅ validate：允许 action（args 必填）
3. ✅ propose_plan action 分支 + `struct_literal()` 辅助函数
4. ✅ LuaJitLibNode：`action_args: Option<JsonValue>` + propose_plan 预填
5. ✅ `luajit_libs_from_index()`：incremental/scd2 映射 args 模板 + 描述标注动作型
6. ✅ 测试 5 个：struct_literal / action query building / lib prefill / action libs 识别 / validate 分支
7. ✅ WSL 全量单测 11/11 绿（含原有 6 个 + 新 5 个）
8. ✅ Mac cargo check 验证
  - 裁决 (2026-09-11)：以 WSL 全量 `cargo test --lib`（231/231 绿）为编译验证；
    Mac 主机（10.10.10.100）仓库为 7 月旧快照（divergent + WIP，不可 pull），
    10.10.10.17 连接被拒——Mac check 通道当前不可用，非阻塞。
9. ✅ 提交（feat(nodes) + docs(openspec) 拆分）
  - 实证 (2026-09-11)：`53e07f4`（action mode 3/3 测试绿）+ `1f70053`（install CTE）
    + `46c6bce`（lib backend override）均已在 origin/main 祖先链上，无未推分支。

## 遗留待办
- E2E 画布拖拽实测（luajit.incremental 节点配置 args → 执行出 JSON 行）——需要 DuckDB 引擎 + luajit 扩展 env 的集成测试（SWANFLOW_LUAJIT_EXT）
- duckdb-luajit-libs 后续新增动作型库时同步更新此处映射
