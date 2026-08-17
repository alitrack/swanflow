## Why

linalg 已具备 7 算子 GPU 后端（matmul/svd/eigh/inv/lu/chol/qr，LuaJIT FFI → cuBLAS/cuSOLVER，`LUA_LINALG_GPU=1` env 门控，2026-08-17/18 实测 SGEMM 2048³=53.4 TFLOPS、TPC-H 6M 行端到端 13.4×）。但两个缺口：

1. **画布节点无法表达 GPU**——`luajit.linalg` 节点没有 backend 概念，GPU 机器上跑哪条路径完全由进程 env 决定，用户无法在流程里选择/验证
2. **SQL 侧无法逐调用选择**——env 门控是进程级的，同一连接内想对比 GPU vs CPU 需要重启

## What Changes

- `LuaJitLibNode` config_schema 增加 **`backend`** 字段（`auto`/`gpu`/`cpu`，默认 `auto`）
- `LuaJitLibNode::propose_plan`：backend ≠ auto 时并入 `args` JSON（仅 object 形态），最终经 `luajit_s('<lib>', <args>)` 传到 Lua 侧
- `luajit_libs_from_index`：`linalg` 识别为 action 型库（预填 SVD 2×2 模板 `{"op":"svd","a":[4,7,2,6],"m":2,"n":2}`）+ 专属描述标注 GPU-capable
- **duckdb-luajit-libs 侧**（linalg.lua `solve()`）：增加 per-call `backend` 覆盖——
  - `'cpu'`：强制 CPU（即使 env 开了 GPU）
  - `'gpu'`：强制 GPU；不可用（无 cuBLAS/cuSOLVER 或 op 无 GPU 版）→ 明确报错
  - `'auto'`/缺省：维持 env 门控（现状不变）
  - GPU 成功路径结果带 `backend:'gpu'` 标记（结果可审计实际后端）

## Capabilities

### New Capabilities
- `linalg-gpu-node`: `luajit.linalg` 画布节点可指定 backend（gpu/cpu/auto），GPU 机器上可视化流程直接选 GPU 数值层

### Modified Capabilities
- `luajit-lib-node`: 新增 backend 配置项，非 auto 时并入 action args；linalg 由普通 INDEX 注册升级为 action 预填 + GPU 描述

## Impact

- **luajit.rs**: LuaJitLibNode 字段/描述/schema + propose_plan 合并逻辑 + INDEX action 匹配表 + 测试
- **linalg.lua**: solve() 分发前置 backend 分支（GPU/CPU 既有路径不动，纯新增覆盖层）
- **兼容**: backend 缺省 = auto = 旧行为完全一致；linalg 旧节点（table/scalar 形态）不受影响；args 为 array 时不合并 backend
- **测试**: 3 个新单测（backend gpu/cpu 并入、auto 不动、INDEX linalg action 识别）

## Test Plan

- `cargo test -p swanflow --lib luajit`：新增 test_lib_node_backend_merge + test_libs_index_action_libs 扩展
- linalg.lua 无 GPU 环境回归：auto→CPU、backend=cpu→CPU、backend=gpu→明确报错（6/6 已在本机通过）
- 遗留端到端（115 有 GPU 环境）：同步 linalg.lua → `luajit_module install` 重跑（缓存命中旧版）→ `luajit_s('linalg', '{"op":"svd",...,"backend":"gpu"}')` 结果带 backend:'gpu'
