## 关键决策

### 1. backend 合并位置：propose_plan，不是 validate
backend 只是「args 的一个键」，放 validate 会引入额外的 schema 分支。propose_plan 里 `cfg` 是可变 JSON，直接 `args["backend"] = ...` 一步到位，SQL 生成前完成。配置存 `backend`（节点层语义），SQL 层透传为 `args.backend`（Lua 层语义），两层解耦。

### 2. args 可能不是 object
action 模式的 args 是 object（struct 字面量），但 table/scalar 模式的 args 是 array（多参数组）。只对 `is_object()` 合并，array 形态直接忽略——backend 对 table/scalar 无意义。

### 3. 借用检查
`cfg.get("backend")` 的引用与 `cfg.get_mut("args")` 冲突（E0502），先 `map(|s| s.to_string())` 取 owned 值再改 cfg。

### 4. linalg.lua 的 backend 语义
- `'cpu'` 在 GPU 可用时也强制 CPU——用于对照实验（同一连接内 A/B）
- `'gpu'` 不可用时报错而非静默回退——显式要求必须显式失败，否则用户以为算了 GPU 实际算了 CPU
- `'auto'`（缺省）= 旧行为：`gpu and gpu_ops[op]` 成立走 GPU，否则 CPU，无 GPU 静默 CPU
- GPU 结果 `res.backend = 'gpu'`：调用方可审计「这次到底跑在哪个后端」

### 5. 既有陷阱（linalg.lua 已解决，backend 层不引入新坑）
- cuSOLVER 32 位 dgesvd 在 Ada N≥768 失败 → 必须 Xgesvd（GPU 路径已用）
- FFI vararg 参数错位（f(...,x,y) 第 3 参变 uint64_t[1]）→ bufferSize 内联精确传参
- cudaFree 后 D2H = use-after-free 段错误 → D2H 先于 free

## 前置验证

- [x] cargo check -p swanflow 0 error
- [x] linalg.lua 无 GPU 回归 6/6（auto→CPU、cpu→CPU、gpu→报错、matmul/svd 数值）
- [ ] cargo test -p swanflow --lib luajit（WSL 全量构建中）
- [ ] 115 端到端 backend='gpu'（需同步 linalg.lua + 重跑 install）
