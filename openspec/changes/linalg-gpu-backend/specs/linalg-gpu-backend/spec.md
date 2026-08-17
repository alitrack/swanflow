# spec: linalg GPU backend

## LuaJitLibNode config

| 字段 | 类型 | 默认 | 语义 |
|------|------|------|------|
| `mode` | string | table | table/scalar/action |
| `source` | string | — | Lua 源码或 lib 名 |
| `args` | object\|array | — | action: struct 字面量；table/scalar: 参数组 |
| `backend` | string | `auto` | `auto`(env 门控)/`gpu`(强制)/`cpu`(强制)；非 auto 且 args 为 object 时并入 args.backend |

## 节点 SQL 形态（linalg）

```
WITH _reg AS (SELECT * FROM luajit_module(mode:='install', sql_name:='linalg'))
SELECT luajit_s('linalg', {op:'svd', a:[4,7,2,6], m:2, n:2, backend:'gpu'}) AS result FROM _reg
```

- backend 缺省/auto：不出现 `backend` 键（SQL 与旧版逐字节一致）
- INDEX 识别 linalg → mode 强制 action + args 预填 SVD 2×2 模板

## Lua 侧协议（linalg.lua solve）

| backend 值 | 行为 |
|------------|------|
| 缺省 / `auto` | `gpu and gpu_ops[op]` → GPU；否则 CPU（无 GPU 静默 CPU） |
| `cpu` | 强制 CPU（即使 GPU 可用） |
| `gpu` | 强制 GPU；gpu 不可用或 op 无 GPU 版 → `{status:'Error', message:'...GPU unavailable...'}` |

- GPU 成功路径：`res.backend = 'gpu'`（JSON 结果带标记，可审计实际后端）
- 既有语义不变：op/a/m/n 等字段、CPU 返回结构、错误结构
