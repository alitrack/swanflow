# linalg-gpu-backend

## 执行前置

- 不触碰他人未提交区（本次仅 luajit.rs + openspec 新目录；duckdb-luajit-libs 的 linalg.lua 独立仓库）
- 测试命令（WSL，需 LD_LIBRARY_PATH 指 libpython3.11）：
  ```bash
  cd /mnt/d/wsl2/swanflow && env LD_LIBRARY_PATH=/home/lhy/.local/share/uv/python/cpython-3.11.14-linux-x86_64-gnu/lib cargo test -p swanflow --lib luajit
  ```
- linalg.lua 回归（无 GPU 环境）：
  ```bash
  /mnt/d/wsl2/luajit/third_party/LuaJIT/src/luajit /tmp/linalg_backend_test.lua
  ```

## 铁律

- backend 缺省 = auto，任何情况下不得改变旧行为（无 backend 键 = 旧版行为逐字节一致）
- backend 只并入 object 形态 args；array 形态忽略
- GPU 结果必须带 backend:'gpu' 标记（可审计）
- 提交按语义拆分：feat(nodes)（luajit.rs）+ docs(openspec)（本目录），直接落 main

## 遗留待办

- [ ] 115 同步 linalg.lua（scp）+ `luajit_module install` 重跑（~/.duckdb/luajit-libs/ 缓存命中旧版，必须重装）
- [ ] 115 端到端：`luajit_s('linalg', '{"op":"svd","a":[...],"backend":"gpu"}')` → 结果 backend:'gpu'
- [ ] linalg.lua GPU 后端整体发布节奏：文章已推草稿（2026-08-18），公开后按需发布 lib 更新
