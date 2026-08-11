# Tasks: LuaJIT Self-Evolution

**Change**: luajit-self-evolution
**Date**: 2026-08-12
**Status**: Implemented ✅（2026-08-12 全部完成，待用户确认推送合并）

## 执行前置

- 分支：`feat/luajit-self-evolution` ✅
- 铁律：未触碰未提交区 `crates/swanflow-core/src/nodes.rs`、
  `crates/swanflow-core/src/registry.rs`（M 状态）、
  `openspec/changes/graph-repair-operators/`、`middleware-stack-compiler/`、
  `schema-config-contract/` ✅
- 测试环境：`LD_LIBRARY_PATH=/home/lhy/.local/share/uv/python/cpython-3.11.14-linux-x86_64-gnu/lib`
  （SwanFlow Python 节点动态链接 libpython3.11，uv 管理的 Python 3.11.14）
- 扩展路径：`SWANFLOW_LUAJIT_EXT=/mnt/d/wsl2/luajit/build/release/luajit.duckdb_extension`

## S1: 版本兼容验证 ✅

- [x] 1.1 /tmp/luajit-verify 最小工程（duckdb `=1.10503.1`）LOAD 1.5.5 编译的
  luajit v0.30 + `luajit_table` → exit 0
- [x] 1.2 确认版本映射 `1.10503.1`=1.5.3、须 `=` 锁定
- [x] 1.3 确认未签名扩展必须启动级 `Config::allow_unsigned_extensions`

## S2: 扩展打进 SwanFlow ✅

- [x] 2.1 `duckdb_backend.rs` open() 换 Config + 环境变量扩展加载（向后兼容）
- [x] 2.2 集成测试 `test_luajit_extension_via_env`（SWANFLOW_LUAJIT_EXT gate，
  不进 CI 硬依赖）
- [x] 2.3 修复 execute_sql：先 query 再读 schema（arrow statement API 陷阱）
- [x] 2.4 修复类型映射：arrow Debug 格式（Int64/Utf8）补齐
- [x] 2.5 `.cargo/config.toml` LD_LIBRARY_PATH 去掉 relative=true（原配置
  未生效导致 libpython 找不到）+ 清理 MY_TEST_VAR 残留

## S3: luajit 壳节点 + INDEX 自动注册 ✅

- [x] 3.1 `nodes/luajit.rs`：`LuaJitNode`（core.luajit，table/scalar 两模式）
- [x] 3.2 schema 发现：scratch DuckDB 连接 LOAD 扩展后执行查询读 schema
- [x] 3.3 `LuaJitLibNode` + `luajit_libs_from_index` + `default_luajit_libs_index`
- [x] 3.4 registry 自动注册（INDEX 存在时 12 个 lib 全注册）
- [x] 3.5 端到端测试：propose_plan → DuckDBScanExec → 2 行 RecordBatch

## S4: create_lua_lib 工具 ✅

- [x] 4.1 AgentTool::CreateLuaLib 枚举 + dispatch 分支
- [x] 4.2 实现：名字清洗、落盘 libs/<cat>/<name>.lua、INDEX append 去重、
  根解析（env > dev clone > 离线缓存）
- [x] 4.3 system prompt 工具说明（含沙箱审批提示）
- [x] 4.4 测试：写盘 + INDEX 更新 + 去重

## 收尾 ✅

- [x] 相关模块全测试绿（luajit 7 个 + duckdb_backend 4 个 + agent 1 个）
- [x] openspec 文档（proposal/design/tasks/specs）
- [ ] 用户确认后推送合并（分支已建，未 push）

## 遗留/待定

- 社区注册表发布（description.yml 已有，审批重新提交后 `INSTALL luajit`）
- 静态编进 bundled duckdb vs LOAD 绝对路径的打包决策（当前 LOAD 路径）
- scalar 模式多列/参数化打磨（当前最小可用）
