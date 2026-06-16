---
id: ADR-003
title: "Adapter协议 — stdin/stdout JSON vs MCP vs gRPC"
status: accepted
date: "2026-06-16"
deciders: ["@lhy"]
tags: ["adapter", "protocol", "ipc", "external-interface"]
affected_crates: ["swanflow", "swanflow-adapter"]
supersedes: []
related: ["ADR-001"]
---

# ADR-003: Adapter协议设计决策

## Context

SwanFlow 需要为外部工具（cc、NPP Agent节点、自定义脚本）提供 API。考虑三种方案：

**Option A — stdin/stdout JSON 协议**
- 一个 binary（`swanflow-adapter`），读一行 JSON → 处理 → 写多行 JSON 到 stdout
- 类似 Docker CLI、JSON-RPC over stdio

**Option B — MCP Server**
- 通过 `nika-mcp` 类似的方式暴露为 MCP Server
- 外部工具通过 MCP 客户端调用

**Option C — gRPC / HTTP**
- 启动一个监听端口，通过 gRPC 或 REST API 通信

## Decision

采用 **Option A — stdin/stdout JSON**，理由：

1. **零网络依赖** — 不需要端口管理、端口冲突、认证
2. **管道兼容** — `cat request.json | swanflow-adapter | jq`
3. **耦合最小** — 外部进程启动 adapter、发送请求、读取事件流、退出
4. **与 cc 的协议一致** — cc 也是 stdin/stdout JSON 管道模式
5. **适合 Agent 节点场景** — NPP Agent 节点需要实时事件流 stdout 逐行输出

协议设计（见 `src/adapter/protocol.rs`）：
- 三个操作：`inspect`, `execute`, `cancel`
- `execute` 返回事件流（每行一个 JSON 事件）
- 事件类型：`node_state_changed`, `progress`, `error`, `run_completed`

## Consequences

**正向**：
- 零运行时依赖（无 HTTP/MCP/gRPC crate）
- 测试简单（直接 pipe JSON）

**负向**：
- 无连接保持（每次调用启动新进程）
- 协议演进较原始（需版本协商）
- 未来扩展 MCP Server 时可能需额外实现
