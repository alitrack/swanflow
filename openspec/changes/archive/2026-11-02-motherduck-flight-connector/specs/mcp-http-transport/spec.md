# mcp-http-transport Specification

## ADDED Requirements

### Requirement: Streamable HTTP 传输

The MCP client SHALL support a streamable HTTP transport targeting `https://api.motherduck.com/mcp` with `Authorization: Bearer $MOTHERDUCK_TOKEN`, reusing the existing JSON-RPC layer, without modifying the existing stdio transport.

MUST handle both response content types: single `application/json` body and `text/event-stream` frames.

#### Scenario: initialize 握手

- **GIVEN** mock MCP server 返回 `initialize` 结果与 `Mcp-Session-Id` 响应头
- **WHEN** 客户端发起 initialize
- **THEN** 握手成功，session id 被记录并在后续请求回带

#### Scenario: SSE 流式响应解析

- **GIVEN** 响应 content-type 为 `text/event-stream`，含一条 `data: {json-rpc 响应}` 帧
- **WHEN** 读取一条 JSON-RPC 消息
- **THEN** 解析器提取 data 载荷并反序列化为 JSON-RPC 响应，正确处理帧边界（半包/多事件拼接）

#### Scenario: 未知 Flight 工具错误传播

- **GIVEN** 服务端返回 JSON-RPC error（如 flight 不存在）
- **WHEN** 调用 `run_flight`
- **THEN** 错误以 Result::Err 返回给节点，错误信息含服务端 message

### Requirement: 传输层不触碰 stdio 现状

The new HTTP transport MUST be additive: the existing stdio `ContentLengthTransport` and `McpClient` behavior MUST remain unchanged, verified by existing stdio tests passing without modification.

#### Scenario: stdio 回归

- **GIVEN** 本 change 合入后
- **WHEN** 运行现有 MCP stdio 相关测试
- **THEN** 全部通过，零改动
