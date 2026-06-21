//! MCP Server — exposes SwanFlow capabilities over stdio for LangFlow / Dify / Claude Code.

use crate::core::engine::SwanFlowEngine;
use crate::core::graph::Workflow;
use crate::mcp::{
    self, ContentLengthTransport, JsonRpcRequest, JsonRpcResponse, McpResource,
    McpResourceContents, McpTool,
};
use crate::nodes::registry::NodeRegistry;
use serde_json::json;
use std::sync::Arc;
use tokio::io;

/// SwanFlow MCP Server.
///
/// Listens for JSON-RPC 2.0 requests on stdin and writes responses to stdout.
/// Supported methods:
/// - `initialize` — server info + capabilities
/// - `tools/list` — list available tools
/// - `tools/call` — call a tool by name
/// - `resources/list` — list available resources
/// - `resources/read` — read a resource by URI
pub struct McpServer {
    engine: SwanFlowEngine,
    registry: Arc<NodeRegistry>,
}

impl McpServer {
    pub fn new(engine: SwanFlowEngine, registry: Arc<NodeRegistry>) -> Self {
        Self { engine, registry }
    }

    /// Run the MCP server loop on stdin/stdout.
    pub async fn run(&self) -> anyhow::Result<()> {
        let stdin = io::stdin();
        let stdout = io::stdout();
        let mut transport = ContentLengthTransport::new(stdin, stdout);

        loop {
            let request = match transport.read_request().await {
                Ok(req) => req,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    break; // stdin closed
                }
                Err(e) => {
                    eprintln!("[MCP Server] Read error: {e}");
                    break;
                }
            };

            let response = self.handle_request(&request).await;
            if let Err(e) = transport.write_response(&response).await {
                eprintln!("[MCP Server] Write error: {e}");
                break;
            }
        }

        Ok(())
    }

    async fn handle_request(&self, request: &JsonRpcRequest) -> JsonRpcResponse {
        let id = request.id.clone();

        match request.method.as_str() {
            "initialize" => {
                let capabilities = json!({
                    "tools": {
                        "listChanged": true
                    },
                    "resources": {
                        "listChanged": true
                    }
                });
                let result = json!({
                    "protocolVersion": "2024-11-05",
                    "capabilities": capabilities,
                    "serverInfo": {
                        "name": "SwanFlow",
                        "version": "0.1.0"
                    }
                });
                mcp::success_response(id, result)
            }

            "tools/list" => {
                let tools = self.list_tools();
                let result = json!({ "tools": tools });
                mcp::success_response(id, result)
            }

            "tools/call" => {
                let name = request.params["name"].as_str().unwrap_or("").to_string();
                let arguments = request
                    .params
                    .get("arguments")
                    .cloned()
                    .unwrap_or(json!({}));

                match self.call_tool(&name, arguments).await {
                    Ok(content) => mcp::success_response(id, json!({ "content": content })),
                    Err(e) => mcp::error_response(id, -1, format!("Tool error: {e}")),
                }
            }

            "resources/list" => {
                let resources = self.list_resources();
                let result = json!({ "resources": resources });
                mcp::success_response(id, result)
            }

            "resources/read" => {
                let uri = request.params["uri"].as_str().unwrap_or("").to_string();
                match self.read_resource(&uri).await {
                    Ok(contents) => mcp::success_response(id, json!({ "contents": contents })),
                    Err(e) => mcp::error_response(id, -1, format!("Resource error: {e}")),
                }
            }

            _ => mcp::error_response(id, -32601, format!("Method not found: {}", request.method)),
        }
    }

    fn list_tools(&self) -> Vec<McpTool> {
        vec![
            McpTool {
                name: "run_workflow".to_string(),
                description: Some(
                    "Execute a SwanFlow workflow from YAML/JSON and return a summary of results"
                        .to_string(),
                ),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "workflow_yaml": {
                            "type": "string",
                            "description": "YAML or JSON workflow definition string"
                        }
                    },
                    "required": ["workflow_yaml"]
                }),
            },
            McpTool {
                name: "inspect_workflow".to_string(),
                description: Some(
                    "Inspect a workflow's schemas and diagnostics without executing it".to_string(),
                ),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "workflow_yaml": {
                            "type": "string",
                            "description": "YAML or JSON workflow definition string"
                        }
                    },
                    "required": ["workflow_yaml"]
                }),
            },
            McpTool {
                name: "list_nodes".to_string(),
                description: Some(
                    "List all registered node types with their specifications".to_string(),
                ),
                input_schema: json!({
                    "type": "object",
                    "properties": {}
                }),
            },
            McpTool {
                name: "execute_sql".to_string(),
                description: Some(
                    "Execute a SQL query against a configured data source".to_string(),
                ),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "sql": { "type": "string", "description": "SQL query to execute" }
                    },
                    "required": ["sql"]
                }),
            },
            McpTool {
                name: "list_connectors".to_string(),
                description: Some("List all available database connectors".to_string()),
                input_schema: json!({
                    "type": "object",
                    "properties": {}
                }),
            },
        ]
    }

    async fn call_tool(
        &self,
        name: &str,
        arguments: serde_json::Value,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        match name {
            "run_workflow" => {
                let yaml_str = arguments["workflow_yaml"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Missing workflow_yaml argument"))?;

                let trimmed = yaml_str.trim();
                let workflow: Workflow = if trimmed.starts_with('{') {
                    serde_json::from_str(trimmed)?
                } else {
                    serde_yaml::from_str(trimmed)?
                };

                let mut rx = self.engine.execute_workflow(&workflow).await?;
                let mut events = Vec::new();
                while let Some(event) = rx.recv().await {
                    events.push(serde_json::to_value(event)?);
                }

                Ok(vec![json!({
                    "type": "text",
                    "text": format!("Workflow '{}' executed. {} events produced.", workflow.name, events.len())
                })])
            }

            "inspect_workflow" => {
                let yaml_str = arguments["workflow_yaml"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Missing workflow_yaml argument"))?;

                let trimmed = yaml_str.trim();
                let workflow: Workflow = if trimmed.starts_with('{') {
                    serde_json::from_str(trimmed)?
                } else {
                    serde_yaml::from_str(trimmed)?
                };

                let metadata = self.engine.infer_metadata(&workflow).await?;

                let summary: Vec<serde_json::Value> = metadata
                    .schemas
                    .iter()
                    .map(|(node_id, schemas)| {
                        json!({
                            "node_id": node_id.to_string(),
                            "output_ports": schemas.len(),
                            "diagnostics": metadata.diagnostics.get(node_id).map(|d| d.len()).unwrap_or(0),
                        })
                    })
                    .collect();

                Ok(vec![json!({
                    "type": "text",
                    "text": serde_json::to_string_pretty(&json!({
                        "workflow": workflow.name,
                        "nodes": summary
                    }))?
                })])
            }

            "list_nodes" => {
                let specs = self.registry.list_specs();
                let spec_json: Vec<serde_json::Value> = specs
                    .iter()
                    .map(|s| {
                        json!({
                            "type_id": s.type_id,
                            "display_name": s.display_name,
                            "description": s.description,
                        })
                    })
                    .collect();

                Ok(vec![json!({
                    "type": "text",
                    "text": serde_json::to_string_pretty(&spec_json)?
                })])
            }

            "execute_sql" => {
                let sql = arguments["sql"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Missing sql argument"))?;

                // SELECT-only guard — reject write operations at protocol level.
                // This is structural AI safety: no prompt can override this check.
                Self::guard_sql(sql)?;

                let df = self.engine.ctx.sql(sql).await?;
                let batches = df.collect().await?;
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

                Ok(vec![json!({
                    "type": "text",
                    "text": format!("SQL executed. {} rows returned across {} batches.", row_count, batches.len())
                })])
            }

            "list_connectors" => {
                let connectors = vec![
                    json!({"name": "postgres", "description": "PostgreSQL connector"}),
                    json!({"name": "mysql", "description": "MySQL connector"}),
                    json!({"name": "clickhouse", "description": "ClickHouse connector"}),
                ];

                Ok(vec![json!({
                    "type": "text",
                    "text": serde_json::to_string_pretty(&connectors)?
                })])
            }

            _ => Err(anyhow::anyhow!("Unknown tool: {name}")),
        }
    }

    /// SELECT-only SQL guard — rejects write operations at protocol level.
    ///
    /// This is structural AI safety: no prompt or model can override this check.
    /// Write operations must go through `run_workflow` with a proper pipeline,
    /// not raw SQL execution.
    fn guard_sql(sql: &str) -> anyhow::Result<()> {
        static FORBIDDEN: &[&str] = &[
            "INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
            "CREATE", "TRUNCATE", "REPLACE", "MERGE",
        ];

        let upper = sql.trim().to_uppercase();
        for keyword in FORBIDDEN {
            if upper.starts_with(keyword)
                || upper.contains(&format!(" {}", keyword))
                || upper.contains(&format!("\n{}", keyword))
                || upper.contains(&format!("\t{}", keyword))
            {
                return Err(anyhow::anyhow!(
                    "Write operation '{}' not allowed via execute_sql. \
                     Use run_workflow with a data sink node for modifications.",
                    keyword
                ));
            }
        }
        Ok(())
    }

    fn list_resources(&self) -> Vec<McpResource> {
        vec![
            McpResource {
                uri: "swanflow://nodes".to_string(),
                name: "Registered Nodes".to_string(),
                description: Some(
                    "List of all registered node types in the workflow engine".to_string(),
                ),
                mime_type: Some("application/json".to_string()),
            },
            McpResource {
                uri: "swanflow://version".to_string(),
                name: "Engine Version".to_string(),
                description: Some("SwanFlow engine version information".to_string()),
                mime_type: Some("application/json".to_string()),
            },
        ]
    }

    async fn read_resource(&self, uri: &str) -> anyhow::Result<Vec<McpResourceContents>> {
        match uri {
            "swanflow://nodes" => {
                let specs = self.registry.list_specs();
                let text = serde_json::to_string_pretty(&specs)?;
                Ok(vec![McpResourceContents {
                    uri: uri.to_string(),
                    mime_type: "application/json".to_string(),
                    text,
                }])
            }
            "swanflow://version" => {
                let text = serde_json::to_string_pretty(&json!({
                    "name": "SwanFlow",
                    "version": env!("CARGO_PKG_VERSION"),
                    "language": "Rust"
                }))?;
                Ok(vec![McpResourceContents {
                    uri: uri.to_string(),
                    mime_type: "application/json".to_string(),
                    text,
                }])
            }
            _ => Err(anyhow::anyhow!("Resource not found: {uri}")),
        }
    }
}
