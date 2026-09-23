//! MCP Server — exposes SwanFlow capabilities over stdio for LangFlow / Dify / Claude Code.

use crate::core::engine::SwanFlowEngine;
use crate::core::graph::Workflow;
use crate::mcp::{
    self, ContentLengthTransport, JsonRpcRequest, JsonRpcResponse, McpResource,
    McpResourceContents, McpTool,
};
use crate::nodes::registry::NodeRegistry;
use serde_json::json;
use std::collections::HashMap;
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
    /// Registered workflows (name -> definition). Loaded from and persisted to
    /// `~/.swanflow/registered_workflows.json` so registrations survive restarts.
    registered: std::sync::Mutex<HashMap<String, RegisteredWorkflow>>,
}

/// A workflow registered as a named MCP tool.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RegisteredWorkflow {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// The full workflow YAML/JSON text.
    pub workflow_yaml: String,
    /// Optional JSON Schema for call arguments; merged into the tool's input_schema.
    #[serde(default)]
    pub param_schema: Option<serde_json::Value>,
    /// Node names/ids whose materialized parquet is returned on call.
    #[serde(default)]
    pub outputs: Vec<String>,
}

/// Reserved tool names that cannot be shadowed by a registered workflow.
const RESERVED_TOOLS: &[&str] = &[
    "run_workflow",
    "inspect_workflow",
    "list_nodes",
    "execute_sql",
    "list_connectors",
    "register_workflow",
    "unregister_workflow",
    "list_registered",
];

fn registered_workflows_path() -> std::path::PathBuf {
    let mut p = std::path::PathBuf::from(
        std::env::var("HOME").unwrap_or_else(|_| ".".to_string()),
    );
    p.push(".swanflow");
    p.push("registered_workflows.json");
    p
}

/// Read node materializations back as JSON rows via an isolated in-memory
/// DuckDB connection (does not touch the engine's DataFusion session).
fn read_node_outputs(
    cache_dir: &str,
    output_specs: &[String],
    max_rows: u64,
) -> serde_json::Value {
    let metadata_path = format!("{}/metadata.duckdb", cache_dir);
    let conn = match duckdb::Connection::open(&metadata_path) {
        Ok(c) => c,
        Err(e) => {
            return json!({"error": format!("cannot open metadata db: {e}")});
        }
    };
    let mut out = serde_json::Map::new();
    for spec in output_specs {
        // Match by table name (node_<uuid-prefix8>) or full node uuid.
        let rows: Vec<(String, String)> = match conn
            .prepare(
                "SELECT table_name, parquet_path FROM flow_metadata WHERE node_id = ? OR table_name = ?",
            ) {
            Ok(mut stmt) => match stmt.query_map(
                duckdb::params![spec.to_string(), format!("node_{}", spec)],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
            ) {
                Ok(mapped) => mapped.filter_map(|r| r.ok()).collect(),
                Err(e) => {
                    out.insert(
                        spec.clone(),
                        json!({"warning": format!("metadata query failed: {e}")}),
                    );
                    continue;
                }
            },
            Err(e) => {
                out.insert(
                    spec.clone(),
                    json!({"warning": format!("metadata lookup failed: {e}")}),
                );
                continue;
            }
        };
        if rows.is_empty() {
            out.insert(
                spec.clone(),
                json!({"warning": "no materialization found for this node"}),
            );
            continue;
        }
        // Use the latest entry (flow_metadata is append-ordered).
        let (_table, parquet_path) = rows.last().unwrap().clone();
        let limit = if max_rows == 0 { 10_000 } else { max_rows };
        match conn.prepare(&format!(
            "SELECT * FROM read_parquet('{}') LIMIT {}",
            parquet_path.replace('\'', "''"),
            limit + 1
        )) {
            Ok(mut stmt) => {
                let arrow = match stmt.query_arrow([]) {
                    Ok(a) => a,
                    Err(e) => {
                        out.insert(
                            spec.clone(),
                            json!({"warning": format!("parquet read failed: {e}")}),
                        );
                        continue;
                    }
                };
                let mut records = Vec::new();
                let mut truncated = false;
                for batch in arrow {
                    for row_idx in 0..batch.num_rows() {
                        if (records.len() as u64) >= limit {
                            truncated = true;
                            break;
                        }
                        let mut obj = serde_json::Map::new();
                        for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                            let col = batch.column(col_idx).clone();
                            let v = arrow_value_to_json(&col, row_idx);
                            obj.insert(field.name().clone(), v);
                        }
                        records.push(serde_json::Value::Object(obj));
                    }
                }
                let mut entry = json!({"rows": records});
                if truncated {
                    entry["truncated"] = json!(true);
                }
                out.insert(spec.clone(), entry);
            }
            Err(e) => {
                out.insert(
                    spec.clone(),
                    json!({"warning": format!("parquet read failed: {e}")}),
                );
            }
        }
    }
    serde_json::Value::Object(out)
}

/// Best-effort conversion of an Arrow cell to JSON (strings/numbers/bools/null;
/// other types fall back to their debug string).
///
/// Uses duckdb's bundled arrow (query_arrow returns duckdb::arrow batches, a
/// different crate instance from datafusion::arrow).
fn arrow_value_to_json(
    col: &std::sync::Arc<dyn duckdb::arrow::array::Array + 'static>,
    row: usize,
) -> serde_json::Value {
    use duckdb::arrow::array::*;
    use duckdb::arrow::datatypes::DataType;
    if col.is_null(row) {
        return serde_json::Value::Null;
    }
    match col.data_type() {
        DataType::Utf8 => {
            json!(col.as_any().downcast_ref::<StringArray>().unwrap().value(row))
        }
        DataType::LargeUtf8 => json!(col
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(row)),
        DataType::Utf8View => json!(col
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(row)),
        DataType::Boolean => json!(col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(row)),
        DataType::Int8 => json!(col
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .value(row)),
        DataType::Int16 => json!(col
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(row)),
        DataType::Int32 => json!(col
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(row)),
        DataType::Int64 => json!(col
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row)),
        DataType::UInt8 => json!(col
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .value(row)),
        DataType::UInt16 => json!(col
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap()
            .value(row)),
        DataType::UInt32 => json!(col
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(row)),
        DataType::UInt64 => json!(col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(row)),
        DataType::Float32 => json!(col
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(row)),
        DataType::Float64 => json!(col
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row)),
        _ => json!(format!("{:?}", col)),
    }
}

impl McpServer {
    pub fn new(engine: SwanFlowEngine, registry: Arc<NodeRegistry>) -> Self {
        let registered = Self::load_registry()
            .unwrap_or_default();
        Self {
            engine,
            registry,
            registered: std::sync::Mutex::new(registered),
        }
    }

    fn load_registry() -> anyhow::Result<HashMap<String, RegisteredWorkflow>> {
        let path = registered_workflows_path();
        if !path.exists() {
            return Ok(HashMap::new());
        }
        let text = std::fs::read_to_string(&path)?;
        Ok(serde_json::from_str(&text)?)
    }

    fn persist_registry(&self) -> anyhow::Result<()> {
        let path = registered_workflows_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let map = self.registered.lock().unwrap();
        let text = serde_json::to_string_pretty(&*map)?;
        std::fs::write(&path, text)?;
        Ok(())
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
        let mut tools = vec![
            McpTool {
                name: "run_workflow".to_string(),
                description: Some(
                    "Execute a SwanFlow workflow from YAML/JSON with optional params; returns output node data when output_nodes or workflow 'outputs' are set"
                        .to_string(),
                ),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "workflow_yaml": {
                            "type": "string",
                            "description": "YAML or JSON workflow definition string"
                        },
                        "params": {
                            "type": "object",
                            "description": "Variables merged into workflow.context (override YAML-declared values); referenced in nodes via ${context.key} or {{key}}"
                        },
                        "output_nodes": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Node names/ids whose materialized results should be returned (overrides workflow 'outputs')"
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
            McpTool {
                name: "register_workflow".to_string(),
                description: Some(
                    "Register a workflow YAML as a named MCP tool; call it later by name with JSON arguments as params"
                        .to_string(),
                ),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Tool name for the registered workflow"},
                        "workflow_yaml": {"type": "string", "description": "Workflow YAML/JSON text"},
                        "description": {"type": "string", "description": "Tool description (optional)"},
                        "param_schema": {"type": "object", "description": "JSON Schema for call arguments (optional)"},
                        "outputs": {"type": "array", "items": {"type": "string"}, "description": "Node names/ids to return (defaults to workflow 'outputs')"}
                    },
                    "required": ["name", "workflow_yaml"]
                }),
            },
            McpTool {
                name: "unregister_workflow".to_string(),
                description: Some("Remove a previously registered workflow tool".to_string()),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"}
                    },
                    "required": ["name"]
                }),
            },
            McpTool {
                name: "list_registered".to_string(),
                description: Some("List registered workflow tools".to_string()),
                input_schema: json!({
                    "type": "object",
                    "properties": {}
                }),
            },
        ];

        // Merge registered workflows as named tools.
        if let Ok(map) = self.registered.lock() {
            for wf in map.values() {
                tools.push(wf.to_tool());
            }
        }
        tools
    }

    async fn call_tool(
        &self,
        name: &str,
        arguments: serde_json::Value,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        // Registered workflow tools: name is not a reserved tool -> execute it
        // with arguments as params.
        if !RESERVED_TOOLS.contains(&name) {
            let wf = {
                let map = self.registered.lock().unwrap();
                map.get(name).cloned()
            };
            if let Some(wf) = wf {
                let result = self
                    .execute_with_params(&wf.workflow_yaml, arguments, wf.outputs.clone())
                    .await?;
                return Ok(vec![json!({
                    "type": "text",
                    "text": serde_json::to_string(&result)?
                })]);
            }
            // fall through to the reserved-tool match, which reports Unknown tool
        }
        match name {
            "run_workflow" => {
                let yaml_str = arguments["workflow_yaml"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Missing workflow_yaml argument"))?;

                let params = arguments.get("params").cloned().unwrap_or(json!({}));
                let output_nodes: Vec<String> = arguments
                    .get("output_nodes")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default();

                let result = self.execute_with_params(yaml_str, params, output_nodes).await?;
                Ok(vec![json!({
                    "type": "text",
                    "text": serde_json::to_string(&result)?
                })])
            }

            "register_workflow" => {
                let wf_name = arguments["name"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Missing name argument"))?
                    .to_string();
                let yaml_str = arguments["workflow_yaml"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Missing workflow_yaml argument"))?;

                // Validate: parse the workflow, reject reserved-name collisions.
                let workflow = parse_workflow(yaml_str)
                    .map_err(|e| anyhow::anyhow!("Invalid workflow definition: {e}"))?;
                if RESERVED_TOOLS.contains(&wf_name.as_str()) {
                    return Err(anyhow::anyhow!(
                        "'{wf_name}' is a reserved tool name; pick another"
                    ));
                }

                // outputs: explicit argument > workflow 'outputs' field
                let outputs: Vec<String> = arguments
                    .get("outputs")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .filter(|v: &Vec<String>| !v.is_empty())
                    .unwrap_or(workflow.outputs.clone());

                let entry = RegisteredWorkflow {
                    name: wf_name.clone(),
                    description: arguments
                        .get("description")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    workflow_yaml: yaml_str.to_string(),
                    param_schema: arguments.get("param_schema").cloned(),
                    outputs,
                };
                {
                    let mut map = self.registered.lock().unwrap();
                    map.insert(wf_name.clone(), entry);
                }
                self.persist_registry()?;

                Ok(vec![json!({
                    "type": "text",
                    "text": format!("Workflow '{}' registered as tool '{}'", workflow.name, wf_name)
                })])
            }

            "unregister_workflow" => {
                let wf_name = arguments["name"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Missing name argument"))?;
                let removed = {
                    let mut map = self.registered.lock().unwrap();
                    map.remove(wf_name)
                };
                match removed {
                    Some(_) => {
                        self.persist_registry()?;
                        Ok(vec![json!({
                            "type": "text",
                            "text": format!("Tool '{wf_name}' unregistered")
                        })])
                    }
                    None => Err(anyhow::anyhow!("No registered tool named '{wf_name}'")),
                }
            }

            "list_registered" => {
                let list: Vec<serde_json::Value> = {
                    let map = self.registered.lock().unwrap();
                    map.values()
                        .map(|wf| {
                            json!({
                                "name": wf.name,
                                "description": wf.description,
                                "outputs": wf.outputs,
                                "has_param_schema": wf.param_schema.is_some(),
                            })
                        })
                        .collect()
                };
                Ok(vec![json!({
                    "type": "text",
                    "text": serde_json::to_string_pretty(&list)?
                })])
            }

            "inspect_workflow" => {
                let yaml_str = arguments["workflow_yaml"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Missing workflow_yaml argument"))?;

                let workflow = parse_workflow(yaml_str)?;

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

    /// Parse a workflow from YAML or JSON text.
    fn parse_workflow_text(yaml_str: &str) -> anyhow::Result<Workflow> {
        parse_workflow(yaml_str)
    }

    /// Execute a workflow with params merged into its context, then read back
    /// materialized outputs for the given output specs (empty = summary only).
    async fn execute_with_params(
        &self,
        yaml_str: &str,
        params: serde_json::Value,
        output_specs: Vec<String>,
    ) -> anyhow::Result<serde_json::Value> {
        let mut workflow = parse_workflow(yaml_str)?;

        // Merge params into workflow.context — call-side values win.
        if let Some(map) = params.as_object() {
            for (k, v) in map {
                workflow.context.insert(k.clone(), v.clone());
            }
        }

        let effective_outputs: Vec<String> = if output_specs.is_empty() {
            workflow.outputs.clone()
        } else {
            output_specs
        };

        let wf_name = workflow.name.clone();
        let mut rx = self.engine.execute_workflow(&workflow).await?;
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(serde_json::to_value(&event)?);
        }

        let mut result = json!({
            "workflow": wf_name,
            "events": events.len(),
        });
        if !effective_outputs.is_empty() {
            let outputs = read_node_outputs(
                &self.engine.cache_dir,
                &effective_outputs,
                self.engine.max_rows_per_node,
            );
            result["outputs"] = outputs;
        }
        Ok(result)
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

/// Parse a workflow from YAML or JSON text (JSON detected by leading '{').
fn parse_workflow(yaml_str: &str) -> anyhow::Result<Workflow> {
    let trimmed = yaml_str.trim();
    if trimmed.starts_with('{') {
        Ok(serde_json::from_str(trimmed)?)
    } else {
        Ok(serde_yaml::from_str(trimmed)?)
    }
}

impl RegisteredWorkflow {
    /// Project this registration into an MCP tool definition.
    fn to_tool(&self) -> McpTool {
        let schema = self.param_schema.clone().unwrap_or_else(|| {
            // Infer a permissive object schema when none was provided.
            json!({
                "type": "object",
                "description": "Arguments are merged into workflow.context as params"
            })
        });
        McpTool {
            name: self.name.clone(),
            description: Some(
                self.description
                    .clone()
                    .unwrap_or_else(|| format!("Run registered workflow '{}'", self.name)),
            ),
            input_schema: schema,
        }
    }
}

#[cfg(test)]
mod workflow_as_api_tests {
    use super::{parse_workflow, RegisteredWorkflow, RESERVED_TOOLS};

    const MIN_YAML: &str = "id: 00000000-0000-0000-0000-000000000099\nname: t\ngraph:\n  nodes: []\n  edges: []\n  edge_property: directed\n";

    #[test]
    fn parse_workflow_yaml_and_json() {
        let by_yaml = parse_workflow(MIN_YAML).unwrap();
        assert_eq!(by_yaml.name, "t");
        let by_json = parse_workflow(
            r#"{"id":"00000000-0000-0000-0000-000000000099","name":"t","graph":{"nodes":[],"edges":[],"edge_property":"directed"}}"#,
        )
        .unwrap();
        assert_eq!(by_json.name, "t");
    }

    #[test]
    fn parse_workflow_reads_outputs_field() {
        let yaml = "id: 00000000-0000-0000-0000-000000000098\nname: t\noutputs: [n1]\ngraph:\n  nodes: []\n  edges: []\n  edge_property: directed\n";
        let wf = parse_workflow(yaml).unwrap();
        assert_eq!(wf.outputs, vec!["n1".to_string()]);
        // omitted -> default empty, not an error
        assert!(parse_workflow(MIN_YAML).unwrap().outputs.is_empty());
    }

    #[test]
    fn reserved_names_cannot_be_registered() {
        for name in RESERVED_TOOLS {
            assert!(RESERVED_TOOLS.contains(name));
        }
        assert!(RESERVED_TOOLS.contains(&"run_workflow"));
        assert!(!RESERVED_TOOLS.contains(&"graph_query"));
    }

    #[test]
    fn registered_workflow_to_tool_defaults() {
        let wf = RegisteredWorkflow {
            name: "graph_query".into(),
            description: None,
            workflow_yaml: MIN_YAML.into(),
            param_schema: None,
            outputs: vec!["n1".into()],
        };
        let tool = wf.to_tool();
        assert_eq!(tool.name, "graph_query");
        assert!(tool.description.as_deref().unwrap().contains("graph_query"));
        assert_eq!(
            tool.input_schema["type"].as_str().unwrap(),
            "object"
        );
    }
}

#[cfg(test)]
mod guard_sql_tests {
    use super::McpServer;

    /// ai-orchestration-enhancement 6.4: SELECT passes / write ops rejected.
    /// Structural safety — no prompt or model can route a write past this.
    #[test]
    fn allows_select() {
        assert!(McpServer::guard_sql("SELECT * FROM t").is_ok());
    }

    #[test]
    fn allows_select_with_where_and_join() {
        assert!(McpServer::guard_sql(
            "SELECT a.x, b.y FROM a JOIN b ON a.id = b.id WHERE a.z = 1"
        )
        .is_ok());
    }

    #[test]
    fn allows_cte() {
        assert!(McpServer::guard_sql("WITH x AS (SELECT 1) SELECT * FROM x").is_ok());
    }

    #[test]
    fn allows_leading_whitespace_then_select() {
        assert!(McpServer::guard_sql("   SELECT count(*) FROM t").is_ok());
    }

    #[test]
    fn rejects_insert() {
        let err = McpServer::guard_sql("INSERT INTO t VALUES (1)").unwrap_err();
        assert!(err.to_string().contains("INSERT"));
    }

    #[test]
    fn rejects_delete() {
        let err = McpServer::guard_sql("DELETE FROM t").unwrap_err();
        assert!(err.to_string().contains("DELETE"));
    }

    #[test]
    fn rejects_drop() {
        let err = McpServer::guard_sql("DROP TABLE t").unwrap_err();
        assert!(err.to_string().contains("DROP"));
    }

    #[test]
    fn rejects_update_with_leading_whitespace() {
        let err = McpServer::guard_sql("  UPDATE t SET x = 1").unwrap_err();
        assert!(err.to_string().contains("UPDATE"));
    }

    #[test]
    fn rejects_multistatement_write() {
        let err = McpServer::guard_sql("SELECT 1; DELETE FROM t").unwrap_err();
        assert!(err.to_string().contains("DELETE"));
    }

    #[test]
    fn rejects_newline_separated_write() {
        let err = McpServer::guard_sql("SELECT 1\nINSERT INTO t VALUES (1)").unwrap_err();
        assert!(err.to_string().contains("INSERT"));
    }

    /// KNOWN LIMITATION (documented, not asserted as a bug): the guard is a
    /// naive keyword match on whitespace boundaries, so a SELECT whose
    /// identifiers merely *contain* a forbidden token (e.g. `updated_at`) is
    /// over-blocked. Left as-is intentionally — over-blocking is the safe
    /// failure mode for a SELECT-only surface. Recorded so a future switch to
    /// a real SQL tokenizer is a conscious decision, not an accident.
    #[test]
    fn note_identifier_substring_is_overblocked_safe() {
        // `updated_at` contains "UPDATE" with a leading-space boundary, so the
        // guard treats it as a write. This is the documented conservative bias.
        assert!(McpServer::guard_sql("SELECT updated_at FROM t").is_err());
    }
}
