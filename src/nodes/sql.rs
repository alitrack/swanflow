use anyhow::Result;
use async_trait::async_trait;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::common::{DFSchema, DFSchemaRef};
use serde_json::Value as JsonValue;
use std::fmt;
use std::sync::Arc;
use uuid::Uuid;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

#[derive(Debug, Clone)]
pub struct SqlProxyLogical {
    pub node_id: Uuid,
    pub query: String,
    pub inputs: Vec<LogicalPlan>,
    pub schema: DFSchemaRef,
    // Add a mutex for internal schema updates if we really need it, 
    // but for now we align in the planner.
}

impl std::cmp::PartialEq for SqlProxyLogical {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id && self.query == other.query && self.inputs == other.inputs
    }
}

impl std::cmp::Eq for SqlProxyLogical {}

impl std::hash::Hash for SqlProxyLogical {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.node_id.hash(state);
        self.query.hash(state);
        self.inputs.hash(state);
    }
}

impl PartialOrd for SqlProxyLogical {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for SqlProxyLogical {
    fn name(&self) -> &str {
        "SqlProxy"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SqlProxy(node_id={}, query={})", self.node_id, self.query)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        Ok(Self {
            node_id: self.node_id,
            query: self.query.clone(),
            inputs,
            schema: self.schema.clone(),
        })
    }
}

pub struct SqlNode;

#[async_trait]
impl NodeBehavior for SqlNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.sql".to_string(),
            display_name: "SQL Transform".to_string(),
            description: "Execute custom SQL on inputs".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input_0".into(),
                description: "Primary Input".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Result".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string" }
                },
                "required": ["query"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["query"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("SQL Query is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let query = config["query"].as_str().unwrap().to_string();
        
        // Attempt early schema discovery if possible
        // We use a temporary context to see if we can parse the SQL
        let ctx = datafusion::prelude::SessionContext::new();
        for (i, input) in inputs.iter().enumerate() {
            let table_name = format!("input_{}", i);
            let df = datafusion::prelude::DataFrame::new(ctx.state(), input.clone());
            ctx.register_table(&table_name, df.into_view()).ok();
        }

        // Also register any already materialized tables if we can find them
        // (but usually we can't here because we don't have the engine's cache_dir)
        
        let schema: DFSchemaRef = match ctx.sql(&query).await {
            Ok(df) => Arc::new(df.schema().clone()),
            Err(_) => {
                // Fallback: if it's a cross-node query (referencing node_XXXX)
                // we might not be able to resolve it yet.
                // In that case, we use a placeholder or input schema.
                if !inputs.is_empty() {
                    inputs[0].schema().clone()
                } else {
                    Arc::new(DFSchema::empty())
                }
            }
        };

        let proxy = SqlProxyLogical {
            node_id: Uuid::nil(),
            query,
            inputs,
            schema,
        };

        Ok(vec![LogicalPlan::Extension(datafusion::logical_expr::Extension {
            node: Arc::new(proxy),
        })])
    }
}
