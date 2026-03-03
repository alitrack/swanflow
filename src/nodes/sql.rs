use anyhow::Result;
use async_trait::async_trait;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::common::{DFSchema, DFSchemaRef, ScalarValue};
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
    pub params: Vec<ScalarValue>,
}

impl std::cmp::PartialEq for SqlProxyLogical {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id && self.query == other.query && self.inputs == other.inputs && self.params == other.params
    }
}

impl std::cmp::Eq for SqlProxyLogical {}

impl std::hash::Hash for SqlProxyLogical {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.node_id.hash(state);
        self.query.hash(state);
        self.inputs.hash(state);
        self.params.hash(state);
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
            params: self.params.clone(),
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
                    "query": { "type": "string" },
                    "params": {
                        "type": "array",
                        "items": { "type": "any" }
                    }
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
        let mut query = config["query"].as_str().unwrap().to_string();
        
        let params = if let Some(p_array) = config["params"].as_array() {
            p_array.iter().map(|v| {
                if let Some(i) = v.as_i64() {
                    ScalarValue::Int64(Some(i))
                } else if let Some(f) = v.as_f64() {
                    ScalarValue::Float64(Some(f))
                } else if let Some(b) = v.as_bool() {
                    ScalarValue::Boolean(Some(b))
                } else {
                    ScalarValue::Utf8(v.as_str().map(|s| s.to_string()))
                }
            }).collect()
        } else {
            vec![]
        };

        // WORKAROUND for DataFusion 52.1.0 sql() limitation with $1 placeholders:
        // We will do a safe-ish string replacement for the logical planning phase.
        // In a real production system, we'd use prepared statements if DataFusion supported them via sql().
        for (i, param) in params.iter().enumerate() {
            let placeholder = format!("${}", i + 1);
            let val_str = match param {
                ScalarValue::Utf8(Some(s)) => format!("'{}'", s.replace("'", "''")),
                _ => format!("{}", param),
            };
            query = query.replace(&placeholder, &val_str);
        }

        // Attempt early schema discovery if possible
        let ctx = datafusion::prelude::SessionContext::new();
        for (i, input) in inputs.iter().enumerate() {
            let table_name = format!("input_{}", i);
            let df = datafusion::prelude::DataFrame::new(ctx.state(), input.clone());
            ctx.register_table(&table_name, df.into_view()).ok();
        }

        let schema: DFSchemaRef = match ctx.sql(&query).await {
            Ok(df) => Arc::new(df.schema().clone()),
            Err(_) => {
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
            params,
        };

        Ok(vec![LogicalPlan::Extension(datafusion::logical_expr::Extension {
            node: Arc::new(proxy),
        })])
    }
}
