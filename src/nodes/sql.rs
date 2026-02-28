use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use serde_json::Value as JsonValue;
use anyhow::Result;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

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
        config["query"].as_str().ok_or_else(|| {
            anyhow::anyhow!("SQL Query is required")
        })?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let query = config["query"].as_str().unwrap();
        
        let ctx = SessionContext::new();
        for (i, plan) in inputs.into_iter().enumerate() {
            let table_name = format!("input_{}", i);
            // In DataFusion 45, we create a DataFrame directly from state and plan
            let df = DataFrame::new(ctx.state(), plan);
            ctx.register_table(&table_name, df.into_view())?;
        }

        let df = ctx.sql(query).await?;
        Ok(vec![df.into_optimized_plan()?])
    }
}
