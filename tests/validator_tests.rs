use anyhow::Result;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use rustflow::nodes::transform::TableSchemaValidatorNode;
use rustflow::nodes::{NodeBehavior, GraphSchemas};
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn test_validator_explicit_mapping() -> Result<()> {
    let ctx = SessionContext::new();
    
    // Create dummy input data with non-standard names
    let schema = Arc::new(Schema::new(vec![
        Field::new("person_name", DataType::Utf8, true),
        Field::new("organization", DataType::Utf8, true),
    ]));
    let df = ctx.read_batch(datafusion::arrow::record_batch::RecordBatch::new_empty(schema))?;
    let input_plan = df.into_optimized_plan()?;

    let validator = TableSchemaValidatorNode;
    let config = json!({
        "mapping": {
            "label": "person_name",
            "properties": "organization"
        }
    });

    let output_plans = validator.propose_plan(vec![input_plan], &config).await?;
    assert_eq!(output_plans.len(), 2);

    // Verify Node Schema (Port 0)
    let node_schema = output_plans[0].schema();
    assert_eq!(node_schema.fields().len(), 3); // id, label, properties
    assert!(node_schema.field_with_unqualified_name("label").is_ok());
    assert!(node_schema.field_with_unqualified_name("properties").is_ok());
    assert!(node_schema.field_with_unqualified_name("id").is_ok());

    Ok(())
}

#[tokio::test]
async fn test_validator_auto_mapping() -> Result<()> {
    let ctx = SessionContext::new();
    
    // Create input data that already has some standard names
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("extra", DataType::Utf8, true),
    ]));
    let df = ctx.read_batch(datafusion::arrow::record_batch::RecordBatch::new_empty(schema))?;
    let input_plan = df.into_optimized_plan()?;

    let validator = TableSchemaValidatorNode;
    let config = json!({}); // No mapping, should auto-detect 'id' and 'label'

    let output_plans = validator.propose_plan(vec![input_plan], &config).await?;
    
    // Verify Node Schema (Port 0)
    let node_schema = output_plans[0].schema();
    assert_eq!(node_schema.fields().len(), 3);
    
    // Verify Edge Schema (Port 1)
    let edge_schema = output_plans[1].schema();
    assert_eq!(edge_schema.fields().len(), 4); // source_id, target_id, type, weight

    Ok(())
}
