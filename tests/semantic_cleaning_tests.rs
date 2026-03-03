use anyhow::Result;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::array::StringArray;
use rustflow::nodes::transform::SemanticCleanerNode;
use rustflow::nodes::NodeBehavior;
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn test_semantic_cleaner_apply_rules() -> Result<()> {
    let ctx = SessionContext::new();
    
    // Create input data with inconsistencies
    let schema = Arc::new(Schema::new(vec![
        Field::new("company", DataType::Utf8, true),
    ]));
    
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["Alibaba", "ALIBABA", "Alibaba Group", "Google"]))]
    )?;
    
    let df = ctx.read_batch(batch)?;
    let input_plan = df.into_optimized_plan()?;

    let cleaner = SemanticCleanerNode;
    let config = json!({
        "column": "company",
        "rules": {
            "Alibaba": "Alibaba Group",
            "ALIBABA": "Alibaba Group"
        }
    });

    let output_plans = cleaner.propose_plan(vec![input_plan], &config).await?;
    let physical_plan = ctx.state().create_physical_plan(&output_plans[0]).await?;
    let results = datafusion::physical_plan::collect(physical_plan, ctx.task_ctx()).await?;
    
    let cleaned_col = results[0]
        .column_by_name("company_cleaned")
        .expect("Column not found")
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(cleaned_col.value(0), "Alibaba Group");
    assert_eq!(cleaned_col.value(1), "Alibaba Group");
    assert_eq!(cleaned_col.value(2), "Alibaba Group"); // Else branch
    assert_eq!(cleaned_col.value(3), "Google");

    Ok(())
}
