use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["A", "B", "A", "C", "B"])),
            Arc::new(Int64Array::from(vec![100, 200, 150, 300, 250])),
        ],
    )?;

    let file = File::create("data.parquet")?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    println!("Generated data.parquet");
    Ok(())
}
