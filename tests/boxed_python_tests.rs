use anyhow::Result;
use rustflow::core::datasource::S3Manager;
use std::time::Duration;

#[tokio::test]
async fn test_s3_manager_presigned_logic() -> Result<()> {
    // Note: This test requires AWS_ACCESS_KEY_ID etc to be set or a local MinIO
    // For CI/Unit test, we just verify the logic if possible or skip
    let bucket = "test-bucket".to_string();

    // We only run this if environment variables are present
    if std::env::var("AWS_ACCESS_KEY_ID").is_err() {
        println!("Skipping S3 test: AWS credentials not found");
        return Ok(());
    }

    let manager = S3Manager::new(bucket).await?;

    let key = "test_data.parquet";
    let expires = Duration::from_secs(3600);

    let get_url = manager.get_presigned_url(key, expires).await?;
    assert!(get_url.contains("test-bucket"));
    assert!(get_url.contains(key));

    let put_url = manager.put_presigned_url(key, expires).await?;
    assert!(put_url.contains("test-bucket"));

    Ok(())
}
