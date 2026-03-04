#!/bin/bash

# Setup MinIO for local S3-compatible development
echo "🚀 Starting MinIO container..."

docker run -d \
  -p 9000:9000 \
  -p 9001:9001 \
  --name swanflow-minio \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  quay.io/minio/minio server /data --console-address ":9001"

echo "✅ MinIO is running at http://localhost:9000"
echo "Console is at http://localhost:9001 (minioadmin/minioadmin)"

# Create the default bucket
echo "📦 Creating 'swanflow' bucket..."
sleep 5
docker exec swanflow-minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec swanflow-minio mc mb local/swanflow

echo "✨ Ready for BoxLite development!"
