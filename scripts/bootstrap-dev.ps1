Write-Host "Starting local crawler platform dependencies..."
docker compose up -d postgres redis kafka minio minio-init

Write-Host ""
Write-Host "Core services:"
Write-Host "  Postgres  : localhost:5432"
Write-Host "  Redis     : localhost:6379"
Write-Host "  Kafka     : localhost:9092"
Write-Host "  MinIO API : localhost:9000"
Write-Host "  MinIO UI  : localhost:9001"
Write-Host ""
Write-Host "Optional services:"
Write-Host "  docker compose --profile analytics up -d clickhouse"
Write-Host "  docker compose --profile search up -d opensearch"
