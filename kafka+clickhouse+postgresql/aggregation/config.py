import os

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123")
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "host=postgres port=5432 dbname=metrics user=metrics password=metrics",
)
SCHEDULE_INTERVAL_SEC = int(os.getenv("SCHEDULE_INTERVAL_SEC", "60"))
TOP_MOVIES_LIMIT = int(os.getenv("TOP_MOVIES_LIMIT", "10"))

# S3 / MinIO
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "movie-analytics")
S3_EXPORT_SCHEDULE_SEC = int(os.getenv("S3_EXPORT_SCHEDULE_SEC", "3600"))
