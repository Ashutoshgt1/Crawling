from __future__ import annotations

import os
from dataclasses import dataclass


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class Settings:
    postgres_dsn: str = os.getenv(
        "POSTGRES_DSN",
        "postgresql://crawler:crawler@localhost:5432/crawler",
    )
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_secure: bool = _bool_env("MINIO_SECURE", False)
    minio_raw_bucket: str = os.getenv("MINIO_RAW_BUCKET", "raw-html")
    minio_screenshot_bucket: str = os.getenv("MINIO_SCREENSHOT_BUCKET", "screenshots")
    minio_evidence_bucket: str = os.getenv("MINIO_EVIDENCE_BUCKET", "evidence")
    user_agent: str = os.getenv(
        "CRAWLER_USER_AGENT",
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36"
        ),
    )
    request_timeout_seconds: float = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
    scheduler_batch_size: int = int(os.getenv("SCHEDULER_BATCH_SIZE", "10"))
    scheduler_worker_limit: int = int(os.getenv("SCHEDULER_WORKER_LIMIT", "4"))
    lease_seconds: int = int(os.getenv("LEASE_SECONDS", "120"))


def load_settings() -> Settings:
    return Settings()
