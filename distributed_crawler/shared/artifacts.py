from __future__ import annotations

import io
import json
from typing import Any

from minio import Minio

from distributed_crawler.shared.config import Settings


class ArtifactStore:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        )

    def put_html(self, key: str, html: str) -> str:
        payload = html.encode("utf-8")
        self.client.put_object(
            self.settings.minio_raw_bucket,
            key,
            io.BytesIO(payload),
            len(payload),
            content_type="text/html; charset=utf-8",
        )
        return key

    def put_json(self, key: str, value: dict[str, Any]) -> str:
        payload = json.dumps(value, ensure_ascii=False).encode("utf-8")
        self.client.put_object(
            self.settings.minio_evidence_bucket,
            key,
            io.BytesIO(payload),
            len(payload),
            content_type="application/json",
        )
        return key

    def get_html(self, key: str) -> str:
        response = self.client.get_object(self.settings.minio_raw_bucket, key)
        try:
            return response.read().decode("utf-8", errors="replace")
        finally:
            response.close()
            response.release_conn()
