from __future__ import annotations

import asyncio
import json
from typing import Any

from distributed_crawler.shared.config import Settings

try:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
except Exception:  # noqa: BLE001
    AIOKafkaConsumer = None
    AIOKafkaProducer = None


FETCH_RESULTS_TOPIC = "fetch_results"
PARSED_DOCUMENTS_TOPIC = "parsed_documents"


class KafkaUnavailableError(RuntimeError):
    pass


class KafkaJsonProducer:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        if AIOKafkaProducer is None:
            raise KafkaUnavailableError("aiokafka is not installed")
        self._producer = AIOKafkaProducer(bootstrap_servers=self.settings.kafka_bootstrap_servers)
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def send(self, topic: str, payload: dict[str, Any]) -> None:
        if self._producer is None:
            raise KafkaUnavailableError("producer is not started")
        await self._producer.send_and_wait(topic, json.dumps(payload, ensure_ascii=False).encode("utf-8"))


class KafkaJsonConsumer:
    def __init__(self, settings: Settings, topic: str, group_id: str) -> None:
        self.settings = settings
        self.topic = topic
        self.group_id = group_id
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        if AIOKafkaConsumer is None:
            raise KafkaUnavailableError("aiokafka is not installed")
        self._consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
        )
        await self._consumer.start()

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

    async def next_json(self) -> dict[str, Any]:
        if self._consumer is None:
            raise KafkaUnavailableError("consumer is not started")
        message = await self._consumer.getone()
        return json.loads(message.value.decode("utf-8"))


async def maybe_start_producer(settings: Settings) -> KafkaJsonProducer | None:
    if not settings.kafka_enabled:
        return None
    producer = KafkaJsonProducer(settings)
    await producer.start()
    return producer


def run_async(coro):
    return asyncio.run(coro)
