from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from distributed_crawler.shared.artifacts import ArtifactStore
from distributed_crawler.shared.config import load_settings
from distributed_crawler.shared.kafka import (
    FETCH_RESULTS_TOPIC,
    PARSED_DOCUMENTS_TOPIC,
    KafkaJsonConsumer,
    maybe_start_producer,
)
from distributed_crawler.shared.parser import parse_document
from distributed_crawler.shared.repository import FrontierRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parser/resolver stage for distributed crawler.")
    parser.add_argument("--mode", choices=("parse", "resolve"), default="parse")
    return parser.parse_args()


async def run_parse_stage() -> None:
    settings = load_settings()
    if not settings.kafka_enabled:
        print("[parser-resolver] Kafka is disabled. Use inline parsing via the HTTP worker or enable Kafka.")
        return
    artifact_store = ArtifactStore(settings)
    consumer = KafkaJsonConsumer(settings, FETCH_RESULTS_TOPIC, group_id="parser-stage")
    producer = await maybe_start_producer(settings)
    await consumer.start()
    try:
        while True:
            message = await consumer.next_json()
            html = artifact_store.get_html(message["html_object_key"])
            parsed = parse_document(message["final_url"], html, settings.max_links_per_page)
            if producer:
                await producer.send(
                    PARSED_DOCUMENTS_TOPIC,
                    {
                        "frontier_id": message["frontier_id"],
                        "final_url": message["final_url"],
                        "host": message["host"],
                        "depth": message["depth"],
                        "parsed": {
                            "observation": parsed.observation,
                            "discovered_links": parsed.discovered_links,
                            "metadata": parsed.metadata,
                        },
                    },
                )
            print(
                f"[parser] parsed frontier_id={message['frontier_id']} "
                f"links={len(parsed.discovered_links)} company={parsed.observation.get('company_name') if parsed.observation else None}"
            )
    finally:
        await consumer.stop()
        if producer:
            await producer.stop()


async def run_resolve_stage() -> None:
    settings = load_settings()
    if not settings.kafka_enabled:
        print("[parser-resolver] Kafka is disabled. Resolve happens inline in fallback mode.")
        return
    repository = FrontierRepository(settings)
    artifact_store = ArtifactStore(settings)
    consumer = KafkaJsonConsumer(settings, PARSED_DOCUMENTS_TOPIC, group_id="resolver-stage")
    await consumer.start()
    try:
        while True:
            message = await consumer.next_json()
            parsed_payload = message["parsed"]
            repository.add_discovered_urls(
                parsed_payload.get("discovered_links") or [],
                source_type="discovered_link",
                depth=message["depth"] + 1,
            )
            observation = parsed_payload.get("observation")
            if observation:
                evidence_key = artifact_store.put_json(
                    f"{message['host']}/{message['frontier_id']}.json",
                    {"observation": observation, "metadata": parsed_payload.get("metadata") or {}},
                )
                from distributed_crawler.shared.types import ParsedDocument

                repository.write_observation_and_resolve(
                    message["final_url"],
                    ParsedDocument(
                        observation=observation,
                        discovered_links=parsed_payload.get("discovered_links") or [],
                        metadata=parsed_payload.get("metadata") or {},
                    ),
                    evidence_key,
                )
            print(
                f"[resolver] resolved frontier_id={message['frontier_id']} "
                f"company={observation.get('company_name') if observation else None}"
            )
    finally:
        await consumer.stop()


def main() -> None:
    args = parse_args()
    if args.mode == "parse":
        asyncio.run(run_parse_stage())
        return
    asyncio.run(run_resolve_stage())


if __name__ == "__main__":
    main()
