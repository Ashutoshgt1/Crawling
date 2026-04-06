from __future__ import annotations

import argparse
import asyncio
import time
from pathlib import Path
import sys

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from distributed_crawler.shared.artifacts import ArtifactStore
from distributed_crawler.shared.config import load_settings
from distributed_crawler.shared.kafka import FETCH_RESULTS_TOPIC, maybe_start_producer
from distributed_crawler.shared.parser import parse_document
from distributed_crawler.shared.repository import FrontierRepository
from distributed_crawler.shared.types import FetchEnvelope, FrontierUrl


async def fetch_one(
    session: aiohttp.ClientSession,
    frontier: FrontierUrl,
    timeout_seconds: float,
) -> FetchEnvelope:
    started = time.perf_counter()
    try:
        async with session.get(frontier.canonical_url, timeout=timeout_seconds, allow_redirects=True) as response:
            html = await response.text(errors="replace")
            return FetchEnvelope(
                frontier=frontier,
                status_code=response.status,
                final_url=str(response.url),
                content_type=response.headers.get("Content-Type"),
                response_bytes=len(html.encode("utf-8")),
                latency_ms=int((time.perf_counter() - started) * 1000),
                html=html,
            )
    except Exception as exc:  # noqa: BLE001
        return FetchEnvelope(
            frontier=frontier,
            status_code=None,
            final_url=frontier.canonical_url,
            content_type=None,
            response_bytes=0,
            latency_ms=int((time.perf_counter() - started) * 1000),
            html=None,
            error=str(exc),
        )


async def process_batch(batch_size: int, concurrency: int) -> None:
    settings = load_settings()
    repository = FrontierRepository(settings)
    artifact_store = ArtifactStore(settings)
    scheduled = repository.schedule_ready_urls(batch_size, settings.lease_seconds, render_mode="http")
    leases = scheduled.leases
    if not leases:
        print("[http-fetch] no ready URLs available")
        return

    connector = aiohttp.TCPConnector(limit=concurrency, limit_per_host=settings.default_host_parallelism, ssl=False)
    timeout = aiohttp.ClientTimeout(total=settings.request_timeout_seconds)
    headers = {"User-Agent": settings.user_agent}
    producer = await maybe_start_producer(settings)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers) as session:
        tasks = [fetch_one(session, lease, settings.request_timeout_seconds) for lease in leases]
        for envelope in await asyncio.gather(*tasks):
            if envelope.error or not envelope.html:
                repository.fail_fetch(envelope.frontier, error=envelope.error or "empty response")
                print(f"[http-fetch] failed url_id={envelope.frontier.id} error={envelope.error}")
                continue

            html_key = f"{envelope.frontier.host}/{envelope.frontier.id}.html"
            artifact_store.put_html(html_key, envelope.html)
            parsed = None if producer else parse_document(envelope.final_url, envelope.html, settings.max_links_per_page)
            repository.complete_fetch(envelope, html_key, parsed)
            if producer:
                await producer.send(
                    FETCH_RESULTS_TOPIC,
                    {
                        "frontier_id": envelope.frontier.id,
                        "canonical_url": envelope.frontier.canonical_url,
                        "host": envelope.frontier.host,
                        "depth": envelope.frontier.depth,
                        "final_url": envelope.final_url,
                        "html_object_key": html_key,
                        "status_code": envelope.status_code,
                        "content_type": envelope.content_type,
                    },
                )
            else:
                if parsed.discovered_links:
                    repository.add_discovered_urls(
                        parsed.discovered_links,
                        source_type="discovered_link",
                        depth=envelope.frontier.depth + 1,
                    )
                if parsed.observation:
                    evidence_key = artifact_store.put_json(
                        f"{envelope.frontier.host}/{envelope.frontier.id}.json",
                        {"observation": parsed.observation, "metadata": parsed.metadata},
                    )
                    repository.write_observation_and_resolve(envelope.final_url, parsed, evidence_key)
            print(
                f"[http-fetch] ok url_id={envelope.frontier.id} status={envelope.status_code} "
                f"kafka={'on' if producer else 'off'} "
                f"company={(parsed.observation.get('company_name') if parsed and parsed.observation else None)}"
            )
    if producer:
        await producer.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Async HTTP fetch worker for distributed crawler.")
    parser.add_argument("--batch-size", type=int, default=None, help="Number of URLs to lease this pass.")
    parser.add_argument("--concurrency", type=int, default=None, help="Parallel HTTP requests.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_settings()
    batch_size = args.batch_size or settings.scheduler_batch_size
    concurrency = args.concurrency or settings.http_concurrency
    asyncio.run(process_batch(batch_size, concurrency))


if __name__ == "__main__":
    main()
