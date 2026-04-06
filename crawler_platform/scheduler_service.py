from __future__ import annotations

import argparse
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from crawler_platform.config import load_settings
from crawler_platform.parser_resolver import parse_html
from crawler_platform.storage import ArtifactStore, LeasedUrl, PostgresStorage


def fetch_url(url: str, user_agent: str, timeout_seconds: float) -> tuple[str, dict[str, Any]]:
    started = time.perf_counter()
    request = Request(url, headers={"User-Agent": user_agent})
    with urlopen(request, timeout=timeout_seconds) as response:
        html = response.read().decode("utf-8", errors="replace")
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return html, {
            "status_code": response.status,
            "final_url": response.geturl(),
            "content_type": response.headers.get("Content-Type"),
            "response_bytes": len(html.encode("utf-8")),
            "latency_ms": elapsed_ms,
        }


def process_lease(
    lease: LeasedUrl,
    storage: PostgresStorage,
    artifact_store: ArtifactStore,
    settings,
) -> None:
    try:
        html, fetch_meta = fetch_url(
            lease.canonical_url,
            user_agent=settings.user_agent,
            timeout_seconds=settings.request_timeout_seconds,
        )
        html_key = f"{lease.host}/{lease.id}.html"
        artifact_store.put_html(html_key, html)
        observation, links, parser_meta = parse_html(fetch_meta["final_url"], html)
        storage.mark_fetch_complete(
            lease,
            status_code=fetch_meta["status_code"],
            final_url=fetch_meta["final_url"],
            content_type=fetch_meta["content_type"],
            response_bytes=fetch_meta["response_bytes"],
            latency_ms=fetch_meta["latency_ms"],
            html_object_key=html_key,
            metadata={"fetch": fetch_meta, "parser": parser_meta},
        )
        if links:
            storage.add_discovered_urls(links[:50], source_type="discovered_link", depth=lease.depth + 1)
        if observation and (observation.get("company_name") or observation.get("email") or observation.get("phone")):
            evidence_key = artifact_store.put_json(
                settings.minio_evidence_bucket,
                f"{lease.host}/{lease.id}.json",
                {"observation": observation, "parser_meta": parser_meta},
            )
            storage.write_observation_and_resolve(fetch_meta["final_url"], observation, evidence_key)
        print(
            f"[scheduler] fetched url_id={lease.id} status={fetch_meta['status_code']} "
            f"links={min(len(links), 50)} company={observation.get('company_name') if observation else None}"
        )
    except HTTPError as exc:
        storage.mark_fetch_failed(lease, reason=f"HTTP {exc.code}")
        print(f"[scheduler] failed url_id={lease.id} reason=HTTP {exc.code}")
    except URLError as exc:
        storage.mark_fetch_failed(lease, reason=f"network error: {exc.reason}")
        print(f"[scheduler] failed url_id={lease.id} reason=network error")
    except Exception as exc:  # noqa: BLE001
        storage.mark_fetch_failed(lease, reason=str(exc))
        print(f"[scheduler] failed url_id={lease.id} reason={exc}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lease ready URLs and process them.")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Number of URLs to lease in one scheduler pass.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_settings()
    storage = PostgresStorage(settings)
    artifact_store = ArtifactStore(settings)
    batch_size = args.batch_size or settings.scheduler_batch_size
    leases = storage.lease_ready_urls(batch_size, settings.lease_seconds)
    if not leases:
        print("[scheduler] no ready URLs available")
        return
    for lease in leases:
        process_lease(lease, storage, artifact_store, settings)


if __name__ == "__main__":
    main()
