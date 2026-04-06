from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class FrontierUrl:
    id: int
    canonical_url: str
    host: str
    render_mode: str
    depth: int
    priority: float
    source_type: str | None


@dataclass(slots=True)
class FetchEnvelope:
    frontier: FrontierUrl
    status_code: int | None
    final_url: str
    content_type: str | None
    response_bytes: int
    latency_ms: int
    html: str | None
    error: str | None = None


@dataclass(slots=True)
class ParsedDocument:
    observation: dict[str, Any] | None
    discovered_links: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ScheduledBatch:
    leases: list[FrontierUrl] = field(default_factory=list)
    deferred: list[FrontierUrl] = field(default_factory=list)
    promoted_to_browser: list[FrontierUrl] = field(default_factory=list)
