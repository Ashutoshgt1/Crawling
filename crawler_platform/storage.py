from __future__ import annotations

import io
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from minio import Minio
from psycopg import connect
from psycopg.rows import dict_row

from crawler_platform.config import Settings
from crawler_platform.url_utils import canonicalize_url, extract_host, guess_etld1, url_hash


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(slots=True)
class LeasedUrl:
    id: int
    canonical_url: str
    host: str
    render_mode: str
    depth: int
    priority: float
    source_type: str | None


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

    def put_json(self, bucket: str, key: str, value: dict[str, Any]) -> str:
        payload = json.dumps(value, ensure_ascii=False).encode("utf-8")
        self.client.put_object(
            bucket,
            key,
            io.BytesIO(payload),
            len(payload),
            content_type="application/json",
        )
        return key


class PostgresStorage:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def connection(self):
        return connect(self.settings.postgres_dsn, row_factory=dict_row)

    def seed_urls(self, urls: list[str], source_type: str = "manual_seed") -> int:
        inserted = 0
        with self.connection() as conn:
            with conn.cursor() as cur:
                for raw_url in urls:
                    canonical = canonicalize_url(raw_url)
                    host = extract_host(canonical)
                    cur.execute(
                        """
                        insert into urls (
                            canonical_url, url_hash, scheme, host, etld1, path, query_norm,
                            source_type, depth, priority, state, render_mode, next_fetch_at
                        )
                        values (
                            %(canonical_url)s, %(url_hash)s, %(scheme)s, %(host)s, %(etld1)s, %(path)s, %(query_norm)s,
                            %(source_type)s, 0, 100, 'ready', 'http', now()
                        )
                        on conflict (url_hash) do nothing
                        """,
                        {
                            "canonical_url": canonical,
                            "url_hash": url_hash(canonical),
                            "scheme": canonical.split("://", 1)[0],
                            "host": host,
                            "etld1": guess_etld1(host),
                            "path": canonical.split(host, 1)[-1].split("?", 1)[0] or "/",
                            "query_norm": canonical.split("?", 1)[1] if "?" in canonical else "",
                            "source_type": source_type,
                        },
                    )
                    inserted += cur.rowcount
            conn.commit()
        return inserted

    def lease_ready_urls(self, limit: int, lease_seconds: int) -> list[LeasedUrl]:
        lease_until = utc_now() + timedelta(seconds=lease_seconds)
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    with candidates as (
                        select id
                        from urls
                        where state = 'ready'
                          and coalesce(next_fetch_at, now()) <= now()
                        order by priority desc, id asc
                        limit %(limit)s
                        for update skip locked
                    )
                    update urls u
                    set state = 'leased',
                        leased_until = %(lease_until)s,
                        updated_at = now()
                    from candidates
                    where u.id = candidates.id
                    returning u.id, u.canonical_url, u.host, u.render_mode, u.depth, u.priority, u.source_type
                    """,
                    {"limit": limit, "lease_until": lease_until},
                )
                leased = [LeasedUrl(**row) for row in cur.fetchall()]
            conn.commit()
        return leased

    def mark_fetch_complete(
        self,
        lease: LeasedUrl,
        *,
        status_code: int | None,
        final_url: str,
        content_type: str | None,
        response_bytes: int,
        latency_ms: int,
        html_object_key: str | None,
        metadata: dict[str, Any],
    ) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    update urls
                    set state = 'fetched',
                        leased_until = null,
                        last_fetched_at = now(),
                        http_status = %(http_status)s,
                        updated_at = now()
                    where id = %(id)s
                    """,
                    {"http_status": status_code, "id": lease.id},
                )
                cur.execute(
                    """
                    insert into fetch_results (
                        url_hash, worker_type, proxy_class, status_code, final_url, content_type,
                        response_bytes, latency_ms, blocked, block_reason, html_object_key, headers_json, meta_json
                    )
                    values (
                        %(url_hash)s, 'http', 'direct', %(status_code)s, %(final_url)s, %(content_type)s,
                        %(response_bytes)s, %(latency_ms)s, false, null, %(html_object_key)s,
                        '{}'::jsonb, %(meta_json)s::jsonb
                    )
                    """,
                    {
                        "url_hash": url_hash(lease.canonical_url),
                        "status_code": status_code,
                        "final_url": final_url,
                        "content_type": content_type,
                        "response_bytes": response_bytes,
                        "latency_ms": latency_ms,
                        "html_object_key": html_object_key,
                        "meta_json": json.dumps(metadata),
                    },
                )
            conn.commit()

    def mark_fetch_failed(self, lease: LeasedUrl, reason: str, retry_delay_seconds: int = 300) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    update urls
                    set state = 'ready',
                        retry_count = retry_count + 1,
                        next_fetch_at = now() + make_interval(secs => %(retry_delay_seconds)s),
                        leased_until = null,
                        updated_at = now()
                    where id = %(id)s
                    """,
                    {"id": lease.id, "retry_delay_seconds": retry_delay_seconds},
                )
                cur.execute(
                    """
                    insert into fetch_results (
                        url_hash, worker_type, proxy_class, blocked, block_reason, meta_json
                    )
                    values (
                        %(url_hash)s, 'http', 'direct', false, %(reason)s, %(meta_json)s::jsonb
                    )
                    """,
                    {
                        "url_hash": url_hash(lease.canonical_url),
                        "reason": reason,
                        "meta_json": json.dumps({"error": reason}),
                    },
                )
            conn.commit()

    def add_discovered_urls(self, urls: list[str], source_type: str, depth: int) -> int:
        inserted = 0
        with self.connection() as conn:
            with conn.cursor() as cur:
                for discovered in urls:
                    canonical = canonicalize_url(discovered)
                    host = extract_host(canonical)
                    cur.execute(
                        """
                        insert into urls (
                            canonical_url, url_hash, scheme, host, etld1, path, query_norm,
                            source_type, depth, priority, state, render_mode, next_fetch_at
                        )
                        values (
                            %(canonical_url)s, %(url_hash)s, %(scheme)s, %(host)s, %(etld1)s, %(path)s, %(query_norm)s,
                            %(source_type)s, %(depth)s, %(priority)s, 'ready', 'http', now()
                        )
                        on conflict (url_hash) do nothing
                        """,
                        {
                            "canonical_url": canonical,
                            "url_hash": url_hash(canonical),
                            "scheme": canonical.split("://", 1)[0],
                            "host": host,
                            "etld1": guess_etld1(host),
                            "path": canonical.split(host, 1)[-1].split("?", 1)[0] or "/",
                            "query_norm": canonical.split("?", 1)[1] if "?" in canonical else "",
                            "source_type": source_type,
                            "depth": depth,
                            "priority": max(10, 100 - depth * 5),
                        },
                    )
                    inserted += cur.rowcount
            conn.commit()
        return inserted

    def write_observation_and_resolve(
        self,
        source_url: str,
        observation: dict[str, Any],
        evidence_object_key: str | None,
    ) -> int:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into observations (
                        url_hash, company_name, website, email, phone, address, city, state_region,
                        country, postal_code, industry, description, social_links, raw_fields,
                        confidence, extractor_version, evidence_object_key
                    )
                    values (
                        %(url_hash)s, %(company_name)s, %(website)s, %(email)s, %(phone)s, %(address)s, %(city)s, %(state_region)s,
                        %(country)s, %(postal_code)s, %(industry)s, %(description)s, %(social_links)s::jsonb, %(raw_fields)s::jsonb,
                        %(confidence)s, %(extractor_version)s, %(evidence_object_key)s
                    )
                    returning id
                    """,
                    {
                        "url_hash": url_hash(source_url),
                        "company_name": observation.get("company_name"),
                        "website": observation.get("website"),
                        "email": observation.get("email"),
                        "phone": observation.get("phone"),
                        "address": observation.get("address"),
                        "city": observation.get("city"),
                        "state_region": observation.get("state_region"),
                        "country": observation.get("country"),
                        "postal_code": observation.get("postal_code"),
                        "industry": observation.get("industry"),
                        "description": observation.get("description"),
                        "social_links": json.dumps(observation.get("social_links") or []),
                        "raw_fields": json.dumps(observation.get("raw_fields") or {}),
                        "confidence": observation.get("confidence", 0.5),
                        "extractor_version": observation.get("extractor_version", "starter-v1"),
                        "evidence_object_key": evidence_object_key,
                    },
                )
                observation_id = cur.fetchone()["id"]

                company_name = observation.get("company_name")
                website = observation.get("website")
                primary_domain = extract_host(website) if website else None
                cur.execute(
                    """
                    select id
                    from companies
                    where (primary_domain is not null and primary_domain = %(primary_domain)s)
                       or (canonical_name is not null and canonical_name = %(canonical_name)s)
                    order by id asc
                    limit 1
                    """,
                    {"primary_domain": primary_domain, "canonical_name": company_name},
                )
                company = cur.fetchone()
                if company:
                    company_id = company["id"]
                    cur.execute(
                        """
                        update companies
                        set canonical_name = coalesce(%(canonical_name)s, canonical_name),
                            primary_domain = coalesce(%(primary_domain)s, primary_domain),
                            primary_email = coalesce(%(primary_email)s, primary_email),
                            primary_phone = coalesce(%(primary_phone)s, primary_phone),
                            primary_address = coalesce(%(primary_address)s, primary_address),
                            city = coalesce(%(city)s, city),
                            state_region = coalesce(%(state_region)s, state_region),
                            country = coalesce(%(country)s, country),
                            postal_code = coalesce(%(postal_code)s, postal_code),
                            industry = coalesce(%(industry)s, industry),
                            description = coalesce(%(description)s, description),
                            confidence = greatest(confidence, %(confidence)s),
                            last_seen_at = now(),
                            updated_at = now()
                        where id = %(id)s
                        """,
                        {
                            "id": company_id,
                            "canonical_name": company_name,
                            "primary_domain": primary_domain,
                            "primary_email": observation.get("email"),
                            "primary_phone": observation.get("phone"),
                            "primary_address": observation.get("address"),
                            "city": observation.get("city"),
                            "state_region": observation.get("state_region"),
                            "country": observation.get("country"),
                            "postal_code": observation.get("postal_code"),
                            "industry": observation.get("industry"),
                            "description": observation.get("description"),
                            "confidence": observation.get("confidence", 0.5),
                        },
                    )
                else:
                    cur.execute(
                        """
                        insert into companies (
                            canonical_name, primary_domain, primary_email, primary_phone, primary_address,
                            city, state_region, country, postal_code, industry, description, confidence
                        )
                        values (
                            %(canonical_name)s, %(primary_domain)s, %(primary_email)s, %(primary_phone)s, %(primary_address)s,
                            %(city)s, %(state_region)s, %(country)s, %(postal_code)s, %(industry)s, %(description)s, %(confidence)s
                        )
                        returning id
                        """,
                        {
                            "canonical_name": company_name or website or source_url,
                            "primary_domain": primary_domain,
                            "primary_email": observation.get("email"),
                            "primary_phone": observation.get("phone"),
                            "primary_address": observation.get("address"),
                            "city": observation.get("city"),
                            "state_region": observation.get("state_region"),
                            "country": observation.get("country"),
                            "postal_code": observation.get("postal_code"),
                            "industry": observation.get("industry"),
                            "description": observation.get("description"),
                            "confidence": observation.get("confidence", 0.5),
                        },
                    )
                    company_id = cur.fetchone()["id"]

                cur.execute(
                    """
                    insert into company_observation_links (company_id, observation_id, match_score, match_reason)
                    values (%(company_id)s, %(observation_id)s, %(match_score)s, %(match_reason)s::jsonb)
                    on conflict do nothing
                    """,
                    {
                        "company_id": company_id,
                        "observation_id": observation_id,
                        "match_score": observation.get("confidence", 0.5),
                        "match_reason": json.dumps({"resolver": "starter-v1"}),
                    },
                )
            conn.commit()
        return observation_id
