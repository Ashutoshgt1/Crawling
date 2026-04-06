from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from psycopg import connect
from psycopg.rows import dict_row

from distributed_crawler.shared.config import Settings
from distributed_crawler.shared.scoring import score_url
from distributed_crawler.shared.types import FetchEnvelope, FrontierUrl, ParsedDocument, ScheduledBatch
from distributed_crawler.shared.url_utils import canonicalize_url, extract_host, guess_etld1, hash_url


def utc_now() -> datetime:
    return datetime.now(UTC)


def _merge_unique(existing: Any, incoming: list[str]) -> str:
    current = existing if isinstance(existing, list) else []
    merged: list[str] = []
    seen: set[str] = set()
    for value in [*current, *incoming]:
        if not value or value in seen:
            continue
        seen.add(value)
        merged.append(value)
    return json.dumps(merged)


def _confidence_details(observation: dict[str, Any]) -> str:
    return json.dumps(
        {
            "last_observation_confidence": observation.get("confidence", 0.5),
            "weighted_confidence": _weighted_confidence(observation),
            "extractor_version": observation.get("extractor_version"),
            "page_type": (observation.get("raw_fields") or {}).get("page_type"),
        }
    )


PAGE_TYPE_WEIGHTS = {
    "contact": 1.0,
    "about": 0.9,
    "leadership": 0.75,
    "careers": 0.55,
    "generic": 0.35,
}


def _page_type(observation: dict[str, Any]) -> str:
    return (observation.get("raw_fields") or {}).get("page_type") or "generic"


def _weighted_confidence(observation: dict[str, Any]) -> float:
    return observation.get("confidence", 0.5) * PAGE_TYPE_WEIGHTS.get(_page_type(observation), 0.35)


def _pick_field(existing_value: Any, existing_score: float, incoming_value: Any, incoming_score: float) -> Any:
    if not incoming_value:
        return existing_value
    if not existing_value:
        return incoming_value
    if incoming_score > existing_score:
        return incoming_value
    return existing_value


class FrontierRepository:
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
                            %(source_type)s, 0, %(priority)s, 'ready', 'http', now()
                        )
                        on conflict (url_hash) do nothing
                        """,
                        {
                            "canonical_url": canonical,
                            "url_hash": hash_url(canonical),
                            "scheme": canonical.split("://", 1)[0],
                            "host": host,
                            "etld1": guess_etld1(host),
                            "path": canonical.split(host, 1)[-1].split("?", 1)[0] or "/",
                            "query_norm": canonical.split("?", 1)[1] if "?" in canonical else "",
                            "source_type": source_type,
                            "priority": score_url(canonical, depth=0, source_type=source_type),
                        },
                    )
                    inserted += cur.rowcount
            conn.commit()
        return inserted

    def lease_ready_urls(self, limit: int, lease_seconds: int, render_mode: str = "http") -> list[FrontierUrl]:
        lease_until = utc_now() + timedelta(seconds=lease_seconds)
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    with candidates as (
                        select id
                        from urls
                        where state = 'ready'
                          and render_mode = %(render_mode)s
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
                    {
                        "render_mode": render_mode,
                        "limit": limit,
                        "lease_until": lease_until,
                    },
                )
                leased = [FrontierUrl(**row) for row in cur.fetchall()]
            conn.commit()
        return leased

    def ready_candidates(self, limit: int, render_mode: str = "http") -> list[FrontierUrl]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select id, canonical_url, host, render_mode, depth, priority, source_type
                    from urls
                    where state = 'ready'
                      and render_mode = %(render_mode)s
                      and coalesce(next_fetch_at, now()) <= now()
                    order by priority desc, id asc
                    limit %(limit)s
                    """,
                    {"render_mode": render_mode, "limit": limit},
                )
                rows = [FrontierUrl(**row) for row in cur.fetchall()]
        return rows

    def lease_specific_urls(self, frontiers: list[FrontierUrl], lease_seconds: int) -> list[FrontierUrl]:
        if not frontiers:
            return []
        ids = [frontier.id for frontier in frontiers]
        lease_until = utc_now() + timedelta(seconds=lease_seconds)
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    update urls
                    set state = 'leased',
                        leased_until = %(lease_until)s,
                        updated_at = now()
                    where id = any(%(ids)s)
                      and state = 'ready'
                    returning id, canonical_url, host, render_mode, depth, priority, source_type
                    """,
                    {"lease_until": lease_until, "ids": ids},
                )
                leased = [FrontierUrl(**row) for row in cur.fetchall()]
            conn.commit()
        return leased

    def schedule_ready_urls(self, limit: int, lease_seconds: int, render_mode: str = "http") -> ScheduledBatch:
        candidates = self.ready_candidates(limit * 5, render_mode=render_mode)
        if not candidates:
            return ScheduledBatch()
        budgets = self.get_domain_budgets([candidate.host for candidate in candidates])
        selected: list[FrontierUrl] = []
        deferred: list[FrontierUrl] = []
        promoted: list[FrontierUrl] = []
        per_host: dict[str, int] = {}

        for candidate in candidates:
            budget = budgets[candidate.host]
            if budget["preferred_render_mode"] == "browser" or budget["challenge_rate"] >= 0.2 or budget["ban_score"] >= 0.5:
                if candidate.render_mode != "browser":
                    promoted.append(candidate)
                    continue

            used = per_host.get(candidate.host, 0)
            if used >= budget["max_parallelism"] or len(selected) >= limit:
                deferred.append(candidate)
                continue

            per_host[candidate.host] = used + 1
            selected.append(candidate)

        if promoted:
            self.promote_urls_to_browser(promoted)
        leased = self.lease_specific_urls(selected, lease_seconds)
        return ScheduledBatch(leases=leased, deferred=deferred, promoted_to_browser=promoted)

    def get_domain_budgets(self, hosts: list[str]) -> dict[str, dict[str, Any]]:
        if not hosts:
            return {}
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select host, crawl_delay_ms, max_parallelism, preferred_render_mode, ban_score, challenge_rate
                    from domain_state
                    where host = any(%(hosts)s)
                    """,
                    {"hosts": hosts},
                )
                rows = cur.fetchall()
        budgets = {
            row["host"]: {
                "crawl_delay_ms": row["crawl_delay_ms"],
                "max_parallelism": row["max_parallelism"],
                "preferred_render_mode": row["preferred_render_mode"],
                "ban_score": row["ban_score"],
                "challenge_rate": row["challenge_rate"],
            }
            for row in rows
        }
        for host in hosts:
            budgets.setdefault(
                host,
                {
                    "crawl_delay_ms": self.settings.default_crawl_delay_ms,
                    "max_parallelism": self.settings.default_host_parallelism,
                    "preferred_render_mode": "http",
                    "ban_score": 0.0,
                    "challenge_rate": 0.0,
                },
            )
        return budgets

    def complete_fetch(self, envelope: FetchEnvelope, html_object_key: str | None, parsed: ParsedDocument | None) -> None:
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
                    {"id": envelope.frontier.id, "http_status": envelope.status_code},
                )
                cur.execute(
                    """
                    insert into fetch_results (
                        url_hash, worker_type, proxy_class, status_code, final_url, content_type,
                        response_bytes, latency_ms, blocked, block_reason, html_object_key, headers_json, meta_json
                    )
                    values (
                        %(url_hash)s, 'http', 'direct', %(status_code)s, %(final_url)s, %(content_type)s,
                        %(response_bytes)s, %(latency_ms)s, false, null, %(html_object_key)s, '{}'::jsonb, %(meta_json)s::jsonb
                    )
                    """,
                    {
                        "url_hash": hash_url(envelope.frontier.canonical_url),
                        "status_code": envelope.status_code,
                        "final_url": envelope.final_url,
                        "content_type": envelope.content_type,
                        "response_bytes": envelope.response_bytes,
                        "latency_ms": envelope.latency_ms,
                        "html_object_key": html_object_key,
                        "meta_json": json.dumps(parsed.metadata if parsed else {}),
                    },
                )
            conn.commit()

    def fail_fetch(self, frontier: FrontierUrl, error: str, retry_delay_seconds: int = 300) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    update urls
                    set state = 'ready',
                        retry_count = retry_count + 1,
                        next_fetch_at = now() + make_interval(secs => %(delay)s),
                        leased_until = null,
                        updated_at = now()
                    where id = %(id)s
                    """,
                    {"id": frontier.id, "delay": retry_delay_seconds},
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
                        "url_hash": hash_url(frontier.canonical_url),
                        "reason": error,
                        "meta_json": json.dumps({"error": error}),
                    },
                )
            conn.commit()

    def promote_urls_to_browser(self, frontiers: list[FrontierUrl]) -> None:
        if not frontiers:
            return
        with self.connection() as conn:
            with conn.cursor() as cur:
                for frontier in frontiers:
                    cur.execute(
                        """
                        update urls
                        set render_mode = 'browser',
                            state = 'ready',
                            leased_until = null,
                            next_fetch_at = now(),
                            updated_at = now()
                        where id = %(id)s
                        """,
                        {"id": frontier.id},
                    )
            conn.commit()

    def add_discovered_urls(self, urls: list[str], source_type: str, depth: int) -> int:
        inserted = 0
        with self.connection() as conn:
            with conn.cursor() as cur:
                for raw_url in urls:
                    canonical = canonicalize_url(raw_url)
                    host = extract_host(canonical)
                    lowered = canonical.lower()
                    render_mode = "browser" if any(
                        hint in canonical.lower()
                        for hint in ("login", "signup", "account", "dashboard", "app.")
                    ) else "http"
                    derived_source_type = source_type
                    if source_type == "discovered_link":
                        if any(token in lowered for token in ("/contact", "/contact-us", "/reach-us", "/locations")):
                            derived_source_type = "important_contact_page"
                        elif any(token in lowered for token in ("/about", "/about-us", "/company", "/overview")):
                            derived_source_type = "important_about_page"
                        elif any(token in lowered for token in ("/leadership", "/management", "/team", "/board")):
                            derived_source_type = "important_leadership_page"
                    cur.execute(
                        """
                        insert into urls (
                            canonical_url, url_hash, scheme, host, etld1, path, query_norm,
                            source_type, depth, priority, state, render_mode, next_fetch_at
                        )
                        values (
                            %(canonical_url)s, %(url_hash)s, %(scheme)s, %(host)s, %(etld1)s, %(path)s, %(query_norm)s,
                            %(source_type)s, %(depth)s, %(priority)s, 'ready', %(render_mode)s, now()
                        )
                        on conflict (url_hash) do nothing
                        """,
                        {
                            "canonical_url": canonical,
                            "url_hash": hash_url(canonical),
                            "scheme": canonical.split("://", 1)[0],
                            "host": host,
                            "etld1": guess_etld1(host),
                            "path": canonical.split(host, 1)[-1].split("?", 1)[0] or "/",
                            "query_norm": canonical.split("?", 1)[1] if "?" in canonical else "",
                            "source_type": derived_source_type,
                            "depth": depth,
                            "priority": score_url(canonical, depth=depth, source_type=derived_source_type),
                            "render_mode": render_mode,
                        },
                    )
                    inserted += cur.rowcount
            conn.commit()
        return inserted

    def write_observation_and_resolve(self, source_url: str, parsed: ParsedDocument, evidence_key: str | None) -> int | None:
        observation = parsed.observation
        if not observation:
            return None
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into observations (
                        url_hash, company_name, website, email, phone, address, city, state_region,
                        country, postal_code, industry, description, social_links, raw_fields,
                        confidence, extractor_version, evidence_object_key, page_type
                    )
                    values (
                        %(url_hash)s, %(company_name)s, %(website)s, %(email)s, %(phone)s, %(address)s, %(city)s, %(state_region)s,
                        %(country)s, %(postal_code)s, %(industry)s, %(description)s, %(social_links)s::jsonb, %(raw_fields)s::jsonb,
                        %(confidence)s, %(extractor_version)s, %(evidence_object_key)s, %(page_type)s
                    )
                    returning id
                    """,
                    {
                        "url_hash": hash_url(source_url),
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
                        "extractor_version": observation.get("extractor_version", "heuristic-v1"),
                        "evidence_object_key": evidence_key,
                        "page_type": (observation.get("raw_fields") or {}).get("page_type"),
                    },
                )
                observation_id = cur.fetchone()["id"]

                primary_domain = extract_host(observation["website"]) if observation.get("website") else None
                cur.execute(
                    """
                    select id
                    from companies
                    where (primary_domain is not null and primary_domain = %(primary_domain)s)
                       or (canonical_name is not null and canonical_name = %(canonical_name)s)
                    order by id asc
                    limit 1
                    """,
                    {
                        "primary_domain": primary_domain,
                        "canonical_name": observation.get("company_name"),
                    },
                )
                company = cur.fetchone()
                if company:
                    company_id = company["id"]
                    cur.execute(
                        """
                        select canonical_name, primary_domain, primary_email, primary_phone, primary_address,
                               city, state_region, country, postal_code, industry, description,
                               social_profiles, source_urls, confidence_details
                        from companies
                        where id = %(id)s
                        """,
                        {"id": company_id},
                    )
                    existing = cur.fetchone()
                    existing_score = float((existing.get("confidence_details") or {}).get("weighted_confidence", 0.0))
                    incoming_score = _weighted_confidence(observation)
                    selected_name = _pick_field(existing.get("canonical_name"), existing_score, observation.get("company_name"), incoming_score)
                    selected_email = _pick_field(existing.get("primary_email"), existing_score, observation.get("email"), incoming_score)
                    selected_phone = _pick_field(existing.get("primary_phone"), existing_score, observation.get("phone"), incoming_score)
                    selected_address = _pick_field(existing.get("primary_address"), existing_score, observation.get("address"), incoming_score)
                    selected_city = _pick_field(existing.get("city"), existing_score, observation.get("city"), incoming_score)
                    selected_state = _pick_field(existing.get("state_region"), existing_score, observation.get("state_region"), incoming_score)
                    selected_country = _pick_field(existing.get("country"), existing_score, observation.get("country"), incoming_score)
                    selected_postal = _pick_field(existing.get("postal_code"), existing_score, observation.get("postal_code"), incoming_score)
                    selected_industry = _pick_field(existing.get("industry"), existing_score, observation.get("industry"), incoming_score)
                    selected_description = _pick_field(existing.get("description"), existing_score, observation.get("description"), incoming_score)
                    cur.execute(
                        """
                        update companies
                        set canonical_name = %(canonical_name)s,
                            primary_domain = coalesce(%(primary_domain)s, primary_domain),
                            primary_email = %(primary_email)s,
                            primary_phone = %(primary_phone)s,
                            primary_address = %(primary_address)s,
                            city = %(city)s,
                            state_region = %(state_region)s,
                            country = %(country)s,
                            postal_code = %(postal_code)s,
                            industry = %(industry)s,
                            description = %(description)s,
                            social_profiles = %(social_profiles)s::jsonb,
                            source_urls = %(source_urls)s::jsonb,
                            confidence = greatest(confidence, %(confidence)s),
                            confidence_details = %(confidence_details)s::jsonb,
                            last_seen_at = now(),
                            updated_at = now()
                        where id = %(id)s
                        """,
                        {
                            "id": company_id,
                            "canonical_name": selected_name,
                            "primary_domain": primary_domain,
                            "primary_email": selected_email,
                            "primary_phone": selected_phone,
                            "primary_address": selected_address,
                            "city": selected_city,
                            "state_region": selected_state,
                            "country": selected_country,
                            "postal_code": selected_postal,
                            "industry": selected_industry,
                            "description": selected_description,
                            "social_profiles": _merge_unique(
                                self._get_company_json(cur, company_id, "social_profiles"),
                                observation.get("social_links") or [],
                            ),
                            "source_urls": _merge_unique(
                                self._get_company_json(cur, company_id, "source_urls"),
                                [source_url],
                            ),
                            "confidence": incoming_score,
                            "confidence_details": _confidence_details(observation),
                        },
                    )
                else:
                    cur.execute(
                        """
                        insert into companies (
                            canonical_name, primary_domain, primary_email, primary_phone, primary_address,
                            city, state_region, country, postal_code, industry, description, social_profiles,
                            source_urls, confidence, confidence_details
                        )
                        values (
                            %(canonical_name)s, %(primary_domain)s, %(primary_email)s, %(primary_phone)s, %(primary_address)s,
                            %(city)s, %(state_region)s, %(country)s, %(postal_code)s, %(industry)s, %(description)s,
                            %(social_profiles)s::jsonb, %(source_urls)s::jsonb, %(confidence)s, %(confidence_details)s::jsonb
                        )
                        returning id
                        """,
                        {
                            "canonical_name": observation.get("company_name") or observation.get("website") or source_url,
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
                            "social_profiles": json.dumps(observation.get("social_links") or []),
                            "source_urls": json.dumps([source_url]),
                            "confidence": observation.get("confidence", 0.5),
                            "confidence_details": _confidence_details(observation),
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
                        "match_reason": json.dumps({"resolver": "starter-merge-v1"}),
                    },
                )
            conn.commit()
        return observation_id

    def _get_company_json(self, cur, company_id: int, field_name: str) -> Any:
        cur.execute(f"select {field_name} from companies where id = %(id)s", {"id": company_id})
        row = cur.fetchone()
        return row[field_name] if row else None
