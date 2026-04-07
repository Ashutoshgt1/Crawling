import argparse
import asyncio
import json
import os
import random
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import mysql.connector
from mysql.connector import MySQLConnection
from playwright.async_api import async_playwright

from crawler import (
    ProxyConfig,
    QueryTask,
    build_browser,
    build_context,
    load_proxies,
    process_query,
)


@dataclass
class MysqlSettings:
    host: str
    port: int
    user: str
    password: str
    database: str
    input_table: str
    output_table: str


@dataclass
class InputCompany:
    matched_keyword: str | None
    raw_company_name: str
    search_company_name: str


ORG_HINTS = (
    "private limited",
    "pvt ltd",
    "limited",
    "llp",
    "transport",
    "roadways",
    "travels",
    "cargo",
    "logistics",
    "packers",
    "movers",
    "express",
    "enterprise",
    "enterprises",
    "solutions",
    "corporation",
    "corp",
    "agency",
    "associates",
)
PARENTHETICAL_NOISE_TERMS = (
    "online tickets booking",
    "ticket booking",
    "booking",
    "online booking",
)


NUMBERED_ITEM_RE = re.compile(r"(?:^|\s)(\d+)\)\s*")
BLOCK_SIGNAL_PATTERNS = (
    "captcha",
    "recaptcha",
    "sorry",
    "forbidden",
    "access denied",
    "blocked",
    "verify you are human",
    "unusual traffic",
    "temporarily unavailable",
    "sign in",
    "robot",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read company_name values from MySQL and save crawler JSON output back to MySQL."
    )
    parser.add_argument("--host", default=os.getenv("MYSQL_HOST", "192.168.1.133"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    parser.add_argument("--user", default=os.getenv("MYSQL_USER", "crawler"))
    parser.add_argument("--password", default=os.getenv("MYSQL_PASSWORD", ""))
    parser.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "master"))
    parser.add_argument("--input-table", default=os.getenv("MYSQL_INPUT_TABLE", "filtered_bharatfleet"))
    parser.add_argument("--output-table", default=os.getenv("MYSQL_OUTPUT_TABLE", "bharatfleet_data"))
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of company names to process.")
    parser.add_argument("--engine", choices=("google", "duckduckgo"), default="google")
    parser.add_argument("--fallback-engine", choices=("google", "duckduckgo"), default="duckduckgo")
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--headful", action="store_true")
    parser.add_argument("--channel", default="msedge")
    parser.add_argument("--proxy-file", default=None)
    parser.add_argument("--min-delay-seconds", type=float, default=4.0)
    parser.add_argument("--max-delay-seconds", type=float, default=9.0)
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument("--batch-cooldown-seconds", type=float, default=45.0)
    parser.add_argument("--block-cooldown-seconds", type=float, default=180.0)
    parser.add_argument("--session-max-queries", type=int, default=4)
    parser.add_argument("--stop-block-rate", type=float, default=0.20)
    parser.add_argument("--min-processed-for-block-guard", type=int, default=10)
    parser.add_argument(
        "--rotate-engines",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Alternate the primary/fallback engine order across queries when both are available.",
    )
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip company names already present in the output table.",
    )
    return parser.parse_args()


def build_mysql_settings(args: argparse.Namespace) -> MysqlSettings:
    if not args.password:
        raise ValueError(
            "MySQL password is required. Pass --password or set MYSQL_PASSWORD in the environment."
        )
    return MysqlSettings(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        input_table=args.input_table,
        output_table=args.output_table,
    )


def open_connection(settings: MysqlSettings) -> MySQLConnection:
    return mysql.connector.connect(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
        database=settings.database,
    )


def normalize_segment(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", value).strip(" ,;:-")
    cleaned = re.sub(r"^(?:m/s|m\.s\.)\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"\(([^)]*)\)",
        lambda match: ""
        if any(term in match.group(1).lower() for term in PARENTHETICAL_NOISE_TERMS)
        else match.group(0),
        cleaned,
    )
    return cleaned.strip(" ,;:-")


def split_numbered_companies(raw_company_name: str) -> list[str]:
    matches = list(NUMBERED_ITEM_RE.finditer(raw_company_name))
    if len(matches) < 2:
        return [raw_company_name]

    segments: list[str] = []
    for index, match in enumerate(matches):
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(raw_company_name)
        segment = normalize_segment(raw_company_name[start:end])
        if segment:
            segments.append(segment)
    return segments or [raw_company_name]


def split_candidate_segments(raw_company_name: str) -> list[str]:
    numbered_segments = split_numbered_companies(raw_company_name)
    segments: list[str] = []
    for numbered_segment in numbered_segments:
        parts = [
            normalize_segment(part)
            for part in re.split(r"[,\n;]+", numbered_segment)
            if normalize_segment(part)
        ]
        segments.extend(parts or [normalize_segment(numbered_segment)])
    return [segment for segment in segments if segment]


def score_segment(segment: str, matched_keyword: str | None) -> int:
    lowered = segment.lower()
    is_bare_domain = "." in segment and " " not in segment
    score = 0
    if matched_keyword and matched_keyword.lower() in lowered:
        score += 100
    if any(hint in lowered for hint in ORG_HINTS):
        score += 25
    if "private limited" in lowered or "limited" in lowered:
        score += 15
    if len(segment.split()) <= 1:
        score -= 10
    if is_bare_domain:
        score -= 90
    if re.fullmatch(r"[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}", segment):
        score -= 20
    return score


def derive_search_company_name(raw_company_name: str, matched_keyword: str | None) -> str:
    segments = split_candidate_segments(raw_company_name)
    if not segments:
        return normalize_segment(raw_company_name)

    ranked = sorted(
        segments,
        key=lambda segment: (score_segment(segment, matched_keyword), len(segment)),
        reverse=True,
    )
    best = ranked[0]
    return best


def choose_engines(
    query_index: int,
    primary_engine: str,
    fallback_engine: str | None,
    rotate_engines: bool,
) -> tuple[str, str | None]:
    if not fallback_engine or not rotate_engines:
        return primary_engine, fallback_engine
    if query_index % 2 == 1:
        return fallback_engine, primary_engine
    return primary_engine, fallback_engine


def infer_record_status(record: dict[str, Any]) -> tuple[str, str | None]:
    status = record.get("status") or "failed"
    details = " | ".join(
        str(value)
        for value in (
            record.get("error"),
            record.get("note"),
        )
        if value
    )
    combined_text_parts = [details.lower()] if details else []
    blocked_hits = 0

    for result in record.get("results") or []:
        title = str(result.get("title") or "")
        final_url = str((result.get("metadata") or {}).get("final_url") or result.get("url") or "")
        haystack = f"{title} {final_url}".lower()
        combined_text_parts.append(haystack)
        if any(pattern in haystack for pattern in BLOCK_SIGNAL_PATTERNS):
            blocked_hits += 1

    combined_text = " ".join(combined_text_parts)
    if any(pattern in combined_text for pattern in BLOCK_SIGNAL_PATTERNS):
        return "blocked", details or "Detected blocking or challenge indicators in search/fetch results."
    if status == "ok" and not (record.get("results") or []):
        return "failed", details or "Search completed but produced no result URLs."
    return status, details or None


async def build_session(
    playwright,
    args: argparse.Namespace,
    proxies: list[ProxyConfig],
    session_index: int,
):
    proxy = proxies[session_index % len(proxies)] if proxies else None
    browser = await build_browser(
        playwright,
        headless=not args.headful,
        channel=args.channel,
        proxy=proxy,
    )
    context = await build_context(browser)
    page = await context.new_page()
    return browser, context, page, proxy


async def close_session(browser, context) -> None:
    await context.close()
    await browser.close()


def fetch_input_companies(
    conn: MySQLConnection,
    settings: MysqlSettings,
    limit: int | None,
    resume: bool,
) -> list[InputCompany]:
    query = f"""
        SELECT f.matched_keyword, f.company_name
        FROM {settings.database}.{settings.input_table} f
        WHERE f.company_name IS NOT NULL
          AND TRIM(f.company_name) <> ''
          AND (
              f.matched_keyword IS NULL
              OR TRIM(f.matched_keyword) = ''
              OR LOWER(f.company_name) LIKE CONCAT('%', LOWER(f.matched_keyword), '%')
          )
        GROUP BY f.matched_keyword, f.company_name
        ORDER BY f.company_name
    """
    if limit:
        query += f" LIMIT {int(limit)}"

    existing_outputs: set[str] = set()
    if resume:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT company_name FROM {settings.database}.{settings.output_table}")
            existing_outputs = {normalize_segment(row[0]) for row in cursor.fetchall() if row[0]}

    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    inputs: list[InputCompany] = []
    seen_search_names: set[str] = set()
    for matched_keyword, raw_company_name in rows:
        search_company_name = derive_search_company_name(raw_company_name, matched_keyword)
        normalized_search_name = normalize_segment(search_company_name)
        if not normalized_search_name:
            continue
        if resume and normalized_search_name in existing_outputs:
            continue
        if normalized_search_name in seen_search_names:
            continue
        seen_search_names.add(normalized_search_name)
        inputs.append(
            InputCompany(
                matched_keyword=normalize_segment(matched_keyword) if matched_keyword else None,
                raw_company_name=normalize_segment(raw_company_name),
                search_company_name=normalized_search_name,
            )
        )
    return inputs


def save_record(conn: MySQLConnection, settings: MysqlSettings, company_name: str, record: dict[str, Any]) -> None:
    payload = json.dumps(record, ensure_ascii=False)
    status = record.get("status")
    error_text = record.get("error")
    query = f"""
        INSERT INTO {settings.database}.{settings.output_table} (company_name, json, status, error_text, processed_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            json = VALUES(json),
            status = VALUES(status),
            error_text = VALUES(error_text),
            processed_at = VALUES(processed_at)
    """
    with conn.cursor() as cursor:
        cursor.execute(query, (company_name, payload, status, error_text))
    conn.commit()


async def run_mysql_pipeline(args: argparse.Namespace) -> None:
    settings = build_mysql_settings(args)
    proxies = load_proxies(Path(args.proxy_file)) if args.proxy_file else []
    conn = open_connection(settings)
    try:
        input_companies = fetch_input_companies(conn, settings, args.limit, args.resume)
        if not input_companies:
            print("No company names found to process.")
            return

        async with async_playwright() as playwright:
            browser = None
            context = None
            page = None
            active_proxy: ProxyConfig | None = None
            session_index = 0
            session_queries = 0
            blocked_count = 0
            try:
                for index, input_company in enumerate(input_companies, start=1):
                    if page is None or session_queries >= args.session_max_queries:
                        if browser and context:
                            await close_session(browser, context)
                            browser = None
                            context = None
                            page = None
                        browser, context, page, active_proxy = await build_session(
                            playwright,
                            args,
                            proxies,
                            session_index,
                        )
                        session_index += 1
                        session_queries = 0

                    primary_engine, fallback_engine = choose_engines(
                        query_index=index,
                        primary_engine=args.engine,
                        fallback_engine=args.fallback_engine,
                        rotate_engines=args.rotate_engines,
                    )
                    task = QueryTask(query_id=index, query=input_company.search_company_name)
                    record = await process_query(
                        page=page,
                        context=context,
                        task=task,
                        primary_engine=primary_engine,
                        fallback_engine=fallback_engine,
                        max_retries=args.max_retries,
                    )
                    status, status_detail = infer_record_status(record)
                    record["status"] = status
                    if status_detail:
                        record["error"] = status_detail
                    record["engine_primary_requested"] = primary_engine
                    record["engine_fallback_requested"] = fallback_engine
                    record["input_source"] = {
                        "matched_keyword": input_company.matched_keyword,
                        "raw_company_name": input_company.raw_company_name,
                        "search_company_name": input_company.search_company_name,
                    }
                    if active_proxy:
                        record["proxy"] = active_proxy.server
                    save_record(conn, settings, input_company.search_company_name, record)
                    print(
                        f"[mysql-pipeline] saved {index}/{len(input_companies)} "
                        f"raw={input_company.raw_company_name!r} "
                        f"search={input_company.search_company_name!r} "
                        f"status={record['status']} "
                        f"engine={primary_engine}"
                    )
                    session_queries += 1
                    if record["status"] == "blocked":
                        blocked_count += 1
                        if browser and context:
                            await close_session(browser, context)
                            browser = None
                            context = None
                            page = None
                        await asyncio.sleep(args.block_cooldown_seconds)
                    else:
                        await asyncio.sleep(
                            random.uniform(args.min_delay_seconds, args.max_delay_seconds)
                        )

                    if (
                        index >= args.min_processed_for_block_guard
                        and index > 0
                        and (blocked_count / index) >= args.stop_block_rate
                    ):
                        print(
                            "[mysql-pipeline] stopping early because the block rate "
                            f"reached {blocked_count}/{index} ({blocked_count / index:.0%})."
                        )
                        break

                    if index % args.batch_size == 0:
                        if browser and context:
                            await close_session(browser, context)
                            browser = None
                            context = None
                            page = None
                        await asyncio.sleep(args.batch_cooldown_seconds)
            finally:
                if browser and context:
                    await close_session(browser, context)
    finally:
        conn.close()


def main() -> None:
    args = parse_args()
    asyncio.run(run_mysql_pipeline(args))


if __name__ == "__main__":
    main()
