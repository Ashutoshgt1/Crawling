import argparse
import asyncio
import json
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from playwright.async_api import (
    Browser,
    BrowserContext,
    Error as PlaywrightError,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)


GOOGLE_URL = "https://www.google.com/"
DUCKDUCKGO_URL = "https://duckduckgo.com/"
DEFAULT_OUTPUT_JSONL = "results.jsonl"
DEFAULT_SUMMARY_JSON = "results.json"
DEFAULT_CHECKPOINT_JSON = "crawler.checkpoint.json"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36"
)
PAGE_TITLE_TIMEOUT_MS = 15000
SEARCH_TIMEOUT_MS = 15000
TEXT_SAMPLE_LIMIT = 20000
METADATA_CONCURRENCY_PER_WORKER = 3
AUTOMATION_STEALTH_SCRIPT = (
    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
)
SOCIAL_DOMAINS = (
    "linkedin.com",
    "facebook.com",
    "instagram.com",
    "x.com",
    "twitter.com",
    "youtube.com",
    "tiktok.com",
)
AGGREGATOR_DOMAINS = (
    "wikipedia.org",
    "linkedin.com",
    "facebook.com",
    "instagram.com",
    "youtube.com",
    "glassdoor.com",
    "indeed.com",
    "ambitionbox.com",
    "crunchbase.com",
    "justdial.com",
    "carwale.com",
)
GENERIC_COMPANY_NAME_CANDIDATES = {
    "contact us",
    "leadership",
    "overview",
    "news & stories",
    "innovation",
    "our businesses",
    "sign in",
}
GENERIC_SOCIAL_PATHS = {
    "",
    "login",
    "signup",
    "jobs",
    "games",
    "feed",
    "home",
    "top-content",
    "pub",
    "dir",
    "search",
    "hashtag",
}
IMPORTANT_PAGE_KEYWORDS = {
    "about": ("/about", "/about-us", "/company", "/overview", "/who-we-are"),
    "contact": ("/contact", "/contact-us", "/support", "/reach-us", "/locations"),
    "careers": ("/careers", "/jobs", "/join-us", "/work-with-us"),
    "leadership": ("/leadership", "/management", "/team", "/board"),
}
GOOGLE_BLOCKED_PREFIXES = (
    "https://www.google.",
    "http://www.google.",
    "https://webcache.googleusercontent.",
    "https://googleads.g.doubleclick.net/",
)
DUCKDUCKGO_BLOCKED_PREFIXES = ("https://duckduckgo.com/",)


@dataclass
class QueryTask:
    query_id: int
    query: str


@dataclass
class ProxyConfig:
    server: str
    username: str | None = None
    password: str | None = None


class ProgressTracker:
    def __init__(
        self,
        checkpoint_path: Path,
        output_path: Path,
        input_path: Path,
        total_queries: int,
        completed_count: int,
    ) -> None:
        self.checkpoint_path = checkpoint_path
        self.output_path = output_path
        self.input_path = input_path
        self.total_queries = total_queries
        self.completed_count = completed_count
        self.succeeded_count = 0
        self.failed_count = 0
        self.lock = asyncio.Lock()

    async def record(self, record: dict[str, Any]) -> None:
        async with self.lock:
            self.completed_count += 1
            if record["status"] == "ok":
                self.succeeded_count += 1
            else:
                self.failed_count += 1

            checkpoint = {
                "input_path": str(self.input_path),
                "output_path": str(self.output_path),
                "total_queries": self.total_queries,
                "completed_count": self.completed_count,
                "succeeded_count": self.succeeded_count,
                "failed_count": self.failed_count,
                "pending_count": self.total_queries - self.completed_count,
                "last_query_id": record["query_id"],
                "last_status": record["status"],
                "updated_at": utc_now(),
                "resume_source": str(self.output_path),
            }
            self.checkpoint_path.write_text(
                json.dumps(checkpoint, indent=2),
                encoding="utf-8",
            )


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def load_queries(input_file: Path) -> list[QueryTask]:
    return [
        QueryTask(query_id=index, query=query)
        for index, line in enumerate(input_file.read_text(encoding="utf-8").splitlines(), start=1)
        if (query := line.strip())
    ]


def iter_jsonl_records(path: Path):
    if not path.exists():
        return

    with path.open("r", encoding="utf-8") as jsonl_file:
        for line in jsonl_file:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                yield json.loads(stripped)
            except json.JSONDecodeError:
                continue


def load_completed_ids(output_path: Path) -> set[int]:
    completed_ids: set[int] = set()
    for record in iter_jsonl_records(output_path):
        query_id = record.get("query_id")
        if isinstance(query_id, int):
            completed_ids.add(query_id)
    return completed_ids


def parse_proxy_line(line: str) -> ProxyConfig | None:
    cleaned = line.strip()
    if not cleaned or cleaned.startswith("#"):
        return None

    if "://" in cleaned:
        parts = urlsplit(cleaned)
        username = parts.username
        password = parts.password
        host = parts.hostname or ""
        port = f":{parts.port}" if parts.port else ""
        server = f"{parts.scheme}://{host}{port}"
        return ProxyConfig(server=server, username=username, password=password)

    chunks = [chunk.strip() for chunk in cleaned.split(",")]
    if len(chunks) == 1:
        return ProxyConfig(server=chunks[0])
    if len(chunks) >= 3:
        return ProxyConfig(server=chunks[0], username=chunks[1] or None, password=chunks[2] or None)
    raise ValueError(f"Unsupported proxy format: {cleaned}")


def load_proxies(proxy_file: Path | None) -> list[ProxyConfig]:
    if proxy_file is None:
        return []
    proxies: list[ProxyConfig] = []
    for line in proxy_file.read_text(encoding="utf-8").splitlines():
        proxy = parse_proxy_line(line)
        if proxy:
            proxies.append(proxy)
    return proxies


def normalize_result_url(url: str) -> str:
    parts = urlsplit(url)
    query_items = parse_qsl(parts.query, keep_blank_values=True)

    if "youtube.com" in parts.netloc:
        query_items = [(key, value) for key, value in query_items if key != "t"]

    if "ford.com" in parts.netloc:
        query_items = [(key, value) for key, value in query_items if key != "srsltid"]

    normalized_query = urlencode(query_items, doseq=True)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, normalized_query, ""))


def normalize_whitespace(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = re.sub(r"\s+", " ", value).strip()
    return cleaned or None


def unique_nonempty(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        cleaned = normalize_whitespace(value)
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            result.append(cleaned)
    return result


def extract_emails(text: str, direct_emails: list[str] | None = None) -> list[str]:
    candidates = list(direct_emails or [])
    candidates.extend(
        re.findall(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", text, flags=re.IGNORECASE)
    )
    cleaned: list[str] = []
    for candidate in candidates:
        normalized = normalize_whitespace(candidate)
        if not normalized:
            continue
        email = normalized.lower().strip(" .,:;")
        if not re.fullmatch(r"[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}", email):
            continue
        if ".." in email or email.startswith("@") or email.endswith("@"):
            continue
        cleaned.append(email)
    return unique_nonempty(cleaned)


def normalize_phone_candidate(value: str) -> str | None:
    cleaned = normalize_whitespace(value)
    if not cleaned:
        return None
    cleaned = cleaned.replace("\u00a0", " ")
    cleaned = re.sub(r"(?:ext|extension|x)\s*\d+$", "", cleaned, flags=re.IGNORECASE).strip(" ,;")
    return cleaned or None


def is_valid_phone_candidate(value: str) -> bool:
    if not value:
        return False
    if re.search(r"[A-Za-z]", value):
        return False
    if "." in value:
        return False
    digit_groups = re.findall(r"\d+", value)
    digits_only = "".join(digit_groups)
    digit_count = len(digits_only)
    if digit_count < 8 or digit_count > 15:
        return False
    if len(digit_groups) >= 5:
        return False
    if len(set(digits_only)) == 1:
        return False
    if re.fullmatch(r"(?:\d\s+){4,}\d", value):
        return False
    if re.search(r"\b(?:19|20)\d{2}\s*-\s*(?:19|20)\d{2}\b", value):
        return False
    if len(digit_groups) >= 2 and all(len(group) == 4 for group in digit_groups[:2]):
        if all(1900 <= int(group) <= 2099 for group in digit_groups):
            return False
    return True


def format_phone_candidate(value: str) -> str:
    normalized = re.sub(r"\s+", " ", value).strip()
    if normalized.startswith("+"):
        return normalized
    digits_only = re.sub(r"\D", "", normalized)
    return digits_only if " " not in normalized and "-" not in normalized and "(" not in normalized else normalized


def extract_labeled_phone_candidates(text: str) -> list[str]:
    patterns = [
        r"(?:phone|tel|telephone|mobile|call us|contact)\s*[:\-]?\s*((?:\+?\d[\d\-\s()]{6,}\d))",
        r"(?:board|switchboard|helpline|hotline)\s*[:\-]?\s*((?:\+?\d[\d\-\s()]{6,}\d))",
    ]
    matches: list[str] = []
    for pattern in patterns:
        matches.extend(re.findall(pattern, text, flags=re.IGNORECASE))
    return matches


def extract_phones(
    text: str,
    tel_links: list[str] | None = None,
    prioritized_texts: list[str] | None = None,
) -> list[str]:
    candidates: list[str] = []
    for tel_link in tel_links or []:
        tel_value = tel_link.removeprefix("tel:")
        normalized = normalize_phone_candidate(tel_value)
        if normalized and is_valid_phone_candidate(normalized):
            candidates.append(format_phone_candidate(normalized))

    search_spaces = list(prioritized_texts or [])
    for chunk in search_spaces:
        for match in extract_labeled_phone_candidates(chunk) + re.findall(
            r"(?:\+?\d[\d\-\s()]{6,}\d)", chunk
        ):
            normalized = normalize_phone_candidate(match)
            if normalized and is_valid_phone_candidate(normalized):
                candidates.append(format_phone_candidate(normalized))

    if not candidates and text:
        for match in extract_labeled_phone_candidates(text):
            normalized = normalize_phone_candidate(match)
            if normalized and is_valid_phone_candidate(normalized):
                candidates.append(format_phone_candidate(normalized))

    return unique_nonempty(candidates)


def is_plausible_location(value: str) -> bool:
    cleaned = normalize_whitespace(value)
    if not cleaned:
        return False
    if len(cleaned) < 6 or len(cleaned) > 140:
        return False
    if re.fullmatch(r"[\d\s,.-]+", cleaned):
        return False
    if cleaned.count(",") > 6:
        return False
    if "," not in cleaned and len(cleaned) > 50:
        return False
    if any(token in cleaned.lower() for token in ("businesses", "solutions", "products", "services")):
        return False
    return bool(re.search(r"[A-Za-z]", cleaned))


def extract_location_mentions(text: str, structured_locations: list[str] | None = None) -> list[str]:
    patterns = [
        r"\bheadquarters(?:ed)?\s+(?:in|at)\s+([A-Z][A-Za-z0-9&.,\-\s]{2,80})",
        r"\bbased\s+in\s+([A-Z][A-Za-z0-9&.,\-\s]{2,80})",
        r"\blocated\s+in\s+([A-Z][A-Za-z0-9&.,\-\s]{2,80})",
        r"\bcorporate office\s*[:\-]?\s*([A-Z][A-Za-z0-9&.,\-\s]{2,80})",
    ]
    matches: list[str] = [value for value in (structured_locations or []) if is_plausible_location(value)]
    for pattern in patterns:
        matches.extend(re.findall(pattern, text, flags=re.IGNORECASE))
    return unique_nonempty([value for value in matches if is_plausible_location(value)][:10])


def is_valid_social_profile(link: str) -> bool:
    parts = urlsplit(link)
    domain = parts.netloc.lower()
    path_segments = [segment for segment in parts.path.split("/") if segment]
    if "linkedin.com" in domain:
        if len(path_segments) < 2:
            return False
        if path_segments[0] not in {"company", "in", "school"}:
            return False
        if path_segments[0] == "in":
            return len(path_segments) >= 2 and path_segments[1] not in GENERIC_SOCIAL_PATHS
        return path_segments[1] not in GENERIC_SOCIAL_PATHS
    if "youtube.com" in domain:
        return parts.path.startswith("/@") or (
            bool(path_segments) and path_segments[0] in {"channel", "c", "user"}
        )
    if "facebook.com" in domain or "instagram.com" in domain or "x.com" in domain or "twitter.com" in domain or "tiktok.com" in domain:
        return bool(path_segments) and path_segments[0].lower() not in GENERIC_SOCIAL_PATHS
    return False


def clean_social_profiles(links: list[str]) -> list[str]:
    profiles: list[str] = []
    for link in links:
        lowered = link.lower()
        if not any(domain in lowered for domain in SOCIAL_DOMAINS):
            continue
        if any(fragment in lowered for fragment in ("/share", "/intent/", "/sharer", "/search?", "/hashtag/")):
            continue
        normalized = link.split("?", 1)[0].rstrip("/")
        if is_valid_social_profile(normalized):
            profiles.append(normalized)
    return unique_nonempty(profiles)


def classify_domain(domain: str) -> str:
    lowered = domain.lower()
    if any(aggregator in lowered for aggregator in AGGREGATOR_DOMAINS):
        return "aggregator_or_directory"
    return "likely_official"


def build_brand_domain_mapping(domain: str, company_name: str | None) -> dict[str, str | None]:
    return {
        "company_name": company_name,
        "domain": domain,
        "root_hint": ".".join(domain.split(".")[-2:]) if "." in domain else domain,
    }


def pick_company_name(candidates: list[str | None]) -> str | None:
    for candidate in candidates:
        cleaned = normalize_whitespace(candidate)
        if is_strong_company_name(cleaned):
            return cleaned
    return None


def pick_company_summary(summary: str | None) -> str | None:
    cleaned = normalize_whitespace(summary)
    if not cleaned or len(cleaned) < 40:
        return None
    return cleaned[:400]


def pick_best_description(candidates: list[str | None]) -> str | None:
    cleaned_candidates = [normalize_whitespace(candidate) for candidate in candidates]
    valid_candidates = [
        candidate
        for candidate in cleaned_candidates
        if candidate and len(candidate) >= 30 and len(candidate) <= 400
    ]
    if not valid_candidates:
        return None
    valid_candidates.sort(key=len)
    return valid_candidates[0]


def build_business_metadata(
    domain: str,
    company_name: str | None,
    page_text: str = "",
    social_links: list[str] | None = None,
    headquarters_candidates: list[str] | None = None,
    direct_emails: list[str] | None = None,
    tel_links: list[str] | None = None,
    prioritized_phone_texts: list[str] | None = None,
    location_texts: list[str] | None = None,
) -> dict[str, Any]:
    location_source_text = " ".join(location_texts or [])
    return {
        "company_name": company_name,
        "headquarters_or_location_mentions": unique_nonempty(
            extract_location_mentions(
                location_source_text,
                structured_locations=headquarters_candidates,
            )
        ),
        "emails": extract_emails(page_text, direct_emails=direct_emails),
        "phones": extract_phones(
            page_text,
            tel_links=tel_links,
            prioritized_texts=prioritized_phone_texts,
        ),
        "social_profiles": clean_social_profiles(social_links or []),
        "brand_domain_mapping": build_brand_domain_mapping(domain, company_name),
        "classification": classify_domain(domain),
    }


def build_result_record(
    url: str,
    title: str,
    final_url: str,
    domain: str,
    description: str | None,
    keywords: str | None,
    canonical_url: str | None,
    business_metadata: dict[str, Any],
) -> dict[str, Any]:
    return {
        "url": url,
        "title": title,
        "metadata": {
            "final_url": final_url,
            "domain": domain,
            "description": description,
            "keywords": keywords,
            "canonical_url": canonical_url,
            "business": business_metadata,
        },
    }


def build_live_status_log(url: str, title: str, duration_seconds: float, status: str) -> str:
    return (
        f"[url] {status} | time={duration_seconds}s | "
        f"title={json.dumps(title, ensure_ascii=False)} | "
        f"url={url}"
    )


def build_query_record(
    task: QueryTask,
    status: str,
    engine: str,
    attempts: int,
    query_started_at: float,
    results: list[dict[str, Any]] | None = None,
    note: str | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    record = {
        "query_id": task.query_id,
        "query": task.query,
        "status": status,
        "engine": engine,
        "attempts": attempts,
        "results": results or [],
        "timestamp": utc_now(),
        "query_duration_seconds": round(time.perf_counter() - query_started_at, 2),
    }
    if note:
        record["note"] = note
    if error:
        record["error"] = error
    return record


def build_empty_company_profile() -> dict[str, Any]:
    return {
        "company_name": None,
        "company_summary": None,
        "official_website": None,
        "primary_domain": None,
        "all_domains": [],
        "headquarters_or_location_mentions": [],
        "emails": [],
        "phones": [],
        "social_profiles": [],
        "source_urls": [],
        "important_pages": {},
    }


def is_strong_company_name(candidate: str | None) -> bool:
    cleaned = normalize_whitespace(candidate)
    if not cleaned:
        return False
    lowered = cleaned.lower()
    if lowered in GENERIC_COMPANY_NAME_CANDIDATES:
        return False
    if any(
        phrase in lowered
        for phrase in (" careers", " jobs", "join us", "with you", "& you", "welcome to", "sign in")
    ):
        return False
    if len(cleaned) < 3:
        return False
    if len(cleaned) > 60:
        return False
    return True


def is_likely_official_url(url: str) -> bool:
    domain = urlsplit(url).netloc
    return classify_domain(domain) == "likely_official"


def merge_company_profile(
    profile: dict[str, Any],
    result_item: dict[str, Any],
) -> dict[str, Any]:
    metadata = result_item.get("metadata", {})
    business = metadata.get("business", {})
    final_url = metadata.get("final_url") or result_item["url"]
    domain = metadata.get("domain") or urlsplit(final_url).netloc

    if final_url not in profile["source_urls"]:
        profile["source_urls"].append(final_url)
    if domain and domain not in profile["all_domains"]:
        profile["all_domains"].append(domain)

    profile["emails"] = unique_nonempty(profile["emails"] + business.get("emails", []))
    profile["phones"] = unique_nonempty(profile["phones"] + business.get("phones", []))
    profile["social_profiles"] = unique_nonempty(
        profile["social_profiles"] + business.get("social_profiles", [])
    )
    profile["headquarters_or_location_mentions"] = unique_nonempty(
        profile["headquarters_or_location_mentions"]
        + business.get("headquarters_or_location_mentions", [])
    )

    if is_likely_official_url(final_url):
        if not profile["official_website"]:
            profile["official_website"] = final_url
        if not profile["primary_domain"] and domain:
            profile["primary_domain"] = domain
        company_name = business.get("company_name")
        if not profile["company_name"] and is_strong_company_name(company_name):
            profile["company_name"] = company_name
        if not profile["company_summary"]:
            profile["company_summary"] = pick_company_summary(metadata.get("description"))

        lowered_url = final_url.lower()
        for page_type, keywords in IMPORTANT_PAGE_KEYWORDS.items():
            if page_type in profile["important_pages"]:
                continue
            if any(keyword in lowered_url for keyword in keywords):
                profile["important_pages"][page_type] = final_url

    return profile


def prioritize_urls(urls: list[str]) -> list[str]:
    indexed_urls = list(enumerate(urls))
    indexed_urls.sort(key=lambda item: (not is_likely_official_url(item[1]), item[0]))
    return [url for _, url in indexed_urls]


def extract_unique_urls(hrefs: list[str], blocked_prefixes: tuple[str, ...]) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for href in hrefs:
        if not href:
            continue
        if href.startswith(blocked_prefixes):
            continue
        normalized_href = normalize_result_url(href)
        if normalized_href not in seen:
            seen.add(normalized_href)
            urls.append(normalized_href)
    return urls


async def dismiss_google_consent(page: Page) -> None:
    consent_selectors = [
        'button:has-text("Accept all")',
        'button:has-text("I agree")',
        'button:has-text("Accept everything")',
        'button:has-text("Reject all")',
        '[aria-label="Accept all"]',
    ]
    for selector in consent_selectors:
        try:
            button = page.locator(selector).first
            if await button.is_visible(timeout=1500):
                await button.click()
                await page.wait_for_timeout(1000)
                return
        except PlaywrightError:
            continue


async def google_requires_verification(page: Page) -> bool:
    if "/sorry/" in page.url:
        return True
    content = await page.content()
    return "g-recaptcha" in content


async def collect_http_links(locator) -> list[str]:
    count = await locator.count()
    hrefs: list[str] = []
    for index in range(count):
        href = await locator.nth(index).get_attribute("href")
        if href:
            hrefs.append(href)
    return hrefs


async def search_google(page: Page, query: str) -> list[str]:
    await page.goto(GOOGLE_URL, wait_until="domcontentloaded", timeout=SEARCH_TIMEOUT_MS)
    await dismiss_google_consent(page)

    search_box = page.locator('textarea[name="q"], input[name="q"]').first
    await search_box.wait_for(timeout=SEARCH_TIMEOUT_MS)
    await search_box.fill(query)
    await search_box.press("Enter")

    await page.wait_for_load_state("domcontentloaded")

    try:
        await page.locator("#search").wait_for(timeout=SEARCH_TIMEOUT_MS)
    except PlaywrightTimeoutError:
        await page.wait_for_timeout(3000)

    if await google_requires_verification(page):
        raise RuntimeError("Google blocked the automated search with a verification page.")

    hrefs = await collect_http_links(page.locator('#search a[href^="http"]'))
    return extract_unique_urls(
        hrefs=hrefs,
        blocked_prefixes=GOOGLE_BLOCKED_PREFIXES,
    )


async def search_duckduckgo(page: Page, query: str) -> list[str]:
    await page.goto(DUCKDUCKGO_URL, wait_until="domcontentloaded", timeout=SEARCH_TIMEOUT_MS)
    search_box = page.locator('input[name="q"]').first
    await search_box.wait_for(timeout=SEARCH_TIMEOUT_MS)
    await search_box.fill(query)
    await search_box.press("Enter")
    await page.wait_for_load_state("domcontentloaded")

    try:
        await page.locator('[data-testid="result"], .results').first.wait_for(timeout=SEARCH_TIMEOUT_MS)
    except PlaywrightTimeoutError:
        await page.wait_for_timeout(3000)

    hrefs = await collect_http_links(
        page.locator('[data-testid="result"] a[href^="http"], .result a[href^="http"]')
    )
    return extract_unique_urls(
        hrefs=hrefs,
        blocked_prefixes=DUCKDUCKGO_BLOCKED_PREFIXES,
    )


async def fetch_page_metadata(
    context: BrowserContext,
    url: str,
) -> dict[str, Any]:
    page = await context.new_page()
    started_at = time.perf_counter()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TITLE_TIMEOUT_MS)
        await page.wait_for_timeout(300)
        metadata_script = """
            (options) => {
                const normalizeText = (value) => {
                    if (!value || typeof value !== "string") return null;
                    const normalized = value.replace(/\\s+/g, " ").trim();
                    return normalized || null;
                };
                const uniqueNonEmpty = (values) => {
                    const seen = new Set();
                    const result = [];
                    for (const value of values) {
                        const normalized = normalizeText(value);
                        if (!normalized) continue;
                        if (seen.has(normalized)) continue;
                        seen.add(normalized);
                        result.push(normalized);
                    }
                    return result;
                };
                const getMeta = (name) => {
                    const selector = `meta[name="${name}"], meta[property="${name}"]`;
                    const element = document.querySelector(selector);
                    return element ? element.getAttribute("content") : null;
                };
                const parseJsonValue = (value) => {
                    if (!value) return [];
                    try {
                        const parsed = JSON.parse(value);
                        return Array.isArray(parsed) ? parsed : [parsed];
                    } catch {
                        return [];
                    }
                };
                const flattenGraph = (node) => {
                    if (!node || typeof node !== "object") return [];
                    if (Array.isArray(node)) {
                        return node.flatMap(flattenGraph);
                    }
                    const values = [node];
                    if (Array.isArray(node["@graph"])) {
                        values.push(...node["@graph"].flatMap(flattenGraph));
                    }
                    return values;
                };
                const isCompanyType = (value) => {
                    const types = Array.isArray(value) ? value : [value];
                    return types.some((item) =>
                        /organization|corporation|brand|localbusiness/i.test(String(item || ""))
                    );
                };
                const extractAddressText = (address) => {
                    if (!address) return null;
                    if (typeof address === "string") return normalizeText(address);
                    if (typeof address !== "object") return null;
                    return normalizeText(
                        [
                            address.streetAddress,
                            address.addressLocality,
                            address.addressRegion,
                            address.postalCode,
                            address.addressCountry,
                        ]
                            .filter(Boolean)
                            .join(", ")
                    );
                };

                const canonical = document.querySelector('link[rel="canonical"]');
                const socialLinks = Array.from(document.querySelectorAll('a[href]'))
                    .map((link) => link.href)
                    .filter((href) => /linkedin|facebook|instagram|twitter|x\\.com|youtube|tiktok/i.test(href));
                const mailtoLinks = Array.from(document.querySelectorAll('a[href^="mailto:"]'))
                    .map((link) => link.getAttribute("href") || "")
                    .map((href) => href.replace(/^mailto:/i, "").split("?")[0]);
                const telLinks = Array.from(document.querySelectorAll('a[href^="tel:"]'))
                    .map((link) => link.getAttribute("href") || "");
                const focusedTextBlocks = Array.from(
                    document.querySelectorAll(
                        'address, footer, [class*="contact" i], [id*="contact" i], [class*="footer" i], [itemprop="address"], [itemprop="telephone"]'
                    )
                )
                    .map((element) => element.textContent || "")
                    .map((text) => normalizeText(text))
                    .filter(Boolean)
                    .slice(0, 20);
                const jsonLdNodes = Array.from(
                    document.querySelectorAll('script[type="application/ld+json"]')
                )
                    .flatMap((script) => parseJsonValue(script.textContent || ""))
                    .flatMap(flattenGraph);
                const organizationNodes = jsonLdNodes.filter((node) => isCompanyType(node["@type"]));
                const schemaCompanyName = organizationNodes
                    .map((node) => normalizeText(node.legalName || node.name))
                    .find(Boolean) || null;
                const schemaAddresses = uniqueNonEmpty(
                    organizationNodes.map((node) => extractAddressText(node.address))
                );
                const schemaTelephones = uniqueNonEmpty(
                    organizationNodes.flatMap((node) => {
                        const values = [];
                        if (node.telephone) values.push(node.telephone);
                        const contactPoint = Array.isArray(node.contactPoint) ? node.contactPoint : [node.contactPoint];
                        for (const entry of contactPoint) {
                            if (entry && entry.telephone) values.push(entry.telephone);
                        }
                        return values;
                    })
                );
                const schemaEmails = uniqueNonEmpty(
                    organizationNodes.flatMap((node) => {
                        const values = [];
                        if (node.email) values.push(node.email);
                        const contactPoint = Array.isArray(node.contactPoint) ? node.contactPoint : [node.contactPoint];
                        for (const entry of contactPoint) {
                            if (entry && entry.email) values.push(entry.email);
                        }
                        return values;
                    })
                );
                const siteName = normalizeText(
                    getMeta("og:site_name")
                    || getMeta("application-name")
                    || getMeta("apple-mobile-web-app-title")
                );
                const logoAlt = normalizeText(
                    document.querySelector('img[alt][src*="logo" i], img[alt*="logo" i]')?.getAttribute("alt")
                );
                const navBrand = normalizeText(
                    document.querySelector(
                        'header [class*="logo" i], nav [class*="logo" i], header [class*="brand" i], nav [class*="brand" i], .navbar-brand'
                    )?.textContent
                );
                const footerText = normalizeText(
                    Array.from(document.querySelectorAll("footer"))
                        .map((footer) => footer.textContent || "")
                        .find((text) => /copyright|all rights reserved|©/i.test(text))
                );
                const bodyText = (document.body ? document.body.innerText : "").slice(0, options.textSampleLimit);
                const h1 = document.querySelector("h1");
                const companyNameCandidates = [
                    schemaCompanyName,
                    siteName,
                    logoAlt,
                    navBrand,
                    footerText,
                    normalizeText(h1 ? h1.textContent : null),
                ];
                  return {
                      title: document.title || null,
                      final_url: window.location.href,
                      description_candidates: [
                          getMeta("description"),
                          getMeta("og:description"),
                          getMeta("twitter:description"),
                      ],
                      keywords: getMeta("keywords"),
                      canonical_url: canonical ? canonical.href : null,
                      company_name_candidates: companyNameCandidates,
                      schema_addresses: schemaAddresses,
                      schema_telephones: schemaTelephones,
                      schema_emails: schemaEmails,
                      social_links: socialLinks,
                      mailto_links: mailtoLinks,
                      tel_links: telLinks,
                      focused_text_blocks: focusedTextBlocks,
                      body_text: bodyText,
                  };
              }
          """
        metadata = await page.evaluate(
            metadata_script,
            {
                "textSampleLimit": TEXT_SAMPLE_LIMIT,
            },
        )
        final_url = metadata.get("final_url") or page.url
        domain = urlsplit(final_url).netloc
        page_text = metadata.get("body_text") or ""
        focused_text_blocks = metadata.get("focused_text_blocks") or []
        company_name = pick_company_name(metadata.get("company_name_candidates") or [])
        schema_addresses = unique_nonempty(metadata.get("schema_addresses") or [])
        description = pick_best_description(metadata.get("description_candidates") or [])
        business_metadata = build_business_metadata(
            domain=domain,
            company_name=company_name,
            page_text=page_text,
            social_links=metadata.get("social_links") or [],
            headquarters_candidates=schema_addresses,
            direct_emails=(metadata.get("mailto_links") or []) + (metadata.get("schema_emails") or []),
            tel_links=(metadata.get("tel_links") or []) + (metadata.get("schema_telephones") or []),
            prioritized_phone_texts=focused_text_blocks,
            location_texts=focused_text_blocks,
        )
        fetch_duration_seconds = round(time.perf_counter() - started_at, 2)
        result = build_result_record(
            url=url,
            title=(metadata.get("title") or url).strip(),
            final_url=final_url,
            domain=domain,
            description=description,
            keywords=normalize_whitespace(metadata.get("keywords")),
            canonical_url=metadata.get("canonical_url"),
            business_metadata=business_metadata,
        )
        print(
            build_live_status_log(
                url=result["url"],
                title=result["title"],
                duration_seconds=fetch_duration_seconds,
                status="ok",
            )
        )
        return result
    except PlaywrightError:
        fetch_duration_seconds = round(time.perf_counter() - started_at, 2)
        domain = urlsplit(url).netloc
        result = build_result_record(
            url=url,
            title=url,
            final_url=url,
            domain=domain,
            description=None,
            keywords=None,
            canonical_url=None,
            business_metadata=build_business_metadata(domain=domain, company_name=None),
        )
        print(
            build_live_status_log(
                url=result["url"],
                title=result["title"],
                duration_seconds=fetch_duration_seconds,
                status="failed",
            )
        )
        return result
    finally:
        await page.close()


async def build_result_items_and_profile(
    context: BrowserContext,
    urls: list[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    ordered_urls = prioritize_urls(urls)
    semaphore = asyncio.Semaphore(METADATA_CONCURRENCY_PER_WORKER)

    async def fetch_with_limit(url: str) -> tuple[str, dict[str, Any]]:
        async with semaphore:
            return url, await fetch_page_metadata(context, url)

    fetched_items = await asyncio.gather(*(fetch_with_limit(url) for url in ordered_urls))
    result_by_url = {url: item for url, item in fetched_items}
    ordered_results = [result_by_url[url] for url in urls if url in result_by_url]

    profile = build_empty_company_profile()
    for item in ordered_results:
        merge_company_profile(profile, item)

    return ordered_results, profile


async def run_search(page: Page, query: str, engine: str) -> list[str]:
    if engine == "google":
        return await search_google(page, query)
    if engine == "duckduckgo":
        return await search_duckduckgo(page, query)
    raise ValueError(f"Unsupported search engine: {engine}")


async def build_browser(playwright: Playwright, headless: bool, channel: str | None, proxy: ProxyConfig | None) -> Browser:
    launch_args = ["--disable-blink-features=AutomationControlled"]
    launch_kwargs: dict[str, Any] = {
        "headless": headless,
        "args": launch_args,
    }
    if proxy:
        launch_kwargs["proxy"] = {
            "server": proxy.server,
            "username": proxy.username,
            "password": proxy.password,
        }

    if channel:
        try:
            return await playwright.chromium.launch(channel=channel, **launch_kwargs)
        except PlaywrightError:
            pass

    return await playwright.chromium.launch(**launch_kwargs)


async def build_context(browser: Browser) -> BrowserContext:
    context = await browser.new_context(
        user_agent=DEFAULT_USER_AGENT,
        locale="en-US",
        viewport={"width": 1366, "height": 768},
    )
    await context.add_init_script(AUTOMATION_STEALTH_SCRIPT)
    return context


async def process_query(
    page: Page,
    context: BrowserContext,
    task: QueryTask,
    primary_engine: str,
    fallback_engine: str | None,
    max_retries: int,
) -> dict[str, Any]:
    last_error: str | None = None
    query_started_at = time.perf_counter()

    for attempt in range(1, max_retries + 1):
        try:
            urls = await run_search(page, task.query, primary_engine)
            results, company_profile = await build_result_items_and_profile(context, urls)
            record = build_query_record(
                task=task,
                status="ok",
                engine=primary_engine,
                attempts=attempt,
                query_started_at=query_started_at,
                results=results,
            )
            record["company_profile"] = company_profile
            return record
        except Exception as exc:
            last_error = str(exc)
            if fallback_engine:
                try:
                    urls = await run_search(page, task.query, fallback_engine)
                    results, company_profile = await build_result_items_and_profile(context, urls)
                    record = build_query_record(
                        task=task,
                        status="ok",
                        engine=fallback_engine,
                        attempts=attempt,
                        query_started_at=query_started_at,
                        results=results,
                        note=last_error,
                    )
                    record["company_profile"] = company_profile
                    return record
                except Exception as fallback_exc:
                    last_error = f"{last_error} | fallback failed: {fallback_exc}"

            if attempt < max_retries:
                await page.goto("about:blank")
                await asyncio.sleep(min(2 * attempt, 10))

    record = build_query_record(
        task=task,
        status="failed",
        engine=primary_engine,
        attempts=max_retries,
        query_started_at=query_started_at,
        error=last_error or "Unknown error",
    )
    record["company_profile"] = build_empty_company_profile()
    return record


async def write_result(output_path: Path, record: dict[str, Any], lock: asyncio.Lock) -> None:
    line = json.dumps(record, ensure_ascii=False) + "\n"
    async with lock:
        with output_path.open("a", encoding="utf-8") as output_file:
            output_file.write(line)


async def worker(
    worker_id: int,
    queue: asyncio.Queue[QueryTask],
    output_path: Path,
    output_lock: asyncio.Lock,
    tracker: ProgressTracker,
    playwright: Playwright,
    headless: bool,
    channel: str | None,
    proxy: ProxyConfig | None,
    engine: str,
    fallback_engine: str | None,
    max_retries: int,
    delay_seconds: float,
) -> None:
    browser = await build_browser(playwright, headless=headless, channel=channel, proxy=proxy)
    context = await build_context(browser)
    page = await context.new_page()

    try:
        while True:
            try:
                task = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            record = await process_query(
                page=page,
                context=context,
                task=task,
                primary_engine=engine,
                fallback_engine=fallback_engine,
                max_retries=max_retries,
            )
            record["worker_id"] = worker_id
            if proxy:
                record["proxy"] = proxy.server

            await write_result(output_path, record, output_lock)
            await tracker.record(record)
            queue.task_done()

            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
    finally:
        await context.close()
        await browser.close()


async def run_crawler(args: argparse.Namespace) -> None:
    input_path = Path(args.input)
    output_path = Path(args.output)
    checkpoint_path = Path(args.checkpoint)
    summary_output_path = Path(args.summary_output) if args.summary_output else None
    proxy_file = Path(args.proxy_file) if args.proxy_file else None

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    all_tasks = load_queries(input_path)
    if not all_tasks:
        raise ValueError("No search queries found in the input file.")

    completed_ids = load_completed_ids(output_path) if args.resume else set()
    pending_tasks = [task for task in all_tasks if task.query_id not in completed_ids]

    tracker = ProgressTracker(
        checkpoint_path=checkpoint_path,
        output_path=output_path,
        input_path=input_path,
        total_queries=len(all_tasks),
        completed_count=len(completed_ids),
    )

    if not pending_tasks:
        print("All queries are already complete. Nothing to do.")
        return

    queue: asyncio.Queue[QueryTask] = asyncio.Queue()
    for task in pending_tasks:
        queue.put_nowait(task)

    proxies = load_proxies(proxy_file)
    output_lock = asyncio.Lock()

    async with async_playwright() as playwright:
        workers = []
        for worker_id in range(1, args.concurrency + 1):
            proxy = proxies[(worker_id - 1) % len(proxies)] if proxies else None
            workers.append(
                asyncio.create_task(
                    worker(
                        worker_id=worker_id,
                        queue=queue,
                        output_path=output_path,
                        output_lock=output_lock,
                        tracker=tracker,
                        playwright=playwright,
                        headless=not args.headful,
                        channel=args.channel,
                        proxy=proxy,
                        engine=args.engine,
                        fallback_engine=args.fallback_engine,
                        max_retries=args.max_retries,
                        delay_seconds=args.delay,
                    )
                )
            )

        await asyncio.gather(*workers)

    if summary_output_path:
        records = list(iter_jsonl_records(output_path))
        summary_output_path.write_text(
            json.dumps(records, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    print(
        f"Completed {len(pending_tasks)} new queries. "
        f"Output: {output_path}. Checkpoint: {checkpoint_path}."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Large-batch Playwright crawler for search-result URLs with resume support."
    )
    parser.add_argument(
        "--input",
        default="queries.txt",
        help="Path to a text file containing one search query per line.",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT_JSONL,
        help="Primary incremental output file in JSONL format.",
    )
    parser.add_argument(
        "--summary-output",
        default=DEFAULT_SUMMARY_JSON,
        help="Aggregated JSON file generated from the JSONL output after the run.",
    )
    parser.add_argument(
        "--checkpoint",
        default=DEFAULT_CHECKPOINT_JSON,
        help="Checkpoint file storing progress metadata.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=3,
        help="Number of concurrent browser workers.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum number of attempts per query before marking it failed.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay in seconds after each processed query per worker.",
    )
    parser.add_argument(
        "--headful",
        action="store_true",
        help="Run browser windows visibly instead of headless.",
    )
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Resume by skipping query IDs already present in the JSONL output.",
    )
    parser.add_argument(
        "--engine",
        choices=("google", "duckduckgo"),
        default="google",
        help="Primary search engine to query.",
    )
    parser.add_argument(
        "--fallback-engine",
        choices=("google", "duckduckgo"),
        default="duckduckgo",
        help="Fallback engine to use when the primary engine fails.",
    )
    parser.add_argument(
        "--proxy-file",
        help=(
            "Optional proxy file. Each line may be a proxy URL like "
            "'http://host:port' or 'http://user:pass@host:port', "
            "or a CSV line 'server,username,password'."
        ),
    )
    parser.add_argument(
        "--channel",
        default="msedge",
        help="Preferred Chromium channel to launch, for example msedge or chrome.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(run_crawler(args))


if __name__ == "__main__":
    main()
