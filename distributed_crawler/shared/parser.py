from __future__ import annotations

import json
import re
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlsplit

from distributed_crawler.shared.types import ParsedDocument


TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
META_DESCRIPTION_RE = re.compile(
    r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']',
    re.IGNORECASE | re.DOTALL,
)
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PHONE_RE = re.compile(r"(?:\+?\d[\d\-\s()]{7,}\d)")
ADDRESS_RE = re.compile(
    r"\b\d{1,5}\s+[A-Za-z0-9.,\-\s]{5,80}(?:road|rd|street|st|avenue|ave|lane|ln|nagar|floor|tower|park|plaza|sector|phase)\b",
    re.IGNORECASE,
)
JSON_LD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)
SOCIAL_DOMAINS = ("linkedin.com", "facebook.com", "instagram.com", "x.com", "twitter.com", "youtube.com")
IMPORTANT_PAGE_PATTERNS = {
    "about": ("/about", "/about-us", "/company", "/who-we-are", "/overview"),
    "contact": ("/contact", "/contact-us", "/reach-us", "/locations"),
    "leadership": ("/leadership", "/management", "/team", "/board"),
    "careers": ("/careers", "/jobs", "/join-us"),
}
PAGE_TYPE_PRIORITY = {
    "contact": 4,
    "about": 3,
    "leadership": 2,
    "careers": 1,
    "generic": 0,
}


class LinkCollector(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.links: list[str] = []
        self.mailto_links: list[str] = []
        self.tel_links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        href = dict(attrs).get("href")
        if not href or href.startswith(("#", "javascript:")):
            return
        if href.startswith("mailto:"):
            self.mailto_links.append(href.removeprefix("mailto:").split("?", 1)[0])
            return
        if href.startswith("tel:"):
            self.tel_links.append(href.removeprefix("tel:"))
            return
        self.links.append(urljoin(self.base_url, href))


def clean_text(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = re.sub(r"\s+", " ", value).strip()
    return cleaned or None


def extract_json_ld_company(html: str) -> dict[str, Any]:
    for raw_match in JSON_LD_RE.findall(html):
        try:
            payload = json.loads(raw_match.strip())
        except json.JSONDecodeError:
            continue
        nodes = payload if isinstance(payload, list) else [payload]
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_type = node.get("@type")
            if node_type not in {"Organization", "Corporation", "LocalBusiness"}:
                continue
            address = node.get("address") or {}
            social_links = node.get("sameAs", [])
            return {
                "company_name": clean_text(node.get("legalName") or node.get("name")),
                "website": clean_text(node.get("url")),
                "email": clean_text(node.get("email")),
                "phone": clean_text(node.get("telephone")),
                "address": clean_text(
                    ", ".join(
                        str(part)
                        for part in [
                            address.get("streetAddress"),
                            address.get("addressLocality"),
                            address.get("addressRegion"),
                            address.get("postalCode"),
                            address.get("addressCountry"),
                        ]
                        if isinstance(address, dict) and part
                    )
                )
                if isinstance(address, dict)
                else clean_text(str(address)),
                "city": clean_text(address.get("addressLocality")) if isinstance(address, dict) else None,
                "state_region": clean_text(address.get("addressRegion")) if isinstance(address, dict) else None,
                "country": clean_text(address.get("addressCountry")) if isinstance(address, dict) else None,
                "postal_code": clean_text(address.get("postalCode")) if isinstance(address, dict) else None,
                "industry": clean_text(node.get("industry")),
                "description": clean_text(node.get("description")),
                "social_links": [link for link in social_links if isinstance(link, str)],
                "raw_fields": node,
                "confidence": 0.92,
                "extractor_version": "jsonld-v1",
            }
    return {}


def classify_page_type(url: str, title: str | None) -> str:
    lowered = f"{url.lower()} {(title or '').lower()}"
    for page_type, patterns in IMPORTANT_PAGE_PATTERNS.items():
        if any(pattern in lowered for pattern in patterns):
            return page_type
    return "generic"


def important_page_links(links: list[str]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {key: [] for key in IMPORTANT_PAGE_PATTERNS}
    for link in links:
        lowered = link.lower()
        for page_type, patterns in IMPORTANT_PAGE_PATTERNS.items():
            if any(pattern in lowered for pattern in patterns):
                grouped[page_type].append(link)
    return {key: value[:5] for key, value in grouped.items() if value}


def important_page_candidates(links: list[str]) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    grouped = important_page_links(links)
    for page_type, page_links in grouped.items():
        for link in page_links:
            candidates.append({"url": link, "page_type": page_type})
    candidates.sort(key=lambda item: PAGE_TYPE_PRIORITY.get(item["page_type"], 0), reverse=True)
    return candidates[:10]


def clean_phone(value: str) -> str | None:
    cleaned = clean_text(value)
    if not cleaned:
        return None
    if len(re.sub(r"\D", "", cleaned)) < 8:
        return None
    return cleaned


def extract_social_links(links: list[str]) -> list[str]:
    filtered: list[str] = []
    for link in links:
        lowered = link.lower()
        if any(domain in lowered for domain in SOCIAL_DOMAINS):
            filtered.append(link.split("?", 1)[0].rstrip("/"))
    return sorted(set(filtered))


def find_best_company_name(title: str | None, jsonld_name: str | None, domain: str) -> str | None:
    if jsonld_name:
        return jsonld_name
    if title and "|" in title:
        return clean_text(title.split("|", 1)[0])
    if title and "-" in title:
        return clean_text(title.split("-", 1)[0])
    if title:
        return title
    return clean_text(domain.split(".")[0].replace("-", " ").replace("_", " ").title())


def parse_document(base_url: str, html: str, max_links: int) -> ParsedDocument:
    collector = LinkCollector(base_url)
    collector.feed(html)

    title_match = TITLE_RE.search(html)
    meta_match = META_DESCRIPTION_RE.search(html)
    title = clean_text(title_match.group(1) if title_match else None)
    description = clean_text(meta_match.group(1) if meta_match else None)
    emails = sorted(set(EMAIL_RE.findall(html) + collector.mailto_links))
    phone_candidates = [*PHONE_RE.findall(html), *collector.tel_links]
    cleaned_phones = [clean_phone(candidate) for candidate in phone_candidates]
    phones = sorted({phone for phone in cleaned_phones if phone})
    addresses = sorted({address.strip(" ,.") for address in ADDRESS_RE.findall(html)})
    page_type = classify_page_type(base_url, title)
    domain = urlsplit(base_url).netloc
    important_candidates = important_page_candidates(collector.links)

    observation = extract_json_ld_company(html)
    if not observation:
        observation = {
            "company_name": find_best_company_name(title, None, domain),
            "website": base_url,
            "email": emails[0] if emails else None,
            "phone": phones[0] if phones else None,
            "address": addresses[0] if addresses else None,
            "city": None,
            "state_region": None,
            "country": None,
            "postal_code": None,
            "industry": None,
            "description": description,
            "social_links": extract_social_links(collector.links),
            "raw_fields": {
                "title": title,
                "description": description,
                "emails": emails[:10],
                "phones": phones[:10],
                "addresses": addresses[:5],
                "important_page_links": important_page_links(collector.links),
                "important_page_candidates": important_candidates,
                "page_type": page_type,
            },
            "confidence": 0.68 if page_type in {"about", "contact"} and (emails or phones or addresses) else 0.50 if (title or emails or phones) else 0.15,
            "extractor_version": "heuristic-v1",
        }
    else:
        observation["company_name"] = find_best_company_name(title, observation.get("company_name"), domain)
        observation["website"] = observation.get("website") or base_url
        observation["email"] = observation.get("email") or (emails[0] if emails else None)
        observation["phone"] = observation.get("phone") or (phones[0] if phones else None)
        observation["address"] = observation.get("address") or (addresses[0] if addresses else None)
        merged_socials = set(observation.get("social_links") or [])
        merged_socials.update(extract_social_links(collector.links))
        observation["social_links"] = sorted(merged_socials)
        raw_fields = dict(observation.get("raw_fields") or {})
        raw_fields.update(
            {
                "title": title,
                "description": description,
                "emails": emails[:10],
                "phones": phones[:10],
                "addresses": addresses[:5],
                "important_page_links": important_page_links(collector.links),
                "important_page_candidates": important_candidates,
                "page_type": page_type,
            }
        )
        observation["raw_fields"] = raw_fields

    return ParsedDocument(
        observation=observation,
        discovered_links=collector.links[:max_links],
        metadata={
            "title": title,
            "description": description,
            "emails": emails[:10],
            "phones": phones[:10],
            "addresses": addresses[:5],
            "page_type": page_type,
            "important_page_links": important_page_links(collector.links),
            "important_page_candidates": important_candidates,
            "link_count": len(collector.links),
        },
    )
