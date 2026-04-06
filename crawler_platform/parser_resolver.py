from __future__ import annotations

import json
import re
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin


TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
META_DESCRIPTION_RE = re.compile(
    r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']',
    re.IGNORECASE | re.DOTALL,
)
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PHONE_RE = re.compile(r"(?:\+?\d[\d\-\s()]{7,}\d)")
JSON_LD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)


class LinkCollector(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        values = dict(attrs)
        href = values.get("href")
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
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
            if node_type in {"Organization", "Corporation", "LocalBusiness"}:
                address = node.get("address") or {}
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
                            if part
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
                    "social_links": [link for link in node.get("sameAs", []) if isinstance(link, str)],
                    "raw_fields": node,
                    "confidence": 0.9,
                    "extractor_version": "starter-jsonld-v1",
                }
    return {}


def parse_html(base_url: str, html: str) -> tuple[dict[str, Any] | None, list[str], dict[str, Any]]:
    collector = LinkCollector(base_url)
    collector.feed(html)

    title_match = TITLE_RE.search(html)
    meta_match = META_DESCRIPTION_RE.search(html)
    title = clean_text(title_match.group(1) if title_match else None)
    description = clean_text(meta_match.group(1) if meta_match else None)
    emails = sorted(set(EMAIL_RE.findall(html)))
    phones = sorted(set(PHONE_RE.findall(html)))

    observation = extract_json_ld_company(html)
    if not observation:
        observation = {
            "company_name": title,
            "website": base_url,
            "email": emails[0] if emails else None,
            "phone": clean_text(phones[0]) if phones else None,
            "address": None,
            "city": None,
            "state_region": None,
            "country": None,
            "postal_code": None,
            "industry": None,
            "description": description,
            "social_links": [],
            "raw_fields": {
                "title": title,
                "description": description,
                "emails": emails[:10],
                "phones": phones[:10],
            },
            "confidence": 0.45 if title else 0.2,
            "extractor_version": "starter-heuristic-v1",
        }

    metadata = {
        "title": title,
        "description": description,
        "emails": emails[:10],
        "phones": phones[:10],
        "link_count": len(collector.links),
    }
    return observation, collector.links, metadata
