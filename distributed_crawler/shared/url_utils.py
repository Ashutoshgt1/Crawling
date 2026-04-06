from __future__ import annotations

import hashlib
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


TRACKING_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
    "fbclid",
    "ref",
    "ref_src",
    "srsltid",
}


def canonicalize_url(url: str) -> str:
    parts = urlsplit(url.strip())
    scheme = (parts.scheme or "https").lower()
    netloc = parts.netloc.lower()
    path = parts.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    query_items = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=False)
        if key.lower() not in TRACKING_PARAMS
    ]
    normalized_query = urlencode(sorted(query_items))
    return urlunsplit((scheme, netloc, path, normalized_query, ""))


def hash_url(url: str) -> bytes:
    return hashlib.sha256(canonicalize_url(url).encode("utf-8")).digest()


def extract_host(url: str) -> str:
    return urlsplit(url).netloc.lower()


def guess_etld1(host: str) -> str:
    parts = [part for part in host.split(".") if part]
    if len(parts) < 2:
        return host
    return ".".join(parts[-2:])
