from __future__ import annotations


HIGH_VALUE_HINTS = (
    "/about",
    "/about-us",
    "/company",
    "/contact",
    "/contact-us",
    "/team",
    "/leadership",
    "/locations",
)

LOW_VALUE_HINTS = (
    "/privacy",
    "/terms",
    "/login",
    "/signin",
    "/cart",
    "/checkout",
)


def score_url(url: str, depth: int, source_type: str) -> float:
    lowered = url.lower()
    score = 100.0
    if source_type == "manual_seed":
        score += 40
    if any(hint in lowered for hint in HIGH_VALUE_HINTS):
        score += 35
    if any(hint in lowered for hint in LOW_VALUE_HINTS):
        score -= 30
    score -= min(depth * 5, 40)
    return max(score, 5.0)
