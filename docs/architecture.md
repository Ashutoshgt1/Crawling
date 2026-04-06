# Crawler Platform Starter

This repo now has a build-first platform starter around the existing Playwright crawler.

## System shape

- `crawler.py`
  Current single-node search-result crawler and metadata extractor.
- `docker-compose.yml`
  Local infrastructure stack for the next stage of development.
- `sql/001_init.sql`
  Canonical relational schema for URL state, fetches, observations, and companies.

## What we build

- Crawl frontier
- Scheduler and politeness controller
- Retry and recrawl planner
- Company observation pipeline
- Entity resolution and canonical company profiles
- Source scoring and domain intelligence

## What we borrow

- Playwright for browser automation
- Postgres for canonical relational state
- Redis for hot coordination and rate budgets
- Kafka for event transport
- MinIO for raw artifacts
- ClickHouse for analytics
- OpenSearch for search

## Local development flow

1. Start infrastructure with `docker compose up -d`.
2. Postgres auto-loads the SQL schema from `sql/001_init.sql`.
3. Keep using `crawler.py` for data collection while building the platform services.
4. Next services to add:
   - `frontier-service`
   - `scheduler-service`
   - `parse-service`
   - `resolver-service`

## Recommended service split

- Go:
  - frontier
  - scheduler
  - HTTP fetcher
- Node.js:
  - browser fetcher
- Python:
  - parser
  - resolver
  - recrawl planner

## First production-grade loop

1. Seed URLs enter `urls` with state `ready`.
2. Scheduler leases `ready` URLs by host/domain budget.
3. Fetchers write raw artifacts and `fetch_results`.
4. Parser emits `observations` plus discovered child URLs.
5. Resolver merges observations into `companies`.
6. Recrawl planner updates `next_fetch_at`.

## Priority guidance

Use host-aware scoring, not a single global queue.

Suggested first formula:

```text
priority =
  0.30 * source_quality +
  0.25 * entity_likelihood +
  0.20 * freshness_urgency +
  0.15 * path_value +
  0.10 * link_context_score -
  0.20 * host_risk_score -
  0.10 * crawl_cost -
  0.05 * depth
```

## Notes

- Browser crawling should stay an escalation path, not the default path.
- Raw HTML and evidence should be stored so extraction logic can be replayed.
- `observations` and `companies` are intentionally separate. This is essential for debugging and reliable merges.
