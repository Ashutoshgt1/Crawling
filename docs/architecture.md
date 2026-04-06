# Crawler Platform Starter

This repo now has a build-first platform starter around the existing Playwright crawler.

## System shape

- `crawler.py`
  Current single-node search-result crawler and metadata extractor.
- `distributed_crawler/`
  Shared core for the new distributed crawler stack.
- `services/frontier/main.py`
  Frontier seed and control entry point.
- `services/scheduler/main.py`
  Scheduler and host-budget entry point.
- `workers/http_fetch/main.py`
  Async high-throughput HTTP fetch worker.
- `workers/browser_fetch/main.py`
  Browser escalation worker placeholder for Playwright-only promoted URLs.
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
4. Primary code path to extend:
   - frontier service
   - scheduler service
   - HTTP fetch workers
   - browser fetch workers
   - parser / resolver logic

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
2. Frontier leases work in batches.
3. Scheduler constrains lease execution by host/domain budget.
4. Async HTTP workers fetch the majority path and store raw HTML.
5. If Kafka is enabled, fetch workers emit fetch events and parser/resolver stages run independently.
6. Parser emits `observations` plus discovered child URLs and important-page hints.
7. Resolver merges observations into `companies` with confidence-aware field preservation.
8. Browser workers are reserved for URLs promoted by scheduler policy.

## Accuracy strategy

- Prefer official-site observations over weak generic pages.
- Extract across page families such as `about`, `contact`, `leadership`, and `locations`.
- Preserve provenance in `observations` and merge into `companies` with confidence details.
- Promote hard or JS-dependent hosts to browser mode through `domain_state`.
- Weight field selection by page type so contact/about evidence wins over generic homepages when values conflict.

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
