# URL Crawler

This project is now optimized for larger batch runs with Playwright.

It also now includes a local platform starter for the next build phase:

- `docker-compose.yml` for self-hosted infra
- `sql/001_init.sql` for canonical crawler and entity tables
- `docs/architecture.md` for the build-first system blueprint
- `scripts/bootstrap-dev.ps1` for fast local startup

It supports:

- concurrent workers
- incremental JSONL output
- checkpoint metadata
- resume support
- retries
- page metadata fetching for every result
- optional proxy rotation

## Platform Starter

Start the local infra stack:

```powershell
.\scripts\bootstrap-dev.ps1
```

Or manually:

```powershell
docker compose up -d postgres redis kafka minio minio-init
```

Optional analytics and search services:

```powershell
docker compose --profile analytics up -d clickhouse
docker compose --profile search up -d opensearch
```

Core local endpoints:

- Postgres: `localhost:5432`
- Redis: `localhost:6379`
- Kafka: `localhost:9092`
- MinIO API: `localhost:9000`
- MinIO Console: `localhost:9001`

The initial relational schema is in [sql/001_init.sql](sql/001_init.sql).
The platform blueprint is in [docs/architecture.md](docs/architecture.md).

## First Service Loop

The repo now includes the first production-shaped loop for:

- frontier seeding
- scheduler leasing
- HTML fetch + raw artifact storage in MinIO
- parser extraction
- observation write + company resolution in Postgres

New package:

- `crawler_platform/`

New scripts:

- `python scripts/seed_urls.py --input seeds.txt`
- `python scripts/run_scheduler.py --batch-size 10`

Expected environment values are listed in `.env.example`.

Typical local flow:

```powershell
.\scripts\bootstrap-dev.ps1
python -m pip install -r requirements.txt
python scripts/seed_urls.py --input seeds.txt
python scripts/run_scheduler.py --batch-size 10
```

What this loop does:

1. inserts seed URLs into the `urls` frontier table
2. leases ready URLs from Postgres
3. fetches HTML over HTTP
4. stores raw HTML in MinIO
5. extracts links and a starter company observation
6. writes observations and resolves into `companies`

## Distributed Crawler Shape

The real large-scale path in this repo is now:

- `services/frontier/main.py`
- `services/scheduler/main.py`
- `workers/http_fetch/main.py`
- `workers/browser_fetch/main.py`
- `services/parser_resolver/main.py`
- `distributed_crawler/shared/`

This is the intended direction for the 1 crore company architecture:

- frontier owns URL state, leasing, and priority
- scheduler owns host/domain budgets and render-path decisions
- HTTP workers handle the high-throughput majority path
- browser workers are only for promoted URLs
- parser/resolver maximizes data extraction and writes canonical company records

Starter commands:

```powershell
python services/frontier/main.py --input seeds.txt
python services/scheduler/main.py --batch-size 100 --render-mode http
python workers/http_fetch/main.py --batch-size 100 --concurrency 20
python services/parser_resolver/main.py --mode parse
python services/parser_resolver/main.py --mode resolve
python workers/browser_fetch/main.py
```

Important:

- `crawler.py` remains as a legacy utility crawler
- the new `distributed_crawler/` + `services/` + `workers/` layout is the architecture path to continue building
- set `KAFKA_ENABLED=true` to split fetch, parse, and resolve into separate stages
- scheduler now applies host-aware selection before leasing and promotes risky hosts to the browser queue
- parser now prioritizes important company pages and extracts richer fields such as addresses, socials, contact links, and page types
- resolver now prefers stronger same-domain evidence from `contact`, `about`, and `leadership` pages over generic pages when selecting company fields

## MySQL Integration

You can read input company names from MySQL and save crawler output JSON back into MySQL with:

```powershell
.\.venv\Scripts\python.exe mysql_pipeline.py `
  --host 192.168.1.133 `
  --port 3306 `
  --user crawler `
  --password "YOUR_PASSWORD" `
  --database master `
  --input-table filtered_bharatfleet `
  --output-table bharatfleet_data `
  --engine google `
  --fallback-engine duckduckgo
```

What it does:

1. reads `company_name` from `master.filtered_bharatfleet`
2. skips names already present in `master.bharatfleet_data` when `--resume` is enabled
3. crawls each company name using the current Playwright crawler flow
4. inserts `(company_name, json)` into `master.bharatfleet_data`

Recommended guarded batch for search-driven crawling:

```powershell
.\.venv\Scripts\python.exe mysql_pipeline.py `
  --host 192.168.1.133 `
  --port 3306 `
  --user crawler `
  --password "YOUR_PASSWORD" `
  --database master `
  --input-table filtered_bharatfleet `
  --output-table bharatfleet_data `
  --engine google `
  --fallback-engine duckduckgo `
  --limit 20 `
  --min-delay-seconds 4 `
  --max-delay-seconds 9 `
  --batch-size 5 `
  --batch-cooldown-seconds 45 `
  --block-cooldown-seconds 180 `
  --session-max-queries 4 `
  --stop-block-rate 0.20
```

These controls reduce block risk by rotating search engines, refreshing browser sessions, adding jitter between queries, cooling down between batches, and stopping early if challenge/block signals rise too much.

## Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m playwright install chromium
```

## Input

Put one search query per line in `queries.txt`.

Example:

```text
L&T
Ford
OpenAI API
```

## Recommended Large-Batch Run

```powershell
python crawler.py `
  --input queries.txt `
  --output results.jsonl `
  --checkpoint crawler.checkpoint.json `
  --summary-output results.json `
  --concurrency 3 `
  --max-retries 3 `
  --delay 1
```

## Output Files

`results.jsonl`

- primary scalable output
- one JSON record per completed query
- safe for long-running jobs and resume

`crawler.checkpoint.json`

- progress metadata
- latest counters and last completed query ID

`results.json`

- optional aggregated JSON built from `results.jsonl`
- useful for inspection after the run

## Important Flags

- `--concurrency 5`
Run more workers in parallel. Increase carefully because search engines may rate-limit aggressive traffic.

- `--resume` or `--no-resume`
Resume is on by default. The crawler skips any query IDs already written to `results.jsonl`.

- `--proxy-file proxies.txt`
Rotate proxies across workers.

- `--engine google`
Choose the main engine.

- `--fallback-engine duckduckgo`
Use a fallback engine if the primary engine fails.

- `--channel msedge`
Prefer a local browser channel like Edge or Chrome before falling back to bundled Chromium.

## Proxy File Format

You can use either format:

```text
http://host1:port
http://user:password@host2:port
```

or:

```text
http://host1:port,username,password
http://host2:port,,
```

## Notes For 1 Lakh Queries

For very large runs:

- use JSONL output, not a single giant JSON object
- keep concurrency modest at first, such as `2` to `5`
- expect Google blocking if you push too fast from one IP
- add proxies only if you have permission to use them

Because titles are always fetched now, large runs will be slower than URL-only crawling. If throughput becomes the bottleneck, the next optimization would be switching title collection to a separate post-processing pass rather than disabling it.

Each result object now includes:

- `url`
- `title`
- `metadata.final_url`
- `metadata.domain`
- `metadata.description`
- `metadata.keywords`
- `metadata.canonical_url`
- `metadata.fetch_duration_seconds`
- `metadata.business.company_name`
- `metadata.business.headquarters_or_location_mentions`
- `metadata.business.emails`
- `metadata.business.phones`
- `metadata.business.social_profiles`
- `metadata.business.brand_domain_mapping`
- `metadata.business.classification`

The crawler also prints terminal logs for every metadata fetch, including the URL and time taken.

Each query record also now includes a smart aggregate:

- `company_profile.company_name`
- `company_profile.official_website`
- `company_profile.primary_domain`
- `company_profile.all_domains`
- `company_profile.headquarters_or_location_mentions`
- `company_profile.emails`
- `company_profile.phones`
- `company_profile.social_profiles`
- `company_profile.source_urls`
- `company_profile.resolved_singletons`

The company profile keeps collecting all multi-value fields across URLs, while singleton fields stop being re-searched once a strong value is found.
