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
