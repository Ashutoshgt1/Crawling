create table if not exists urls (
    id bigserial primary key,
    canonical_url text not null,
    url_hash bytea not null unique,
    scheme text,
    host text not null,
    etld1 text,
    path text,
    query_norm text,
    source_type text,
    parent_url_hash bytea,
    depth int default 0,
    priority double precision default 0,
    state text not null,
    render_mode text not null,
    discovery_score double precision default 0,
    entity_score double precision default 0,
    host_risk_score double precision default 0,
    freshness_score double precision default 0,
    retry_count int default 0,
    next_fetch_at timestamptz,
    leased_until timestamptz,
    last_fetched_at timestamptz,
    http_status int,
    content_hash bytea,
    created_at timestamptz default now(),
    updated_at timestamptz default now()
);

create index if not exists idx_urls_host_state_nextfetch on urls(host, state, next_fetch_at);
create index if not exists idx_urls_etld1 on urls(etld1);
create index if not exists idx_urls_state_priority on urls(state, priority desc);

create table if not exists domain_state (
    host text primary key,
    etld1 text,
    robots_fetched_at timestamptz,
    robots_etag text,
    crawl_delay_ms int default 1000,
    max_parallelism int default 1,
    preferred_render_mode text default 'http',
    preferred_proxy_class text default 'direct',
    success_rate double precision default 1.0,
    challenge_rate double precision default 0.0,
    avg_latency_ms double precision default 0,
    ban_score double precision default 0,
    last_blocked_at timestamptz,
    session_required boolean default false,
    recrawl_interval_hours int default 168,
    updated_at timestamptz default now()
);

create table if not exists fetch_results (
    id bigserial primary key,
    url_hash bytea not null,
    fetched_at timestamptz not null default now(),
    worker_type text not null,
    proxy_class text,
    status_code int,
    final_url text,
    content_type text,
    response_bytes bigint,
    latency_ms int,
    blocked boolean default false,
    block_reason text,
    html_object_key text,
    screenshot_object_key text,
    headers_json jsonb,
    meta_json jsonb
);

create index if not exists idx_fetch_results_urlhash on fetch_results(url_hash, fetched_at desc);

create table if not exists observations (
    id bigserial primary key,
    url_hash bytea not null,
    observed_at timestamptz not null default now(),
    company_name text,
    website text,
    email text,
    phone text,
    address text,
    city text,
    state_region text,
    country text,
    postal_code text,
    industry text,
    description text,
    social_links jsonb,
    raw_fields jsonb,
    confidence double precision not null,
    extractor_version text not null,
    evidence_object_key text
);

create index if not exists idx_observations_name on observations(company_name);
create index if not exists idx_observations_website on observations(website);
create index if not exists idx_observations_phone on observations(phone);

create table if not exists companies (
    id bigserial primary key,
    canonical_name text not null,
    primary_domain text,
    primary_email text,
    primary_phone text,
    primary_address text,
    city text,
    state_region text,
    country text,
    postal_code text,
    industry text,
    description text,
    confidence double precision not null,
    status text not null default 'active',
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_companies_domain on companies(primary_domain);
create index if not exists idx_companies_name on companies(canonical_name);

create table if not exists company_observation_links (
    company_id bigint not null references companies(id),
    observation_id bigint not null references observations(id),
    match_score double precision not null,
    match_reason jsonb,
    linked_at timestamptz default now(),
    primary key (company_id, observation_id)
);

create table if not exists crawl_jobs (
    id bigserial primary key,
    job_type text not null,
    status text not null,
    input_json jsonb,
    started_at timestamptz,
    finished_at timestamptz,
    error_text text
);
