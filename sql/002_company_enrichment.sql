alter table companies
    add column if not exists social_profiles jsonb default '[]'::jsonb,
    add column if not exists source_urls jsonb default '[]'::jsonb,
    add column if not exists confidence_details jsonb default '{}'::jsonb;

alter table observations
    add column if not exists page_type text;
