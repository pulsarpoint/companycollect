{{ config(materialized='table') }}

with normalized as (
  select
    *,
    normalized_url(website) as website_normalized_url,
    website_host(website) as website_host,
    website_path(website) as website_path,
    root_domain(website) as root_domain
  from {{ source('norway_brreg', 'entities') }}
)

select
  org_number,
  website as website_url,
  website_normalized_url,
  website_host,
  root_domain,
  nullif(website_path, '') as website_path,
  cast(null as date) as registered_on,
  cast(null as date) as ended_on,
  true as is_current,
  true as is_primary,
  source_slug as source_system,
  source_run_id,
  source_record_id,
  source_payload_hash,
  now() as resolved_at
from normalized
where org_number is not null
  and org_number != ''
  and website_normalized_url is not null
  and website_normalized_url != ''
  and root_domain is not null
  and root_domain != ''
