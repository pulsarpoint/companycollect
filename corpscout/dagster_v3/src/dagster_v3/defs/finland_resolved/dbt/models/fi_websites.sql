{{ config(materialized='table') }}

select
  business_id,
  website_url,
  website_normalized_url,
  website_host,
  website_path,
  try_cast(nullif(website_registered_on, '') as date) as registered_on,
  try_cast(nullif(website_ended_on, '') as date) as ended_on,
  website_ended_on is null or website_ended_on = '' as is_current,
  true as is_primary,
  source_slug as source_system,
  source_run_id,
  source_record_id,
  source_payload_hash,
  now() as resolved_at
from {{ source('finland_prhytj', 'all_companies') }}
where business_id is not null
  and business_id != ''
  and website_normalized_url is not null
  and website_normalized_url != ''
