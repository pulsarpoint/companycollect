{{ config(materialized='table') }}

select
  business_id,
  country_iso2,
  primary_name as name,
  lower(primary_name) as name_normalized,
  try_cast(nullif(registration_date, '') as date) as registration_date,
  try_cast(nullif(end_date, '') as date) as end_date,
  lifecycle_status,
  coalesce(is_active, false) as is_active,
  cast(null as varchar) as legal_form_code,
  cast(null as varchar) as legal_form_description_original,
  cast(null as varchar) as legal_form_description_language,
  cast(null as varchar) as legal_form_description_en,
  cast(null as timestamp) as legal_form_description_translated_at,
  cast(null as varchar) as legal_form_description_translation_provider,
  cast(null as varchar) as legal_form_description_translation_model,
  nullif(website_normalized_url, '') as primary_website_url,
  nullif(website_host, '') as primary_website_host,
  source_slug as source_system,
  source_run_id,
  source_record_id,
  source_payload_hash,
  now() as resolved_at
from {{ source('finland_prhytj', 'all_companies') }}
where business_id is not null and business_id != ''
