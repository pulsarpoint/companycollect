{{ config(materialized='table') }}

select
  org_number,
  country_iso2,
  legal_name as name,
  lower(legal_name) as name_normalized,
  try_cast(nullif(registration_date, '') as date) as registration_date,
  try_cast(nullif(incorporation_date, '') as date) as incorporation_date,
  status as lifecycle_status,
  coalesce(is_active, false) as is_active,
  nullif(legal_form_code, '') as legal_form_code,
  nullif(legal_form_description_original, '') as legal_form_description_original,
  nullif(company_description_original, '') as company_description_original,
  nullif(articles_purpose_original, '') as articles_purpose_original,
  nullif(activity_text_original, '') as activity_text_original,
  nullif(normalized_url(website), '') as primary_website_url,
  nullif(website_host(website), '') as primary_website_host,
  source_slug as source_system,
  source_run_id,
  source_record_id,
  source_payload_hash,
  now() as resolved_at
from {{ source('norway_brreg', 'entities') }}
where org_number is not null
  and org_number != ''
