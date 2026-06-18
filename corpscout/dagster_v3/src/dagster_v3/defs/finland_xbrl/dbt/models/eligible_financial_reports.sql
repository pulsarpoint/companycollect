{{ config(materialized='table') }}

select
    reports.business_id,
    reports.financial_date,
    reports.registration_date,
    companies.primary_name,
    companies.website_normalized_url,
    reports.discovery_registered_date_start,
    reports.discovery_registered_date_end,
    reports.source_run_id,
    reports.source_record_number
from {{ source('finland_prh_xbrl', 'financial_reports') }} as reports
inner join {{ source('finland_prhytj', 'all_companies') }} as companies
    on reports.business_id = companies.business_id
where companies.is_active = true
  and coalesce(companies.website_normalized_url, '') <> ''
order by reports.business_id, reports.financial_date
