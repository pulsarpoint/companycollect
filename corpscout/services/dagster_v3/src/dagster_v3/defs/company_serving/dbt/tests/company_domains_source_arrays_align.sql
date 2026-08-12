SELECT
    country_code,
    company_id,
    root_domain
FROM {{ ref('company_domains_build') }}
WHERE length(source_names) != length(source_confidences)
   OR length(source_names) != length(source_record_ids)
   OR length(source_names) != length(source_urls)
   OR length(source_names) != length(confidence_bases)
   OR length(source_names) = 0
