SELECT links.country_code, links.company_id, links.section, links.item_key, links.source_record_uid
FROM {{ ref('company_section_item_source_links_build') }} AS links
LEFT JOIN (
  SELECT source_record_uid FROM {{ source('corpscout', 'company_source_records') }}
  UNION DISTINCT
  SELECT source_record_uid FROM {{ ref('company_serving_source_records_build') }}
) AS records USING (source_record_uid)
WHERE records.source_record_uid = ''
