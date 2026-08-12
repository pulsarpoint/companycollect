SELECT country_code, company_id, section
FROM {{ ref('company_section_presence_current_build') }}
WHERE section NOT IN (
  'gleif', 'wikidata', 'management', 'descriptions', 'domains', 'contracts',
  'financials', 'industries', 'addresses', 'sources', 'technology'
) OR item_count = 0
