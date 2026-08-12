SELECT country_code, company_id, management_id, person_id, source_systems
FROM {{ ref('company_management_current_build') }}
WHERE person_id IS NOT NULL
  AND NOT has(source_systems, 'se_xbrl_signatures')
