SELECT gleif.country_code, gleif.company_id, gleif.lei
FROM {{ ref('company_gleif_current_build') }} AS gleif
LEFT JOIN {{ ref('company_external_identifier_current_build') }} AS identifiers
  ON identifiers.country_code = gleif.country_code
 AND identifiers.company_id = gleif.company_id
 AND identifiers.identifier_scheme = 'lei'
 AND identifiers.identifier_value = gleif.lei
WHERE identifiers.identifier_value = ''
