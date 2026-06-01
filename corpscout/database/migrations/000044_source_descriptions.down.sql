UPDATE data_sources
SET description = NULL,
    updated_at = now()
WHERE name IN (
    'ai_company_profile',
    'ariregister',
    'brreg',
    'companies_house',
    'cvr',
    'gleif',
    'nvd_cpe',
    'nvd_cve',
    'opencorporates',
    'wikidata'
);
