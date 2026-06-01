ALTER TABLE data_sources
    ADD COLUMN display_name text NOT NULL DEFAULT '',
    ADD COLUMN description  text NOT NULL DEFAULT '';

UPDATE data_sources SET
    display_name = 'Norwegian Business Register',
    description  = 'Official registry of all businesses registered in Norway, operated by the Brønnøysund Register Centre. Covers approximately 1 million entities including limited companies (AS), sole traders, and other organisational forms. Provides organisation number, company name, status, and registered website. Data is publicly available with no authentication required.'
WHERE name = 'brreg';

UPDATE data_sources SET
    display_name = 'UK Companies House',
    description  = 'The official register of UK companies, maintained by Companies House (an executive agency of the UK government). Contains over 5 million active and dissolved companies including limited companies, LLPs, and public limited companies. Provides company number, registered name, status, company type, and registered address. Requires a free API key.'
WHERE name = 'companies_house';

UPDATE data_sources SET
    display_name = 'Danish Central Business Register (CVR)',
    description  = 'Det Centrale Virksomhedsregister — the authoritative register of all Danish businesses, maintained by the Danish Business Authority. Covers approximately 900,000 active entities including limited companies (A/S, ApS), sole traders, and associations. Provides CVR number, company name, status, address, and website. Requires a free API token.'
WHERE name = 'cvr';

UPDATE data_sources SET
    display_name = 'Global Legal Entity Identifier Foundation (GLEIF)',
    description  = 'Maintained by the Global LEI Foundation, this database contains over 3 million Legal Entity Identifiers (LEIs) assigned to organisations participating in global financial markets. An LEI is a 20-character ISO 17442 code that uniquely identifies a legal entity worldwide. Particularly strong coverage of financial institutions, investment funds, and publicly traded companies. No authentication required.'
WHERE name = 'gleif';

UPDATE data_sources SET
    display_name = 'Estonian Business Register (Äriregister)',
    description  = 'Official registry of all companies registered in Estonia, maintained by the Centre of Registers and Information Systems (RIK). Contains all legal entities including private limited companies (OÜ), public limited companies (AS), sole traders, and non-profit associations. Data is published daily as a public open-data bulk download with no authentication required.'
WHERE name = 'ariregister';

UPDATE data_sources SET
    display_name = 'OpenCorporates',
    description  = 'The largest open database of companies in the world, aggregating company registration data from over 140 jurisdictions. Normalises records from national registries into a consistent format. Particularly useful for cross-border research and discovering companies in countries without their own dedicated adapter. An API key is optional but strongly recommended — anonymous rate limits are very restrictive.'
WHERE name = 'opencorporates';

UPDATE data_sources SET
    display_name = 'Wikidata',
    description  = 'The free, collaborative knowledge base maintained by the Wikimedia Foundation. Contains structured data about companies that have been added by volunteers from Wikipedia and other sources. Coverage is uneven — strongest for large, well-known corporations — but provides website URLs and country associations that other registries often lack. Data is queried via the SPARQL endpoint and is subject to public rate limits.'
WHERE name = 'wikidata';
