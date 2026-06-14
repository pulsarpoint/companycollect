# Croatia — Search Attempts Log

## Attempt 1
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Sudski registar Hrvatska API sudreg open data JSON tvrtke OIB MBS pravosudje data.gov.hr`
- Language: Croatian/English
- Why this query was tried: Find the authoritative register + an open API/bulk.
- Top relevant URLs:
  - https://data.gov.hr/ckan/en/dataset/sudski-registar
  - https://sudreg-podaci.pravosudje.hr/docs/services
  - https://sudreg-data.gov.hr
- Result: Sudski registar (Ministry of Justice) has an OPEN REST/OpenAPI; FREE registration → Client ID/Secret + Ocp-Apim-Subscription-Key. Fields: court, MBS, OIB, status, name, address, share capital, legal form.
- Decision: Sudski registar API = the open company spine.

## Attempt 2
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `FINA RGFI registar godišnjih financijskih izvještaja javna objava download data financial statements Croatia API`
- Language: Croatian/English
- Why this query was tried: Find financial-statement access + format.
- Top relevant URLs:
  - https://www.fina.hr/eng/public-services-business/registries/annual-financial-statements-registry-rgfi
  - http://rgfi.fina.hr/JavnaObjava-web
  - https://data.gov.hr/ckan/dataset/registar-godisnjih-financijskih-izvjestaja-javna-objava
- Result: FINA RGFI javna objava — free after registration; balance sheet + income statement (abbreviated) + notes available as MACHINE-READABLE OPEN CSV (esp. micro/small). data.gov.hr CKAN dataset. Fuller FINA products paid.
- Decision: FINA RGFI = open structured financials.

## Attempt 3 (live probing)
- Date/time: 2026-06-14
- Source: curl (sudreg API) + data.gov.hr CKAN package_show
- Result:
  - sudreg-data.gov.hr returned an Oracle APIM 404 for a guessed path (API behind a subscription key).
  - CKAN package_show confirmed BOTH datasets under license "Otvorena dozvola (OD)": "sudski-registar" (resource = sudreg-data.gov.hr portal) and "registar-godisnjih-financijskih-izvjestaja-javna-objava" (CSV resources whose URLs point to the FINA RGFI login page).
- Decision: Both core sources are OPEN-LICENSED but behind a FREE registration. Could not download a per-company sample here (sudreg API key; FINA login). Documented; structures from the OpenAPI + RGFI standard forms. Schematic normalized sample.
