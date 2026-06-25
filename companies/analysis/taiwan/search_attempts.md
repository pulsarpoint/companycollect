# Taiwan Search Attempts

## Attempt 1

- Date/time: 2026-06-25
- Search engine or source: direct HTTP probes (official hosts)
- Query: MOEA GCIS portal, GCIS open data, data.gov.tw, TWSE
- Language: English / Chinese
- Why this query was tried: locate the official company registry, open-data portal, and exchange.
- Top relevant URLs:
  - https://gcis.nat.gov.tw/ → 302 → /mainNew/
  - https://data.gcis.nat.gov.tw/ → 302 → /main/index
  - https://data.gov.tw/ → HTTP 200
  - https://www.twse.com.tw/en/ → HTTP 200
- Result: GCIS portal and open-data subdomain reachable; data.gov.tw and TWSE up.
- Decision: test the GCIS OpenData REST API for company basic data.

## Attempt 2

- Date/time: 2026-06-25
- Search engine or source: GCIS OpenData API
- Query: api/5F64D864-61CB-4D0D-8AD9-492047CC1EA6 with $filter on Company_Name / Business_Accounting_NO
- Language: Chinese
- Why this query was tried: confirm the company-basic-data dataset is queryable and open.
- Top relevant URLs:
  - https://data.gcis.nat.gov.tw/od/data/api/5F64D864-61CB-4D0D-8AD9-492047CC1EA6?$format=json&$filter=Business_Accounting_NO eq 22099131&$top=1 → HTTP 200
- Result: Querying by 統一編號 returns the full TSMC record (name, status, capital, paid-in
  capital, responsible person, location, registering authority, setup/change dates). The
  `Company_Name like …` filter returned an empty body (finicky); spaces in the filter must
  be URL-encoded (use curl -G --data-urlencode).
- Decision: GCIS = recommended; reliable access path is `eq` by Business_Accounting_NO.

## Attempt 3

- Date/time: 2026-06-25
- Search engine or source: TWSE OpenAPI
- Query: https://openapi.twse.com.tw/v1/opendata/t187ap03_L
- Language: Chinese
- Why this query was tried: find open listed-company basic info with a join key to GCIS.
- Top relevant URLs:
  - https://openapi.twse.com.tw/v1/opendata/t187ap03_L → HTTP 200, 1,089 records
- Result: Full open JSON; rich fields incl. 公司代號, 公司名稱, 營利事業統一編號 (join key),
  chairman/GM/spokesperson, industry, address, setup/listing dates (Gregorian), paid-in
  capital, English name, website, email, auditor. TSMC (2330) 統一編號 22099131 matches GCIS.
- Decision: TWSE = recommended; join to GCIS on the unified business number.

## Attempt 4

- Date/time: 2026-06-25
- Search engine or source: TPEx OpenAPI
- Query: https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O
- Language: English/Chinese
- Why this query was tried: cover the OTC market with the same join key.
- Top relevant URLs:
  - https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O → HTTP 200, 890 records
- Result: Full open JSON (English field names) incl. SecuritiesCompanyCode, CompanyName,
  UnifiedBusinessNo. (join key), Chairman, GeneralManager, industry, address.
- Decision: TPEx = recommended; complements TWSE for OTC.
