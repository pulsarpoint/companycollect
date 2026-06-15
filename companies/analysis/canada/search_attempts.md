# Canada — Search Attempts

## Attempt 1
- Date/time: 2026-06-15
- Source: WebSearch
- Query: `Corporations Canada federal corporation open data open.canada.ca download CSV corporation number directors`
- Result: Corporations Canada "Federal Corporations" dataset on open.canada.ca (CSV, split into parts), real-time API with status/address/directors; covers CBCA business corporations, NFP, cooperatives, etc.; ISED.
- Decision: query CKAN for resource URLs.

## Attempt 2
- Date/time: 2026-06-15
- Source: WebSearch
- Query: `Canada company financial statements SEDAR+ reporting issuers ... Quebec REQ BC OrgBook open data`
- Result: SEDAR+ (CSA) = free open access to public-company financial statements/filings; no national regulator (13 provincial regulators). Provincial registries separate.
- Decision: financials = SEDAR+ (reporting issuers); note provincial fragmentation.

## Attempt 3
- Date/time: 2026-06-15
- Source: curl (open.canada.ca CKAN package_search + package_show)
- Result: dataset id 0032ce54 (org ic = ISED), **OGL**. Resources: active/inactive × CBCA/non-CBCA CSVs (EN+FR) on CloudFront.
- Decision: download the active CBCA EN CSV.

## Attempt 4
- Date/time: 2026-06-15
- Source: curl + python
- Query: download corporations-active-cbca-en.csv (102 MB)
- Result: **642,720** active CBCA corporations; 17 columns (Corporation number, BN, names EN/FR, legislation, status, anniversary date, full address, annual filing/meeting, director counts). Real record: MINDANGLER CAPITAL INC. (corp # 8660115, BN 835752437, Ottawa ON).
- Decision: Corporations Canada federal dataset = recommended; note it covers federally-incorporated only.
