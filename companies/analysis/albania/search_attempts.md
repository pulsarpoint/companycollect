# Albania — Search Attempts

## Attempt 1
- Date/time: 2026-06-24
- Source: QKB, opencorporates.al, opendata.gov.al, tatime.gov.al
- URL: qkb.gov.al ; opencorporates.al ; opendata.gov.al ; tatime.gov.al
- Language: Albanian
- Why: QKB is the registrar; Open Data Albania mirrors it; check the open-data portal + tax authority.
- Result: all 200.
- Decision: Pursue Open Data Albania (open) + QKB (official extract).

## Attempt 2
- Date/time: 2026-06-24
- Source: opendata.gov.al CKAN
- URL: https://opendata.gov.al/api/3/action/package_search?q=biznes
- Language: Albanian
- Why: Find a QKB business-register dataset.
- Result: 404 (CKAN API not at the standard path).
- Decision: Use opencorporates.al instead.

## Attempt 3
- Date/time: 2026-06-24
- Source: opencorporates.al (Open Data Albania)
- URL: https://opencorporates.al/sq/company/
- Language: Albanian
- Why: The open QKB company data.
- Result: company list with 4,459 NIPTs + names + per-company pages (/sq/company/{NIPT}). Fields: NIPT, name, administrator, owners, former names.
- Decision: RECOMMENDED (open). Used as the real sample.

## Attempt 4
- Date/time: 2026-06-24
- Source: QKB official register
- URL: https://qkb.gov.al/
- Language: Albanian
- Why: Authoritative per-company extract + financial statements.
- Result: free per-company extract (ekstrakt) by NIPT/name; financial statements (bilanci) filed; no open bulk.
- Decision: RECOMMENDED (official, per-company).
