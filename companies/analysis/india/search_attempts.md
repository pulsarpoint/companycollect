# India — Search Attempts

## Attempt 1

- Date/time: 2026-06-15
- Source: data.gov.in (Open Government Data Platform India) + MCA portal
- Query / URL: https://www.data.gov.in/ ; https://www.mca.gov.in/
- Language: English
- Why: MCA is the company registrar; data.gov.in is India's official open-data portal.
- Result: data.gov.in HTTP 200; MCA portal HTTP 403 (WAF). data.gov.in catalog search returns MCA "Company Master Data".
- Decision: Pursue data.gov.in (open); MCA portal blocked.

## Attempt 2

- Date/time: 2026-06-15
- Source: data.gov.in internal backend search (/backend/dms/v1/...)
- Query / URL: several /backend/dms/v1 paths
- Language: English
- Why: Find MCA resource IDs.
- Result: 500 / 302 — the new portal's internal API is not directly usable.
- Decision: Switch to the documented OGD API gateway (api.data.gov.in).

## Attempt 3

- Date/time: 2026-06-15
- Source: OGD API gateway (api.data.gov.in /lists)
- Query / URL: https://api.data.gov.in/lists?api-key=<public-sample-key>&filters[org]=Ministry of Corporate Affairs&filters[title]=company master
- Language: English
- Why: Enumerate MCA Company Master Data resources.
- Result: HTTP 200 — **128** matching resources (state × year, 2015–2021), each with CIN + capital fields and a resource_id.
- Decision: RECOMMENDED. This is the open bulk/API route.

## Attempt 4

- Date/time: 2026-06-15
- Source: OGD API resource fetch
- Query / URL: https://api.data.gov.in/resource/6a6e802c-...(Nagaland 2015) and 87f853c6-...(Mizoram 2021)
- Language: English
- Why: Capture the real schema and records.
- Result: HTTP 200 — real records. 2015 vs 2021 schema variants documented. Saved.
- Decision: Used as the real sample (emails redacted).

## Attempt 5

- Date/time: 2026-06-15
- Source: GODL-India license + MCA financials
- Query / URL: https://www.data.gov.in/government-open-data-license-india ; MCA AOC-4/XBRL
- Language: English
- Why: Confirm reuse terms and the financials route.
- Result: GODL-India confirmed (free reuse incl. commercial, attribution). Full financials are pay-per-document on MCA21; listed financials via BSE/NSE.
- Decision: GODL open; financials paid (MCA) or listed-only (exchanges).
