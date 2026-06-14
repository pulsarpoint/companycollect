# Greece — Search Attempts Log

## Attempt 1
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Greece GEMI ΓΕΜΗ General Commercial Registry company data open data API businessregistry.gr financial statements ισολογισμός`
- Language: English + Greek
- Why this query was tried: Identify the authoritative register, any open API/bulk, and the financials path.
- Top relevant URLs:
  - https://www.businessportal.gr/en/home-en/
  - https://publicity.businessportal.gr/
  - https://opencorporates.com/registers/88
- Result: GEMI is the authoritative register (Law 4919/2022), free web search; an undocumented public REST API at publicity.businessportal.gr/api powers the UI.
- Decision: Probe the GEMI API gently; check data.gov.gr and financials access.

## Attempt 2
- Date/time: 2026-06-14
- Source: curl (live) — GEMI /api probing
- Query: GET /api/opendata(/companies), /api/companies/search, /api/search (+ variants), POST /api/search
- Result: opendata/companies/search paths → HTTP 404; /api/search → HTTP 500 `{"message":"Error"}` then repeated calls → **HTTP 429 Too Many Requests**. Portal HTML includes **recaptcha/api.js render token**.
- Decision: API is undocumented, rate-limited and reCAPTCHA-protected → automated/bulk access blocked; do not bypass. Recommend manual lookups only.

## Attempt 3
- Date/time: 2026-06-14
- Source: curl (live) — open data portal + provenance
- Query: data.gov.gr root/datasets; OpenSanctions gr_gemi / gr_companies
- Result: data.gov.gr root HTTP 200 (curated statistical API, token-gated; not the company register); /datasets/ 404. OpenSanctions gr_gemi & gr_companies → HTTP 404 (no FTM GEMI mirror).
- Decision: No open company bulk via the portal or an open mirror.

## Attempt 4
- Date/time: 2026-06-14
- Source: WebSearch (Greek)
- Query: `ΓΕΜΗ ισολογισμοί δημοσίευση οικονομικές καταστάσεις open data bulk download AADE ΑΦΜ μητρώο VIES`
- Result: Companies publish annual financial statements (ΕΛΠ/IFRS) + balance sheets to GEMI; they appear as documents on the company page. No bulk/open structured figures documented.
- Decision: Financials = document-based PDF on GEMI; useful_secondary.

## Attempt 5
- Date/time: 2026-06-14
- Source: curl (live) — AADE / VIES / portals
- Query: HEAD/GET AADE RgWsPublic SOAP; VIES checkVatService; businessportal.gr; publicity.businessportal.gr
- Result: AADE RgWsPublic → HTTP 200 (reachable; requires registered TaxisNet credentials). VIES → HTTP 405 (needs SOAP POST). Both portals → HTTP 200.
- Decision: AADE RgWsPublic = blocked_by_authentication (credentials). VIES = useful_secondary (VAT validation). Documented; built a schematic normalized sample (no per-company open record was lawfully downloadable).
