# South Africa — Search Attempts

## Attempt 1

- Date/time: 2026-06-16
- Source: CIPC, BizPortal, data.gov.za, eTenders, JSE
- URL: cipc.co.za ; bizportal.gov.za ; data.gov.za ; etenders.gov.za ; jse.co.za
- Language: English
- Why: CIPC is the registrar; check the open-data portal, procurement, and the exchange.
- Result: CIPC/BizPortal/eTenders/JSE 200; data.gov.za 000 (unreachable); OCDS API 400 (needs params).
- Decision: Pursue the eTenders OCDS API (open); CIPC registry is paid.

## Attempt 2

- Date/time: 2026-06-16
- Source: National Treasury eTenders OCDS API
- URL: https://ocds-api.etenders.gov.za/api/OCDSReleases?PageNumber=1&PageSize=3&dateFrom=2025-01-01&dateTo=2025-03-31
- Language: English
- Why: Open procurement is the realistic open source of SA company names.
- Result: HTTP 200; full OCDS releases (tender/parties/awards/contracts). License = ODC-PDDL (public domain); publisher = National Treasury. Real suppliers + ZAR award values.
- Decision: RECOMMENDED (open). Used as the real sample.

## Attempt 3

- Date/time: 2026-06-16
- Source: OCDS identifier scan
- URL: …/OCDSReleases?PageSize=50
- Language: English
- Why: Check whether supplier registration numbers (CIPC) appear in OCDS.
- Result: supplier parties carry only `legalName` — no registration-number identifier observed.
- Decision: OCDS gives names only; joining to CIPC needs name matching.

## Attempt 4

- Date/time: 2026-06-16
- Source: CIPC BizPortal
- URL: https://www.bizportal.gov.za/
- Language: English
- Why: Check for a free CIPC company search.
- Result: BizPortal is a company-registration service ("register your company in 1 day"), not a free search; CIPC search/disclosures/AFS are paid (eServices, customer code).
- Decision: blocked_by_payment (registry).

## Attempt 5

- Date/time: 2026-06-16
- Source: CSD + JSE
- URL: https://secure.csd.gov.za/ ; https://www.jse.co.za/
- Language: English
- Why: Government supplier registry + listed financials.
- Result: CSD login-gated (mandatory government-supplier DB); JSE/SENS = public listed-company disclosures.
- Decision: CSD blocked_by_authentication; JSE useful_secondary_source (listed financials).
