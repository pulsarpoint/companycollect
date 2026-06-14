# Company data sources for Czech Republic

## Status

- Official bulk data: **found** (Justice Veřejný rejstřík open data — `dataor.justice.cz`, full+actual XML/CSV per legal form/court/year)
- Official API: **found** (ARES REST API — `ares.gov.cz`, per-IČO GET + `/vyhledat` search)
- Open data portal: **found** (NKOD / `data.gov.cz`; MF open-data `data.mf.gov.cz`)
- License: **open, exact terms to confirm** (CKAN package license field empty; ARES open data terms to confirm)
- Recommended ingestion path: **hybrid — Justice VR bulk for the deep register + ARES API for enrichment/lookup**

## Best source

Two official open sources combine into a very rich profile, both keyed on **IČO** (8-digit company id):

1. **ARES API** (Ministry of Finance) — the aggregator: one GET returns name, DIČ (VAT), legal form,
   fully structured address (with obec/okres/kraj codes), CZ-NACE activity, and per-register status. Search
   via POST `/vyhledat` with paging. Verified live (Alza.cz a.s., IČO 27082440).
2. **Veřejný rejstřík bulk** (Ministry of Justice, `dataor.justice.cz`) — the deepest structured register:
   share capital, **officers (with date of birth)**, **shareholders for a.s. (AKCIONAR)**, supervisory board,
   activity, insolvency, court file mark. Downloaded a real 15 MB / ~192 MB a.s. Prague dump (~16,758 firms).

**Financial statements** (účetní závěrka — balance sheet + income statement) are filed into the
**Sbírka listin** and are **free to view** at `or.justice.cz`, but they are **document-based PDFs** — no
official structured/XBRL bulk. Structured figures need OCR/parsing or a commercial provider.

## Next action

Resolve the exact CKAN package licence (empty field) and the ARES open-data terms; then ingest the Justice
`*-actual-*` XML dumps (all legal forms × court regions) keyed on IČO, and enrich via the ARES API. Treat
officer/shareholder DOB as personal data under GDPR.
