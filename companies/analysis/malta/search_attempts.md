# Malta — Search Attempts Log

## Attempt 1
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Malta Business Registry MBR company search open data data.gov.mt registry download annual accounts financial statements API beneficial owners`
- Language: English
- Why this query was tried: Identify the register, any open bulk/API, and financials access.
- Top relevant URLs:
  - https://mbr.mt/
  - https://baros.mbr.mt/app/home
  - https://opencorporates.com/registers/152
- Result: MBR = authoritative; free basic search; certified documents EUR 5-25; register holds officers, shareholders, financial info, status; MBR has launched API packages; UBO restricted to legitimate interest (July 2025).
- Decision: Check data.gov.mt for open company data; probe MBR portals + API.

## Attempt 2
- Date/time: 2026-06-14
- Source: curl (live) — data.gov.mt API + MBR portals
- Query: data.gov.mt CKAN/uData package_search; mbr.mt; registry.mbr.mt; baros.mbr.mt
- Result: data.gov.mt API paths → 404 (non-standard) and homepage → 403 (WAF). mbr.mt → 200; registry.mbr.mt + baros.mbr.mt → 403 (WAF). MBR api-packages/services pages → 404.
- Decision: data.gov.mt is not the register + WAF-blocked; MBR registry portals WAF-blocked for automation.

## Attempt 3
- Date/time: 2026-06-14
- Source: curl (live) — MBR promo + OpenCorporates
- Query: mbr.mt/promo/company-search/ ; opencorporates.com/registers/152
- Result: Promo page confirms Free search + paid Document purchase. OpenCorporates has the Malta register (HTTP 200).
- Decision: mbr_register = recommended (manual); automation blocked (WAF) / paid (documents + API). Built a schematic normalized sample.

## Attempt 4
- Date/time: 2026-06-14
- Source: WebSearch + documentation
- Query: annual accounts / GAPSME / API packages / UBO
- Result: Annual accounts (IFRS/GAPSME) + annual return filed to MBR; accessed as paid documents (EUR 5-25). MBR API packages = subscription. UBO restricted (legitimate interest, post-CJEU). VAT = MT + 8 digits.
- Decision: mbr_annual_accounts = blocked_by_payment; mbr_api = blocked_by_payment (paid packages = sanctioned automation); rbe_register = blocked_by_authentication; vies_vat = useful_secondary; commercial aggregators = realistic bulk/financials path.
