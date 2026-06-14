# Cyprus — License & Terms Notes

> Cyprus's company identity is open data; financials are public but paid + document-based. Record attribution.

## DRCIP Registrar — open data (data.gov.cy)
- The DRCIP publishes the company list (+ officers) as **open data** on **data.gov.cy** (Registrar group #30).
  Free reuse under the portal's open-data terms — **confirm the exact licence** on the dataset page before
  redistribution; attribute the DRCIP / data.gov.cy.
- The free **eSearch** gives basic info per company (single lookups).

## HE32 + audited financial statements
- Filed in the register and **public**, but the scanned annual returns + financial statements are obtained
  via a **paid detailed search (€10 per company)**. No bulk redistribution rights implied; documents are
  individually paid. **Document-based (PDF)** — no structured open figures.

## data.gov.cy
- Licence is **per dataset** (open data). Check each dataset's licence field. (CKAN-like API on a
  non-standard path here.)

## UBO register (beneficial ownership)
- **Restricted** (access conditions / fee, post-CJEU). **Not** open.

## OpenSanctions cy_companies
- A FollowTheMoney mirror of the DRCIP open CSV under **CC-BY-NC 4.0** (commercial use needs a separate
  OpenSanctions licence). The authoritative open source is the **data.gov.cy CSV** itself (use that for
  commercial reuse, under its open terms).

## Tax Department
- TIC / VAT validation only (VIES). Not redistributable as a list.

## Commercial aggregators
- Proprietary, paid, per-vendor contract. They resell DRCIP register + parsed financial statements.

## Personal data / GDPR
- The open CSV **names officers** (directors/secretary) — personal data; apply a GDPR lawful basis +
  retention policy before persisting. Beneficial owners are restricted.

## Summary recommendation
- **Free to use (with attribution)**: the **DRCIP open CSV** (data.gov.cy) + free eSearch — confirm the exact
  open licence.
- **Paid/restricted**: financial statements (€10 detailed search); UBO; commercial aggregators. For commercial
  reuse of the company list, use the **data.gov.cy** source (not the CC-BY-NC OpenSanctions mirror).
