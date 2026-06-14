# APR Automated Data Delivery Web-Service Field Catalog

> **PLANNING-ONLY / PAID.** Cataloged from public APR service descriptions only.
> Free for state bodies; paid (contract) for banks/businesses. No raw records or
> values retrieved. Fields below are the concepts the **open data omits**.

## Source Summary

- Country: Serbia
- Source type: official_registry
- Organization: Agencija za privredne registre (APR)
- URL: https://www.apr.gov.rs/usluge/epodaci-na-zahtev-korisnika/automatizovano-izdavanje-podataka-(veb-servis).2413.html
- License: contract / terms of use
- Access: restricted (authentication + payment; contract; contact apr-podaci@apr.gov.rs)
- Freshness: real-time / continuous
- Record shape: planning-only
- Primary keys: `maticni_broj`
- Join keys: `maticni_broj`, `pib`

## Fields (concepts from public docs)

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| pib | PIB | Tax id (9-digit) | string | identifier | planning-only; the key open gap |
| preduzetnici[] | preduzetnici | Sole traders | array | identifier | planning-only; not in open data |
| zastupnici[] | zastupnici/direktori | Directors | array | person | planning-only; PII |
| stvarni_vlasnici[] | stvarni vlasnici | Beneficial owners | array | ownership | planning-only; PII |
| finansijski_izvestaji_istorija[] | RGFI history | Multi-year financials | array | financial | planning-only |

## Interpretation Notes

- This paid web service is the route to everything the open feed lacks: **PIB
  (tax id/VAT)**, **sole traders (preduzetnici)**, **directors/representatives**,
  **beneficial owners** (Central Register of Beneficial Owners), and **multi-year
  financial history**.
- Keep entirely **planning-only**; do not synthesise values. Free for state
  bodies, otherwise per contract.
