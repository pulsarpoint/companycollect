# Serbia Company Profile — Mapping Report

Serbia is **fully-open for identity + a one-year financial summary**: APR
publishes the company register and the latest annual financial statements as
**public-domain** JSON APIs, both keyed on **matični broj** (8-digit), refreshed
monthly. The gaps (PIB/VAT, address, directors, beneficial owners, sole traders,
multi-year history) sit behind the **paid APR web service**.

## Mapping Table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.maticni_broj | apr_companies | Podaci.<key> | maticni_broj | register | company id + join key |
| tax_identifiers.pib | apr_webservice | pib | maticni_broj | PLANNING-ONLY | not in open data |
| legal_identity.legal_name | apr_companies | PoslovnoIme | — | register > fin-stmts name | Latin |
| legal_identity.legal_form | apr_companies | NazivPravneForme | — | register | Cyrillic |
| status.status_raw / status | apr_companies | NazivStatus | — | register | map Cyrillic → enum |
| activity.kd2010_code | apr_companies | SifraDelatnosti | — | register | KD2010 ≈ NACE Rev.2 |
| incorporation.incorporation_date | apr_companies | DatumOsnivanja | — | register | ISO |
| registered_location.municipality_* | apr_companies | SifraOpstine/NazivOpstine | — | register | municipality only |
| registered_location.street_address | apr_webservice | — | maticni_broj | PLANNING-ONLY | not open |
| financial_statements[] | apr_financial_statements | PoslovnaImovina/Kapital/UkupniPrihodi/NetoDobitak/… | maticni_broj | fin-stmts | thousands RSD; latest year only |
| officers[] | apr_webservice | zastupnici[] | maticni_broj | PLANNING-ONLY | paid; PII |
| beneficial_owners[] | apr_webservice | stvarni_vlasnici[] | maticni_broj | PLANNING-ONLY | paid; PII |

## Source Precedence

1. **apr_companies** — authoritative for identity, status, legal form, activity,
   incorporation date, municipality. Public-domain.
2. **apr_financial_statements** — authoritative for the **latest** annual
   financials. Public-domain. Join on matični broj.
3. **apr_ngo** — separate entity stream (associations/foundations); not merged
   into the company profile by default.
4. **apr_webservice** — the only route to PIB, address, directors, beneficial
   owners, sole traders, and financial history → **planning-only** (paid).
5. **opencorporates** — aggregator mirror; cross-check only (restricted bulk).

On a name conflict, prefer **apr_companies** `PoslovnoIme` over the financial-
statement name.

## Join Keys

- **matični broj** (8-digit) is the single universal join key across companies,
  financial statements, NGOs, and the paid web service. There is no PIB in the
  open data, so VAT/tax joins are not possible from open sources alone.

## Missing / Restricted

- **PIB (tax id / VAT)** — not in open data (paid web service).
- **Street address** — only municipality is open.
- **Directors / shareholders / beneficial owners** — paid web service; PII.
- **Sole traders (preduzetnici)** — entirely absent from the open feed.
- **Multi-year financial history** — open feed exposes only the latest year.
- **Script**: status/legal-form/municipality are **Cyrillic** — normalise.
