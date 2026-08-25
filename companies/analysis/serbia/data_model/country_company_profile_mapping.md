# Serbia Company Profile — Mapping Report

Serbia is **fully-open for identity + a one-year financial summary**: APR
publishes the company register and the latest annual financial statements as
JSON open-data APIs under the **Serbian Open Data License (SODL 1.0)**, both keyed on **matični broj** (8-digit), refreshed
monthly. The gaps (PIB/VAT, address, representatives, sole traders and
multi-year history) require paid APR products. Beneficial ownership is a
**separate APR Central Register (CEV) source**, not part of SP3/SP4.

## Mapping Table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.maticni_broj | apr_companies | Podaci.<key> | maticni_broj | register | company id + join key |
| tax_identifiers.pib | apr_webservice | pib | maticni_broj | PLANNING-ONLY | not in open data |
| legal_identity.legal_name | apr_companies | PoslovnoIme | — | register > fin-stmts name | Latin, Cyrillic, or mixed script |
| legal_identity.legal_form | apr_companies | NazivPravneForme | — | register | Cyrillic |
| status.status_raw / status | apr_companies | NazivStatus | — | register | map Cyrillic → enum |
| activity.kd2010_code | apr_companies | SifraDelatnosti | — | register | KD2010 ≈ NACE Rev.2 |
| incorporation.incorporation_date | apr_companies | DatumOsnivanja | — | register | ISO |
| registered_location.municipality_* | apr_companies | SifraOpstine/NazivOpstine | — | register | municipality only |
| registered_location.street_address | apr_webservice | — | maticni_broj | PLANNING-ONLY | not open |
| financial_statements[] | apr_financial_statements | PoslovnaImovina/Kapital/UkupniPrihodi/NetoDobitak/… | maticni_broj | fin-stmts | thousands RSD; latest year only |
| officers.availability | mapper envelope | acquisition state | maticni_broj | required | prevents empty `records` from meaning “confirmed none” when SP3/SP4 was not acquired |
| officers.records[] | apr_webservice | SP3/SP4 schema TBD | maticni_broj | PLANNING-ONLY | paid; PII; public UI confirms name/function/masked JMBG/independent authority concepts |
| beneficial_owners.availability | mapper envelope | acquisition state | maticni_broj | required | `not_acquired`/`access_restricted` is not “no owner” |
| beneficial_owners.records[] | apr_beneficial_owners | CEV schema TBD | maticni_broj | BLOCKED-AUTH | separate eID/contract source; field contract based on current law/APR docs |

## Source Precedence

1. **apr_companies** — authoritative for identity, status, legal form, activity,
   incorporation date, municipality. Public under SODL 1.0.
2. **apr_financial_statements** — authoritative for the **latest** annual
   financials. Public under SODL 1.0. Join on matični broj.
3. **apr_ngo** — separate entity stream (associations/foundations); not merged
   into the company profile by default.
4. **apr_webservice** — PIB, address, sole traders and SP3/SP4 representatives
   → **planning-only** (paid).
5. **apr_beneficial_owners** — separate CEV owner source → **blocked by
   authentication/contract and privacy review**.
6. **apr_public_search** — manual semantic spot-check only; never an ingestion
   source because APR prohibits automated collection.
7. **opencorporates** — aggregator mirror; cross-check only (restricted bulk).

On a name conflict, prefer **apr_companies** `PoslovnoIme` over the financial-
statement name.

## Join Keys

- **matični broj** (8-digit) is the single universal join key across companies,
  financial statements, NGOs, and the paid web service. There is no PIB in the
  open data, so VAT/tax joins are not possible from open sources alone.

## Missing / Restricted

- **PIB (tax id / VAT)** — not in open data (paid web service).
- **Street address** — only municipality is open.
- **Directors / representatives** — paid SP3/SP4 products; PII.
- **Beneficial owners** — separate restricted CEV source; eID/contract and
  privacy controls required.
- **Company members/shareholders** — separate from both representatives and
  statutory beneficial owners; never infer CEV ownership from the UI section.
- **Sole traders (preduzetnici)** — entirely absent from the open feed.
- **Multi-year financial history** — open feed exposes only the latest year.
- **Script**: status/legal-form/municipality are **Cyrillic** — normalise.
