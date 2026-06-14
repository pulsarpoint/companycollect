# Company Data Analysis For Czech Republic

## Summary

The Czech Republic is a **fully-open** country for company identity and structure, and **partially open** for
financials. Everything joins on the **IČO** (8-digit). Two official open sources, both verified live, combine
into a very rich profile: the **ARES API** (Ministry of Finance) gives clean aggregated identity (name, DIČ,
legal form, fully structured RUIAN address, CZ-NACE, per-register status), and the **Veřejný rejstřík open
bulk** (`dataor.justice.cz`) gives the deepest structure — **share capital, officers (with date of birth),
shareholders for a.s. (AKCIONAR) / members for s.r.o. (SPOLECNIK), boards, liquidation and insolvency**.
**Financial statements** (účetní závěrka) are **free to view but document-based PDF** in the Sbírka listin —
no official XBRL/CSV — so structured figures need OCR/parsing or a commercial provider.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| ares_api | ARES REST API | recommended | public | open data (confirm) | **API spine** (clean identity) |
| justice_vr_bulk | Veřejný rejstřík open bulk | recommended | public | open data (confirm) | **deep bulk spine** (officers/owners/capital) |
| justice_sbirka_listin | Sbírka listin (účetní závěrka) | useful_secondary | public | public docs | **financials** (PDF) |
| csu_res | ČSÚ RES | useful_secondary | public | open data | primary CZ-NACE, sector, size band |
| vat_register | Registr DPH / VIES | useful_secondary | public | validation | DIČ + unreliable-payer flag |
| rzp_trades | RŽP trade register | useful_secondary | public | public | trade licences, OSVČ |
| ares_opendata_bulk | ARES bulk export | useful_secondary | public | open data | full-population alternative |
| nkod_portal | NKOD / data.gov.cz | useful_secondary | public | per dataset | discovery (URLs + licences) |

## What Each Source Contributes

- **ares_api** — the clean aggregator: IČO, obchodniJmeno, DIČ, pravniForma, fully structured `sidlo` with
  RUIAN codes, czNace2008, datumVzniku/Zaniku, and `seznamRegistraci` per-register status. Verified live
  (Alza.cz a.s., IČO 27082440; search via `/vyhledat`).
- **justice_vr_bulk** — the deep open register (downloaded a real 15 MB a.s. Prague dump, ~16,758 firms):
  share capital, scope of business, board + supervisory-board members (with DOB), **shareholders/members**,
  court file mark, liquidation, insolvency. Record = `<Subjekt>` + typed `<Udaj>` keyed by `udajTyp/kod`.
- **justice_sbirka_listin** — the financial statements (rozvaha + výkaz zisku a ztráty + příloha), výroční
  zpráva and auditor report, free to view as PDF. Document-based; structured only after OCR/parsing.
- **csu_res** — primary CZ-NACE, institutional sector, employee-size band.
- **vat_register** — confirms DIČ/VAT registration and the Czech **unreliable-payer (nespolehlivý plátce)** risk
  flag; published bank accounts.
- **rzp_trades** — trade-licence detail and coverage of sole traders (OSVČ).
- **ares_opendata_bulk** — bulk export for full-population loads instead of per-IČO API calls.
- **nkod_portal** — DCAT-AP catalog to resolve exact resource URLs and licences (incl. the empty Justice
  licence).

## Proposed Country Company Profile

`country_company_profile.schema.json` is keyed on `registration.ico` and groups: `tax_identifiers`
(DIČ + VAT + unreliable-payer), `legal_identity`, `status`, `activity` (CZ-NACE + primary + free-text scope),
`incorporation`, `registered_location` (with RUIAN code), `capital` (share capital CZK + % paid),
`officers[]` (board + supervisory, **DOB → PII**), `owners[]` (shareholders/members), `financial_statements[]`
(Sbírka listin PDF — structured after OCR) and `documents[]`. Every section carries `source_provenance`. The
example record uses **real ARES values** for Alza.cz a.s. with the Justice structure illustrated; officer/owner
identities are redacted (GDPR) and financials shown empty (PDF).

## Join And Precedence Rules

- **Single join key:** IČO — normalize zero-padding (ARES padded `00006947`; Justice unpadded `3431509`).
- **DIČ** = CZ + IČO; validate via Registr DPH / VIES.
- **Precedence:** ARES authoritative for clean identity/address/status; Justice authoritative for the deep
  structure (capital, officers, owners, boards, insolvency); ČSÚ RES for the primary activity; VAT register for
  the risk flag. Financials only from the Sbírka listin (PDF).

## Missing Or Restricted Data

- **Financials** are not structured open data — PDF only (Sbírka listin); need OCR/parsing or a commercial
  provider.
- **Beneficial ownership** (Evidence skutečných majitelů) is a separate, **access-controlled** register — not
  included; registered shareholders/members (AKCIONAR/SPOLECNIK) are the open ownership signal.
- **Exact employee headcount** is not open (ČSÚ size band only).
- **GDPR:** officers and natural-person shareholders carry **date of birth** — apply a lawful basis +
  retention, no direct-marketing reuse.
- **Compliance to-dos:** confirm the exact open-data licences (empty CKAN licence; ARES terms).

## Common Mapper Notes

A future cross-country mapper can map company_id/registration_number ← IČO, tax_id/vat_id ← DIČ, legal_name/
status/legal_form/incorporation_date/dissolution_date/registered_address ← ARES, activity_code ← CZ-NACE,
officers ← Justice board/supervisory, owners ← Justice shareholders/members. Map `financials` to the Sbírka
listin (PDF, OCR step), keep `owners` distinct from beneficial owners, and mark exact headcount and beneficial
ownership as `not_available_in_open_sources`.
