# Company Data Analysis For Russia

## Summary

Russia offers **strong open financial data and identity** (GIR BO) plus an **open
SME company list** (RSMP), with the full authoritative register (EGRUL —
directors/founders/capital/history) **paid**. Keyed on the **OGRN** (13-digit
company id) and **INN** (10-digit tax id), a rich profile can be built openly:
identity, legal form (OKOPF), ownership (OKFS), activity (OKVED), address, status,
and **annual financial statements** (balance sheet + income statement, RUB), plus
SME category + headcount. The example uses real GIR BO data.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| gir_bo | ГИР БО (GIR BO) | recommended | public, no key | open (FNS) | Open identity + financial statements |
| rsmp_sme_register | РСМП (SME register) | recommended | public bulk | open (FNS) | Open SME company list |
| egrul | ЕГРЮЛ (EGRUL) | blocked_payment | free per-company / paid bulk | free/paid | Directors/founders/capital/history |
| fns_opendata_taxinfo | FNS open data (tax info) | ready | public bulk | open (FNS) | Per-INN enrichment |

## What Each Source Contributes

- **gir_bo** — open identity (INN, OGRN, KPP, OKOPF legal form, OKFS ownership,
  OKPO, OKVED, region, status) + filed **annual financial statements** (balance
  sheet form 1, income statement form 2; RUB), free API + bulk. Verified live
  (Gazprom 2021-2025, Lukoil). Banks excluded.
- **rsmp_sme_register** — the Unified SME Register: INN, OGRN, name, region, OKVED,
  SME category (micro/small/medium), date included, headcount. Open monthly bulk
  XML (~2.25 GB, XSD verified). ~6M SMEs.
- **egrul** — the authoritative full register (directors/founders, charter capital,
  status, registration date, history); free per-company extract, paid full FTP
  bulk. Planning-only; directors/founders are personal data.
- **fns_opendata_taxinfo** — per-INN open datasets (headcount, income/expense, paid
  taxes, tax regimes, disqualified persons).

## Proposed Country Company Profile

A single object keyed on `registration.ogrn` (+ INN, KPP):

- `registration` — OGRN, INN, KPP.
- `tax_identifiers` — tax_id = INN; vat_id null (no separate VAT number).
- `legal_identity` — names, OKOPF legal form, OKFS ownership.
- `status`, `activity` (OKVED), `registered_location`.
- `sme` — category, headcount, date included (RSMP).
- `financial_statements[]` — GIR BO balance + income, RUB.
- `officers[]` — directors/founders (EGRUL; paid; personal data).
- `tax_enrichment` — FNS per-INN sets.
- `source_provenance[]`.

## Join And Precedence Rules

- **Join keys**: OGRN (13-digit) + INN (10-digit) across all sources.
- **Precedence**: GIR BO (open identity + financials) > RSMP (open SME list) > FNS
  open sets (enrichment) > EGRUL (authoritative directors/founders/history, paid).
- **No VAT number** — the INN is the tax id.

## Missing Or Restricted Data

- **Directors / founders / capital / full history** — EGRUL (paid bulk; free
  per-company); personal data (152-ФЗ).
- **Bank financials** — not in GIR BO (Central Bank).
- **A separate VAT number** — Russia uses the INN.

## Common Mapper Notes

- Map `company_id`/`registration_number` -> OGRN; `tax_id` -> INN; `vat_id` not
  available.
- Map `financials` from GIR BO (balance + income, RUB) — a strong open source.
- Redact directors/founders and individual-entrepreneur data (152-ФЗ).
