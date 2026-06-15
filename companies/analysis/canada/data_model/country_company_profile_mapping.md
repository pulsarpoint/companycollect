# Canada Company Profile — Mapping Report

Canada has **no single national register**: federal (Corporations Canada, open) +
13 provincial registries (mixed access). The federal open dataset is rich but
**federal-only**. Identifiers: **corporation number** (federal id) + **BN** (CRA
tax id, the cross-source join key); **no separate VAT**. Financials are open only
for reporting issuers (SEDAR+).

## Mapping Table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.corporation_number | corporations_canada_federal | Corporation number | corp # | federal | id (federal) |
| registration.business_number | corporations_canada_federal | Business number (BN) | BN | federal | tax + cross-source join |
| registration.provincial_registry_number | provincial_registries | registry_number | BN/name | provincial | for provincial cos |
| tax_identifiers.tax_id | corporations_canada_federal | Business number (BN) | BN | derived | = BN |
| legal_identity.legal_name | corporations_canada_federal | Corporate name - form 1 | — | federal | EN |
| legal_identity.legal_name_alt | corporations_canada_federal | Corporate name - form 2 | — | federal | FR |
| legal_identity.governing_legislation | corporations_canada_federal | Governing legislation | — | federal | CBCA |
| status.status | corporations_canada_federal | Status | — | federal > API real-time | Active/Inactive/Dissolved |
| incorporation.anniversary_date | corporations_canada_federal | Anniversary date | — | federal | ≈ incorporation |
| registered_location.* | corporations_canada_federal | Street/City/Province/Postal/Country | — | federal | full address |
| directors.min/max | corporations_canada_federal | Min/Max directors | — | federal | counts |
| directors.director_list | corporations_canada_api | directors[] | corp # | PLANNING-ONLY | names; PII |
| financial_statements[] | sedar_plus | financial statements | BN/name | PLANNING-ONLY | reporting issuers only |

## Source Precedence

1. **Corporations Canada Federal** — authoritative open identity for federal
   corporations (OGL).
2. **Corporations Canada API** — real-time status + director names (planning-only;
   PII).
3. **Provincial registries** — provincial corporations (mixed access).
4. **SEDAR+** — reporting-issuer financials (planning-only).

## Join Keys

- **Corporation number** (federal) is the federal id; **BN** (CRA) is the
  **cross-source** join key (federal ↔ provincial ↔ SEDAR+). `vat_id` not
  available (GST/HST = BN+RT). Provincial companies use a **provincial registry
  number** (e.g. Québec NEQ) and are not in the federal dataset.

## Missing / Restricted

- **Provincial companies** — not in the federal dataset; need provincial registries
  (mixed/paid).
- **Director names** — federal API / provincial (PII; planning-only here).
- **NAICS / activity code** — not in the federal dataset.
- **Financials** — reporting-issuers only (SEDAR+); private not public.
- **Beneficial owners** — no public register (the ISED federal BO registry is being
  established).
