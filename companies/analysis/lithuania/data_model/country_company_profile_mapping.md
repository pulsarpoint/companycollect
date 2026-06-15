# Lithuania Company Profile — Source Mapping

> **Fully open, no API key.** Keyed on the **company code (įmonės kodas, 9-digit)**
> = legal-entity taxpayer code. Identity and **financial statements** both come
> from the Registrų centras JAR via the data.gov.lt Spinta API. VAT (PVM kodas) is
> a separate registration (VIES). Directors are personal data (GDPR).

## Field mapping

| Profile path | Source | Source path | Join key | Freshness | License/Access | Precedence / Notes |
|---|---|---|---|---|---|---|
| registration.company_code | rc_jar_legal_entities | ja_kodas | company_code | live | CC-BY/open | Authoritative id. |
| tax_identifiers.tax_id | rc_jar_legal_entities | ja_kodas | — | live | open | = company code. |
| tax_identifiers.vat_id | — (EU VIES) | — | — | — | not in register | PVM kodas; separate. |
| legal_identity.legal_name | rc_jar_legal_entities | ja_pavadinimas | — | live | open | Primary name. |
| legal_identity.legal_form | rc_jar_legal_entities | forma._id → Forma | — | live | open | Resolve to kodas + LT/EN name (168 forms). |
| status.status | rc_jar_legal_entities | statusas._id → Statusas | — | live | open | Resolve to kodas + LT/EN (31 statuses). |
| status.status_date | rc_jar_legal_entities | stat_data | — | live | open | |
| incorporation.registration_date | rc_jar_legal_entities | reg_data | — | live | open | |
| incorporation.deregistration_date | rc_jar_legal_entities | isreg_data | — | live | open | null = active. |
| registered_location.registered_address | rc_jar_legal_entities | buveines/Buveine.adresas | company_code | live | open | Address model (JuridinisAsmuo address often null). |
| financial_statements[] (balance) | rc_jar_balance_sheets | line items (line_name/reiksme) | juridinis_asmuo._id | annual | open | EUR; aggregate per period. |
| financial_statements[] (P&L) | rc_jar_income_statements | line items (line_name/reiksme) | juridinis_asmuo._id | annual | open | EUR; aggregate per period. |
| officers[] | rc_jar_supplementary (valdymo_organai) | management bodies | company_code | live | open | PLANNING-ONLY; personal data (GDPR) — redact. |

## Source precedence

1. **rc_jar_legal_entities** — authoritative identity (name, form, status, dates).
2. **rc_jar_balance_sheets** + **rc_jar_income_statements** — authoritative open
   financial statements (EUR), aggregated per company + period.
3. Supplementary models (`buveines` for address, `ja_kapitalas` for capital,
   `valdymo_organai` for directors) enrich; directors are personal data.

All sources are the same official register (Registrų centras) via the same API —
no conflicting mirrors. There is no aggregator dependency.

## Join keys

- **Company code (`ja_kodas`, 9-digit)** is the human/business join key across all
  models and = the taxpayer code.
- The Spinta **`_id` UUID** is the API's internal join: financial models reference
  `juridinis_asmuo._id` → `JuridinisAsmuo._id`; `forma._id` → `Forma`; `statusas._id`
  → `Statusas`.
- **VAT (PVM kodas)** is not in JAR — obtain via EU VIES.

## Missing / restricted data

- **VAT number** — separate registration (VIES), not in JAR.
- **Beneficial owners** — JANGIS (beneficial-ownership register) is access-
  controlled; not in this open JAR set.
- **Directors** — available (`valdymo_organai`) but **personal data (GDPR)** —
  redact in committed output.
- **Financials are line items** — must be aggregated per company + period; coverage
  depends on filing compliance (late/non-filers in `fa_veluojantys` /
  `fa_dokumentu_nepateike`).
