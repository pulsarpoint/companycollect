# Russia — Common Field Mapping Suggestions

> Suggestion layer only for a future cross-country mapper. It does **not**
> constrain the Russia-specific profile. Identity + financials are open (GIR BO);
> the SME list is open (RSMP); directors/founders are paid (EGRUL).

| Common field | Russia mapping | Status |
|---|---|---|
| company_id | registration.ogrn (13-digit OGRN) | open (GIR BO / RSMP) |
| registration_number | registration.ogrn | open |
| tax_id | registration.inn (10-digit INN) | open |
| vat_id | not_available_in_open_sources | Russia uses the INN for VAT (НДС); no separate number |
| legal_name | legal_identity.short_name / full_name | open |
| status | status.status_code (GIR BO) / EGRUL status | open (GIR BO) |
| legal_form | legal_identity.okopf_legal_form (ОКОПФ) | open |
| incorporation_date | not_available_in_open_sources | EGRUL registration date (paid/per-company) |
| dissolution_date | not_available_in_open_sources | EGRUL status/date |
| registered_address | registered_location.registered_address | open (GIR BO) |
| activity_code | activity.okved_code (ОКВЭД2) | open |
| financials | financial_statements[] (GIR BO; balance + income, RUB) | open |
| officers | officers[] (EGRUL directors/founders) | paid / per-company; personal data (152-ФЗ) |
| owners | officers[] founders (EGRUL) | paid; personal data |
| source_provenance | source_provenance[] | available |

## Notes

- **Two anchors**: `company_id`/`registration_number` -> **OGRN**; `tax_id` ->
  **INN**. Russia has **VAT (НДС)** but **no separate VAT number** — the INN is the
  tax id; mark `vat_id` as not available.
- **Financials are open and first-class** via **GIR BO** (balance sheet + income
  statement, RUB) — a strong open financial source. Banks are excluded (Central
  Bank). The SME register (RSMP) adds the broad company list + category + headcount.
- **Directors/founders/capital/history** are in **EGRUL** — free per-company
  extract, paid full bulk; **personal data (152-ФЗ)**, redact.
