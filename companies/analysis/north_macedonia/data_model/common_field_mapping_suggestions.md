# Common field mapping suggestions — North Macedonia

> Suggestion only for a future cross-country mapper. It does **not** constrain the
> country-specific North Macedonia profile, which stays keyed on the ЕМБС.

| Common field | North Macedonia source path | Notes |
|---|---|---|
| company_id | `registration.embs` (CRM ЕМБС) | 7-digit entity id |
| registration_number | `registration.embs` | = ЕМБС |
| tax_id | `tax_identifiers.edb_tax_id` (ЕДБ) | 13-digit |
| vat_id | `tax_identifiers.vat_id` (ДДВ број) | UJP; VAT registration |
| legal_name | `legal_identity.business_name` | Назив / Име |
| status | `status.status_text` | активен/ликвидација/стечај/избришан |
| legal_form | `legal_identity.legal_form` | ДОО/ДООЕЛ/АД/ТП |
| incorporation_date | not_available_in_open_sources | paid detail |
| dissolution_date | not_available_in_open_sources | paid detail |
| registered_address | `registered_location.registered_address` | paid detail |
| activity_code | `activity.activity_code` | НКД ~NACE |
| financials | `financial_statements[]` | CRM Annual Accounts, MKD — PAID (planning-only) |
| officers | `owners[]` (Управители) | PERSONAL DATA — redact; paid |
| owners | `owners[]` (Основачи) | PERSONAL DATA — redact; paid |
| source_provenance | `source_provenance[]` |  |

## Cross-country notes

- `company_id == registration_number == ЕМБС`; `tax_id == ЕДБ`; `vat_id` (ДДВ) is
  separate (UJP).
- The defining constraint is **commercial distribution**: the CRM sells the
  register and financials (`blocked_payment`); only a free basic search is open.
- **Financial statements exist for all companies** (CRM Registry of Annual
  Accounts) — a strength — but they are **paid**. Currency **MKD**.
- Treat managers/founders as personal data — redact in shared outputs.
