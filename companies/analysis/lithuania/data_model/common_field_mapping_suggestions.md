# Lithuania — Common Field Mapping Suggestions

> Suggestion layer only for a future cross-country mapper. It does **not**
> constrain the Lithuania-specific profile. Everything below is **open** (no key),
> except the VAT number (VIES) and directors (GDPR personal data).

| Common field | Lithuania mapping | Status |
|---|---|---|
| company_id | registration.company_code (įmonės kodas, 9-digit) | open |
| registration_number | registration.company_code | open |
| tax_id | registration.company_code (legal-entity taxpayer code) | open |
| vat_id | not_available_in_open_register | PVM kodas (LT+digits) via EU VIES |
| legal_name | legal_identity.legal_name (ja_pavadinimas) | open |
| status | status.status (Statusas code list, LT+EN) | open |
| legal_form | legal_identity.legal_form (Forma code list, LT+EN) | open |
| incorporation_date | incorporation.registration_date (reg_data) | open |
| dissolution_date | incorporation.deregistration_date (isreg_data) | open |
| registered_address | registered_location.registered_address (Buveine) | open |
| activity_code | not_available_in_open_sources | no NACE/EVRK code in the JAR base models observed |
| financials | financial_statements[] (BalansoAtaskaita + PelnoAtaskaita) | open; EUR; line items aggregated per period |
| officers | officers[] (valdymo_organai) | open but personal data (GDPR) — redact |
| owners | not_available_in_open_sources | beneficial owners (JANGIS) access-controlled |
| source_provenance | source_provenance[] | available |

## Notes

- **Single anchor**: the company code is simultaneously `company_id`,
  `registration_number`, and `tax_id`. The VAT number is separate (VIES); do not
  expect it in the register.
- **Financials are first-class and open** for Lithuania (balance sheet + P&L,
  EUR) — but they arrive as **granular line items**; aggregate per company +
  period before mapping `financials`.
- **Code lists**: resolve `forma`/`statusas` UUID references against the `Forma`
  (168) and `Statusas` (31) models, which carry both **Lithuanian and English**
  labels.
- **Personal data**: directors (`valdymo_organai`) are personal data under the
  **GDPR** — redact in any committed sample.
