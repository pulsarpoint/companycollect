# Common field mapping suggestions — Kosovo

> Suggestion only for a future cross-country mapper. It does **not** constrain the
> country-specific Kosovo profile, which stays keyed on the NUI.

| Common field | Kosovo source path | Notes |
|---|---|---|
| company_id | `registration.nui` (ARBK NumriUnikIdentifikues) | 9-digit; = fiscal number |
| registration_number | `registration.business_number_nrb` (NRB) |  |
| tax_id | `tax_identifiers.fiscal_number` (= NUI) | ARBK NumriFiskal / ATK FiscalNo |
| vat_id | `tax_identifiers.vat_id` (Numri i TVSH) | separate; only if VAT-registered |
| legal_name | `legal_identity.business_name` (Emri) |  |
| status | `status.status_text` | Aktiv/Pasiv/Shuar |
| legal_form | `legal_identity.legal_form` | B.I./O.P./Sh.P.K./Sh.A. |
| incorporation_date | `status.registration_date` |  |
| dissolution_date | not_available_in_open_sources |  |
| registered_address | `registered_location.registered_address` |  |
| activity_code | `activity.primary_activity` | NACE-aligned |
| financials | `capital.registered_capital` only | no financial statements published |
| officers | not_available_in_open_sources | ARBK shows owners, not directors separately |
| owners | `owners[]` (Pronarët) | PERSONAL DATA — redact |
| source_provenance | `source_provenance[]` |  |

## Cross-country notes

- `company_id == tax_id == NUI` (= Numri Fiskal); `vat_id` (Numri TVSH) is separate.
- **Access is the defining constraint**: both official sources are CAPTCHA/bearer
  gated, so Kosovo is `blocked_authentication` for programmatic ingestion — it
  needs an official ARBK/ATK data-sharing arrangement, not scraping.
- **No financial statements** are published (only registered capital).
- Currency **EUR**; data tri-lingual (sq/sr/en). Treat owners as personal data.
