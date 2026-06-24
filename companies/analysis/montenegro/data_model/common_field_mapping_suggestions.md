# Common field mapping suggestions — Montenegro

> Suggestion only for a future cross-country mapper. It does **not** constrain the
> country-specific Montenegro profile, which stays keyed on the PIB.

| Common field | Montenegro source path | Notes |
|---|---|---|
| company_id | `registration.pib` (CRPS PIB) | 8-digit; = tax id |
| registration_number | `registration.registration_number` (CRPS) |  |
| tax_id | `tax_identifiers.tax_id` (= PIB) |  |
| vat_id | `tax_identifiers.vat_id` (PDV broj) | separate |
| legal_name | `legal_identity.business_name` | CRPS Naziv (or Javna preduzeća Naziv) |
| status | `status.status_text` | aktivno/likvidacija/stečaj/brisano |
| legal_form | `legal_identity.legal_form` | DOO/AD/OD/KD |
| incorporation_date | `status.registration_date` |  |
| dissolution_date | not_available_in_open_sources |  |
| registered_address | `registered_location.registered_address` |  |
| activity_code | `activity.activity_code` | KD ~NACE |
| financials | `financial_statements[]` | filed at CRPS; NOT open (planning-only) |
| officers | not_available_in_open_sources | CRPS shows founders/representatives |
| owners | `owners[]` (Osnivači) | PERSONAL DATA — redact natural persons |
| source_provenance | `source_provenance[]` |  |

## Cross-country notes

- `company_id == tax_id == PIB`; `vat_id` (PDV broj) is separate.
- The defining issue is **availability**: CRPS (the only company-level source) was
  **down (503)** and its legacy domain is **parked**, so Montenegro is
  `insufficient_transport_info` pending the portal returning — not scraping.
- The **only working open dataset** is `data.gov.me` public enterprises (a partial,
  name-keyed list).
- **No financial statements** are published openly. Currency **EUR**.
