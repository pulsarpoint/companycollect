# Common Field Mapping Suggestions — Georgia

This file is a **suggestion** for a future cross-country mapper. It does **not**
constrain the country-specific Georgia profile, which is the source of truth.

Georgia's sources are gated (NAPR = CAPTCHA; reportal = anti-forgery token; data.gov.ge
firewalled here), so these mappings are **planning-only** until an access path is established.

| Common field | Georgia mapping | Source | Notes |
|---|---|---|---|
| company_id | registration.identification_code | napr_enreg | 9-digit ID code |
| registration_number | registration.identification_code | napr_enreg | same code |
| tax_id | registration.identification_code | napr_enreg | the ID code IS the tax id |
| vat_id | registration.identification_code | napr_enreg | no separate VAT id; ID code used |
| legal_name | legal_identity.legal_name | napr_enreg | reportal orgName as alt |
| status | status.status_text | napr_enreg | active/liquidated |
| legal_form | legal_identity.legal_form | napr_enreg | შპს/სს/ი.მ |
| incorporation_date | status.registration_date | napr_enreg | Gregorian |
| dissolution_date | status (liquidation) | napr_enreg | status only; planning-only |
| registered_address | registered_location.registered_address | napr_enreg | CAPTCHA-gated |
| activity_code | activity.nace_codes | reportal_saras | NACE Rev.2 |
| financials | financial_statements | reportal_saras | PDF; GEL; token-gated |
| officers | officers | napr_enreg | **REDACT — personal data** |
| owners | owners | napr_enreg | **REDACT — personal data** |
| source_provenance | source_provenance | all | per-section |

Concepts notes for Georgia:

- `tax_id` / `vat_id` — there is **no separate** number; the **9-digit identification code**
  serves as registration number and tax id.
- `financials` — available as **PDF** filings at reportal.ge (browser/token), not structured.
- `officers` / `owners` — available only via **NAPR extracts** (CAPTCHA-gated) and are
  personal data under the Law on Personal Data Protection.
- All registry fields are **planning-only** until the NAPR CAPTCHA / an official data channel
  is resolved; data.gov.ge should be re-checked from another network.
