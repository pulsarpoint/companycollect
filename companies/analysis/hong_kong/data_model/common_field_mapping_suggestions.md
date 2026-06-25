# Common Field Mapping Suggestions — Hong Kong

This file is a **suggestion** for a future cross-country mapper. It does **not**
constrain the country-specific Hong Kong profile, which is the source of truth.

The CR open feed is fully open and implementable; ICRIS fields are **planning-only**
(pay-per-use) and HKEX listed data is browser-public (static xlsx is a template).

| Common field | Hong Kong mapping | Source | Notes |
|---|---|---|---|
| company_id | registration.br_number | cr_open_data_newly_registered | BR Number (open); CR Company Number via ICRIS |
| registration_number | registration.cr_company_number | icris_esearch | CR Company Number (paid); BR Number as open fallback |
| tax_id | registration.br_number | cr_open_data_newly_registered | BR Number is the business id |
| vat_id | not_available_in_open_sources | — | Hong Kong has no VAT |
| legal_name | legal_identity.legal_name_en | cr_open_data_newly_registered | + legal_name_zh |
| status | status.status_text | cr_open_data_newly_registered / icris_esearch | event (open) vs ICRIS Company Status |
| legal_form | legal_identity.company_type | icris_esearch | planning-only (paid) |
| incorporation_date | status.incorporation_date | cr_open_data_newly_registered | DD-MM-YYYY→ISO |
| dissolution_date | not_available_in_open_sources | icris_esearch | status only via ICRIS (paid) |
| registered_address | registered_location.registered_office_address | icris_esearch | planning-only (paid) |
| activity_code | not_available_in_open_sources | — | not published in these sources |
| financials | not_available_in_open_sources | — | HK companies file at CR but not in open data |
| officers | officers | icris_esearch | planning-only; **REDACT — personal data** |
| owners | not_available_in_open_sources | — | shareholders via ICRIS NAR1 docs (paid); personal data |
| source_provenance | source_provenance | all | per-section |

Concepts **not available from open sources** for Hong Kong:

- `legal_form`, `registered_address`, `officers`, `owners`, `dissolution_date`, `vat_id`,
  `activity_code`, `financials` — all require **ICRIS (pay-per-use)** or are not published.
  Only the **BR Number, names, and incorporation/registration dates** are openly available
  (CR RNC063 feed).
- Directors/shareholders are personal data under the **PDPO** even where obtainable (ICRIS).
