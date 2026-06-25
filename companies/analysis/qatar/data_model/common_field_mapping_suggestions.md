# Common Field Mapping Suggestions — Qatar

This file is a **suggestion** for a future cross-country mapper. It does **not**
constrain the country-specific Qatar profile, which is the source of truth.

All Qatar company sources are gated in some way (MoCI = lookup-only/auth-gated; QFC =
browser-public but postback-driven; QSE = browser-public but AJAX with no identified data
endpoint), so these mappings are **planning-only** until an access path is established.

| Common field | Qatar mapping | Source | Notes |
|---|---|---|---|
| company_id | registration.cr_number | moci_commercial_registration | onshore; QFC Number for financial centre |
| registration_number | registration.cr_number | moci_commercial_registration | CR number (QFC Number alt) |
| tax_id | not_available_in_open_sources | — | establishment/tax card via Dhareeba (not open) |
| vat_id | not_available_in_open_sources | — | no general VAT register as of investigation |
| legal_name | legal_identity.legal_name | moci_commercial_registration / qfc_public_register / qse_listed | MoCI preferred |
| status | status.status_text | moci_commercial_registration / qfc_public_register | CR status / QFCA licensed |
| legal_form | legal_identity.legal_form | moci_commercial_registration | W.L.L./Q.P.S.C./sole prop./branch |
| incorporation_date | status.date_of_registration | qfc_public_register / moci_commercial_registration | Gregorian |
| dissolution_date | not_available_in_open_sources | — | CR cancellation not openly published |
| registered_address | registered_location.address | qfc_public_register / moci_commercial_registration | redact individual addresses |
| activity_code | activity.activities | moci_commercial_registration | ISIC-based; QSE sector for listed |
| financials | financial_statements | qse_listed | QAR; **listed only** |
| officers | officers | qfc_public_register / moci_commercial_registration | **REDACT — personal data** |
| owners | not_available_in_open_sources | — | gated MoCI; personal data (Law 13/2016) |
| source_provenance | source_provenance | all | per-section |

Concepts **not available from open sources** for Qatar:

- `tax_id` / `vat_id` — establishment/tax card via the General Tax Authority (Dhareeba);
  no open register; no general VAT register as of investigation.
- `owners` / beneficial ownership — gated MoCI; personal data under Law No. 13 of 2016.
- Private-company `financials` — only QSE-listed financials are public.
- Any per-company value without clearing the MoCI auth gate, the QFC postback, or the QSE
  AJAX endpoint.
