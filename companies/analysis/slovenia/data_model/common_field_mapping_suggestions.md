# Slovenia — Common Field Mapping Suggestions

> Suggestions only for a future cross-country mapper. This does **not** constrain
> the country-specific Slovenia profile, which is authoritative.

| Common field | Slovenia source | Slovenia path | Notes |
|---|---|---|---|
| company_id | ajpes_prs | registration.maticna_stevilka | 10-digit matična |
| registration_number | ajpes_prs | registration.maticna_stevilka | same as company_id |
| tax_id | furs_zavezanci_po | tax_identifiers.davcna_stevilka | 8-digit davčna |
| vat_id | furs_zavezanci_po | tax_identifiers.vat_id | SI + davčna |
| legal_name | ajpes_prs | legal_identity.legal_name | |
| status | ajpes_restprsinfo | status.status | not_available_in_open_sources (credentialed) |
| legal_form | ajpes_prs | legal_identity.legal_form | text label |
| incorporation_date | ajpes_restprsinfo | incorporation.registration_date | not_available_in_open_sources |
| dissolution_date | ajpes_restprsinfo | — | not_available_in_open_sources |
| registered_address | ajpes_prs | registered_location | structured |
| activity_code | furs_zavezanci_po | activity.skd_code | SKD ≈ NACE Rev.2 |
| financials | ajpes_jolp / ajpes_fipo | financial_statements[] | not_available_in_open_sources (view-only / paid) |
| officers | ajpes_restprsinfo | officers[] | not_available_in_open_sources |
| owners | (court register) | — | not_available_in_open_sources |
| source_provenance | (all) | source_provenance[] | per-section provenance |

## Cross-country notes

- **Two-open-source, one-key (matična).** Identity (PRS) and tax/VAT/activity
  (FURS) are open and join cleanly; `vat_id = "SI" + davčna` (davčna from FURS,
  **not** in PRS). A mapper must use FURS for any tax/VAT field.
- **Financials are the gap.** Unlike SK (open RÚZ), Slovenia exposes **no open
  structured financials** — JOLP is view-only, Fi=Po is paid. Mark
  `financials` planning-only.
- **Status/incorporation/officers/owners** also not open (credentialed
  restPrsInfo / court register) — mark planning-only.
- **Encoding care**: PRS UTF-16, FURS UTF-8 semicolon (trim spaces).
