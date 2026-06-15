# Australia — Common Field Mapping Suggestions

> Suggestions only for a future cross-country mapper. This does **not** constrain
> the country-specific Australia profile, which is authoritative.

| Common field | Australia source | Australia path | Notes |
|---|---|---|---|
| company_id | abr_bulk_extract | registration.abn | 11-digit ABN |
| registration_number | abr_bulk_extract | registration.acn | 9-digit ACN (companies); else ABN |
| tax_id | abr_bulk_extract | registration.abn | = ABN |
| vat_id | (none) | — | not_available (no VAT; GST registration flag instead) |
| legal_name | abr_bulk_extract | legal_identity.legal_name | |
| status | abr_bulk_extract | status.abn_status | ABN ACT/CAN; precise ASIC status is paid |
| legal_form | abr_bulk_extract | legal_identity.entity_type | EntityTypeText |
| incorporation_date | asic_company_register | incorporation.incorporation_date | not_available_in_open_sources (paid ASIC) |
| dissolution_date | asic_company_register | status.asic_company_status | not_available_in_open_sources (paid ASIC) |
| registered_address | abr_bulk_extract / asic_company_register | registered_location | open: state+postcode; full street paid (ASIC) |
| activity_code | (none) | — | not_available_in_open_sources (no ANZSIC in the public ABR) |
| financials | asx_listed / asic_financial_reports | financial_statements[] | listed-only (free ASX) or paid (ASIC); AUD |
| officers | asic_company_register | officers[] | not_available_in_open_sources (paid ASIC) |
| owners | asic_company_register | — | not_available_in_open_sources (paid ASIC) |
| source_provenance | (all) | source_provenance[] | per-section provenance |

## Cross-country notes

- **Two open identifiers**: `company_id`/`tax_id` = **ABN**; companies also have an
  **ACN** (registration number). **No VAT id** — a mapper must leave `vat_id`
  empty (GST registration is the indirect-tax flag).
- **Open identity, paid detail**: the ABR gives name/type/state/postcode/GST for
  free, but **street address, incorporation date, ANZSIC, officers** require **paid
  ASIC** — mark those `not_available_in_open_sources` for an open-only pipeline.
- **Financials**: **listed-only** open (free via ASX) or **paid** (ASIC); most
  small proprietary companies don't lodge — treat `financials` as mostly
  unavailable openly.
- **All-entity feed**: the ABR covers sole traders/trusts/etc. too — filter by ACN/
  entity type for companies; redact individual names.
