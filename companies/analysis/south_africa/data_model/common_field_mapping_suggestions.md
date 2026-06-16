# South Africa — Common Field Mapping Suggestions

> Suggestion layer only for a future cross-country mapper. It does **not**
> constrain the South-Africa-specific profile. The open layer (OCDS) is
> name-keyed procurement; the registry (CIPC) and financials are paid/listed.

| Common field | South Africa mapping | Status |
|---|---|---|
| company_id | identity.registration_number (CIPC YYYY/NNNNNN/NN) | paid; not in open data |
| registration_number | identity.registration_number (CIPC) | paid; not in open data |
| tax_id | tax_identifiers.income_tax_number (SARS) | not_available_in_open_sources |
| vat_id | tax_identifiers.vat_id (SARS, 10-digit, starts 4) | not_available_in_open_sources |
| legal_name | identity.legal_name (OCDS supplier name / CIPC enterprise name) | open (OCDS, names) |
| status | legal_identity.company_status (CIPC) | paid |
| legal_form | legal_identity.company_type (CIPC; reg-number suffix) | paid (inferable from suffix) |
| incorporation_date | legal_identity.registration_date (CIPC) | paid |
| dissolution_date | not_available_in_open_sources | CIPC status flag (Deregistered) |
| registered_address | legal_identity.registered_address (CIPC) | paid |
| activity_code | not_available_in_open_sources | OCDS tender title is procurement context, not a company activity code |
| financials | financial_statements[] (CIPC AFS paid / JSE listed) | paid / listed-only |
| officers | officers[] (CIPC directors) | paid; personal data (POPIA) |
| owners | not_available_in_open_sources | beneficial owners (CIPC BO register) not openly published |
| source_provenance | source_provenance[] | available |

## Notes

- **No open company id.** The authoritative key (CIPC registration number) is paid
  and absent from the open data; map `company_id`/`registration_number` to it but
  treat it as **not-in-open-data**. The open OCDS layer is keyed on **name** only,
  so cross-source links are name-based and approximate.
- **Tax**: South Africa has **VAT** (10-digit, starts `4`) and a SARS income-tax
  number, but neither is openly published — mark both `not_available_in_open_sources`.
- **The open contribution** is the company **name** + **procurement activity**
  (buyers, ZAR award values) from OCDS — a partial, public-sector view. The award
  value is the contract value, **not** company revenue.
- **Financials/officers/registry detail** are paid (CIPC) or listed-only (JSE/SENS);
  directors are personal data (POPIA) — redact.
