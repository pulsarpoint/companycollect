# Canada — Common Field Mapping Suggestions

> Suggestions only for a future cross-country mapper. This does **not** constrain
> the country-specific Canada profile, which is authoritative.

| Common field | Canada source | Canada path | Notes |
|---|---|---|---|
| company_id | corporations_canada_federal | registration.corporation_number | federal id; provincial cos use the provincial registry number |
| registration_number | corporations_canada_federal | registration.corporation_number | |
| tax_id | corporations_canada_federal | registration.business_number | CRA BN (cross-source key) |
| vat_id | (none) | — | not_available (no VAT; GST/HST = BN + RT) |
| legal_name | corporations_canada_federal | legal_identity.legal_name | EN; FR alt available |
| status | corporations_canada_federal | status.status | Active/Inactive/Dissolved |
| legal_form | corporations_canada_federal | legal_identity.governing_legislation | CBCA etc. |
| incorporation_date | corporations_canada_federal | incorporation.anniversary_date | ≈; exact via API |
| dissolution_date | corporations_canada_federal | status (inactive files) | from inactive/dissolved files |
| registered_address | corporations_canada_federal | registered_location | full address |
| activity_code | (none) | — | not_available_in_open_sources (no NAICS in the federal dataset) |
| financials | sedar_plus | financial_statements[] | reporting-issuers only; private not available |
| officers | corporations_canada_api | directors.director_list | not in bulk; API (PII) |
| owners | (none) | — | not_available_in_open_sources (no public BO register yet) |
| source_provenance | (all) | source_provenance[] | per-section provenance |

## Cross-country notes

- **No single national register** (like the US): federal (Corporations Canada,
  open) + 13 provincial registries (mixed access). A cross-country mapper must
  treat Canada as **multi-jurisdiction** — the federal dataset is a **subset**.
- **Two federal identifiers**: `company_id` = corporation number; `tax_id` = **BN**
  (the cross-source join key). **No VAT id** (GST/HST = BN + RT).
- **Open identity, gaps elsewhere**: full address + status + bilingual names are
  free (OGL), but **NAICS, director names, financials, beneficial owners** are
  not in the federal bulk — mark them `not_available`/planning-only for an
  open-only pipeline.
- **Financials**: reporting-issuers only (SEDAR+); private-company financials not
  public.
