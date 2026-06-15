# Vietnam — Common Field Mapping Suggestions

> Suggestions only for a future cross-country mapper. This does **not** constrain
> the country-specific Vietnam profile, which is authoritative.

Vietnam is a **gated/no-open-bulk** country: the data exists but is CAPTCHA-gated
(per-company) or paid (bulk), so most fields are `gated_or_planning_only` rather
than `not_available_in_open_sources` (the data exists, just not openly in bulk).

| Common field | Vietnam source | Vietnam path | Notes |
|---|---|---|---|
| company_id | nbrp_search | registration.enterprise_code | = tax code (10–13 digits) |
| registration_number | nbrp_search | registration.enterprise_code | same as company_id |
| tax_id | gdt_taxpayer_lookup | tax_identifiers.tax_code | = enterprise code |
| vat_id | gdt_taxpayer_lookup | tax_identifiers.vat_id | no separate VAT number (tax code serves VAT) |
| legal_name | nbrp_search | legal_identity.legal_name | gated |
| status | nbrp_search | status.status | active/suspended/dissolved |
| legal_form | nbrp_search | legal_identity.legal_form | TNHH/CP/DNTN |
| incorporation_date | nbrp_search | incorporation.establishment_date | gated |
| dissolution_date | nbrp_search | status (Đã giải thể) | gated; implied by status |
| registered_address | nbrp_search | registered_location.head_office_address | gated |
| activity_code | nbrp_search | activity.vsic_codes | VSIC; gated |
| financials | hose_hnx_ssc_disclosure | financial_statements[] | listed-only; non-listed not_available |
| officers | nbrp_search | officers[] | legal representative only; PII |
| owners | (none) | — | not_available_in_open_sources |
| source_provenance | (all) | source_provenance[] | per-section provenance |

## Cross-country notes

- **One number for everything**: `company_id = tax_id = vat_id =` the enterprise
  code (mã số doanh nghiệp = mã số thuế). A mapper can treat the three as
  identical for Vietnam and must **not** expect a separate VAT number.
- **No open bulk**: unlike RO/UK/UA, there is no open bulk register — identity is
  per-company gated or paid (MOU). Treat Vietnam as **manual/licensed-first**.
- **Financials**: open only for **listed** companies (HOSE/HNX/SSC), VND/VAS;
  non-listed financials are not published.
- **Ownership**: only the **legal representative** is open (personal data);
  shareholders/beneficial owners are `not_available_in_open_sources`.
