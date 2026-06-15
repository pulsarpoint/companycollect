# Mexico — Common Field Mapping Suggestions

> Suggestion layer only for a future cross-country mapper. It does **not**
> constrain the Mexico-specific profile. The open layer (DENUE) is
> establishment-level; the legal registry and tax id are not openly bulk-available.

| Common field | Mexico mapping | Status |
|---|---|---|
| company_id | identity.denue_id / clee (DENUE establishment) | open (DENUE) |
| registration_number | identity.folio_mercantil_electronico (RPC) | paid; not in DENUE |
| tax_id | identity.rfc (RFC, 12-char companies) | SAT/registry; not in DENUE (name join) |
| vat_id | identity.rfc | same value — no separate VAT id (Mexico has IVA) |
| legal_name | legal_identity.legal_name (raz_social) | open (DENUE) |
| status | status.in_directory (DENUE) + status.risk_69b (SAT) | open |
| legal_form | legal_identity.legal_form (tipo societario / inferred suffix) | paid for authoritative; inferable from name |
| incorporation_date | registry_details.incorporation_date (RPC) | paid; not open |
| dissolution_date | not_available_in_open_sources | — |
| registered_address | locations[].address (DENUE, geolocated) | open (establishment address) |
| activity_code | activity.scian_code (SCIAN) | open (DENUE) |
| financials | financial_statements[] (BMV/CNBV) | listed-only; private not public |
| officers | not_available_in_open_sources | notarial/registry docs (fee-based) |
| owners | not_available_in_open_sources | beneficial owners not openly published |
| source_provenance | source_provenance[] | available |

## Notes

- **No single anchor**: Mexico lacks one open company id linking the statistical
  directory (DENUE id/clee), the legal registry (folio mercantil), and the tax
  authority (RFC). Map `company_id` to the **DENUE id/clee** for the open layer,
  but treat cross-source links as **name-based and approximate**.
- **RFC = tax id = VAT id** (12-char for companies); there is no separate VAT
  number. The RFC is not in DENUE.
- **Establishment vs entity**: DENUE rows are establishments; one legal entity may
  span several rows.
- **Financials**: map `financials` only for listed issuers (BMV/CNBV, MXN); private
  companies have none.
- **Personal data**: DENUE `telefono`/`correoelec`, and individual RFCs/names, are
  personal data under **LFPDPPP** — redact in committed samples.
