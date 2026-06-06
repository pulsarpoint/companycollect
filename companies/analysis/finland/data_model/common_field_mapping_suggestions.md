# Finland — Common Field Mapping Suggestions

> **Suggestion only.** This file proposes how Finland's company data could map to a
> future cross-country common schema. It does **not** constrain the country-specific
> profile in `country_company_profile.schema.json`, which remains authoritative for
> Finland. Use this only when building a cross-country mapper.

| Common field | Finland mapping | Source path | Notes |
|---|---|---|---|
| company_id | registration.business_id | businessId.value | Y-tunnus; also national tax number |
| registration_number | registration.business_id | businessId.value | Same as company_id in FI |
| tax_id | registration.business_id | businessId.value | Y-tunnus is the tax number |
| vat_id | registration.vat_id | derived: `FI`+digits | Liability confirmed via register 6 |
| legal_name | legal_identity.legal_name | names[?type=1 && endDate=null].name | |
| status | status.is_active / trade_register_status_code | tradeRegisterStatus (+ endDate) | NOT the raw `status` field |
| legal_form | legal_identity.legal_form.label_en | companyForms[?endDate=null] | e.g. Limited company (Oy) |
| incorporation_date | status.incorporation_date | registrationDate | |
| dissolution_date | status.dissolution_date | endDate | |
| registered_address | addresses[?address_type_code=1 or 2] | addresses[] | visiting(1)/postal(2) |
| activity_code | activity.code (+ code_set) | mainBusinessLine.type | TOL → NACE Rev.2 mappable |
| financials | financial_statements[] | **not_available_in_open_sources** (this endpoint) | Available via separate PRH financial statement API (planning-only) |
| officers | **not_available_in_open_sources** | — | Board/representatives not in open companies data |
| owners | **not_available_in_open_sources** | — | No beneficial-ownership data in PRH open data |
| source_provenance | source_provenance[] | — | Stamped at ingest |

## Cross-country normalization notes

- **Activity codes:** Finnish TOL (typeCodeSet TOIMI2/3/4) is NACE-derived; a
  TOL→NACE Rev.2 crosswalk gives a comparable `activity_code`. Watch mixed vintages.
- **VAT format:** Finnish VAT = `FI` + 8 digits (Y-tunnus without the dash). This is
  derivable, but only assert VAT status when register 6 has an active entry.
- **EU linkage:** `eu_id` (BRIS EUID, `FIFPRO.<business_id>`) is the natural key for
  linking Finland to other EU registries in a cross-country graph.
- **Officers / owners / financials** are the main gaps versus richer jurisdictions —
  mark them `not_available_in_open_sources` for Finland's open companies endpoint
  (financials become available only when the separate PRH financial-statement API is
  integrated).
