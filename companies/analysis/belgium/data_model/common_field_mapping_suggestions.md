# Belgium — Common Field Mapping Suggestions

> **Suggestion only.** Proposes how Belgium's country-specific profile *could* map onto a future
> cross-country company schema. It does **not** constrain `country_company_profile.schema.json`. The
> country-specific model is authoritative.

| Common field | Belgium source path | Notes |
|---|---|---|
| company_id | registration.enterprise_number | Single 10-digit key; = VAT root. |
| registration_number | registration.enterprise_number | Same number. |
| tax_id | registration.enterprise_number | No separate tax id; enterprise number is the fiscal id. |
| vat_id | registration.vat_id ("BE" + digits) | Derived. |
| legal_name | legal_identity.name | KBO denomination (social name); multilingual. |
| status | status.derived | KBO Status/JuridicalSituation (+ Moniteur). |
| legal_form | legal_identity.legal_form | KBO JuridicalForm (code.csv). |
| incorporation_date | (KBO enterprise.StartDate) | open. |
| dissolution_date | (KBO JuridicalSituation / Moniteur) | open. |
| registered_address | registered_location.* | open (KBO). |
| activity_code | activity.nace_main | **NACE-BEL — open and clean**. |
| financials | financial_statements[] | **OPEN structured XBRL** (NBB CBSO) — free, no paid tier. |
| officers | not_available_in_open_sources | KBO open data does not carry directors; Moniteur acts mention appointments. |
| owners | beneficial_owners[] = restricted (planning-only) | UBO restricted. |
| source_provenance | source_provenance[] | per-source + access flag. |

## Cross-country notes for a future mapper

- **Belgium is a top-tier open case** (with Norway/France/Poland): a cross-country mapper gets identity +
  **structured financials** + activity + establishments + acts for free, joined on **one clean key**
  (EnterpriseNumber = VAT root). Best-in-class structured financials (full XBRL since 2007).
- **No separate tax id** — the enterprise number is the company id, registration number, and VAT root.
- **Activity code (NACE-BEL) is open and clean** — not `not_available_in_open_sources`.
- **Officers/directors are NOT in the KBO open data** — only in the gazette acts (Moniteur) and paid
  aggregators; a cross-country `officers` mapper should treat Belgium's officers as gazette-derived/sparse.
- **Owners (UBO)**: restricted (planning-only).
- **Financials** need Belgian-GAAP XBRL schema-variant handling; micro/abbreviated omit revenue. Currency EUR.
- **Access caveat**: both core open sources require a **free registration/account** (not payment).
