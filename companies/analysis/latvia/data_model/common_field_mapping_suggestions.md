# Latvia — Common Field Mapping Suggestions

> **Suggestion layer only.** This does NOT constrain the country-specific Latvia profile
> (`country_company_profile.schema.json`). It is a hint for a future cross-country mapper.

| Common field | Latvia source | Latvia path | Notes |
|---|---|---|---|
| company_id | ur_register | regcode | 11-digit registration number |
| registration_number | ur_register | regcode | same |
| tax_id | not_available_in_open_sources | — | no separate tax id; VAT below |
| vat_id | vid_vat | LV + regcode | derivable; validate via VIES |
| legal_name | ur_register | name | name_in_quotes = bare firm name |
| status | ur_register | terminated + closed | derived |
| legal_form | ur_register | type_text (+ type) | SIA/AS/IK |
| incorporation_date | ur_register | registered | |
| dissolution_date | ur_register | terminated | |
| registered_address | ur_register | address (+ index, atvk) | |
| activity_code | not_available_in_open_sources | — | NACE via other UR/CSP datasets if needed |
| financials | ur_financial_statements | financial_statements + balance_sheets + income_statements + cash_flow | **structured** line items + employees; EUR (pre-2014 LVL) |
| officers | ur_officers_members | amatpersonas | PII |
| owners | ur_officers_members (members) / ur_beneficial_owners | dalībnieki / patiesie labuma guvēji | registered members AND beneficial owners (both open) |
| source_provenance | (all) | source_provenance[] | per-section provenance |

## Cross-Country Notes

- Latvia is **best-in-class fully-open** and uniquely **CC0 (public domain)**: a single source (UR) supplies
  identity, **structured financial statements** (with **employee counts**), **registered members**, **beneficial
  owners** and officers — all open, no attribution required.
- For a cross-country `financials` field, Latvia is a model of **structured** open financials (balance sheet +
  income statement + cash flow line items) — no PDF/OCR. Map directly; pivot the four CSVs per report.
- For `owners`, Latvia lets you populate **both** registered members (dalībnieki) and beneficial owners
  (patiesie labuma guvēji) — keep them distinct.
- `tax_id` and `activity_code` are `not_available_in_open_sources` in the register CSV (VAT = LV+regcode; NACE
  via other datasets).
- The **regcode** is the clean universal key (also inside the SEPA id and the VAT number).
