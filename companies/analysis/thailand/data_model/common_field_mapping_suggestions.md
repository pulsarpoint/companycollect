# Common field mapping suggestions — Thailand

> Suggestion only for a future cross-country mapper. It does **not** constrain the
> country-specific Thailand profile, which stays keyed on the 13-digit juristic ID.

| Common field | Thailand source path | Notes |
|---|---|---|
| company_id | `registration.juristic_id` (DBD) | 13-digit; also the Tax ID |
| registration_number | `registration.juristic_id` | same number |
| tax_id | `tax_identifiers.tax_id` (= juristic_id) | same number |
| vat_id | `tax_identifiers.vat_id` (= juristic_id) | no separate VAT number |
| legal_name | `legal_identity.name_en` (+ name_th) | DBD |
| status | `status.status_text` | ยังดำเนินกิจการอยู่ = active |
| legal_form | `legal_identity.legal_form` | บริษัทจำกัด/มหาชน/ห้างหุ้นส่วน |
| incorporation_date | `status.register_date` | YYYYMMDD |
| dissolution_date | not_available_in_open_sources | status implies it |
| registered_address | `registered_location.registered_address` | DBD structured |
| activity_code | `activity.tsic_code` | TSIC |
| financials | `capital.*` (open) + `financial_statements[]` (gated) | capital open; statements login/SET |
| officers | not_available_in_open_sources | directors not in open API (PDPA) |
| owners | not_available_in_open_sources | shareholders not in open API (PDPA) |
| source_provenance | `source_provenance[]` |  |

## Cross-country notes

- Thailand collapses three identifiers into **one 13-digit number**: `company_id ==
  registration_number == tax_id == vat_id`. Clean single-key joins.
- **Capital** is openly available (DBD OpenAPI); **full financial statements** are
  login-gated (DataWarehouse) or listed-only (SET). Currency **THB**.
- The **DBD OpenAPI** is a rare fully-open official company API — high-value, no key.
- Officers/owners are not open (PDPA); do not expect them from the open API.
