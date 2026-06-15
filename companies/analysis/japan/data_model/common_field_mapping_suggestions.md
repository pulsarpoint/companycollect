# Japan — Common Field Mapping Suggestions

> Suggestion layer only for a future cross-country mapper. It does **not**
> constrain the Japan-specific profile. Identity fields are open (NTA); enrichment
> and financials are free-key/token or paid.

| Common field | Japan mapping | Status |
|---|---|---|
| company_id | registration.corporate_number (法人番号, 13-digit) | open (NTA) |
| registration_number | registration.corporate_number | open (NTA) |
| tax_id | registration.corporate_number | open (same value) |
| vat_id | not_available_in_open_sources | no separate VAT; invoice no = "T"+corporate number |
| legal_name | legal_identity.legal_name (商号又は名称) | open (NTA) |
| status | status.status (closeDate ⇒ closed, else active) | open (NTA) |
| legal_form | legal_identity.corporate_kind (101/201/301/401/499) | open (NTA); coarse — specific form (株式会社/合同会社) only inferable from the name |
| incorporation_date | company_details.establishment_date (gBizINFO) / paid registry | free token / paid — NOT NTA (assignment_date ≠ incorporation) |
| dissolution_date | status.close_date | open (NTA) |
| registered_address | registered_location.* (都道府県+市区町村+丁目番地) | open (NTA) |
| activity_code | company_details.business_items (gBizINFO) | free token; no formal JSIC code in open data |
| financials | financial_statements[] (EDINET XBRL) / gBizINFO finance[] | free key/token; listed/obligated only |
| officers | officers[] (paid Legal Affairs Bureau registry) | paid; APPI personal data |
| owners | not_available_in_open_sources | no open beneficial-ownership register |
| source_provenance | source_provenance[] | available |

## Notes

- **Single anchor:** the 13-digit corporate number is simultaneously
  `company_id`, `registration_number`, and `tax_id`. A cross-country mapper should
  not expect a separate VAT number for Japan.
- **Access tiers:** identity = fully open bulk (NTA); financials = free key
  (EDINET) / free token (gBizINFO), listed/obligated only; officers + definitive
  incorporation/capital = paid registry.
- **Date caution:** never map NTA `assignment_date` to `incorporation_date`
  (mass assignment in 2015 makes it meaningless as a founding date).
- **Personal data:** officer/director data (paid registry) is personal data under
  **APPI** — redact in any committed sample.
