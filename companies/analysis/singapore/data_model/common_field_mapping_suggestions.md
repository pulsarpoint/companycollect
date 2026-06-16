# Singapore — Common Field Mapping Suggestions

> Suggestion layer only for a future cross-country mapper. It does **not**
> constrain the Singapore-specific profile. The registry list (ACRA) is open;
> officer names / financials are paid (BizFile) or listed-only (SGX).

| Common field | Singapore mapping | Status |
|---|---|---|
| company_id | registration.uen (Unique Entity Number) | open (ACRA) |
| registration_number | registration.uen | open (ACRA) |
| tax_id | registration.uen (entity tax reference) | open (ACRA) |
| vat_id | not_available_in_open_sources | GST country; GST reg = UEN, no separate VAT |
| legal_name | legal_identity.legal_name (entity_name) | open (ACRA) |
| status | status.status (entity_status_description; Live -> active) | open (ACRA) |
| legal_form | legal_identity.entity_type (+ company_type) | open (ACRA) |
| incorporation_date | incorporation.registration_incorporation_date | open (ACRA) |
| dissolution_date | not_available_in_open_sources | status flag instead (Terminated/Struck Off) |
| registered_address | registered_location.registered_address | open (ACRA) |
| activity_code | activity.primary_ssic_code (SSIC) | open (ACRA) |
| financials | financial_statements[] (BizFile XBRL / SGX) | paid / listed-only — planning-only |
| officers | officers[] (BizFile names) — officers_summary.no_of_officers is the open count | paid (names); count open |
| owners | not_available_in_open_sources | shareholders only in the paid BizFile profile (PDPA) |
| source_provenance | source_provenance[] | available |

## Notes

- **Single strong anchor**: the **UEN** is `company_id`, `registration_number`, and
  `tax_id` at once. Do not expect a separate VAT id (Singapore has GST; the GST reg
  is generally the UEN).
- **Officers**: the open ACRA dataset exposes only the **count** (`no_of_officers`).
  Officer/shareholder **names** are personal data (PDPA) and only in the paid
  BizFile profile — redact in any committed sample.
- **Financials**: map `financials` from SGX (open, listed) or BizFile (paid,
  private), SGD. There is no broad open financial source.
- **Bonus open fields**: up to 15 **former names** and up to 5 **audit firms** (each
  with its own UEN) are open and useful.
