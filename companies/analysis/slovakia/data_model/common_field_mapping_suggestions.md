# Slovakia — Common Field Mapping Suggestions

> Suggestions only for a future cross-country mapper. This does **not** constrain
> the country-specific Slovakia profile, which is authoritative.

| Common field | Slovakia source | Slovakia path | Notes |
|---|---|---|---|
| company_id | ruz_accounting_units | registration.ico | 8-digit IČO |
| registration_number | ruz_accounting_units | registration.ico | same as company_id |
| tax_id | ruz_accounting_units | tax_identifiers.dic | DIČ |
| vat_id | ruz_accounting_units | tax_identifiers.vat_id | SK + DIČ |
| legal_name | rpo | legal_identity.legal_name | history available |
| status | ruz_accounting_units | status.dissolution_date | no flag; derive active |
| legal_form | rpo | legal_identity.legal_form | code 112 = s.r.o. |
| incorporation_date | rpo | incorporation.incorporation_date | establishment |
| dissolution_date | ruz_accounting_units | status.dissolution_date | datumZrusenia |
| registered_address | rpo | registered_location | RPO street + RÚZ region/district |
| activity_code | ruz_accounting_units | activity.sk_nace | SK NACE |
| financials | ruz_financial_reports | financial_statements[] | structured; EUR; multi-year |
| officers | rpo | officers[] | OPEN but PII — redact |
| owners | rpo | owners[] | OPEN but PII — redact |
| source_provenance | (all) | source_provenance[] | per-section provenance |

## Cross-country notes

- **Two official open sources, one key.** Everything joins on **IČO**; `vat_id =
  "SK" + DIČ` (DIČ from RÚZ). A cross-country mapper can derive `vat_id` from RÚZ.
- **Officers AND owners are open** (RPO) — unusual richness vs RS/PT — but they
  are **personal data**; map with redaction and a lawful basis.
- **Financials are open and structured** (RÚZ, CC0), multi-year, EUR — but require
  **template-based decoding** of positional arrays; budget for caching templates.
  Large filers may expose only PDF.
- **No status enum** — derive `status` from the dissolution date.
