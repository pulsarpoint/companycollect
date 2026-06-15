# South Korea — Common Field Mapping Suggestions

> Suggestion layer only for a future cross-country mapper. It does **not**
> constrain the Korea-specific profile. Identity + financials are via OpenDART
> (free key); status via the NTS API (free key); legal form/capital/directors are
> paid (court registry).

| Common field | South Korea mapping | Status |
|---|---|---|
| company_id | registration.corp_registration_number (법인등록번호) or dart_corp_code | open via OpenDART (free key) |
| registration_number | registration.corp_registration_number (13-digit) | open (free key) |
| tax_id | registration.business_registration_number (사업자등록번호, 10-digit) | open (free key) |
| vat_id | registration.business_registration_number | same value — no separate VAT id |
| legal_name | legal_identity.legal_name (corp_name) | open (free key) |
| status | status.business_status (NTS) / market_class (DART) | open (free key) |
| legal_form | legal_identity.legal_form (court registry) / market_class | paid for exact form |
| incorporation_date | incorporation.establishment_date (est_dt) | open (free key) |
| dissolution_date | status.closure_date (NTS end_dt) | open (free key) |
| registered_address | registered_location.registered_address (adres) | open (free key) |
| activity_code | activity.industry_code_ksic (induty_code) | open (free key) |
| financials | financial_statements[] (OpenDART fnlttSinglAcntAll) | open (free key); KRW; DART-registered only |
| officers | officers[] (CEO via DART; directors via court registry) | gated; personal data (PIPA) |
| owners | not_available_in_open_sources | shareholders not openly published (DART majority-holder filings partial) |
| source_provenance | source_provenance[] | available |

## Notes

- **Two anchors**: map `company_id`/`registration_number` to the **corporate
  registration number** (or DART corp_code), and `tax_id`/`vat_id` both to the
  **business registration number** (no separate VAT id).
- **Access tier**: identity + financials need a free OpenDART key; status needs a
  free data.go.kr key; legal form/capital/directors are paid. Coverage of the open
  layer = listed + external-audit companies.
- **Personal data**: CEO/director names are personal data under **PIPA** — redact
  in any committed sample.
