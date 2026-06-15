# China — Common Field Mapping Suggestions

> Suggestion layer only for a future cross-country mapper. It does **not**
> constrain the country-specific China profile. China is portal-gated (no open
> bulk; GSXT real-name + CAPTCHA), and most values below are **planning-only**.

| Common field | China mapping | Status |
|---|---|---|
| company_id | registration.uscc (USCC, 18-char) | gated (GSXT) |
| registration_number | registration.uscc | gated; USCC is the modern unified id |
| tax_id | registration.uscc | USCC = taxpayer id |
| vat_id | not_available_in_open_sources | no separate VAT in China |
| legal_name | legal_identity.legal_name (企业名称) | gated |
| status | status.status (存续/在营→active, 注销→deregistered, 吊销→revoked) | gated |
| legal_form | legal_identity.company_type (类型) | gated |
| incorporation_date | incorporation.establishment_date (成立日期) | gated |
| dissolution_date | not_available_in_open_sources | only status flag (注销) is exposed |
| registered_address | registered_location.registered_address (住所) | gated |
| activity_code | not_available_in_open_sources | 经营范围 is free text, no code |
| financials | financial_statements[] (cninfo/SSE/SZSE) | listed only; CNY; ASBE; planning-only |
| officers | officers[] (法定代表人 only) | gated; PERSONAL DATA (PIPL) |
| owners | not_available_in_open_sources | shareholders only via paid aggregators (PIPL) |
| source_provenance | source_provenance[] | available |

## Notes

- **USCC** is the single anchor: it is simultaneously `company_id`,
  `registration_number`, and `tax_id`. A cross-country mapper should not expect a
  distinct VAT number for China.
- **Access reality:** unlike fully-open registers (UK CH, RO EDR, UA EDR), China
  has **no open bulk/API**. Treat any China company_id/identity mapping as
  gated/planning-only unless sourced via a licensed provider.
- **Financials** map only for listed issuers; non-listed have no public
  financials. Join listed financials via stock_code → USCC.
- **Personal data:** legal representative and (aggregator-only) shareholders are
  personal data under **PIPL**, and **cross-border data-export** rules apply.
