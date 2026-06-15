# Registry Information Service (登記情報提供サービス) Field Catalog

> **PLANNING-ONLY / PAID.** The full commercial register run by the Ministry of
> Justice / Legal Affairs Bureau is a **pay-per-record** service with no open
> bulk or API. Cataloged from public documentation only — no records fetched, no
> values copied. It is the only source of **officers, capital, purpose, and
> incorporation date**, which are absent from the open NTA data.

## Source Summary

- Country: Japan
- Source type: official_registry
- Organization: Ministry of Justice (法務省) / Legal Affairs Bureau (法務局)
- URL: https://www1.touki.or.jp/
- License: restricted (paid)
- Access: paid per-record (registration required)
- Freshness: real-time registry
- Record shape: per-company registry certificate (登記事項)
- Primary keys: `company_registration_number` (会社法人等番号, 12-digit)
- Join keys: `corporate_number` (13-digit; = 12-digit base + check digit)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| company_registration_number | 会社法人等番号 | 12-digit registration number | string | identifier | base of corporate number |
| company_name | 商号 | Trade name | string | legal_name | paid |
| head_office | 本店 | Head-office address | string | address | paid |
| capital | 資本金の額 | Registered capital (JPY) | integer | financial | not in NTA |
| purpose | 目的 | Business purpose(s) | string | activity | not in NTA |
| directors | 役員に関する事項 | Directors/officers | array | person | **PERSONAL DATA (APPI)** |
| establishment | 会社成立の年月日 | Incorporation date | date | date | authoritative founding date |

## Interpretation Notes

- The **会社法人等番号** (12-digit company registration number) is the base of the
  13-digit corporate number (corporate number = check digit + this number), so it
  joins to NTA.
- This registry is the **only** source of **officers/directors** and the
  authoritative **incorporation date, capital, and purpose** — none of which are
  in the open NTA dataset. gBizINFO provides some of these (capital, establishment
  date) for free, derived ultimately from this registry.
- **Officer/director records are personal data** under Japan's **APPI** and must
  be redacted in any committed output. No officer data is included here.
- **Access**: pay-per-record view; no bulk/API. Keep planning-only; verify terms
  before any use.
- No raw sample record (paid source).
