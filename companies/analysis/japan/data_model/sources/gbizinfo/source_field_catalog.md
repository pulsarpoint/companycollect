# gBizINFO (Gビズインフォ) Field Catalog

> **PLANNING-ONLY for field values.** The gBizINFO REST API returns HTTP **500**
> without a free token, and the public API doc confirms 利用申請 → APIトークン.
> Fields below are from the public API specification; no records were fetched
> without a token. Data is reusable under the government standard terms of use
> (≈ CC-BY) once a token is obtained.

## Source Summary

- Country: Japan
- Source type: government_aggregator
- Organization: Ministry of Economy, Trade and Industry (経済産業省)
- URL: https://info.gbiz.go.jp/hojin/v1/hojin
- License: government standard terms of use (≈ CC-BY 4.0)
- Access: public with a free token (X-hojinInfo-api-token)
- Freshness: periodic (aggregated from several government systems)
- Record shape: JSON, `hojin-infos[]` array
- Primary keys: `corporate_number`
- Join keys: `corporate_number`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| corporate_number | corporate_number | 13-digit corporate number | string | identifier | join key |
| name | name | Company name | string | legal_name | |
| location | location | Address | string | address | |
| date_of_establishment | date_of_establishment | Establishment date | date | date | **fills NTA's gap (true founding date)** |
| capital_stock | capital_stock | Registered capital (JPY) | integer | financial | not in NTA |
| employee_number | employee_number | Employees | integer | employment | not in NTA |
| business_summary | business_summary | Business description | string | activity | free text |
| business_items[] | business_items | Industry item codes | array | activity | closest to an industry code |
| finance[] | 財務情報 | Financial figures | array | financial | lighter than EDINET |
| procurement[] | 調達情報 | Procurement/awards | array | raw_extension | enrichment |
| subsidy[] | 補助金情報 | Subsidies/grants | array | raw_extension | enrichment |
| certification[] | 認定情報 | Certifications | array | raw_extension | enrichment |

## Interpretation Notes

- **Access**: send a free token via the `X-hojinInfo-api-token` header. Endpoints
  include `/hojin/v1/hojin?name=...` (search) and `/hojin/v1/hojin/{corporate_number}`
  (detail), plus sub-resources `/finance`, `/procurement`, `/subsidy`,
  `/certification`, `/patent`, `/workplace`. A bulk CSV is also offered.
- **Why it matters**: gBizINFO supplies the fields NTA lacks — **establishment
  date, capital, employees, business description, industry items, and light
  financials** — keyed on the same 13-digit corporate number, so it joins
  cleanly. Treat EDINET as the authoritative financial source; gBizINFO finance is
  a lighter, broader supplement.
- **Coverage** varies by sub-dataset; many companies have only basic info.
- No raw sample record is included (token-gated source).
