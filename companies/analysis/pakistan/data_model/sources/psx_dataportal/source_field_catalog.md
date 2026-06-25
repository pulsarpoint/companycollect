# Pakistan Stock Exchange — Data Portal Field Catalog

## Source Summary

- Country: Pakistan
- Source type: financial_disclosure
- Organization: Pakistan Stock Exchange Limited (PSX)
- URL: https://dps.psx.com.pk/symbols
- License: PSX terms (reuse terms unconfirmed)
- Access: **public open JSON API** (no auth/payment) + per-company HTML pages
- Freshness: daily
- Record shape: JSON array of symbol objects (+ per-company HTML at /company/{symbol})
- Primary keys: symbol
- Join keys: symbol, name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| symbol | symbol | PSX ticker | string | identifier | OGDC, HBL, LUCK | listed key |
| name | name | Company/security name | string | legal_name | Habib Bank Limited | |
| sectorName | sectorName | PSX sector | string | activity | COMMERCIAL BANKS, CEMENT | |
| isETF | isETF | Is an ETF | boolean | metadata | false | filter out |
| isDebt | isDebt | Is a debt instrument | boolean | metadata | false | filter out for equities |
| company_page.registered_address | Registered Address | Registered office | string | address |  | per-company HTML |
| company_page.free_float | Free Float / Shares | Free float + shares | string | financial |  | per-company HTML; PKR |

## Interpretation Notes

- The **PSX data portal** exposes an **open JSON API** at `dps.psx.com.pk/symbols` —
  **verified live: 1,068 symbols**, of which **744 are non-debt/non-ETF equities**. Each
  record has `symbol`, `name`, `sectorName`, `isETF`, `isDebt`. Filter `isDebt=false` and
  `isETF=false` for company equities (debt rows append an instrument suffix like `TFC6`).
- **Per-company pages** at `dps.psx.com.pk/company/{symbol}` are browser-public HTML adding
  registered address, free float, and shares outstanding (parse the HTML).
- **Scope**: **listed companies only**. To enrich with the registry identifier (CUIN) or tax
  id (NTN), join by **name** to SECP/FBR (PSX does not publish CUIN/NTN). **Currency** PKR.
- A real symbols array is saved at `raw/api/psx_symbols.json` and a company page at
  `raw/api/psx_company_ogdc.html`; `sample_record.json` included (public market data).
