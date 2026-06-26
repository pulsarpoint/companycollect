# Republican Stock Exchange 'Toshkent' (UZSE) — listed issuers Field Catalog

## Source Summary

- Country: Uzbekistan
- Source type: financial_disclosure
- Organization: Republican Stock Exchange 'Toshkent' (uzse.uz)
- URL: https://uzse.uz/issuers
- License: public disclosure
- Access: **public via browser; JS SPA** (REST backend route not located)
- Freshness: event-driven
- Record shape: listed-issuers JS SPA
- Primary keys: isin
- Join keys: isin, issuer_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| issuer_name | Issuer Name | Listed company name | string | legal_name |  | join to EGRPO by name |
| ticker | Ticker | UZSE ticker | string | identifier |  | listed only |
| isin | ISIN | Securities id | string | identifier |  | listed-security key |

## Interpretation Notes

- The **Republican Stock Exchange 'Toshkent'** (`uzse.uz`) lists issuers/securities. The site
  is **reachable** but a **JS SPA**: `/issuers/` returns a small (~11 KB) shell and issuer data
  is loaded client-side. A REST backend **exists** (`uzse.uz/api/...` returns JSON
  `{"status":404,...}` for guessed paths) but the correct issuers route was **not located**.
  Browser-public but not cleanly automatable from this environment. Listed companies only.
- **Join**: UZSE does **not** publish the STIR/INN, so listed issuers join to the EGRPO
  register by **name**. **Currency** UZS.
- No `sample_record.json`: SPA shell only; no structured data captured.
