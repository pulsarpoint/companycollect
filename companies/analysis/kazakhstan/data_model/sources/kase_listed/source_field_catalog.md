# Kazakhstan Stock Exchange (KASE) — listed companies Field Catalog

## Source Summary

- Country: Kazakhstan
- Source type: financial_disclosure
- Organization: Kazakhstan Stock Exchange (KASE)
- URL: https://kase.kz/en/
- License: public disclosure
- Access: **public via browser** (listing pages redirect / SPA; no clean static list/API)
- Freshness: event-driven
- Record shape: listed-issuer pages (HTML / SPA)
- Primary keys: isin
- Join keys: isin, issuer_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| issuer_name | Issuer Name | Listed company name | string | legal_name |  | join to gbd_ul by name |
| ticker | Ticker | KASE ticker | string | identifier |  | listed only |
| isin | ISIN | Securities id | string | identifier | KZ... | Kazakhstani ISINs begin KZ |

## Interpretation Notes

- The **Kazakhstan Stock Exchange** lists companies/securities (shares, bonds). The site is
  **browser-public** but the `/en/shares` and `/en/issuers` pages **301-redirect** (SPA /
  trailing-variant), and **no clean open JSON API was confirmed** from static fetches. Listed
  companies only; Kazakhstani **ISINs** (`KZxxxxxxxxxx`).
- **Join**: KASE does **not** publish the BIN, so listed issuers join to `gbd_ul` by **name**.
  **Currency** KZT.
- No `sample_record.json`: listing pages redirect (SPA); no structured data captured.
