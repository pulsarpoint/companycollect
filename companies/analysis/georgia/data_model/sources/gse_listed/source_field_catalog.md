# Georgian Stock Exchange (GSE) — listed securities Field Catalog

## Source Summary

- Country: Georgia
- Source type: financial_disclosure
- Organization: Georgian Stock Exchange (GSE / JSC Georgian Stock Exchange)
- URL: https://gse.ge/en/securities
- License: public disclosure
- Access: **public via browser** (HTML page; no clean JSON API found)
- Freshness: event-driven
- Record shape: listed-securities HTML page
- Primary keys: isin
- Join keys: isin, security_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| isin | ISIN | Securities id | string | identifier | GE1100000029, GE2700604186 | Georgian ISINs begin GE |
| security_name | Security / Issuer Name | Security/issuer name | string | legal_name |  | listed only |
| issuer | Issuer | Issuing company | string | legal_name |  | join to NAPR by name |

## Interpretation Notes

- The **Georgian Stock Exchange** securities page is **browser-public** and exposes Georgian
  **ISINs** — **32 distinct `GExxxxxxxxxx` codes were observed** (e.g. `GE1100000029`,
  `GE2700604186`). It is a small market; **listed companies only**.
- **No clean open JSON API** was found — the page is HTML. The page does **not** publish the
  NAPR identification code, so listed issuers must be joined to the register by **name**.
- A real raw page is saved at `raw/pages/gse_securities.html`. No `sample_record.json` is
  included (individual issuer names were not parsed from the page).
