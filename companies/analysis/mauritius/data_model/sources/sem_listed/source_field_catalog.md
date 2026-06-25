# Stock Exchange of Mauritius (SEM) — listed companies Field Catalog

## Source Summary

- Country: Mauritius
- Source type: financial_disclosure
- Organization: Stock Exchange of Mauritius Ltd (SEM)
- URL: https://www.stockexchangeofmauritius.com/
- License: public disclosure
- Access: **public via browser** (navigable HTML; no clean list/API)
- Freshness: event-driven
- Record shape: per-issuer listing pages (HTML/PDF)
- Primary keys: issuer_name
- Join keys: issuer_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| issuer_name | Issuer / Company Name | Listed company name | string | legal_name |  | join to CBRD by name |
| market_segment | Market Segment | SEM segment | string | metadata | Official Market, DEM | |
| published_accounts | Published Accounts | Financial statements | array | financial |  | PDF; MUR |
| announcements | Company Announcements | Disclosures | array | filing |  | per-issuer |

## Interpretation Notes

- The **Stock Exchange of Mauritius** publishes, **browser-public**, per-issuer pages across
  `/listing-issuer-services/` for the **Official Market** and **DEM (Development & Enterprise
  Market)** segments, including **published accounts** (financial statements, PDF) and
  **company announcements**.
- **No clean single listed-companies list or JSON API** was found — the content is navigable
  HTML per issuer/segment. **Listed companies only**.
- **Join**: SEM does not publish the BRN, so listed issuers join to the CBRD register by
  **name**. **Currency** MUR.
- No `sample_record.json`: navigable HTML only; no structured per-issuer record captured.
