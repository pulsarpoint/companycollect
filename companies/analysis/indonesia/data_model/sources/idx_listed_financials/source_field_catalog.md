# IDX — Bursa Efek Indonesia (listed financials) Field Catalog

## Source Summary

- Country: Indonesia
- Source type: financial_disclosure
- Organization: PT Bursa Efek Indonesia (Indonesia Stock Exchange)
- URL: https://www.idx.co.id/
- License: public disclosure
- Access: public via browser; **Cloudflare-gated** for automation (HTTP 403)
- Freshness: quarterly / annual
- Record shape: JSON listed-company profiles + financial filings (Cloudflare-gated)
- Primary keys: ticker (Kode Emiten)
- Join keys: ticker, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| KodeEmiten | ticker | Stock ticker / issuer code | string | identifier | BBCA, TLKM, ASII | listed-company key |
| NamaEmiten | company_name | Listed company name | string | legal_name | PT Bank Central Asia Tbk | |
| TanggalPencatatan | listing_date | Listing date | date | date |  | |
| sector | sector | IDX industry classification | string | activity |  | IDX-IC |
| financial_statements | Laporan Keuangan | Financial statements | array | financial |  | listed only; IDR; PDF+XBRL |
| annual_report | Laporan Tahunan | Annual report | document | document |  | PDF |
| npwp | NPWP (in filings) | Tax id | string | identifier |  | join to AHU/OSS where present |

## Interpretation Notes

- **IDX** is the open source of **listed-company financial statements** (laporan
  keuangan) and annual reports — the only broadly **open financial** dataset for
  Indonesia (~900 listed companies). Examples (public knowledge): **BBCA** (PT Bank
  Central Asia Tbk), **TLKM** (PT Telkom Indonesia Tbk), **ASII** (PT Astra
  International Tbk).
- **Access**: the listed-company profile API returned **HTTP 403 with a Cloudflare
  challenge** — public via the browser but **Cloudflare-gated** for automated
  requests. **Not bypassed**; example values here are **public-knowledge tickers/
  names**, not scraped.
- **Scope**: listed companies only; **private-company financials are not public**.
- **Join**: the **ticker** keys the listed entity; filings carry the **NPWP**, which
  joins to AHU/OSS for the full legal identity. Currency **IDR**.
