# Taiwan Stock Exchange (TWSE) — Listed Company Basic Info Field Catalog

## Source Summary

- Country: Taiwan
- Source type: financial_disclosure
- Organization: Taiwan Stock Exchange (TWSE / 臺灣證券交易所)
- URL: https://openapi.twse.com.tw/v1/opendata/t187ap03_L
- License: Open Government Data License, Taiwan
- Access: **public open JSON API** (no auth/payment)
- Freshness: daily
- Record shape: JSON array of listed-company objects (~1,089)
- Primary keys: 公司代號 (company code)
- Join keys: 營利事業統一編號 (= GCIS 統一編號), 公司代號

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| 公司代號 | 公司代號 | Securities code | string | identifier | 2330 | listed key |
| 營利事業統一編號 | 營利事業統一編號 | 統一編號 | string | identifier | 22099131 | **JOIN KEY to GCIS** |
| 公司名稱 | 公司名稱 | Company name | string | legal_name | 台灣積體電路製造股份有限公司 | |
| 公司簡稱 | 公司簡稱 | Short name | string | legal_name | 台積電 | |
| 英文簡稱 | 英文簡稱 | English short name | string | legal_name | TSMC | |
| 產業別 | 產業別 | Industry code | string | activity | 24 | TWSE codes |
| 住址 | 住址 | Address (Chinese) | string | address | 新竹科學園區… | |
| 英文通訊地址 | 英文通訊地址 | English address | string | address | No. 8, Li-Hsin Rd. 6… | |
| 董事長 | 董事長 | Chairman | string | person |  | **PERSONAL DATA — redact** |
| 總經理 | 總經理 | General manager | string | person |  | **PERSONAL DATA — redact** |
| 成立日期 | 成立日期 | Establishment date | string | date | 19870221 | **Gregorian** |
| 上市日期 | 上市日期 | Listing date | string | date | 19940905 | Gregorian |
| 實收資本額 | 實收資本額 | Paid-in capital | string | financial | 259323700670 | TWD |
| 網址… | 網址/電子郵件信箱/簽證會計師事務所 | Website/email/auditor | string | metadata | https://www.tsmc.com | auditor names = personal data |

## Interpretation Notes

- **Fully open** TWSE OpenAPI; one JSON array of all main-board listed companies (~1,089).
  Very rich (33 fields total): identity, industry, addresses (Chinese + English),
  governance persons, dates, capital, par value, shares, transfer agent, auditor, contact.
- **Join**: 營利事業統一編號 = **GCIS 統一編號** (verified TSMC = 22099131). 公司代號 is the
  4-digit listed code (TSMC = 2330).
- **Dates**: 成立日期 / 上市日期 are **Gregorian** `YYYYMMDD`; but 出表日期 (report date) is
  **ROC** (`1150624`). Handle per field.
- **Currency** TWD. **Personal data**: 董事長 / 總經理 / 發言人 / 代理發言人 / 簽證會計師1/2
  are natural persons — redact (PDPA).
- A real TSMC record is saved at `raw/samples/twse_listed_sample_2330.json`. No
  `sample_record.json` with cleartext personal names is reproduced here.
- More TWSE OpenAPI endpoints (financial statements, dividends, governance) exist under
  `openapi.twse.com.tw` for later financial enrichment.
