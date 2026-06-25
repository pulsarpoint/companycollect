# MOEA GCIS — Company Registration Basic Data Field Catalog

## Source Summary

- Country: Taiwan
- Source type: official_registry
- Organization: Ministry of Economic Affairs, Department of Commerce (GCIS / 商工登記公示資料)
- URL: https://data.gcis.nat.gov.tw/od/data/api/5F64D864-61CB-4D0D-8AD9-492047CC1EA6
- License: Open Government Data License, Taiwan
- Access: **public open JSON REST API** (no auth/payment)
- Freshness: monthly
- Record shape: JSON array of company objects
- Primary keys: Business_Accounting_NO (統一編號)
- Join keys: Business_Accounting_NO

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| Business_Accounting_NO | Business_Accounting_NO | 統一編號 (8-digit) | string | identifier | 22099131 | primary key / join key |
| Company_Name | Company_Name | Company name (Chinese) | string | legal_name | 台灣積體電路製造股份有限公司 | display name |
| Company_Status_Desc | Company_Status_Desc | Status | string | status | 核准設立 | approved/established |
| Capital_Stock_Amount | Capital_Stock_Amount | Authorized capital | integer | financial | 280500000000 | TWD |
| Paid_In_Capital_Amount | Paid_In_Capital_Amount | Paid-in capital | integer | financial | 259323700670 | TWD |
| Responsible_Name | Responsible_Name | Legal representative | string | person |  | **PERSONAL DATA — redact** |
| Company_Location | Company_Location | Registered address | string | address | 新竹科學園區… | |
| Register_Organization_Desc | Register_Organization_Desc | Registering authority | string | metadata | 科學園區管理局 | which authority registered |
| Company_Setup_Date | Company_Setup_Date | Establishment date | string | date | 0760221 | **ROC date** (→1987-02-21) |
| Change_Of_Approval_Data | Change_Of_Approval_Data | Last approval change | string | date | 1150618 | ROC date (→2026-06-18) |
| Revoke_App_Date | Revoke_App_Date | Revocation date | string | date |  | ROC; empty if active |
| Sus_App_Date… | Sus_App_Date/Beg/End, Case_Status* | Suspension / case status | string | date |  | ROC dates; group |

## Interpretation Notes

- **Authoritative open register** for **all** Taiwanese companies (公司登記基本資料). The
  REST API is **fully open** (no auth/payment). The reliable access path is
  `$filter=Business_Accounting_NO eq <8-digit 統一編號>` (URL-encode spaces). The
  `Company_Name like …` filter was **finicky** (empty body in tests); for bulk-by-name use
  GCIS downloadable dataset files.
- **統一編號 (Business_Accounting_NO)** is the **universal join key** — identical to TWSE
  `營利事業統一編號` and TPEx `UnifiedBusinessNo.` (verified for TSMC = 22099131).
- **Dates are ROC/Minguo** `YYYMMDD` (calendar year = ROC year + 1911). Convert on ingest.
- **Currency** TWD. **Language** Traditional Chinese.
- **Responsible_Name** is a natural person — redact (PDPA). Its value is intentionally
  omitted from this catalog and from committed samples.
- A real single-company response is saved at `raw/api/gcis_tsmc.json` (TSMC). No
  `sample_record.json` with the personal name is reproduced here.
