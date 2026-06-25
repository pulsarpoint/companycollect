# Taiwan Schema Notes

## Identifiers

- **統一編號 (Unified Business Number)** — 8-digit national business id. The universal join
  key. GCIS `Business_Accounting_NO` == TWSE `營利事業統一編號` == TPEx `UnifiedBusinessNo.`
  (verified: TSMC = `22099131`).
- **公司代號 / SecuritiesCompanyCode** — 4-digit securities code for listed companies
  (e.g. TSMC = `2330`). Listed-only; from TWSE/TPEx.

## GCIS Company Basic Data — observed fields (verified from TSMC record)

| Field | Meaning |
|---|---|
| Business_Accounting_NO | 統一編號 (8-digit unified business number) — primary key |
| Company_Name | Registered company name (Chinese) |
| Company_Status_Desc | Status (e.g. 核准設立 = approved/established) |
| Capital_Stock_Amount | Authorized capital (TWD) |
| Paid_In_Capital_Amount | Paid-in capital (TWD) |
| Responsible_Name | Legal representative (natural person — **redact**) |
| Company_Location | Registered address |
| Register_Organization_Desc | Registering authority |
| Company_Setup_Date | Establishment date — **ROC/Minguo** YYYMMDD (076→1987) |
| Change_Of_Approval_Data | Last approval-change date — ROC YYYMMDD |
| Revoke_App_Date / Sus_* / Case_Status* | Revocation / suspension / case status fields |

## TWSE Listed Company Basic Info — observed fields

公司代號 (code), 公司名稱, 公司簡稱, 外國企業註冊地國, 產業別 (industry), 住址,
**營利事業統一編號** (join key), 董事長/總經理/發言人/代理發言人 (persons — **redact**),
總機電話, 成立日期 / 上市日期 (**Gregorian** YYYYMMDD), 普通股每股面額 (par value),
實收資本額 (paid-in capital), 私募股數, 特別股, 編制財務報表類型, 股票過戶機構/電話/地址,
簽證會計師事務所/簽證會計師1/2 (auditor — persons), 英文簡稱, 英文通訊地址, 傳真機號碼,
電子郵件信箱, 網址, 已發行普通股數.

## TPEx OTC Company Basic Info — observed fields

Date, SecuritiesCompanyCode, CompanyName, CompanyAbbreviation, Registration (foreign reg.
country), SecuritiesIndustryCode, Address, **UnifiedBusinessNo.** (join key), Chairman,
GeneralManager.

## Dates, language, encoding

- **GCIS dates: ROC/Minguo** — format `YYYMMDD` where calendar year = ROC year + 1911
  (e.g. `0760221` = 1987-02-21; `1150618` = 2026-06-18). Convert on ingest.
- **TWSE/TPEx 成立日期/上市日期: Gregorian** `YYYYMMDD` (e.g. `19870221`). But TWSE
  `出表日期` (report date) is ROC (`1150624`). Field-by-field date handling required.
- Language: Traditional Chinese (UTF-8); TWSE adds English short name/address.
- Currency: New Taiwan Dollar (TWD).

## Mapping to internal model

- company_id ← 統一編號 (Business_Accounting_NO)
- registration_number ← 統一編號
- tax_id / vat_id ← 統一編號 (the unified business number serves as the tax id)
- legal_name ← Company_Name (Chinese); legal_name_en ← TWSE 英文簡稱 (listed)
- company_code ← 公司代號 / SecuritiesCompanyCode (listed)
- status ← Company_Status_Desc
- incorporation_date ← Company_Setup_Date (ROC→Gregorian) / 成立日期 (Gregorian)
- registered_address ← Company_Location / 住址 / Address
- financials/capital ← Paid_In_Capital_Amount, Capital_Stock_Amount (TWD)
- officers ← Responsible_Name / 董事長 / 總經理 (**redact**)
- source_url, source_name, source_retrieved_at preserved per record
