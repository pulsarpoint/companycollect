# Taiwan Company Data Investigation

## Goal

Find official/open sources for Taiwanese company data: registry, identifiers, status,
capital, activities, officers, and listing/financial data, with reproducible access notes.

## What was found

Taiwan has **excellent open company data** — among the most open registries investigated.
Three official open JSON APIs cover the full company population plus the listed markets,
and they share a single join key.

1. **MOEA GCIS — Company Registration Basic Data (OpenData API)** — Ministry of Economic
   Affairs, Department of Commerce (商工登記公示資料 / Government Commerce Industrial
   Services). `https://data.gcis.nat.gov.tw/od/data/api/5F64D864-61CB-4D0D-8AD9-492047CC1EA6`.
   The official register of **all** Taiwanese companies, keyed on the 8-digit
   **統一編號 (Unified Business Number / `Business_Accounting_NO`)**. **Fully open** JSON
   REST API — no authentication, no payment. Verified live: a query by `Business_Accounting_NO eq 22099131`
   returned **TSMC** with `Company_Name` (台灣積體電路製造股份有限公司),
   `Company_Status_Desc` (核准設立), `Capital_Stock_Amount` (280,500,000,000),
   `Paid_In_Capital_Amount` (259,323,700,670), `Responsible_Name` (legal representative —
   personal data), `Company_Location`, `Register_Organization_Desc`, `Company_Setup_Date`
   (`0760221` = ROC 076 → 1987-02-21), and `Change_Of_Approval_Data` (`1150618` → 2026-06-18).
   The API is OData-style (`$format`, `$filter`, `$skip`, `$top`); the `eq` filter by
   `Business_Accounting_NO` is the reliable access path. The `Company_Name like …` filter
   was **finicky** (returned an empty body in tests) — prefer ID lookups, or use the GCIS
   downloadable dataset files for bulk.

2. **Taiwan Stock Exchange (TWSE) — Listed Company Basic Info OpenAPI** —
   `https://openapi.twse.com.tw/v1/opendata/t187ap03_L`. **Fully open** JSON. Verified
   live: **1,089 listed companies** as a single array. Very rich: 公司代號 (securities
   code, e.g. 2330), 公司名稱, 公司簡稱, **營利事業統一編號** (= GCIS join key),
   產業別 (industry), 住址, 董事長/總經理/發言人 (chairman/GM/spokesperson — personal
   data), 成立日期 / 上市日期 (Gregorian `YYYYMMDD`), 實收資本額 (paid-in capital),
   普通股每股面額 (par value), 英文簡稱 / 英文通訊地址, 網址, 電子郵件信箱,
   簽證會計師事務所 (auditor). Many more TWSE OpenAPI endpoints exist (financials,
   dividends, governance) under `openapi.twse.com.tw`.

3. **Taipei Exchange (TPEx) — OTC Listed Company Basic Info OpenAPI** —
   `https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O`. **Fully open** JSON. Verified
   live: **890 OTC companies** with English field names: `SecuritiesCompanyCode`,
   `CompanyName`, `UnifiedBusinessNo.` (= GCIS join key), `SecuritiesIndustryCode`,
   `Address`, `Chairman`, `GeneralManager`. Complements TWSE for the OTC market.

4. **data.gov.tw** — the National Development Council open-data catalog (HTTP 200). It
   **indexes** the GCIS and TWSE/TPEx datasets rather than hosting a separate register;
   useful for discovering additional GCIS datasets (directors/董監事, branches/分公司,
   business registration/商業登記 for sole proprietorships and partnerships).

## Join key

All three company sources share the **統一編號 (Unified Business Number)**:
GCIS `Business_Accounting_NO` == TWSE `營利事業統一編號` == TPEx `UnifiedBusinessNo.`
(verified for TSMC = `22099131`). This gives a clean universal → listed join.

## What was NOT found / caveats

- The GCIS `Company_Name like` filter was unreliable in testing; bulk-by-name needs the
  downloadable dataset files or careful filter syntax.
- Directors/supervisors (董監事), branch offices, and sole-proprietor business registration
  are additional GCIS datasets not pulled in this pass (catalog via data.gov.tw).
- Officer/responsible-person names are present in the open data but are personal data under
  Taiwan's PDPA — redact in stored profiles and committed samples.

## Conclusion

Taiwan is a **recommended / API** country with **fully open** official company data. Use
**GCIS** (by 統一編號) for the universal company layer and **TWSE + TPEx** for the listed
layer, joined on the unified business number.

## Recommended ingestion approach

API. GCIS by-統一編號 lookups (or GCIS bulk dataset files) for all companies; TWSE and TPEx
full listed arrays for the disclosure/listing layer. Convert ROC/Minguo dates (GCIS) to
Gregorian; TWSE/TPEx setup/listing dates are already Gregorian. Redact personal names.
