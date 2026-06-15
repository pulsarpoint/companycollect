# OpenDART API (FSS electronic disclosure) Field Catalog

> **PLANNING-ONLY for field values.** Every OpenDART endpoint returns
> `{"status":"900"}` (or a 302 for corpCode.xml) without a free `crtfc_key`. The
> schema below is confirmed from the **official OpenDART API guide** — no records
> were fetched without a key. The disclosed data is public and reusable.

## Source Summary

- Country: South Korea
- Source type: financial_disclosure
- Organization: Financial Supervisory Service (금융감독원) / DART
- URL: https://opendart.fss.or.kr/api/
- License: public disclosure (reusable); free API key
- Access: public with a free key (`crtfc_key`)
- Freshness: live (filing-driven)
- Record shape: corpCode.xml (entity list) + company.json (identity) + fnlttSinglAcnt(All).json (financials)
- Primary keys: `corp_code`
- Join keys: `corp_code`, `jurir_no`, `bizr_no`, `stock_code`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| corpCode.list[].corp_code | corp_code | DART 8-digit code | string | identifier | join key |
| corpCode.list[].corp_name / corp_eng_name | corp_name(_eng) | Name KO/EN | string | legal_name | |
| corpCode.list[].stock_code | stock_code | 6-digit listing code | string | identifier | listed only |
| company.corp_cls | corp_cls | Market class | string | legal_form | Y/K/N/E |
| company.jurir_no | jurir_no | 법인등록번호 (13-digit) | string | identifier | corp reg no; court link |
| company.bizr_no | bizr_no | 사업자등록번호 (10-digit) | string | identifier | tax id = VAT no; NTS link |
| company.ceo_nm | ceo_nm | CEO name | string | person | **PERSONAL DATA (PIPA) — redact** |
| company.est_dt | est_dt | Establishment date | date | date | YYYYMMDD |
| company.adres | adres | Address | string | address | |
| company.induty_code | induty_code | Industry (KSIC) | string | activity | |
| company.acc_mt | acc_mt | Fiscal year-end month | string | metadata | |
| financial.bsns_year + reprt_code | bsns_year/reprt_code | Year + report | string | date | 11011 annual etc. |
| financial.sj_div + account_nm | sj_div/account_nm | Statement + account | string | financial | BS/IS/CIS/CF/SCE |
| financial.thstrm_amount | thstrm_amount | Current-term amount | decimal | financial | KRW; fs_div CFS/OFS |

## Interpretation Notes

- **Three endpoints, one key**: `corpCode.xml` (full DART entity list) → use the
  `corp_code` to call `company.json` (identity) and `fnlttSinglAcnt(All).json`
  (financials). All require `crtfc_key`.
- **Identifiers**: `jurir_no` = 법인등록번호 (13-digit corporate registration number,
  court-issued); `bizr_no` = 사업자등록번호 (10-digit business registration number,
  NTS-issued = **tax id and VAT number**; Korea has no separate VAT id). Keep both
  as strings (leading zeros).
- **Coverage**: all **listed** (corp_cls Y/K/N) + **external-audit** companies
  (corp_cls E) — the disclosure-obligated universe, not every micro-company.
- **Financials**: `fnlttSinglAcnt` returns key accounts; `fnlttSinglAcntAll`
  returns the full statement (BS/IS/CIS/CF/SCE). `fs_div` CFS=consolidated,
  OFS=separate; amounts in **KRW**; `thstrm`/`frmtrm`/`bfefrmtrm` = this/prior/two-
  years-prior.
- **Personal data**: `ceo_nm` is personal data (PIPA) — redact.
- No raw sample record (key-gated); the combined example is schematic.
