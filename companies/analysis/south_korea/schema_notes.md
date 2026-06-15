# South Korea — Schema Notes

## Identifiers

- **법인등록번호 (Corporate Registration Number)** — **13 digits**, issued by the
  court commercial registry (IROS). The company registration id. OpenDART:
  `jurir_no`.
- **사업자등록번호 (Business Registration Number)** — **10 digits**, issued by the
  National Tax Service (NTS). The **tax id**, and simultaneously the **VAT number**
  (Korea has VAT / 부가가치세 but no separate VAT id). OpenDART: `bizr_no`; NTS API:
  `b_no`.
- **corp_code** — DART's internal **8-digit** code; the join key within OpenDART.
- **stock_code** — 6-digit listing code (listed companies only).
- Cross-source join: `corp_code` within OpenDART; **business registration number**
  links OpenDART ↔ NTS status API; **corporate registration number** links to the
  court registry.

## OpenDART `corpCode.xml` (bulk entity list)

| Field | Meaning |
|---|---|
| corp_code | DART 8-digit code (key) |
| corp_name | Korean name |
| corp_eng_name | English name |
| stock_code | 6-digit listing code (blank if unlisted) |
| modify_date | Last modified (YYYYMMDD) |

## OpenDART `company.json` (identity)

| Field | Meaning |
|---|---|
| corp_code | DART code |
| corp_name / corp_name_eng | Name (KO / EN) |
| stock_name / stock_code | Listed name / 6-digit code |
| corp_cls | Market class: Y=KOSPI, K=KOSDAQ, N=KONEX, E=other (external-audit) |
| jurir_no | 법인등록번호 (corporate registration number, 13-digit) |
| bizr_no | 사업자등록번호 (business registration number, 10-digit = tax id) |
| ceo_nm | CEO name — **personal data (PIPA), redact** |
| est_dt | Establishment date (YYYYMMDD) |
| adres | Registered address |
| induty_code | Industry code (KSIC) |
| acc_mt | Accounting month (fiscal year-end month) |
| hm_url / ir_url / phn_no / fax_no | Homepage / IR / phone / fax |

## OpenDART `fnlttSinglAcnt(All).json` (financial statements)

| Field | Meaning |
|---|---|
| bsns_year | Business (fiscal) year |
| reprt_code | 11011 annual / 11012 half / 11013 Q1 / 11014 Q3 |
| fs_div | CFS = consolidated, OFS = separate |
| sj_div | Statement: BS / IS / CIS / CF / SCE |
| sj_nm | Statement name |
| account_nm | Account line name (e.g. 매출액 revenue, 당기순이익 net income) |
| thstrm_amount | Current-term amount |
| frmtrm_amount | Prior-term amount |
| bfefrmtrm_amount | Two-years-prior amount |
| currency | Currency (KRW) |

- `fnlttSinglAcnt` returns key accounts; `fnlttSinglAcntAll` returns the full
  statement. Amounts in **KRW**.

## NTS business-status API (`/nts-businessman/v1/status`)

| Field | Meaning |
|---|---|
| b_no | 사업자등록번호 (10-digit) |
| b_stt / b_stt_cd | Business status: 계속사업자 active / 휴업 suspended / 폐업 closed |
| tax_type / tax_type_cd | VAT taxpayer type (general / simplified / tax-exempt / etc.) |
| end_dt | Closure date (if closed) |
| utcc_yn | Whether a unit-taxation business |

## Dates, money, encoding

- Dates: `YYYYMMDD` (OpenDART). Normalize to `YYYY-MM-DD`.
- Money: **KRW** integers.
- Encoding: UTF-8 JSON; `corpCode.xml` is XML inside a ZIP.

## Internal model mapping

```text
company_id          <- corp_code (within DART) / jurir_no (registration)
registration_number <- jurir_no (법인등록번호, 13-digit)
tax_id              <- bizr_no (사업자등록번호, 10-digit)
vat_id              <- bizr_no (same value; no separate VAT id)
legal_name          <- corp_name (+ corp_name_eng)
status              <- NTS b_stt (active/suspended/closed); DART listing class via corp_cls
legal_form          <- (주식회사 etc. embedded in name; court registry for exact form)
incorporation_date  <- est_dt
registered_address  <- adres
activity_code        <- induty_code (KSIC)
financials          <- fnlttSinglAcnt(All) (KRW; DART-registered companies)
officers            <- ceo_nm (OpenDART; personal data) / directors (court registry, paid)
```
