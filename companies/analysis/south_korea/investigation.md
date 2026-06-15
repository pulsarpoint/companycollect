# South Korea Company Data Investigation

## Conclusion

South Korea has an **excellent open financial + identity API (free key)** for the
disclosure-obligated universe, plus **free-key tax-status lookups**, with the
**full unlisted register fee-based**:

- **Identity + financials (free key):** the **OpenDART API** (Financial
  Supervisory Service). With a free `crtfc_key` it serves the bulk DART entity
  list (`corpCode.xml`), company identity (`company.json`, incl. both the
  corporate and business registration numbers), and **XBRL financial statements**
  (`fnlttSinglAcnt(All)`) in KRW. Coverage = all **listed** + **external-audit**
  companies.
- **Tax status (free key):** the **NTS business-status API** (data.go.kr / odcloud)
  returns business-registration status by 사업자등록번호.
- **Full register (paid):** the **IROS** Supreme Court commercial registry holds
  every company incl. **unlisted**, but extracts are fee-based per issue.

## What was verified live

- **OpenDART**: `list.json`, `company.json`, `fnlttSinglAcnt.json` all return
  `{"status":"900"}` (rejected) and `corpCode.xml` returns **302** without a key →
  free `crtfc_key` required. The field schemas were confirmed from the official API
  guide pages (company / corpCode / financial-statement endpoints).
- **NTS business-status API**: POST without a key → **401**
  `{"code":-401,"msg":"인증키는 필수 항목 입니다."}` ("authentication key is
  required") → free data.go.kr service key required.
- **DART portal, data.go.kr, IROS**: all reachable (HTTP 200).

## Identifiers

- **법인등록번호 (Corporate Registration Number)** — **13-digit**, issued by the
  court commercial registry (IROS). The company registration id. In OpenDART:
  `jurir_no`.
- **사업자등록번호 (Business Registration Number)** — **10-digit**, issued by the
  National Tax Service. The **tax id**. In OpenDART: `bizr_no`; in the NTS API:
  `b_no`. Korea has **VAT (부가가치세)**, but the **VAT number is the business
  registration number** — there is **no separate VAT id**.
- **corp_code** — DART's internal **8-digit** code; the join key within OpenDART
  (corpCode ↔ company ↔ financials).
- **stock_code** — 6-digit listing code (listed companies only).

## OpenDART field schema (confirmed from the API guide)

- `corpCode.xml`: `corp_code`, `corp_name`, `corp_eng_name`, `stock_code`,
  `modify_date`.
- `company.json`: `corp_code`, `corp_name`, `corp_name_eng`, `stock_name`,
  `stock_code`, `corp_cls` (Y=KOSPI / K=KOSDAQ / N=KONEX / E=other/external-audit),
  `jurir_no`, `bizr_no`, `adres`, `hm_url`, `ir_url`, `phn_no`, `fax_no`,
  `induty_code` (KSIC), `est_dt`, `acc_mt`, `ceo_nm`.
- `fnlttSinglAcnt(All).json`: `bsns_year`, `reprt_code` (11011 annual / 11012 H1 /
  11013 Q1 / 11014 Q3), `fs_div` (CFS consolidated / OFS separate), `sj_div`
  (BS/IS/CIS/CF/SCE), `sj_nm`, `account_nm`, `thstrm_amount` (current),
  `frmtrm_amount` (prior), `bfefrmtrm_amount` (two-years-prior), `currency`.

## What is NOT openly available (free)

- **Unlisted micro/SME companies' full registration** — court registry, fee-based.
- **Financials of non-DART companies** — only DART-registered (listed +
  external-audit) file via DART.
- **Directors** — court registry (paid); CEO name is in OpenDART (personal data).

## Recommended ingestion

1. **OpenDART** (free key): `corpCode.xml` → `company.json` per corp_code →
   `fnlttSinglAcntAll` per year/report. Identity + financials.
2. **NTS status API** (free key): enrich tax-registration status by 사업자등록번호.
3. Treat IROS / KED / NICE as paid options for the unlisted long tail.
4. CEO/director names are personal data (PIPA) — redact in shared samples.
