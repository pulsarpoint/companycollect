# Company Data Analysis For Taiwan

## Summary

Taiwan offers **excellent, fully open** company data — among the best of any country
investigated. A rich company profile can be built **entirely from open JSON APIs**, with no
authentication, payment, or scraping. The **MOEA GCIS Company Registration Basic Data API**
is the authoritative register for **all** companies, keyed on the 8-digit **統一編號
(Unified Business Number)**, which is also the national tax id. The **Taiwan Stock Exchange
(TWSE)** and **Taipei Exchange (TPEx)** OpenAPIs enrich the **listed** subset and carry the
same 統一編號 as a clean join key plus a 4-digit securities code. All three were verified
live (TSMC, 統一編號 22099131, TWSE code 2330).

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| gcis_company_basic | MOEA GCIS Company Basic Data API | ready | open JSON API | OGDL Taiwan | Universal register: identity, status, capital, address (all companies) |
| twse_listed | TWSE Listed Company Basic Info OpenAPI | ready | open JSON API | OGDL Taiwan | Listed (main board): code, English name, industry, listing, governance |
| tpex_listed | TPEx OTC Company Basic Info OpenAPI | ready | open JSON API | OGDL Taiwan | Listed (OTC): code, industry, governance |

## What Each Source Contributes

- **GCIS** — the authoritative open register for every Taiwanese company: 統一編號, company
  name, status (核准設立 etc.), authorized & paid-in capital (TWD), responsible person
  (redact), registered address, registering authority, and ROC-dated establishment /
  last-change / suspension fields. Reliable access by `$filter=Business_Accounting_NO eq …`.
- **TWSE** — rich disclosure for the ~1,089 main-board issuers: securities code, 統一編號
  (join), Chinese/English names, industry, addresses (Chinese + English), chairman/GM/
  spokesperson (redact), Gregorian establishment & listing dates, paid-in capital, par
  value, transfer agent, auditor, website, email. Further OpenAPI endpoints add financials.
- **TPEx** — the same shape (English field names) for the ~890 OTC issuers: securities code,
  UnifiedBusinessNo. (join), name, industry, address, chairman/GM (redact).

## Proposed Country Company Profile

A 統一編號-keyed object with sections: `registration` (unified_business_number +
securities_code), `legal_identity` (Chinese/English/short names), `status` (+ ROC→Gregorian
dates), `activity` (TWSE/TPEx industry), `registered_location` (Chinese/English address +
registering authority), `capital` (TWD), `officers` (redacted), `listing` (market, code,
listing date, website), each with `source_provenance`. The example is anchored on **TSMC**
(統一編號 22099131, TWSE 2330) with the responsible-person/chairman name redacted.

## Join And Precedence Rules

- **Primary key**: 統一編號 (universal). **Securities code** keys the listed subset.
- **Join**: GCIS ⟵統一編號⟶ TWSE/TPEx (a company is in TWSE *or* TPEx, or unlisted).
- **Precedence**: GCIS authoritative for registered identity/status/capital/address; TWSE/
  TPEx authoritative for listing, English name, industry, website.
- **Dates**: GCIS = ROC/Minguo (convert AD = ROC + 1911); TWSE/TPEx incorporation/listing =
  Gregorian. **Currency** TWD; **language** Traditional Chinese (+ English from TWSE).

## Missing Or Restricted Data

- **Personal data** (responsible person, chairman, GM, spokesperson, auditor) is in the open
  data but must be **redacted** per the PDPA in stored/committed outputs.
- **legal_form** is not a discrete GCIS basic field (inferable from the name suffix).
- **Owners / shareholders / directors-supervisors (董監事)**, branch offices (分公司),
  business activity items (營業項目), and sole-proprietor business registration (商業登記)
  are **separate GCIS datasets** — openly available, not modeled in this pass.
- **Detailed financials** beyond capital come from additional **TWSE/TPEx OpenAPI** endpoints.

## Common Mapper Notes

`company_id` / `registration_number` / `tax_id` / `vat_id` all map to the 統一編號;
`financials` → capital (TWD) plus TWSE financial endpoints; `officers` redacted. `legal_form`
and `owners` are `not_available_in_open_sources` from the modeled basic datasets (available
via other open GCIS datasets). All three sources are `ready` — no access gating.
