# Taiwan Company Profile — Mapping

Taiwan has **fully open** official company data. The **MOEA GCIS** Company Basic Data API is
authoritative for **all** companies, keyed on the 8-digit **統一編號 (Unified Business
Number)**. **TWSE** (main board) and **TPEx** (OTC) OpenAPIs enrich the **listed** subset
and carry the same 統一編號 as a join key plus a 4-digit securities code. All three are open
JSON APIs (Open Government Data License, Taiwan).

## Identifiers

- **統一編號 (Unified Business Number)** — 8-digit; primary key and tax id. Universal join:
  GCIS `Business_Accounting_NO` == TWSE `營利事業統一編號` == TPEx `UnifiedBusinessNo.`
  (verified for TSMC = `22099131`).
- **Securities code** — 4-digit listed code (TSMC = `2330`); from TWSE/TPEx, listed only.

## Mapping table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.unified_business_number | gcis_company_basic | Business_Accounting_NO | yes | GCIS | primary key / tax id |
| registration.securities_code | twse_listed / tpex_listed | 公司代號 / SecuritiesCompanyCode | yes | TWSE>TPEx | listed only |
| legal_identity.legal_name | gcis_company_basic | Company_Name | no | GCIS > TWSE | |
| legal_identity.legal_name_en | twse_listed | 英文簡稱 | no | TWSE | listed only |
| legal_identity.short_name | twse_listed | 公司簡稱 | no | TWSE | |
| status.status_text | gcis_company_basic | Company_Status_Desc | no | GCIS | 核准設立 etc. |
| status.incorporation_date | gcis_company_basic | Company_Setup_Date | no | GCIS (ROC) / TWSE (Gregorian) | convert ROC |
| status.last_change_date | gcis_company_basic | Change_Of_Approval_Data | no | GCIS | ROC→Gregorian |
| activity.industry_code | twse_listed / tpex_listed | 產業別 / SecuritiesIndustryCode | no | TWSE/TPEx | listed only |
| registered_location.address | gcis_company_basic | Company_Location | no | GCIS > TWSE | |
| registered_location.address_en | twse_listed | 英文通訊地址 | no | TWSE | listed only |
| registered_location.register_organization | gcis_company_basic | Register_Organization_Desc | no | GCIS | |
| capital.* | gcis_company_basic | Capital_Stock_Amount / Paid_In_Capital_Amount | no | GCIS | TWD |
| officers[] | gcis_company_basic / twse_listed / tpex_listed | Responsible_Name / 董事長,總經理 / Chairman,GeneralManager | no | — | **PERSONAL DATA — REDACT** |
| listing.* | twse_listed / tpex_listed | 公司代號,上市日期,網址 / SecuritiesCompanyCode | no | TWSE/TPEx | listed only |
| source_provenance[] | all | n/a | n/a | n/a | per-section provenance |

## Precedence and joins

- **Universal identity / registration / status / capital / address**: **GCIS** is
  authoritative (covers all companies). **English name, industry, listing, English address,
  website**: from **TWSE/TPEx** (listed subset).
- **Join**: GCIS ⟵統一編號⟶ TWSE/TPEx. A company is in TWSE *or* TPEx (or neither, if
  unlisted). Where both a GCIS and a TWSE value exist for the same concept (e.g. address),
  prefer GCIS for the registered value and keep TWSE as the disclosure/English variant.
- **Dates**: GCIS = **ROC/Minguo** (convert: AD = ROC + 1911); TWSE/TPEx incorporation &
  listing dates = **Gregorian**. Store ISO 8601 after conversion.
- **Currency** TWD. **Language** Traditional Chinese (+ English from TWSE).

## Missing / restricted

- **Personal data** (responsible person, chairman, GM, spokesperson, auditor) is present in
  the open data but must be **redacted** per Taiwan's PDPA in stored/committed outputs.
- **Directors/supervisors (董監事)**, branch offices (分公司), and sole-proprietor business
  registration (商業登記) are **additional GCIS datasets** not modeled in this pass —
  available openly via the GCIS catalog for later enrichment.
- **VAT**: there is no separate VAT id — the 統一編號 serves as the unified business/tax id.
