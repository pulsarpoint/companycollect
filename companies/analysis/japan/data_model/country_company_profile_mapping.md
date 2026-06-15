# Japan Company Profile — Source Mapping

> Keyed on the **13-digit Corporate Number (法人番号)** = company id = corporate
> taxpayer number. **No separate VAT** (invoice number = `T` + corporate number).
> Identity is fully open (NTA); financials are key/token-gated (EDINET/gBizINFO,
> both free); officers are paid-only (Legal Affairs Bureau, planning-only).

## Field mapping

| Profile path | Source | Source path | Join key | Freshness | License/Access | Precedence / Notes |
|---|---|---|---|---|---|---|
| registration.corporate_number | nta_houjin_bangou | col2.corporateNumber | corporate_number | monthly+daily | free/open | Authoritative id. |
| registration.assignment_date | nta_houjin_bangou | col23.assignmentDate | — | monthly+daily | free/open | NOT incorporation. |
| tax_identifiers.tax_id | nta_houjin_bangou | col2.corporateNumber | — | — | free/open | = corporate number. |
| tax_identifiers.vat_id | — | — | — | — | n/a | No VAT in Japan. |
| legal_identity.legal_name | nta_houjin_bangou | col7.name | — | monthly+daily | free/open | Primary name. |
| legal_identity.legal_name_en | nta_houjin_bangou | col25.enName | — | monthly+daily | free/open | Opt-in; often null. |
| legal_identity.legal_name_kana | nta_houjin_bangou | col29.furigana | — | monthly+daily | free/open | Phonetic. |
| legal_identity.corporate_kind | nta_houjin_bangou | col9.kind | — | monthly+daily | free/open | 101/201/301/401/499. |
| status.status / close_date / close_cause | nta_houjin_bangou | col19/col20 | — | monthly+daily | free/open | closeDate present ⇒ closed. |
| status.successor_corporate_number | nta_houjin_bangou | col21 | corporate_number | monthly+daily | free/open | Merger link. |
| registered_location.* | nta_houjin_bangou | col10-12,14-16 | — | monthly+daily | free/open | Address. |
| company_details.establishment_date | gbizinfo | date_of_establishment | corporate_number | periodic | free token | **Authoritative founding date** (NTA lacks it). |
| company_details.capital_stock | gbizinfo | capital_stock | corporate_number | periodic | free token | Not in NTA. |
| company_details.employee_number | gbizinfo | employee_number | corporate_number | periodic | free token | Not in NTA. |
| company_details.business_summary / business_items | gbizinfo | business_summary / business_items | corporate_number | periodic | free token | Closest to an industry code. |
| financial_statements[] | edinet_xbrl | results[] + XBRL facts | JCN→corporate_number | daily | free key | Listed/obligated only; JPY. |
| officers[] | houki_toukibo | registry.directors | 12-digit reg no → corporate_number | real-time | paid | PLANNING-ONLY; APPI personal data. |

## Source precedence

1. **nta_houjin_bangou** — authoritative for identity, name, address,
   corporate kind, registry-closure status. Fully open, freshest cadence.
2. **edinet_xbrl** — authoritative for **financials** of listed/disclosure-
   obligated companies (XBRL). Free key.
3. **gbizinfo** — authoritative-ish for **establishment date, capital, employees,
   industry items**; lighter financials. Free token. Use for the fields NTA lacks.
4. **houki_toukibo** — paid registry; only source of **officers** and the
   definitive incorporation date/capital/purpose. Planning-only.

Conflict rules:
- **Founding date:** prefer gBizINFO `date_of_establishment` (or the paid registry)
  over NTA `assignment_date`, which is the number-assignment date, not incorporation.
- **Financials:** prefer EDINET XBRL (authoritative) over gBizINFO `finance[]`.
- **Name/address:** prefer NTA (statutory publication, freshest).

## Join keys

- **13-digit corporate number** everywhere. EDINET exposes it as `JCN`; gBizINFO
  as `corporate_number`; the paid registry uses the 12-digit 会社法人等番号, which
  is the base of the 13-digit number.

## Missing / restricted data

- **Officers/directors** — paid registry only (APPI personal data; redact).
- **True incorporation date, capital, purpose** — gBizINFO (free, partial) or paid
  registry; NOT in NTA.
- **Financials of non-listed companies** — not openly available (EDINET = listed/
  obligated only).
- **Industry code** — no formal NACE/JSIC code in NTA open data; gBizINFO
  business_items is the closest.
- **No VAT number** — taxpayer id = corporate number.
