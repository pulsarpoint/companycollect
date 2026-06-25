# Company Data Analysis For Hong Kong

## Summary

Hong Kong offers a **genuinely open but incremental** official feed plus an **authoritative
paid** full register. The open layer is the **Companies Registry open data on data.gov.hk**
(RNC063 weekly CSVs): newly **incorporated local** companies (RNC063L) and newly
**registered non-HK** companies (RNC063F), keyed on the **BR Number** (IRD Business
Registration number), with English/Chinese names and incorporation/registration dates, and
**no personal data** (verified `RNC063L_20241230.csv` = 3,286 rows). Richer particulars — CR
Company Number, type, status, registered office, directors, charges — live in **ICRIS
e-Search**, which is **interactive and pay-per-use** (planning-only). **HKEX List of
Securities** covers listed stocks but its static xlsx is a template (populated server-side).
A maintained company list can be built from the open feed; full particulars require ICRIS.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| cr_open_data_newly_registered | CR Newly Inc./Reg. Companies (Open Data) | ready | open CSV | data.gov.hk terms | Open incremental list: BR Number, names, dates |
| icris_esearch | Companies Registry ICRIS e-Search | blocked_payment | interactive pay-per-use | restricted | Authoritative full register: CR Number, type, status, directors, charges |
| hkex_securities | HKEX List of Securities | insufficient_transport_info | browser-public | HKEX terms | Listed stocks: stock code, ISIN (template via static URL) |

(`data_gov_hk` is the catalog/portal — the access path to the CR feed — not modeled as a separate data source.)

## What Each Source Contributes

- **CR open data (RNC063)** — the open, free layer: English + Chinese company name, **BR
  Number**, incorporation date (local) / registration date (non-HK), and name-change date.
  Weekly, incremental, no PII. Two streams (RNC063L local, RNC063F non-HK).
- **ICRIS e-Search** — the authoritative full register: **CR Company Number**, company type,
  status (Live/Dissolved/Struck off), registered office address, directors, company
  secretary, and charges/documents. Pay-per-use; personal data (PDPO) — redact. Planning-only.
- **HKEX** — listed-stock identity (stock code, name, ISIN) for the listed subset;
  browser-public, but the static xlsx is a template (populated server-side).

## Proposed Country Company Profile

A registration-keyed object (BR Number from the open feed; CR Company Number from ICRIS)
with sections: `registration` (br_number + cr_company_number), `legal_identity` (English/
Chinese name + company_type), `status` (event/Company Status + incorporation/name-change
dates), `registered_location` (ICRIS), `officers` (ICRIS, redacted), `listing` (HKEX), each
with `source_provenance`. The example is anchored on a **real open-feed company**
(3PLUS SOLUTIONS GLOBAL LIMITED / 眾加國際有限公司, BR Number 77552157) with CR Company
Number, address, and officers null (those require ICRIS).

## Join And Precedence Rules

- **Two identifiers**: BR Number (open feed) and CR Company Number (ICRIS). Join the two by
  **company name / BR Number**; HKEX joins to the register by **name** (no HK id published).
- **Precedence**: open feed authoritative for free identity (name, BR Number, dates); ICRIS
  authoritative for full particulars (paid). HKEX for listing.
- **Dates**: open feed `DD-MM-YYYY` → ISO 8601. **Currency** HKD. **Language** English +
  Traditional Chinese.

## Missing Or Restricted Data

- The **open feed is incremental and company-level only** — no registered address, status
  detail, officers, ownership, activity, or financials. All of those require **ICRIS
  (pay-per-use)** or are not published.
- **Directors / company secretary / shareholders** are personal data under the **PDPO** —
  redact; available only via ICRIS (paid).
- **HKEX** populated securities list is not cleanly available via the static xlsx.
- **No VAT** in Hong Kong — the **BR Number** is the business id.

## Common Mapper Notes

`company_id` / `tax_id` → BR Number (open); `registration_number` → CR Company Number
(ICRIS, paid). `legal_form`, `registered_address`, `officers`, `owners`, `financials`,
`activity_code`, `vat_id` are `not_available_in_open_sources` (ICRIS-paid or non-existent).
Only the CR open feed is `ready`; ICRIS is `blocked_payment`; HKEX is
`insufficient_transport_info`.
