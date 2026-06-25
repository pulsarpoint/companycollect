# QFC Public Register Field Catalog

## Source Summary

- Country: Qatar
- Source type: official_registry
- Organization: Qatar Financial Centre Authority (QFCA)
- URL: https://eservices.qfc.qa/qfcpublicregister/publicregister.aspx
- License: unknown public register (public visibility ≠ reuse permission)
- Access: **browser-public; ASP.NET search postback** (no clean GET/bulk/API)
- Freshness: live
- Record shape: ASP.NET GridView populated by search postback (`__VIEWSTATE`)
- Primary keys: qfc_number
- Join keys: qfc_number, firm_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| firm_name | Firm Name | QFC firm name | string | legal_name |  | QFC firms only |
| qfc_number | QFC Number | QFC registration number | string | identifier |  | ≠ onshore CR |
| senior_executive_function | Senior Executive Function | Approved exec function | string | relationship |  | firm↔person role |
| approved_individual_full_name | Full Name | Approved individual name | string | person |  | **PERSONAL DATA — redact** |
| address | Address | Registered address | string | address |  | personal for individuals |
| date_of_registration | Date Of Registration | Registration date | date | date |  | Gregorian |
| qfca_licensed | QFCA Licensed | Licensing status | string | status |  | |

## Interpretation Notes

- The **QFC Public Register** lists **QFC-licensed firms**, **approved individuals**
  (with senior executive functions), **registered insolvency practitioners**, and
  **official liquidators**. It is **browser-public** with **no login or payment**, but it
  is a single ASP.NET page (`publicregister.aspx`) driven by `__VIEWSTATE` and a **search
  postback** — the result grid is **empty on a plain GET**, so there is **no clean
  GET/bulk/API**. Fields here are documented from the page's table headers; **no
  per-firm values were captured** (none fabricated).
- **Scope**: this is the **financial centre** register only. It is **not** the onshore
  Qatari companies registry (that is MoCI, keyed on the CR number). The **QFC Number** is a
  distinct identifier from the onshore CR number.
- **Personal data**: approved-individual full names and addresses are personal data under
  Qatar's Personal Data Privacy Protection Law (Law No. 13 of 2016) — redact.
- No `sample_record.json` is included: the register is postback-driven and license is an
  unknown public register, so no raw records were captured or are reproduced here.
