# DBD OpenAPI — Juristic Person Field Catalog

## Source Summary

- Country: Thailand
- Source type: official_registry
- Organization: Department of Business Development (DBD), Ministry of Commerce
- URL: https://openapi.dbd.go.th/api/v1/juristic_person/{id}
- License: official open API
- Access: **public, open, no key**
- Freshness: live register
- Record shape: JSON envelope `{status, data[]}`, one `cd:OrganizationJuristicPerson`
- Primary keys: cd:OrganizationJuristicID (13-digit)
- Join keys: cd:OrganizationJuristicID

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| …JuristicID | cd:OrganizationJuristicID | 13-digit juristic id | string | identifier | 0107544000108 | = company id = Tax ID |
| …NameTH | cd:OrganizationJuristicNameTH | Name (Thai) | string | legal_name | บริษัท ปตท. จำกัด (มหาชน) | |
| …NameEN | cd:OrganizationJuristicNameEN | Name (English) | string | legal_name | PTT PUBLIC COMPANY LIMITED | may be empty |
| …Type | cd:OrganizationJuristicType | Legal form | string | legal_form | บริษัทมหาชนจำกัด | |
| …RegisterDate | cd:OrganizationJuristicRegisterDate | Registration date | date | date | 20011001 | YYYYMMDD |
| …Status | cd:OrganizationJuristicStatus | Status | string | status | ยังดำเนินกิจการอยู่ | active |
| …ObjectiveCode | td:JuristicObjectiveCode | TSIC activity code | string | activity | 71209 | + TH/EN text |
| …ObjectiveTextEN | td:JuristicObjectiveTextEN | Activity (English) | string | activity | Internet access activities... | |
| …RegisterCapital | cd:OrganizationJuristicRegisterCapital | Registered capital | decimal | financial | 28562996250.0 | THB |
| …PaidUpCapital | cd:OrganizationJuristicPaidUpCapital | Paid-up capital | decimal | financial | 596740267.00 | THB |
| …BranchName | cd:OrganizationJuristicBranchName | Branch name | string | metadata | สำนักงานใหญ่ | head office |
| …Address | cd:OrganizationJuristicAddress | Structured address | object | address |  | province in CountrySubDivision |
| status.code | status.code | API status (1000=Success) | string | metadata | 1000 | check first |

## Interpretation Notes

- **Open official API** (DBD, Ministry of Commerce): `GET /api/v1/juristic_person/
  {13-digit-id}` returns JSON with **no token**. Verified live: PTT
  (0107544000108), Bangkok Bank (0107536000374), CP All (0107542000011), Internet
  Thailand (0107544000094).
- **JSON keys use XML-namespace prefixes** (`cd:`, `td:`, `cr:`). The envelope is
  `{status:{code,description}, data:[{cd:OrganizationJuristicPerson:{...}}]}` —
  check `status.code == "1000"` before parsing.
- **Identifiers**: the **13-digit juristic ID is the company id and the Tax ID**
  (one number); VAT uses the same number (no separate VAT id).
- **Address** is a nested object with administrative-area codes
  (`cr:CountrySubDivisionCode` = province, e.g. `TH-10` = Bangkok; City =
  district; CitySubDivision = sub-district).
- **Financial**: registered & paid-up **capital (THB)** are in this endpoint; full
  statements are not (see DataWarehouse).
- **Access**: per-company by 13-digit ID; **no open bulk enumeration** endpoint was
  found — iterate over an ID worklist. Dates are `YYYYMMDD` (Gregorian).
- **No personal data** (directors/shareholders) in this endpoint — PDPA-safe.
