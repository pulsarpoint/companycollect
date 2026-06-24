# Schema notes — Thailand

## Identifiers

| Field | Description |
|---|---|
| **Juristic Person ID (เลขทะเบียนนิติบุคคล)** | **13-digit** company registration number = company id = **Tax ID** (one number for registration and tax). |
| **VAT** | VAT-registered businesses (ภ.พ.20) use the **same 13-digit Tax ID** — no separate VAT number. |
| **TSIC code** | Thailand Standard Industrial Classification (activity / objective code). |

## DBD OpenAPI record (`/api/v1/juristic_person/{id}`) — observed fields

| Path | Meaning | Notes |
|---|---|---|
| `status.code` / `status.description` | API result (1000 = Success) | envelope |
| `data[].cd:OrganizationJuristicPerson.cd:OrganizationJuristicID` | 13-digit juristic ID | company id = Tax ID |
| `…cd:OrganizationJuristicNameTH` | Name (Thai) | |
| `…cd:OrganizationJuristicNameEN` | Name (English) | |
| `…cd:OrganizationJuristicType` | Legal form | บริษัทจำกัด / บริษัทมหาชนจำกัด / ห้างหุ้นส่วน |
| `…cd:OrganizationJuristicRegisterDate` | Registration date | YYYYMMDD |
| `…cd:OrganizationJuristicStatus` | Status | ยังดำเนินกิจการอยู่ = active |
| `…cd:OrganizationJuristicObjective.td:JuristicObjective.td:JuristicObjectiveCode` | TSIC activity code | + TH/EN text |
| `…cd:OrganizationJuristicRegisterCapital` | Registered capital | THB |
| `…cd:OrganizationJuristicPaidUpCapital` | Paid-up capital | THB |
| `…cd:OrganizationJuristicBranchName` | Branch name | สำนักงานใหญ่ = head office |
| `…cd:OrganizationJuristicAddress.cr:AddressType` | Structured address | Address/Building/AddressNo/Road + CitySubDivision/City/CountrySubDivision codes |

## DBD DataWarehouse (financials, login-gated)

Per juristic ID, per year: **balance sheet** (total assets/liabilities/equity),
**income statement** (revenue, net profit), and financial ratios. Currency THB.

## Legal forms (ประเภทนิติบุคคล)

| Thai | English |
|---|---|
| บริษัทจำกัด | Private limited company (Co., Ltd.) |
| บริษัทมหาชนจำกัด | Public limited company (PLC) |
| ห้างหุ้นส่วนจำกัด | Limited partnership |
| ห้างหุ้นส่วนสามัญนิติบุคคล | Registered ordinary partnership |

## Status values

`ยังดำเนินกิจการอยู่` (active), `เสร็จการชำระบัญชี` (liquidated/dissolved),
`ร้าง` (struck off / defunct), `พิทักษ์ทรัพย์` (receivership).

## Internal model mapping

```
company_id          <- Juristic Person ID (13-digit)
registration_number <- Juristic Person ID
tax_id              <- Juristic Person ID (same number)
vat_id              <- same 13-digit Tax ID (no separate VAT number)
legal_name          <- NameEN (+ NameTH)
company_type        <- JuristicType (บริษัทจำกัด/บริษัทมหาชนจำกัด/ห้างหุ้นส่วน)
status              <- JuristicStatus
incorporation_date  <- RegisterDate (YYYYMMDD)
registered_address  <- structured Address
activity_code       <- TSIC JuristicObjectiveCode
capital             <- RegisterCapital / PaidUpCapital (THB)
financials          <- DBD DataWarehouse (login) / SET (listed), THB
owners/officers      <- directors/shareholders (PDPA; NOT in open API)
country             <- "Thailand"
```

## Encoding / formats

- UTF-8; Thai + English. Currency **THB**. Dates **YYYYMMDD** in the API.
- The open API uses XML-namespace-style JSON keys (`cd:`, `td:`, `cr:` prefixes).
