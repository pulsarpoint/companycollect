# Thailand — combined profile mapping

## Join keys & precedence

- **Primary join key: the 13-digit juristic person ID** — it is the company
  registration number **and** the Tax ID (one number; VAT uses the same number).
  For listed companies the **SET symbol** is an additional key (filings carry the
  juristic ID to join back).
- **Precedence**: the **DBD OpenAPI** is authoritative for identity, status,
  activity, capital, and address (open, no key). **DBD DataWarehouse** is
  authoritative for full **financial statements** (login). **SET** covers the
  listed subset.

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.juristic_id | dbd_openapi_juristic | cd:OrganizationJuristicID | juristic_id | authoritative | = Tax ID |
| tax_identifiers.tax_id | dbd_openapi_juristic | cd:OrganizationJuristicID | juristic_id | authoritative | same number |
| tax_identifiers.vat_id | dbd_openapi_juristic | cd:OrganizationJuristicID | juristic_id | authoritative | no separate VAT number |
| legal_identity.name_en/th | dbd_openapi_juristic | cd:OrganizationJuristicNameEN/TH | juristic_id | authoritative |  |
| legal_identity.legal_form | dbd_openapi_juristic | cd:OrganizationJuristicType | juristic_id | authoritative | บริษัทจำกัด/มหาชน/ห้างหุ้นส่วน |
| status.status_text | dbd_openapi_juristic | cd:OrganizationJuristicStatus | juristic_id | authoritative |  |
| status.register_date | dbd_openapi_juristic | cd:OrganizationJuristicRegisterDate | juristic_id | authoritative | YYYYMMDD |
| activity.tsic_code | dbd_openapi_juristic | td:JuristicObjectiveCode | juristic_id | authoritative | TSIC |
| registered_location.* | dbd_openapi_juristic | cd:OrganizationJuristicAddress | juristic_id | authoritative | structured |
| capital.* | dbd_openapi_juristic | RegisterCapital/PaidUpCapital | juristic_id | authoritative | THB |
| listing.* | set_listed | symbol/sector | symbol/juristic_id | authoritative (listed) | SET |
| financial_statements[] | dbd_datawarehouse | balance/income | juristic_id | authoritative (gated) | THB; login / SET |

## Freshness

- DBD OpenAPI: **live**. DataWarehouse: **annual** (login). SET: **quarterly**.

## Missing-data notes

- **No open bulk enumeration** — the DBD OpenAPI is per-company by 13-digit ID
  (drive by an ID worklist).
- **Full financial statements** are **login-gated** (DataWarehouse) or listed-only
  (SET); only **capital** is open (via the OpenAPI).
- **No separate VAT number** (same as Tax ID).
- **Directors/shareholders** (PDPA) are not exposed by the open API.
- `data.go.th` was WAF-blocked for automation from this environment.
