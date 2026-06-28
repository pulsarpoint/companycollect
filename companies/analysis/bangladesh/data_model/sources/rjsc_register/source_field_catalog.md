# RJSC — Registrar of Joint Stock Companies and Firms Field Catalog

## Source Summary

- Country: Bangladesh
- Source type: official_registry
- Organization: Registrar of Joint Stock Companies and Firms (RJSC), Ministry of Commerce
- URL: https://www.roc.gov.bd/ (eservices portal)
- License: restricted
- Access: **name search free; documents pay-per-use** (no open bulk/API; TLS cert issue)
- Freshness: live
- Record shape: per-entity extract (planning-only)
- Primary keys: rjsc_registration_number
- Join keys: rjsc_registration_number, entity_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| rjsc_registration_number | Registration Number | RJSC registrar id | string | identifier |  | registry key |
| entity_name | Entity Name | Registered name | string | legal_name |  | free name search |
| entity_type | Entity Type | Type | string | legal_form |  | company/firm/society/trade org |
| status | Status | Status | string | status |  | active/struck-off |
| registration_date | Registration Date | Inc./reg. date | date | date |  | true incorporation date |
| registered_address | Registered Address | Registered office | string | address |  | |
| authorized_capital | Authorized Capital | Authorized capital | decimal | financial |  | BDT |
| paid_up_capital | Paid-up Capital | Paid-up capital | decimal | financial |  | BDT |
| directors | Directors | Directors | array | person |  | **PERSONAL DATA — redact** (paid docs) |

## Interpretation Notes

- **RJSC** is the **authoritative** Bangladeshi registrar — it registers **companies,
  partnership firms, societies, and trade organizations**, keyed on the **RJSC registration
  number**. The eservices portal offers a **free company name search** (returns name +
  number), but full **particulars/documents are pay-per-use**, and there is **no open
  bulk/API**. The main site had a **TLS intermediate-certificate issue** and the eservices
  host (`eservices.roc.gov.bd`) **did not resolve** from this environment. All fields here are
  **planning-only**, documented from public knowledge — **no values captured** (no payment).
- **Identifier**: the **RJSC registration number** is the registry key. The **registration_date**
  here is the true **incorporation date** (DSE only gives the listing year). Join DSE↔RJSC by
  **name**.
- **Personal data**: directors are natural persons — redact; they appear in paid documents
  (e.g. return of directors).
- No `sample_record.json`: restricted/paid source, nothing captured.
