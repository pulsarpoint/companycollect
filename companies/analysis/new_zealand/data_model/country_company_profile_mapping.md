# New Zealand Company Profile — Source Mapping

> Keyed on the **13-digit NZBN** (universal entity id; a GLN). The Companies
> Register **company number** is the secondary id (NZBN
> `sourceRegisterUniqueIdentifier`). Identity is from the **NZBN API** (free
> subscription key — key-gated/planning-only here). NZ has **GST, not VAT**;
> IRD/GST numbers are not public. Financials exist only for **FMC reporting
> entities** (Disclose/Companies registers). Directors are personal data.

## Field mapping

| Profile path | Source | Source path | Join key | Freshness | License/Access | Precedence / Notes |
|---|---|---|---|---|---|---|
| registration.nzbn | nzbn_api | nzbn | nzbn | live | free key | Authoritative id. |
| registration.company_number | nzbn_api | sourceRegisterUniqueIdentifier | company_number | live | free key | = company number (sourceRegister=COMPANIES). |
| registration.source_register | nzbn_api | sourceRegister | — | live | free key | COMPANIES/IR/etc. |
| tax_identifiers.ird_number / gst_number / vat_id | — | — | — | — | not available | IRD/GST not public; no VAT. |
| legal_identity.legal_name | nzbn_api | entityName | — | live | free key | Primary name. |
| legal_identity.entity_type | nzbn_api | entityTypeDescription | — | live | free key | LTD/sole trader/etc. |
| legal_identity.trading_names | nzbn_api | tradingNames[].name | — | live | free key | |
| status.status | nzbn_api | entityStatusDescription | — | live | free key | Registered/Removed/In liquidation. |
| incorporation.registration_date | nzbn_api | registrationDate | — | live | free key | |
| activity.industry_classifications | nzbn_api | industryClassifications[] | — | live | free key | ANZSIC 2006. |
| registered_location.* | nzbn_api | addresses.addressList[type] | — | live | free key | REGISTERED/SERVICE. |
| financial_statements[] | disclose_register / companies_register | offer.financial_statements / company.documents | nzbn / company_number | filing-driven | public docs | PLANNING-ONLY; FMC reporting entities only; NZD. |
| officers[] | companies_register | company.directors[] | company_number | live | gated | PLANNING-ONLY; personal data (Privacy Act) — redact. |

## Source precedence

1. **nzbn_api** — authoritative for identity, status, type, addresses, trading
   names, ANZSIC industry, and the company number. Free subscription key.
2. **companies_register** — directors/shareholders (personal data) and filed
   documents; search-only. Use to refine status or for officers when lawful.
3. **disclose_register** — financial statements for the FMC-reporting subset.

Conflict rules:
- **Identity:** NZBN API is authoritative; the Companies Register UI is a fallback
  for fields not in the public NZBN tier (directors, some documents).
- **Financials:** Disclose Register / Companies Register filed statements — only
  for entities required to file; ordinary companies have none.

## Join keys

- **NZBN (13-digit)** is the universal key. The **company number**
  (`sourceRegisterUniqueIdentifier`) links to the Companies Register and the
  Disclose Register's issuer. IRD/GST are not available to join on.

## Missing / restricted data

- **Free bulk** — none; NZBN API per-entity/search (free key) only.
- **Financial statements** — only FMC reporting entities (issuers, large/overseas-
  owned, managed investment schemes). Most companies file none.
- **IRD/GST numbers** — not public; **no VAT** (GST regime).
- **Directors/shareholders** — Companies Register only; personal data (Privacy Act
  2020), not in the public NZBN tier.
