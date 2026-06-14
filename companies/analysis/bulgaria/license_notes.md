# Bulgaria — License & Terms Notes

> Bulgaria's registry data is open-ish (CC-BY publications), but financials are document-based and full bulk
> needs an agreement. Record attribution.

## Търговски регистър (Commercial Register) — Registry Agency
- **Free public search** for single-company lookups (unrestricted).
- The Registry Agency's **daily publications** are published on **data.egov.bg under CC-BY** — free reuse,
  including commercial, **with attribution** to the Registry Agency / data.egov.bg.
- **Bulk extraction for a commercial database** is distinguished from single lookups and requires a
  **data-sharing agreement** with the Registry Agency. Do not assume the right to mass-extract via the
  public search.
- The official **web service / API** for integration requires **registration/contract**.

## ГФО — Annual Financial Statements
- Filed to the Commercial Register and **public** (by 30 June). Reuse of the filed documents is per the
  register terms. They are **PDF documents** (no structured open data); attribute the register.

## data.egov.bg
- License is **per dataset** (the Registry Agency dataset is **CC-BY**). Resource data via the portal API
  may need an **api_key**. Check each dataset's license field.

## CompanyBook.BG + commercial aggregators
- CompanyBook: non-financial data free (per its terms); **financials paid**. APIS and others: proprietary,
  paid, per-vendor contract. The underlying register data remains CC-BY (attribute the Registry Agency).

## Beneficial ownership (Регистър на действителните собственици)
- Filed within the commercial register; **access conditions apply** (legitimate interest post-CJEU). Not
  open bulk.

## Personal data / GDPR
- Register data includes natural persons (sole traders, managers, beneficial owners). Apply a GDPR lawful
  basis + retention policy before persisting personal data; honor the register's reuse terms.

## Summary recommendation
- **Free to use (with attribution)**: the **CC-BY daily publications** (data.egov.bg) + free public search
  (single lookups). For the **full bulk**, obtain a **data-sharing agreement**.
- **Financials** are public PDFs — parse them or use a paid provider; attribute the register.
- Beneficial ownership is access-restricted.
