# New Zealand — Schema Notes

## Identifiers

- **NZBN (New Zealand Business Number)** — 13 digits, a GS1 **GLN** (starts
  `9429…`). Issued to every NZ business entity (companies, sole traders,
  partnerships, trusts, government, incorporated societies, etc.). The **universal
  join key** across NZ registers.
- **Company number** — the Companies Register's own identifier for companies.
  Exposed in the NZBN API as `sourceRegisterUniqueIdentifier` when
  `sourceRegister=COMPANIES`.
- **IRD number** — Inland Revenue tax id. **Not public.**
- **GST number** = the IRD number for GST-registered entities. **Not public.**
  New Zealand has **GST, not VAT** — there is no VAT number.

## NZBN API v5 entity — publicly available fields

| Path | Meaning |
|---|---|
| nzbn | 13-digit NZBN (key) |
| entityName | Legal/registered entity name |
| entityTypeCode / entityTypeDescription | LTD (NZ Limited Company), sole trader, partnership, trust, government, overseas company, etc. |
| entityStatusCode / entityStatusDescription | Registered / Removed / In liquidation / In receivership / Struck off, etc. |
| registrationDate | Registration/incorporation date |
| sourceRegister | Originating register (e.g. COMPANIES, IR for sole traders) |
| sourceRegisterUniqueIdentifier | Id in that register (e.g. Companies Register company number) |
| addresses[] (addressType: REGISTERED / SERVICE / POSTAL) | address1-4, postCode, countryCode |
| tradingNames[] (name, startDate, endDate) | Trading/business names |
| emailAddresses[] | Contact emails (where published) |
| phoneNumbers[] | Contact phones (where published) |
| websites[] | Websites |
| industryClassifications[] | ANZSIC classification code + description |
| companyDetails (for companies) | company number, NZSX listing, constitution-filed flag, insolvency/receivership/liquidation details |

> **Restricted / not in the public tier**: `gstNumbers`, `roles` (directors /
> office holders). Those need elevated authorisation and/or the Companies Register
> UI, and are personal data.

## Companies Register (per-company)

company number, NZBN, company name, status, incorporation date, registered office
and address for service, **directors** and **shareholders** (personal data), and
**filed documents** (annual returns; **financial statements for FMC reporting
entities**). Public search; no free bulk/API.

## Disclose Register (FMA)

FMC offers and managed investment schemes under the **Financial Markets Conduct
Act 2013**: issuer, scheme, **financial statements**, product disclosure
statements, fund updates. Public document search; PDF (some XBRL). Covers only
FMC reporting entities/issuers.

## Dates, money, encoding

- Dates: ISO `YYYY-MM-DD` from the NZBN API.
- Currency: **NZD** (financial statements).
- Encoding: UTF-8 JSON (NZBN API); financial statements are PDF/XBRL documents.

## Internal model mapping

```text
company_id          <- nzbn (or company_number for companies)
registration_number <- company_number (sourceRegisterUniqueIdentifier)
tax_id              <- null (IRD number not public)
vat_id              <- null (GST country; GST number not public)
legal_name          <- entityName
company_type        <- entityTypeDescription
status              <- entityStatusDescription
incorporation_date  <- registrationDate
registered_address  <- addresses[type=REGISTERED]
activity_code       <- industryClassifications[] (ANZSIC)
financials          <- Companies/Disclose registers (FMC reporting entities only; NZD)
officers            <- Companies Register directors (personal data; not in public NZBN tier)
```
