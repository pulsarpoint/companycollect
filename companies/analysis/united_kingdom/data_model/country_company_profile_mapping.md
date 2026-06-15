# United Kingdom Company Profile — Mapping Report

The UK is **best-in-class fully-open** under the **Open Government Licence**:
register, accounts, and beneficial ownership are all free bulk products, plus a
free-key API for officers. Everything keys on the **company number** (8 chars).
No tax id/VAT in Companies House. Officers and PSC are personal data.

## Mapping Table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.company_number | ch_basic_company_data | CompanyNumber | company number | register | id + join key |
| legal_identity.legal_name | ch_basic_company_data | CompanyName | — | register > accounts name | |
| legal_identity.legal_form | ch_basic_company_data | CompanyCategory | — | register | |
| legal_identity.previous_names | ch_basic_company_data | PreviousName_1..10 | — | register | name history |
| status.status | ch_basic_company_data | CompanyStatus | — | register | Active/Dissolved/… |
| activity.sic_codes | ch_basic_company_data | SICCode.SicText_1..4 | — | register | UK SIC 2007 |
| incorporation.incorporation_date/dissolution_date | ch_basic_company_data | IncorporationDate/DissolutionDate | — | register | DD/MM/YYYY |
| registered_location.* | ch_basic_company_data | RegAddress.* | — | register | |
| accounts_meta.* | ch_basic_company_data | Accounts.* | — | register | which accounts filed |
| financial_statements[] | ch_accounts_bulk | core:* iXBRL facts | company number | accounts | GBP; e-filed only |
| officers[] | ch_rest_api | /officers items[] | company number | API (free key) | PII; only source of officers |
| owners[] | ch_psc_snapshot | data.* | company number | PSC snapshot | OPEN but PII |

## Source Precedence

1. **ch_basic_company_data** — authoritative for identity, legal form, status,
   SIC, dates, address, previous names. OGL, monthly.
2. **ch_accounts_bulk** — authoritative for **financials** (iXBRL/FRC). OGL,
   daily/monthly. Join on company number.
3. **ch_psc_snapshot** — beneficial owners (PSC). OGL, free bulk.
4. **ch_rest_api** — **officers**, filing history, charges, documents (free key).

On a name conflict, prefer the **register** `CompanyName` over the iXBRL entity
name.

## Join Keys

- **Company number** (8 chars; zero-pad numeric) joins register ↔ accounts ↔ PSC ↔
  API. **No tax id / VAT** in Companies House (VAT is HMRC, separate).

## Missing / Restricted

- **Tax id / VAT** — not in Companies House.
- **Financials coverage** — electronically-filed accounts only (~60–75%);
  paper/scanned excluded. iXBRL needs multi-context parsing.
- **Officers** — only via the free-key REST API (no bulk).
- **Personal data** — officers + PSC — redact.
