# MCA Company Master Data (data.gov.in / OGD API) Field Catalog

## Source Summary

- Country: India
- Source type: official_registry
- Organization: Ministry of Corporate Affairs (republished via NIC data.gov.in)
- URL: https://api.data.gov.in/resource/{resource_id}
- License: GODL-India (free reuse incl. commercial, attribution)
- Access: public with a free API key (public sample key works for testing)
- Freshness: point-in-time snapshots, state-wise (2015–2021)
- Record shape: flat JSON `records[]`, one object per company
- Primary keys: `corporate_identification_number` (CIN)
- Join keys: `corporate_identification_number` (CIN)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| records[].corporate_identification_number | CORPORATE_IDENTIFICATION_NUMBER | CIN (21-char) | string | identifier | L20101NL1985PLC002284 | join key; structured |
| records[].company_name | COMPANY_NAME | Company name | string | legal_name | SANGRAHALAYA TIMBER AND CRAFTS LTD | |
| records[].company_status | COMPANY_STATUS | Registry status | string | status | ACTIVE, STRIKE OFF | normalize casing |
| records[].company_class | COMPANY_CLASS | Public/Private | string | legal_form | Public | |
| records[].company_category | COMPANY_CATEGORY | Liability category | string | legal_form | Company Limited by Shares | |
| records[].company_sub_category | (SUB_)CATEGORY | Ownership sub-category | string | legal_form | Indian Non-Government Company | name differs by snapshot |
| records[].authorized_capital | AUTHORIZED_CAPITAL / authorized_cap | Authorized capital (INR) | double | financial | 200100000 | capital only |
| records[].paidup_capital | PAIDUP_CAPITAL | Paid-up capital (INR) | double | financial | 199999600 | capital only |
| records[].date_of_registration | DATE_OF_REGISTRATION | Incorporation date | string | date | 1-4-1985 / 2016-05-25T… | format differs |
| records[].registered_state | REGISTERED_STATE | State | string | geography | Nagaland | also in CIN |
| records[].registrar_of_companies | REGISTRAR_OF_COMPANIES | RoC office | string | metadata | RoC-Shillong | |
| records[].principal_business_activity | PRINCIPAL_BUSINESS_ACTIVITY(_AS_PER_CIN) | Activity desc | string | activity | Manufacturing (Wood Products) | free text |
| records[].industrial_class | industrial_class | 4-digit class (2021) | string | activity | 1100 | 2021 only |
| records[].registered_office_address | REGISTERED_OFFICE_ADDRESS | Address | string | address | P.O. - NAGINIMARA … | free text |
| records[].latest_year_ar | latest_year_ar | Latest annual-return year (2021) | string | filing | 2020-03-31T… | marker, not figures |
| records[].latest_year_bs | latest_year_bs | Latest balance-sheet year (2021) | string | filing | 2020-03-31T… | marker, not figures |

### Personal-data field (NOT in the catalog above; redacted)

- `email_addr` (2021 snapshots) — company contact email, frequently a personal
  address. **Personal data (DPDP Act 2023).** Excluded from the structured field
  list and **redacted** in the sample record. Do not redistribute.

## Interpretation Notes

- **Verified from real data** via the OGD API (public sample key): Nagaland 2015
  resource (`6a6e802c-…`) and Mizoram 2021 resource (`87f853c6-…`). 128 MCA
  "Company Master Data" resources exist (state × year, 2015–2021).
- **CIN structure** (`L20101NL1985PLC002284`): `L`/`U` listed/unlisted · `20101`
  5-digit industry · `NL` state · `1985` incorporation year · `PLC` type (PLC
  public, PTC private, NPL sec-8, GOI/GAP govt, FTC foreign, OPC one-person) ·
  `002284` 6-digit RoC sequence. The CIN is the universal join key.
- **No tax ids in the open data**: the corporate **PAN** (income-tax id) and
  **GSTIN** (GST registration) are not present. India uses **GST, not VAT** — no
  VAT number exists.
- **Financials**: only **authorized & paid-up capital** plus **latest filing-year
  markers** (`latest_year_ar/bs`). **No P&L / balance-sheet figures** — those are
  paid MCA documents or listed-company disclosures.
- **Schema drift**: 2015 vs 2021 snapshots differ in field names/casing/date
  formats (documented above); an ingester must handle both.
- **Freshness**: snapshots are point-in-time (latest 2021), not a live feed.
