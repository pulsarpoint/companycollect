# National Treasury eTenders — OCDS Field Catalog

## Source Summary

- Country: South Africa
- Source type: procurement
- Organization: National Treasury (South Africa)
- URL: https://ocds-api.etenders.gov.za/api/OCDSReleases
- License: Open Data Commons PDDL (public domain)
- Access: public, **no key** (paginated; dateFrom/dateTo required)
- Freshness: ongoing (procurement-driven)
- Record shape: OCDS release package → `releases[]`
- Primary keys: `ocid`
- Join keys: supplier `name` / `legalName` (no registration number)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| releases[].ocid | ocid | Process id | string | identifier | ocds-9t57fa-125224 | not a company id |
| releases[].tag | tag | Stage | array | metadata | compiled / award | suppliers in award/compiled |
| releases[].tender.title | tender.title | Tender title | string | activity | | procurement context |
| releases[].buyer.name | buyer.name | Government buyer | string | relationship | ESKOM | not the supplier |
| releases[].parties[].name + roles | parties[] | Party + roles | string/array | legal_name/metadata | GRASSROOTS HOLDINGS / [supplier] | filter supplier |
| releases[].parties[].identifier.legalName | identifier.legalName | Legal name | string | legal_name | | **only identifier (no reg number)** |
| awards[].suppliers[].name | suppliers[].name | Awarded supplier | string | legal_name | AMESTRA HOLDINGS | company that won |
| awards[].value.amount / .currency | value | Award value | decimal | financial | 7724415731.0 ZAR | **award value, NOT revenue** |
| awards[].date | awards[].date | Award date | datetime | date | | |

## Interpretation Notes

- **Verified from real data**: OCDS releases with `license` = ODC-PDDL (public
  domain), publisher National Treasury. Real awarded suppliers: AMESTRA HOLDINGS
  (ESKOM, ZAR 7,724,415,731), BASIL KE YONA CONSTRUCTION (Johannesburg Water, ZAR
  66,534,975), GRASSROOTS HOLDINGS (Sol Plaatje Municipality, ZAR 2,490,000).
- **This is procurement, not a company register.** It surfaces company **names**
  that have **won government tenders**, with **award values (ZAR)** and the
  government **buyer** — a partial, name-keyed view of South African firms.
- **No registration number**: the supplier party carries only `legalName`; there
  is **no CIPC registration number** or scheme. Joining to CIPC requires **name
  matching** (approximate).
- **Award value ≠ company financials**: `awards[].value.amount` is the contract
  value, not revenue. Aggregate per supplier name for a "public-sector activity"
  signal.
- **Access**: JSON, no key; paginate with `PageNumber`/`PageSize` and provide
  `dateFrom`/`dateTo`. Currency ZAR.
- **Personal data**: some supplier names may be **sole proprietors** (natural
  persons) — handle per POPIA.
