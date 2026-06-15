# ABR / ABN Lookup Bulk Extract Field Catalog

## Source Summary

- Country: Australia
- Source type: official_registry
- Organization: Australian Taxation Office (ATO) — Australian Business Register, via data.gov.au
- URL: https://data.gov.au/data/dataset/abn-bulk-extract
- License: CC-BY 3.0 Australia
- Access: public
- Freshness: weekly
- Record shape: XML `<ABR>` records in two ~492 MB zips (20 XML files: Public01..20)
- Primary keys: `ABN`
- Join keys: `ABN`, `ASICNumber` (ACN)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| ABN | ABN | Australian Business Number | string | identifier | 11000000948 | id + tax id |
| ABN/@status | status | ABN status | string | status | ACT/CAN | not ASIC status |
| ABN/@ABNStatusFromDate | ABNStatusFromDate | ABN status date | date | date | 19991101 | ≈ ABN registration, not incorporation |
| EntityType/EntityTypeInd | EntityTypeInd | Entity-type code | string | legal_form | PUB/PRV/IND | |
| EntityType/EntityTypeText | EntityTypeText | Entity-type text | string | legal_form | Australian Public Company | |
| MainEntity/NonIndividualNameText | NonIndividualNameText | Org/company name | string | legal_name | QBE INSURANCE (INTERNATIONAL) LTD | |
| MainEntity/IndividualName | IndividualName | Person name | string | person | (sole traders) | **PII — redact** |
| BusinessAddress/State | State | State | string | geography | NSW | no street |
| BusinessAddress/Postcode | Postcode | Postcode | string | address | 2000 | postcode only |
| ASICNumber | ASICNumber | ACN/ARBN | string | identifier | 000000948 | companies only; join to ASIC |
| GST (@status,@GSTStatusFromDate) | GST | GST registration | string | status | ACT, 20000701 | indirect-tax flag |
| OtherEntity/NonIndividualName | OtherEntity | Trading/business names | array | legal_name | …LIMITED | alt names |

## Interpretation Notes

- **The open identity backbone**: a free, **CC-BY 3.0 AU**, **weekly** bulk of
  **every ABN holder** (companies, sole traders, trusts, partnerships, super
  funds, government). Two zips (~492 MB each), 20 XML files. Stream `<ABR>`
  records.
- **Companies** are those with an `ASICNumber` (ACN); `EntityType` distinguishes
  public (PUB) / private (PRV) companies from individuals (IND). Filter by
  ASICNumber/EntityType to isolate companies.
- **Identifiers**: ABN (11-digit, id + tax id); ACN (9-digit, companies). Australia
  has **no separate VAT** — `GST` registration is the indirect-tax flag.
- **Not in this extract**: street address (only state + postcode),
  **incorporation date** (only the ABN status date), **ANZSIC** activity code,
  officers, financials — those need paid ASIC.
- **PII**: individual/sole-trader names (`IndividualName`) — redact. Dates are
  `YYYYMMDD`.
- `sample_record.json` is a real company record (QBE Insurance (International) Ltd),
  company-level only.
