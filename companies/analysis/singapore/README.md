# Company data sources for Singapore

## Status

- Official bulk data: **found** (ACRA "Information on Corporate Entities" — open CSV on data.gov.sg, A–Z)
- Official API: **found** (data.gov.sg poll-download API; no key)
- Open data portal: **found** (data.gov.sg)
- License: **known** — Singapore Open Data Licence (free reuse with attribution)
- Recommended ingestion path: **bulk** (download the A–Z ACRA CSV datasets)

## Best source

**ACRA Information on Corporate Entities** (Accounting and Corporate Regulatory
Authority), published openly on **data.gov.sg** as a family of CSV datasets split
by first letter (A–Z + others). Every entity is keyed by its **UEN (Unique Entity
Number)**. Each record (53 columns) has: entity name, entity type
(Local Company / Sole Proprietorship / Partnership / LLP / …), business
constitution, **status** (Live / Terminated / Cancelled / Ceased / …),
registration/incorporation date, full registered address, **primary & secondary
SSIC activity** codes, number of officers, up to **15 former names**, and up to
**5 audit firms**.

Verified live: downloaded the 'B' dataset via the data.gov.sg poll-download API
(30.6 MB) — **93,896 entities**, real records (e.g. UEN `191900023K`
BRIDGESTONE SINGAPORE PTE LTD, Live Company; BATA SHOE (SINGAPORE) PRIVATE LIMITED).

## Financial data

ACRA financial statements (filed in **XBRL** via BizFinx) are **not** in the open
dataset — they are sold via **ACRA BizFile+** (business profiles / financial
statements / extracts are **paid** per document). For **listed** companies,
financial statements are public via **SGX (Singapore Exchange)**. So private-company
financials are paid; listed-company financials are open via SGX.

## Identifiers & tax

- **UEN (Unique Entity Number)** — the universal entity id (company id, registration
  number, and the entity's tax reference). Format varies by entity class
  (businesses 9-char `nnnnnnnnX`; local companies `yyyynnnnnX`; other entities
  `TyyPQnnnnX`).
- Singapore has **GST** (not VAT). The GST registration number for a local entity is
  generally its **UEN**; there is no separate VAT number.

## Next action

Download the A–Z ACRA "Information on Corporate Entities" CSV datasets (open, via
the data.gov.sg poll-download API), keyed on UEN. Add ACRA BizFile financial
statements (paid) and SGX (listed) as financial enrichments. Sample uses real
ACRA data.
