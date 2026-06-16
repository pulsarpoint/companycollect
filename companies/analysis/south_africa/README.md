# Company data sources for South Africa

## Status

- Official bulk data: **not found** (the CIPC company register is paid; no open bulk)
- Official API: **found, but the registry API is paid**; the open API is procurement (OCDS)
- Open data portal: data.gov.za was unreachable; the live open source is the eTenders OCDS API
- License: **known** — eTenders OCDS is Open Data Commons PDDL (public domain); CIPC is restricted/paid
- Recommended ingestion path: **API** (eTenders OCDS) for the open layer; CIPC for the authoritative registry is paid

## Best (open) source

**National Treasury eTenders — OCDS procurement API**
(`https://ocds-api.etenders.gov.za/api/OCDSReleases`). Publishes government
procurement as OCDS releases under the **Open Data Commons PDDL (public domain)**.
Each release has the tender, the **buyer** (government department/municipality),
**awards**, and **supplier parties** (company **names**) with **award values in
ZAR**. This is the main *open* source of South African company **names** + their
public-sector activity — but it is **partial** (only firms transacting with
government) and the supplier identifier is just the legal **name** (no CIPC
registration number).

Verified live: pulled OCDS releases — real awarded suppliers e.g. AMESTRA HOLDINGS
(ESKOM, ZAR 7.72bn), BASIL KE YONA CONSTRUCTION (Johannesburg Water, ZAR 66.5m),
GRASSROOTS HOLDINGS (Sol Plaatje Municipality, ZAR 2.49m).

## The authoritative registry (paid)

The company register is the **CIPC (Companies and Intellectual Property
Commission)**. Companies are identified by a **registration number** in the form
`YYYY/NNNNNN/NN` (the `/NN` suffix denotes type: `/07` private (Pty Ltd), `/06`
public, `/08` NPC, `/23` external, etc.). CIPC company search, disclosures,
director info, and annual financial statements are sold via **CIPC eServices /
BizPortal** (a customer code + fees). **No open bulk/API.** BizPortal is primarily
a company-**registration** service, not a free search.

## Financial data

- **Private companies: not public.** CIPC holds annual financial statements (AFS,
  filed in **iXBRL**) but does not publish them openly — they are paid.
- **Listed companies:** financials are public via the **JSE (Johannesburg Stock
  Exchange)** / **SENS** announcements. That is the only open financial route, and
  only for listed issuers.

## Identifiers & tax

- **Company registration number** (CIPC) — `YYYY/NNNNNN/NN`; the authoritative id,
  but in the paid registry.
- **Income tax number** (SARS, 10-digit) and **VAT number** (10-digit starting
  with `4`) — South Africa has **VAT**, but these are **not openly published**.
- The open OCDS data keys suppliers on **name** only.

## Next action

Ingest the eTenders **OCDS** API (open, PDDL) for company names + procurement
activity; treat **CIPC** (registry, AFS) as a paid authoritative source and
**JSE/SENS** as the listed-financial route. Sample uses real OCDS data.
