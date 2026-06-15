# Australia — Schema Notes

## Identifiers

- **ABN** — Australian Business Number, **11 digits**; universal business id and
  **tax id**. The ABR primary key.
- **ACN** — Australian Company Number, **9 digits**; for **companies** (ASIC). In
  the ABR it is `ASICNumber`. A company's ABN is usually 2 check digits + its ACN.
- **ARBN** — registered foreign companies / registrable bodies (also `ASICNumber`).
- **GST registration** — indirect-tax flag; Australia has **no separate VAT
  number**.
- **ANZSIC** — industry code — **not** in the public ABR extract.

## ABR Bulk Extract — `<ABR>` record (XML, per bulkextract.xsd)

| Path | Meaning |
|---|---|
| @recordLastUpdatedDate / @replaced | Record metadata |
| ABN (@status, @ABNStatusFromDate) | ABN + status (ACT=active, CAN=cancelled) + date |
| EntityType/EntityTypeInd | Entity-type code (PUB, PRV, IND, …) |
| EntityType/EntityTypeText | Entity-type text (e.g. "Australian Public Company", "Australian Private Company", "Individual/Sole Trader") |
| MainEntity/NonIndividualName/NonIndividualNameText | Org/company name (type MN) |
| MainEntity/IndividualName (GivenName/FamilyName/NameTitle) | Person name (sole traders) — PII |
| MainEntity/BusinessAddress/AddressDetails/State, Postcode | Main business location (state + postcode only) |
| ASICNumber (@ASICNumberType) | ACN/ARBN for companies |
| GST (@status, @GSTStatusFromDate) | GST registration |
| OtherEntity/NonIndividualName (type TRD/BN) | Trading / business names |
| DGR (@DGRStatusFromDate) | Deductible Gift Recipient status |

Notes: covers **all ABN holders** (companies, sole traders, trusts, partnerships,
super funds, government) — filter by `ASICNumber` present / EntityType for
companies. **No street address** (only state + postcode), **no incorporation
date** (only the ABN registration date), **no ANZSIC**.

## ASIC company register (paid) — adds

ACN, registered office (full street) address, **incorporation date**, current
company status (Registered/Deregistered/Strike-off), company type, officeholders.

## Financials

ASIC financial reports (paid PDF; only lodging companies) + ASX (listed). VND? No
— **AUD**. VAS? No — **AASB / IFRS**.

## Mapping to internal model

| Internal | Australia source |
|---|---|
| company_id | ABN (or ACN for companies) |
| registration_number | ACN (companies) / ABN |
| tax_id | ABN |
| vat_id | not_available (GST registration flag instead) |
| legal_name | ABR NonIndividualNameText (or IndividualName) |
| company_type / legal_form | ABR EntityTypeText |
| status | ABR ABN @status (ACT/CAN); ASIC company status (paid) |
| incorporation_date | ASIC (paid) — not in ABR (ABR has ABN registration date) |
| dissolution_date | ASIC (paid) |
| registered_address | ABR: state+postcode only; full address ASIC (paid) |
| activity_code | not_available (no ANZSIC in the public extract) |
| financials | ASIC (paid) / ASX (listed); not in ABR |
| officers | ASIC officeholders (paid; PII) |
| owners | ASIC (paid) — shareholders; not open |

## Gotchas

- ABR covers **all ABN holders**, not just companies — filter for companies.
- **No street address / incorporation date / ANZSIC** in the open ABR extract —
  these need paid ASIC.
- **No separate VAT** — GST registration is the flag.
- Sole-trader/officeholder **names** are personal data — redact.
