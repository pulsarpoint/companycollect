# Australia Company Data — Investigation

## Conclusion

Australia has an **open identity backbone** (the ABR Bulk Extract) but
**financials are mostly paid / listed-only**:

- **Identity**: the **Australian Business Register (ABR) / ABN Lookup Bulk
  Extract** (Australian Taxation Office, via data.gov.au) is a **free, CC-BY 3.0
  AU**, **weekly** bulk of **every ABN holder** — XML in two ~492 MB zips
  (`public_split_1_10.zip`, `public_split_11_20.zip`), 20 XML files. Verified
  live (real record: QBE INSURANCE (INTERNATIONAL) LTD, ABN 11000000948, ACN
  000000948).
- **Company-register detail**: **ASIC** holds the company register (ACN,
  registered office, incorporation date, status, officeholders) — mostly **paid
  per extract** via ASIC Connect.
- **Financials**: lodged with **ASIC** and available **per-document for a fee**;
  **only certain companies must lodge** (public, large proprietary, disclosing
  entities, schemes). Most small proprietary companies do **not** lodge publicly.
  **Listed companies** disclose via the **ASX**.

## Identifiers

- **ABN** — Australian Business Number, **11 digits**; the universal business id
  and the **tax id**. Present for all registered businesses.
- **ACN** — Australian Company Number, **9 digits**; for **companies** registered
  with ASIC. The ABN of a company typically = 2 check digits + its ACN.
- **ARBN** — for registered foreign companies / registrable bodies.
- **GST registration** — the indirect-tax flag (Australia has **no separate VAT
  number**; GST registration status serves that role).
- **ANZSIC** — industry classification — **not** in the public ABR extract.

## Sources found

### 1. ABR / ABN Lookup Bulk Extract (data.gov.au) — RECOMMENDED
- Dataset `https://data.gov.au/data/dataset/abn-bulk-extract`. **CC-BY 3.0 AU**,
  **weekly**. Resources: `bulkextract.xsd` (schema), readme PDF, Part 1
  `public_split_1_10.zip` (492 MB), Part 2 `public_split_11_20.zip`, resource-list
  CSV.
- Record shape `<ABR>`: `ABN` (status ACT/CAN, ABNStatusFromDate), `EntityType`
  (EntityTypeInd + EntityTypeText), `MainEntity` → `NonIndividualName`
  (NonIndividualNameText) for orgs or `IndividualName` (GivenName/FamilyName) for
  sole traders, `BusinessAddress` (State, Postcode), `ASICNumber` (= ACN),
  `GST` (status, GSTStatusFromDate), `OtherEntity` (trading/business names, types
  TRD/BN), `DGR`. **Downloaded Part 1 + XSD; extracted a real company record.**
- Covers **all entity types**, not just companies — filter by `ASICNumber`
  present (or EntityType) for companies.

### 2. ABN Lookup web services — free API (GUID)
- `https://abr.business.gov.au/Tools/WebServices` — free SOAP/JSON per-ABN/ACN
  lookup after registering for a free **GUID** (authentication token). Returns the
  same public fields per entity. Good for incremental enrichment; not for bulk
  (use the extract).

### 3. ASIC company register (ASIC Connect) — paid / partial
- `https://asic.gov.au/` — the authoritative **company** register: ACN, registered
  office address, incorporation date, company status, type, officeholders.
  **Paid per extract/document** via ASIC Connect (purchased searches). ASIC also
  publishes some **free** datasets on data.gov.au (e.g. **Business Names register**,
  banned/disqualified persons) but the **company register** detail and documents
  are paid.

### 4. ASIC financial reports — paid
- Financial reports are lodged with ASIC and **bought per document** via ASIC
  Connect. **Only certain companies must lodge** (public companies, large
  proprietary companies, disclosing entities, registered schemes). Small
  proprietary companies generally do **not** lodge publicly.

### 5. ASX — listed-company financials — listed-only
- Listed entities lodge financial reports/announcements with the **ASX**
  (`asx.com.au`), publicly viewable per company (PDF). Covers only listed issuers.

## What was NOT bypassed

- Only the open CC-BY ABR bulk extract was downloaded. ASIC paid extracts/documents
  and the ABN Lookup GUID gate were not circumvented. Person names (sole-trader
  individuals, officeholders) are personal data — redact.

## Recommended ingestion

Bulk-load the **ABR Bulk Extract** (2 zips) keyed on **ABN** (+ ACN for
companies). Enrich per-ABN via the free **ABN Lookup API** (GUID) if needed. For
company-register detail and financials, use **ASIC** (paid) and **ASX** (listed).
No separate VAT id — use GST registration.
