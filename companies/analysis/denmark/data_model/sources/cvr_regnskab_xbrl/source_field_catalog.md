# Regnskaber — XBRL / iXBRL Documents (DCCA taxonomy) Field Catalog

## Source Summary

- Country: Denmark
- Source type: official_financial_documents (XBRL / Inline XBRL instances)
- Organization: Erhvervsstyrelsen (Danish Business Authority)
- URL: `http://regnskaber.virk.dk/{id}/{token}.xml` (discovered via `cvr_offentliggoerelser.dokumenter[].dokumentUrl`)
- License: Free / open; same CVR reuse terms
- Access: **public, no authentication** (documents openly downloadable)
- Freshness: per filing (annual / interim)
- Record shape: XBRL instance — facts + `xbrli:context` (entity, period, dimensions); taxonomies `fsa`/`gsd`/`cmn`/`sob` (DCCA) plus `ifrs-full`/ESEF for listed groups
- Primary keys: reporting CVR + context period
- Join keys: `gsd:IdentificationNumberCvrOfReportingEntity`, `gsd:LegalEntityIdentifierOfReportingEntity`

> **Basis: a real downloaded, gzip-decompressed XBRL instance** —
> `raw/samples/maersk_delaarsrapport.xbrl.xml` (A.P. Møller - Mærsk A/S, CVR 22756214,
> Q1-2026 interim). That sample is an **identity + management-review** instance: it carries
> company identity, address, board/executive members, period, reporting class and audit
> status, but **not** the income-statement / balance-sheet line items. The monetary `fsa:`
> facts (revenue, profit, assets, equity, …) are part of the same DCCA taxonomy and appear in
> full annual reports — they are documented here with **medium** confidence as not observed
> in this particular sample.

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| `//gsd:IdentificationNumberCvrOfReportingEntity` | CVR (reporting) | Company the report is about | integer | identifier | 22756214 | Join key |
| `//gsd:LegalEntityIdentifierOfReportingEntity` | LEI | ISO 17442 LEI | string | identifier | 549300D2K6PKKKXVNN73 | Listed/IFRS only |
| `//gsd:NameOfReportingEntity` | name | Report-stated legal name | string | legal_name | A.P. Møller - Mærsk A/S | Prefer CVR name |
| `//gsd:AddressOfReportingEntityStreetName` | street | Street name | string | address | Esplanaden | + building id |
| `//gsd:AddressOfReportingEntityStreetBuildingIdentifier` | building | House/building no. | string | address | 50 | |
| `//gsd:AddressOfReportingEntityPostCodeIdentifier` | postcode | Postal code | string | address | 1263 | |
| `//gsd:AddressOfReportingEntityDistrictName` | district | Town/district | string | address | Copenhagen K | |
| `//gsd:AddressOfReportingEntityCountry` | country | Country | string | geography | Denmark | |
| `//gsd:ReportingPeriodStartDate` | period start | Figures period start | date | date | 2026-01-01 | =context period |
| `//gsd:ReportingPeriodEndDate` | period end | Figures period end | date | date | 2026-03-31 | + Preceding* comparatives |
| `//fsa:ClassOfReportingEntity` | reporting class | Danish regnskabsklasse A–D | string | filing | Reporting class D | governs disclosures |
| `//gsd:InformationOnTypeOfSubmittedReport` | report type | Annual vs interim | string | filing | Interim report (other than 6 months) | |
| `//cmn:TypeOfAuditorAssistance` | auditor assistance | Audit/review/none | string | filing | No audit assistance | audit signal |
| `//cmn:NameAndSurnameOfMemberOfExecutiveBoard` | exec name | Executive board (direktion) member | string | person | Vincent Clerc | + title; GDPR |
| `//cmn:TitleOfMemberOfExecutiveBoard` | exec title | Title (CEO/CFO) | string | person | CEO, CFO | match by context |
| `//cmn:NameAndSurnameOfMemberOfSupervisoryBoard` | board name | Supervisory board (bestyrelse) member | string | person | Robert Mærsk Uggla | + Chair/Vice Chair; GDPR |
| `//fsa:*` | line items | Revenue/profit/assets/equity/… | decimal | financial | — (not in this sample) | resolve context + unit |
| `//xbrli:context/.../cmn:ConsolidatedSoloDimension` | consolidated/solo | Group vs parent-only | string | metadata | cmn:ConsolidatedMember | honour before storing facts |

## Interpretation Notes

- **Taxonomies.** Danish DCCA: `gsd:` general/identity, `fsa:` financial-statement facts,
  `cmn:` common (boards, dimensions), `sob:` statements/declarations. Listed groups add
  `ifrs-full:` and ESEF. The schema is referenced via `link:schemaRef` (e.g.
  `entryDanishGAAP…20241001.xsd`), which identifies the taxonomy version.
- **Contexts are mandatory.** Every fact has a `contextRef` pointing to an `xbrli:context`
  that fixes the **entity** (scheme `iso/17442` LEI here, or a CVR scheme for non-LEI filers),
  the **period** (instant or start/end), and any **dimensions**. Two dimensions seen:
  `cmn:ConsolidatedSoloDimension` (ConsolidatedMember vs SoloMember) and typed member
  dimensions for board/executive identification. **Always resolve the context** before using
  a value — never store a `fsa:` figure without its period and consolidated/solo flag.
- **Currency** comes from the `xbrli:unit` referenced by each monetary fact's `unitRef`
  (`iso4217:DKK` etc.). The sample has no monetary units (identity-only instance).
- **Inline vs plain XBRL.** Filings provide both `application/xhtml+xml` (Inline XBRL, human +
  machine) and `application/xml` (plain XBRL). For extraction, the plain XBRL (`.xml`) is
  simplest; iXBRL requires un-embedding `ix:` facts from the XHTML.
- **Gzip on download.** `regnskaber.virk.dk` serves documents gzip-compressed even when the
  `Content-Type` is `text/xml`; the saved sample was decompressed.
- **People data.** Board/executive names are personal data — apply GDPR handling.
- **Source typo.** Prior-period end is tagged `gsd:PredingReportingPeriodEndDate`
  (misspelled in the taxonomy); match it literally.
