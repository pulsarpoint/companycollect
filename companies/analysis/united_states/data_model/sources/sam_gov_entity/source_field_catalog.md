# SAM.gov Entity Management Field Catalog (PLANNING-ONLY)

> **Planning-only source.** SAM.gov requires a free account + API key (authentication). It was **not downloaded** in this investigation. Every field below is cataloged from public SAM.gov API documentation and the source-inventory notes — **no raw records, observed values, or extracted field values are included**. Only the FOIA-releasable **Public** extract may be used; do not request or store FOUO/Sensitive data.

## Source Summary

- Country: United States
- Source type: official_registry (federal — entities doing business with the US government)
- Organization: U.S. General Services Administration (GSA)
- URL: https://api.sam.gov/entity-information/v3/entities
- License: Public extract released under FOIA (Public sensitivity level only)
- Access: authenticated — free SAM.gov account + API key (System Account, 'Read Public')
- Freshness: daily / monthly extracts
- Record shape: JSON entity with nested `entityRegistration` / `coreData` / `assertions` sections
- Primary keys: `ueiSAM`
- Join keys: `ueiSAM`, `cageCode`, `EIN` (when present — usually restricted in public tier)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| entityRegistration.ueiSAM | ueiSAM | 12-char Unique Entity ID (replaced DUNS) | string | identifier | Federal primary key |
| entityRegistration.cageCode | cageCode | CAGE supplier code | string | identifier | Secondary id |
| entityRegistration.legalBusinessName | legalBusinessName | Legal name | string | legal_name | |
| entityRegistration.registrationStatus | registrationStatus | Federal registration status | string | status | ≠ state standing |
| entityRegistration.registrationDate/expirationDate | registrationDate/expirationDate | SAM registration dates | date | date | Not incorporation date |
| coreData.physicalAddress | physicalAddress | Physical address object | object | address | |
| coreData…entityStructureCode | entityStructureCode | Legal/org structure | string | legal_form | path approximate |
| assertions…naicsList[] | naicsList | NAICS industry codes | array | activity | Richest industry classification |
| coreData…EIN | EIN/TIN | Federal tax id | string | identifier | Usually restricted in public extract |

## Interpretation Notes

- **Coverage:** only entities registered to do business with the US federal government (contractors, grantees). It is **not** a general company register and overlaps partially with SEC/IRS/state data.
- **Three federal identifiers, none universal:** `ueiSAM` (here), `CIK` (SEC), `EIN` (IRS). A single company can carry all three. EIN, when exposed, is the bridge between SAM and IRS — but EIN/TIN is sensitive and **typically redacted in the Public (FOIA) extract**, so do not rely on it.
- **NAICS is the payoff field:** SAM provides standardized NAICS industry codes, the most useful cross-source industry classification among the analyzed US sources (SEC uses SIC, IRS uses NTEE, Colorado has only a legal-form code).
- **Status semantics differ:** `registrationStatus` is about federal-award eligibility, not corporate standing — keep it distinct from a state register's `entitystatus`.
- **Field paths are documented-but-unverified.** Confirm exact nesting/names against a real API response before ingestion; `source_confidence` is set to medium/low accordingly. No `sample_record.json` is provided because the source is authenticated and not license-cleared for storing raw records here.
