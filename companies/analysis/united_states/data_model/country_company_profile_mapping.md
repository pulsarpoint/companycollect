# United States Company Profile — Mapping Report

This maps each combined-profile field to its source field(s), with join keys,
freshness, license/access, precedence, and missing-data notes. The US has **no
national company register**, so the profile is a multi-source layered model
keyed by whichever identifiers exist.

## Identifier / join keys

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| identifiers.cik | sec_edgar | `<index>.cik_str` | yes | federal id for public companies | Zero-pad to 10 digits |
| identifiers.ticker | sec_edgar | `<index>.ticker` | secondary | — | Not unique across share classes |
| identifiers.ein | irs_eo_bmf | `EIN` | yes (strongest cross-source) | nonprofit/federal tax id | 9-digit string; links IRS↔SAM |
| identifiers.uei_sam | sam_gov_entity | `entityRegistration.ueiSAM` | yes | federal contractors | PLANNING-ONLY (auth) |
| identifiers.cage_code | sam_gov_entity | `entityRegistration.cageCode` | secondary | — | PLANNING-ONLY (auth) |
| identifiers.state_registrations[].state_entity_id | colorado_business_entities | `entityid` | yes (with state_code) | authoritative for private cos | Unique only within state |
| identifiers.primary_id | derived | — | yes | CIK > EIN > UEI > state_code:entity_id | Canonical id selection |

## Core company fields

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| legal_name | colorado_business_entities / sec_edgar / irs_eo_bmf | `entityname` / `title` / `NAME` | — | state register > SEC title > IRS NAME | Strip appended delinquency notes from CO names |
| alternate_names[] | sec_edgar / irs_eo_bmf | submissions formerNames / `SORT_NAME` | — | — | Former names planning-only (SEC submissions) |
| entity_classification.is_public_company | sec_edgar | presence | — | — | True if in EDGAR |
| entity_classification.is_nonprofit | irs_eo_bmf | presence | — | — | True if in EO BMF |
| entity_classification.is_federal_registrant | sam_gov_entity | presence | — | — | PLANNING-ONLY |
| entity_classification.state_entity_type | colorado_business_entities | `entitytype` | — | state authoritative | e.g. DLLC, FPC |
| entity_classification.irs_subsection | irs_eo_bmf | `SUBSECTION` | — | — | Pub 5926 code (03=501(c)(3)) |
| entity_classification.irs_organization_structure | irs_eo_bmf | `ORGANIZATION` | — | — | 1=Corp,2=Trust,5=Assoc |
| entity_classification.irs_foundation_code | irs_eo_bmf | `FOUNDATION` | — | — | Pub 5926 code |

## Status

| Profile path | Source | Source path | Precedence | Notes |
|---|---|---|---|---|
| status.state_status | colorado_business_entities | `entitystatus` | primary for corporate standing | Good Standing / Delinquent / … |
| status.irs_exempt_status | irs_eo_bmf | `STATUS` | nonprofit exemption only | 01 = active; revocations in separate list |
| status.sam_registration_status | sam_gov_entity | `registrationStatus` | federal eligibility only | PLANNING-ONLY; ≠ corporate standing |

## Activity / industry

| Profile path | Source | Source path | Code list | Notes |
|---|---|---|---|---|
| activity.sic_code | sec_edgar | submissions `sic` | SIC | PLANNING-ONLY (submissions API) |
| activity.naics_codes[] | sam_gov_entity | `naicsList` | NAICS | PLANNING-ONLY (auth); richest industry taxonomy |
| activity.ntee_code | irs_eo_bmf | `NTEE_CD` | NTEE | Nonprofits only; preferred over legacy ACTIVITY |

## Addresses & agent

| Profile path | Source | Source path | Notes |
|---|---|---|---|
| addresses[] (principal) | colorado_business_entities | `principaladdress1`/`principalcity`/`principalstate`/`principalzipcode`/`principalcountry` | Principal office |
| addresses[] (mailing) | colorado_business_entities | `mailingaddress1`/… | Sparse |
| addresses[] (irs_mailing) | irs_eo_bmf | `STREET`/`CITY`/`STATE`/`ZIP` | Often PO box |
| addresses[] (physical) | sam_gov_entity | `coreData.physicalAddress` | PLANNING-ONLY |
| registered_agent | colorado_business_entities | `agentfirstname`/`agentlastname`/`agentorganizationname` + `agentprincipal*` | Service of process; not an owner |

## Dates & jurisdiction

| Profile path | Source | Source path | Precedence | Notes |
|---|---|---|---|---|
| dates.formation_date | colorado_business_entities | `entityformdate` | best incorporation date | ISO8601 .000 millis; date-only meaning |
| dates.irs_ruling_date | irs_eo_bmf | `RULING` | approximate proxy only | YYYYMM recognition date, NOT formation |
| jurisdiction.jurisdiction_of_formation | colorado_business_entities | `jurisdictonofformation` | — | Misspelled key; CO=domestic, else foreign |
| jurisdiction.state_of_incorporation | sec_edgar | submissions `stateOfIncorporation` | — | PLANNING-ONLY |

## Financials

| Profile path | Source | Source path | Notes |
|---|---|---|---|
| nonprofit_financials.asset_amount/income_amount/revenue_amount | irs_eo_bmf | `ASSET_AMT`/`INCOME_AMT`/`REVENUE_AMT` | Nonprofits only; whole USD; sparse; period = `TAX_PERIOD` |
| nonprofit_financials.filing_requirement | irs_eo_bmf | `FILING_REQ_CD` | Pub 5926 code |
| public_company_financials[] | sec_financials | companyfacts `facts.us-gaap.<Concept>.units.USD[]` / Financial Statement Data Sets | **OPEN (ready)**; SEC filers only; join on CIK; USD; pick by form=10-K + latest end |
| identifiers.state_registrations[].state_entity_id (NY) | new_york_active_corporations | `dos_id` | **OPEN (ready)**; NY-scoped; second concrete free state (data.ny.gov) |

## Source provenance

| Profile path | Source | Notes |
|---|---|---|
| source_provenance[] | all | One entry per contributing source with retrieved_at, license, access, planning_only, join_key_used |

## Precedence rules (summary)

1. **Identifier precedence for `primary_id`:** CIK (public) → EIN (nonprofit/tax) → ueiSAM (federal contractor) → `state_code:state_entity_id` (private).
2. **Legal name:** state register (authoritative for the legal entity) > SEC `title` > IRS `NAME`. Clean CO names of appended status text.
3. **Corporate standing:** use state `entitystatus`. IRS `STATUS` and SAM `registrationStatus` describe *different* things (tax exemption, federal eligibility) — never overwrite corporate standing with them.
4. **Incorporation date:** state `entityformdate` is authoritative. IRS `RULING` is only a recognition-date proxy; SEC/SAM dates are registration/recognition, not formation.
5. **Official over aggregator:** prefer authoritative state/federal sources over OpenCorporates; use OC (restricted, planning-only) only as fallback/dedup.
6. **Freshness:** SEC near real-time; IRS monthly (2nd Tue); SAM daily/monthly; Colorado regularly updated. Prefer the freshest source on conflict, subject to the precedence above.

## Missing-data notes

- **No open beneficial-ownership / officers / directors** in any analyzed open source. Only the state **registered agent** (service of process) is available — not an owner. (FinCEN BOI data exists but is access-controlled, not open.)
- **No EIN in SEC ticker file** and EIN usually **redacted in the SAM public extract** — so the strongest cross-source key (EIN) is reliably present only from IRS.
- **For-profit private-company financials are not in open sources** (only nonprofit financials from IRS and public-company XBRL from SEC).
- **Industry codes are fragmented:** SIC (SEC), NAICS (SAM), NTEE (IRS), none in the Colorado open feed.
- **National private-company coverage requires aggregating 51 state registers**, most of which paywall bulk data.
