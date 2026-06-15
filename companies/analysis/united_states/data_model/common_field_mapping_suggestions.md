# Common Field Mapping Suggestions — United States

> **Suggestion only.** This file proposes how the United States country-specific
> profile *could* map onto a future cross-country mapper. It does **not**
> constrain the US country profile (`country_company_profile.schema.json`),
> which intentionally models US-specific multi-source layering (no national
> register; CIK/EIN/UEI/state-id identifiers). Use this only when building a
> later global view.

## Suggested mappings

| Common field | US source / profile path | Notes |
|---|---|---|
| `company_id` | `identifiers.primary_id` | Derived: CIK > EIN > ueiSAM > `state_code:state_entity_id`. No single national id exists. |
| `registration_number` | `identifiers.state_registrations[].state_entity_id` (or `cik`) | State entity id is the registration number for private cos; CIK for public. |
| `tax_id` | `identifiers.ein` | IRS EIN (9-digit). Present mainly from IRS; redacted in SAM public extract. |
| `vat_id` | `not_available_in_open_sources` | The US has no VAT system; no VAT id concept. |
| `legal_name` | `legal_name` | Precedence: state > SEC > IRS. |
| `status` | `status.state_status` | Use state corporate standing; do NOT substitute IRS/SAM status (different meaning). |
| `legal_form` | `entity_classification.state_entity_type` (+ `irs_subsection`/`irs_organization_structure`) | State entity type (DLLC, FPC…) primary; IRS codes for nonprofits. |
| `incorporation_date` | `dates.formation_date` | State formation date authoritative; IRS ruling date is only an approximate proxy. |
| `dissolution_date` | `not_available_in_open_sources` | Not in the analyzed open feeds; inferable only indirectly from status (e.g. Delinquent/Dissolved) without an explicit date. |
| `registered_address` | `addresses[]` where `address_role` = principal/physical | State principal address primary; IRS mailing / SAM physical as fallback. |
| `activity_code` | `activity.naics_codes` ∥ `activity.sic_code` ∥ `activity.ntee_code` | Fragmented taxonomies: NAICS (SAM, planning), SIC (SEC, planning), NTEE (IRS nonprofits). No single code in the open Colorado feed. |
| `financials` | `public_company_financials[]` (SEC XBRL — **open/ready** via `sec_financials`) ∥ `nonprofit_financials` (IRS EO BMF) | Public-company financials are now a **ready** open source (SEC companyfacts API + quarterly Financial Statement Data Sets, USD, join on CIK); nonprofit financials via IRS. **Private for-profit financials remain `not_available_in_open_sources`.** |
| `officers` | `not_available_in_open_sources` | No open officer/director data. |
| `owners` | `not_available_in_open_sources` | No open beneficial-ownership data (FinCEN BOI is access-controlled, not open). The state registered agent is NOT an owner. |
| `source_provenance` | `source_provenance[]` | Already structured per source with license/access/planning_only flags. |

## Caveats for a global mapper

- **Identifier non-uniqueness:** the US has no single national company id. A
  global mapper must accept multiple parallel identifiers (CIK, EIN, ueiSAM,
  state id) and a derived `primary_id`, not assume one registration number.
- **Status semantics are not comparable across US sources** — only state
  `entitystatus` maps to a global `status`. IRS/SAM statuses are domain-specific.
- **Coverage is partial and overlapping:** the same company can appear in SEC +
  IRS + SAM + multiple states. Deduplicate on EIN where present, else
  `state_code:state_entity_id`, else CIK.
- **Planning-only fields** (SEC submissions/companyfacts, all SAM fields,
  OpenCorporates) must be flagged in the global view until verified/ingested.
