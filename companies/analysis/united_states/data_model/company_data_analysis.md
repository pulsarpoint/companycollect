# Company Data Analysis For United States

## Summary

The United States is **structurally different** from most countries: there is
**no centralized national company register**. Company formation is a per-state
function (50 states + DC), and federal datasets each cover only a *slice* of
companies. A practical US company profile is therefore not a single registration
record but a **multi-source layered model**, keyed by whichever identifiers
exist for the entity:

- **CIK** — public / SEC-reporting companies (SEC EDGAR)
- **EIN** — tax-exempt nonprofits, national & EIN-keyed (IRS EO BMF)
- **ueiSAM / CAGE** — federal contractors & grantees (SAM.gov, auth-only)
- **state entity id** — the authoritative register for the mass of private
  companies (e.g. Colorado open data; most states paywall bulk)

From the **fully-open** sources (SEC, IRS, Colorado) we can build a solid
profile for public companies, nonprofits, and Colorado-registered entities:
name, identifiers, legal form, status, address, registered agent, formation
date, and — for nonprofits — financials. Broad **private-company** coverage and
**richer federal** data (addresses/SIC/financials from SEC, NAICS from SAM)
require either hitting per-CIK SEC APIs, an authenticated SAM key, or
aggregating many paid state registers.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| sec_edgar | SEC EDGAR | recommended | public (UA header req'd) | US gov / public domain | Federal — public companies |
| irs_eo_bmf | IRS Exempt Orgs Business Master File | recommended | public | US gov / public domain | Federal — nonprofits (national, EIN-keyed) |
| colorado_business_entities | Colorado Business Entities | recommended | public (Socrata) | open data (verify terms) | State exemplar — private companies |
| sam_gov_entity | SAM.gov Entity Management | recommended | authenticated (free key) | FOIA public extract | Federal contractors — **planning-only** |
| state_sos_registries | 50 states + DC SoS registries | useful_secondary | mixed (bulk often paid) | varies per state | Authoritative for private cos — **generic/planning** |
| opencorporates | OpenCorporates | blocked_by_license_uncertainty | partial / paid bulk | restricted/share-alike | Cross-state aggregator — **planning-only** |
| data.gov catalog | Data.gov business-entity catalog | useful_secondary | public | varies | **Discovery portal only** (no company fields; not cataloged) |

## What Each Source Contributes

- **SEC EDGAR** — `company_tickers.json` (downloaded) gives CIK + ticker + name
  for ~10,405 public companies. Richer fields (addresses, SIC, state of
  incorporation, EIN, former names, filings, XBRL financials) are one hop away
  via the `data.sec.gov` submissions/companyfacts APIs but were **not
  downloaded** — cataloged as planning-only. Requires a descriptive User-Agent
  header with contact email (else HTTP 403); 10 req/s/IP.
- **IRS EO BMF** — the richest *open* single dataset analyzed: 28 fields per
  nonprofit, national and EIN-keyed, including coded classification (SUBSECTION,
  NTEE, FOUNDATION, etc. via Pub 5926), addresses, ruling date, and financial
  amounts (assets/income/revenue). Nonprofits only.
- **Colorado Business Entities** — exemplar open state register: entity id,
  name, principal & mailing addresses, status, legal-form code, jurisdiction of
  formation, registered agent (person or commercial org), and a true
  **formation date** — the most reliable incorporation date across all sources.
- **SAM.gov** (planning-only) — would add ueiSAM/CAGE identifiers, legal name,
  physical address, federal registration status, and **NAICS** industry codes
  for federal contractors. Authenticated; public extract is FOIA-releasable;
  EIN typically redacted.
- **State SoS registries** (generic/planning) — the only authoritative source
  for most private companies, but 51 incompatible schemas with mostly paid bulk.
- **OpenCorporates** (planning-only) — best normalized cross-state view, but
  bulk/commercial use needs a paid license; comparison/fallback only.
- **Data.gov** — a CKAN discovery catalog to *find* more datasets; it holds
  dataset metadata, not company records, so no field catalog was produced.

## Proposed Country Company Profile

`country_company_profile.schema.json` models the US-specific reality:

- An **`identifiers`** object holding all parallel ids (cik, ticker, ein,
  uei_sam, cage_code, an array of `state_registrations`) plus a derived
  `primary_id`. At least one identifier is required; none is universal.
- Classification flags (`is_public_company`, `is_nonprofit`,
  `is_federal_registrant`) plus state legal-form and IRS structural codes.
- **Separate status fields** for state standing, IRS exemption, and SAM
  registration — because they mean different things.
- Arrays for `addresses` (roles: principal/mailing/physical/irs_mailing),
  `alternate_names`, `public_company_financials`, and `source_provenance`.
- A `registered_agent` object (person or organization) — explicitly *not* an
  owner.
- A nonprofit-only `nonprofit_financials` object and a planning-only
  public-company XBRL financials array.

`country_company_profile.example.json` is a conformant record built from a
**real observed Colorado entity** (state layer fully populated; federal layers
honestly null because that entity is neither public nor a nonprofit).

## Join And Precedence Rules

- **Join keys:** EIN is the strongest cross-source link (IRS↔SAM) when present;
  CIK joins SEC data; `state_code + ':' + state_entity_id` keys state records.
- **`primary_id` precedence:** CIK → EIN → ueiSAM → `state_code:entity_id`.
- **Legal name:** state register > SEC title > IRS NAME.
- **Corporate standing:** use state `entitystatus`; never overwrite it with
  IRS/SAM status (different domains).
- **Incorporation date:** state `entityformdate` authoritative; IRS `RULING` is
  only a YYYYMM recognition-date proxy.
- **Official over aggregator:** prefer state/federal over OpenCorporates.
- **Freshness:** SEC near real-time, SAM daily/monthly, IRS monthly (2nd Tue),
  Colorado regularly updated.
- **Deduplication:** one company can appear across SEC + IRS + SAM + several
  states; dedupe on EIN, else state key, else CIK.

## Missing Or Restricted Data

**Unavailable from open/public sources:**
- Beneficial owners, officers, and directors (only the state registered agent is
  open; FinCEN BOI is access-controlled, not open).
- Private for-profit company financials.
- Explicit dissolution dates (only status flags like Delinquent/Dissolved).
- A single national company identifier (none exists).
- VAT id (no US VAT system).

**Available only from restricted / authenticated / paid sources:**
- NAICS industry codes, ueiSAM/CAGE, physical address — **SAM.gov** (free key,
  authenticated; planning-only here).
- SEC addresses, SIC, state of incorporation, former names, XBRL financials —
  **SEC submissions/companyfacts APIs** (open but not downloaded; planning-only).
- Comprehensive private-company coverage — **state bulk feeds** (mostly paid) or
  **OpenCorporates** (paid/license-uncertain).

## Common Mapper Notes

A future cross-country mapper should treat the US as a *multi-identifier,
partial-coverage* country: accept parallel ids rather than one registration
number, map only state `entitystatus` to a global status, flag planning-only
fields, and mark `owners`/`officers`/`vat_id`/`dissolution_date`/private
`financials` as `not_available_in_open_sources`. See
`common_field_mapping_suggestions.md` for the field-by-field proposal.
