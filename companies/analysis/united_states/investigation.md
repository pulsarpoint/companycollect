# Investigation: Company data sources for the United States

## Summary

The United States is **structurally different** from most countries: there is **no centralized national company register**. Business entity formation is handled by each **state's Secretary of State** (or equivalent), producing 50+ independent registers with inconsistent access, formats, schemas, and pricing. Federal datasets exist but each covers only a **slice** of all companies.

To assemble broad company coverage you must combine:
- **Federal datasets** (public companies, nonprofits, federal contractors), and
- **State registries** (the only authoritative source for the mass of private companies).

## What was found

### Federal sources (mostly fully open)

1. **SEC EDGAR** — U.S. Securities and Exchange Commission.
   - Covers public / SEC-reporting companies (~10,405 with tickers; more filers without).
   - Open bulk files: `submissions.zip`, `companyfacts.zip`, and `company_tickers.json` (downloaded here).
   - REST APIs under `https://data.sec.gov/`.
   - Constraints: **10 req/s/IP**, and a **descriptive User-Agent header with contact email is mandatory** (no header → HTTP 403). License: US Government work (public domain).

2. **IRS Exempt Organizations Business Master File (EO BMF)** — Internal Revenue Service.
   - National, EIN-keyed dataset of all tax-exempt nonprofits, split into 4 regional CSVs (`eo1.csv`–`eo4.csv`).
   - Updated monthly (2nd Tuesday). Fully open. Data dictionary: Publication 5926.

3. **SAM.gov Entity Management** — General Services Administration.
   - Entities registered to do business with the federal government (contractors/grantees), keyed by UEI/CAGE.
   - Public extract API + Entity Management API; returns JSON/CSV. **Requires a free SAM.gov account + API key** (System Account with "Read Public"). Public data is FOIA-releasable.

### State sources (authoritative for private companies; access varies)

4. **State Secretary of State registries (50 + DC)** — the only authoritative source for the bulk of US private companies.
   - Free online search everywhere; **bulk downloads are frequently paid** (e.g. Arizona $2,000+/yr, South Carolina $12,000/yr for UCC, North Carolina $750 setup + $250/yr).
   - A minority offer free/open bulk or APIs: **Colorado** (open data portal), and reportedly **Oregon, Connecticut, Iowa, Minnesota**.

5. **Colorado Business Entities** (exemplar free state registry) — Colorado Information Marketplace (Socrata).
   - 1M+ entities back to the 1800s; fields include entity id, name, addresses, status, type, registered agent, formation date.
   - Socrata SODA API (JSON/CSV/XML) with `$limit`/`$offset`/`$where` pagination. A 5-row sample was downloaded.

### Discovery / aggregation

6. **Data.gov** (`catalog.data.gov`, tag `business-entity`) — CKAN catalog to discover further federal/state datasets.
7. **OpenCorporates** — best single cross-state aggregator (all 50 states normalized), but **bulk/commercial use requires a license/payment**. Useful as comparison/fallback only.

## What was NOT found / limitations

- No free, single, authoritative national private-company dataset.
- No free bulk feed for many large states (CA bulk is limited; many states paywall bulk).
- SAM.gov bulk requires authentication (free key, but a step).

## Recommendation

Use a **hybrid, layered** approach:
- **Layer 1 (free, federal, immediate):** SEC EDGAR + IRS EO BMF.
- **Layer 2 (free, state):** Colorado and other open-data states via Socrata/APIs/CSV.
- **Layer 3 (free w/ key):** SAM.gov for federal contractors.
- **Layer 4 (paid / per-state):** remaining state registries — evaluate cost vs. an aggregator (OpenCorporates) for cross-state coverage.

Deduplicate across layers (a single company may appear in SEC + its state register + SAM.gov). Use EIN where available (IRS/SAM) and state entity id + state code as the primary key for state data; CIK for SEC.
