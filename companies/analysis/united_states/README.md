# Company data sources for United States

## Status

- Official bulk data: **found (partial)** — strong at federal level (SEC, IRS) and some states; most private-company state registries charge for bulk.
- Official API: **found** — SEC EDGAR (open), SAM.gov (key required), state open-data APIs (e.g. Colorado Socrata).
- Open data portal: **found** — data.gov + state portals (e.g. data.colorado.gov).
- License: **mostly open** for federal (US Government works / public domain); **varies/paid** for state registries.
- Recommended ingestion path: **hybrid** — bulk for federal + free states, API for incremental, per-state evaluation for the rest.

## Key fact

The US has **no single national company register**. Company formation happens at the **state level (50 states + DC)**, so comprehensive private-company coverage requires aggregating ~51 jurisdictions, many of which charge for bulk data. Federal sources cover specific slices: public companies (SEC), federal contractors (SAM.gov), and nonprofits (IRS).

## Best sources

1. **SEC EDGAR** — public/SEC-reporting companies. Fully open, near real-time, no key (just a User-Agent header). Best federal source for public companies. **Financial data is concretely covered** via the SEC **companyfacts** XBRL API (per CIK) + the quarterly **Financial Statement Data Sets** (all filers) — verified live (e.g. Apple FY2025 revenue $416.16B). Private for-profit financials remain unavailable openly.
2. **IRS EO BMF** — all tax-exempt nonprofits, EIN-keyed, national CSV, fully open, monthly.
3. **SAM.gov** — federal contractors/grantees. Public extracts (FOIA), requires a free API key.
4. **Colorado Business Entities** and **New York Active Corporations** — two concrete, free, open state registries (Socrata API/CSV), both verified live. Template for other free states (Washington, Oregon, Connecticut, Iowa, Minnesota).

## Next action

- For full private-company coverage, evaluate each state's bulk/API offering and cost (start with the free states; budget for paid ones or use an aggregator).
- For an immediate, fully-open foundation: ingest SEC EDGAR (identity) + **SEC XBRL financials** (companyfacts / Financial Statement Data Sets) + IRS EO BMF + free-state open data (Colorado, New York, …).
- Obtain a SAM.gov System Account API key to add federal-contractor entities.
- Expand state coverage by adding more free Socrata/CSV states (Washington, Oregon, Connecticut, Iowa, Minnesota) on the New York/Colorado pattern.
