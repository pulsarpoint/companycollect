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

1. **SEC EDGAR** — public/SEC-reporting companies. Fully open, near real-time, no key (just a User-Agent header). Best federal source for public companies.
2. **IRS EO BMF** — all tax-exempt nonprofits, EIN-keyed, national CSV, fully open, monthly.
3. **SAM.gov** — federal contractors/grantees. Public extracts (FOIA), requires a free API key.
4. **Colorado Business Entities** — exemplar of a free, open state registry (1M+ entities via Socrata API/CSV). Template for other free states (Oregon, Connecticut, Iowa, Minnesota).

## Next action

- For full private-company coverage, evaluate each state's bulk/API offering and cost (start with the free states; budget for paid ones or use an aggregator).
- For an immediate, fully-open foundation: ingest SEC EDGAR + IRS EO BMF + free-state open data (Colorado etc.).
- Obtain a SAM.gov System Account API key to add federal-contractor entities.
