# License / terms notes — United States

> Public ≠ freely reusable. Verify per source before redistribution.

## SEC EDGAR
- Works of the U.S. federal government are generally **not subject to copyright (public domain)** in the US.
- Operational terms: must send a **User-Agent header identifying the app + contact email**; honor the **10 req/s/IP** rate limit. Abuse can lead to IP blocking.
- Conclusion: **Safe to ingest and store.** Respect rate limits.

## IRS EO BMF
- U.S. federal government work → effectively **public domain** in the US.
- No explicit dataset license on the page; standard government-data terms apply. Disclaimer-of-endorsement notice present.
- Conclusion: **Safe to ingest and store.**

## SAM.gov (Entity)
- "Public" extract = data releasable under **FOIA**. Other sensitivity levels (FOUO, Sensitive) are restricted — only use the **Public** extract.
- Requires accepting SAM.gov terms when creating the account/API key.
- Conclusion: **Public extract safe to ingest**; do not request or store FOUO/Sensitive data without authorization.

## Colorado Business Entities (data.colorado.gov)
- Published as open data via the Colorado Information Marketplace (Socrata). Confirm the portal's open-data terms/attribution expectations before commercial redistribution.
- Conclusion: **Likely safe (open data)** — verify Colorado open-data terms; attribution recommended.

## State Secretary of State registries (general)
- **Highly variable.** Many states restrict bulk/commercial reuse or charge fees; some prohibit using bulk data for marketing/solicitation. **Each state's bulk-data agreement must be read individually.**
- Conclusion: **Per-state legal review required** before bulk ingestion.

## OpenCorporates
- Aggregated data under **OpenCorporates' own terms**; bulk/commercial use requires a paid license. Some data carries share-alike obligations.
- Conclusion: **Do not bulk-ingest without a license.** API use within their free-tier terms only.

## Overall
- Federal sources (SEC, IRS, SAM public) are the safest for free ingestion.
- State and aggregator data require per-source license review; treat as **unknown/restricted until confirmed**.
