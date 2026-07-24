# Sweden UHM procurement source

The source is Upphandlingsmyndigheten's complete “Antal kontrakterade anbud
med leverantörer” CSV. A run stores a content-addressed immutable CSV in
object storage, preserves every original Swedish column in DuckDB, and uses
set-based SQL to normalize the fields needed by Corpscout.

Every normalized supplier observation is published to ClickHouse for aggregate
procurement-market analysis, including suppliers that cannot be linked to the
Swedish company registry. `company_match_status` records whether the row is an
`exact` company match, an `unmatched_company`, or ineligible/private. The original
44-column CSV remains preserved in DuckDB and the immutable source snapshot.

Only rows marked `Kontrakterad` whose normalized supplier identity is exactly
ten digits are eligible for company matching. Company-level evidence requires
an exact join to `corpscout.se_companies.company_id`; 12-digit person-keyed IDs,
missing identifiers, and unmatched suppliers cannot turn the company signal
green.

The published grain is one source award/supplier observation. It is evidence
that the company was named as a contracted supplier in the covered advertised
procurement data, not proof of all government contracts or proof that a
contract is currently active.

This source contains historical contracted-bid observations. It does not answer
whether a procurement is currently open for offers; active opportunities require
a separate notice/deadline/status/URL pipeline.
