# France DECP procurement design

## 1. Source overview

- **Country/source:** France — Données essentielles de la commande publique.
- **Module:** `defs/france_decp_procurement/`
- **DuckDB:** `data/france_decp_procurement_source.duckdb`
- **Pool:** `france_decp_procurement_duckdb`
- **ClickHouse:** `corpscout.fr_decp_contract_holders`, migration `000207`.
- **Dataset:** official cumulative semicolon-delimited CSV,
  `decp-2022-marches-valides`, Licence Ouverte 2.0, no authentication.
- **Logical grain:** one holder position for one buyer and contract:
  `(acheteur_id, id, holder_ordinal, holder_id_raw)`.

## 2. Ingest mode

Non-partitioned full refresh. The official portal publishes one cumulative CSV,
so a monthly snapshot is downloaded in full and stored with an S3 manifest.
Partitioning would add bookkeeping without avoiding the full source download.

## 3. Loading

The streamed snapshot is validated before publication to S3. DuckDB's native
`read_csv` reader stages the wide CSV with source columns preserved as text.
Python does not loop over the production rows.

## 4. Transform

Set-based DuckDB SQL expands the three holder slots. The official cumulative
file can publish the same logical contract-holder repeatedly as corrections or
later contract, modification, and subcontract publications arrive.

Candidates are ranked within `source_record_id` by:

1. the freshest publication date across the base, modification, subcontract,
   and subcontract-modification fields;
2. the freshest equivalent notification date;
3. descending source line number as a deterministic tie-breaker.

Only rank one is published. Materialization metadata reports
`source_version_rows`, `candidate_rows`, and `collapsed_version_rows`, making
the source's repeated-version volume visible.

## 5. ClickHouse schema

`fr_decp_contract_holders` uses
`ReplacingMergeTree(resolved_at) ORDER BY source_record_id`, matching the
logical holder grain. The exporter checks that the candidate stage contains
exactly one row per `source_record_id` before inserting, then atomically swaps
the complete target table.

No migration change is required for version deduplication because the existing
sort key already represents the intended grain.

## 6. Translation

Company names are identifiers/proper nouns and are not translated. Contract
titles and procedure labels currently remain in their official French form in
the government-contract view; adding the shared translation-cache loader is a
separate source enhancement.

## 6b. Contacts

The procurement dataset contains buyer and holder identifiers, not company
website, email, phone, fax, or social contact fields. Company contacts remain
owned by company-register sources.

## 6c. Industry and Wikidata

The source carries CPV procurement classifications, not company activity
classifications. France Sirene remains authoritative for NAF/NACE and the
Wikidata registry-number seed.

## 7. Currency

The contract amount is retained in native EUR and converted to USD in the
separate `france_decp_contract_holders_usd` asset. Modification and subcontract
amounts remain source-native EUR in the existing ClickHouse contract.

## 8. Scheduling

`france_decp_procurement_job` runs the complete snapshot-to-ClickHouse chain.
The monthly schedule is `20 5 10 * *`, Europe/Paris, and remains stopped by
default until manually validated.

## 9. Issues found

- The cumulative CSV contains repeated versions of a logical contract-holder.
  Counting physical candidates against a `ReplacingMergeTree` target produced
  `746,919` candidates but `726,881` published rows because ClickHouse merged
  duplicate sort keys asynchronously.
- Relying on ClickHouse merging made row-count validation timing-dependent.
  Deduplication now happens deterministically in DuckDB, and ClickHouse verifies
  the candidate key uniqueness before publishing.

## 10. Verification

- `tests/test_france_decp_procurement.py`
- `tests/test_national_procurement_assets.py`
- `uv run dg check defs`
- Live validation: materialize the complete France DECP job, confirm
  `collapsed_version_rows` is nonzero, and verify ClickHouse row count equals
  `candidate_rows`.
