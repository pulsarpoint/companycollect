# Common Crawl task 1 — database migrations

Status: implemented and locally validated on 2026-09-25; not applied to production. Parent: [Common Crawl graph ingestion and ranking history](2026-09-25-commoncrawl-graph-ranking-history.md).

Delivered ClickHouse migration `000447_corpscout_commoncrawl_domain_graph_ranks` and PostgreSQL migration `000126_commoncrawl_graph_catalog`. PostgreSQL tables use the public schema with the names below. Integration tests exercise disposable PostgreSQL 17 and ClickHouse 26.5 servers. `tests/test_commoncrawl_graph_migrations.py` plus the existing ClickHouse migration suite passed (158 tests); `uv run dg check defs` and Ruff checks passed.

Final request states are `queued`, `launching`, `running`, `succeeded`, `failed`, `canceled`; the first three share the partial unique index per release. `source_manifest` is pinned before launching; the application's later request code must preserve it. A failed/canceled/succeeded request needs `finished_at`; running/succeeded requests need a unique Dagster run ID. The singleton control row starts with automatic imports disabled and no active release. Future writers explicitly update `updated_at`.

## Objective

Provide the storage contract for release discovery, complete ranking history, import requests and latest-full-graph retention. This task writes and tests additive migrations. Data ingestion, consumer cutover, production migration application and deletion of existing data are later tasks.

## Existing definitions to preserve

- ClickHouse `commoncrawl_domain_graph_nodes` and `commoncrawl_domain_graph_edges` already have release partitions and lookup projections. Keep their definitions.
- Keep `commoncrawl_domain_graph_snapshots` and `commoncrawl_domain_connections` working for existing graph readers. Reader/retention changes belong to task 4.
- Keep `commoncrawl_domain_graph_signals`, its data and current readers. Its replacement is populated in task 3 and consumers switch in tasks 7–8.
- No separate ranking-publication table or marker asset. Availability comes from complete partitions in the serving ranking table.

## Migration A — ClickHouse ranking table

Create `corpscout.commoncrawl_domain_graph_ranks` with:

| Column | Proposed type | Meaning |
| --- | --- | --- |
| `graph_release` | `LowCardinality(String)` | Exact official graph release ID |
| `root_domain` | `String` | Source domain with reversed labels restored |
| `cc_harmonic_centrality` | `Float64` | Published harmonic score |
| `cc_harmonic_rank` | `UInt64` | Published harmonic position |
| `cc_pagerank` | `Float64` | Published PageRank score |
| `cc_pagerank_rank` | `UInt64` | Published PageRank position |
| `n_hosts` | `Nullable(UInt32)` | Host count; missing historical values remain unknown |
| `source_run_id` | `String` | Import run identity for reconciliation |
| `loaded_at` | `DateTime64(3, 'UTC')` | Import timestamp, never release chronology |

Use `MergeTree`, `PARTITION BY graph_release`, `ORDER BY (root_domain, graph_release)`. One logical row per domain/release is enforced by staging validation; the engine does not enforce uniqueness. The only importer write path is complete, validated, nonempty release replacement. Scores/positions are required for a supported ranking format; unknown formats fail explicitly until a format adapter exists.

Do not copy the existing 121 million rows in the schema migration. A subsequent validated import or explicit legacy reconciliation owns data transfer. Do not add per-row file URLs/checksums or speculative projections. Preserve the existing field names consumed by ranking code where applicable.

## Migration B — application PostgreSQL metadata

Use the application's existing PostgreSQL migration system and naming/schema conventions. These are application tables, never edits to Dagster's internal storage. The following logical table names and contracts are the handoff; place them consistently with the repository's conventions.

| Table | Minimum contract |
| --- | --- |
| `commoncrawl_graph_releases` | Release ID primary key, source index URL, constituent crawl IDs, nullable coverage start/end and source publication time, discovered/last-checked timestamps, graph retention state and retirement time |
| `commoncrawl_graph_release_files` | Release FK, artifact kind (`nodes`, `edges`, `ranks`), graph level (`domain` initially), current source URL/ETag/size, expected row count when available, schema version, availability and cache metadata; unique release/level/artifact key |
| `commoncrawl_graph_import_requests` | UUID request ID, release FK, selection (`full`/`ranks`), origin, requester, retry-parent identity, status, nullable Dagster run ID, creation/update timestamps and concise error; immutable source manifest pinned for the attempt |
| `commoncrawl_graph_state` | Singleton row holding nullable active graph release FK, automatic-import policy, initial discovery baseline, discovery-attempt/success timestamps and latest discovery error |

The import request's pinned source manifest records URLs, validators, checksums/schema information and cache identities used by that attempt. Keep this small structured metadata on the request; catalog refresh may update discovered source metadata but must not rewrite a running attempt's source identity. Do not create a per-domain processing ledger.

Use constraints for allowed artifact kinds, graph levels, selections and statuses; nonnegative sizes/counts; and coverage end not preceding coverage start when both are known. Unknown upstream metadata remains NULL. All timestamps carry time zones. Historical release names, including cross-year names, must fit without a restrictive single-year regex.

Enforce at most one active import request per release, across both full/ranks selections, with a partial unique index on nonterminal request states. Define the exact active-state list once in the migration contract and keep the request sensor consistent with it. Retrying a failed request creates a new request ID. Use a nullable unique Dagster run ID to support safe run association and recovery.

The singleton active-graph reference is the future transactional switch point; this schema does not itself prove ClickHouse readiness. Task 4 verifies graph/rank availability before changing it. Default active release and discovery baseline to NULL and automatic imports to disabled. Do not infer current state or enable imports in a migration.

Graph lifecycle values must distinguish not imported, retained and intentionally retired. Ranking readiness is derived from ClickHouse serving partitions; avoid a second PostgreSQL ranking-completion flag that could disagree with the data. Request and cache state describe execution progress, not authoritative data availability.

## Validation and completion criteria

1. Read repository migration conventions and the Dagster project authoring guidance. Allocate new migration numbers from the current checkout; do not modify previously applied migrations.
2. Add migrations to existing contract checks, including `EXPECTED_MIGRATIONS` for ClickHouse, and document exact table/column names for subsequent tasks.
3. Apply migrations to isolated PostgreSQL/ClickHouse instances, including an upgrade fixture with existing graph and legacy ranking tables. Verify their data and definitions remain intact.
4. Verify the rank table has the intended partition/sort keys and supports replacement from a matching staging table. Verify the PostgreSQL uniqueness/FK/check constraints, including concurrent attempts for one release, a retry after terminal failure and valid cross-year IDs.
5. Provide repository-conventional rollback definitions and test them only against disposable data. Rollback must leave pre-existing tables intact; dropping the new tables would remove their new data and is not an automatic production recovery strategy.
6. Run the relevant migration tests. No production connection or full Common Crawl download is needed for this task.

Deliverable: reviewed migration files, contract/integration tests and the finalized schema notes. No new importer, schedule, sensor or UI is part of task 1.
