# Open PageRank domains design doc

## 1. Source overview
- **Country / registry**: Global domain rank list from DomCop/Open PageRank.
- **Module**: `defs/open_page_rank/` · DuckDB `data/open_page_rank_source.duckdb` · pool `open_page_rank_duckdb`
- **ClickHouse tables**: `corpscout.open_page_rank_domains` (`000040_corpscout_open_page_rank_domains`)
- **Datasets used**:
  | dataset | url | format | size | cadence | auth? |
  |---|---|---|---|---|---|
  | DomCop/Open PageRank top 10M domains | `https://www.domcop.com/files/top/top10milliondomains.csv.zip` | ZIP CSV | about 1GB zipped | source-managed; scheduled weekly in Corpscout | no |
- **Entity key**: `domain` with `source_rank`; expected record count about 10,000,000.

## 2. Ingest mode (§2 of guidelines) — and why
- Chosen: bulk file full-refresh.
- Why: the source publishes one full ZIP CSV, so partitions and API pagination add state without benefit.
- Format choice: CSV inside ZIP. The source page exposes columns `Rank`, `Domain`, `Open Page Rank`, and `Extension`.
- If partitioned: not partitioned; each run replaces the current list.

## 3. Loading (§3)
- Reader: dlt filesystem resource piped to `read_csv_duckdb(use_pyarrow=True, header=True, all_varchar=True)`.
- Why: avoids Python row loops and lets DuckDB/Arrow handle the large CSV.
- Staging shape: raw dlt table `open_page_rank_raw.open_page_rank_raw_domains`; values remain text until transform.
- Checkpoints / per-file split: one raw ZIP in S3 per run, one extracted CSV in a temporary directory.

## 4. Transform (§5)
- Mechanism: set-based DuckDB SQL.
- Shape: cast rank to unsigned integer, lower-case `domain` and `extension`, cast Open PageRank to `Float64`, attach run/source metadata.

## 5. ClickHouse schema — and DDL deviations
- Table + grain: `open_page_rank_domains`, one row per `(source_system, source_list_name, source_rank, domain)` in the current source snapshot.
- `ORDER BY`: `(root_domain, source_system, source_list_name, domain)` · engine: `ReplacingMergeTree(resolved_at)`.
- Deviation: no company key, contacts, country, industry, translation, or currency columns because this is a domain rank list, not a company registry.
- Export subset: all columns in `OPEN_PAGE_RANK_DOMAINS_COLUMNS`; no raw payload columns exported.

## 6. Translation (§8)
No translatable fields. Domain names and source labels are not translated.

## 6b. Contacts (§8b) — MANDATORY to assess
No company contact data is present. This source is a ranked domain universe only.

## 7. Currency (§7)
No monetary amounts.

## 8. Scheduling (§9)
- Job: `open_page_rank_domains_refresh_job`.
- Schedule: `open_page_rank_domains_weekly`, Sunday 02:45 UTC, default stopped until first live validation.

## 9. Issues found during processing
- The source ZIP is large, so raw download and S3 read/write must use file streaming methods, not `read_bytes` or `write_bytes`.
- Dagster config annotations in `open_page_rank/assets.py` must not be postponed with `from __future__ import annotations`; this Dagster/Python combination fails to resolve the imported config class when it is stored as a string annotation.

## 10. Verification
- Tests: `tests/test_open_page_rank_source.py`, `tests/test_open_page_rank_dlt_csv.py`, `tests/test_open_page_rank_transforms.py`, `tests/test_open_page_rank_assets.py`, `tests/test_clickhouse_migrations.py`.
- Live: apply ClickHouse migrations, materialize `open_page_rank_domains_refresh_job`, then verify ClickHouse row count is close to 10M and top ranks are populated.
