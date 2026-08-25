# Serbia APR company people design doc

## 1. Source overview

- **Country / registry**: Serbia — Agencija za privredne registre (APR)
- **Module**: `defs/serbia_apr_company_people/`
- **Representative DuckDB**: `data/serbia_apr_representatives_source.duckdb`
  using pool `serbia_apr_representatives_duckdb`
- **Beneficial-owner DuckDB**:
  `data/serbia_apr_beneficial_owners_source.duckdb` using pool
  `serbia_apr_beneficial_owners_duckdb`
- **ClickHouse migration**: `000319_corpscout_rs_apr_company_people`
- **Entity key**: eight-digit APR `matični broj` stored as `company_id`

The module has two independent source boundaries:

| source | access | expected contents |
|---|---|---|
| APR SP3/SP4 status data | paid delivery or contracted service | legal/other representatives, directors, boards and procurists |
| APR Central Register of Beneficial Owners (CEV) | separate eID/contracted service | statutory beneficial owners and their legal basis |

SP2+SP3+SP4 does not include CEV beneficial ownership. Separate DuckDB files,
pools and multi-assets prevent one contract from blocking the other.

## 2. Ingest mode — and why

This pass implements only the DuckDB-to-ClickHouse publication boundary. The
upstream acquisition assets will be created after APR supplies the contracted
payload formats and change semantics.

The future upstream assets are:

- `serbia_apr_company_representative_observations_duckdb`
- `serbia_apr_company_representatives_current_duckdb`
- `serbia_apr_company_beneficial_owner_observations_duckdb`
- `serbia_apr_company_beneficial_owners_current_duckdb`

The eventual ingest mode must be chosen independently for SP3/SP4 and CEV from
the actual APR contract: full-snapshot replacement for complete deliveries, or
event ingestion plus set-based history/current resolution for change feeds.

## 3. Loading

DuckDB owns the normalized source tables. The next pass must create the four
tables with the exact names and column contracts in `tables.py`.

Raw payloads and `source_payload_hash` remain in restricted DuckDB/object
storage. ClickHouse receives only the typed semantic columns, cheap source
record/run lineage and `state_fingerprint` needed for relationship state.

Both ClickHouse publishers refuse zero-row DuckDB inputs. This protects an
existing national table from being erased by a missing, unauthorized or failed
source load.

## 4. Transform

No transform occurs in the publisher. DuckDB tables must already contain the
target typed shape. Future normalization should be set-based DuckDB SQL, not
Python row loops.

The observation tables supplied to the publisher must represent the complete
history intended to remain queryable in ClickHouse, not only the latest batch.
That makes the publication rebuildable from durable raw source artifacts and
allows atomic full replacement.

## 5. ClickHouse schema — and DDL deviations

| table | grain | engine / publication |
|---|---|---|
| `rs_apr_company_representative_observations` | one observed representative relationship state | `MergeTree`; replaced atomically with its current table |
| `rs_apr_company_representatives_current` | latest state per company and relationship | `ReplacingMergeTree(resolved_at)`; readers use `FINAL WHERE is_current` |
| `rs_apr_company_beneficial_owner_observations` | one observed owner/basis state | `MergeTree`; replaced atomically with its current table |
| `rs_apr_company_beneficial_owners_current` | latest state per company and owner relationship | `ReplacingMergeTree(resolved_at)`; readers use `FINAL WHERE is_current` |

The migration owns all DDL. Publishers assert that target tables exist, build
stage tables with `CREATE TABLE AS`, load DuckDB rows, then use `EXCHANGE
TABLES`. If an exchange fails, the shared publisher reverses completed
exchanges before cleanup.

Representative and beneficial-owner pairs are separate atomic operations.
Dagster exposes four table assets, but neither pair is subsettable because
publishing history without current state, or current state without history,
would create an inconsistent source view.

## 6. Translation

No translation loader is needed for this source-level audit model:

- person and trust names are proper nouns;
- `role_code`, `basis_code` and boolean authority fields are normalized;
- `function_title`, relationship kind and basis label remain raw source
  evidence rather than serving text.

If a user-facing Serbian label is later required, it should use the shared
translation cache/view rather than adding `_en` columns to these base tables.

## 6b. Contacts

No contact information exists in SP3/SP4 or CEV company-person records. Contact
data belongs to the separate Serbian company-core/contact source investigation;
these assets must not infer contact facts from personal records.

## 7. Currency

Not applicable. The four tables contain no monetary values.

## 8. Scheduling

No jobs or schedules are registered in this pass. The ClickHouse assets become
materializable after the future DuckDB assets are implemented and tested.
Cadence must follow the contracted APR delivery SLA.

## 9. Privacy, security and issues

- Never store or log raw JMBG, passport, foreign identity-card,
  foreigner-number or refugee-card values.
- `personal_identifier_hmac` may contain only an approved keyed HMAC-SHA256
  produced before DuckDB publication. Plain SHA-256 is not acceptable.
- Keep the HMAC key outside Dagster, DuckDB and ClickHouse.
- Treat the DuckDB files and ClickHouse tables as restricted personal data.
- Company members/shareholders must not be inferred to be CEV beneficial
  owners.
- APR's exact stable identifiers and deletion semantics remain unknown. The
  future resolver must not merge people by name or infer deletion from absence.

## 10. Verification

- `tests/test_serbia_apr_company_people.py` pins asset dependencies, migration
  column order, independent source boundaries, atomic pair replacement and
  zero-row protection.
- `tests/test_clickhouse_migrations.py` registers migration `000319` in the
  explicit ledger.
- Required commands: `uv run pytest -q
  tests/test_serbia_apr_company_people.py`, `uv run pytest -q
  tests/test_clickhouse_migrations.py`, and `uv run dg check defs`.
- Before first live materialization, apply migration `000319`, build redacted or
  synthetic DuckDB fixtures with correct types, materialize each source pair,
  and verify row counts plus `FINAL WHERE is_current` behavior.
