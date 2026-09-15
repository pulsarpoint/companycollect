# Brave initialization asset pilot — 2026-09-15

Implemented in `bcf64d340`; independent initialization concurrency in `f2acbe32c`.
Both deployments completed with zero failed Ansible tasks. Dagster stayed
active/running with zero supervisor restarts. The four deployed runtime files
matched the validated local hashes. ClickHouse migration 413 and PostgreSQL
migration 122 preserve existing inputs, result rows and progress.

The new `company_brave_search_input` asset accepts source/column mappings,
company IDs and parameterized column filters. It prepares a fixed selection in
ClickHouse under a task ID. The processing asset accepts that task ID and chooses
its query type/template on the first processing run. The combined
`company_brave_search_workflow` connects both assets through the run task tag.

## Live verification

Task: `f8e663a4-8ea8-41d2-9e15-efc6da4f3a71`.

| Operation | Dagster run | Result | Run duration |
|---|---|---|---:|
| initialize | `563e485e-f965-4cf0-bc2d-0e5b57a2f4b7` | SUCCESS | 21.7 s |
| initialize_again | `58ffa785-fe8c-4b28-bb94-ac02e6a29bbb` | SUCCESS | 21.5 s |
| process | `aef3f2e2-2978-4811-b14f-2d334c1efda4` | SUCCESS | 21.9 s |

- Selected the same eight pilot companies from `se_company_basic_info FINAL`,
  using both explicit company IDs and `status: [active]`.
- Compared every selected input row with the expected source projection.
- After initialization: eight ClickHouse rows, one PostgreSQL task in `selected`
  state, zero PostgreSQL item records and zero new responses.
- Repeated initialization with the same task ID and filters: the selected rows
  were unchanged, with no duplicates.
- Processing consumed the prepared task using its saved selection. All eight
  inputs reused matching published answers within the 30-day freshness window.
  Their progress records reference the accepted successful results.
- Final counts: total 8, skipped 8, remaining 0, unpublished 0. No new Brave
  responses or external requests were needed in this verification.
- The eight legacy ClickHouse input rows retain their empty selection task ID.
  The table UUID remains `ed67e5fa-2bb1-429a-85d5-8368115f8cd5`.

This verifies initialization, repeatability and handoff. The earlier
[Brave live pilot](company-brave-clickhouse-input-pilot-2026-09-15.md) covers actual
browser requests across all four routes.

## Local checks

All targeted checks passed, including 12 initialization tests and 132 migration
checks, along with the existing processing-store, publication and browser tests.
Dagster definition validation and scoped Ruff checks passed.

The initialization suite used disposable PostgreSQL 17 and ClickHouse 26.5 servers:

- A three-million-row source was selected and inserted entirely inside ClickHouse;
  PostgreSQL contained one task, zero item records and zero result rows afterward.
- Independent selections of the same company retained their own names and counts.
- An interrupted initialization was retried without duplicating its rows or
  changing another task's selection.
- Concurrent initialization of the same task was fenced by a PostgreSQL session lock.
- A populated queue migration retained its original table UUID and legacy selection.
- The combined asset execution passed both automatic and explicit task IDs and
  processed eight fixture queries with a peak of four browser requests.

The three-million-row check is a local correctness test, not a production
throughput benchmark. Inputs remain fixed once confirmed; retention/cleanup of
completed task selections is not automated.

See [the operator guide](company-brave-processing.md) for launch configuration.
Local evidence: `/tmp/brave-initialization-pilot-*.json`,
`/tmp/brave-initialization-pilot.log`, `/tmp/brave-init-tests.log`,
`/tmp/brave-init-migration-tests.log`, and `/tmp/brave-init-pool-deploy.log`.
