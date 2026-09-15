# Brave fixed ClickHouse input pilot — 2026-09-15

Runtime commit: `d703f1d2f`. PostgreSQL migration 120; ClickHouse migration 411.
Deployment: Ansible content hot-sync, 33 successful tasks, zero failed tasks.
The Dagster service remained active/running with zero restarts. All four runtime
files matched the locally validated SHA-256 hashes after deployment.

## Live results

- Task: `03e9aeae-f9e7-4a6e-ae47-2a64926244ed`.
- Initial run: `eb07c160-ab61-4812-9a29-2338ba850d40`, SUCCESS in 54.6 seconds.
- Resume run: `4d6efedc-945e-4c7d-b276-cbd05a904a15`, SUCCESS in 21.9 seconds.
- Input: eight distinct rows in `corpscout.company_processing_input`, prepared with
  ClickHouse `INSERT SELECT` from selected registry rows. No input payloads were
  inserted into PostgreSQL. The ClickHouse queue still contains all eight rows.
- Four request slots: one direct and one on each of three proxies. Each route
  completed two responses. All eight succeeded on attempt one.
- Eight durable PostgreSQL responses matched all exported columns in ClickHouse,
  including exact answer text, query, company attribution, IDs and timestamps.
- Three acknowledged export batches; zero remaining work and zero unpublished outcomes.
- Resume created no new results, attempts, export batches or physical ClickHouse rows.
- All eight responses from the previous task
  `2305a766-dae3-4f33-a30c-c808ea958819` also matched ClickHouse after migration.

The copied answers totaled 1512 UTF-8 bytes. This is a
functional browser/storage check, not an independent validation of the returned
websites or a production throughput benchmark.

## Observed bounded admission

The task used `input_batch_size: 4`, `requests_per_route: 1`,
`freshness_days: 0`, `export_batch_size: 4`, and a ten-second export interval.
Its fixed total remained eight throughout these observations:

| UTC | Admitted IDs | Running | Succeeded | Remaining |
|---|---:|---:|---:|---:|
| 15:43:25 | 4 | 4 | 0 | 8 |
| 15:43:35 | 5 | 4 | 1 | 7 |
| 15:43:40 | 7 | 4 | 3 | 5 |
| 15:43:45 | 8 | 4 | 4 | 4 |
| 15:43:50 | 8 | 1 | 7 | 1 |
| 15:44:00 | 8 | 0 | 8 | 0 |

PostgreSQL progress records contain only `task_id`, `input_id`, state, attempt,
lease fields, next retry time and accepted result ID. They have no `input_data`,
`query` or work-key column. The rendered request belongs to the saved result.

## Local validation

169 tests passed using disposable PostgreSQL 17 / ClickHouse 26.5 instances and
browser fixtures. Dagster definition validation and scoped Ruff checks passed.
The real three-million-row ClickHouse test verified zero PostgreSQL item records
at registration, then exactly 100 after reading/admitting one page; remaining work
was still exactly 3,000,000. It also exercised bounded refill and recovery after
out-of-order completion. No three-million-request Brave test was performed.

Other checks covered concurrent claims, expired leases, retry budgets, atomic
cursor/admission rollback, input replacement and missing rows, freshness reuse,
legacy migration preservation, partial publication, lost acknowledgments and
publication outages. The input table must remain unchanged while unfinished work
exists; UUID and row checks do not provide database-enforced table immutability.

Operator configuration and retention requirements are in
[company-brave-processing.md](company-brave-processing.md).

Local evidence is retained in `/tmp/brave-ch-input-pilot-*.json`,
`/tmp/brave-ch-input-pilot.log`, `/tmp/brave-queue-validation.log`,
`/tmp/brave-queue-defs.log`, and `/tmp/brave-queue-deploy.log`.
