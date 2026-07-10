# Correctness-Hardening — task → git tag map

Execution of [plans/2026-07-10-cc-dns-worker-correctness-hardening.md](plans/2026-07-10-cc-dns-worker-correctness-hardening.md).
Each task is implemented, reviewed, committed, and tagged so the fix can be checked out and verified in isolation.

- **Branch:** `harden-correctness` (merged to local `main` at the end; tags survive the merge).
- **Tag scheme:** `harden-task-NN` points at the last commit of that task.
- **Verify a fix:** `git show harden-task-NN` (the commit range), or `git diff harden-task-<NN-1> harden-task-NN` for the task's isolated diff, then run the task's tests.

| Tag | Task | Fix | Verify with |
|---|---|---|---|
| `harden-task-00` | 0 | Containment: drop `--axfr`/`--host-enrich` from prod flags | ansible flag set has both off |
| _pending_ | 1 | Classify authoritative targets; block non-public dials | `go test ./internal/resolve/ -run Target` |
| _pending_ | 2 | Preserve NS hostname↔IP endpoint identity | `go test ./internal/resolve ./internal/store` |
| _pending_ | 3 | Typed AXFR outcomes + definitive TCP preflight | `go test ./internal/resolve -run AXFR` |
| _pending_ | 4 | Durable SQLite AXFR work queue (stage-then-checkpoint) | `go test ./internal/store ./cmd/cc-dns-worker -run AXFR` |
| _pending_ | 5 | Retry-safe `dns_axfr_latest` / `dns_axfr_state_changes` | `go test ./internal/load` + migration test |
| _pending_ | 6 | Coherent `scan --axfr` / `run` / `load` AXFR behavior | `go test ./cmd/cc-dns-worker` |
| _pending_ | 7 | Retry-safe raw DNS record observations | `go test ./internal/load` + migration test |
| _pending_ | 8 | Split record vs domain-summary load progress | `go test ./internal/store ./internal/load` |
| _pending_ | 9 | Explicit DNS observation quality (done/partial/error) | `go test ./internal/resolve ./internal/store` |
| _pending_ | 10 | Circuit breaker enforces its contract | `go test ./internal/scheduler` |
| _pending_ | 11 | Scope + stream CT/registry enrichment | `go test ./internal/hostsource` |
| _pending_ | 12 | Cancellation, state validation, safe pruning | `go test ./cmd/cc-dns-worker` |
| _pending_ | 13 | Explicit SQLite schema/decoding failures | `go test ./internal/store` |
| _pending_ | 14 | Config validation, slog, cockroachdb/errors, deps | `go test ./... && go vet ./...` |
| _pending_ | 15 | Deployment hardening + schema preflight | `ansible-playbook --syntax-check site.yml` |
| _pending_ | 16 | Docs, integration suite, rollout | integration scenarios |

Status is tracked in `.superpowers/sdd/progress.md`.
