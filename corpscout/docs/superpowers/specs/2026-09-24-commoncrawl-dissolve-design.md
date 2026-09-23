# Dissolve `corpscout/commoncrawl/` into `services/`

**Date:** 2026-09-24
**Status:** approved in chat, awaiting spec review
**Owner decisions:** dissolve the folder entirely; keep all server-side paths unchanged.

## 1. Why

`corpscout/commoncrawl/` was the home of every Common Crawl subsystem. In July 2026 the WARC
processor moved to `services/cc-processor/` (commit `9e1f3fce8`), but the folder kept the two DNS
scanners, their Ansible deployments, offline embedding tooling, design docs, and a README,
ARCHITECTURE and Makefile that still point at the departed processor. The two scanners are
long-running deployed services and belong beside the other services. After they move, nothing in
the folder is a live system, so the folder goes away rather than lingering half-empty.

## 2. Current state (verified 2026-09-24)

| Item | Where | Notes |
|---|---|---|
| `cc-dns-scan` | `commoncrawl/cc-dns-scan/` (57 tracked files) | Go module `cc-dns-scan`, Go 1.25, no `replace` directives, own `docs/superpowers/plans/` |
| `cc-dns-axfr` | `commoncrawl/cc-dns-axfr/` (34 files) | Go module `cc-dns-axfr`; reads cc-dns-scan output only through ClickHouse |
| Scanner playbooks | `commoncrawl/deploy/cc_dns_scan/`, `commoncrawl/deploy/cc_dns_axfr/` (26 files) | Target `hetzner01` (`ansible_user=root`); cross-compile on the control machine from `{{ playbook_dir }}/../../cc-dns-<x>`; install to `/opt/companycollect/corpscout/commoncrawl/cc-dns-<x>`; state dir `/opt/companycollect/corpscout/commoncrawl/cc-dns-worker`; scan playbook also owns Unbound + OS tuning roles; both leave the service **stopped and disabled** after a deploy |
| Live services | hetzner01 | `cc-dns-scan` active (cycle since 2026-09-19, ~3,500 qps), `cc-dns-axfr` active (cycle since 2026-09-05, 126k transfers). Resumable SQLite state in the state dir. |
| Embedding tooling | `commoncrawl/embedding-ab/` (11), `commoncrawl/embedding-tools/` (6), `commoncrawl/embedding-vllm.sh` | Standalone `uv` projects for cc-processor's stored page embeddings; `embedding-ab/uv.lock` exists untracked (ignored by the root `.gitignore`) |
| Ranks loader | `commoncrawl/domain_ranking/load-domain-ranks.sh` | Comment references `commoncrawl/cc-processor/.env` |
| Docs | `commoncrawl/docs/` (14 files) | 2 DNS specs, 8 processor/embedding docs, 4 dated cc-processor plans under `docs/superpowers/plans/` |
| Stale pointers | `commoncrawl/README.md`, `ARCHITECTURE.md`, `Makefile`, `.dockerignore` | Makefile runs `$(MAKE) -C cc-processor`, which no longer exists here |
| Untracked local | `commoncrawl/dist/` (129 MB July `cc-crawl` / `cc-enrich-worker` binaries), `commoncrawl/data/` (empty) | Not in git |
| Root `.gitignore` | `corpscout/commoncrawl/**/uv.lock` + `!corpscout/commoncrawl/cc-processor/cc-warc-index-builder/uv.lock` | Exception is stale; that lock is tracked at `services/cc-processor/cc-warc-index-builder/uv.lock` |
| Historical docs citing old paths | `docs/superpowers/specs/2026-07-05-…`, `docs/superpowers/plans/2026-07-05-…`, `cc-dns-scan/docs/superpowers/plans/*` | Dated records |

No path under `commoncrawl/`, `services/cc-processor/` or the root `.gitignore` is part of the
uncommitted work currently in the tree.

## 3. Target layout

Every tracked path moves with `git mv`; nothing is copied.

| From | To |
|---|---|
| `commoncrawl/cc-dns-scan/**` | `services/cc-dns-scan/**` |
| `commoncrawl/deploy/cc_dns_scan/**` | `services/cc-dns-scan/ansible/**` |
| `commoncrawl/docs/hostname-discovery-spec.md` | `services/cc-dns-scan/docs/hostname-discovery-spec.md` |
| `commoncrawl/cc-dns-axfr/**` | `services/cc-dns-axfr/**` |
| `commoncrawl/deploy/cc_dns_axfr/**` | `services/cc-dns-axfr/ansible/**` |
| `commoncrawl/docs/axfr-zone-transfer-spec.md` | `services/cc-dns-axfr/docs/axfr-zone-transfer-spec.md` |
| `commoncrawl/embedding-ab/**` | `services/cc-processor/tools/embedding-ab/**` |
| `commoncrawl/embedding-tools/**` | `services/cc-processor/tools/embedding-tools/**` |
| `commoncrawl/embedding-vllm.sh` | `services/cc-processor/tools/embedding-vllm.sh` |
| `commoncrawl/domain_ranking/load-domain-ranks.sh` | `services/cc-processor/tools/load-domain-ranks.sh` |
| `commoncrawl/docs/{cc-crawl-design,content-analysis-design,embed-only-mode-plan,embeddings-design,migration-plan,raw-staging-pipeline-design,schema,tech-mode-review-2026-07-02}.md` | `services/cc-processor/docs/` |
| `commoncrawl/docs/superpowers/plans/*.md` (4 files) | `services/cc-processor/docs/superpowers/plans/` |

Deleted from git: `commoncrawl/README.md`, `commoncrawl/ARCHITECTURE.md`, `commoncrawl/Makefile`,
`commoncrawl/.dockerignore`. Deleted locally (untracked): `commoncrawl/dist/`, `commoncrawl/data/`.
After this, `corpscout/commoncrawl/` does not exist.

Conventions applied: the playbook directory is named `ansible/`, matching the two Go services
already in `services/` (`translator`, `pulsarprotectctlog`). The hostname-discovery spec covers both
scanners and lands with `cc-dns-scan`, which the AXFR README already treats as the upstream. Go module
names are unchanged, so no import path changes anywhere.

## 4. Playbook changes

Per playbook, one variable changes in `ansible/group_vars/<group>/vars.yml`:

```yaml
cc_dns_scan_source_dir: "{{ playbook_dir }}/.."      # was "{{ playbook_dir }}/../../cc-dns-scan"
cc_dns_axfr_source_dir: "{{ playbook_dir }}/.."      # was "{{ playbook_dir }}/../../cc-dns-axfr"
```

Unchanged on purpose (owner decision): `*_deploy_dir`, `cc_dns_state_dir`, service names, run flags,
inventory, Unbound and OS-tuning roles, and the post-deploy stopped/disabled behaviour. Each
`ansible/README.md` gets its repository paths corrected and the prerequisite reworded from "checked
out beside the deploy directory" to "the playbook lives inside the service directory".

Neither playbook is run against hetzner01 as part of this change. Both scanners are mid-cycle and
the playbooks stop the service.

## 5. Pointer and hygiene edits

- `services/cc-processor/tools/embedding-ab/README.md`, `.../embedding-tools/README.md`: repository
  `cd` paths become `corpscout/services/cc-processor/tools/<name>`. The server path
  `/opt/companycollect/corpscout/commoncrawl/embedding-ab` in embedding-ab's README is a real host
  path and stays.
- `services/cc-processor/tools/load-domain-ranks.sh`: comment points at `services/cc-processor/.env`.
- `services/cc-processor/README.md` and `services/cc-processor/deploy/README.md`: repository paths
  `corpscout/commoncrawl/cc-processor…` become `corpscout/services/cc-processor…`. Server paths
  under `/opt/companycollect/corpscout/commoncrawl/cc-processor` are real and stay.
- Scanner READMEs: the sibling links `../cc-dns-axfr/` and `../cc-dns-scan/` remain valid. Each
  README adds one line pointing at its `ansible/` and `docs/` folders.
- Root `.gitignore`: replace the two `corpscout/commoncrawl/…` lines with
  `corpscout/services/cc-processor/tools/**/uv.lock`. The tracked
  `cc-warc-index-builder/uv.lock` is outside that pattern, so no exception is needed.
- Historical dated specs and plans that cite `commoncrawl/…` paths are left untouched.

## 6. Commits

1. `refactor(commoncrawl): move DNS scanners, tooling and docs under services/` -- `git mv` of
   everything in section 3 plus `git rm` of the four stale files. No content edits, so git records
   100% renames and `git log --follow` keeps history.
2. `chore(services): fix paths after the commoncrawl move` -- the edits in sections 4 and 5.

Only these paths are staged; the unrelated uncommitted work elsewhere in the tree is not touched.

## 7. Verification (before commit 2 is finalized)

- `go build ./... && go vet ./... && go test ./...` in `services/cc-dns-scan` and
  `services/cc-dns-axfr`.
- `ansible-playbook --syntax-check site.yml` in both `ansible/` directories.
- The playbooks' exact build step, run from each new source dir:
  `GOOS=linux GOARCH=amd64 go build -o bin/cc-dns-<x> ./cmd/cc-dns-<x>` -- proves the new relative
  source path resolves to a buildable module.
- `git ls-files corpscout/commoncrawl` is empty and the directory is gone.
- `rg "corpscout/commoncrawl|commoncrawl/cc-dns|commoncrawl/deploy|commoncrawl/embedding"` over
  live docs (READMEs, playbooks, scripts) returns only server paths and dated records.

## 8. Non-goals

- No redeploy and no change on hetzner01.
- No rename of ClickHouse tables or Dagster assets named `commoncrawl_*`; those are data names.
- No edits to the scanners' Go code.

## 9. Follow-ups

- Rename the hetzner01 install dirs to `/opt/companycollect/corpscout/services/cc-dns-<x>` and
  migrate the state dir, scheduled between scan cycles. Requires a small migration task in each
  playbook and a stop/move/start.
- Bring the cc-processor deploy README's `deploy_root` wording in line once that rename lands.
