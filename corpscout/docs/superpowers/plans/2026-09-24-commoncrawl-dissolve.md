# Dissolve `corpscout/commoncrawl/` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the two DNS scanners and their Ansible playbooks under `services/`, fold the Common Crawl embedding tooling and docs into `services/cc-processor/`, and delete the `commoncrawl/` folder, without changing anything on the deployed hosts.

**Architecture:** Pure `git mv` relocation in one commit (so git records 100% renames), then a second commit that fixes the handful of path pointers the move invalidates: one playbook variable per scanner, README paths, a script's `.env` path, and the root `.gitignore`. No Go code, no ClickHouse, no Dagster, no host changes.

**Tech Stack:** git, Go 1.25+ modules (`cc-dns-scan`, `cc-dns-axfr`), Ansible (two playbooks, run only in `--syntax-check`), ripgrep for verification.

**Spec:** `docs/superpowers/specs/2026-09-24-commoncrawl-dissolve-design.md` (absolute: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/docs/superpowers/specs/2026-09-24-commoncrawl-dissolve-design.md`)

## Global Constraints

- Working directory for every command unless stated: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout` (called `corpscout/` below). The git repository root is ONE level up: `/Users/graovic/pulsarpoint/ppoint/companycollect`. The root `.gitignore` is therefore `../.gitignore` from `corpscout/`, i.e. `/Users/graovic/pulsarpoint/ppoint/companycollect/.gitignore`.
- **Never run either Ansible playbook against a host.** Both scanners are mid-cycle on `hetzner01` and the playbooks stop the service. Only `ansible-playbook --syntax-check` is allowed.
- **Server-side paths stay exactly as they are** (`/opt/companycollect/corpscout/commoncrawl/...`). Any string starting with `/opt/companycollect/` is a host path and must NOT be edited.
- Go module names stay `cc-dns-scan` and `cc-dns-axfr`. No `go.mod` edits.
- Two main commits: Task 1 commits the moves; Tasks 2-5 only stage; Task 6 commits the edits. Pointers that review finds afterwards go into small append-only follow-up commits (spec section 6 item 3), never into an amend of a commit already on `main`. Stage only the paths named in this plan. The tree holds ~290 unrelated dirty files from other workstreams; never `git add -A`, never `git add .`.
- Commit messages follow Conventional Commits and end with the line `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- `ansible-playbook` refuses non-blocking stdio in this tool. Always run it as `ansible-playbook ... < /dev/null > /tmp/ansible-out.txt 2>&1; echo "rc=$?"; cat /tmp/ansible-out.txt`.
- Use `rg`, not `grep`. Use `sed -n` / `cat` to read; use the Edit tool (or `python3` with exact strings) to change files. macOS `sed -i` needs `sed -i ''`.
- Do not touch dated documents under any `docs/superpowers/plans/` or `docs/superpowers/specs/` other than the two files this plan owns. Historical path references in them are intentional records.

---

### Task 1: Move everything and delete the stale files (commit 1)

**Files:**
- Move (git): all 154 tracked paths under `commoncrawl/` listed in the spec's section 3
- Delete (git): `commoncrawl/README.md`, `commoncrawl/ARCHITECTURE.md`, `commoncrawl/Makefile`, `commoncrawl/.dockerignore`
- Delete (disk, untracked): `commoncrawl/dist/`, `commoncrawl/data/`

**Interfaces:**
- Consumes: nothing.
- Produces: the directories `services/cc-dns-scan/` (with `ansible/`, `docs/`), `services/cc-dns-axfr/` (with `ansible/`, `docs/`), `services/cc-processor/tools/`, `services/cc-processor/docs/`. Tasks 2-6 edit files at these paths.

- [ ] **Step 1: Confirm the paths are clean and the target names are free**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git status --short -- commoncrawl services/cc-processor ../.gitignore
for p in services/cc-dns-scan services/cc-dns-axfr services/cc-processor/tools services/cc-processor/docs; do test -e "$p" && echo "EXISTS: $p"; done
git ls-files commoncrawl | wc -l
```
Expected: the `git status` prints nothing; no `EXISTS:` lines; the count is `154`.
If `git status` prints anything, STOP: another workstream touched these paths since the spec was written.

- [ ] **Step 2: Move the two scanners with their playbooks and specs**

`git mv <dir> <new-dir>` requires the destination NOT to exist (if it exists, git moves the source *into* it). Do not pre-create `services/cc-dns-scan` or `services/cc-dns-axfr`.

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git mv commoncrawl/cc-dns-scan services/cc-dns-scan
git mv commoncrawl/deploy/cc_dns_scan services/cc-dns-scan/ansible
git mv commoncrawl/docs/hostname-discovery-spec.md services/cc-dns-scan/docs/hostname-discovery-spec.md

git mv commoncrawl/cc-dns-axfr services/cc-dns-axfr
git mv commoncrawl/deploy/cc_dns_axfr services/cc-dns-axfr/ansible
mkdir -p services/cc-dns-axfr/docs
git mv commoncrawl/docs/axfr-zone-transfer-spec.md services/cc-dns-axfr/docs/axfr-zone-transfer-spec.md
```
Expected: no output. (`services/cc-dns-scan/docs/` already exists because the package tracks `docs/superpowers/plans/`; `services/cc-dns-axfr/docs/` did not, hence the `mkdir`.)

- [ ] **Step 3: Move the embedding tooling, the ranks loader and the processor docs**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
mkdir -p services/cc-processor/tools services/cc-processor/docs
git mv commoncrawl/embedding-ab services/cc-processor/tools/embedding-ab
git mv commoncrawl/embedding-tools services/cc-processor/tools/embedding-tools
git mv commoncrawl/embedding-vllm.sh services/cc-processor/tools/embedding-vllm.sh
git mv commoncrawl/domain_ranking/load-domain-ranks.sh services/cc-processor/tools/load-domain-ranks.sh

for f in cc-crawl-design content-analysis-design embed-only-mode-plan embeddings-design migration-plan raw-staging-pipeline-design schema tech-mode-review-2026-07-02; do
  git mv "commoncrawl/docs/$f.md" "services/cc-processor/docs/$f.md"
done
git mv commoncrawl/docs/superpowers services/cc-processor/docs/superpowers
```
Expected: no output. `git mv` renames directories on disk, so the ignored `embedding-ab/.venv/`, `embedding-ab/uv.lock` and the scanners' `bin/` build outputs travel with their packages.

- [ ] **Step 4: Delete the stale pointer files and the untracked local artifacts, then remove the folder**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git rm -q commoncrawl/README.md commoncrawl/ARCHITECTURE.md commoncrawl/Makefile commoncrawl/.dockerignore
rm -rf commoncrawl/dist commoncrawl/data
find commoncrawl -type d -empty -delete 2>/dev/null
ls -la commoncrawl 2>&1 | head
```
Expected: `ls: commoncrawl: No such file or directory`.
If `ls` shows leftover files (for example `.DS_Store`), list them, delete them with `rm`, rerun the `find ... -delete`, and confirm the folder is gone. Never delete a file whose name you do not recognise as a local artifact: STOP and report it instead.

- [ ] **Step 5: Verify the move is rename-only and complete**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git ls-files commoncrawl | wc -l
git diff --cached --name-status | awk '{print substr($1,1,1)}' | sort | uniq -c
git diff --cached --name-status | rg '^R' | rg -v '^R100' || echo "all renames are 100%"
git diff --cached --name-status | rg '^D'
```
Expected:
- first line `0`;
- the histogram shows `150 R` and `4 D` (154 tracked paths minus the 4 deletions are renames);
- `all renames are 100%`;
- exactly four `D` lines: `commoncrawl/.dockerignore`, `commoncrawl/ARCHITECTURE.md`, `commoncrawl/Makefile`, `commoncrawl/README.md`.

- [ ] **Step 6: Prove both Go modules still build, vet and test from their new homes**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/cc-dns-scan && go build ./... && go vet ./... && go test ./... 2>&1 | tail -5
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/cc-dns-axfr && go build ./... && go vet ./... && go test ./... 2>&1 | tail -5
```
Expected: every `go test` line ends in `ok` or `[no test files]`; no `FAIL`. (Module names are unchanged, so imports resolve exactly as before.)

- [ ] **Step 7: Commit the move**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git status --short | rg -v '^(R|D) ' | rg 'commoncrawl|services/cc-dns|services/cc-processor/(tools|docs)' && echo "UNEXPECTED unstaged change above - STOP" || true
git commit -q -m "$(cat <<'MSG'
refactor(commoncrawl): move DNS scanners, tooling and docs under services/

Pure git renames, no content edits: cc-dns-scan and cc-dns-axfr with
their Ansible playbooks (now <service>/ansible/) and their design specs
(<service>/docs/), the embedding tooling and the domain-ranks loader
(services/cc-processor/tools/), and the processor docs
(services/cc-processor/docs/). The stale commoncrawl README,
ARCHITECTURE, Makefile and .dockerignore, which still pointed at the
processor that left in July, are deleted. Nothing on any host changes;
the playbooks' path fix follows in the next commit.

Spec: docs/superpowers/specs/2026-09-24-commoncrawl-dissolve-design.md

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
MSG
)"
git show --stat --format='%h %s' HEAD | tail -1
git log --oneline --follow -2 -- services/cc-dns-scan/go.mod
```
Expected: the stat line says `154 files changed` and reports only `deletions(-)`, no insertions (renames add nothing; the four deletions remove lines); the `--follow -2` log shows this commit AND the pre-move commit that last touched `go.mod` (history preserved).

---

### Task 2: Point both playbooks at their new source dir and fix their READMEs (stage only)

**Files:**
- Modify: `services/cc-dns-scan/ansible/group_vars/cc_dns_scan/vars.yml:3`
- Modify: `services/cc-dns-axfr/ansible/group_vars/cc_dns_axfr/vars.yml:3`
- Modify: `services/cc-dns-scan/ansible/README.md` (lines 5, 20-21, 41, 64, 103, 129)
- Modify: `services/cc-dns-axfr/ansible/README.md` (lines 4, 24, 45, 57, 110)

**Interfaces:**
- Consumes: the directories produced by Task 1.
- Produces: `cc_dns_scan_source_dir` and `cc_dns_axfr_source_dir` both equal to `{{ playbook_dir }}/..`, i.e. the service directory. Task 6's build check relies on that value.

- [ ] **Step 1: Write the failing check**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
rg -n 'source_dir: "\{\{ playbook_dir \}\}/\.\./\.\./cc-dns-' services/cc-dns-scan/ansible services/cc-dns-axfr/ansible
rg -n 'commoncrawl/deploy|\.\./\.\./cc-dns-(scan|axfr)/cmd|checked out beside|\.\./cc_dns_scan`|^deploy/cc_dns_' services/cc-dns-scan/ansible/README.md services/cc-dns-axfr/ansible/README.md | wc -l
```
Expected: two `source_dir` hits (one per playbook) and a README hit count of `11`. These are the lines this task removes.

- [ ] **Step 2: Change the source dir variable in both playbooks**

In `services/cc-dns-scan/ansible/group_vars/cc_dns_scan/vars.yml` replace the line
```yaml
cc_dns_scan_source_dir: "{{ playbook_dir }}/../../cc-dns-scan"
```
with
```yaml
cc_dns_scan_source_dir: "{{ playbook_dir }}/.."
```

In `services/cc-dns-axfr/ansible/group_vars/cc_dns_axfr/vars.yml` replace the line
```yaml
cc_dns_axfr_source_dir: "{{ playbook_dir }}/../../cc-dns-axfr"
```
with
```yaml
cc_dns_axfr_source_dir: "{{ playbook_dir }}/.."
```

Leave every other line of both files untouched, in particular `*_deploy_dir` and `cc_dns_state_dir`, which are host paths.

- [ ] **Step 3: Fix the DNS scanner playbook README**

In `services/cc-dns-scan/ansible/README.md`:

Replace
```markdown
- builds `../../cc-dns-scan/cmd/cc-dns-scan` for Linux/AMD64 on the control machine;
```
with
```markdown
- builds this service's `cmd/cc-dns-scan` (one directory up from this playbook) for Linux/AMD64 on the control machine;
```

Replace
```markdown
- Ansible and Go are installed on the control machine, with `cc-dns-scan` checked out beside the
  `deploy` directory.
```
with
```markdown
- Ansible and Go are installed on the control machine. This playbook lives inside the
  `services/cc-dns-scan` directory and builds the Go module one level up from `ansible/`.
```

Replace all three occurrences of
```bash
cd corpscout/commoncrawl/deploy/cc_dns_scan
```
with
```bash
cd corpscout/services/cc-dns-scan/ansible
```

In the `## Layout` tree replace the first line
```text
deploy/cc_dns_scan/
```
with
```text
services/cc-dns-scan/ansible/
```

- [ ] **Step 4: Fix the AXFR playbook README**

In `services/cc-dns-axfr/ansible/README.md`:

Replace
```markdown
builds `../../cc-dns-axfr/cmd/cc-dns-axfr` on the control machine and installs:
```
with
```markdown
builds this service's `cmd/cc-dns-axfr` (one directory up from this playbook) on the control machine and installs:
```

Replace
```markdown
- The independent DNS package at `../cc_dns_scan` has completed the initial cutover.
```
with
```markdown
- The independent DNS package at `../../cc-dns-scan/ansible` has completed the initial cutover.
```

Replace both occurrences of
```bash
cd corpscout/commoncrawl/deploy/cc_dns_axfr
```
with
```bash
cd corpscout/services/cc-dns-axfr/ansible
```

In the `## Layout` tree replace the first line
```text
deploy/cc_dns_axfr/
```
with
```text
services/cc-dns-axfr/ansible/
```

- [ ] **Step 5: Run the check again and syntax-check both playbooks**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
rg -n 'source_dir:' services/cc-dns-scan/ansible/group_vars/cc_dns_scan/vars.yml services/cc-dns-axfr/ansible/group_vars/cc_dns_axfr/vars.yml
rg -n 'commoncrawl/deploy|\.\./\.\./cc-dns-(scan|axfr)/cmd|checked out beside|\.\./cc_dns_scan`|^deploy/cc_dns_' services/cc-dns-scan/ansible/README.md services/cc-dns-axfr/ansible/README.md | wc -l
for s in cc-dns-scan cc-dns-axfr; do
  ( cd services/$s/ansible && ansible-playbook --syntax-check site.yml < /dev/null > /tmp/ansible-out-$s.txt 2>&1; echo "$s rc=$?"; cat /tmp/ansible-out-$s.txt )
done
```
Expected: both `source_dir` lines read `"{{ playbook_dir }}/.."`; the README count is `0`; both syntax checks print `rc=0` and `playbook: site.yml`. The vault password file `~/.config/ansible/cc-dns-scan` exists on this machine, so vault-encrypted group vars parse. If a syntax check fails, read the error: it must not be about a missing role or file (that would mean Task 1 moved something incorrectly).

- [ ] **Step 6: Stage (do not commit)**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add services/cc-dns-scan/ansible/group_vars/cc_dns_scan/vars.yml services/cc-dns-axfr/ansible/group_vars/cc_dns_axfr/vars.yml services/cc-dns-scan/ansible/README.md services/cc-dns-axfr/ansible/README.md
git diff --cached --stat | tail -1
```
Expected: `4 files changed`.

---

### Task 3: Fix the scanner READMEs and the two moved DNS specs (stage only)

**Files:**
- Modify: `services/cc-dns-scan/README.md` (the `## Deploy` section, plus one new line before `## Data flow`)
- Modify: `services/cc-dns-axfr/README.md` (the `## Deploy` section, plus one new line before `## Build and test`)
- Modify: `services/cc-dns-scan/docs/hostname-discovery-spec.md:4`
- Modify: `services/cc-dns-axfr/docs/axfr-zone-transfer-spec.md:4-5`

**Interfaces:**
- Consumes: the `ansible/` and `docs/` directories produced by Task 1.
- Produces: nothing other tasks use.

- [ ] **Step 1: Write the failing check**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
rg -n '\.\./deploy/cc_dns|cd \.\./cc_dns_axfr' services/cc-dns-scan/README.md services/cc-dns-axfr/README.md | wc -l
rg -n '\(\.\./cc-dns-(scan|axfr)/\)' services/cc-dns-scan/docs/hostname-discovery-spec.md services/cc-dns-axfr/docs/axfr-zone-transfer-spec.md | wc -l
```
Expected: `8` and `3`. Both must be `0` after this task.

- [ ] **Step 2: Rewrite the DNS scanner README's Deploy section and add the docs pointer**

In `services/cc-dns-scan/README.md` replace
```markdown
Deployment is owned by [`../deploy/cc_dns_scan`](../deploy/cc_dns_scan/), including the DNS host's
Unbound and OS tuning. Deploy this package before [`cc_dns_axfr`](../deploy/cc_dns_axfr/). The
playbook installs `cc-dns-scan.service` but deliberately leaves it stopped and disabled:

```bash
cd ../deploy/cc_dns_scan
ansible-playbook site.yml
ssh root@hetzner01 'systemctl enable --now cc-dns-scan'
ssh root@hetzner01 'journalctl -u cc-dns-scan -n 100 -f'

cd ../cc_dns_axfr
ansible-playbook site.yml
```
with
```markdown
Deployment is owned by [`ansible/`](ansible/), including the DNS host's Unbound and OS tuning.
Deploy this package before [`../cc-dns-axfr/ansible`](../cc-dns-axfr/ansible/). The playbook
installs `cc-dns-scan.service` but deliberately leaves it stopped and disabled:

```bash
cd ansible
ansible-playbook site.yml
ssh root@hetzner01 'systemctl enable --now cc-dns-scan'
ssh root@hetzner01 'journalctl -u cc-dns-scan -n 100 -f'

cd ../../cc-dns-axfr/ansible
ansible-playbook site.yml
```
(The two `ssh` lines that follow for `cc-dns-axfr` stay as they are.)

Then insert this paragraph immediately before the `## Data flow` heading (leave one blank line on each side):
```markdown
Design notes live in [`docs/hostname-discovery-spec.md`](docs/hostname-discovery-spec.md); dated
implementation plans are under `docs/superpowers/plans/`. Deployment is in [`ansible/`](ansible/).
```

- [ ] **Step 3: Rewrite the AXFR README's Deploy section and add the docs pointer**

In `services/cc-dns-axfr/README.md` replace
```markdown
Production deployment is owned by [`../deploy/cc_dns_axfr`](../deploy/cc_dns_axfr/). Deploy and verify
[`cc_dns_scan`](../deploy/cc_dns_scan/) first. Both playbooks install their units but deliberately
leave them stopped and disabled:

```bash
cd ../deploy/cc_dns_scan
ansible-playbook site.yml
ssh root@hetzner01 'systemctl enable --now cc-dns-scan'
ssh root@hetzner01 'journalctl -u cc-dns-scan -n 100 -f'

cd ../cc_dns_axfr
ansible-playbook site.yml
```
with
```markdown
Production deployment is owned by [`ansible/`](ansible/). Deploy and verify
[`../cc-dns-scan/ansible`](../cc-dns-scan/ansible/) first. Both playbooks install their units but
deliberately leave them stopped and disabled:

```bash
cd ../cc-dns-scan/ansible
ansible-playbook site.yml
ssh root@hetzner01 'systemctl enable --now cc-dns-scan'
ssh root@hetzner01 'journalctl -u cc-dns-scan -n 100 -f'

cd ../../cc-dns-axfr/ansible
ansible-playbook site.yml
```

Then insert this paragraph immediately before the `## Build and test` heading (one blank line on each side):
```markdown
Design notes live in [`docs/axfr-zone-transfer-spec.md`](docs/axfr-zone-transfer-spec.md).
Deployment is in [`ansible/`](ansible/).
```

- [ ] **Step 4: Fix the sibling links inside the two moved specs**

In `services/cc-dns-scan/docs/hostname-discovery-spec.md` line 4, change the two link targets:
`](../cc-dns-scan/)` becomes `](../)` and `](../cc-dns-axfr/)` becomes `](../../cc-dns-axfr/)`. The link text stays.

In `services/cc-dns-axfr/docs/axfr-zone-transfer-spec.md` lines 4-5:
`](../cc-dns-axfr/)` becomes `](../)` and `](../cc-dns-scan/)` becomes `](../../cc-dns-scan/)`. The link text stays.

- [ ] **Step 5: Run the check again and confirm every relative link in the four files resolves**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
rg -n '\.\./deploy/cc_dns|cd \.\./cc_dns_axfr' services/cc-dns-scan/README.md services/cc-dns-axfr/README.md | wc -l
rg -n '\(\.\./cc-dns-(scan|axfr)/\)' services/cc-dns-scan/docs/hostname-discovery-spec.md services/cc-dns-axfr/docs/axfr-zone-transfer-spec.md | wc -l
for f in services/cc-dns-scan/README.md services/cc-dns-axfr/README.md services/cc-dns-scan/docs/hostname-discovery-spec.md services/cc-dns-axfr/docs/axfr-zone-transfer-spec.md; do
  d=$(dirname "$f")
  rg -o '\]\(([^)#]+)\)' -r '$1' "$f" | rg -v '^https?://' | sort -u | while read -r t; do test -e "$d/$t" || echo "BROKEN in $f: $t"; done
done; echo "link check done"
```
Expected: `0`, `0`, then only `link check done` (no `BROKEN` lines).

- [ ] **Step 6: Stage (do not commit)**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add services/cc-dns-scan/README.md services/cc-dns-axfr/README.md services/cc-dns-scan/docs/hostname-discovery-spec.md services/cc-dns-axfr/docs/axfr-zone-transfer-spec.md
git diff --cached --stat | tail -1
```
Expected: `8 files changed` (Task 2's four plus these four).

---

### Task 4: Fix the cc-processor tooling and README pointers (stage only)

**Files:**
- Modify: `services/cc-processor/tools/embedding-ab/README.md:29`
- Modify: `services/cc-processor/tools/embedding-tools/README.md:26`
- Modify: `services/cc-processor/tools/load-domain-ranks.sh:22-23,35`
- Modify: `services/cc-processor/README.md:63,352`
- Modify: `services/cc-processor/deploy/README.md:31,74`

**Interfaces:**
- Consumes: `services/cc-processor/tools/` from Task 1.
- Produces: `load-domain-ranks.sh` sources `services/cc-processor/.env` relative to its own location.

- [ ] **Step 1: Write the failing check**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
rg -n 'corpscout/commoncrawl/(cc-processor|embedding)|commoncrawl/cc-processor/\.env|dirname "\$0"\)/cc-processor|cd \.\. && make' services/cc-processor --glob '!**/.venv/**' --glob '!**/superpowers/**' | rg -v '/opt/companycollect/'
```
Expected: exactly nine hits: embedding-ab README line 29, embedding-tools README line 26, `load-domain-ranks.sh` lines 22, 23 and 35, `services/cc-processor/README.md` lines 63 and 352, `services/cc-processor/deploy/README.md` lines 31 and 74. Lines containing `/opt/companycollect/` are host paths; the trailing `rg -v` drops them on purpose.

- [ ] **Step 2: Fix the two embedding READMEs**

In `services/cc-processor/tools/embedding-ab/README.md` replace
```bash
cd corpscout/commoncrawl/embedding-ab
```
with
```bash
cd corpscout/services/cc-processor/tools/embedding-ab
```
Leave the later `cd /opt/companycollect/corpscout/commoncrawl/embedding-ab` line alone: it is a host path.

In `services/cc-processor/tools/embedding-tools/README.md` replace
```bash
cd corpscout/commoncrawl/embedding-tools
```
with
```bash
cd corpscout/services/cc-processor/tools/embedding-tools
```

- [ ] **Step 3: Fix the ranks loader's comment and its `.env` source line**

In `services/cc-processor/tools/load-domain-ranks.sh` replace
```bash
#   - commoncrawl/cc-processor/.env with CLICKHOUSE_HOST / _NATIVE_PORT / _USER / _PASSWORD
#   - the table exists:  (cd .. && make clickhouse-migrate-up)
```
with
```bash
#   - services/cc-processor/.env (one directory up) with CLICKHOUSE_HOST / _NATIVE_PORT / _USER / _PASSWORD
#   - the table exists:  (cd ../../.. && make clickhouse-migrate-up)   # from corpscout/ root
```
and replace
```bash
set -a; . "$(dirname "$0")/cc-processor/.env"; set +a
```
with
```bash
set -a; . "$(dirname "$0")/../.env"; set +a
```

- [ ] **Step 4: Fix the repository paths in the two cc-processor READMEs**

In `services/cc-processor/README.md` replace
```bash
cd corpscout/commoncrawl/cc-processor
```
with
```bash
cd corpscout/services/cc-processor
```
and replace
```bash
cd corpscout/commoncrawl/cc-processor/deploy
```
with
```bash
cd corpscout/services/cc-processor/deploy
```
Do NOT touch line 369 (`/opt/companycollect/corpscout/commoncrawl/cc-processor/cc-enrich-worker/bin/cc-enrich-worker`); it is a host path.

In `services/cc-processor/deploy/README.md` replace both occurrences of
```bash
cd corpscout/commoncrawl/cc-processor/deploy
```
with
```bash
cd corpscout/services/cc-processor/deploy
```
Lines 25, 39 and 61 mention `/opt/companycollect/corpscout/commoncrawl/cc-processor`; they are host paths and stay.

- [ ] **Step 5: Run the check again and prove the script's `.env` path resolves**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
rg -n 'corpscout/commoncrawl/(cc-processor|embedding)|commoncrawl/cc-processor/\.env|dirname "\$0"\)/cc-processor|cd \.\. && make' services/cc-processor --glob '!**/.venv/**' --glob '!**/superpowers/**' | rg -v '/opt/companycollect/' | wc -l
bash -n services/cc-processor/tools/load-domain-ranks.sh && echo "syntax ok"
d=services/cc-processor/tools; echo "script will source: $d/../.env -> $(cd $d/.. && pwd)/.env"; ls services/cc-processor/.env.example
rg -n '^clickhouse-migrate-up:' Makefile
```
Expected: `0`; `syntax ok`; the resolved path ends in `services/cc-processor/.env` and `.env.example` is listed beside it (the real `.env` is gitignored and may be absent locally, which is fine); the Makefile target exists.

- [ ] **Step 6: Stage (do not commit)**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add services/cc-processor/tools/embedding-ab/README.md services/cc-processor/tools/embedding-tools/README.md services/cc-processor/tools/load-domain-ranks.sh services/cc-processor/README.md services/cc-processor/deploy/README.md
git diff --cached --stat | tail -1
```
Expected: `13 files changed`.

---

### Task 5: Replace the root `.gitignore` rule (stage only)

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/.gitignore:58-60`

**Interfaces:**
- Consumes: `services/cc-processor/tools/embedding-ab/uv.lock` exists on disk, untracked, from Task 1.
- Produces: nothing other tasks use.

- [ ] **Step 1: Write the failing check**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git status --short services/cc-processor/tools | rg 'uv.lock' && echo "LOCK IS VISIBLE (expected before the fix)"
git check-ignore -v services/cc-processor/tools/embedding-ab/uv.lock || echo "not ignored (expected before the fix)"
```
Expected: the lock shows as `??` (untracked) and `not ignored` because the old rule only covers `corpscout/commoncrawl/**`.

- [ ] **Step 2: Replace the rule**

In `/Users/graovic/pulsarpoint/ppoint/companycollect/.gitignore` replace the three lines
```gitignore
# uv lockfiles in the commoncrawl helper projects (dagster_v3 tracks its own).
corpscout/commoncrawl/**/uv.lock
!corpscout/commoncrawl/cc-processor/cc-warc-index-builder/uv.lock
```
with the two lines
```gitignore
# uv lockfiles in the cc-processor helper tools (dagster_v3 and cc-warc-index-builder track their own).
corpscout/services/cc-processor/tools/**/uv.lock
```

- [ ] **Step 3: Run the check again**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git check-ignore -v services/cc-processor/tools/embedding-ab/uv.lock
git status --short services/cc-processor/tools | rg 'uv.lock' || echo "lock hidden"
git ls-files services/cc-processor/cc-warc-index-builder/uv.lock
```
Expected: the first line names the new rule; `lock hidden`; the third line prints the tracked `cc-warc-index-builder/uv.lock` path (it is outside the new pattern, so it stays tracked and needs no exception).

- [ ] **Step 4: Stage (do not commit)**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add ../.gitignore
git diff --cached --stat | tail -1
```
Expected: `14 files changed`.

---

### Task 6: Final verification sweep and commit 2

**Files:**
- No new edits. Verifies Tasks 2-5 and commits them.

**Interfaces:**
- Consumes: everything staged by Tasks 2-5.
- Produces: the second and final commit.

- [ ] **Step 1: Confirm only the intended 14 files are staged and nothing else moved**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git diff --cached --name-only
git diff --cached --name-only | wc -l
```
Expected: `14`, and the list is exactly: `.gitignore` (repo root), `corpscout/services/cc-dns-axfr/README.md`, `corpscout/services/cc-dns-axfr/ansible/README.md`, `corpscout/services/cc-dns-axfr/ansible/group_vars/cc_dns_axfr/vars.yml`, `corpscout/services/cc-dns-axfr/docs/axfr-zone-transfer-spec.md`, `corpscout/services/cc-dns-scan/README.md`, `corpscout/services/cc-dns-scan/ansible/README.md`, `corpscout/services/cc-dns-scan/ansible/group_vars/cc_dns_scan/vars.yml`, `corpscout/services/cc-dns-scan/docs/hostname-discovery-spec.md`, `corpscout/services/cc-processor/README.md`, `corpscout/services/cc-processor/deploy/README.md`, `corpscout/services/cc-processor/tools/embedding-ab/README.md`, `corpscout/services/cc-processor/tools/embedding-tools/README.md`, `corpscout/services/cc-processor/tools/load-domain-ranks.sh`.

- [ ] **Step 2: Sweep the live files for stale repository paths**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
rg -n 'commoncrawl' services/cc-dns-scan services/cc-dns-axfr services/cc-processor ../.gitignore \
  --glob '!**/.venv/**' --glob '!**/bin/**' --glob '!**/superpowers/**' --glob '!**/go.sum' \
  --glob '!services/cc-processor/docs/**' \
  | rg -v '/opt/companycollect/corpscout/commoncrawl|\{\{ deploy_root \}\}/corpscout/commoncrawl|commoncrawl_|corpscout\.commoncrawl|Common Crawl|commoncrawl\.org|s3://crawls/commoncrawl|"commoncrawl"|commoncrawl/catalogs|commoncrawl2|CC-MAIN' \
  || echo "no stale repository paths"
```
Expected: `no stale repository paths`. Allowed survivors are excluded by the second filter: host paths under `/opt/...` or built from `{{ deploy_root }}`, the `"commoncrawl"` S3 bucket and `commoncrawl/catalogs` prefix literals in Go/Python, ClickHouse table names (`commoncrawl_*`, `corpscout.commoncrawl…`), the words "Common Crawl", the `commoncrawl.org` site, the S3 prefix, the `commoncrawl2` hostname and `CC-MAIN` crawl ids. If anything else prints, it is a missed pointer: record it for the follow-up commit that spec section 6 item 3 permits, fix it there, and rerun this sweep afterwards. The eight moved
processor design docs under `services/cc-processor/docs/` are excluded on purpose: they are
historical designs and keep their original text (spec section 5).

- [ ] **Step 3: Reproduce each playbook's exact build step from the new source dir**

This is the check that the `{{ playbook_dir }}/..` value from Task 2 points at a buildable module. It runs the same command, environment and working directory the playbooks use (`ansible/../` = the service dir).

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/cc-dns-scan/ansible/.. && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/cc-dns-scan ./cmd/cc-dns-scan && file bin/cc-dns-scan
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/cc-dns-axfr/ansible/.. && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/cc-dns-axfr ./cmd/cc-dns-axfr && file bin/cc-dns-axfr
```
Expected: both `file` lines say `ELF 64-bit LSB executable, x86-64, ... statically linked`. The `bin/` outputs are gitignored by each package's own `.gitignore`.

- [ ] **Step 4: Re-run the Go checks and both syntax checks one last time**

Run:
```bash
for s in cc-dns-scan cc-dns-axfr; do
  ( cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/$s && go vet ./... && go test ./... 2>&1 | rg -c '^ok|no test files' )
  ( cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/$s/ansible && ansible-playbook --syntax-check site.yml < /dev/null > /tmp/ansible-out-$s.txt 2>&1; echo "$s syntax rc=$?" )
done
```
Expected: a positive package count for each module and `rc=0` for both playbooks.

- [ ] **Step 5: Commit**

Run:
```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git commit -q -m "$(cat <<'MSG'
chore(services): fix paths after the commoncrawl move

Both scanner playbooks now build from "{{ playbook_dir }}/.." (the service
directory) instead of "../../cc-dns-<x>". README repository paths, the
scanner cross-links, the two moved DNS specs' sibling links, and the
domain-ranks loader's .env source line (broken since the July cc-processor
move) follow the new layout. The root .gitignore rule for the embedding
tools' uv.lock moves to services/cc-processor/tools. Host-side paths under
/opt/companycollect/corpscout/commoncrawl are deliberately unchanged: both
scanners are mid-cycle on hetzner01 and no deploy is part of this change.

Spec: docs/superpowers/specs/2026-09-24-commoncrawl-dissolve-design.md

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
MSG
)"
git log --oneline -3
git status --short services/cc-dns-scan services/cc-dns-axfr services/cc-processor ../.gitignore | rg -v '^\?\? .*/(bin|\.venv)/' || echo "clean"
```
Expected: the top two commits are this one and Task 1's `refactor(commoncrawl): ...`; the final line is `clean` (only ignored `bin/` and `.venv/` dirs may remain, and they are filtered out).

- [ ] **Step 6: Report**

Report to the owner: both commit hashes, the output of Step 2 (no stale repository paths), confirmation that no playbook was run against a host, and the two follow-ups from the spec's section 9 (host path rename between cycles; the cc-processor deploy README `deploy_root` wording).

---

### Fix round 1 (recorded after execution)

The first sweep run surfaced four in-scope pointers no brief named; they were fixed in a third
commit after review (spec section 5 covers them: playbook text and "the cc-processor READMEs"):
- `services/cc-dns-axfr/ansible/roles/cc_dns_axfr/tasks/main.yml:19` message
  `Deploy corpscout/commoncrawl/deploy/cc_dns_scan first` → `Deploy corpscout/services/cc-dns-scan/ansible first`.
- `services/cc-processor/cc-enrich-worker/README.md:252` `From \`commoncrawl/cc-processor/\`:` → `From \`services/cc-processor/\`:`.
- `services/cc-processor/cc-warc-index-builder/README.md:38` and `:52` `cd corpscout/commoncrawl/cc-processor…` → `cd corpscout/services/cc-processor…`.
The sweep filter above was widened at the same time (deploy_root-built host paths, S3 bucket literals).
