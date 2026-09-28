# dns_detect slice 2 Implementation Plan: SPF, DKIM, DMARC, TXT and aligned rule kinds

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve TXT, SPF, DKIM and DMARC records per record, the same way slice 1 resolves NS/SOA/MX/CNAME. Also make provider-recon's definitions validator and the resolver's knowledge accept exactly the same rule kinds, so a definition that validates can never make the resolver refuse all knowledge.

**Architecture:**
- **One list of kinds.** `model.DNSRuleKinds` is the single list of `(record_type, match_field)` pairs. The validator checks against it, and `knowledge.Kind` becomes an alias of `model.DNSRuleKind`.
- **Routing.** New routes in `resolve.Route` send records to four new analyzers:
  - apex TXT: `SPF` for `v=spf1…`, otherwise `TXT`;
  - `_name` TXT: `TXT`, matched by name;
  - `<selector>._domainkey` CNAME and TXT: `DKIM`;
  - `_dmarc` TXT: `DMARC`.
- **Shared helpers:** `LabelRule` (rules only, no fallback) joins slice 1's `LabelHost`, and `txtValue` joins quoted TXT strings.
- **Definitions** in provider-recon are rewritten to the new kinds, then redeployed.

**Tech Stack:** Go 1.25 in `services/provider_recon`, stdlib testing.

**Spec:** `docs/superpowers/specs/2026-09-28-dns-detect-service-design.md` (revision 2): "Rule kinds", "Routing and analyzers" (the slice-2 rows). Owner ruling 2026-09-28: ordinary table tests per record type, no pipeline or harness.

## Global Constraints

- **Allowed rule kinds** (both validator and knowledge): NS/target, MX/target, CNAME/target, TXT/value, TXT/name, SPF/include, DKIM/selector, DKIM/target, DMARC/report. Everything else is refused.
- **Matcher `exists`** is refused for every DNS rule, in the validator and in knowledge. HTTP rules keep it.
- **New service type:** `dmarc_reporting`.
- **No DNS lookups.** SPF is only what the record says. `ip4:`/`ip6:` are slice 3 and give nothing yet.
- **Output shape:** everything is per record, with the record's window, deterministic output, and `[]` rather than null. Identical result rows from one record are de-duplicated.
- **Git:** commit by explicit path from the `corpscout` root, in a worktree under `companycollect/.worktrees/`. Each commit ends with `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.
- **Checks** (in `services/provider_recon`): `go test -race ./...`, `go vet ./...`, `test -z "$(gofmt -l .)"`, and `go run ./cmd/provider-recon validate` against `definitions/`.

## Behaviour per record type (the test tables)

**TXT value parsing** (`txtValue`):
- `"a" "b"` → `ab`
- `"v=spf1 include:x" " ~all"` → `v=spf1 include:x ~all`
- `"say \"hi\""` → `say "hi"`
- `plain` → `plain`
- an empty value → empty

**TXT analyzer, apex (non-SPF):**
- Rules only (`TXT/value`), one row per service type of the matched service, with the unquoted value as subject.
- No match → nothing. There is no provider-key fallback for TXT.

**TXT analyzer, `_name` TXT** (the name is under the domain and its first label starts with `_`, other than `_dmarc`/`_domainkey`):
- Rules only (`TXT/name`), matched against the name part below the domain, e.g. `_amazonses` or `_github-challenge-acme`.

**SPF analyzer** (apex TXT whose unquoted value starts with `v=spf1`, case-insensitive):
- `include:h` / `redirect=h`: `LabelHost(SPF/include, email_sending, h)`, so a rule wins, then self-hosted, a provider key, or unmapped.
- `+`, `-`, `~` and `?` qualifiers are stripped first.
- `a` / `mx`:
  - bare, or with only `/cidr`: `email_sending`, self-hosted, with the domain as subject;
  - `a:h` / `mx:h`: `LabelHost(h)`.
- A host containing a macro (`%{`) is not labelled; it gives the finding `spf_macro`.
- `ip4:`, `ip6:`, `ptr`, `exists:` and `all` give nothing (IPs arrive in slice 3).
- Static DNS lookups (`include`, `a`, `mx`, `ptr`, `exists`, `redirect`) above 10 give the finding `spf_lookup_budget_exceeded` with the count.
- Duplicate rows (e.g. bare `a` and `mx`) are de-duplicated.

**DKIM analyzer** (`<selector>._domainkey.<domain>`, CNAME or TXT):
- The selector is the name before `._domainkey.`.
- A `DKIM/selector` rule match gives rows with the selector as subject.
- CNAME: the target also goes through `LabelHost(DKIM/target, email_sending)`.
- TXT: only the selector rules apply.

**DMARC analyzer** (`_dmarc.<domain>` TXT):
- The value must start with `v=DMARC1`, otherwise the finding `dmarc_invalid`.
- The `rua` and `ruf` tags are split on commas.
- For each `mailto:` URI, the `!size` suffix is dropped and the host is taken after `@`. A mailbox inside the domain itself gives nothing; any other host goes through `LabelHost(DMARC/report, dmarc_reporting)`.

## Definition rewrites (provider-recon YAML)

| Provider service | Before | After |
|---|---|---|
| `google.workspace-sending` | TXT/value contains `include:_spf.google.com` | SPF/include suffix `_spf.google.com`; DKIM/selector exact `google` (confidence 0.7) |
| `microsoft.365-sending` | TXT/value contains `include:spf.protection.outlook.com`; CNAME/target suffix `onmicrosoft.com` | SPF/include suffix `spf.protection.outlook.com`; DKIM/target suffix `onmicrosoft.com`; DKIM/selector exact `selector1` and `selector2` (confidence 0.6) |
| `aws.ses` | TXT/value contains `include:amazonses.com`; CNAME/target suffix `dkim.amazonses.com`; TXT/name prefix `_amazonses.` | SPF/include suffix `amazonses.com`; DKIM/target suffix `dkim.amazonses.com`; TXT/name exact `_amazonses` (confidence 0.9) |

---

### Task 1: One list of rule kinds; validator and knowledge aligned

**Files:**
- Modify: `internal/model/document.go`, `internal/definitions/validate.go`, `internal/definitions/definitions_test.go`, `internal/detect/knowledge/knowledge.go`, `internal/detect/knowledge/knowledge_test.go`

- [ ] **Step 1: Failing tests.**
  - Definitions:
    - `A/value` is refused with `unsupported dns rule kind A/value`;
    - `TXT/all` is refused;
    - an `exists` DNS rule is refused;
    - `SPF/include`, `DKIM/selector`, `DKIM/target` and `DMARC/report` rules validate;
    - the service type `dmarc_reporting` validates.

    The existing "bad record type" case expects the new message.
  - Knowledge: an `exists` DNS rule is refused.
- [ ] **Step 2: Implement.**
  - `model.DNSRuleKind` and `model.DNSRuleKinds`, plus `model.IsDNSRuleKind(recordType, matchField)`.
  - `dmarc_reporting` added to the model's service types.
  - The validator checks the pair and refuses `exists`.
  - `knowledge.Kind = model.DNSRuleKind`; the knowledge vars use it, and knowledge refuses `exists`.
- [ ] **Step 3:** Run the checks; the definitions validate.
- [ ] **Step 4:** Commit: `feat(provider_recon): one list of DNS rule kinds shared by the validator and the resolver`.

### Task 2: TXT values, the TXT analyzer and routing

**Files:**
- Modify: `internal/detect/resolve/label.go`, `analyzers.go`, `resolve.go`, `resolve_test.go`, `fake_test.go`
- Create: `internal/detect/resolve/txt.go`

- [ ] **Step 1: Failing tests:**
  - the `txtValue` table;
  - apex TXT with a rule gives a row, and without one gives nothing;
  - `_amazonses` TXT is matched by name;
  - `_acme-challenge.www` TXT with no rule gives nothing;
  - apex TXT `v=spf1…` is routed to SPF, not TXT;
  - the TXT inside `_dmarc`/`_domainkey` is not routed to TXT.
- [ ] **Step 2: Implement** `txtValue`, `LabelRule`, the `TXT` analyzer and the routes.
- [ ] **Step 3:** Run the checks.
- [ ] **Step 4:** Commit: `feat(provider_recon): detect TXT verification records by value and by name`.

### Task 3: SPF analyzer

**Files:** Create `internal/detect/resolve/spf.go`; modify `resolve_test.go` and `resolve.go` (routing and de-duplication).

- [ ] **Step 1: Failing tests** covering the whole SPF table above:
  - a real split string: `"v=spf1 include:_spf.google.com" " include:sendgrid.net ~all"` → google (rule) plus `sendgrid.net` (unmapped);
  - qualifiers;
  - `redirect=`;
  - bare `a` and `mx` → one self-hosted row;
  - `a:mail.other.net`;
  - a macro → finding;
  - `ip4` → nothing;
  - 11 includes → the budget finding;
  - uppercase `V=SPF1`.
- [ ] **Step 2: Implement.**
- [ ] **Step 3:** Run the checks.
- [ ] **Step 4:** Commit: `feat(provider_recon): SPF analyzer (include/redirect/a/mx, macros and lookup budget as findings)`.

### Task 4: DKIM analyzer

**Files:** Create `internal/detect/resolve/dkim.go`; modify the tests and routing.

- [ ] **Step 1: Failing tests:**
  - `selector1._domainkey` CNAME to `selector1-x._domainkey.t.onmicrosoft.com` → a target rule row plus a selector rule row;
  - `k1._domainkey` CNAME `dkim.mcsv.net.` → unmapped `mcsv.net`;
  - `google._domainkey` TXT `v=DKIM1; …` → the selector rule;
  - a TXT with an unknown selector → nothing;
  - a DKIM name that isn't under this domain → not routed.
- [ ] **Step 2: Implement.**
- [ ] **Step 3:** Run the checks.
- [ ] **Step 4:** Commit: `feat(provider_recon): DKIM analyzer (selector and CNAME target)`.

### Task 5: DMARC analyzer

**Files:** Create `internal/detect/resolve/dmarc.go`; modify the tests and routing.

- [ ] **Step 1: Failing tests:**
  - `v=DMARC1; p=reject; rua=mailto:a@rua.dmarcian.com,mailto:dmarc@example.se; ruf=mailto:x@ruf.agari.com!10m` → dmarcian (rule) plus `agari.com` (unmapped); the own-domain mailbox gives nothing;
  - `p=none` without rua → nothing;
  - a value that isn't DMARC → the finding `dmarc_invalid`;
  - `_dmarc.sub.example.se` → not routed.
- [ ] **Step 2: Implement.**
- [ ] **Step 3:** Run the checks.
- [ ] **Step 4:** Commit: `feat(provider_recon): DMARC analyzer (rua/ruf reporting providers)`.

### Task 6: Rewrite the definitions, redeploy, and check on real records

- [ ] **Step 1:** Rewrite `google.yaml`, `microsoft.yaml` and `aws.yaml` per the table. Run `go run ./cmd/provider-recon validate` (expected: OK), plus the checks.
- [ ] **Step 2:** Update the README routing section and the spec's slice-2 routing rows if the code differs.
- [ ] **Step 3:** Commit: `feat(provider_recon): SPF, DKIM and TXT-name rules in the new rule kinds`.
- [ ] **Step 4:** Redeploy provider-recon (Ansible, as in slices 0–1) and trigger a collect. Expected: `succeeded`, 0 issues. The Google, Microsoft and AWS documents then carry SPF/DKIM kinds.
- [ ] **Step 5: Real-record check** (not committed). Resolve all records of spotify.com, volvo.com and loopia.se (every type) against the 37 fresh documents. Expected:
  - spotify.com shows `email_sending google` (SPF include and TXT), `email_sending sendgrid.net` and `mcsv.net` (DKIM);
  - volvo.com shows `email_sending microsoft` if it publishes Microsoft SPF/DKIM;
  - DMARC reporting providers are listed wherever `_dmarc` has `rua`.

  Record the unexpected rows in the ledger for the owner.
