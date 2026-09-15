# People actions and asset group analysis

Sweden → Processing (`/admin/se/processing`) offers two global operations in
the People workflow, alongside live Dagster status and recent history. Table filters
and company selections are not part of either launch request. Each Dagster asset
uses its existing change detection across all Swedish companies, with an option to
force LLM matching during full processing. Each operation opens its configuration dialog.

| Operation | Dagster job | Steps |
| --- | --- | --- |
| Sync inputs | `se_company_person_sync_job` | Four suggestion extractors → normalize → input snapshots and hashes |
| Full processing | `se_company_person_refresh_job` | Same sync → LLM match → fold and publish |

Both operations read already ingested Bolagsverket, ESEF, Wikidata and Ratsit tables.
They do not download new registry data or crawl source websites. Sync requires no
LLM profile or prompt and never selects the matching or publishing assets.
These are the only named global People jobs. The former publish-only and
extract-and-match jobs, the stopped weekly schedule and the standalone bucket-fold
action have been removed.

## Prompts and model profiles

Settings → People prompts (`/admin/settings/people-prompts`) stores named prompts
in the `people_prompt` table in the same SQLite settings database as LLM profiles.
The current built-in matcher prompt is seeded once. The default database path is
`data/settings/settings.sqlite`; `BACKOFFICE_SETTINGS_DATABASE_PATH` overrides it.
No ClickHouse migration is needed for these settings.

Editing a prompt increments its revision. Stale editor submissions and launch
forms that previewed an older revision are rejected. Full processing chooses
an existing LLM profile and a saved prompt. The server reads them from SQLite and
sends their values to `ops.se_company_person_match.config`:

```json
{
  "provider": "openrouter",
  "model": "chosen/model",
  "base_url": "https://openrouter.ai/api/v1",
  "api_key_environment_variable": "WORKER_LLM_KEY",
  "prompt_version": "people:<prompt-id>:r<revision>",
  "system_prompt": "<the saved matching instructions>",
  "temperature": 0,
  "concurrency": 1,
  "changed_only": true
}
```

The prompt text is an immutable snapshot in the Dagster run config. Subsequent
SQLite edits do not affect queued runs or retries. Only the API key environment
variable's name travels; the Dagster worker resolves its value locally.

Matching compares stored candidate data, prompt text and effective model settings
(provider, model, endpoint and sampling settings). By default, unchanged inputs
reuse successful match state; changing prompt text or effective model settings
requests new matches. Prompt IDs, names and SQLite revisions are recorded for
traceability but do not invalidate a result by themselves. Changes only to normalized
row bindings replay the saved answer without an LLM call. See the Dagster
[input hash documentation](../../dagster_v3/docs/se-company-person-match-input.md)
for snapshot maintenance and legacy-state handling.

The Full processing action's **Only match new or changed input** checkbox is checked by
default and sends `changed_only=true` to the matcher. Unchecking it sends `false`
and calls the LLM again for all eligible companies, including unchanged input.
The choice applies only to matching; normalization and publication still use
change detection. Sync inputs does not offer this choice or call the LLM.

## How the group works

1. The four extractors compare each source's current per-company state with its
   stored suggestions. They write changed observations and tombstones for removed
   source slots. These are current source observations, not an append-only ledger.
2. Normalization converts suggestions to typed name tokens, roles and normalized
   IDs. It revisits changed suggestion IDs and normalizer versions. Only `ok`
   parsed rows participate in matching and publication. Normalization also updates
   each affected company's matching input snapshot and hashes. The input asset
   repairs any remaining missing or stale snapshots; Sync inputs stops here.
3. Matching reads the stored machine-source candidates and calls the LLM for companies
   with at least two sources. Candidate IDs in the prompt are compact `c0`, `c1`,
   etc. The existing parser expects a JSON object containing a `pairs` array.
4. Folding combines deterministic name rules, LLM pairs at confidence ≥ 0.8,
   birth-year constraints, reviewer decisions and source precedence. It writes
   history before updating `se_company_person`.

The new nonpartitioned `se_company_person_publish` asset visits all 64 existing
fold buckets sequentially in the fold concurrency pool. It depends on both the
normalizer and matcher, so Full processing publishes only after matching completes.
The targeted per-company fold remains available to apply reviewer corrections,
including Activate and Fold now on the company's People page. Input-snapshot repair
and precedence export remain internal maintenance assets.

Pairs are admitted only when both their input hash and answer timestamp match
the current successful state row. This prevents a forced re-match that returns
fewer or no pairs from leaving omitted pairs active. The backoffice's possible
matches query uses the same condition.

## Operational limits found during analysis

- Single-source companies are normalized and published without LLM matching.
  More than 400 candidates is an explicit match error; candidates are not silently
  truncated. Invalid answers are sticky until input changes; transient failures
  retry. A page with mostly failed calls trips the existing circuit breaker.
- The LLM candidate grouping uses source and name tokens. Same-name people with
  different birth years can share a candidate; the fold retains its birth-year
  separation checks, but this remains a limitation of candidate construction.
- Published role rows and the company list's presence flags use independently
  refreshed ClickHouse views. A successful publish does not synchronously refresh
  those views.
- The two new jobs and matcher config must be deployed/reloaded in the connected
  Dagster code location before backoffice can launch them. Worker hosts need the
  environment variable named by the chosen LLM profile. SQLite remains local to
  backoffice and is never read by Dagster.

Verification covers SQLite persistence and stale edits, exact launch payloads,
absence of company filters and secrets, prompt/model cache changes, job selection
and config validation, and ClickHouse behavior when a new answer drops old pairs.
No live LLM run was launched during implementation.
