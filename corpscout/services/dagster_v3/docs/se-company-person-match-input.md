# Current People matching input

Migration `000407_corpscout_se_company_person_match_input` adds the current-input table
and six provenance columns to `se_company_person_match_state`. Apply the migration
before deploying the updated Dagster code.

## Writers and lineage

All four source extractors (Bolagsverket, ESEF, Wikidata and Ratsit) continue writing
suggestions. Their shared `se_company_person_normalize` step writes normalized rows,
then reads each affected company's **complete** normalized set and writes its input
snapshot. A changed-source page alone is never used as the full input.

The targeted `se_company_person_fold_companies` path calls the same normalizer, so it
maintains snapshots too. Both writers and the `se_company_person_match_input` repair
asset share `se_company_person_normalize`'s concurrency pool (limit 1). The matcher
retains its own single-run pool to prevent concurrent duplicate calls.

The refresh, publish and extract jobs include the input asset. The matcher depends on
it and reads current snapshots rather than rebuilding candidates from normalized rows.
Publish-only jobs still make no matcher LLM calls.

## Stored columns

`se_company_person_match_input FINAL` contains one current row per company:

| Column | Meaning |
| --- | --- |
| `data_hash` | SHA-256 of the canonical model-visible candidate list |
| `bindings_hash` | SHA-256 of the candidates' current normalized IDs and member IDs |
| `input_snapshot` | Versioned JSON candidates, including the exact member mapping |
| `eligible` | At least two machine sources with valid people |
| `hash_version` | Snapshot/candidate algorithm version; increment when rebuilding is needed |
| `normalized_at` | Latest normalized-row timestamp included in this snapshot |
| `computed_at` | Snapshot read-start time; replacement version |

Candidates use the matcher's shared builder. Their model order depends on content,
not source observation IDs. Names, normalized tokens, birth years, ages, external-role
flags and the capped role list affect `data_hash`. Reviewer entries and metadata the
model never sees do not. New record IDs or duplicate filings with identical information
change only `bindings_hash`.

Removed people are represented by source tombstones, which normalize to `no_person`.
The snapshot is rebuilt even if the company becomes ineligible or empty; its previous
eligible row cannot remain current.

## Repair and initial backfill

A normalized write and its snapshot insert are separate ClickHouse statements. The
normalization scope includes companies with missing snapshots, differing normalized
watermarks or an older hash version. An interruption after the normalized insert is
therefore repaired on the next run, even when no raw rows need normalization anymore.

Materialize `se_company_person_match_input` to backfill existing normalized data without
source downloads or model calls. Default `changed_only=true` repairs gaps; `false`
rebuilds every snapshot. `company_ids` and `page_size` support a bounded repair. Reads
use `FINAL`, and snapshots are written in batches.

For maintenance in the deployed Dagster UI, open the `se_company_person_match_input`
asset and materialize **only that asset** with this launchpad config:

```yaml
ops:
  se_company_person_match_input:
    config:
      changed_only: true
      page_size: 5000
```

After the initial backfill, zero refreshed companies is a successful result when every
snapshot is current. Use `changed_only: false` on this asset for an intentional full
snapshot rebuild; it still makes no model calls. To include pending raw suggestions,
materialize `se_company_person_normalize` before the input asset.

For regular operation, the backoffice offers two global actions:

- **Sync inputs** (`se_company_person_sync_job`) reads the four ingested source tables,
  extracts suggestions, normalizes them and maintains input snapshots and hashes.
  It makes no LLM calls and does not publish people.
- **Full processing** (`se_company_person_refresh_job`) runs the same sync, then matches
  with the selected LLM profile and saved People prompt, and folds/publishes results.
  Keep **Only match new or changed input** checked to reuse unchanged successful results.

Both jobs include the input asset; an additional manual snapshot run is unnecessary.
Neither action is scoped by backoffice company filters. The standalone input asset
remains available for maintenance without rerunning source extraction.

## Matching and reuse

New match-state rows store `data_hash`, `bindings_hash`, `prompt_hash`, `model_hash`,
`input_snapshot` and `config_snapshot` alongside the existing result and usage columns.
The config snapshot contains the exact system prompt and effective provider, endpoint,
model, temperature, answer-token budget, response format and provider options. It contains
no API-key value. Prompt IDs, names and SQLite revisions are not cache inputs.

With `changed_only=true`:

- Equal data and config hashes reuse success without moving `matched_at`.
- A binding-only change replays the saved response against the current members, writes
  current pairs first, then the matching state. `rebound` counts these companies and
  model-call/token counters remain zero. Stored raw response and usage remain those of
  the reused answer. `matched_at` is the pair activation time, including a replay;
  `source_run_id` identifies the pipeline run that activated it, not necessarily a new call.
- Actual data, prompt or effective model changes trigger processing. Operational options
  such as concurrency, timeout or API-key environment-variable name do not invalidate results.
- Transient failures retry; other failures remain sticky for the same data/config.
- `changed_only=false` forces a new call for every eligible company within the configured cap.

Pair activation still joins `company_id`, `input_hash` and `matched_at`; a hash check alone
never rewrites the state timestamp. This preserves fold visibility and prevents old
pairs omitted from a replacement answer from leaking back into the result.

Pre-migration state has empty provenance fields, meaning **unknown**. Legacy hashes
remain recognizable, including saved prompts under their original revision, so deployment
does not automatically send all historical successes through the model. Those rows gain
provenance on their next actual attempt. Their missing historical input cannot establish
safe binding-only replay; a changed legacy binding therefore needs a new call.

This is current input versus latest activated result, not an append-only attempt history.
