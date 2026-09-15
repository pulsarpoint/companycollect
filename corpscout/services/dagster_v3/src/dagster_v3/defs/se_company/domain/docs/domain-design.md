# Swedish company domains

Domains follow the same entity model as company information, finance, addresses and
people. Grain: a Swedish company and registrable domain, with multiple independent
source observations and multiple domains allowed per company. These jobs read ingested
sources; neither operation crawls or downloads websites.

## Storage

Migration 000408 owns the tables:

- `se_company_domain_suggestion`: current source observation by company/source/slot;
  typed claims plus source record, evidence, stable slot and removal tombstones.
- `se_company_domain`: current folded company/domain association, primary choice,
  provenance, verification and reviewer status, activity and fold metadata.
- `se_company_domain_history`: changed output images, changed fields, change kind and
  Dagster run ID. Unchanged folds do not create history.
- `se_company_domain_precedence`: global, company and domain-specific field/source ranks.
- `se_company_domain_rule`: current reviewer decision per company/domain, with reversible
  release rows. Reviewer decisions take priority over machine claims and LLM output.
- `se_company_domain_verification`: append-only model attempts indexed by company/domain/input hash,
  including exact input JSON and prompt, evidence/prompt/model hashes, verdict, reason,
  usage, errors and run ID. The latest attempt for the current input is the cache entry.

The existing `company_domains` and `company_domain_current` become serving projections
updated by the existing serving refresh. Backoffice readers use `company_domains_resolved`,
a live view of the entity with the newest canonical review rules applied.
Existing domain reviews and published rows are imported before cutover. Reviews continue
through their current backoffice route and are recorded in the entity rule table.

LLM responses are written directly to ClickHouse with acknowledged inserts. There is no
S3 buffer or separate response journal. ClickHouse must be available before processing
starts; each attempt is persisted before the next model request. A database read or write
failure stops the verification asset immediately and prevents downstream publication in
that run. Previously saved successful answers remain reusable. If a response was received
but its insert failed, retrying may require another model request for that association.
Keep `wait_for_async_insert=1` when asynchronous inserts are enabled so a successful insert
acknowledges the storage flush, not just acceptance into an in-memory buffer.

## Sources and precedence

Initial contributors are Wikidata official websites linked through Swedish identifiers or
verified company LEIs, ESEF filing domain evidence, and Common Crawl identity matches.
Each extractor owns its source rows. A source withdrawal writes a tombstone; it cannot
remove another source's evidence. Source sync compares stable content, excluding routine
run IDs and extraction timestamps, so rebuilding unchanged source tables is a no-op.

Precedence is per field (`website`, `association`, `primary`): reviewer 20000,
ESEF 900, Wikidata 800, Common Crawl 600. A high rank does not turn a weak mention into
proof. Company/domain overrides take precedence over company-wide and global ranks.
A company can have many connected domains; primary selection is deterministic and picks
at most one active domain. Reviewer primary decisions lead, followed by positive primary
claims, source precedence, confidence and domain name. No uniqueness across companies:
a legitimate group domain may be shared.

## Verification

The default optional LLM scope is uncertain or conflicting associations. It evaluates
supplied company identity and source evidence, not an imagined live website visit.
Auditor/social/third-party-only filing references are excluded by the ESEF extractor.
The separate `se_company_domain_verification` asset reads source suggestions and writes
`se_company_domain_verification`. A verdict is `connected`, `not_connected` or `uncertain`,
with supporting evidence IDs and a `confidence` score from 0 to 1. This is confidence in
the verdict, not the probability of connection: `not_connected` at 0.98 is a confident
rejection. `se_company_domain_publish` then reads those saved answers and never calls a model.
A reviewer decision is authoritative. Positive and negative model verdicts require high
confidence; an uncertain outcome stays available for review and is not asserted as a
confirmed company website.

Input fingerprints cover stable company identity and sorted source evidence. Effective
prompt text and model settings have separate hashes; changing either reruns verification,
while prompt naming/revision or normal extraction timestamps alone do not. Results are
reused only for their exact input hash. With verification disabled, the latest answer for the current evidence can be reused
without choosing a new model or prompt. With `changed_only=true`, successful answers are
reused and failed attempts (including invalid answers) can retry. Failed or partial
verification must not silently retain a stale positive answer.

Complete JSON in Markdown fences or an exact `answer` wrapper is unwrapped before strict
verdict and supplied-evidence validation. Saved invalid responses are also revalidated
when read, without another model request. Incomplete JSON and unsupported citations remain
invalid. The attempt table keeps its original validation outcome and token usage; recovered
verdicts appear in the published domain and its history. Materialization metadata counts
these as `verification_recovered`, so recovery never looks like an additional paid attempt.

## Operations

Sweden → Processing gains Domains with the same two global actions:

1. **Sync inputs**: sync the three source suggestion sets and precedence.
2. **Full processing**: sync → `se_company_domain_verification` → `se_company_domain_publish`.

Full processing has model and prompt configuration when verification is enabled, defaults
to changed input only, and verifies every eligible association across all companies.
There is no default run-wide request cutoff. Dagster owns run history.
Publication and verification share the `se_company_domain_fold` concurrency pool. Global precedence and
fold-version changes participate in change detection; an ordinary full-processing run
applies them without requiring a hidden maintenance action.

The launcher passes the same verification profile to both steps. The verification asset
uses it for model requests; the publication asset only hashes it to select exact cached
answers. This prevents an old prompt/model answer from being published when the new
verification run reaches its cap. With verification disabled, the verification asset is
a no-op and publication may reuse the latest answer for the current evidence.

A provider or invalid-response failure fails the verification asset after saving all
completed attempts and prevents downstream publication. A retry reuses successful
attempts and retries HTTP failures and invalid responses. Set `retry_failed_only=true`
on the verification asset to preserve successes and skip never-processed associations,
even if `changed_only=false`. This option matches the exact evidence, prompt and model
settings fingerprint. Normal processing (`retry_failed_only=false`) includes new inputs.
Removing the former backoffice token cap also matches saved attempts under that cap when
all other settings and evidence are identical. This preserves successes and lets failed-only
retries resume those failures without the cap. The latest matching attempt still wins, so
a newer failure cannot resurrect an older success. Other settings changes produce a new
input that a failed-only retry skips. No schema augmentation is required.

The backoffice omits `max_tokens`, and domain verification defaults it to `None` instead
of inheriting the shared description profile's token limit. The API request omits this
parameter, leaving output limits to the provider. Dagster can still pass an explicit
per-request limit when requested. The optional Dagster `max_llm_calls` setting also defaults
to `None`; backoffice does not supply it or expose a request-limit field. A manually bounded
Dagster run can still set it explicitly. Each attempt checks one company–domain pair;
successful cached answers are reused, and page size only bounds memory and database reads.
Invalid-response errors report the finish reason and token usage, and distinguish an
exhausted output budget from an empty or malformed answer. Verification logs each failed
association and reports progress between calls.

Publication logs announce schema checks, scope preparation, every page's company ID
range, input reads, verification lookup, folding and separate history/main writes.
Each stage reports elapsed time; folding emits progress at least every 10 seconds
between companies. Batch logs include at most five changed-domain examples and totals
for active, withdrawn, rejected and unverified rows. A batch is logged as complete only
after its writes succeed. The final materialization includes completed pages, elapsed
milliseconds and the same outcome counters. Logs omit prompts and raw source evidence.
