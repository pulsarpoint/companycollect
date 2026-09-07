# Technology discovery and administrator review

This change extends the alias-only flow with catalog-assisted extraction and an
administrator review queue in the Corpscout backoffice.

1. Sync the ClickHouse catalog and accepted aliases to one validated local snapshot.
   Pin its content hash for a crawl. Search runs locally; credentials and the complete
   catalog are not sent to the model.
2. Provide `search_technologies` during extraction. Preserve the observed name and
   evidence separately from the canonical name. Exact and unique normalized matches
   take precedence over accepted aliases; search similarity only suggests candidates.
3. Emit `matched` or `proposed` (pending administrator review). A proposal requires a
   successful local search and structured proposed metadata. Failed searches are
   processing errors; ambiguous named technologies go to review. Unknown metadata
   stays unknown for administrator review.
4. Submit proposals with stable IDs, original observations, source evidence and the
   catalog version searched. The backend rechecks the current catalog, validates the
   payload and records submissions in `corpscout.new_tech`. Retries reuse IDs.
5. An administrator can approve a new canonical entry, map to an existing entry, or
   reject. Decisions are an append-only audit ledger. A mapping is attached to the
   proposal; a global alias requires a separate explicit reviewer choice.
6. Approved entries and aliases feed the catalog asset as reviewed input, so a catalog
   refresh retains them. Publication remains distinct from approval. Original evidence
   is not rewritten when a proposal is resolved.

The catalog currently uses the exact technology name as its identity. Company
attribution remains separate from the page host, including external job boards.
Neither a catalog match nor administrator approval turns a job requirement into
confirmed company-wide technology usage.

## Running the crawler

Install the package with `python -m pip install -e ./company_research` from
`codex-sd-examples`. Configure these values in the process environment or one explicitly
selected env file:

- `OPENROUTER_API_KEY` for extraction and link assessment.
- `CLICKHOUSE_HOST`, `CLICKHOUSE_NATIVE_PORT`, `CLICKHOUSE_USER`,
  `CLICKHOUSE_PASSWORD`, and optional `CLICKHOUSE_SECURE` for catalog sync. Use an
  account that can read the catalog, aliases and publish log; the client enables
  ClickHouse's read-only setting.
- `TECHNOLOGY_SUBMISSION_TOKEN` for explicit backend submission. Configure the same
  secret, at least 24 characters, on the backoffice server. It is never sent to the LLM.

```sh
company-research https://example.com --env-file /path/to/crawler.env \
  --technology-catalog data/technology-catalog.json \
  --output-dir data/example > example.json

company-research-submit data/example/result.json \
  --backend-url https://internal-backoffice.example.com \
  --env-file /path/to/crawler.env
```

Sync verifies that the catalog and aliases belong to a completed publication before
atomically replacing the cache. The crawl pins that snapshot in its output directory.
With `--offline-catalog`, an explicit `--technology-catalog` path is required and no
refresh is attempted. An unavailable or inconsistent catalog prevents the CLI from
starting a crawl. Python callers pass a validated `TechnologyCatalog` explicitly.

Local search returns at most ten candidates per query and preserves punctuation.
`Git`, `git` and `GIT` resolve to the same uniquely normalized canonical identity;
`C`, `C++` and `C#` remain distinct. Exact canonical identities take precedence when
the existing catalog contains a case collision. Similarity never automatically creates
an alias. A model-selected canonical identity must occur in the actual search results.
Full tool results stay in call artifacts; findings keep compact query/candidate traces.

## Submission and review

The backend endpoint is `POST /admin/api/technology-submissions`. It accepts submission
schema `1.0` generated from the current research schema `1.3`, with a maximum of 500 observations
and 8 MB per request. It validates the complete batch before inserting proposals.
The saved run ID and record ID make retransmission safe. Findings with a catalog
processing error remain in the research JSON and are skipped with a stderr message.

This endpoint stores **proposed technologies and their evidence**. Matched records are
validated and counted in its response; it does not yet persist all known company
technology observations. The broader observation pipeline described in the database
plan remains a separate step.

Open **Technologies → Review crawler proposals** in the existing internal backoffice
(`/admin/technology-proposals`). A reviewer can:

- Approve a new catalog entry after checking its canonical name, description, official
  website, existing category IDs, and SaaS/OSS attributes.
- Map the proposal to an existing canonical technology.
- Reject the proposal while retaining its submitted evidence.

Every decision requires a reviewer identity and note. This follows the existing
internal backoffice access model; the reviewer field is an audit assertion, not a new
authentication system. A global alias is a separate explicit choice. Case variants
do not need aliases. Pending means no review exists; there is no `unresolved` state.

`new_tech` retains one observation per `(proposal_id, run_id, record_id)`. Proposals
with the same normalized proposed name and website share a proposal ID. Review history
is append-only in `technology_proposal_reviews`; latest-review and mapping views expose
the current decision. Approval removes the item from the pending queue without
deleting evidence. The form detects stale reviews, and publication rejects conflicting
identities/aliases. ClickHouse does not provide a transactional compare-and-set for
simultaneous reviews, so conflicting concurrent decisions may require another review.

## Publishing reviewed technologies

Apply migrations `388` and `389` before deploying the updated backoffice and Dagster
asset. Run `technology_catalog_clickhouse` through the normal Dagster deployment to
publish approved entries and aliases. Approval and publication are separate operations.
The asset reloads accepted reviews on every refresh, so source catalog refreshes retain
administrator additions. It does not invent DNS fingerprints for LLM proposals.

A later rejection removes that review's contribution on the next publication; the
original proposal and previous reviews remain available. Historical observations keep
their original source and proposed identity, which downstream code can resolve through
`technology_proposal_mappings`.

Deployment status on 2026-09-07: migration 389 was applied to the configured live
ClickHouse and verified clean. The proposal and review tables are empty. The updated
backoffice and Dagster code has been validated locally; it has not been deployed, and
the live catalog has not been materialized with the new reviewed layer.

## Verification

Crawler checks from this directory:

```sh
COMPANY_RESEARCH_BROWSER_TEST=1 ../.venv/bin/python -m unittest discover -s tests -q
uvx ty check --python ../.venv src/company_research
```

The browser test uses real Crawl4AI against a local site and a controlled model HTTP
boundary. Backoffice Vitest tests consume a fixture generated by the Python crawler.
The Dagster `test_technology_aliases_clickhouse.py` integration test starts a disposable
ClickHouse, calls the actual backend submission/review code, and verifies that repeated
publication preserves approved technologies and aliases. It also checks that rejection
supersedes approval while keeping the audit history. The review form was exercised
through a browser against this disposable database.

These controlled tests do not establish live model accuracy. Paid live replay artifacts
are saved under `data/technology-proposals-live-*`; inspect their completeness, source
validation and catalog decisions before using them as benchmark results.

On 2026-09-07, a read-only sync retrieved 7,981 live catalog entries. A saved real
Kongsberg Software Developer page was submitted to DeepSeek with the local search
tool. The provider returned valid schema JSON inside a Markdown block despite the
strict-output request. The client now accepts one complete fenced JSON document and
records `json_wrapper_removed`, while preserving the raw response and applying all
normal schema/catalog/evidence checks. Malformed or multiple JSON blocks still fail.

Replaying those saved responses through the corrected client completed without schema
or catalog errors: 21 records, including 15 technology observations. Three observations
matched known identities (`C` twice under different statement types and `Git → git`);
twelve became proposals. Fourteen technology observations passed source checks and
one Windows API observation remained `needs_review`. The catalog lacks several common
non-web technologies, so many legitimate job-ad technologies enter the proposal queue.

Artifacts: `data/technology-proposals-live-20260907T135529Z/validated-replay/result.json`.
This was a replay of actual responses, not a fresh successful end-to-end API run.
Live attempts also encountered HTTP 429 and a timeout. The two responses used by the
successful replay reported $0.007023492; the attempt's successful calls including the
subsequent correction search totalled $0.008403444. The replay made no paid calls.
This single page is a functional check, not an accuracy
benchmark across sites.
