# ESEF domain context and relationship explanations

Domain extraction keeps every occurrence in `esef_domains.evidence_json`. The
existing short `surrounding_text` remains compatible with website verification.
An additive `source_context` object stores a version, local text, containing
section/page text, heading and table context, a locator, truncation flags and a
reading-order warning. It is source text, not a relationship classification.

Migrations 000423 and 000424 own an append-only `esef_domain_relationship_analysis` table.
Each attempt retains the exact model input, prompt hash, model settings, raw
response, validation outcome and usage. Results contain free-text English
statements with exact quotations tied to source occurrences; there is no business
relationship taxonomy. An unexplained mention remains an observation. Neither a
statement nor a name match establishes legal ownership or a verified counterparty.

`esef_domain_relationships` analyzes the raw ESEF observations before the Swedish
website-candidate filters. It does not update company websites or human decisions.
Publication is a view over successful analyses that still match the current
evidence, document package and reporting identity. Sweden is resolved using the
existing register-verified LEI map. No LLM calls occur in domain publication.

Rollout order: validate/apply migration; deploy extractor; re-extract selected
archived documents; inspect context; run bounded relationship analysis; review
quotations and statements; enable the company-page presentation; expand in batches.
The domain extractor's version controls re-extraction independently of financial
parsing. Do not enable a corpus-wide version backfill before the context pilot.

## Running a bounded analysis

Materialize `esef_domains_clickhouse` through `esef_domains_job` with explicit
`source_document_ids` first. This reads the archived package; it does not re-run
financial parsing. Inspect `evidence_json[*].source_context` before analysis.

Run `esef_domain_relationships_job` with this configuration (omit `execute: true`
for a selection-only preview):

```yaml
ops:
  esef_domain_relationships:
    config:
      execute: true
      source_document_ids:
        - 549300ZNNKE1UFGNNE47-2024-12-31-ESEF-SE-0
      max_domains: 25
```

The default model is the existing DeepSeek profile, with thinking enabled for this
context interpretation step and a 12,000-token output limit. At most `max_domains` are
processed, one at a time. There is no automatic relationship-analysis sensor.
Re-running skips successes with the same evidence, issuer identity, period,
prompt, model and semantic model settings. Failed attempts are retained and may
be retried; the run fails visibly after persisting them. Context over the configured
size cap is not silently shortened: it is recorded as `context_limit` for review.

The current publication views exclude interpretations after their source evidence
or reporting identity changes. A failed retry does not hide a still-current
successful interpretation. UI queries use explicit, stable view column names.

The company Domains page separates active company websites, source-grounded
relationship descriptions and website-candidate review history. Each description
has its report period, exact quotations, source locations, surrounding context
and original report link. Website assessments and relationship interpretations
remain independent; a rejected company website can still be a meaningful mention.

## Deployed pilot: 18 September 2026

- ClickHouse migrations 000423 and 000424 are applied.
- Öresund's 2024 archived report was re-extracted in Dagster run
  `b9b9d305-cab3-4ee1-894c-81eb250fb3ae`: 14 domains retained with extended context.
- Relationship analysis completed successfully in run
  `e1ee027f-c4eb-4677-8ca9-8da17001f357`, using prompt v3 and the 12,000-token
  output limit. All 14 published domains reference that successful run.
- An unchanged pending-work scan returned zero domains, confirming reuse of
  those successful results. Invalid quotations and earlier output-limit failures
  remain in the attempt history and are excluded from publication.
- The original domain-extraction sensor was restored to RUNNING; its normal
  batches are backfilling source context. Bulk relationship analysis remains a
  separate, bounded operation with no automatic analysis sensor.
- The company page was checked in the browser, including website separation,
  candidate history, report quotations, original report links and expandable
  source context.

Exact quotations are checked against saved source text. The prose remains an LLM
interpretation, not independent verification. In particular, text extracted from
multiple columns can leave attribution ambiguous. Related names are preserved
without inventing registry identifiers or forcing counterparty matches. Website
crawl evidence uses the separate [website relationship adapter](../website_relationships/design.md),
sharing quotation validation and the company-page presentation.
