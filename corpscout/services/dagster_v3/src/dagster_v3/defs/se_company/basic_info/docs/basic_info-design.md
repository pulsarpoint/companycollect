# se_company.basic_info

The basic-info entity of the 2026-09-03 SE basic-info design
(`docs/superpowers/specs/2026-09-03-se-company-basic-info-design.md`).

| Module | Responsibility |
| --- | --- |
| `tables.py` | Table names and column tuples, pinned against migrations 000376-000379 |
| `precedence.py` | `BASIC_INFO_PRECEDENCE`: numbers per field per source; the reviewer is a source ranked 10000 |
| `fold.py` | `fold_basic_info`: pure, one company, highest precedence wins, ties to newest `observed_at` then smaller uid; no row without a register legal name |
| `batch.py` | Reads current suggestion rows (`FINAL`), folds in pages of 20,000, rewrites every folded company's main row plus one history row per change |
| `assets.py` | `se_company_basic_info_fold` (64 hash buckets, `multi_run(1)`, pool `se_company_basic_info_fold`), `se_company_basic_info_fold_companies` (targeted), `se_company_basic_info_precedence_clickhouse` |

`batch.py` writes the history row before the main row: the two statements are not one
transaction, so a failure between them costs a duplicate history row on retry rather than
a published main row with no first-publish history entry. `fold.py` treats an empty string
like NULL — a source never "says empty", so `''` never supplies a field and never wins —
and normalises a naive `observed_at` to UTC before comparing across suggestions, so ties
break consistently regardless of a source's tz-awareness.

Suggestion rows come from the slice-2 extractors (`se_basic_info_suggestions_<source>`) and,
for the `reviewer` source, from the backoffice (slice 3). NULL in a value column is "no
opinion". There is no content hash: an extractor writes a new row when the source's
current record has a newer `observed_at` than the current suggestion row, and the fold
rewrites the main row of every company it folds (so `folded_at` advances and the
changed-only selection converges) while adding a history row only where a value or
source changed.

Operating the fold: materialize one `bucket_NN` partition or launch a backfill of all 64
from the UI; `changed_only` (default true) skips companies folded after their newest
suggestion. `se_company_basic_info_fold_companies` takes `company_ids` and re-folds them
whatever their bucket. Nothing is scheduled. Resolved 2026-09-04: every folded company is
rewritten with a new main row, so `folded_at` advances on every fold and `changed_only`
converges instead of re-selecting an unchanged company forever; `page_size` (default
20,000) is the knob to lower if a run's per-page memory presses the host.

## Extractors (slice 2)

Each source extractor (`se_basic_info_suggestions_<source>`) writes one wide suggestion row
per company, reading its own register table and stamping `observed_at` from the record that
grounds the row:

- `scb` reads `se_scb_companies` FINAL: `legal_name`, `legal_form_code`, `status` (from
  `source_status_code`), `incorporation_date` (`registration_date`); `observed_at` is the
  register row's own `observed_at`.
- `bolagsverket` reads `se_bolagsverket_companies` FINAL joined to `text_translations`:
  `legal_name`, `legal_form_code`, `status` (active iff `deregistration_date` is NULL),
  `incorporation_date`, `description`/`description_language`/`description_sv` (the Swedish
  activity text, translated to English when `text_translations` has a match); `observed_at`
  is the **later** of the register row's own `observed_at` and the translation's stamp
  (`toDateTime64(max(version), 3, 'UTC')`). The translation pipeline fills
  `text_translations` asynchronously, so the register row is not the only input: with the
  register stamp alone, a company translated after its last extraction would keep
  `description_language = 'sv'` and the Swedish text on the English-facing `description`
  until its register record next changed. `bolagsverket_current_sql()` carries the same two
  CTEs and LEFT JOIN, unscoped, so the change scan and the SELECT compute the same
  `observed_at` and the scan still converges.
- `esef` reads `esef_document_company_information` (SE filings only, newest per company by
  `resolved_at`, `fiscal_year`, then `prompt_version`/`model_name`/`source_record_uid` --
  the uid is a hash over `package_sha256`, so it cannot separate two extractions of the
  same package on its own): `lei`, `description`, `description_language`; `observed_at` is
  the filing's `resolved_at`.
- `wikidata` reads `wikidata_companies` joined to the register spine by orgnr or a current
  LEI (newest linked entity per company): `legal_name` (`official_name`),
  `incorporation_date`, `wikidata_id`, `description`/`description_language` (`en`);
  `observed_at` is the entity's `resolved_at`.
- `ratsit` reads `se_ratsit_company` FINAL (newest normalized report per company, pinned
  normalizer version): `legal_name`, `status` (from the Swedish status text),
  `description`/`description_language` (`sv`)/`description_sv`; `observed_at` is the
  report's `normalized_at`.
- `llm` merges the other five sources' description text into one English/Swedish pair (see
  below); `observed_at` is the cached observation's own `created_at` when an answer is
  reused, or this run's write time when freshly answered.

Change rule: every SQL extractor visits a company only when its source table's record is
newer than the company's current suggestion row from that same source, or the company has
never been suggested by that source -- `changed_scope_sql` compares `current_sql`'s
`observed_at` per company against the suggestion table. `execute=false` (default) previews
the count without writing; `execute=true` inserts the page.

How the scan pages: `scope_pages` (extract.py) runs the scope query **once** into a scratch
table `corpscout._tmp_basic_info_scope_<uuid>` (`MergeTree ORDER BY company_id`), keyset-
pages that small table, and drops it in a `finally`. The scope SQL itself is therefore
unpaged. Keyset-paging the scope query directly made every page re-read the register's and
the suggestion table's whole remaining tail and sort it (the `LIMIT` sits above the sort):
about N^2/(2*page_size) row reads, with a per-page memory peak of the whole remaining id
set rather than the page. `SCAN_QUERY_SETTINGS` (`max_execution_time = 1800`) bounds the
scope INSERT and the page reads, and `ID_BOUND_QUERY_SETTINGS` carries the same ceiling, so
a pathological statement fails visibly instead of holding the run and its pool slot.
`max_companies` caps a run at 5,000,000 companies (default and maximum), above both
registers -- SCB 1.82 M, Bolagsverket 2.86 M -- so one materialization can converge;
`stopped_at_cap` in the metadata says whether the cap ended the run.

LLM gate and cache: `llm_scope_sql` selects a company only when it has two or more
non-reviewer, non-llm sources with a description, and either has no `llm` suggestion row
yet or the newest of those sources' `observed_at` is newer than the llm row's
`suggested_at` -- not `observed_at`, because a cached answer's `observed_at` is its own
`created_at` and can predate the texts it merged, so comparing against it would never
converge. Reviewer rows are excluded from both sides of the gate: a human decision is not
source text to merge, and it must not count toward the two-source threshold or look like
new evidence the model hasn't seen. Each request is content-addressed by its inputs and
`prompt_version`; a repeat request (same source texts, same `SUGGESTION_PROMPT_VERSION` =
`"se-company-basic-info-description-v1"`) is answered from the observation cache instead of
calling the model, so bumping `SUGGESTION_PROMPT_VERSION` is how a prompt change forces
every affected company to be re-answered. Every source entry in the payload carries
`"language"` -- the suggestion row's `description_language`, or `"und"` when the source did
not say -- because a Swedish text without a `text_sv` twin (ratsit's) would otherwise reach
the model unlabelled. `suggested_at` is stamped just before the suggestion insert, after
the page's model calls, so a fold running while a long page is still calling cannot skip
the rows that page is about to write; observations flushed mid-page carry their own flush
instant as `created_at`.

The LLM extractor's `max_companies` defaults to **5,000** (max 1,000,000), far below the
SQL extractors' 5,000,000: every eligible company that is not already in the observation
cache is a paid call, and a new `SUGGESTION_PROMPT_VERSION` empties that cache. Preview
first, read `would_call_model` as the budget, then raise the cap in stages.

How to run: preview first with `execute: false` to see the count of companies that would be
visited; set `execute: true` to write. The `llm` extractor additionally requires an `llm:`
profile (`provider`, `model`, `base_url`, `temperature`, `max_tokens`, `prompt_version`,
`concurrency`) -- there is no default, so a bare Materialize fails config validation rather
than silently spending on a default or a preview model. `se_company_basic_info_extract_job`
(`jobs.py`) selects all six extractors; `se_company_basic_info_weekly` schedules it Mondays
06:40 UTC (`40 6 * * 1`) with every source's `execute: true`, `page_size: 20000` on the five
SQL extractors, `max_companies: 5000` on the llm and the pinned
`deepseek`/`deepseek-v4-flash` profile, registered STOPPED -- turn it on only when ready to
run automated weekly extraction.

Re-running bolagsverket after a translation backfill is no longer needed for coverage (the
scan follows the translation stamp), but `since: "2000-01-01T00:00:00Z"` still re-selects
every register row when a mapping change makes a full re-extraction necessary.
