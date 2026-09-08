# SE basic info, slice 6: the `economic_activity` field

Date: 2026-09-08, after slice 5 (spine retired). Owner decision the same day: "Yes we should
create economic activity as a field", three-valued, on the basic-info entity. Promised in the
2026-09-08 amendment of the 2026-09-03 design, section 4.

## Facts (prod, 2026-09-08, `se_company_basic_info` = 3,523,558 companies)

- `status` is legal existence: Bolagsverket's deregistration decides (precedence 1000), SCB's
  Företagsstatus decides only for the companies Bolagsverket does not file (900), Ratsit last.
- SCB's Företagsstatus (`se_scb_companies.source_status_code`) is economic activity: `1` =
  registered for VAT, F-tax or as an employer; `0` = never was; `9` = was, no longer.
- Cross-tab of the two, every entity row:

| Bolagsverket | SCB 1 (active) | SCB 0 (never) | SCB 9 (no longer) | not in SCB |
|---|---|---|---|---|
| registered | 861,948 | 152,274 | 87,499 | 4,969 |
| deregistered | 40,468 | 1,645 | 6,735 | 1,699,680 |
| not filed | 463,695 | 173,061 | 31,584 | 0 |

- Every "registered" company shows `status = 'active'`; 239,773 of them (22 percent) have no
  economic activity, and the entity cannot say so today. The 40,468 "deregistered but SCB
  active" companies are invisible too.
- The entity pattern: one value column per field on the suggestion table (NULL = no opinion),
  the value plus `<field>_source` on the main and history tables, a precedence map in
  `precedence.py`, one row on the Info tab and the edit sheet. `status` is the one
  non-nullable value (`''` when unknown) and the fold special-cases it by name.
- The extractors emit every value column explicitly in DDL order (`SUGGESTION_SELECT_COLUMNS
  = 4 keys + VALUE_COLUMNS`); the fold and the LLM extractor build rows from the column tuples,
  so they need no per-column code. A version bump on an extractor does not re-select
  companies; a full re-extract needs `since: "2000-01-01T00:00:00Z"`.
- The ledger's next free number is 000394 (000393, the address rename, is committed on main
  and not yet applied on prod).

## Design

### Field

`economic_activity`, values `active`, `never`, `ceased`, from SCB codes `1`, `0`, `9`. Three
values rather than a boolean: a company that never registered for anything (dormant, shell,
brand new) and one that stopped are different signals. Unknown is `''` on the main row, like
`status`; NULL on a suggestion row means the source has no opinion.

### Tables (migration 000394, ALTER only, metadata-only on ReplacingMergeTree)

- `se_company_basic_info_suggestion`: `economic_activity Nullable(String)` AFTER `status`.
- `se_company_basic_info` and `_history`: `economic_activity LowCardinality(String)` AFTER
  `status_source`, `economic_activity_source LowCardinality(String)` AFTER it. Existing rows
  read `''` / `''` until the fold rewrites them.
- Down: the three DROP COLUMNs. The ALTER lines follow the one-clause-per-line format
  `tests/se_company_ddl.py` replays, so the column tuples stay pinned to the deployed DDL.

### Python

- `tables.py`: `VALUE_COLUMNS` gains `economic_activity` after `status`; `MAIN_COLUMNS` gains
  the pair after `status_source`; a new `NON_NULLABLE_FIELDS = ("status", "economic_activity")`
  replaces the fold's by-name special case (`'' when unknown`, `'' -> None` when comparing).
- `precedence.py`: `"economic_activity": {"reviewer": 20000, "scb": 1000}` between `status`
  and `incorporation_date` (map order must equal `FOLDED_FIELDS`). Only SCB knows it.
- `scb.py`: `multiIf(source_status_code = '1', 'active', source_status_code = '0', 'never',
  source_status_code = '9', 'ceased', NULL) AS economic_activity`; `SCB_EXTRACTOR_VERSION =
  "scb-v2"`. The other SQL extractors (bolagsverket, esef, ratsit, wikidata) emit
  `CAST(NULL AS Nullable(String)) AS economic_activity` in the same position. The LLM extractor
  and the fold need no change beyond the tuples.
- The precedence export asset writes the two new global rows on its next run.

### Backoffice

- `se-basic-info-fields.ts`: the field after `status` (label "Economic activity", kind
  `text`), `BASIC_INFO_ECONOMIC_ACTIVITIES = ["active", "never", "ceased"]`, a validator like
  `status`'s, and a display label map (Active / Never registered / Ceased) the workspace and
  the edit sheet use.
- `se-basic-info.server.ts`: the value and source columns in the three SQL builders and the
  row types; `SUGGESTION_VALUE_FIELDS` and the insert row gain the field so a reviewer can set
  it.
- The edit sheet renders a three-option select; the workspace shows the label.
- `countries.ts` SE shell projects `i.economic_activity AS economic_activity` so the public
  page shows it as a content field.

### Not touched

`status` and its precedence, the serving view `se_companies_serving` (a staged swap for one
column is not worth it yet), the list filters (a follow-up once the column exists), the SCB
address extractor, the address entity.

## Rollout

1. Prod ledger: the other session applies 000393; then `make clickhouse-migrate-up-one` for
   000394 (instant).
2. Deploy dagster_v3 (no dbt change: the defs-state refresh is a formality but stays in the
   recipe); merge the backoffice.
3. Materialise `se_company_basic_info_precedence_clickhouse` (the two new rows).
4. Re-extract SCB with `execute: true, since: "2000-01-01T00:00:00Z"` (3.5M rows, the version
   bump alone re-selects nothing).
5. Full fold: backfill the 64 buckets of `se_company_basic_info_fold`. Every company with an
   SCB row re-folds; the field is `''` for the 1,704,649 companies SCB does not file.
6. Smoke the Info tab (Handelsbanken `?field=economic_activity`) and the public SE page.
