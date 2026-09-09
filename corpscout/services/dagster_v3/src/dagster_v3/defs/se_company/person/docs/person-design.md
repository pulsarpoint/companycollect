# se_company.person (slice 0)

The shipped part of the 2026-09-09 SE company person entity design
(`docs/superpowers/specs/2026-09-09-se-company-person-entity-design.md`); read that for
everything past the modules below -- the fold, the extractors and the backoffice.

| Module | Responsibility |
| --- | --- |
| `tables.py` | Table names/column tuples, pinned against migration 000396; main table built as `se_company_person_v2`, renamed in the last slice |
| `roles.py` | Per-source role maps (`role_code_for`), moved verbatim from `sweden_financial`/`esef_filings`/`wikidata`'s own `roles.py`; an unmapped label publishes as itself, lowercased and trimmed |
| `normalize_se.py` | `normalize_se_person`: pure Swedish parser -- splits, folds and classifies a delivered name and role; never guesses a missing half |
| `normalize.py` | The normalize SQL (`changed_scope_sql`, `changed_rows_sql`, `all_scope_sql`, `all_rows_sql`, `normalized_insert_sql`) and the paging/write loop (`normalize_all`, `normalize_companies`) |
| `assets.py` | The Dagster asset `se_company_person_normalize` |

## Change rule

A raw row is (re)normalized when its normalized row is missing, on a `suggestion_id` that
does not match the raw row's own, or on an older `normalizer_version`. Two UNION ALL branches
(LEFT ANTI JOIN for "missing", INNER JOIN for the other two) stand in for one LEFT JOIN so the
result does not depend on `join_use_nulls`. Unlike the address entity's timestamp-based scan,
this table carries no `suggested_at` -- `suggestion_id` is already `sha256(company_id,
source, slot, suggested_at)`, so comparing it directly is enough. `changed_only=false`
re-normalizes every raw row regardless.

## parse_status

- `ok` -- a first token and a last token both exist.
- `partial` -- one name word, or nothing but initials; stored, never folded into a person.
- `no_person` -- the name field holds a role word, a number, a date, a company suffix (AB,
  HB, KB), or nothing. Rules read the NAME field only: the August 2026 audit found roles in
  the name field and dates in the role field, so role mapping never touches the name and the
  no-person checks never touch the role.

## The `data` contract

`data` is a `String` holding a JSON object -- never the native ClickHouse `JSON` type (owner
ruling 2026-09-09). Every table carrying it declares
`CONSTRAINT valid_data CHECK JSONType(data) = 'Object'` and `DEFAULT '{}'` (controller ruling
2026-09-09), so an insert that omits `data` passes the constraint instead of tripping it on an
empty string. `normalize.py`'s read (`_raw_select`) coerces anything that is not a JSON object
to `'{}'` -- `if(JSONType(r.data) = 'Object', r.data, '{}') AS data` -- so a malformed row
degrades to the empty object rather than failing a page's insert; between the default and that
coercion, the constraint only ever fires on a hand-written row that explicitly supplies
something other than an object (proved by `test_se_company_person_normalize_clickhouse_local.py`,
`Code: 469`, `[1,2]`). `data` is an ordinary `String`, so this entity's read/write is the address
entity's -- no native-JSON insert path. The extractors are the only place that BUILDS an
object, and they do it with `toJSONString(map(...))` over all-String values, never by
concatenating source text, so the constraint holds by construction.

## The `role_key` column

`role_original` is the human label a source delivers (Bolagsverket's Swedish role word, the
ESEF role phrase, ...); `role_key` is that source's own machine code beside it --
Bolagsverket's `role_kind`, Wikidata's property id, ESEF's `role_category`. The `roles.py`
per-source maps key on `role_key` first, falling back to `role_original`, since a code
outlives label rewordings that a human label does not. Slice 1's extractors fill `role_key`
straight from the source's own code column -- Bolagsverket's `role_kind`, ESEF's
`role_category`, Wikidata's `role_property` -- and never out of `data`.

## Interrupted-migration runbook (000396)

000396 stops `corpscout.se_companies_serving`, creates the six tables above, re-points the
view's query with `ALTER TABLE ... MODIFY QUERY`, then restarts the view. If the migrate
client drops between the STOP and the re-point landing, the view is left stopped, serving
stale contents with nothing alerting on it. Recovery by hand: check `system.view_refreshes`
for the view's status; run `SYSTEM START VIEW corpscout.se_companies_serving`; if the ALTER
never landed, run it from 000396's `.up.sql`; then `migrate force 396` to match reality.

## Running the asset

`se_company_person_normalize` (pool `se_company_person_normalize`, group `se_company_person`) takes `PersonNormalizeConfig`:
`changed_only` (default `true`; `false` re-normalizes every row, e.g. after a version bump),
`company_ids` (default `[]` = every company, scanned into a scratch table and paged; named
ids page in memory with no scan), and `page_size` (default `PAGE_SIZE` = 20,000, max 50,000).

## Extractors (slice 1)

`se_company_person_suggestions_<source>` (`bolagsverket`, `esef`, `wikidata`), on the same
basic-info extract helper (`suggestions.py::define_person_suggestion_asset`), each write one
raw suggestion row per (company, slot):

| module | source | slot | notes |
| --- | --- | --- | --- |
| `bolagsverket.py` | `se_financial_report_signatories` | report `source_record_uid` + `signatory_uid` | split name, `role_kind` as `role_key`, fiscal year as the role year, `data` = signatory kind, statement key, person seq |
| `esef.py` | `se_esef_document_people` (a view -- never `FINAL` after it) | `source_document_id` + `candidate_uid` | full name, `role_category` as `role_key`, document fiscal year, `data` = organization, status, confidence, evidence ids, model, prompt |
| `wikidata.py` | `wikidata_company_people` + `wikidata_persons`, linked by orgnr or LEI | `Q<company>:P<property>:Q<person>` | full name, birth year, QID, property id as `role_key`, the role span, `data` = description, image, url, normalized name, is_current |

The universe is `se_company_basic_info`: every extractor joins the folded company and drops
everything else. The change scan is a per-company state hash, not a timestamp -- a sha256
over what the source delivers now against the company's stored live rows; a company is
visited only when the two sides differ, so the scan converges after one pass (a timestamp
does not work here: Bolagsverket's source table is rebuilt whole on every run, so "newer
than last time" is either everything or nothing). Tombstones are per slot, not per company:
a `LEFT ANTI JOIN` of the stored live slots against the same `live` CTE, writing every
person column NULL and `data` `{}`. `suggestion_id` and `suggested_at` both come from one
`WITH (SELECT now64(3, 'UTC')) AS stamp` bound once per statement, so the two always agree.
The suggestion table stores neither `source_run_id` nor `extractor_version`. `execute:
false` (the default) previews the count without writing.

`se_company_person_extract_job` (`jobs.py`) selects the three extractors and
`se_company_person_normalize` (which now `deps` on them); `se_company_person_weekly`
schedules it Mondays 07:25 UTC (`25 7 * * 1`) with `execute: true`, `page_size: 10000` per
extractor and `changed_only: true` on the normalize asset, registered STOPPED.
