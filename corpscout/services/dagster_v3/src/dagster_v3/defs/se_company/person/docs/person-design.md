# se_company.person (slices 0-1)

The shipped part of the 2026-09-09 SE company person entity design
(`docs/superpowers/specs/2026-09-09-se-company-person-entity-design.md`); read that for
everything past the modules below -- the fold and the backoffice.

| Module | Responsibility |
| --- | --- |
| `tables.py` | Table names/column tuples, pinned against migration 000396; main table built as `se_company_person_v2`, renamed in the last slice |
| `roles.py` | Per-source role maps (`role_code_for`), moved verbatim from `sweden_financial`/`esef_filings`/`wikidata`'s own `roles.py`; an unmapped label publishes as itself, lowercased and trimmed |
| `normalize_se.py` | `normalize_se_person`: pure Swedish parser -- splits, folds and classifies a delivered name and role; never guesses a missing half |
| `normalize.py` | The normalize SQL (`changed_scope_sql`, `changed_rows_sql`, `all_scope_sql`, `all_rows_sql`, `normalized_insert_sql`) and the paging/write loop (`normalize_all`, `normalize_companies`) |
| `assets.py` | The Dagster asset `se_company_person_normalize` |
| `suggestions.py` | The person `SuggestionTarget`, the shared column lists (`PERSON_SELECT_COLUMNS`/`PERSON_STATE_COLUMNS`) and the four SQL builders every source shares (`live_select_sql`, `person_state_sql`, `person_changed_scope_sql`, `person_select_sql`) |
| `bolagsverket.py` | The Bolagsverket signatory extractor `se_company_person_suggestions_bolagsverket`: split name, `role_kind` as `role_key`, and the `has_company = 0` deregistration tombstone |
| `esef.py` | The ESEF document-people extractor `se_company_person_suggestions_esef`: one name string, `role_category` as `role_key`, slot = `source_document_id` + `candidate_uid` |
| `wikidata.py` | The Wikidata company-person extractor `se_company_person_suggestions_wikidata`: orgnr/LEI-linked statements, slot = `Q<company>:P<property>:Q<person>` |
| `jobs.py` | `se_company_person_extract_job` (the three extractors plus the normalize asset) and the STOPPED `se_company_person_weekly` schedule (`25 7 * * 1`) |
| `precedence.py` | The `name` spelling order (`PERSON_PRECEDENCE`, `precedence_for`, `precedence_rows`): reviewer 20000, ratsit 1000 (reserved), bolagsverket 900, wikidata 600, esef 400. It decides the published spelling and the `data` merge, never who is published |
| `fold.py` | The pure fold: identity sets (equal first/last tokens with the unique-minimal-superset middle rule, or a shared QID, never across two birth years), the canonical name and `person_key`, the reviewer rules, the member/roles/`data` blocks, the lifecycle diff and the history entries |
| `batch.py` | The fold's SQL and paging: selection, the four page reads under `FINAL`, history-then-main writes, `FoldCounts`, `fold_companies`, `fold_bucket` |
| `se_company_person_fold` | 64 static buckets (`bucket_00`..`bucket_63`, `modulo(cityHash64(company_id), 64)`), no pool, `BackfillPolicy.multi_run(max_partitions_per_run=1)`; config `changed_only` (default true) and `page_size` (default 20,000) |
| `se_company_person_fold_companies` | The targeted fold for the backoffice's Fold now: normalizes `company_ids` first (always `changed_only`), then folds them (`changed_only` false by default) |
| `se_company_person_precedence_clickhouse` | Exports `PERSON_PRECEDENCE` as the global (`company_id = ''`, `field = 'name'`) rows; re-running it re-folds every company |

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
| `bolagsverket.py` | `se_financial_report_signatories` | report `source_record_uid` + `signatory_uid` | split name, `role_kind` as `role_key`, fiscal year as the role year, `data` = signatory kind, statement key, person seq; a company with `has_company = 0` in `se_bolagsverket_companies FINAL` gets tombstones for all its Bolagsverket slots (`LEFT ANTI JOIN`, see `bolagsverket.py`) |
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

## The fold

**Identity** (spec 5.1). Two `ok` rows of one company are the same person when their
`first_tokens` and `last_tokens` are equal and their `middle_tokens` are equal, or one side's
middle tokens are a proper subset of the other's *and that other is the unique minimal
superset* among the distinct middle-token sets sharing those first and last tokens -- or when
both carry the same non-empty `wikidata_id` -- and never when both carry a `birth_year` and
the years differ. Sets are the transitive closure of that relation, computed only inside the
(first, last)-token and QID groupings a matching pair must share; a closed set that still
holds two birth years (reached through a year-less member) is split by year, one sub-set per
year with year-less members attached breadth first to a sub-set they directly match. The
canonical name is the folded tokens of a set's most complete member (most tokens, then the
longest joined string, then alphabetically first); `person_key` hashes the company id and
that name -- the same `address_key` shape `normalize_se.py` uses for the address entity. Two
sets of one company may not share a key (`ReplacingMergeTree ORDER BY (company_id,
person_key)` would collapse them into one person), so a set whose canonical name collides
with another's takes a discriminator: its birth year when it has one, else the smallest
`source:slot` of its members; if that STILL collides -- a split rule leaving two sets that
share both the name and the year, say -- the smallest member id is appended to the
discriminator too. A set alone under its (name, discriminator) pair keeps the plain key,
which is what makes keys stable across folds. Reviewer rules apply after the grouping, merge
rules then split rules, each kind ordered by `rule_id`: a merge rule's keys resolve through
the PREVIOUS published rows (key -> that row's member pairs -> the new set holding any of
them) and the resolved sets join, re-keyed from their joined canonical name; a split rule's
named slots leave their sets and form one set of their own. A key or slot that resolves to
nothing is ignored and the rule counted in `stale_rules`.

**The published row** (spec 5.3-5.5). Every `member_*` array, `slots` and `normalized_ids`
follow member order `(-precedence_for(source, company_precedence), source, slot)`; `sources`
is the same order deduplicated. `display_name`/`first_name`/`last_name`/`text_source` come
from the highest-`name`-precedence member, ties broken by the most complete spelling.
`birth_year` and `wikidata_id` come from the first member in member order that carries one.
Roles are the union over members of `(role_code, year)` pairs with the sources that saw each,
sorted by year then code: a member's years are its own `role_year` (a fiscal year) when it
has one, else its `role_from`..`role_to` span expanded year by year with a missing `role_to`
read as "to the current UTC year", else `role_to`'s year alone, else **the current UTC year
alone** -- a role with no date at all is taken as held now (controller ruling 2026-09-10:
dropping it would hide most of Wikidata's roles, 290 of 466 on prod). `first_year`/
`last_year` are the min/max of that union; `current_roles` are the codes at `last_year`.
`data` merges the members' JSON objects key by key in the same precedence order, two objects
merging one level down and arrays replaced; a `reviewer` member's object wins outright, never
merged into a lower member's.

**Lifecycle and the five change kinds** (spec 5.6). Every set publishes `active = 1` unless a
hide rule names its key -- resolved by key equality against the fold's own live sets first,
and when the key is gone (a fuller spelling re-keyed the set) through the PREVIOUS published
members instead, the same way a merge rule resolves, so a reviewer's Hide cannot silently
reverse itself when the set it named gets re-keyed. Every previously published key with no
set this fold is re-emitted `active = 0, inactive_reason = 'withdrawn'`, keeping its last
blocks. History is appended before the main write, one row per person whose published columns
changed (compared over every column except `folded_at`, `fold_version`, `source_run_id`),
carrying the PREVIOUS main row's columns. The five `change_kind` values: `created` (no
previous row -- the new image is its own genesis entry), `withdrawn` (becomes withdrawn and
was not), `hidden` (becomes hidden and was not), `reactivated` (`active` goes 0 -> 1: a
withdrawn person returning, or a hide rule Reset), else `updated`. A person whose columns did
not change gets no history row, but its main row is still rewritten with the fold's
`folded_at` -- exactly what makes the selection below converge.

Because role years and `last_year` are computed from `current_year` at fold time, an OPEN or
DATELESS Wikidata span (a `role_from` with no `role_to`, or a role with no date at all) keeps
expanding with the calendar even when nothing in the database changes -- but the selection
below only re-folds a company when one of its INPUTS changes, never merely because a year
turned over. A company with only such a role therefore freezes at whatever `last_year` its
last fold happened to compute, until something else re-selects it. The runbook answer is a
yearly `changed_only: false` re-fold of the whole table, not a selection change: teaching the
selection to watch the clock would re-fold every company holding an open or dateless role on
every single run.

A company is folded when it has no main row and at least one `ok` normalized row, or when its
newest input is newer than its `max(folded_at)`. "Newest input" is the newest of: its newest
normalized row **of any status** (a row leaving `ok` changes the published set), its newest
rule version **of any `active` value** (a Reset is a new version with `active = 0`, and
filtering it out here would make a rule permanent), its own newest precedence row, and the
global precedence export's stamp — so re-exporting the dictionary re-folds every company
once. Re-running a folded bucket selects nothing, because the fold rewrites every row of
every folded company with the run's `folded_at`, whether or not anything changed; the history
table is what records what actually changed.
