# se_company.person (slices 0-5)

The shipped part of the 2026-09-09 SE company person entity design
(`docs/superpowers/specs/2026-09-09-se-company-person-entity-design.md`); read that for
everything past the modules below -- the backoffice.

| Module | Responsibility |
| --- | --- |
| `tables.py` | Table names/column tuples, pinned against migration 000396; main table `se_company_person` since migration 000398 (built as `se_company_person_v2`, because the 2026-08-19 table held the final name until slice 0 dropped it); also the slice-5 role view's name and SELECT (ROLE_VIEW, build_se_company_person_role_sql, migration 000402) |
| `roles.py` | Per-source role maps (`role_code_for`), moved verbatim from `sweden_financial`/`esef_filings`/`wikidata`'s own `roles.py`; an unmapped label publishes as itself, lowercased and trimmed |
| `normalize_se.py` | `normalize_se_person`: pure Swedish parser -- splits, folds and classifies a delivered name and role; never guesses a missing half |
| `normalize.py` | The normalize SQL (`changed_scope_sql`, `changed_rows_sql`, `all_scope_sql`, `all_rows_sql`, `normalized_insert_sql`) and the paging/write loop (`normalize_all`, `normalize_companies`) |
| `assets.py` | Normalization, input snapshots, LLM matching, publication, targeted correction folds and precedence export |
| `suggestions.py` | The person `SuggestionTarget`, the shared column lists (`PERSON_SELECT_COLUMNS`/`PERSON_STATE_COLUMNS`) and the four SQL builders every source shares (`live_select_sql`, `person_state_sql`, `person_changed_scope_sql`, `person_select_sql`) |
| `bolagsverket.py` | The Bolagsverket signatory extractor `se_company_person_suggestions_bolagsverket`: split name, `role_kind` as `role_key`, and the `has_company = 0` deregistration tombstone |
| `esef.py` | The ESEF document-people extractor `se_company_person_suggestions_esef`: one name string, `role_category` as `role_key`, slot = `source_document_id` + `candidate_uid` |
| `wikidata.py` | The Wikidata company-person extractor `se_company_person_suggestions_wikidata`: orgnr/LEI-linked statements, slot = `Q<company>:P<property>:Q<person>` |
| `ratsit.py` | The Ratsit responsible-people extractor `se_company_person_suggestions_ratsit`: the newest normalized report per company, one row per named person, slot = the profile-URL token (role-qualified when a report repeats it, `idx:<person_index>` without a URL), `role_key` NULL |
| `jobs.py` | The two global workflows: `se_company_person_sync_job` (extract → normalize → input hashes) and `se_company_person_refresh_job` (same sync → match → publish). No People schedule. |
| `precedence.py` | The `name` spelling order (`PERSON_PRECEDENCE`, `precedence_for`, `precedence_rows`): reviewer 20000, ratsit 1000, bolagsverket 900, wikidata 600, esef 400. It decides the published spelling and the `data` merge, never who is published |
| `fold.py` | The pure fold: identity sets (equal first/last tokens with the unique-minimal-superset middle rule, or a shared QID, never across two birth years), the canonical name and `person_key`, the reviewer rules, the member/roles/`data` blocks, the lifecycle diff and the history entries |
| `batch.py` | The fold's SQL and paging: selection, the four page reads under `FINAL`, history-then-main writes, `FoldCounts`, `fold_companies`, `fold_bucket` |
| `match.py` | The LLM matching phase: candidates per source per name-token triple, the versioned prompt and its parser, the change scan and the paged run loop, `PersonMatchProfile` |
| `se_company_person_match` | One call per company whose normalized `ok` rows span two or more machine sources; writes `se_company_person_match` (scored pairs) and `se_company_person_match_state` (one row per company). Pool `se_company_person_match` (limit 1), retried 3 times with exponential backoff; `provider` and `model` have no defaults |
| `se_company_person_publish` | Publishes all companies by visiting 64 hash buckets sequentially in the fold concurrency pool. Part of Full processing; config `changed_only` (default true) and `page_size` (default 20,000). |
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

## Interrupted-migration runbook (000396 and 000398)

000396 stops `corpscout.se_companies_serving`, creates the six tables above, re-points the
view's query with `ALTER TABLE ... MODIFY QUERY`, then restarts the view. If the migrate
client drops between the STOP and the re-point landing, the view is left stopped, serving
stale contents with nothing alerting on it. Recovery by hand: check `system.view_refreshes`
for the view's status; run `SYSTEM START VIEW corpscout.se_companies_serving`; if the ALTER
never landed, run it from 000396's `.up.sql`; then `migrate force 396` to match reality.

000398 is the same shape with a rename in the middle and no CREATEs: `SYSTEM STOP VIEW`,
`RENAME TABLE corpscout.se_company_person_v2 TO corpscout.se_company_person`, `ALTER TABLE
... MODIFY QUERY`, `SYSTEM START VIEW`. Between the RENAME and the ALTER the view's stored
query names a table that no longer exists, which is why the view is stopped first. Recovery
by hand, if the migrate client drops in that window:

1. `SELECT view, status, last_success_time, exception FROM system.view_refreshes WHERE
   database = 'corpscout' AND view = 'se_companies_serving'` -- a stopped view still lists here.
2. `SELECT name FROM system.tables WHERE database = 'corpscout' AND name LIKE
   'se_company_person%'` says whether the RENAME landed.
3. If it landed but the ALTER did not, run the `ALTER TABLE ... MODIFY QUERY` statement
   verbatim from `000398_corpscout_se_company_person_rename.up.sql`; if neither landed,
   re-running the whole up file is safe once the view has been started again.
4. `SYSTEM START VIEW corpscout.se_companies_serving`.
5. `migrate force 398`.

Apply it OUTSIDE the :45 refresh window -- the refresh takes 13 to 15 minutes, so start just
after one finishes. Neither 000396 nor 000398 uses `SYSTEM WAIT VIEW`, so neither can hit the
staged-swap failure mode that left the ledger dirty at 391: for that one see
`se_company/address/docs/address-design.md`, section "If a serving swap is interrupted" and
"If the 000393 rename is interrupted", and memory `se-companies-serving-view` (the migrate
client's `read_timeout=300` against a 27-minute `SYSTEM WAIT VIEW`).

## Running the asset

`se_company_person_normalize` (pool `se_company_person_normalize`, group `se_company_person`) takes `PersonNormalizeConfig`:
`changed_only` (default `true`; `false` re-normalizes every row, e.g. after a version bump),
`company_ids` (default `[]` = every company, scanned into a scratch table and paged; named
ids page in memory with no scan), and `page_size` (default `PAGE_SIZE` = 20,000, max 50,000).

## Extractors (slice 1; ratsit 2026-09-11)

`se_company_person_suggestions_<source>` (`bolagsverket`, `esef`, `wikidata`, `ratsit`), on the same
basic-info extract helper (`suggestions.py::define_person_suggestion_asset`), each write one
raw suggestion row per (company, slot):

| module | source | slot | notes |
| --- | --- | --- | --- |
| `bolagsverket.py` | `se_financial_report_signatories` | report `source_record_uid` + `signatory_uid` | split name, `role_kind` as `role_key`, fiscal year as the role year, `data` = signatory kind, statement key, person seq; a company with `has_company = 0` in `se_bolagsverket_companies FINAL` gets tombstones for all its Bolagsverket slots (`LEFT ANTI JOIN`, see `bolagsverket.py`) |
| `esef.py` | `se_esef_document_people` (a view -- never `FINAL` after it) | `source_document_id` + `candidate_uid` | full name, `role_category` as `role_key`, document fiscal year, `data` = organization, status, confidence, evidence ids, model, prompt |
| `wikidata.py` | `wikidata_company_people` + `wikidata_persons`, linked by orgnr or LEI | `Q<company>:P<property>:Q<person>` | full name, birth year, QID, property id as `role_key`, the role span, `data` = description, image, url, normalized name, is_current |
| `ratsit.py` | `se_ratsit_company` + `se_ratsit_responsible_people`, joined on the report key, inside the basic-info universe | the `profile_url` token, `:<role>` when a report repeats it, else `idx:<person_index>` | one name string, birth year from the URL's date, the Swedish label as `role_original` with `role_key` NULL (no machine code), the scan year as the role year, `data` = age, identity_available, profile_url, display_name_raw, ratsit person id, external |

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

`se_company_person_sync_job` selects the four extractors, normalization and input
snapshot maintenance. `se_company_person_refresh_job` adds LLM matching and publication.
Backoffice sends `execute: true`, `page_size: 10000` per extractor and incremental
normalization/input maintenance. Full processing also supplies the saved LLM profile
and prompt, with changed-only matching enabled by default. There is no People schedule.
The old extract-and-match job and standalone partitioned fold have been retired.

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

## LLM identity matching

Spec `docs/superpowers/specs/2026-09-11-se-company-person-llm-matching-design.md`. Between
normalize and the fold, `se_company_person_match` groups each company's normalized `ok` rows
from the four MACHINE sources (`bolagsverket`, `esef`, `wikidata`, `ratsit` -- a reviewer row
never reaches a model) into candidates, one per source per `(first_tokens, middle_tokens,
last_tokens)` triple, carrying the group's longest `display_name`, any birth year, Ratsit's
`data.age` (the smallest the group carries, so the value never follows row order) and
`external` flag, and at most 20 distinct `(role_code, role_year)` pairs -- the 20 MOST RECENT
when the cut bites, presented ascending, because the years that separate two people are this
decade's and not the 1990s'. A company is in scope when its candidates span two or more
sources. The candidate list is serialized deterministically (sorted by source then id, sorted
keys) and hashed.

WHAT THE CHANGE SCAN SENDS. A company with no state row, or one whose stored `input_hash`
differs, is sent. A company whose hash is unchanged is sent again only when its stored error
is TRANSIENT: `rate_limited:` and `http_error:` (the provider's weather) and `unexpected:` --
that last one because its cause is usually a bug in `match.py`, and a bug fix does not move a
candidate hash, so treating it as sticky would strand every company it touched until its
people changed. `invalid_response:` (malformed, truncated or empty) and `too many candidates`
are STICKY for the same input: re-sending buys the same failure at the same price, so the
company is skipped and counted as `skipped_sticky`, and -- because a skip writes NO state row
-- its `matched_at` stops moving, which is what keeps the fold from re-selecting it every run
as well. A sticky company is sent again the moment its candidates change.

One request per company, `concurrency` in flight through `info.map_ordered`, temperature 0,
JSON mode, thinking disabled on deepseek, timeout 120 s, the SDK's two retries. The model
reads the candidates under SHORT ordinal ids `c0`..`cN`: a 64-character normalized id is
about 16 tokens it would have to read once and echo twice per pair, and it has no use for
one. `parse_match_response` maps the ordinal back, so every stored pair still names real
ids, and the HASHED rendering keeps the full ids -- moving the prompt to ordinals moved no
stored `input_hash`. `max_tokens` is
`min(max(4_000, 120 x candidates, profile.max_tokens), 32_000)`: the profile's value wins only
when it is LARGER, the floor scales with the list so prod's 159-candidate company (19,080) is
not truncated at the default, and the ceiling is the `le` run config itself may ask for, so a
company near the 400-candidate cap asks for a budget a caller could have set by hand rather
than one the provider may refuse.

The answer is `{"pairs": [{"a", "b", "confidence", "reason"}]}`; the parser accepts both id
orders, stores the pair with the ids ascending, keeps the higher confidence of a repeated
pair, drops and counts unknown ids (an ordinal past the end of the list, or an invented one),
self-pairs and confidences outside [0, 1], and stores a pair whose two candidates carry
different birth years at `confidence = 0` with reason `birth-year conflict`. A failed call or
an unparseable answer becomes a state row with `error` and the raw text, and the run
continues; a truncated (`finish_reason = length`) or empty answer is recorded the same way
WITH its usage and its exact text, because it was paid for either way. A company with more
than 400 candidates is skipped with `error = 'too many candidates'` rather than truncated.

THE CIRCUIT BREAKER. Per-company errors must not turn a provider outage into a green run.
Once a page has attempted at least 20 calls, a failure share above 50% raises after that
page's rows are written: the pages already on disk stay, the failing page's state rows stay
too as the evidence of what happened, and the asset's `RetryPolicy` (3 retries, exponential
from 60 s) supplies the backoff. Without it an outage wrote an error row for every company,
reported success, and left all of them to be paid for again on the next run.

The fold reads the pairs at or above `MATCH_THRESHOLD` (0.8) whose `input_hash` equals the
company's current state row, and `max(matched_at)` from the state table as a FIFTH selection
watermark. `identity_sets_before_split` unions every member of one side with every member of
the other AFTER the name and QID pairs and through the same `_years_conflict` veto, before the
birth-year split and before `apply_rules` -- so a reviewer's split or merge rule still has the
last word, and a Reset of that split lets the match apply again. THE SPLIT HONOURS THE PAIRS:
a year-less row that entered a two-year set through a pair alone matches no bucket by name, so
the split attaches it to the bucket holding a member it is PAIRED with before it tries the
name relation, and only then falls back to the smallest year. Without that the row landed on
the wrong person AND lost its `llm_match`, because `pairs_within` then found only one side of
the pair inside the set. A published person whose set
was joined records it in the fold-owned `data.llm_match` key (`pairs` with the two names, the
confidence and the reason, plus `model` and `prompt_version`), written after
`merge_member_data`; `RESERVED_DATA_KEYS` stays the reviewer's three. `FOLD_VERSION` is
`se-person-fold-v2`.

`MATCH_THRESHOLD` is compared INCLUSIVELY (`>= 0.8`) here and in `match_pairs_sql`, and
`se_company_person_match.confidence` is `Float64` so that comparison is exact. Float32 would
store this constant's twin as 0.800000011920929 and a threshold of 0.7 as 0.69999998807907104
-- a pair scored at exactly the threshold would then be kept or dropped by the storage format
rather than by the constant. The `llm_match` JSON still rounds the confidence to four
decimals: `data` is in `_COMPARED`, so that text decides whether a person counts as changed.

Matching compares the model-visible candidate data and effective prompt/model settings.
A source observation or normalizer-version change that only changes row IDs can replay
its stored answer against the current members without new model calls. Actual candidate
or configuration changes require processing. See the current
[input hash documentation](../../../../../../docs/se-company-person-match-input.md)
for the data, binding and configuration hashes and legacy-state handling.

## Roles as rows (`se_company_person_role`, slice 5)

`corpscout.se_company_person_role` (migration 000402, spec section 11) is a REFRESHABLE
materialized view: `REFRESH EVERY 1 HOUR OFFSET 20 MINUTE`, `ENGINE = MergeTree ORDER BY
(company_id, person_key, role_year, role_code, source, slot)`, created `EMPTY` so the
migration returns at once and the first build is an explicit `SYSTEM REFRESH VIEW`. The
engine is declared inside the view, so the name IS the table readers query. Its SELECT
lives in `tables.py::build_se_company_person_role_sql()` and the migration body is pinned
against a fresh render by `tests/test_se_company_person_role_view.py`, exactly as the
serving view is pinned. The SELECT ends with the same trailing `SETTINGS` block every
serving refresh has carried since 000347/000391 (grace_hash spill joins, external
group-by/sort, a 12 GiB `max_memory_usage`) -- an unbounded hourly refresh over 1.1M
persons and 5.6M normalized rows would face the same shared-server ceiling that block
already exists to avoid.

One row per published ACTIVE person and per role-carrying normalized observation the fold
built them from: `ARRAY JOIN` over the main row's `normalized_ids`, `INNER JOIN` back to
`se_company_person_normalized`, `WHERE p.active = 1 AND n.role_code IS NOT NULL`. Prod
2026-09-12: 3,837,006 rows over 1,113,485 persons; 160,279 active persons hold no role and
get no row. Nothing writes it -- the fold, the normalizer, the rules and the precedence do
not know it exists -- and the person row's role arrays stay the fold's own summary.

Six things to know before reading it:

- **It lags a fold by up to an hour.** The view rebuilds at :20, so a person folded at :25
  keeps their previous rows until the next :20 and a brand-new person has none at all. The
  backoffice panel compares each row's `folded_at` against the live person row's own
  `folded_at`: empty OR older means stale, and either way the panel falls back to the
  person row's own arrays and says "the roles view has not rebuilt since this fold" (F2).
  If the lag ever stops being acceptable the same SELECT moves into the fold.
- **It follows the FOLD's members, not today's normalized rows.** `normalized_ids` names
  the versions the CURRENT published row was folded from, so an observation re-normalized
  since then (the tab's re-fold-pending badge) drops out until the next fold.
- **It never holds a row for a hidden or withdrawn person.** The WHERE is `p.active = 1`,
  so an inactive person's empty read is not staleness -- it is the view working exactly as
  designed. The panel tells the two apart (F3): only an ACTIVE person's empty-or-stale
  read gets the "has not rebuilt" note; an inactive one gets "hidden and withdrawn persons
  are not in the roles view" instead, so the reviewer is never told a rebuild would help.
- **A missing view degrades, it does not fail the tab.** If migration 000402 has not
  landed yet, or a rollback dropped the view, the backoffice's eighth read
  (`loadPersonRoleRows` in `se-company-person-entity.server.ts`) catches the UNKNOWN_TABLE
  -- or any other failure reading it -- logs it once, and answers `[]` (F4). The panel
  then renders the same fallback it shows in the ordinary lag window.
- **`role_year` 0 means "no FISCAL year", not "no year at all".** The column is `UInt16`
  because the year is in the sort key and `allow_nullable_key` is off. Wikidata delivers a
  role's span in `role_from`/`role_to` and never a fiscal year, so every Wikidata role (290
  of 466 on prod) lands under 0 here, while the fold's `role_years` array expands that span
  into the real years the role was held, taking an open or dateless one as held now.
- **`is_current` is on the person's OWN latest observed year, not "now".** It is 1 exactly
  for the codes on `current_roles`, computed at fold time from `last_year` -- a person last
  seen in 2019 has their 2019 rows flagged current, even years later. The flag is per code,
  not per person: two roles can differ. The backoffice badge carries this as a `title`.
- **The name is reused.** It was the 2026-08-19 model's role table, dropped by hand in
  slice 0. The spent script `clickhouse/operations/se_person_retirement_drops.sql` still
  names it and must never be run again.

### Roles view health

`system.view_refreshes` is the runbook check until the People pipeline gets its own
`companies_current_refresh_is_healthy`-style asset check (`companies_current.py`'s
pattern for `se_companies_serving`, spec section on refresh health): a refresh that throws
keeps serving its last good rows at full speed, so a stuck or failing refresh is invisible
from row counts alone.

```sql
SELECT view, status, last_success_time, exception
FROM system.view_refreshes
WHERE view = 'se_company_person_role'
```

An exception, or a `last_success_time` older than 3 hours (three refresh intervals), means
the view is serving stale rows at full speed -- the same failure mode
`companies_current_refresh_is_healthy` catches for the serving view, not yet wired up here.

## Known limits

- **A split rule goes stale every filing year.** `_apply_split` pins the rule's `slots`, but
  Bolagsverket mints a brand-new slot (`<statement_key>:<signatory_uid>`) on every annual
  report, so the person a reviewer split out rejoins the very set it was split from the moment
  next year's filing lands, silently. Merge and hide survive a re-key (they resolve through
  the previous published members, and old slots persist for ever); split alone does not.
  A split rule pins slots and the Split dialog says so (Ruling 9); a durable split
  expression is deferred.
- **Publication visits buckets serially behind `FOLD_POOL`.** A page's `current_normalized_sql` read is
  a `FINAL` scan of the normalized table, and the bucket hash scatters a page's ids over the
  whole primary key, so nearly every granule matches: measured on prod at 5.6M rows / 2.57
  GiB / 7.7 s / 366 MiB for one 9,000-company page. 64 of those in parallel would press the
  server's memory, so publication is pooled at limit 1 and visits one bucket at a time
  (~30 s each, ~30 min for all 64).
- **An idle precedence re-export no longer moves the watermark.** `export_precedence` reads
  the stored global rows first and inserts nothing when they already equal
  `precedence_rows()`, so a "materialise everything" click on an unchanged dictionary does not
  re-stamp `decided_at` and therefore does not re-select every company on the next fold.
