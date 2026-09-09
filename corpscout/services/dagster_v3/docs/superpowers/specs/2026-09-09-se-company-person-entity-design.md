# SE company person entity: design

Owner-approved in conversation 2026-09-09 (sections 1 to 7 agreed one by one). Third entity on the
basic-info shape after `se_company_basic_info` (2026-09-03) and `se_company_address` (2026-09-06):
per-source raw suggestions, a stored normalized layer, a Python fold into one main table with
history, rules and precedence, and a backoffice tab. Sweden only.

## 1. Why

People for Swedish companies were built three times (the cross-country `country_person` model, the
`se_company_person` three-asset resolution with LLM suggestions, the identity experiment) and never
used by anything the owner keeps: the public Management section re-derives from raw sources, the
serving view only asks "has people", and the admin pages show evidence beside a model that was
truncated on 2026-08-27. Meanwhile basic info and addresses converged on one shape that reviewers
and the fold understand. People get that shape, from scratch: the old chain is deleted first (owner
ruling 2026-09-09: "remove everything people related built so far and create from start, this is
still dev version not used anywhere"), nothing is paritied against it.

## 2. Scope

- Sources now: Bolagsverket annual-report signatories (`se_financial_report_signatories`), ESEF
  people (`se_esef_document_people`, the country-scoped view of the country-agnostic extraction),
  Wikidata (`wikidata_company_people` + `wikidata_persons` linked to an SE company). Ratsit is
  reserved as a source with the highest trust and lands as one extractor module when its people
  data exists. SCB carries no people.
- One row per company and person. There is no cross-company person identity: Sweden publishes no
  person identifier, so "same person" only ever means within one company.
- No source ranking decides who is published: every person from every source is published, and a
  person seen by several sources is one row that keeps every source. A trust order decides only
  which spelling is displayed when the sources spell one person differently.
- The backoffice is the only consumer. The public Management section, its dbt model and its
  loaders are removed in slice 0 (owner ruling: "only backoffice is important now"); the serving
  view keeps its `has_people` flags because the admin companies list filters on them.
- Out of scope: Ratsit extraction itself, other countries, LLM merging, a person's addresses.

## 3. Tables

All in `corpscout`, created by migrations (the next free numbers at merge time), schema owned by
the migration, column order pinned by `tables.py` constants and a DDL test as for addresses.

### 3.1 `se_company_person_suggestion` (raw)

One current row per company, source and slot, never normalized. A source that stops delivering
a slot writes a row with every person column NULL (tombstone), as the address extractors do.

```
company_id            String
source                LowCardinality(String)   -- bolagsverket | esef | wikidata | reviewer | reviewer_draft (ratsit reserved)
slot                  String                   -- the source's stable observation id (3.1.1)
suggestion_id         FixedString(64)          -- sha256(company_id, source, slot, suggested_at)
suggested_at          DateTime64(3, 'UTC')     -- one WITH-bound now64() per statement
source_record_id      String
full_name             Nullable(String)         -- as delivered when the source gives one string
first_name            Nullable(String)         -- as delivered when the source splits
last_name             Nullable(String)
birth_year            Nullable(UInt16)
wikidata_id           Nullable(String)
role_original         Nullable(String)         -- the source's role label, untouched
role_key              Nullable(String)         -- the source's own role code when it has one
fiscal_year           Nullable(UInt16)         -- Bolagsverket and ESEF
role_from             Nullable(Date)           -- Wikidata
role_to               Nullable(Date)
document_ref          Nullable(String)         -- report / document identifier for the evidence link
data                  String DEFAULT '{}'      -- a JSON object, the source's extras (3.1.2)
ENGINE ReplacingMergeTree(suggested_at) ORDER BY (company_id, source, slot)
```

3.1.1 Slots. Bolagsverket: the report's source record uid plus the signatory uid (one slot per
signature line, so a person signing two years has two slots). ESEF: the document id plus the
person's index in the extraction. Wikidata: the QID plus the company link id. Reviewer: `r` plus
17 digits, as for addresses. A slot is stable across runs; a changed observation rewrites its slot.

3.1.2 `data`. Everything the source knows about the person beyond the named columns, as a JSON
object stored as text (the pinned ClickHouse Python driver cannot read or insert the native JSON
type; a later migration may switch the column once it can; the column defaults to `{}` and a
CHECK constraint refuses anything but an object): Wikidata's description, occupations, nationality, image; ESEF's title or position text and
the section it came from; Bolagsverket's signatory kind. Extractors fill it; nothing is dropped
at the raw layer.

### 3.2 `se_company_person_normalized`

One row per suggestion row, written by the normalize asset from the current suggestions
(`changed_only` by default, the full set on a normalizer version bump).

```
company_id            String
source                LowCardinality(String)
slot                  String
suggestion_id         FixedString(64)
normalized_id         FixedString(64)          -- sha256(suggestion_id, normalizer_version)
normalizer_version    LowCardinality(String)   -- se-person-normalizer-v1
parse_status          LowCardinality(String)   -- ok | partial | no_person
parse_notes           Array(String)
first_tokens          Array(String)            -- identity tokens (4)
middle_tokens         Array(String)
last_tokens           Array(String)
display_first         String                   -- spelling as delivered, cleaned
display_last          String
display_name          String
birth_year            Nullable(UInt16)
wikidata_id           Nullable(String)
role_code             Nullable(String)         -- catalog code or the original label (4.3)
role_key              Nullable(String)         -- passed through
role_year             Nullable(UInt16)
role_from             Nullable(Date)
role_to               Nullable(Date)
data                  String DEFAULT '{}'      -- passed through
normalized_at         DateTime64(3, 'UTC')
ENGINE ReplacingMergeTree(normalized_at) ORDER BY (company_id, source, slot)
```

### 3.3 `se_company_person_v2` (main, renamed to `se_company_person` in the last slice)

One row per company and person. Built as `_v2` because the old table holds the name until
slice 0 drops it; the rename is a `RENAME TABLE` plus `MODIFY QUERY` on the serving view, the
proven address recipe.

```
company_id            String
person_key            FixedString(64)          -- sha256(company_id, canonical folded name) (5.1)
display_name          String                   -- from the highest-trust member (5.3)
first_name            String
last_name             String
birth_year            Nullable(UInt16)
wikidata_id           Nullable(String)
sources               Array(LowCardinality(String))
slots                 Array(String)
normalized_ids        Array(FixedString(64))
member_sources        Array(LowCardinality(String))  -- the per-source block, parallel arrays
member_slots          Array(String)
member_names          Array(String)
member_birth_years    Array(Nullable(UInt16))
member_wikidata_ids   Array(String)
member_data           Array(String)            -- each member's data object as JSON text
role_codes            Array(String)            -- the roles block, parallel arrays (5.4)
role_years            Array(UInt16)
role_sources          Array(Array(String))
current_roles         Array(String)
first_year            Nullable(UInt16)
last_year             Nullable(UInt16)
text_source           LowCardinality(String)   -- whose spelling won
data                  String DEFAULT '{}'      -- a JSON object, the merged object (5.5)
active                UInt8
inactive_reason       LowCardinality(String)   -- '' | hidden | withdrawn
folded_at             DateTime64(3, 'UTC')
fold_version          LowCardinality(String)
source_run_id         String
ENGINE ReplacingMergeTree(folded_at) ORDER BY (company_id, person_key)
```

### 3.4 `se_company_person_history`

Append-only, one row per person whose main row changed in a fold, written before the main write:
the previous main row's columns plus `changed_at`, `change_kind` (created | updated | hidden |
withdrawn | reactivated) and `fold_run_id`. Same shape and rules as the address history.

### 3.5 `se_company_person_rule`

```
company_id   String
rule_id      FixedString(64)
kind         LowCardinality(String)   -- hide | merge | split
person_keys  Array(FixedString(64))   -- hide: one key; merge: two or more keys
slots        Array(String)            -- split: the slots to keep apart from the rest of their set
active       UInt8                    -- 0 undoes the rule (Reset)
note         String
created_at   DateTime64(3, 'UTC')
created_by   String
ENGINE ReplacingMergeTree(created_at) ORDER BY (company_id, rule_id)
```

A merge rule names keys as they were when the reviewer wrote it; the fold resolves them to
members and re-keys the joined set. A split rule keeps its slots in a set of their own.

### 3.6 `se_company_person_precedence`

The spelling trust order, one row per (field, source): `name`: reviewer 20000, ratsit 1000
(reserved), bolagsverket 900, wikidata 600, esef 400. Exported from `precedence.py` by an asset,
as for addresses. No other field has a precedence: birth year and QID are single-source facts and
roles are a union.

### 3.7 Kept as is

`company_person_role_type` (the 25-code role catalog, also seeded for Serbia) is the role
catalog the normalizer maps into. The role `corpscout_person_correction_writer` stays (other
grants share it).

## 4. The normalizer

`normalize_se.py`, pure functions over the suggestion row, versioned `se-person-normalizer-v1`,
with a golden corpus (`tests/fixtures/se_persons/golden.jsonl`) of at least 40 real and
synthetic cases covering every rule below.

4.1 Display spelling. Trim, collapse whitespace, remove titles and honorifics (Dr, Prof, Herr,
Fru, Jur kand, Civ ing and the common Swedish set), keep the delivered casing and diacritics.
When only `full_name` is delivered: a comma form is "Last, First"; otherwise the last token is the
last name and the particle set (von, af, de, van, der, la, le) glues to it ("Carl von Essen":
first Carl, last von Essen). ESEF and Wikidata deliver full names; Bolagsverket delivers the split.

4.2 Identity tokens. Case-fold; fold diacritics to ASCII (Håkan and Hakan meet; Ö and O meet);
split hyphens (Sven-Erik gives sven, erik); strip periods from initials (S.E. gives s, e);
particles fold with the last name (von essen). `first_tokens` is the first token, `middle_tokens`
the rest before the last name, `last_tokens` the last name's tokens.

4.3 Roles. The per-source role maps (moved from `sweden_financial/roles.py`,
`esef_filings/roles.py`, `wikidata/roles.py` into the package) map the delivered label into the
catalog; an unmapped label publishes as itself, lowercased and trimmed (owner ruling 2026-08-28:
never dropped, never bucketed). `role_year` is Bolagsverket's fiscal year, the ESEF document's
fiscal year, and for Wikidata every year from `role_from` to `role_to` or the current year (the
fold expands the span; the normalized row keeps the dates).

4.4 `parse_status`. `ok`: a first and a last token exist. `partial`: one token, or initials
only. `no_person`: the name field holds a role word (styrelseledamot, ordförande, revisor and
the rest of the catalog's labels), a number, a date, a company suffix (AB, HB, KB) or is empty;
the August audit found names and dates in the role field and roles in the name field. Partial
and no_person rows are stored with their notes and never folded into a person.

## 5. The fold

`fold.py` is a pure function over one company's normalized rows (status ok), rules and the
precedence table; `batch.py` drives the ClickHouse I/O in pages of 20,000 companies. Two assets:
`se_company_person_fold` on 64 static hash buckets of `company_id` (no pool: no DuckDB and no
geocoding, buckets run in parallel) and `se_company_person_fold_companies` for the tab's
targeted fold (normalizes the company first, as the address twin does).

### 5.1 Identity

Two rows of one company are the same person when (a) their `first_tokens` and `last_tokens`
are equal and their `middle_tokens` are equal or one side's middle tokens are the unique minimal
superset of the other's (a lone middle name), or (b) both carry the same Wikidata QID; and in
either case not when both carry a `birth_year` and the years differ. Sets are the transitive
closure of that relation (owner choice: the August K3 rule plus the birth-year guard). The
canonical name of a set is the folded tokens of its most complete member (most tokens, then
the longest, then alphabetical); `person_key` is the sha256 of the company id and that canonical
name, so a person keeps their key across folds unless a rule changes the set.

### 5.2 Rules

After the identity grouping: a merge rule joins the sets that contain its keys (keys that no
longer exist are ignored and the rule is reported stale); a split rule moves its slots into a set
of their own; a hide rule marks the set inactive with reason hidden; an inactive rule is ignored.
Rules bind to the fold's output and survive re-folds.

### 5.3 The person row

Per set: `members` from every member (source, slot, display spelling, birth year, QID, data);
`display_name`, `first_name`, `last_name` from the member of the highest `name` precedence
(reviewer above all; ties by the most complete spelling); `text_source` that member's source;
`birth_year` and `wikidata_id` from whichever member carries one (a conflict on birth year
cannot reach here); `sources`, `slots`, `normalized_ids` from all members.

### 5.4 Roles

The union over members of (role code, year) pairs, each with the sources that saw it, sorted by
year then code; `first_year` and `last_year` the min and max year; `current_roles` the codes of
`last_year`. A person who left the board keeps their years and stays active: that is history
worth showing.

### 5.5 `data`

The members' objects merged key by key: a key present in one member is taken; a key present in
several takes the value of the higher `name`-precedence member; reviewer keys win outright.
Nested objects merge the same way one level down; arrays are replaced, not concatenated.

### 5.6 Lifecycle, history, selection

Every person is published and active. A person goes inactive with reason withdrawn only when
every observation behind them is a tombstone (or the reviewer withdrew their own row), hidden
when a hide rule names them. Set replacement per company as for addresses: the fold rewrites
the company's rows; a person absent from the new fold whose observations are gone is written
inactive once, then left alone. History rows are appended before the main write. Selection:
companies with a normalized row, rule or precedence newer than their last fold; rerunning a
folded bucket selects nothing.

## 6. Extractors

`suggestions.py` and the shared `extract.py` helper from basic info (a `SuggestionTarget` for
the person table). One module per source:

- `bolagsverket.py`: from `se_financial_report_signatories` (first and last name, role label,
  fiscal year, signatory kind, report record uid); tombstones on `has_company = 0`.
- `esef.py`: from the country-scoped `se_esef_document_people` view (full name, role or title
  text, document id, fiscal year); when the other session's dedicated extraction table lands it
  is a one-line source change.
- `wikidata.py`: from `wikidata_company_people` joined to `wikidata_persons` and to the SE
  company through `wikidata_company_identifiers` (org number or LEI): label, birth year, QID,
  role label, start and end dates, description and occupations into `data`.
- `ratsit.py`: reserved; not written until the data exists.

Each extractor is a change-scan SQL (`changed_only`) with one `WITH`-bound `now64()` per
statement (never two calls), stamps `suggestion_id` and `suggested_at`, and is covered by a
clickhouse-local test over fixture rows. Job `se_company_person_extract_job` = the three
extractors plus `se_company_person_normalize`; schedule `se_company_person_weekly` Monday 07:15
UTC, STOPPED (the fold stays manual, launched from the backoffice or by backfill).

## 7. Backoffice

The Address tab's six-file split, one module each: `app/lib/se-company-person-entity.server.ts`
(one read over the six tables into a detail object, the reviewer writes, the targeted-fold
launch), `app/components/admin/se-person-workspace.tsx`, `se-person-edit-sheet.tsx`,
`app/lib/se-person-fields.ts` (client-safe catalog and validation), `se-person-decision-form.ts`
(intents), `fold-run-poller.tsx` reused as is. Route `admin-se-company-person.tsx` at
`/admin/se/company/:companyId/people`.

Left column: the company's persons, active first, with display name, birth year, QID, current
roles and the sources that saw them. Right panel for the selected person: the members block
(every source's spelling and extras side by side), the roles timeline (year by role by source),
the `data` object, the history and the raw evidence rows behind the members.

Actions: Add (a reviewer draft: name, birth year, roles with years, data; Activate writes a
reviewer suggestion at slot `r` plus digits and launches the targeted fold), Correct (a reviewer
row plus a hide rule on the corrected key unless the reviewer's text folds back to the same key),
Remove (a hide rule; a reviewer-only row is tombstoned instead), Merge (two or more selected
persons, a merge rule), Split (the slots to separate, a split rule), Reset (the rules on a
person set inactive), Fold now. Every write is an append; nothing edits a published row. The
edit sheet's `data` field is a JSON editor validated as an object; the note field allows line
breaks and refuses other control characters, as for addresses.

The admin People list `/admin/se/people` is rewritten as a plain server-paged list over the
main table with filters for company, name, source, role, year and status, counts strip, and a
link into each company's tab. The old person workspace, pipeline and stale-corrections pages
are deleted, not ported.

## 8. Readers and retirement

Readers after the cutover: the serving view's `has_people`, `people_bolagsverket` and
`people_esef` (active rows of the main table, `has(sources, ...)`), through the builder
constant; the backoffice tab and list. Nothing else.

Retired in slice 0, before anything new runs: the `defs/company_people/` package
(`source_views.py`, `normalization.py`, `roles.py`, `corrections.py`, `identity_eval.py`,
`merge.py`), its jobs, the correction sensor and its tests; the backoffice routes
`admin-se-people`, `admin-se-people-person`, `admin-se-people-pipeline`,
`admin-se-people-stale-corrections`, `admin-se-company-people`, their libs and components and
tests, and the person writer functions in `clickhouse.server.ts`; the public Management section
(`management-section.tsx`, the management part of `company-sections.server.ts`, the dbt models
`company_management_current_build.sql` and the people half of
`company_section_item_source_links_build.sql`, with `company_management_current` and the
orphaned `company_management_observations` on the drop list; the section component and the
French officers chain that shares it stay, only the Swedish loader and section go); the tables `se_company_person`, `se_company_person_role`, `se_company_person_role_draft`,
`se_company_person_correction`, `se_company_person_enrichment_observation`,
`se_company_person_collision_candidate`, `se_company_person_v1_role_baseline` if present, the
views `se_company_person_bolagsverket`, `se_company_person_esef`, `se_company_person_wikidata`
and `company_management_current`, dropped by an owner-run script under the ledger policy with
the migration-file edits (000288 to 000296 as far as they declare these objects, 000330, 000331,
000395's re-issue of the ESEF view, 000328/000332/000333 already emptied or historical). The
serving view is re-pointed at the new tables in the same migration that creates them, so the
flags read empty tables until the first fold.

## 9. Slices

0. Tables, normalizer, retirement: the six tables and the serving re-point in one migration; the
   normalizer with its golden corpus and the normalize asset; the deletions of section 8 and the
   owner-run drops.
   Shipped 2026-09-09 (plan `2026-09-09-se-company-person-0-tables-normalizer-retirement.md`,
   main 49ef7ed2): migration 000396 (renumbered from 000395 at merge because the ESEF slice took
   it) created the six tables with `data String DEFAULT '{}'` under a `JSONType(data) = 'Object'`
   CHECK and re-pointed the serving view's people flags at `se_company_person_v2` in place;
   `se_company/person/` holds `tables.py`, `roles.py` (the three per-source role maps moved in),
   `normalize_se.py` (`se-person-normalizer-v1`, 55-case golden corpus), `normalize.py` and the
   `se_company_person_normalize` asset (clickhouse-local proof under both `join_use_nulls`
   settings). Deleted: the `company_people` package with nine jobs, the correction sensor, its
   leaves and tests (19 files, 13,355 lines), the three old role-map modules, the backoffice
   People area (four admin pages, the company People tab, seven components, fifteen libs, the
   person writer and job constants, nav and breadcrumbs), the Swedish Management section's
   loader, section entry, dbt model, the people half of the source-links model and the
   `company_serving` MANAGEMENT contract (the component and the French officers chain stay);
   twenty-four historical migration files emptied or narrowed under the ledger policy with two
   guards. Prod: deployed, 000396 applied in seconds (ledger 396 clean), the 19:45 UTC serving
   refresh succeeded with `has_people` at 0 (13.8 min); precheck clean (no view read any of the
   thirteen); the owner's word ("do it") and the thirteen drops ran in two seconds (about 4.5
   GiB: `company_management_current` and its build twin 5.6M rows each, `_observations` 19.5M,
   the role drafts 3.5M, the collision candidates, the empty person, role, correction and
   enrichment tables, the three views; `se_company_person_v1_role_baseline` never existed);
   postcheck all dropped with the twelve kept objects present, `system.tables` 453 to 441; the
   admin companies list, the company area without a People tab, the Address tab, the deleted
   routes (404) and the SE and FR public detail pages render as expected. Rulings on the way:
   `data` is JSON text (the pinned driver cannot read the native type), `role_key` is a real
   column, the orphaned dbt build target joined the drops, the Management removal is Sweden-only.
1. Extractors: Bolagsverket, ESEF, Wikidata; the extract job and the stopped weekly; a prod run
   with the `parse_status` distribution per source and a spot check of twenty Bolagsverket rows.
2. Fold: precedence, fold, batch, the two fold assets; the first full fold over 64 buckets;
   readouts (persons, members per person, sources sets, roles per year, no key twice).
3. Backoffice: the tab and the list; owner smoke.
4. Rename and docs: `se_company_person_v2` to `se_company_person` with the serving re-point,
   the design doc under the package, memory.

Each slice is one plan executed with subagent-driven development, reviewed, merged and deployed
before the next; shipped records are appended here as for addresses.

## 10. Names

Tables `se_company_person_suggestion`, `se_company_person_normalized`, `se_company_person_v2`
(renamed `se_company_person` in slice 4), `se_company_person_history`, `se_company_person_rule`,
`se_company_person_precedence`; catalog `company_person_role_type` (kept). Package
`dagster_v3.defs.se_company.person` (`tables`, `normalize_se`, `normalize`, `suggestions`,
`bolagsverket`, `esef`, `wikidata`, `precedence`, `fold`, `batch`, `assets`, `jobs`, later
`ratsit`). Assets `se_company_person_suggestions_<source>`, `se_company_person_normalize`,
`se_company_person_fold`, `se_company_person_fold_companies`,
`se_company_person_precedence_clickhouse`; job `se_company_person_extract_job`; schedule
`se_company_person_weekly`. Backoffice `app/lib/se-company-person-entity.server.ts`,
`app/lib/se-person-fields.ts`, `app/lib/se-person-decision-form.ts`,
`app/components/admin/se-person-workspace.tsx`, `app/components/admin/se-person-edit-sheet.tsx`,
routes `admin-se-company-person.tsx` and `admin-se-people.tsx`.
