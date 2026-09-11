# Ratsit as a source for the SE company entities — design

Date: 2026-09-11. Owner decisions 2026-09-10/11 (people as proposed with spelling precedence
1000; establishments as workplace addresses with the town fix in the same slice; the Ratsit
legal form dropped because it fills nothing; the dead backoffice Ratsit inspector removed).

## 1. Purpose

Ratsit's normalized tables (`se_ratsit_company`, `se_ratsit_responsible_people`,
`se_ratsit_establishments`, written by the `sweden_ratsit` pipeline's `se_ratsit_normalized`
multi-asset) feed the three SE company entities in full:

- basic info: the description (and Swedish description) for the ~860k companies the existing
  extractor never visited;
- people: a new person extractor (VD, deputies, external signatories, procurists, service
  recipients) with a birth year for 92% of rows;
- addresses: the company postal address with the correct postal town, plus one `workplace`
  address per establishment.

The scope is processing the tables that exist. Scheduling the Ratsit scan (`sweden_ratsit`,
CloakBrowser, pilot prefix, no schedule) is out of scope, as is a Ratsit establishments entity,
Ratsit's financials, industry codes and summaries (the summaries are comparative-statistics
prose, not descriptions), and Ratsit's legal form (every basic-info row already carries a
legal form code from SCB or Bolagsverket; Ratsit's text would fill nothing).

The dead backoffice page `/admin/se/companies/ratsit` (the Ratsit request inspector) reads
`se_company_ratsit_crawl_results`, dropped by migration 000334; it is deleted.

## 2. Facts the design rests on (prod, 2026-09-10/11)

Sources:

- `se_ratsit_company`: 947,294 rows / 947,200 companies (60% of the 1.57M active), rows
  appended per scan and never deleted (`ORDER BY (company_id, result_sha256,
  normalizer_version)`); the current report is the newest `normalized_at` per company under
  `RATSIT_NORMALIZER_VERSION`. `business_description` on 878,596; `source_date_modified`
  (Date32) is the scan date; address columns `address_street/postal_code/locality/county`;
  928,560 companies carry a street and a postcode.
- `se_ratsit_responsible_people`: 301,536 rows (301,081 joined to the current report) /
  236,814 companies; `person_index`, `display_name(_raw)`, `name`, `age`, `identity_available`,
  `role` (VD 196k, Extern firmatecknare 51k, Vice VD 26k, Extern VD 17k, Delgivningsbar person
  8k, Extern vice VD 1.4k, Prokurist 1.3k, Ställföreträdande VD 21, Aktuarie 2), `profile_url`
  `https://www.ratsit.se/<YYYYMMDD>-<Name>_<Town>/<token>`. 277,592 rows carry the birth date
  in the URL, 277,646 the trailing token (a per-person id: 191,434 distinct tokens, the same
  person at several companies shares it), 23,432 rows are nameless (GDPR-limited evidence:
  a role without a name or URL). 31 (company, token) pairs carry two rows (a `Delgivningsbar
  person` who is also VD or Vice VD). No scan date on this table.
- `se_ratsit_establishments`: 846,718 rows / 766,313 companies; 732,626 carry a street and a
  postcode; `identifier` on every row, unique within a company on all but 324 rows;
  `name`, NACE mapping, employee range; max 1,718 per company (131 companies over 100).
  The establishment `address_locality` is the postal town (4 of 732,626 differ from the
  register town for the postcode).

State of the existing extractors:

- `se_basic_info_suggestions_ratsit` (ratsit-v2) last ran 2026-09-08 and
  `se_company_address_suggestions_ratsit` (ratsit-address-v1) 2026-09-06, both over the
  83,790 companies Ratsit had normalized by 08-30. The 863,504 companies normalized on 09-09
  never reached any entity; the three weeklies (`se_company_basic_info_weekly`,
  `se_company_address_weekly`, `se_company_person_weekly`) are STOPPED. The change scan
  picks the unvisited companies up on the next run (no suggestion row yet).
- Basic info today: `description_source` = ratsit for 75 companies, empty for 667,794.
  Description precedence: reviewer 20000, llm 2000, esef 800, wikidata 600, bolagsverket 400,
  ratsit 300 (`basic_info/precedence.py`); `description_sv`: reviewer, llm, bolagsverket 400,
  ratsit 300.
- Addresses today: Ratsit contributes 83,696 `postal` rows in slot `company`. The Ratsit
  company row carries the municipality as the locality on 260,862 of 928,491 addresses
  (Stockholm for Bromma 9,871, Göteborg for Västra Frölunda 6,930, Nacka for Saltsjö-Boo
  4,575, Gotland for Visby 3,355 ...). The normalizer's `city` is part of `location_key`,
  `address_key` and every fold compatibility test, so a municipality-as-town row folds into
  its own set beside the register address and geocodes as its own key: the ~26k near-duplicate
  second addresses of the address entity's follow-up list.
- Postcode → postal town is deterministic in the SCB register: 15,698 postcodes, 38 with
  more than one town spelling, 12 with a minority above 5%. Every Ratsit postcode (company
  and establishment) is known to the register.
- People today: 236,814 companies have Ratsit people; 105,759 of them have no person in
  `se_company_person` at all. Person precedence already reserves ratsit at 1000 and the
  ClickHouse precedence table carries the row.
- Address kinds (`address/tables.py::KINDS`): `postal, visiting, visiting_or_postal,
  registered, workplace, unknown`; the address spec (2026-09-06, section 3) says
  "establishments come later as kind `workplace`"; the backoffice labels it "Workplace".

## 3. Slice 1 — basic info from every Ratsit company, and the dead page

No extractor change: `basic_info/ratsit.py` stays at ratsit-v2 (legal name, status,
description, description_sv, description_language). The work is the prod run plus the
backoffice deletion.

### 3.1 The dead Ratsit inspector

Delete, in `corpscout/services/backoffice`:

- `app/routes/admin-se-companies-ratsit.tsx`
- `app/components/admin/se-ratsit-request-browser.tsx`
- `app/lib/se-ratsit-results.server.ts`, `app/lib/se-ratsit-results.ts`
- `tests/admin-se-companies-ratsit.test.ts`, `app/lib/se-ratsit-results.test.ts`,
  `app/lib/se-ratsit-results.server.test.ts`

Edit:

- `app/routes.ts`: remove `route("ratsit", "routes/admin-se-companies-ratsit.tsx")` under
  `se/companies`.
- `app/lib/se-companies-tabs.ts`: remove `{ value: "ratsit", label: "Ratsit" }` from
  `SE_COMPANIES_TABS`; the tab helpers stay generic.
- `app/routes/admin-se-companies-layout.tsx`: the header prose no longer says "inspect Ratsit
  captures".
- `app/components/admin/admin-sidebar.tsx`: the comment listing the tabs drops Ratsit.

Nothing else imports the cluster; no e2e test references the route. The generated
`.react-router/types/.../+types/admin-se-companies-ratsit.ts` regenerates itself. Live
readers of `source = 'ratsit'` rows in the entity tables (people, addresses, basic info) are
untouched. `pnpm typecheck` stays clean and the vitest suite changes only by the three deleted files'
tests (the suite has known failures outside this cluster; the deletion adds no test).

### 3.2 Prod run

1. `se_basic_info_suggestions_ratsit` with `execute: true, page_size: 10000`: the change
   scan selects every company without a Ratsit suggestion row (the 863,504) plus any whose
   report is newer. Expect 87 pages, ~863.5k candidates.
2. `se_company_basic_info_fold` over all 64 buckets (changed-only, the default: the fold's
   watermark sees the newer suggestions). Readout: `description_source = 'ratsit'` and
   `description_sv_source = 'ratsit'` counts with FINAL (expect several hundred thousand,
   the companies with no Bolagsverket description), companies with an empty description
   before vs after.
3. The hourly serving refresh follows on its own.

Record the counts in section 8.

## 4. Slice 2 — the Ratsit person extractor

### 4.1 Module and shape

`src/dagster_v3/defs/se_company/person/ratsit.py`, on the shape of `bolagsverket.py` /
`esef.py` / `wikidata.py`: `PERSON_SOURCE = "ratsit"`,
`RATSIT_PERSON_EXTRACTOR_VERSION = "ratsit-person-v1"`, `RATSIT_COLUMN_SQL` covering the
16 `PERSON_SELECT_COLUMNS`, `ratsit_live_sql(scoped)`, `ratsit_changed_scope_sql()`,
`ratsit_select_sql()` (live rows UNION ALL per-slot tombstones through
`suggestions.person_select_sql`), `ratsit_current_sql()`, and the asset
`se_company_person_suggestions_ratsit` from `define_person_suggestion_asset` with
`deps=[dg.AssetKey("se_ratsit_company"), dg.AssetKey("se_ratsit_responsible_people")]` (the
table-named keys the `se_ratsit_normalized` multi-asset declares; `se_ratsit_normalized`
itself is the function name, not a key, and a dep on it makes a phantom node) and the
`normalizer_version` select parameter.
`assets.EXTRACTOR_SOURCES` becomes `("bolagsverket", "esef", "wikidata", "ratsit")`, which
carries the asset into `se_company_person_extract_job`, the weekly's run config and the
normalize asset's deps. The state-hash change scan and the per-slot tombstones are the
shared ones; nothing about the scan changes.

### 4.2 Live rows

The current report per company: `se_ratsit_company FINAL WHERE normalizer_version =
%(normalizer_version)s`, newest `normalized_at` (ties: highest `result_sha256`), `LIMIT 1 BY
company_id`; people rows join it on `(company_id, result_sha256, normalizer_version)`, so
superseded scans' rows never appear. A row is live only when `name` is non-empty after trim
(23,432 nameless rows are skipped: they carry no identity). Columns:

| column | value |
| --- | --- |
| `company_id`, `source` | the company; `'ratsit'` |
| `slot` | the URL token `extract(profile_url, '/([A-Za-z0-9_-]+)$')`; when the same token appears on more than one row of the report, `concat(token, ':', lowerUTF8(trim(role)))`; when there is no token, `concat('idx:', toString(person_index))` |
| `source_record_id` | `concat('ratsit:', toString(result_sha256), ':', toString(person_index))` |
| `full_name` | `name` trimmed; `first_name`, `last_name` NULL (the normalizer splits) |
| `birth_year` | `toUInt16OrNull(substring(extract(profile_url, '^https://www\\.ratsit\\.se/(\\d{8})-'), 1, 4))`, NULL when the URL carries no date |
| `wikidata_id` | NULL |
| `role_original` | `role` trimmed (the Swedish label as delivered) |
| `role_key` | NULL: Ratsit delivers no machine code; `roles.py` maps the label |
| `fiscal_year` | `toYear(source_date_modified)` of the current report, else `toYear(normalized_at)`: the role is current at the scan |
| `role_from`, `role_to`, `document_ref` | NULL |
| `data` | `toJSONString(mapFilter((k, v) -> v != '', map(...)))` over String values coalesced to `''`: `age` (`ifNull(toString(age), '')`), `identity_available` (`'true'`/`'false'`), `profile_url`, `display_name_raw`, `ratsit_person_id` (the token), `external` (`'true'` when the role starts with `Extern`, else `'false'`); the filter drops the keys whose source value is NULL or empty, so a `map` alone (which would render `"age":null`) is not enough |

The slot rule keeps a person's slot stable across scans (the token is Ratsit's person id)
so split and merge rules keyed on slots survive re-scans; the 31 two-row tokens get
role-qualified slots; `idx:` slots exist only for the handful of named rows without a URL.

### 4.3 Roles

`roles.py::SOURCE_ROLE_MAPPINGS["ratsit"]`, keyed by the lowercased, trimmed label as
`role_code_for` matches it:

| Ratsit label | canonical code |
| --- | --- |
| `vd`, `extern vd` | `chief_executive_officer` |
| `vice vd`, `extern vice vd`, `ställföreträdande vd` | `deputy_chief_executive_officer` |
| `extern firmatecknare` | `legal_representative` |
| `prokurist` | `procurist` |
| `delgivningsbar person` | `other_representative` |

`aktuarie` (2 rows) stays unmapped and publishes as itself, per the passthrough rule. The
external flag lives in `data.external`, not in the code. `roles.py` line 93's sentence that
ratsit has no map is updated; `SOURCE_ROLELESS_CODES` gets no ratsit entry.

### 4.4 Wording, docs, backoffice

- `precedence.py` docstring and `docs/person-design.md` (line 19, the module table, the
  extractor table, and the `jobs.py` row that says the extract job is "the three extractors
  plus the normalize asset", now four): ratsit is a source with an extractor, spelling precedence 1000 above
  Bolagsverket because Ratsit's names are register spellings with a birth date.
- `tests/test_se_company_person_precedence.py`: the "reserved, no extractor yet" wording goes;
  the pinned dict is unchanged.
- Backoffice `app/lib/se-person-fields.ts`: `MAIN_PERSON_SOURCES` stops excluding `ratsit`
  (the People list's Source filter offers Ratsit); the comment and
  `tests/se-person-fields.test.ts` follow. `SOURCE_LABELS.ratsit = "Ratsit"` exists. The tab
  renders `data` as JSON, so the new keys need no UI work.

### 4.5 Tests

- `tests/test_se_company_person_extractors_sql.py`: ratsit joins the `EXTRACTORS` dict (16
  columns, `%(company_ids)s` twice, UNION ALL tombstone shape, state-hash scope) plus a pinned
  `test_ratsit_slot_is_the_profile_token_with_role_and_index_fallbacks` that asserts the
  slot expression, the nameless filter, the birth-year and fiscal-year expressions and the
  `data` keys.
- `tests/test_se_company_person_extractors_clickhouse_local.py`: the Ratsit tables cannot ride
  `WANTED_CREATES` (it matches `CREATE TABLE` statements by name in a migration, and 000346
  only `ALTER TABLE ... ADD COLUMN`s the v2 people columns; `se_ratsit_company`'s DDL already
  sits in `tests/fixtures/se_basic_info_source_tables.sql`, which this test loads), so
  `tests/fixtures/se_company_person_source_tables.sql` gets the `se_ratsit_responsible_people`
  CREATE with the 000346 columns inlined. The fixture file gets two companies: one with a VD carrying a URL with a date and
  token, a `Delgivningsbar person` sharing that token (role-qualified slot), a nameless row
  (skipped) and a row without a URL (`idx:` slot); the second company's older scan must not
  leak (two reports, only the newest counts). Assertions: rows, slots, birth year, fiscal
  year from `source_date_modified`, `data.external`, and a second run that tombstones a
  vanished slot.
- `tests/test_se_company_person_roles.py` (new; `role_code_for` is exercised through the
  normalize tests today): every label in the table above maps, `aktuarie` passes through,
  `Extern VD` keeps the CEO code.
- Unit count pins that enumerate extractor sources or job selections update by construction
  (`EXTRACTOR_ASSET_NAMES`).

### 4.6 Prod run

1. Deploy. 2. `se_company_person_suggestions_ratsit` `execute: true, page_size: 10000`
(every company is new to the source: ~236,814 companies, ~277k live rows). 3.
`se_company_person_normalize` (`changed_only: true`). 4. `se_company_person_fold` backfill over
the 64 buckets (changed-only: the watermark sees the newer normalized rows; pool
`se_company_person_fold`, serial). Readouts: main rows with `has(sources, 'ratsit')`,
companies gaining their first person (expect ~106k), created/updated/merged counts from the
run metadata, a spot check of a company with Bolagsverket + Ratsit rows (the Ratsit spelling
wins, the birth year lands), the People tab on the owner's dev server. 5. The serving
refresh follows (`has_people` grows by the first-person companies).

## 5. Slice 3 — Ratsit addresses: the postal town and the establishments

### 5.1 The postal-town dictionary

The Ratsit address SQL joins a dictionary subquery `towns(postal_code, post_town)` built from
the SCB register rows the SCB address extractor reads (`corpscout.se_scb_companies FINAL`
with `has_company = 1`): the postcode with spaces removed, the most frequent trimmed `post_town`
spelling per postcode (ties broken by the alphabetically first spelling; `LIMIT 1 BY
postal_code` after `ORDER BY count() DESC, post_town`). Every Ratsit row's `post_town` is the
dictionary town for its postcode; when the postcode is unknown to the register (none today)
the row keeps Ratsit's locality. The subquery is recomputed per page (SCB is 1.8M rows, a
second or two); no new table, no migration. The asset declares the read:
`deps=[dg.AssetKey("se_ratsit_company"), dg.AssetKey("se_ratsit_establishments"),
dg.AssetKey("sweden_company_scb_companies_clickhouse")]` (the SCB key `address/scb.py` uses;
the current `se_ratsit_normalized` dep is a phantom node and goes). The lineage ruling holds:
the extractor reads a register source table, never another extractor's output nor the fold.

### 5.2 Rows

Live rows for a company come from its current report (as in 4.2). Two kinds:

- The company row, as today: slot `company`, kind `postal`, `source_record_uid`
  `concat('ratsit:', toString(result_sha256))`, `street_address`, `postal_code`, `county` as
  delivered (trimmed, empty → NULL), `post_town` from the dictionary, `raw_address` and
  `care_of` NULL (the normalizer splits the care-of Ratsit glues onto the street; do not put
  the original locality into `raw_address`, the normalizer parses that column).
- One row per establishment of the report with a non-empty `address_street` and
  `address_postal_code`: slot `concat('est:', identifier)`, or `concat('est:', identifier, ':',
  toString(establishment_index))` when the identifier repeats within the report (324 rows);
  kind `workplace`; `source_record_uid` `concat('ratsit:', toString(result_sha256), ':est:',
  toString(establishment_index))`; the same address columns with the dictionary town.
  Establishment `name` and employee counts are not carried (the suggestion has no data
  column); the slot keeps the establishment identifier for a later establishments entity.

`observed_at` on every row is the report's `normalized_at`. The address module gets its own
`ratsit_current_sql` over `se_ratsit_company FINAL` (newest report per company, `observed_at`
= `normalized_at`) instead of reusing basic info's translation-aware one, whose translation
stamp exceeds the stored address `observed_at` and re-selects the 77k translated companies on
every address run. The extractor version becomes `ratsit-address-v2`.

### 5.3 Tombstones

Ratsit's address extractor writes none today. With many slots per company it must: for the
paged companies, every stored live Ratsit slot (a row in `se_company_address_suggestion FINAL`
with `source = 'ratsit'` and a non-NULL `street_address`) that is absent from the live set
gets a NULL row (its slot and kind kept, address columns NULL, `observed_at` the current
report's), the UNION ALL / LEFT ANTI JOIN shape of `person/suggestions.py::person_select_sql`.
No address-side helper exists (SCB and Bolagsverket tombstone by nulling the single-slot
row on `has_company = 0`), so the slice adds one in `address/suggestions.py`
(`address_select_sql(*, live_sql, source)` returning live UNION ALL tombstones) and the
Ratsit module uses it. The normalizer files a NULL row as `no_address` and the fold drops
that slot from the set. Since
Ratsit never deletes companies, a whole-company tombstone is not needed.

### 5.4 Fold effect

`kinds` on a published row is the distinct member kinds in member order, so the 288,840
establishments that share the company's postal street and postcode merge into the postal
row (`kinds = ['postal', 'workplace']` or with the register kinds first). With the dictionary
town the Ratsit company row now matches the register row's `city`, so it joins the register
set; the old municipality-town sets (the ~26k duplicates and every other Ratsit-only set
whose town changes) are withdrawn by the whole-set rewrite. Ratsit's spelling precedence
stays 300. Location keys of corrected rows equal the register keys, so they geocode from the
cache; establishments at new locations are warmed before the fold.

### 5.5 Tests

- `tests/test_se_company_address_extractors_sql.py`: the pinned Ratsit test asserts both
  kinds, both slot expressions, the dictionary join on the SCB table, the UNION ALL tombstone
  branch, `%(company_ids)s` bound in every branch, and `ratsit-address-v2`; the thirteen-
  column loop still passes.
- The clickhouse-local address extractor test (or a new one on the person test's pattern):
  fixtures with `se_scb_companies` rows that establish a postcode's town, a Ratsit company
  whose locality is the municipality (row comes out with the register town), two
  establishments (one sharing the postal address, one elsewhere, one without a street that
  is skipped), a repeated identifier, and a second run after an establishment vanishes
  (tombstone row).
- Fold unit test (`tests/test_se_company_address_fold*.py`): a `workplace` member sharing the
  postal location publishes one row with `kinds` `['postal', 'workplace']`.

### 5.6 Prod run

1. Deploy. 2. `se_company_address_suggestions_ratsit` with `execute: true, page_size: 10000,
since: "2000-01-01T00:00:00Z"` (the town fix changes rows without moving `observed_at`, so
the change scan alone would skip the 83,696 visited companies): every company with a report
is in scope, ~95 pages, ~947k company rows (the company row is emitted whether or not it
carries a street, as today; the normalizer files the streetless ones as `no_address`) +
~732k establishment rows. 3. `se_company_address_normalize` (`changed_only: true`).
4. `se_address_geocodes_warm` (chunk size 150,000 as in the address slices) so new location
keys are matched in bulk.
5. `se_company_address_fold` backfill over the 64 buckets (changed-only; pool
`sweden_address_osm_duckdb`; ~75 s per bucket last time, expect longer with ~1M changed
companies). Readouts: rows with `has(sources, 'ratsit')`, rows with `has(kinds,
'workplace')`, withdrawn history rows for the run, the Malmö/Oxie and Stockholm/Bromma
samples now folded into the register set, `se_companies_serving` address counts after the
refresh, the Address tab on the owner's dev server for a company with establishments.

## 6. Risks and rulings

- Ratsit's descriptions at 300 lose to Bolagsverket's at 400; the gain is the ~667k
  companies without any description. If the owner wants Ratsit's longer texts to win, that
  is a precedence change plus a `changed_only=False` fold, out of this scope.
- `fiscal_year` from the scan date means a re-scan next year adds a new year to the same
  slot (the fold's role years union); a role that disappears is tombstoned and the last
  year stays in history. Same rule as Wikidata's dateless roles.
- The 131 companies with over 100 establishments (max 1,718) make large fold sets; the fold
  is per company and linear in members per candidate, so they cost seconds, not minutes.
- The serving view has only `has_people`; no per-source flags change.
- Weeklies stay STOPPED (owner decision 2026-09-08); the runs are launched by hand.
- The extractors read Ratsit tables written by the 128-bucket normalize asset; the person
  and address extractors are unpartitioned, so the 128-vs-64 mapping problem of the
  basic-info lineage does not arise.

## 7. Names

Modules `se_company/person/ratsit.py` (new), `se_company/address/ratsit.py` (rewritten),
`se_company/basic_info/ratsit.py` (unchanged). Assets `se_company_person_suggestions_ratsit`
(new), `se_company_address_suggestions_ratsit`, `se_basic_info_suggestions_ratsit`. Versions
`ratsit-person-v1`, `ratsit-address-v2`, `ratsit-v2`. Slots: people = the profile token;
addresses = `company` and `est:<identifier>`. Kind `workplace`. Deleted backoffice files in
3.1.

## 8. Slices

Each slice is one plan under `docs/superpowers/plans/`, executed with subagent-driven
development, reviewed, merged to main and run on prod before the next; shipped records are
appended below each item.

1. Basic info from every Ratsit company (prod run) and the dead inspector page (backoffice).
   Shipped 2026-09-11 (plan `2026-09-11-se-ratsit-1-info-and-inspector.md`, main 7b66e6cf):
   the backoffice lost the dead Ratsit request inspector — 11 files, +2/−1,382 lines (the
   route, its component, two libs, three test files, the `ratsit` route line, the tab entry,
   the layout prose, the sidebar comment); typecheck clean, the vitest suite went from
   128 files / 1,352 tests to 125 / 1,340 with the same pre-existing failures; the owner's
   dev server answers 200/200/404 for info, geocoding and the old ratsit path. Prod: the
   preview of `se_basic_info_suggestions_ratsit` found exactly the 863,504 companies the
   spec predicted (87 pages); the execute run (6cb86fdb) inserted 863,504 rows in 4 min and
   a second preview found 0 candidates (converged); the Ratsit suggestion set now covers all
   947,200 companies with a report (878,502 with a description, 213,283 already translated).
   Fold backfill cgocdvzm over the 64 buckets: 64/64 SUCCESS in 66 min (22:53–23:59 UTC),
   changed-only honoured (≈13,550 of ≈55,000 companies considered per bucket), 715 companies
   changed in total. Readouts before → after (FINAL): `description_source = 'ratsit'`
   75 → 789, `description_sv_source = 'ratsit'` 77 → 792, companies with no description
   667,794 → 667,080, no Swedish description 668,465 → 667,750, `legal_name` and `status`
   from Ratsit 0 → 0; the 23:45 serving refresh ran over the half-folded table and succeeded
   (finished 23:57:29, no exception), the 00:45 one carries the rest; the three weeklies
   stayed STOPPED. Finding: Ratsit's `business_description` is the registered
   verksamhetsbeskrivning Bolagsverket already delivers — identical on 854,687 of the
   877,710 companies with both (871,818 share the first 60 characters, same average length
   177), so of 878,502 Ratsit-described companies 876,816 already publish Bolagsverket's text
   at 400 and only 626 had none; the 667k description-less companies are mostly inactive
   register-only rows (68,381 in Ratsit, 626 with a Ratsit text). Ratsit is therefore a
   fallback description source for a few hundred companies, and a precedence change would
   gain nothing. Rulings: the extract and the fold ran ahead of the merge (the code they run
   is unchanged since 2026-09-08, the merge only carried the backoffice deletion); preview
   before execute.
2. The Ratsit person extractor, roles, wording, backoffice filter; prod extract, normalize,
   fold.
3. Ratsit addresses: the postal-town dictionary, the establishments as `workplace`, per-slot
   tombstones; prod re-extract with `since`, normalize, warm, fold.
