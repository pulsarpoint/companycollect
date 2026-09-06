# SE company addresses: raw suggestions, normalized suggestions, fold, history

Date: 2026-09-06. Status: approved in brainstorming, awaiting the owner's review of this file.

Second entity on the shape of the basic-info design
(`2026-09-03-se-company-basic-info-design.md`). It replaces the address model that went live
on 2026-08-24 (`2026-08-23-se-company-address-design.md`: two per-source artifacts, a merged
final with copied geocodes, a correction ledger with a sensor) and the shared-identity chain
the geocoder takes its demand from. Nothing of the old model is reused; it runs beside the new
one until the cutover in section 9 retires it.

## 1. Why

Addresses were the first domain the basic-info review named as a half-model: per-source
artifacts plus a correction ledger, no history, an evidence-hash staleness rule for
corrections, and coordinates copied into company rows at resolve time so every matcher
improvement had to re-resolve companies. The geocoder itself (the versioned matcher, the
outcome store, the centroid fallback) is sound; the way it is wired to companies is not: its
identity is built from raw observations by a second normalization that the published table
cannot see, so addresses that are never published still get matched, and six indirect readers
keep the chain from being retired.

Owner decisions, 2026-09-05 and 2026-09-06:

- The same shape as basic info: per-source tables, one suggestion table per entity, one main
  table written by a Python fold, one history table, the reviewer a source like any other.
- A company has several active addresses. Every address that survives the fold is published
  and active; there is no single winner per company. The extractor list is the trusted-source
  list: an address delivered by any source with an extractor is published, whether or not
  another source delivers it. Precedence never filters addresses (decision 2026-09-06).
- Two sources that deliver the same normalized address contribute to one published address.
  "Same" is compatibility, not text equality: a suggestion missing a part (house number,
  care-of, unit) merges into the more complete one, and the complete text is published.
- Geocoding is a deterministic function of the address text under a matcher version and an
  OSM reference version. It runs inside the fold on every address change, silently, and its
  result lives on the address row. The reviewer never checks or confirms coordinates.
- Normalization is its own stored layer between the raw suggestions and the fold, with its
  own version, so a parser fix re-normalizes stored text without touching sources or folds.
- The reviewer can add, correct and remove addresses. Removal is a per-company rule keyed by
  the address; correction is a removal plus a reviewer address written together.
- Sources in the first cut: SCB, Bolagsverket, Ratsit's company address, the reviewer. ESEF
  has no address columns in our tables (a separate extraction would be needed); Ratsit's
  establishments come later as kind `workplace`.
- Build beside the current model, prove parity, switch readers, retire the old chain in an
  owner-gated step.

## 2. Scope

In scope: the six tables, the Swedish normalizer and the normalize asset, the three
extractors, the fold with compatibility grouping, rules, set replacement, history and
in-page geocoding, the geocode cache adoption, the precedence export, the backoffice Address
tab with its five actions, parity, the reader switch and the retirement list.

Out of scope: the workplace extractor, ESEF address extraction, other countries' normalizers
(the normalizer is one function per country; only Sweden is written), any change to the
matcher, the workbench scripts, the centroid tables or the OSM refresh, and the sensor
(manual folds first, as for basic info).

## 3. Tables

All in database `corpscout`, ClickHouse 26.5, golang-migrate migrations numbered after the
ledger head at plan time (first line `CREATE DATABASE IF NOT EXISTS corpscout;`, no `;`
inside comments, last line a statement). Every `company_id` column carries
`CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')`.

### 3.1 `se_company_address_suggestion` (raw)

One current row per company, source and slot: what the source delivered, never normalized.

```
company_id            String
source                LowCardinality(String)   -- scb, bolagsverket, ratsit, reviewer, reviewer_draft
slot                  String                   -- '' for scb/bolagsverket, 'company' for ratsit, a uuid for reviewer rows
suggestion_id         FixedString(64)          -- sha256(company_id, source, slot, suggested_at): this version's id
source_record_uid     String                   -- the source row this came from; '' for reviewer rows
observed_at           DateTime64(3, 'UTC')
kind                  LowCardinality(String)   -- postal, visiting, visiting_or_postal, registered, workplace, unknown
raw_address           Nullable(String)         -- the packed string when the source delivers one (Bolagsverket)
care_of               Nullable(String)         -- as delivered
street_address        Nullable(String)         -- as delivered (street line, may include a box)
postal_code           Nullable(String)         -- as delivered
post_town             Nullable(String)         -- as delivered
county                Nullable(String)         -- as delivered (Ratsit)
country_code          Nullable(String)         -- as delivered, NULL when the source has none
decided_by            Nullable(String)         -- reviewer rows only
note                  Nullable(String)         -- reviewer rows only
replaces_key          Nullable(FixedString(64)) -- reviewer rows written by Correct: the published key they replace
suggested_at          DateTime64(3, 'UTC')     -- version
source_run_id         String
extractor_version     LowCardinality(String)
ENGINE = ReplacingMergeTree(suggested_at) ORDER BY (company_id, source, slot)
```

A source that stops delivering an address for a slot, or stops delivering the company,
writes a row with every address column NULL. A reviewer removal of the reviewer's own address
is the same NULL row under that slot.

### 3.2 `se_company_address_normalized`

Same key and row count as 3.1, written only by the normalize asset.

```
company_id            String
source                LowCardinality(String)
slot                  String
normalized_id         FixedString(64)          -- sha256(company_id, source, slot, normalized_at): this version's id
suggestion_id         FixedString(64)          -- the raw version this was computed from
suggested_at          DateTime64(3, 'UTC')     -- that raw version's stamp
kind                  LowCardinality(String)   -- carried through
care_of               Nullable(String)
box                   Nullable(String)         -- box number, digits and letters
street_name           Nullable(String)
house_number          Nullable(String)         -- with entrance letter ('5', '5B')
unit                  Nullable(String)         -- floor or apartment ('3 tr', 'lgh 1201')
postal_code           Nullable(String)         -- five digits
city                  Nullable(String)
country_code          LowCardinality(String)   -- 'SE' unless the source says otherwise or the text says utlandet
normalized_address    String                   -- one display line, '' when no address
address_key           FixedString(64)          -- sha256 of this row's own identity (section 5.1); the empty identity's hash when no address
parse_status          LowCardinality(String)   -- ok, partial, no_address, foreign
parse_notes           String                   -- what the parser dropped or guessed, '' when nothing
normalizer_version    LowCardinality(String)
normalized_at         DateTime64(3, 'UTC')     -- version
ENGINE = ReplacingMergeTree(normalized_at) ORDER BY (company_id, source, slot)
```

Drafts are normalized too, so the panel can show how a typed address parses; the fold
ignores them by source.

### 3.3 `se_company_address_v2` (main, renamed to `se_company_address` at cutover)

One row per company and published address. The name `se_company_address` belongs to the old
final table until the cutover; the new table is built as `se_company_address_v2`, and the
cutover renames the old one to `se_company_address_legacy` and the new one to
`se_company_address` in one `RENAME TABLE` statement before readers switch, so the final
name matches the basic-info convention.

```
company_id            String
address_key           FixedString(64)          -- sha256 of the published identity
care_of, box, street_name, house_number, unit, postal_code, city   -- as in 3.2, Nullable
country_code          LowCardinality(String)
normalized_address    String
kinds                 Array(LowCardinality(String))   -- from every contributing suggestion, distinct
sources               Array(LowCardinality(String))   -- contributing sources, parallel to slots and normalized_ids
slots                 Array(String)
normalized_ids        Array(FixedString(64))   -- the exact normalized versions the fold merged into this row
text_source           LowCardinality(String)   -- whose components were published
active                UInt8
inactive_reason       LowCardinality(String)   -- '', withdrawn, hidden
latitude              Nullable(Float64)
longitude             Nullable(Float64)
geocode_status        LowCardinality(String)   -- matched_exact, matched_corrected, matched_site, matched_area, matched_street, unmatched, ambiguous, postal_box, invalid, foreign
geocode_method        LowCardinality(String)
geocode_confidence    Nullable(Float64)
geocode_precision     LowCardinality(String)
geocode_policy        LowCardinality(String)   -- matcher policy version
geocode_reference     String                   -- OSM reference md5
geocoded_at           Nullable(DateTime64(3, 'UTC'))
normalizer_version    LowCardinality(String)
folded_at             DateTime64(3, 'UTC')     -- version
fold_version          LowCardinality(String)
source_run_id         String
ENGINE = ReplacingMergeTree(folded_at) ORDER BY (company_id, address_key)
```

Readers take `FINAL` rows `WHERE active = 1`. The geocode block is the only part of the row
the reviewer never influences.

### 3.4 `se_company_address_history`

The columns of 3.3 (`normalized_ids` included, so every historical row names the exact
normalized versions it came from), `ENGINE = MergeTree ORDER BY (company_id, address_key, folded_at)`. One
row is appended whenever a published row differs from its previous version in anything but
the geocode block, `geocoded_at`, `folded_at` and `source_run_id`: a new address, a changed
component, a changed provenance, activation, `withdrawn`, `hidden`.

### 3.5 `se_company_address_rule`

Per-company decisions keyed by address.

```
company_id   String
address_key  FixedString(64)
action       LowCardinality(String)   -- hide
removed      UInt8 DEFAULT 0          -- 1 releases the rule
decided_by   LowCardinality(String) DEFAULT ''
note         String DEFAULT ''
decided_at   DateTime64(3, 'UTC')     -- version
ENGINE = ReplacingMergeTree(decided_at) ORDER BY (company_id, address_key, action)
```

### 3.6 `se_company_address_precedence`

The basic-info precedence shape (`company_id`, `field`, `source`, `precedence`, `removed`,
`decided_by`, `note`, `decided_at`), exported from code with `company_id = ''` and
`field = 'text'`. Company-scoped rows are allowed by the table but nothing writes them yet.
Global order: reviewer 20000, bolagsverket 1000, scb 900, ratsit 300. Precedence is a
spelling tie-break only: it picks whose text is published when two members of one merged
address are equally complete (section 5.2). It never decides whether an address is
published; every trusted source's address is.

### 3.7 Geocode cache

`se_address_geocodes` (migration 000317) is kept as the geocode function's cache. Its
`address_id FixedString(64)` now holds the published `address_key`; every other column keeps
its meaning, including `policy_version` and `reference_md5`, which together with the key form
the lookup. The cache is an implementation detail of the function: no reader outside the
function uses it, and it can be truncated and rebuilt.

## 4. The normalizer

`normalize_se_address(raw: RawAddress) -> NormalizedAddress`, pure, in
`se_company/address/normalize_se.py`, one function per country behind a
`normalize_address(country, raw)` dispatcher that only knows Sweden.

Steps, in order:

1. NFKC, case folding, whitespace collapse, trailing punctuation dropped, on every field.
2. Bolagsverket's packed string split on commas into parts; the parts are classified as
   care-of (`c/o`, `co`, `att:`), box (`box N`, `postbox N`, `pl N` is not a box), street
   line, and the postal part (`NNN NN Town`), in that order of recognition.
3. The street line is split into `street_name`, `house_number` (digits plus an optional
   entrance letter, ranges like `5-7` kept as delivered) and `unit` (`N tr`, `lgh NNNN`,
   `bv`, `nb`), the same components migration 000278 introduced for the old canonicalization.
4. `postal_code` reduced to digits; anything but five digits, or `00000`, becomes NULL with a
   parse note.
5. `city` from the postal part or the delivered town; `utlandet` marks the row `foreign` and
   sets `country_code` from the text when it names a country, else `''`.
6. `parse_status`: `ok` when a street or a box, a postcode and a city were found; `partial`
   when a postcode or a city is missing but a street or box exists; `no_address` when nothing
   usable was delivered; `foreign` as above.

The normalizer never expands abbreviations, corrects spelling or guesses a house number: that
is the matcher's work, with its own versioned variants and its zero-regression bar. A golden
corpus (`tests/fixtures/se_addresses/*.jsonl`) drawn from real SCB, Bolagsverket and Ratsit
rows covers boxes, care-of, floors, entrance letters, ranges, `utlandet`, empty and packed
cases; `normalizer_version` (`se-address-normalizer-v1`) is bumped with every behaviour
change, and the bump is what re-normalizes stored rows.

The normalize asset `se_company_address_normalize` reads the raw table FINAL, selects rows
whose `suggested_at` is newer than their normalized row's, or whose normalized row is on an
older `normalizer_version`, pages them (20,000 companies), normalizes in Python and inserts
new versions with `normalized_at` stamped at write time.

## 5. The fold

`fold_company_addresses(company_id, normalized_rows, published_rows, rules, precedence,
geocodes) -> FoldResult` is pure, in `se_company/address/fold.py`; `batch.py` reads and
writes around it; `assets.py` exposes `se_company_address_fold` (64 hash buckets, pool
`se_company_address_fold`, `multi_run(1)`) and `se_company_address_fold_companies`.

### 5.1 Identity

A suggestion's identity is `(country_code, postal_code, city, street_name, box, house_number,
unit, care_of)` after normalization, and `address_key` is the sha256 of those eight joined
with `\n`, NULL as `''`. The published row's key is computed over the published components,
so when a more complete text arrives the key changes: the old row is withdrawn, the new one
appears, and history shows both.

### 5.2 Compatibility

Input: the company's normalized rows FINAL with `source != 'reviewer_draft'` and
`parse_status IN ('ok', 'partial')`; `foreign` rows are published as their own addresses with
`geocode_status = 'foreign'` and no coordinates.

Every suggestion becomes or joins a published address; nothing below filters. Two
suggestions are compatible when they share `country_code`, `postal_code`, `city` and
`street_name` (or both carry a box and the boxes are equal), and each of `house_number`,
`unit`, `care_of` is equal or NULL on at least one side. Rows are sorted by completeness (the
count of non-NULL components) descending, then by precedence descending, then by
`observed_at` descending, then by source and slot. Each row in that order joins the first
published candidate it is compatible with, or starts a new candidate. The candidate's
components are the union of its members' components where the members agree and the most
complete member's where only one has a value; the published `text_source` is the first member
in sort order. `kinds`, `sources`, `slots` collect every member.

A `partial` row (no postcode) merges with a candidate only when its `city` and
`street_name` equal that candidate's and exactly one candidate of the company has that
city and street; otherwise it publishes as its own partial address, which the panel flags.

### 5.3 Rules and set replacement

A `hide` rule with `removed = 0` for a candidate's key turns the row inactive with
`inactive_reason = 'hidden'`; the row keeps its components and provenance so the panel can
show what is hidden. A rule whose key matches no candidate is inert.

The fold rewrites the company's whole set: every candidate as a new main-row version
(`active = 1` or `hidden`), and every previously published key that produced no candidate as
a new version with `active = 0`, `inactive_reason = 'withdrawn'`, components and provenance
kept. A withdrawn address that comes back becomes active again with a fresh history row.

### 5.4 History

For every key the fold writes, the new row is compared with the current main row on the
components, `normalized_address`, `kinds`, `sources`, `slots`, `text_source`, `active`,
`inactive_reason`; a difference, or no previous row, appends the new row to history. The
geocode block never triggers history. `batch.py` writes history before main, as basic info
does.

**Lineage.** Every layer points upstream by a stored id: a published row's `normalized_ids`
name the normalized versions it merged, each normalized row's `suggestion_id` names the raw
version it parsed, and each raw row's `source_record_uid` names the source record. Source
tables are not touched: a source row can feed many versions over time and its loader does
not know normalization exists. The suggestion and normalized tables keep only the current
version per key, so an id in an old history row may name a version that has been merged
away; the current version is one key lookup away, and a published row whose
`normalized_ids` differ from the current normalized rows is shown as re-fold pending.

### 5.5 Selection

`changed_only` (default true) selects a company when any of:

- a normalized row with `source != 'reviewer_draft'` has `normalized_at` newer than the
  newest `folded_at` of the company's main rows;
- a rule version (`removed` included) has `decided_at` newer than that `folded_at`;
- any main row carries `geocode_policy` or `geocode_reference` different from the current
  ones, or `normalizer_version` different from the current one;
- the company has normalized rows but no main row.

Every folded company is rewritten with a new `folded_at`, so selection converges. A matcher
bump, an OSM refresh or a normalizer bump therefore re-folds the population on the next run
with no manual step.

## 6. Geocoding

`geocode_addresses(batch: Sequence[NormalizedAddress], *, policy, reference) ->
Mapping[address_key, GeocodeOutcome]` in `se_company/address/geocode.py`, called once per
fold page over the page's distinct keys.

1. Cache lookup in `se_address_geocodes` by `(address_key, policy_version, reference_md5)`.
2. Misses are written as query documents into the OSM workbench DuckDB
   (`data/sweden_address_osm_source.duckdb`, resource `sweden_address_osm_duckdb`, opened
   read-only for the reference tables, a per-run temporary schema for the query and result
   tables) and matched by the existing engine, `replace_address_resolution_candidates` then
   `replace_address_resolution_results`, under `SWEDEN_ADDRESS_RESOLUTION_POLICY`.
3. Outcomes `unmatched`, `ambiguous` and `postal_box` go through the centroid fallback with
   the same postcode-then-city rule and spread cap as `geocode_serving_overlay`, labelled
   `centroid_fallback` and `matched_area`. Box addresses therefore get their town centroid.
   `foreign` and `invalid` rows get their status and no coordinates and skip the matcher.
4. New outcomes are inserted into the cache before the page's main rows are written, so a
   crash between the two costs nothing on retry.

The fold's bucket pool is the pool the OSM refresh asset takes, so an extract swap never
races a fold. The matcher policy, the workbench scripts and the golden matcher corpus are
untouched by this design; a policy bump is still a constant change plus a workbench
measurement, and the selection rule of section 5.5 propagates it.

A workbench that cannot be opened fails the run. The fold never publishes rows without a
geocode block.

**Adoption.** `se_address_geocodes_adopt_keys`, a one-off asset, computes the new
`address_key` for every identity in `se_addresses_current` from its normalized fields through
the section-4 normalizer, and inserts a copy of that identity's current outcome (the same
two-stage rank as `se_address_geocodes_current`) under the new key with its original
`policy_version` and `reference_md5`. Identities whose normalized text does not reproduce
under the new normalizer are not adopted and are matched on their first fold. The asset
reports adopted, skipped and ambiguous counts; it runs once before the first full fold.

## 7. Extractors

`se_company_address_suggestions_<source>` on the basic-info extract helper: a change scan
comparing the source row's `observed_at` with the current raw suggestion's, scratch-table
paging, `execute: false` preview, `max_companies` cap.

- `scb`: `se_scb_companies` FINAL: `care_of`, `street_address`,
  `postal_code`, `post_town`; kind `visiting_or_postal`, slot `''`; `observed_at` the
  register row's. A tombstoned company writes the NULL row.
- `bolagsverket`: `se_bolagsverket_companies` FINAL: `postal_address`
  into `raw_address`; kind `postal`, slot `''`. A tombstoned company (`has_company = 0`) writes the NULL row.
- `ratsit`: `se_ratsit_company` FINAL, newest normalized report per company:
  `address_street`, `address_postal_code`, `address_locality`, `address_county`; kind
  `postal`, slot `company`; `observed_at` the report's `normalized_at`.
- Reviewer rows come only from the backoffice (section 8).

`se_company_address_extract_job` selects the three extractors and the normalize asset;
`se_company_address_v2_weekly` is registered stopped (an interim name: the old model's
`address_legacy.py` registers `se_company_address_weekly` until the cutover, which renames
the new one).

## 8. Backoffice

Route `/admin/se/company/:companyId/address` replaces the current Address tab. Two-thirds
and one-third.

Left card: the company's published addresses, active first, one row each with the
normalized line, kinds, sources, geocode status and precision, and `draft` or `hidden`
badges; withdrawn and hidden rows under a collapsed group. A row links to
`?address=<key>`; Edit-style buttons sit outside the link. Below: History (newest first) and
the fold poller.

Right panel for the selected address: one entry per contributing source, resolved through
the row's `normalized_ids`, with the raw text beside its parsed components and
`parse_status`, a "re-fold pending" mark when the current normalized version differs from
the one the row was folded from, the published `text_source` and why (most complete, or
tie-break), the geocode block with its versions, and the rule in force.

Actions, each through the confirmation dialog, all in one action round-trip:

- **Remove**: on a source-delivered address inserts a `hide` rule; on a reviewer-typed
  address inserts a reviewer raw row with the address columns NULL for that slot.
- **Reset to default**: inserts the rule again with `removed = 1`.
- **Add address**: the right-side sheet (care-of, box or street line, postcode, city,
  country preset `SE`, kind select, note); Save draft writes a `reviewer_draft` raw row under
  a new slot; its parsed components appear after Fold now (the targeted fold normalizes the
  company's raw rows before folding) or the next normalize run; Activate writes the
  `reviewer` row and clears the draft in one insert; Discard clears the draft.
- **Correct**: the same sheet prefilled from the published address, carrying the key it
  replaces in `replaces_key`; Activate writes the `hide` rule for that key and the reviewer
  row together.
- **Fold now**: launches `se_company_address_fold_companies` for the company; the page
  reloads when the run finishes.

Validation lives in the client-safe parser: a box or a street line, a five-digit postcode, a
city, lengths capped (street 200, care-of 200, city 100, note 500), plain text, `kind` from
the catalogue. The results carry the intent, as the basic-info route does.

The corrections queue page keeps reading the old ledger until the cutover retires it.

## 9. Slices, parity, cutover

0. Tables and normalizer: migrations, the `se_company/address` package skeleton, the
   Swedish normalizer with its golden corpus, the normalize asset.
   Shipped 2026-09-06 (plan `2026-09-06-se-company-address-0-tables-normalizer.md`, merged
   fast-forward as main 0c058dba): migrations 000382-000387 applied on prod (ledger 387), the
   package with `tables.py`, `normalize_se.py` (48-case golden corpus, `se-address-normalizer-v1`),
   `normalize.py` and the `se_company_address_normalize` asset, proven on clickhouse-local under
   both `join_use_nulls` settings; dagster hot-synced; the first normalize run succeeded with
   zero rows (no extractor yet). The whole-branch review caught that the per-page read binds the
   id list four times and would have exceeded a 1 MiB `max_query_size` at 20,000 ids; the
   module owns a 4 MiB budget with a render-size guard test. The old model's module became
   `address_legacy.py`.
1. Extractors: SCB, Bolagsverket, Ratsit; run on prod; read the `parse_status` distribution
   and spot-check packed Bolagsverket parses against the old chain's `normalized_address`.
2. Fold and geocoding: compatibility, rules, set replacement, history, the geocode function,
   the adoption asset, the precedence export, both fold assets; adoption first, then all 64
   buckets.
3. Backoffice: the Address tab on the new tables.
4. Cutover: parity, reader switch, retirement.

**Parity**: per company, the set of active `normalized_address` lines in the new table
against the current `se_company_address` (`is_current = 1`) lines, classified as identical,
superset (Ratsit or a completeness merge added an address), or different, with a sampled
review of the different ones; plus adopted versus freshly matched geocode counts. Every
difference must trace to a rule in this document.

**Reader switch** (after the rename of section 3.3): `sweden_company/companies_current.py` (the served company view), the
backoffice address and geocoding list pages, and anything else the parity step finds.

**Retirement**, owner-gated, hand-run drops per the ledger policy: `se_company_address_scb`,
`se_company_address_bolagsverket`, the old `se_company_address`, `se_company_address_correction`
with its sensor and weekly, `se_company_addresses` and `se_company_addresses_current` with the
register-load steps that fill them, `se_company_addresses_canonical_current`,
`se_company_address_members_current`, `se_addresses_current`,
`se_company_address_links_current`, the `se_address_geocodes_current` view, the demand scan
assets and `geocode_legacy_adoption`. The cutover also renames `se_company_address_v2_weekly`
to `se_company_address_weekly` once the old schedule is gone. Kept: the matcher, the policy constant, the workbench
and its scripts, `se_postcode_centroids`, `se_city_centroids`, `se_address_geocodes`.

## 10. Names

Tables `se_company_address_suggestion`, `se_company_address_normalized`,
`se_company_address_v2` (renamed `se_company_address` at cutover), `se_company_address_history`, `se_company_address_rule`,
`se_company_address_precedence`. Package `dagster_v3.defs.se_company.address` (`tables`,
`normalize_se`, `normalize`, `suggestions`, `extract` shared from basic info, `scb`, `bolagsverket`,
`ratsit`, `precedence`, `fold`, `geocode`, `batch`, `assets`, `jobs`); the old model's module
`se_company/address.py` was renamed `address_legacy.py` on 2026-09-06 so the package can take
the name, its definitions unchanged until the cutover retires them. Assets
`se_company_address_suggestions_<source>`, `se_company_address_normalize`,
`se_company_address_fold`, `se_company_address_fold_companies`,
`se_company_address_precedence_clickhouse`, `se_address_geocodes_adopt_keys`. Backoffice
`app/lib/se-company-address-entity.server.ts`, `app/lib/se-address-fields.ts`,
`app/lib/se-address-decision-form.ts`, `app/components/admin/se-address-workspace.tsx`,
`app/components/admin/se-address-edit-sheet.tsx`, route `admin-se-company-address.tsx`.
