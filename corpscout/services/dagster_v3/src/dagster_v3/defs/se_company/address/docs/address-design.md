# se_company.address (slices 0-2b)

The shipped part of the 2026-09-06 SE company address entity design
(`docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md`); read that for
everything past the modules below -- the backoffice Address tab, parity and the cutover.

| Module | Responsibility |
| --- | --- |
| `tables.py` | Table names and column tuples, pinned against migrations 000382-000387; the main table was built as `se_company_address_v2` and renamed by 000393 |
| `normalize_se.py` | `normalize_se_address`: pure Swedish parser -- splits, folds and classifies; never expands abbreviations, corrects spelling or guesses a house number (that is the geocoder's job) |
| `normalize.py` | The normalize step's SQL (`changed_scope_sql`, `changed_rows_sql`, `all_scope_sql`, `all_rows_sql`, `normalized_insert_sql`) and the paging/write loop (`normalize_all`, `normalize_companies`) |
| `assets.py` | The Dagster assets: `se_company_address_normalize`, `se_company_address_precedence_clickhouse`, `se_company_address_fold`, `se_company_address_fold_companies` |
| `geocode.py` | `geocode_addresses`: one served outcome per location key -- the store as cache, the OSM workbench as matcher, the centroid overlay on read (slice 2a) |
| `warm.py` | `se_address_geocodes_warm`, the bulk warm step that hands every current location key to `geocode_addresses` in 150,000-key chunks so the matcher runs in the bulk mode it is built for (amended 2026-09-07) |
| `precedence.py` | `ADDRESS_PRECEDENCE`/`precedence_rows`/`precedence_for` (`FIELD = 'text'`): the source order the fold's sort key uses to break a completeness tie, and per-company overrides read out of `se_company_address_precedence` |
| `fold.py` | The pure per-company fold, `fold_company_addresses` (spec section 5): compatibility grouping into `_Candidate`s, hide/withdraw against a previous published set, `NormalizedRow`/`PublishedAddress`. No I/O, no clock -- the geocode block is attached afterwards by `PublishedAddress.with_geocode` |
| `batch.py` | The fold's SQL (`normalized_watermarks_sql`, `stale_companies_sql`, `current_normalized_sql`, `current_main_rows_sql`, `hidden_keys_sql`, `company_precedence_sql`, `main_insert_sql`, `history_insert_sql`) and the paging/write loop (`fold_bucket`, `fold_companies`): selection, in-page geocoding of the page's distinct location keys, history-then-main write |
| `se_company_address_fold` (asset) | The 64 hash-bucket partitioned fold (`BackfillPolicy.multi_run(max_partitions_per_run=1)`); pool `sweden_address_osm_duckdb` (`osm_tables.DUCKDB_POOL`, shared with the OSM workbench so an extract swap never races a fold); config `AddressFoldConfig` (`changed_only`, `page_size`) |
| `se_company_address_fold_companies` (asset) | The targeted fold over `config.company_ids`, whatever their bucket -- the backoffice's Fold now button; same pool; config `AddressFoldCompaniesConfig` (`company_ids`, `changed_only` defaulting `false`, `page_size`) |
| `se_company_address_precedence_clickhouse` (asset) | Exports `ADDRESS_PRECEDENCE` to `se_company_address_precedence` as global rules (`company_id ''`); no pool; no config -- re-run after changing the dictionary |

## Selection (fold)

`changed_only=true` (the default on `AddressFoldConfig`, and explicitly `false` by default on
`AddressFoldCompaniesConfig`) folds a company only when one of four conditions holds: it has
no main row yet and at least one publishable normalized row; its newest normalized row is
newer than its fold; its newest rule (`se_company_address_rule`) is newer than its fold; or
`stale_companies_sql` names it -- its active row's `geocode_policy`, `geocode_reference` or
`normalizer_version` no longer matches the run's current ones. `changed_only=false` re-folds
the whole bucket or company list regardless, writing history only where a compared field
actually changed.

`stale_companies_sql` carries two exclusions on top of that OR: a row with `inactive_reason
= 'withdrawn'` or `geocode_status = 'foreign'` never re-triggers a fold on policy drift alone
(a foreign row has no coordinate to refresh, and a withdrawn one converges the moment its
company is next folded for any other reason). `legacy_adopted_v1` is excluded too, but for a
different reason -- it names the one-time import, which sits on no resolver version at all,
so it is defined as never stale rather than merely skipped.

After a `NORMALIZER_VERSION` bump, run `se_company_address_normalize` before any fold: until
a company has been (re)normalized, its main row's `normalizer_version` still does not match
the new constant, so `stale_companies_sql` keeps marking it stale and every fold pass
rewrites it -- still with the old normalizer's output, because the normalized row itself has
not been recomputed yet. The weekly chain (`sweden_company_address_geocoding_weekly_job`,
Tuesday 01:05 Stockholm, spec `docs/superpowers/specs/2026-09-24-address-weekly-chain-design.md`)
normalizes before it warms and folds, so the scheduled path always has this ordering; only a
bucket or backfill launched by hand needs the operator to keep it.

## Change rule

A raw suggestion row is (re)normalized when its normalized row is missing, when the raw
row's `suggested_at` is newer than the normalized row's, or when the normalized row was
written on an older `normalizer_version` than the current one. Two UNION ALL branches (a
LEFT ANTI JOIN for "missing", an INNER JOIN for the other two) stand in for one LEFT JOIN so
the result does not depend on ClickHouse's `join_use_nulls` setting. `changed_only=false`
re-normalizes every raw row regardless of any of this.

## parse_status

- `ok` -- a street or box, a postcode and a city were all found.
- `partial` -- a street or box was found but the postcode or the city is missing.
- `no_address` -- nothing usable was delivered, or the source marks the address unknown.
- `foreign` -- the post town says `utlandet`, or the source's own `country_code` isn't `SE`.
  Components stay NULL (the Swedish rules do not apply), but since 2026-09-08 the row still
  carries a `normalized_address`: the delivered parts joined for display (care-of, street,
  postcode, town, country). A published row with an empty line renders blank on the Address
  tab and hides the detail page's Contact & location card altogether.

Amended 2026-09-08: a valid postcode with a known town and no street or box is a `partial`
address; the old chain published these 27,786 companies and the new one now does too,
geocoded to the postcode centroid.

## Parse rules (v3)

`NORMALIZER_VERSION` is `se-address-normalizer-v3`, which is v2 plus that one rule: a row
whose only usable content is a valid postcode and a known town (`SEB, STIFTELSER &
FÖRETAG, 106 40 Stockholm` -- a big-company postal code) publishes as a `partial` carrying
its care-of, postcode and city, noted `no street or box`. Everything else without a box or
a street stays `no_address`. The fold never glues such a row onto a street candidate
(`partial_compatible` requires a location line on both sides); two sources delivering the
same postal point merge through the LOCATION-LESS rule (2026-09-08): a partial with no
street and no box joins the first candidate that has neither either and agrees on country,
postcode and city, with `_one_sided_ok` on house_number/unit/care_of -- so `c/o x, 106 40
Stockholm` and a bare `106 40 Stockholm` are one address carrying the care-of, while two
different care-ofs at one postal code stay two. The geocoder serves it the postcode centroid.

## Parse rules (v2)

v2 added four rules seen in the prod `parse_notes` readout on top of the v1 rules above:
a box number may be written with a space
(`Box 531 65` parses to box `53165`); a box found after a customer reference or a name
(`NABO 118849 BOX 843`) is parsed as that box, with the prefix taken as `care_of` when none
was delivered directly; `plan N`, the roman numerals `ii`/`iii`/`iv`, a bare four-digit
apartment number, and `n b`/`nb`/`kv` are all recognized as `unit`; and text left over after a
box is dropped with a `parse_notes` entry rather than silently discarded. `location_key`
(`location_components`/`location_key` in `normalize_se.py`) is the sha256 of
`country_code, postal_code, city, street_name, box, house_number, unit` joined with `\n`
(NULL as `''`) -- the identity without `care_of`, so the same physical address is matched once
regardless of who receives mail there; `identity_components`/`address_key` are unchanged.

## The packed Bolagsverket format

Bolagsverket delivers one string in `raw_address`: five `$`-separated parts, in order --
street line, care-of name, town, postcode, country token. A missing trailing part defaults
to `''`; the country token is upper-cased and truncated to two characters. Example:
`"Box 5305$$STOCKHOLM$10247$SE-LAND"` splits to street line `Box 5305`, care-of `''`, town
`STOCKHOLM`, postcode `10247`, country `SE`.

## Running the asset

`se_company_address_normalize` (`AddressNormalizeConfig`): `changed_only` (default `true`)
selects only rows that need (re)normalizing; `company_ids` (default empty, meaning every
company) targets specific companies and pages them in memory instead of scanning; `page_size`
(default 20,000, max 50,000) bounds a page's row count.

## Extractors (slice 1)

`se_company_address_suggestions_<source>` (`scb`, `bolagsverket`, `ratsit`, `esef`), on the
same basic-info extract helper (`suggestions.py::define_address_suggestion_asset`), each
write one raw suggestion row per company per source:

- `scb` reads `se_scb_companies` FINAL: `care_of`, `street_address`, `postal_code`,
  `post_town` as delivered, kind `visiting_or_postal`, slot `''`.
- `bolagsverket` reads `se_bolagsverket_companies` FINAL: the packed `postal_address`
  string into `raw_address`, kind `postal`, slot `''` -- the normalizer parses it.
- `ratsit` (`ratsit-address-v2`) reads `se_ratsit_company` FINAL and
  `se_ratsit_establishments` FINAL, newest normalized report per company: the company's
  `address_street`, `address_postal_code` and `address_county` as delivered, kind `postal`,
  slot `company`, plus one row per establishment of that report carrying a street and a
  postcode — kind `workplace`, slot `est:<identifier>` with the establishment index appended
  when a report repeats the identifier. `post_town` on every row comes from a postcode →
  town dictionary rebuilt per page from `se_scb_companies` FINAL (`has_company = 1`, the
  most frequent trimmed spelling per digits-only postcode, ties alphabetically), because
  Ratsit delivers the municipality as the locality on about 28% of company addresses and the
  normalizer's `city` is part of both `location_key` and `address_key`. A slot the newest
  report no longer delivers gets a NULL row through
  `suggestions.py::address_select_sql`, which pairs the live rows with per-slot tombstones
  stamped with the current report's `observed_at`; `scb` and `bolagsverket` keep their
  single-slot tombstone instead.
- `esef` reads `se_esef_facts` joined to `se_esef_filings`: the
  `AddressOfRegisteredOfficeOfEntity` fact of the company's newest filing with a period end
  no later than today, cleaned (tags, whitespace, trailing punctuation) and re-packed into
  the Bolagsverket packed format when a Swedish postcode is found, otherwise delivered as
  street and town components split at the last comma; kind `registered`, slot `''`.
  `source_record_uid` is the filing package's uid; address precedence carries `esef: 500`
  (display order only).

A company a source stops delivering writes a tombstone: a NULL row, not a deleted one.
`suggestion_id` is stamped from a `WITH (SELECT now64(3, 'UTC')) AS stamp` scalar subquery
bound once per statement -- two bare `now64()` calls in one statement are not guaranteed the
same instant (measured about 0.5% of executions differ, desynchronizing a whole page) -- not
from `suggested_at` read back, so the id always matches the row it names.

Change rule: the shared helper's -- a company is visited when its source table's record is
newer than the company's current suggestion row from that source, or it has never been
suggested by that source; `execute: false` (default) previews the count without writing.

The backoffice's Company actions → Addresses menu offers two global jobs:
`se_company_address_sync_job` selects the four extractors and normalization;
`se_company_address_refresh_job` adds geocode warming and `se_company_address_publish`.
The latter folds all 64 buckets sequentially using the existing OSM workbench pool.
The backoffice sends `execute: true`, `page_size: 10000` to each extractor, and
`changed_only: true`, `page_size: 20000` to normalization and publication. No company
filters or LLM settings are sent. Since 2026-09-24 the fold also runs on a schedule again: the
weekly chain in `sweden_company/address_geocoding_assets.py` extracts, normalizes, refreshes
OSM and the centroids, warms, folds all 64 buckets with `changed_only=True`, then refreshes the
companies serving view (Tuesday 01:05 Stockholm; about 2.5 h, of which the fold is 75 min,
until the cache-invalidation redesign). The 2026-09-15 decision to keep the fold manual was
reversed because nothing had published addresses since 2026-09-13. Internal bucket and
targeted correction folds remain
available; they share the DuckDB pool with the weekly and wait while it runs.

## Geocoding (slice 2a)

`geocode_addresses(addresses, *, clickhouse, duckdb, run_id, matched_at)` in `geocode.py`
returns one `GeocodeOutcome` per LOCATION key. Pure orchestration -- no Dagster, no
resources -- so the fold, a backfill script and the tests drive the same code. Three steps:
a cache read over `corpscout.se_address_geocodes` (through the store's own
`build_current_geocodes_sql` read rule), the shadow's resolver on the misses over per-run
tables in `sweden_company_enrichment` dropped in a `finally`, and the
postcode-then-city centroid overlay applied ON READ and never written, so an address the
matcher could not place is served a coarse coordinate today and a precise one the moment a
later extract matches it.

`foreign` and `no_address` addresses never reach it: the fold filters them out and the
function raises `ValueError` naming the location key if handed one.

**The per-extract caches (2026-09-07).** Two inputs the resolver needs are shared, not
per-call: the OSM reference documents (keyed on the extract's md5) and the fuzzy reference
street postings built from them (keyed on the md5, the policy version -- the posting rule
is the policy's -- and the documents' own `built_at`, because the shadow run rebuilds the
documents unconditionally under an unmoved md5). A postings manifest whose table has gone
missing is no cache either. `ensure_reference_postings` builds whichever moved and returns
the md5; both live as real tables in `sweden_company_enrichment`, beside the manifests that
record what they were built for. The postings used to be rebuilt inside
`replace_address_resolution_candidates` on every call -- an unnest of every reference
street's deletion signatures plus a DISTINCT over millions of rows. A one-shot rematch pays
that once; the fold calls the geocode function once per 20,000-company PAGE and paid it per
page (measured on prod 2026-09-07: page 1 of `bucket_00` sat in the candidates step past 20
minutes). `_match` now passes the cached table to the engine by name. The five per-run
tables and the QUERY-side postings the engine builds from them stay per call.

**The hit rule.** A cached row is a hit when its `(policy_version, reference_md5)` is the
pair this run computes with, or when its `policy_version` is `legacy_adopted_v1` -- the
one-time import, which is on no resolver version at all and would be thrown away on the
first run if a version mismatch re-matched it. Nothing else is a hit. In particular the
`adopted:<old address_id>` run-id prefix on the imported rows is provenance, not a
cache pin: an adopted row keeps the versions of the outcome it copied and is re-matched
after the next policy bump or OSM extract like any other row.

**Provenance.** Every row written carries the extract's five `source_*` columns --
`source_url`, `source_object_key`, `source_md5`, `source_snapshot_at`,
`source_retrieved_at` -- read once per call by `extract_provenance` off
`sweden_address_osm.address_points` with the same `first(... order by source_record_id)`
projection the retired promotion step used. No stored row may carry a NULL in one of them --
the contract the retired store-completeness check used to assert (`missing_provenance`,
deleted with the demand chain in slice 4b). `source_md5` is the row's own `reference_md5`
(both are the same read) and `store_row` refuses any other pairing. The two per-RECORD
columns, `source_record_id` and `source_record_url`, stay NULL and are not part of that
contract. `candidate_count` is clamped to 65,535, the `UInt16` column's ceiling, exactly as
the retired promotion step's `least(65535, ...)` clamped it.

**Query settings.** Both id-bound reads -- the cache lookup and the centroid fallback --
bind up to `CACHE_LOOKUP_CHUNK` (5,000) values that clickhouse-driver substitutes CLIENT
side, so they land in the statement text: 341,333 and 461,740 bytes, past ClickHouse's
262,144-byte default `max_query_size`. Both pass `GEOCODE_QUERY_SETTINGS`
(`max_query_size` 1 MiB, `max_execution_time` 1800).

## Adoption (slice 2a, one-off)

The one-off `se_address_geocodes_adopt_keys` (slice 2a) copied 2,019,120 outcomes from the
old identities onto location keys on 2026-09-06 and was deleted in slice 4c with
`se_addresses_current`, the table it read. The adopted rows stay in `se_address_geocodes`
under the `legacy_adopted_v1` family.

## Warm step (amended 2026-09-07)

`se_address_geocodes_warm` reads every distinct location key of the current `ok`/`partial`
normalized rows and hands them to `geocode_addresses` in chunks of 150,000, so the matcher
runs in bulk (the mode it is built for) and the fold pages find their keys in the cache. It
runs once before the first full fold and after every OSM extract refresh; the fold still
geocodes in-page whatever the warm step did not cover, so nothing depends on it for
correctness. `AddressWarmConfig` (`chunk_size`, `limit`); pool `sweden_address_osm_duckdb`
(`osm_tables.DUCKDB_POOL`), same as the fold's. Metadata:
`keys/chunks/cache_hits/matched/geocoded/fallback`. The asset also carries
dependencies on `sweden_osm_addresses_duckdb` and `se_company_address_normalize`, and rides in
`sweden_company_address_geocoding_weekly_job` (`sweden_company/address_geocoding_assets.py`),
so the weekly OSM refresh always warms the cache for the new extract. It also runs after
normalization in the backoffice full-processing job; that job uses the existing OSM data
and never selects the OSM download or reference-data refresh assets. The existing OSM
freshness warning check is selected with warming in both jobs.

## Backoffice (slice 3, 2026-09-07)

The Address tab (`corpscout/services/backoffice`, spec section 8) is five files over
`chQuery`/`chInsertSeCompanyAddressSuggestions`/`chInsertSeCompanyAddressRules`:
`app/lib/se-address-fields.ts` (the catalogue, `selectedAddressFromSearch`,
`validateSeAddressInput`, `addressFoldPending`), `app/lib/se-address-decision-form.ts`
(`parseSeAddressDecision`, the six intents `remove`/`reset`/`fold-now`/`save-draft`/
`activate`/`discard`), `app/lib/se-company-address-entity.server.ts` (`loadSeAddressDetail`
over the six tables, the five reviewer writes and `launchSeAddressFold`),
`app/components/admin/se-address-workspace.tsx` (the two-column tab) and
`app/components/admin/se-address-edit-sheet.tsx` (the Add / Correct / edit-draft sheet); the
route `admin-se-company-address.tsx` dispatches the six intents to those functions.

A reviewer address lives in a raw row (`se_company_address_suggestion`) under slot
`r<the stamp's 17 digits>` (Ruling 2 -- for example `r20260907203355123`): Save draft writes
it under `reviewer_draft`, Activate writes the `reviewer` row under that same slot so the two
share one lineage, and Discard/Activate leave a tombstone version (every address column NULL,
the slot's `kind` carried over) rather than deleting anything. `suggestion_id` is
`sha256(company_id "\n" source "\n" slot "\n" stamp)` hex with the stamp spelled exactly as
`suggested_at` -- the same computation the extractors run in SQL
(`suggestions.py::ADDRESS_TRAILING_SELECT_SQL`). Remove writes a `hide` rule
(`se_company_address_rule`, `action='hide'`) when the published row carries any non-reviewer
member, leaving every member in place; on a reviewer-only row it tombstones every reviewer
slot instead, since a mixed row's key is computed over the union of its members' components
and retiring one would re-key the row and orphan the hide rule (Ruling 1, amended). Reset to
default releases the hide rule (`removed=1`). Fold now (`launchSeAddressFold`) launches
`se_company_address_fold_companies` for the one company id -- the targeted fold normalizes
that company's raw rows first (Task 1's `targeted_fold`), so a draft saved a moment earlier
parses before it folds.

### The tab is slim, and the workplaces are paged (2026-09-13)

Ratsit's establishments put 1,502 `workplace` rows on one kommun, and the tab rendered them
as 7.8 MB of HTML: markup for every row plus a hydration payload carrying every row's
members with their normalized and raw rows. `loadSeAddressDetail(companyId, {selectedKey,
workplacePage, workplaceQuery})` now returns list rows WITHOUT members -- the row's own
columns, the hide rule in force and a re-fold-pending flag computed in ClickHouse
(`has(current_pairs, …) AND NOT has(current_triples, …)` over the company's current
normalized versions, `groupArray`-ed once per query) -- and resolves members, the
`text_source` reason and the raw text for ONE row, from two reads bound to that row's own
`(source, slot)` pairs (`arrayZip({memberSources:Array(String)}, {memberSlots:Array(String)})`).
That row is the `?address=<key>` row when the key names one of the company's, else the first
active row of the list -- the default the tab has always had -- and there is none only when
the company has no active company address at all. "Fold pending" is one row of five scalar aggregates
(`ADDRESS_FOLD_STATE_SQL`) instead of two whole-company reads. Drafts load on their own
(`source = 'reviewer_draft'`), History keeps its cap of 200.

Rows whose `kinds` are exactly `['workplace']` and that are active leave the Addresses card
for a "Workplaces (<total>)" card below it -- migration 000403's split, so the tab and the
serving view draw the same line -- paged 50 a page and filtered over `normalized_address`
with `positionCaseInsensitiveUTF8` (a literal needle: nothing to escape). Page and filter
live in the URL as `?workplaces=<page>` and `?workplace_q=<text>` beside `?address=`, and
`addressSearchString` in `app/lib/se-address-fields.ts` builds every link on the page from
all three, so selecting an address keeps the workplace page and paging keeps the selected
address; a POST keeps both through React Router's default form action. The map shows the
company's active addresses plus the current workplace page. The five reviewer writes still
read the company whole -- they are one POST per click, and Remove must be able to find a
workplace row by key. Measured on the 1,502-workplace kommun: 7,817,093 → 429,319 bytes
(about 7% over the 400 KB target, accepted; the residual is per-row markup).

## Readers (slice 4a, 2026-09-08)

The entity's four readers are the serving view (`sweden_company/companies_current.py`,
migration 000392), the domain-suggestion match features
(`company_domain_suggestions/.../stg_se_company_match_features.sql`), the reconciliation and
centroid assets (`sweden_company/centroid_assets.py`), and the backoffice
(`app/lib/se-company-address-entity.server.ts`, `address-quality.server.ts`,
`address-companies.server.ts`). Two mapping rules bind them all: the published
`normalized_address` is a DISPLAY line, so a reader that needs a street either strips the
trailing `, NNN NN Town` (yielding `''` for a postcode-only line, which has no comma to cut
at) or -- where the value is a JOIN KEY, as in the match features -- builds it from the row's
own components (`Box N`, else street_name + house_number + unit) so a `c/o` prefix cannot
enter the key. And a row without a street and without a box is location-less: it never takes
the primary slot from a row that has one.

### The serving view does not publish workplace-only rows (migration 000403)

The serving view publishes a company's addresses as an UNCAPPED JSON array and picks one
primary address by a tiebreak over `kinds`. Ratsit's establishments (slice 3) put hundreds of
`workplace` rows on some companies -- one holds 1,607, which is a ~400 KB serving JSON -- and
355 companies have establishments but no `visiting_or_postal` row at all, so a workplace could
become the address the companies and geocoding lists print as the company's own. The view's
`company_addresses` CTE therefore excludes rows whose `kinds` is EXACTLY `['workplace']`, and
because the array, `address_count` and the primary pick all read that one CTE, they drop those
rows together. A row the FOLD merged -- an establishment repeating the company's own postal
street and postcode, `kinds = ['postal', 'workplace']` -- is the company's address and stays.
This is a serving rule and not a fold rule: every row stays in `se_company_address` and on the
backoffice Address tab, which reads the entity directly. Migration 000403 carries the
repointed query on 000393's in-place recipe below (`SYSTEM STOP VIEW`, `ALTER TABLE ... MODIFY
QUERY`, `SYSTEM START VIEW`, no `_next` and no `SYSTEM WAIT VIEW`), and the same
interrupted-run recovery applies with `migrate force 403`.

### If a serving swap is interrupted

Migration 000392 (like 000344/000347 before it) is a staged swap: `DROP TABLE IF EXISTS
...serving_retired`, `SYSTEM STOP VIEW ...serving`, `CREATE MATERIALIZED VIEW
...serving_next`, `SYSTEM WAIT VIEW ...serving_next`, then one `RENAME TABLE`. **If the
migrate client drops during `SYSTEM WAIT VIEW`** -- a dropped ssh session, a client timeout --
the server keeps building `_next`, but the ledger is left dirty with the STOP landed and the
RENAME NOT landed: the live view is no longer refreshing and the new one is not serving.
Finish it by hand:

1. `SELECT view, status, last_success_time, exception FROM system.view_refreshes WHERE
   database = 'corpscout'` -- wait until `se_companies_serving_next` reports a success.
2. Run the `RENAME TABLE` statement verbatim from the bottom of the migration file (it swaps
   `se_companies_serving` to `_retired` and `_next` to `se_companies_serving`).
3. `migrate force <n>` with the migration's number (392 here), so the ledger records the
   version the database is actually at. Do NOT re-run the up file: its `CREATE` would fail on
   an existing `_next`, and its `DROP` would take out the view just parked under `_retired`.

### If the 000393 rename is interrupted

Migration 000393 (below) is not a staged swap: `SYSTEM STOP VIEW`, one `RENAME TABLE`, `ALTER
TABLE ... MODIFY QUERY`, `SYSTEM START VIEW`. **If the migrate client drops between the STOP
and the START**, the view is left stopped and serving its last contents at full speed with
nothing raising anywhere. Finish it by hand:

1. `SELECT view, status, last_success_time, exception FROM system.view_refreshes WHERE
   database = 'corpscout' AND view = 'se_companies_serving'` -- a stopped view still lists
   here.
2. If the RENAME landed but the MODIFY QUERY did not, run the `ALTER TABLE ... MODIFY QUERY`
   statement verbatim from the migration file; if neither landed, re-running the whole up file
   is safe once the view has been started again.
3. `SYSTEM START VIEW corpscout.se_companies_serving`.
4. `migrate force 393`.

## Rename and retirement (slice 4b, 2026-09-08)

Migration 000393 gives the address entity its final name: `corpscout.se_company_address_v2`
becomes `corpscout.se_company_address`, and the old final table of the 2026-08-24 model --
the one `se_company_address` named before this migration -- parks under
`se_company_address_legacy` until slice 4c drops it by hand under the ledger policy. One
`RENAME TABLE` moves both names at once, so there is no instant at which `se_company_address`
resolves to nothing.

This is NOT a staged swap like 000391/000392 (see above): the view's definition is otherwise
unchanged, only the table name it reads changes, so 000393 stops the view, renames the
tables, repoints the query in place with `ALTER TABLE ... MODIFY QUERY`, and starts the view
again -- there is no `_next` view to build and no `SYSTEM WAIT VIEW` to sit through.
