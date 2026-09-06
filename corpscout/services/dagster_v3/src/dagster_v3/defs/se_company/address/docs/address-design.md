# se_company.address (slice 0)

Slice 0 of the 2026-09-06 SE company address entity design
(`docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md`); read that for
everything past the four modules below -- compatibility grouping, geocoding, the precedence
export, the backoffice Address tab, parity and the cutover.

| Module | Responsibility |
| --- | --- |
| `tables.py` | Table names and column tuples, pinned against migrations 000382-000387 |
| `normalize_se.py` | `normalize_se_address`: pure Swedish parser -- splits, folds and classifies; never expands abbreviations, corrects spelling or guesses a house number (that is the geocoder's job) |
| `normalize.py` | The normalize step's SQL (`changed_scope_sql`, `changed_rows_sql`, `all_scope_sql`, `all_rows_sql`, `normalized_insert_sql`) and the paging/write loop (`normalize_all`, `normalize_companies`) |
| `assets.py` | `se_company_address_normalize`, the one asset this slice ships |

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

`se_company_address_suggestions_<source>` (`scb`, `bolagsverket`, `ratsit`), on the same
basic-info extract helper (`suggestions.py::define_address_suggestion_asset`), each write
one raw suggestion row per company per source:

- `scb` reads `se_scb_companies` FINAL: `care_of`, `street_address`, `postal_code`,
  `post_town` as delivered, kind `visiting_or_postal`, slot `''`.
- `bolagsverket` reads `se_bolagsverket_companies` FINAL: the packed `postal_address`
  string into `raw_address`, kind `postal`, slot `''` -- the normalizer parses it.
- `ratsit` reads `se_ratsit_company` FINAL, newest normalized report per company:
  `address_street`, `address_postal_code`, `address_locality`, `address_county`, kind
  `postal`, slot `company`.

A company a source stops delivering writes a tombstone: a NULL row, not a deleted one.
`suggestion_id` is stamped from the INSERT's own `now64()` (one evaluation per query), not
from `suggested_at` read back, so the id always matches the row it names.

Change rule: the shared helper's -- a company is visited when its source table's record is
newer than the company's current suggestion row from that source, or it has never been
suggested by that source; `execute: false` (default) previews the count without writing.

`se_company_address_extract_job` (`jobs.py`) selects the three extractors and
`se_company_address_normalize` (which now `deps` on them); `se_company_address_v2_weekly`
schedules it Mondays 07:05 UTC (`5 7 * * 1`) with `execute: true`, `page_size: 20000` per
extractor and `changed_only: true` on the normalize asset, registered STOPPED. The `v2`
interim name avoids colliding with `address_legacy.py`'s own `se_company_address_weekly`
until the cutover retires that schedule and this one takes the canonical name.
