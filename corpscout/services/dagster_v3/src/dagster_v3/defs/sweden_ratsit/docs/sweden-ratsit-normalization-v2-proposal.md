# Sweden Ratsit normalization v2 proposal

Status: implemented locally by migration `000346` and
`ratsit-normalizer-v2`; deployment, S3 replay, production audit, and the final
compatibility-column cleanup remain pending. The existing S3 reports can be
reparsed, so the rollout does not require another Ratsit browser request.

## Decision summary

1. Parse establishment employee ranges into minimum, maximum, and open-ended
   fields while retaining the source string.
2. Filter the two observed summary boilerplate families with deterministic,
   named text rules. Do not use fuzzy matching or an LLM.
3. Treat missing SNI markers as absent rows. Map valid five-digit SNI 2025
   values to NACE Rev. 2.1 by their first four digits and join labels from the
   canonical `corpscout.nace_categories` table.
4. Split responsible-person display text into raw display text, name, age, and
   an explicit identity flag. Keep role-only observations.
5. Store all Swedish postal codes as five compact digits.
6. Remove `se_ratsit_people_at_address` from company normalization. A future
   person-page crawl must be a separate restricted, opt-in asset.
7. Omit financial periods that contain no values. Keep employee-only periods
   with an explicit period kind. Keep both asset-total fields for source
   fidelity, but document one canonical field for downstream use.
8. Publish the new rows as `ratsit-normalizer-v2`. Version 1 and version 2 can
   coexist during verification because the existing table identity includes
   `normalizer_version`.

## Observed production baseline

The proposal is based on 94 successfully normalized companies from the
100-company run:

| Segment | Observation |
| --- | --- |
| establishments | 374 employee values, all expressed as a range or labelled value |
| summaries | 419 rows: 270 company-specific and 149 boilerplate |
| company industries | four `Uppgift saknas` missing-value markers |
| establishment industries | one `00000 -` missing-value marker |
| responsible people | 334 identified people and 121 role-only observations |
| financial periods | 596 rows: 6 empty and 13 employee-only |
| people at address | zero rows because only company pages are fetched |

## 1. Establishment employee ranges

### Final columns

Replace `number_of_employees` with:

| Column | Type | Meaning |
| --- | --- | --- |
| `number_of_employees_raw` | `Nullable(String)` | exact Ratsit value |
| `employee_count_min` | `Nullable(UInt32)` | inclusive lower bound |
| `employee_count_max` | `Nullable(UInt32)` | inclusive upper bound; null for an open range |
| `employee_count_open_ended` | `Bool` | true only when no upper bound is supplied |

Examples:

| Source value | min | max | open ended |
| --- | ---: | ---: | --- |
| `0 anställda` | 0 | 0 | false |
| `1-4 anställda` | 1 | 4 | false |
| `10000- anställda` | 10000 | null | true |
| unrecognized text | null | null | false |

The parser should normalize whitespace and accept the singular/plural suffix,
but it must not guess when the remaining text does not match an exact integer,
closed range, or open range. Unrecognized values remain recoverable in the raw
column and increment an `employee_ranges_unparsed` materialization metric.

Validation rules:

- `employee_count_max >= employee_count_min` when both are present;
- an open-ended range requires a minimum and a null maximum;
- a parsed closed value requires both minimum and maximum.

## 2. Company-summary boilerplate

### Recommended filtering method

Use a small code-owned classifier with named, deterministic rules after
trimming and collapsing whitespace:

| Reason code | Match |
| --- | --- |
| `ratsit_source_disclaimer` | exact normalized text equal to `Informationen kommer från myndigheter, privata aktörer och egna insamlingar från Ratsit. Läs mer om datan här.` |
| `municipality_statistics_cta` | normalized text starts with `Läs mer om intressant företagsstatistik i ` |

This is preferable to a hash list because it handles whitespace changes and
municipality names. It is preferable to fuzzy matching or an LLM because the
two known families are exact product copy, and a broad semantic classifier
would eventually discard real company descriptions.

The normalizer should:

1. enumerate the source array before filtering;
2. retain the original `summary_index` so lineage still points to the S3 array;
3. omit matched rows from `se_ratsit_company_summaries`;
4. make `se_ratsit_company.summary_count` the number of retained rows;
5. emit source, retained, and filtered counts by reason in Dagster metadata.

The discarded text does not need a second ClickHouse table because the complete
source document remains in S3. New boilerplate families should only be added
after they appear repeatedly in reviewed data and have a narrow regression
test with false-positive examples.

## 3. Industry codes and NACE

### Missing-value handling

Normalize a source industry only when its compact code matches `^[0-9]{5}$`
and is not `00000`. Therefore both `Uppgift saknas` and `00000 -` produce no
industry row. Count them as `industry_missing_markers` in materialization
metadata rather than treating them as normalization failures.

### Classification revision

The Ratsit sample contains `62900 - Annan it- och dataverksamhet`, which is an
SNI 2025 code. SNI 2025 replaced SNI 2007 on 8 December 2025 and is based on
NACE Rev. 2.1. SCB states that SNI and NACE are identical for the first four
levels; the fifth SNI digit is the Swedish detail level. See the
[SCB SNI classification page](https://www.scb.se/dokumentation/klassifikationer-och-standarder/standard-for-svensk-naringsgrensindelning-sni/)
and the [SCB entry for 62.900](https://snisok.scb.se/62900).

Do not reuse the current `se_industries.nace_rev2_class_code` value blindly:
that table contains SNI 2007 to NACE Rev. 2 mappings. Reuse the same mapping
rule, but identify Ratsit data as SNI 2025 and target NACE Rev. 2.1.

### Columns on both industry-bearing segments

Add the following to `se_ratsit_company_industry_codes` and, for the single
industry attached to a workplace, `se_ratsit_establishments`:

| Column | Value or rule |
| --- | --- |
| `source_industry_code` | validated compact five-digit SNI code |
| `source_industry_code_set` | `SNI_2025` |
| `industry_description_original` | Ratsit's Swedish label |
| `nace_revision` | `NACE_REV_2_1` |
| `nace_code` | dotted first four digits, for example `62.90` |
| `nace_normalized_code` | first four digits, for example `6290` |
| `nace_mapping_method` | `sni_four_digit_prefix` |
| `nace_mapping_status` | `mapped` or `unmapped` |

`mapped` means a row exists in `corpscout.nace_categories` for
`(classification_version = 'NACE_REV_2_1', normalized_code)`. A syntactically
valid SNI value without a reference match should be retained as `unmapped` and
surfaced in metadata; it should not fail the complete company report.

### Easy filtering by NACE name

Do not copy NACE English labels into every content-addressed Ratsit row. Labels
belong to the canonical, revisioned `nace_categories` reference and copying
them would require reparsing every Ratsit report when a reference label changes.

Create two normal ClickHouse views:

- `corpscout.se_ratsit_company_industries_with_nace`
- `corpscout.se_ratsit_establishments_with_nace`

Each view joins its base table to `corpscout.nace_categories` using
`nace_revision = classification_version` and
`nace_normalized_code = normalized_code`. It should expose:

- `nace_class_name_en` from the matched class `description_en`;
- `nace_section_code` from the matched class;
- `nace_section_name_en` from a second join to the section row.

This supports direct filters by either precise activity name or broad NACE
section without creating a second taxonomy. The Dagster multi-asset should
declare `nace_categories_clickhouse` as an upstream dependency and fail before
writing if the reference table is absent or empty.

## 4. Responsible people

### Final columns

| Column | Type | Rule |
| --- | --- | --- |
| `display_name_raw` | `Nullable(String)` | source text, including the age suffix |
| `name` | `Nullable(String)` | suffix removed when it matches `Name (NN)` |
| `age` | `Nullable(UInt16)` | parsed suffix |
| `identity_available` | `Bool` | true when a source person name or profile is available |
| `role` | existing | preserve role text |
| `profile_url` | existing | preserve Ratsit profile URL |

Use one anchored parser for `^(name) (age)$`, with the age in parentheses. When
display text is present but does not match, keep it as both raw display text and
`name`, leave `age` null, and record an unparsed-name metric. Do not derive a
birth date from a current age.

Role-only observations remain rows with `identity_available = false`, null
name/age/profile fields, and their role populated. Downstream person resolvers
must start with `WHERE identity_available`, so a role label can never become a
person identity.

## 5. Postal codes

Use one strict Swedish postal-code normalizer for company and establishment
addresses:

1. remove ASCII whitespace;
2. require exactly five digits;
3. store the compact `NNNNN` form;
4. leave an absent code null; reject a non-empty malformed code for that report
   rather than silently changing it.

This makes company and establishment addresses directly comparable. Formatting
for display (`NNN NN`) belongs at the API/UI boundary, not in source tables.

## 6. People at address

Remove all of the following from the company normalization path:

- the `se_ratsit_people_at_address` Dagster output;
- the table constant and insert path;
- `people_at_address_count` from `se_ratsit_company`;
- the ClickHouse table after the v2 code no longer expects it.

The company crawler must continue to make exactly one company-page request per
selected company. If residents-at-address data is needed later, implement it as
a separate manually enabled asset/job with its own company/person selection,
request cap, rate limit, access policy, and S3 prefix. It must not be an implicit
child of `se_ratsit_normalized` because it adds requests and processes personal
address data unrelated to a confirmed company role.

The JSON parser may ignore a legacy empty `people_at_address` key for backward
compatibility, but it must not normalize it into a company table.

## 7. Financial periods

### Row classification

For every source period, classify populated values after excluding identifiers,
scope, fiscal year, dates, and monetary unit:

| Condition | Action / `period_kind` |
| --- | --- |
| no financial value and no employee count | omit row |
| employee count only | keep as `employment_only` |
| financial values only | keep as `financial_only` |
| financial values and employee count | keep as `financial_and_employment` |

Add `period_kind LowCardinality(String)` to
`se_ratsit_financial_periods`. This is smaller and more faithful to the source
than introducing an employment-history table for only 13 current rows. A
separate employment table can be added later if another source needs a common
employment-history model.

The parent `se_ratsit_financial_reports.period_count` and company
`financial_period_count` must count retained periods, not source placeholders.
Dagster metadata should additionally report `financial_periods_source`,
`financial_periods_empty_omitted`, and each retained period kind.

### Field semantics

- Keep both `total_assets_amount` and `balance_sheet_total_amount` in v2 for
  source fidelity.
- Define `total_assets_amount` as the downstream canonical balance-sheet total.
  Monitor the equality invariant, but do not coalesce contradictory future
  values silently.
- Keep `average_salary` as a unit-neutral source number. The report-level MSEK
  unit must not be applied to it until Ratsit's salary unit is confirmed.
- Other monetary fields continue to use the report-level `monetary_unit` with
  no implicit rescaling.

## Normalizer and asset behavior

Keep the existing single, non-subsettable `se_ratsit_normalized` multi-asset and
set `RATSIT_NORMALIZER_VERSION = 'ratsit-normalizer-v2'`. Direct typed mapping
from the small S3 JSON documents remains appropriate; no dlt, DuckDB, service
layer, or generic rule framework is needed.

Version 2 should read the existing latest successful S3 objects, even when an
identical JSON hash already has version 1 rows. The version in the ClickHouse
identity makes the replay idempotent and permits side-by-side validation without
new web requests.

Materialization metadata should include at least:

- selected, already-v2-normalized, inserted, and rejected report counts;
- retained and filtered summaries by reason;
- valid, missing-marker, mapped, and unmapped industries;
- parsed and unparsed establishment employee ranges;
- identified, role-only, and unparsed responsible people;
- source, omitted-empty, employment-only, and financial period counts;
- inserted row count for every output table.

## Migration and deployment sequence

Migrations run before application deployment, so avoid renaming or dropping
columns still required by the deployed v1 code.

1. Add a forward migration after `000343` that only adds the v2 columns and
   creates the NACE label views. Keep the v1 columns and people-at-address table
   temporarily.
2. Deploy the v2 normalizer. During this compatibility phase it may continue to
   populate the old nullable columns where necessary, but consumers select only
   `normalizer_version = 'ratsit-normalizer-v2'`.
3. Materialize `se_ratsit_normalize_job` against the existing S3 JSON. This step
   performs zero Ratsit requests.
4. Re-run the production audit and compare v2 counts to the expected baseline:
   270 retained summaries, 374 parsed establishment ranges, five omitted SNI
   markers, 334 identified plus 121 role-only people, six omitted empty periods,
   and 13 employment-only periods, subject to any newer S3 report hashes.
5. Switch serving queries to v2.
6. Apply a cleanup migration that drops `number_of_employees`, `display_name`,
   the superseded `industry_code`/`industry_description` columns,
   `people_at_address_count`, and `se_ratsit_people_at_address` after the v2 code
   is running everywhere. Version 1 rows can remain temporarily or be deleted
   in a separately observed maintenance operation.

Do not edit migration `000343`; it is already part of deployed schema history.

## Required tests

- employee parsing for zero, exact, closed, open-ended, whitespace, and invalid
  values;
- both boilerplate rules, whitespace normalization, source-index preservation,
  and company-specific false positives that must remain;
- missing SNI markers, five-digit validation, the `62900 -> 62.90` NACE Rev. 2.1
  mapping, and an unknown-but-valid unmapped code;
- NACE view joins for class and section names;
- named, role-only, malformed-age, and profile-only responsible observations;
- compact and spaced postal codes plus malformed values;
- empty, employment-only, financial-only, and mixed financial periods;
- parent child-count invariants after filtering;
- a full saved Skanska JSON normalization fixture;
- migration assertions for the final columns, views, and removed table.
