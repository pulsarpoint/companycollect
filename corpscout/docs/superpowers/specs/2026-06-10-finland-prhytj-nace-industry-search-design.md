# Finland PRH YTJ NACE Industry Search Design

## Goal

Add industry search and filtering to the Finland PRH YTJ company explorer by mapping Finland source industry values to NACE, while preserving the original PRH YTJ `TOIMI*` values in explorer output.

The UI should let users search by industry name or code, select NACE hierarchy levels such as section, division, group, or class, and also filter by original Finland source industry values.

## Current State

Finland PRH YTJ normalized ClickHouse tables already store source industry data:

- `fi_prhytj_business_lines`
- `fi_prhytj_business_line_descriptions`

The explorer cache exposes:

- `main_business_line_code`
- `main_business_line_code_set`
- `main_business_line_description_en`

The current downloaded PRH YTJ file contains these source code sets:

- `TOIMI4`
- `TOIMI3`
- `TOIMI2`
- `TOIMI`
- missing code set

`TOIMI4` is the dominant current code set in the downloaded data. `TOIMI3` is also common. `TOIMI2`, `TOIMI`, and missing code sets must not be blindly mapped to current NACE.

NACE reference data is mirrored into ClickHouse under `corpscout_reference`:

- `nace_classifications`
- `nace_codes`
- `nace_code_aliases`

`nace_codes` includes hierarchy columns:

- `section_code`
- `division_code`
- `group_code`
- `class_code`

## Mapping Design

Create a source-specific ClickHouse mapping table:

```text
corpscout_sources.fi_prhytj_industry_nace_mappings
```

It stores one row per distinct Finland PRH YTJ source industry value.

Columns:

```text
source_code_set
source_code
source_code_prefix4
source_code_dotted4
source_extra_digit
source_description_en
nace_revision
nace_code
nace_normalized_code
nace_section_code
nace_division_code
nace_group_code
nace_class_code
nace_title_en
mapping_method
mapping_status
mapped_at
```

Mapping rules for this phase:

```text
TOIMI4 -> NACE Rev. 2.1 by four-digit prefix
TOIMI3 -> NACE Rev. 2 by four-digit prefix
TOIMI2 -> unmapped
TOIMI -> unmapped
missing code set -> unmapped
```

Example:

```text
source_code_set: TOIMI4
source_code: 68203
source_code_prefix4: 6820
source_code_dotted4: 68.20
source_extra_digit: 3
nace_revision: 2.1
nace_code: 68.20
nace_normalized_code: 6820
mapping_method: toimi_5_digit_prefix
mapping_status: mapped
```

Unmapped rows should still be stored with `mapping_status = 'unmapped'` so the system can report coverage and show original source industries in the UI.

## Explorer Cache Output

Extend `fi_prhytj_company_explorer_cache` with mapped NACE fields:

```text
nace_revision
nace_code
nace_normalized_code
nace_section_code
nace_division_code
nace_group_code
nace_class_code
nace_title_en
nace_mapping_method
nace_mapping_status
```

Keep the original source fields:

```text
main_business_line_code
main_business_line_code_set
main_business_line_description_en
```

The explorer API should expose both the source truth and the mapped NACE projection.

Example response shape:

```json
{
  "mainBusinessLineCode": "68203",
  "mainBusinessLineCodeSet": "TOIMI4",
  "mainBusinessLineDescriptionEn": "Rental and operating of own or leased non-residential real estate",
  "naceRevision": "2.1",
  "naceCode": "68.20",
  "naceSectionCode": "L",
  "naceDivisionCode": "68",
  "naceGroupCode": "68.2",
  "naceClassCode": "68.20",
  "naceTitleEn": "Real estate activities",
  "naceMappingStatus": "mapped"
}
```

## API Design

Add a NACE reference endpoint:

```text
GET /api/v1/reference/nace?revision=2.1
GET /api/v1/reference/nace?revision=2
```

Return flat rows with parent links. The frontend builds trees or grouped lists from the flat structure.

Example:

```json
{
  "revision": "2.1",
  "code": "68.20",
  "normalizedCode": "6820",
  "levelName": "class",
  "parentCode": "68.2",
  "title": "Real estate activities"
}
```

Extend Finland explorer filter options:

```text
GET /api/v1/sources/finland_prhytj/explorer/filter-options
```

Return a combined searchable industry option list:

```json
{
  "industryOptions": [
    {
      "id": "nace:2.1:68.20",
      "kind": "nace",
      "filterValue": "68.20",
      "revision": "2.1",
      "levelName": "class",
      "code": "68.20",
      "title": "Real estate activities",
      "breadcrumb": "L / 68 / 68.2",
      "companyCount": 132845,
      "searchText": "68.20 real estate activities l 68 68.2"
    },
    {
      "id": "source:TOIMI4:68203",
      "kind": "source_industry",
      "filterValue": "TOIMI4:68203",
      "codeSet": "TOIMI4",
      "code": "68203",
      "title": "Rental and operating of own or leased non-residential real estate",
      "mappedNaceCode": "68.20",
      "companyCount": 33252,
      "searchText": "68203 TOIMI4 rental operating real estate 68.20"
    }
  ]
}
```

The backend does not need a persisted reverse-search table yet. The option list can be generated from the explorer cache and mapping table because the row count is small enough for a filter sheet dropdown.

## Filter Semantics

Explorer company search accepts repeated filters:

```text
GET /api/v1/sources/finland_prhytj/explorer?industry_nace=68
GET /api/v1/sources/finland_prhytj/explorer?industry_nace=68.20
GET /api/v1/sources/finland_prhytj/explorer?source_industry=TOIMI4:68203
```

`industry_nace` filter behavior:

```text
industry_nace=L     -> nace_section_code = 'L'
industry_nace=68    -> nace_division_code = '68'
industry_nace=68.2  -> nace_group_code = '68.2'
industry_nace=68.20 -> nace_class_code = '68.20'
```

Filtering by `industry_nace=68` includes both mapped revisions:

```text
TOIMI4 mapped to NACE Rev. 2.1 division 68
TOIMI3 mapped to NACE Rev. 2 division 68
```

Multiple values inside the same filter group are ORed:

```text
industry_nace=68&industry_nace=82
```

Different filter groups are ANDed:

```text
industry_nace=68&source_industry=TOIMI4:68203
```

The source industry filter is an exact source value match:

```text
source_code_set = 'TOIMI4'
source_code = '68203'
```

## UI Design

Extend the Finland explorer filter sheet with an Industry section.

Use a searchable dropdown or combobox backed by `industryOptions`:

- Empty state shows the first options sorted by `companyCount`.
- Typing searches title, NACE code, source code, code set, and breadcrumb through `searchText`.
- Selecting a NACE option writes `industry_nace=<code>` to the URL.
- Selecting a source industry option writes `source_industry=<codeSet>:<code>` to the URL.
- Selected filters appear as removable badges.
- Clear filters removes `industry_nace`, `source_industry`, `form`, `active`, and pagination.

The UI should present source industry options with the original `TOIMI*` value and the mapped NACE code when available.

## Data Flow

1. NACE sync loads reference rows into `corpscout_reference.nace_codes`.
2. Finland PRH YTJ import loads normalized source rows into `fi_prhytj_business_lines` and `fi_prhytj_business_line_descriptions`.
3. A new source action named `map_industries_to_nace` reads distinct Finland source industries and writes `fi_prhytj_industry_nace_mappings`.
4. Explorer cache refresh joins `fi_prhytj_company_explorer` to `fi_prhytj_industry_nace_mappings` and stores mapped NACE fields in `fi_prhytj_company_explorer_cache`.
5. Explorer APIs read only from the cache and mapping table for company rows and filter options.

## Error Handling

If NACE reference tables are empty, `map_industries_to_nace` should fail with a clear operational error.

If a source industry cannot be mapped, it should not fail the action. Store it with `mapping_status = 'unmapped'`.

Explorer rows must continue to show original source industry fields when mapped NACE fields are null.

Boundary layers log errors once with `log/slog`. Lower layers wrap and return errors with `github.com/cockroachdb/errors`.

## Testing

Backend coverage:

- `TOIMI4 68203 -> NACE Rev. 2.1 68.20`
- `TOIMI3 68203 -> NACE Rev. 2 68.20`
- `TOIMI2 68203 -> unmapped`
- missing code set -> unmapped
- `industry_nace=68` filters by division
- `industry_nace=68.2` filters by group
- `industry_nace=68.20` filters by class
- multiple `industry_nace` values OR together
- `source_industry=TOIMI4:68203` filters exact source industry
- filter options return searchable industry options with counts

Frontend coverage:

- URL parsing and serialization for `industry_nace`
- URL parsing and serialization for `source_industry`
- dropdown search by NACE title
- dropdown search by NACE code
- dropdown search by TOIMI code
- selected industry badges
- clear filters behavior

## Future Work

Add official correspondence tables for `TOIMI2`, `TOIMI`, and other older Finland classifications.

If industry options become too large across many countries and sources, add a server-side `q=` search endpoint or a dedicated ClickHouse search options table. This is not needed for the first Finland PRH YTJ implementation.
