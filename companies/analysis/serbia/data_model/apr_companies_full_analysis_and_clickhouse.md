# APR Companies Open API: Full Snapshot Analysis and ClickHouse Proposal

## Executive summary

The APR Companies Open API is an authoritative company-master snapshot for
Serbian **privredna društva**. The downloaded response contains **133,634**
companies as at **2026-07-31**. It provides the registration number, registered
name, municipality, current status, incorporation date, legal form, and principal
activity code. It does **not** provide PIB/VAT, street address, representatives,
procurists, board members, founders, beneficial owners, or sole traders.

The entire snapshot was scanned, not sampled. All 133,634 records have all seven
record fields present, non-null, non-empty, and typed as strings. Registration
numbers are unique and valid eight-digit strings. The source is suitable for a
strict typed load, provided the raw response is retained and every new snapshot
passes schema and population checks before publication.

The recommended ClickHouse design is:

1. `rs_apr_company_snapshot_runs` — one manifest row per downloaded snapshot;
2. `rs_apr_company_observations` — append-only company state by snapshot date;
3. `rs_apr_companies_current` — an atomically replaced serving table containing
   exactly the latest accepted complete snapshot.

Do not store the raw 58 MB JSON body in ClickHouse. Preserve it in object/file
storage with its metadata and SHA-256, transform it to a typed canonical artifact,
then load ClickHouse. Do not create a representatives table for this source because
the response contains no representative records.

## Downloaded artifact

| Property | Value |
|---|---|
| Official endpoint | `https://openapi.apr.gov.rs/api/opendata/companies` |
| Local raw file | `companies/data/serbia/raw/api/apr_companies_2026-07-31.json` |
| Snapshot date (`DatumPreseka`) | `2026-07-31` |
| Retrieved | `2026-08-24` |
| Content size | 57,673,691 bytes |
| SHA-256 | `ce68aecfd0ec7fc9c8abe7293f7b4101f4e67ac2812dd6ac266519a76aef3d6c` |
| Record count | 133,634 |
| Format | UTF-8 JSON |
| Authentication | None |
| License | Serbian Open Data License (`sodl`, SODL 1.0) |

APR returns the complete snapshot in a single `GET`. `HEAD` is not supported
(HTTP 405); a downloader must use `GET`. The data.gov.rs description calls the
resource monthly, while its machine metadata says continuous, so the embedded
`DatumPreseka` is the authoritative snapshot identifier.

## Response structure

The top level has exactly two concepts:

```json
{
  "DatumPreseka": "2026-07-31",
  "Podaci": {
    "21141666": {
      "PoslovnoIme": "ENEKS MONT PLUS DOO KRUŠEVAC",
      "SifraOpstine": "70670",
      "NazivOpstine": "КРУШЕВАЦ",
      "NazivStatus": "Активан",
      "DatumOsnivanja": "2015-10-09",
      "NazivPravneForme": "Друштво са ограниченом одговорношћу",
      "SifraDelatnosti": "4322"
    }
  }
}
```

`Podaci` is a JSON object keyed by matčni broj, not an array. The map key is
part of the record and must not be lost during decoding.

## Field-by-field interpretation

| Source path | Proposed column | Meaning | Observed contract | ClickHouse type |
|---|---|---|---|---|
| `Podaci.<key>` | `company_id`, `registration_number` | APR matčni broj | 133,634 unique values; all exactly 8 digits; 17,775 start with `0` | `String` |
| `PoslovnoIme` | `legal_name` | Full registered business name | required; 133,632 distinct; length 8–397 | `String` |
| `SifraOpstine` | `municipality_code` | Registered-seat municipality code | required; 192 distinct; all exactly 5 digits | `LowCardinality(String)` |
| `NazivOpstine` | `municipality_name` | Registered-seat municipality name | required; 192 distinct; Cyrillic | `LowCardinality(String)` |
| `NazivStatus` | `source_status`, `status` | Current source status and mapped status | required; 4 distinct | `LowCardinality(String)` |
| `DatumOsnivanja` | `incorporation_date` | Incorporation date | required; all valid ISO dates; 1918-12-22 to 2026-07-31 | `Date32` |
| `NazivPravneForme` | `legal_form` | Legal form | required; 13 distinct; Cyrillic | `LowCardinality(String)` |
| `SifraDelatnosti` | `primary_activity_code` | Principal KD2010 activity code | required; 571 distinct; all exactly 4 digits | `LowCardinality(String)` |

Important type decisions:

- Matčni broj must remain a string. Converting it to an integer would destroy
  leading zeroes for 17,775 companies.
- `Date32` is required because ordinary ClickHouse `Date` does not represent the
  pre-1970 incorporation dates in this snapshot.
- `legal_name` is high-cardinality and should remain a normal `String`.
- Status, legal form, municipality, and activity code benefit from
  `LowCardinality` encoding.
- Source fields can be non-null in the first implementation. A future missing
  field should quarantine the snapshot as schema drift, not silently weaken the
  contract to nullable data.

## Complete population profile

### Identity and completeness

| Check | Result |
|---|---:|
| Records | 133,634 |
| Distinct matčni broj | 133,634 |
| Invalid eight-digit keys | 0 |
| Keys with a leading zero | 17,775 |
| Lexicographic key range | `00003506`–`29516901` |
| Missing record fields | 0 |
| Null record fields | 0 |
| Empty record fields | 0 |
| Unexpected non-string record fields | 0 |

### Status distribution and mapping

| APR value | Canonical value | `is_active` | Count | Share |
|---|---|---:|---:|---:|
| `Активан` | `active` | 1 | 125,005 | 93.54% |
| `У ликвидацији` | `liquidation` | 0 | 6,185 | 4.63% |
| `У стечају` | `bankruptcy` | 0 | 1,360 | 1.02% |
| `У принудној ликвидацији` | `compulsory_liquidation` | 0 | 1,084 | 0.81% |

Keep both the original APR value and the mapped value. A newly observed APR
status must fail the controlled mapping check until explicitly reviewed.

### Legal-form distribution

| Legal form | Count |
|---|---:|
| Друштво са ограниченом одговорношћу | 126,093 |
| Задруга | 3,102 |
| Представништво страног привредног друштва | 1,281 |
| Огранак страног привредног друштва | 914 |
| Акционарско друштво | 709 |
| Ортачко друштво | 611 |
| Јавно предузеће | 538 |
| Друштвено предузеће | 117 |
| Командитно друштво | 105 |
| Отворено акционарско друштво | 98 |
| Задружни савез | 32 |
| Друго | 31 |
| Затворено акционарско друштво | 3 |

DOO accounts for 94.36% of the snapshot. Keep the authoritative Cyrillic value;
introduce a separate curated form code only when a reviewed mapping exists.

### Incorporation dates

- All 133,634 values parse as `YYYY-MM-DD`.
- Earliest: `1918-12-22`.
- Latest: `2026-07-31`.
- No incorporation date is later than the snapshot date.
- There are 10,836 distinct incorporation dates.

### Municipalities

- There are 192 distinct codes and 192 distinct names.
- Every code is five digits.
- Code-to-name and name-to-code are both one-to-one in this snapshot.
- Largest counts are Novi Beograd 12,785; Novi Sad 11,805; Stari Grad 7,355;
  Zemun 5,922; and Voždovac 5,835.

This consistency is a useful validation rule but does not make the snapshot an
authoritative municipality reference list. A separate reference table should be
loaded only from a documented official code list.

### Principal activities

- There are 571 distinct four-digit `SifraDelatnosti` values.
- The values are KD2010 codes, aligned with NACE Rev. 2, but the feed supplies no
  activity labels.
- Most frequent codes are `4690` (12,929), `4120` (6,609), `7022` (6,256),
  `6201` (5,135), and `4941` (5,102).

Do not invent activity labels in this pipeline. Join a separate reviewed KD2010
reference table when it becomes available.

### Name quality

- 133,632 distinct source names occur across 133,634 companies.
- Two exact/normalized duplicate-name pairs exist; names are not identifiers.
- 10,602 names contain at least one Cyrillic code point, so the name field is not
  reliably Latin-only and can contain mixed-script confusables.
- 1,582 names have leading or trailing whitespace.
- 7,789 contain repeated whitespace.
- At least two names contain control/format characters: one CRLF and one leading
  byte-order mark (`U+FEFF`).
- The maximum observed name length is 397 Unicode characters.

Preserve `legal_name` exactly as supplied for provenance. If search/display needs
a cleaned version, derive a separate value using Unicode NFKC, BOM removal,
trim, and internal-whitespace collapse. Never replace the source value or use a
normalized name as a company key.

## Data not present in this response

The following cannot be pulled from this endpoint:

- PIB / Serbian tax number or VAT identifier;
- street, house number, postal code, email, or phone;
- legal representatives/directors (APR paid group SP3);
- other representatives, procurists, supervisory or executive boards (SP4);
- founders/members/shareholders (SP5);
- branch representatives (SP6);
- beneficial owners;
- sole traders (`preduzetnici`);
- historical status/name/address events; or
- a dissolution/deletion date.

APR's paid status-data products can add selected fields, but they are a separate
contracted source and must have their own source catalogs, privacy review, raw
snapshots, and one-to-many ClickHouse tables. For the current population, SP2 +
SP3 + SP4 would be an indicative **4,677,190 RSD** one-off price at the published
per-entity rates, before confirming scope, VAT, contract terms, and eligible
records with APR.

## Proposed ClickHouse tables

### 1. Snapshot manifest

This records what was downloaded and whether it was accepted. It makes every
serving row traceable to a source artifact.

```sql
CREATE TABLE IF NOT EXISTS corpscout.rs_apr_company_snapshot_runs
(
    source_run_id String,
    snapshot_date Date32,
    source_url String,
    source_object_key String,
    payload_sha256 FixedString(64),
    payload_bytes UInt64,
    record_count UInt64,
    schema_fingerprint FixedString(64),
    run_status LowCardinality(String),
    retrieved_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    accepted_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (snapshot_date, source_run_id);
```

Recommended `run_status` values are `downloaded`, `validated`, `accepted`, and
`rejected`. Keep rejection details in orchestration metadata/logs rather than a
wide free-text column in the analytical table.

### 2. Historical observations

One row represents one company's state in one accepted complete APR snapshot.

```sql
CREATE TABLE IF NOT EXISTS corpscout.rs_apr_company_observations
(
    company_id String,
    registration_number String,
    legal_name String,
    municipality_code LowCardinality(String),
    municipality_name LowCardinality(String),
    source_status LowCardinality(String),
    status LowCardinality(String),
    is_active UInt8,
    incorporation_date Date32,
    legal_form LowCardinality(String),
    primary_activity_code LowCardinality(String),

    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    source_record_uid FixedString(64) DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nstructured\n',
        'serbia_apr_companies\nregistry_company\n',
        source_record_id, '\n', lowerUTF8(source_payload_hash)
    )))),
    state_fingerprint FixedString(64),
    snapshot_date Date32,
    updated_from_raw_at DateTime64(3, 'UTC'),
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
PARTITION BY toYear(snapshot_date)
ORDER BY (company_id, snapshot_date);
```

`source_record_id` is the matčni broj. `source_payload_hash` is a deterministic
SHA-256 of the canonicalized source record; `state_fingerprint` hashes the typed
business state. Keeping both concepts allows provenance identity to remain stable
even if mapper implementation details later change. Annual partitioning avoids
creating one tiny partition for each monthly snapshot while still enabling useful
pruning.

### 3. Latest accepted snapshot

This is the low-latency serving table. It deliberately uses `MergeTree`, not a
view requiring `FINAL` or `argMax` on every query.

```sql
CREATE TABLE IF NOT EXISTS corpscout.rs_apr_companies_current
(
    company_id String,
    registration_number String,
    legal_name String,
    municipality_code LowCardinality(String),
    municipality_name LowCardinality(String),
    source_status LowCardinality(String),
    status LowCardinality(String),
    is_active UInt8,
    incorporation_date Date32,
    legal_form LowCardinality(String),
    primary_activity_code LowCardinality(String),

    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    source_record_uid FixedString(64),
    state_fingerprint FixedString(64),
    snapshot_date Date32,
    updated_from_raw_at DateTime64(3, 'UTC'),
    observed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY company_id;
```

For each accepted run, build `rs_apr_companies_current__staging`, validate it,
then atomically swap it with `rs_apr_companies_current` using `EXCHANGE TABLES`.
This guarantees that records missing from the newest complete feed do not remain
in the current table. It also prevents readers from seeing a partially loaded
snapshot.

### Optional reference tables

Do not create dimensions merely to normalize 133,634 rows. The embedded
low-cardinality strings are sufficient initially. Add these only when official
code-list sources are acquired:

- `rs_kd2010_activities(activity_code, name_sr_cyrl, name_sr_latn, valid_from, valid_to)`;
- `rs_municipalities(municipality_code, name_sr_cyrl, valid_from, valid_to)`.

### Future paid representatives tables

If SP3/SP4 are contracted, representatives are one-to-many and should not be
added as arrays or repeated company rows in `rs_apr_companies_current`. Create a
separate observation/current pair keyed by a deterministic representative
identity, for example:

```text
rs_apr_company_representative_observations
rs_apr_company_representatives_current
```

Their exact DDL must wait for a real paid payload and field catalog. Personal
data, retention, access control, and lawful-purpose review are mandatory.

## Load and publication flow

```text
APR GET
  -> immutable raw JSON + HTTP metadata + SHA-256
  -> validate envelope and full population
  -> canonical typed rows (prefer Parquet)
  -> append accepted rows to observations
  -> build and validate current staging table
  -> atomic EXCHANGE TABLES
  -> mark snapshot run accepted
```

Use the embedded snapshot date, not retrieval time, as the business observation
date. `observed_at` and `updated_from_raw_at` are pipeline timestamps and must be
UTC.

## Acceptance checks for every new snapshot

Reject or quarantine a run before publishing it when any of these checks fail:

1. Response is not UTF-8 JSON with `DatumPreseka` and object-valued `Podaci`.
2. Snapshot date is invalid, in the future, or older than the current accepted run.
3. Record count is implausibly low; alert on a change greater than 5% from the
   previous accepted snapshot and require review rather than accepting blindly.
4. A map key is not exactly eight digits or appears more than once after parsing.
5. The seven expected fields are missing, null, empty, or not strings.
6. Municipality/activity codes violate their five-/four-digit shapes.
7. Incorporation date is invalid, later than the snapshot date, or cannot fit
   `Date32`.
8. A new status appears without an explicit canonical mapping.
9. A municipality code maps to multiple names inside one snapshot.
10. Canonical row count or distinct company count differs from source count.
11. Staging-table count, distinct key count, snapshot date, and aggregate
    fingerprint do not match the validated canonical artifact.

Name whitespace and mixed scripts should be reported as quality metrics but not
reject the run, because they are present in authoritative source values.

## Query and serving considerations

- Primary lookups by matčni broj are efficient with `ORDER BY company_id`.
- Filters on status, municipality, legal form, and activity are compact because
  those columns use `LowCardinality`, but they are not primary-sort dimensions.
- If name search becomes a requirement, add a separately specified normalized
  search column and appropriate text/token index after measuring real queries.
- Do not infer a legal deletion or dissolution from absence in a later snapshot.
  Absence only means "not present in this feed snapshot." Preserve history and
  expose an `in_latest_snapshot` concept outside the authoritative status model
  if product behavior needs it.
- Do not use company name, municipality, or PIB assumptions to merge records.
  Matični broj is the source key.

## Risks and open decisions

- The endpoint has no documented versioned schema; strict validation is the main
  defense against silent drift.
- Publication cadence metadata is inconsistent; orchestrate periodic checks but
  deduplicate by snapshot date and payload hash.
- The open feed is scoped to companies, not the complete universe of Serbian
  registered entities.
- Source names contain whitespace artifacts, mixed scripts, and Unicode
  confusables. Search normalization requires careful testing.
- SODL attribution and license notice must travel with downstream exports.
- Representatives and other paid fields are not authorized or modeled from this
  response; their acquisition would materially expand cost, privacy, and schema
  scope.

## Recommendation

Implement the three-table design above and treat this source as the authoritative
Serbian company-master snapshot for the fields it actually contains. Keep the
loader strict, retain immutable raw and canonical artifacts, publish current data
with an atomic table swap, and keep representatives as a separate future paid
source rather than pretending they exist in the open API.
