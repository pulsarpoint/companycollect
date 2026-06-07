# Finland Countrydata Profile Schema Design

## Summary

Create a country-level PostgreSQL schema named `countrydata_finland` that stores
queryable, denormalized Finland company profile data projected from the existing
Finland PRH YTJ v3 raw source schema.

The existing source schema, `countrydata_finland_prh_ytj`, remains responsible for
source metadata, download audit runs, raw records, payload hashes, and source-level
provenance. The new country schema becomes the stable Finland company profile
surface that Corpscout and future country modules can query without reading PRH raw
JSON directly.

The first loader for this schema is PRH YTJ v3. The schema is intentionally
country-level rather than source-level so later Finland sources can enrich or
override profile fields without changing the query surface.

## Goals

- Add a separate `countrydata_finland` schema for Finland profile tables.
- Keep `countrydata_finland_prh_ytj` as the source-specific raw/audit schema.
- Store all values obtainable from PRH YTJ v3 in typed relational columns where they
  are useful for filtering, joining, translation, or display.
- Store repeatable source arrays as country-level child tables instead of leaving
  them only in JSONB.
- Add `_en` columns for text values that may be Finnish, Swedish, or otherwise not
  English.
- Link Finland profile rows to the default-schema `countries` table.
- Link industry rows to `nace_codes` when a reliable mapping can be made.
- Add translation state to `countrydata_finland.companies`, including an
  `is_translated` boolean that makes it cheap to find companies needing translation.
- Preserve raw-source provenance through foreign keys to
  `countrydata_finland_prh_ytj.raw_records`.

## Non-Goals

- Do not replace the raw PRH source schema.
- Do not merge Finland profile rows into the default-schema `companies` table in
  this phase.
- Do not create a global cross-country profile schema yet.
- Do not invent data not present in PRH YTJ v3, such as officers, beneficial owners,
  phone numbers, email addresses, or financial figures.
- Do not hard-code uncertain PRH code meanings beyond the mappings already proven by
  source samples. Uncertain codes remain stored verbatim.

## Existing Context

The current source schema is `countrydata_finland_prh_ytj`.

It already contains:

- `sources`
- `download_runs`
- `raw_records`

`raw_records` contains one current row per `business_id` plus historical payload
versions by `(business_id, payload_hash)`. It stores a small set of extracted
columns and the full `raw_payload` JSONB.

The generated Finland data-model analysis under
`companies/analysis/finland/data_model` identifies PRH YTJ v3 as the primary
official source. The key interpretation rules from that analysis are:

- `businessId.value` is the primary key.
- `tradeRegisterStatus` is the real active/ceased signal.
- Top-level `status` is not a liveness flag and must be preserved only as a raw
  source code.
- `names[]`, `companyForms[]`, `registeredEntries[]`, and `addresses[]` are
  important repeatable values.
- `mainBusinessLine.type` is a Finnish TOL industry code that may be mappable to
  NACE.
- `registeredEntries[]` can derive VAT, employer, and prepayment-register flags.
- PRH descriptions are often multilingual with `languageCode`: `1` Finnish, `2`
  Swedish, `3` English.

## Schema Boundary

Use two layers:

```text
countrydata_finland_prh_ytj
  sources
  download_runs
  raw_records
    |
    | project current raw records
    v
countrydata_finland
  companies
  company_names
  legal_forms
  industries
  addresses
  registered_entries
  tax_registrations
  websites
  company_situations
```

`countrydata_finland_prh_ytj` answers source-ingestion questions:

- What source was fetched?
- When was it downloaded?
- What did the raw payload look like?
- Which raw payload hash is current for a Finnish Business ID?

`countrydata_finland` answers country-profile questions:

- What is this Finnish company?
- What are its current and historical source-provided names?
- Is it active?
- What is its legal form?
- What industry does it report, and can that be linked to NACE?
- What addresses, website, tax-register flags, and register entries are available?
- Does the company need translation?

## Naming

Country profile schema:

```sql
countrydata_finland
```

Source raw schema:

```sql
countrydata_finland_prh_ytj
```

The country profile schema uses plain business table names because the schema name
already scopes them to Finland:

- `countrydata_finland.companies`
- `countrydata_finland.company_names`
- `countrydata_finland.legal_forms`
- `countrydata_finland.industries`
- `countrydata_finland.addresses`
- `countrydata_finland.registered_entries`
- `countrydata_finland.tax_registrations`
- `countrydata_finland.websites`
- `countrydata_finland.company_situations`

## Common Table Columns

Most country profile tables should include:

```sql
id UUID PRIMARY KEY DEFAULT gen_random_uuid()
company_id UUID REFERENCES countrydata_finland.companies(id) ON DELETE CASCADE
raw_record_id UUID NOT NULL REFERENCES countrydata_finland_prh_ytj.raw_records(id) ON DELETE RESTRICT
business_id TEXT NOT NULL
source_item_hash TEXT
raw_item_payload JSONB NOT NULL DEFAULT '{}'::jsonb
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
metadata JSONB NOT NULL DEFAULT '{}'::jsonb
created_at TIMESTAMPTZ NOT NULL DEFAULT now()
updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
```

`source_item_hash` is a deterministic hash of the source sub-object or derived item.
It allows the projector to preserve translated columns when the source item has not
changed and reset translation state only when source text changes.

`raw_item_payload` stores the source sub-object for that row. It is not the primary
query surface, but it gives us local evidence without re-reading the full raw
payload.

`evidence` stores derivation details, source paths, mapping decisions, and
confidence. It must always be a JSON object.

## Main Table: `companies`

`countrydata_finland.companies` is the current, denormalized company row. It has one
row per Finnish Business ID.

Recommended shape:

```sql
CREATE TABLE countrydata_finland.companies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  country_id UUID NOT NULL REFERENCES countries(id) ON DELETE RESTRICT,
  raw_record_id UUID NOT NULL REFERENCES countrydata_finland_prh_ytj.raw_records(id) ON DELETE RESTRICT,
  source_id UUID NOT NULL REFERENCES countrydata_finland_prh_ytj.sources(id) ON DELETE RESTRICT,
  download_run_id UUID REFERENCES countrydata_finland_prh_ytj.download_runs(id) ON DELETE SET NULL,

  business_id TEXT NOT NULL,
  business_id_registration_date DATE,
  business_id_source_code TEXT,
  vat_id TEXT,
  euid TEXT,
  euid_source_code TEXT,

  legal_name TEXT,
  legal_name_en TEXT,
  legal_name_normalized TEXT,

  trade_register_status_code TEXT,
  trade_register_status_label TEXT,
  trade_register_status_label_en TEXT,
  raw_status_code TEXT,
  lifecycle_status TEXT NOT NULL DEFAULT 'unknown',
  is_active BOOLEAN,

  legal_form_code TEXT,
  legal_form_label TEXT,
  legal_form_label_en TEXT,

  primary_industry_code TEXT,
  primary_industry_code_set TEXT,
  primary_industry_label TEXT,
  primary_industry_label_en TEXT,
  primary_industry_registered_on DATE,
  primary_nace_code_id UUID REFERENCES nace_codes(id) ON DELETE RESTRICT,
  primary_nace_code TEXT,
  primary_nace_revision TEXT,
  primary_nace_title TEXT,
  primary_nace_title_en TEXT,
  primary_nace_mapping_method TEXT,
  primary_nace_mapping_confidence REAL,

  visiting_street TEXT,
  visiting_street_en TEXT,
  visiting_post_code TEXT,
  visiting_city TEXT,
  visiting_city_en TEXT,
  visiting_city_sv TEXT,
  visiting_municipality_code TEXT,

  postal_street TEXT,
  postal_street_en TEXT,
  postal_post_code TEXT,
  postal_city TEXT,
  postal_city_en TEXT,
  postal_city_sv TEXT,
  postal_municipality_code TEXT,

  website_url TEXT,
  website_normalized_url TEXT,
  website_host TEXT,

  has_vat_registration BOOLEAN,
  has_employer_registration BOOLEAN,
  has_prepayment_register BOOLEAN,

  incorporation_date DATE,
  dissolution_date DATE,
  source_updated_at TIMESTAMPTZ,
  country_iso2 TEXT NOT NULL DEFAULT 'FI',

  payload_hash TEXT NOT NULL,
  profile_version TEXT NOT NULL DEFAULT 'countrydata_finland.profile.v1',
  normalized_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

  translation_source_hash TEXT,
  is_translated BOOLEAN NOT NULL DEFAULT false,
  translated_at TIMESTAMPTZ,
  translation_version TEXT,
  translation_error TEXT,

  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT uq_countrydata_finland_companies_business_id UNIQUE (business_id),
  CONSTRAINT chk_countrydata_finland_companies_country CHECK (country_iso2 = 'FI'),
  CONSTRAINT chk_countrydata_finland_companies_lifecycle CHECK (
    lifecycle_status IN ('active', 'ceased', 'intermediate', 'unknown')
  ),
  CONSTRAINT chk_countrydata_finland_companies_nace_confidence CHECK (
    primary_nace_mapping_confidence IS NULL
    OR primary_nace_mapping_confidence BETWEEN 0 AND 1
  ),
  CONSTRAINT chk_countrydata_finland_companies_normalized_payload CHECK (jsonb_typeof(normalized_payload) = 'object'),
  CONSTRAINT chk_countrydata_finland_companies_evidence CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_countrydata_finland_companies_metadata CHECK (jsonb_typeof(metadata) = 'object')
);
```

Indexes:

```sql
CREATE UNIQUE INDEX idx_countrydata_finland_companies_business_id
  ON countrydata_finland.companies (business_id);

CREATE INDEX idx_countrydata_finland_companies_country
  ON countrydata_finland.companies (country_id);

CREATE INDEX idx_countrydata_finland_companies_name
  ON countrydata_finland.companies (legal_name_normalized)
  WHERE legal_name_normalized IS NOT NULL;

CREATE INDEX idx_countrydata_finland_companies_active
  ON countrydata_finland.companies (is_active, lifecycle_status);

CREATE INDEX idx_countrydata_finland_companies_translation_queue
  ON countrydata_finland.companies (is_translated, updated_at)
  WHERE is_translated = false;

CREATE INDEX idx_countrydata_finland_companies_nace
  ON countrydata_finland.companies (primary_nace_code_id)
  WHERE primary_nace_code_id IS NOT NULL;
```

### Translation State

`is_translated` is an aggregate flag for the whole Finland company profile. It is
set to `false` when any source text that may need English output changes.

`translation_source_hash` is computed from the current source text inputs that feed
translation:

- local legal name rendering if translated
- legal form labels
- industry labels
- address text and city values
- registered-entry labels
- company-situation labels

When the projector sees that `translation_source_hash` changed, it sets
`is_translated = false`, clears `translated_at`, and records the new hash. A
translation worker can then fetch companies with:

```sql
SELECT id
FROM countrydata_finland.companies
WHERE is_translated = false
ORDER BY updated_at
LIMIT $1;
```

When all required `_en` columns for that company and its child rows are populated or
confirmed unnecessary, the translation worker sets `is_translated = true`,
`translated_at = now()`, and `translation_version` to the translation pipeline
version.

This design avoids scanning every child table to find translation work while still
allowing child-level `_en` columns.

## `company_names`

Stores current and historical names from `names[]`.

Important fields:

```sql
company_id UUID NOT NULL REFERENCES countrydata_finland.companies(id) ON DELETE CASCADE
raw_record_id UUID NOT NULL REFERENCES countrydata_finland_prh_ytj.raw_records(id) ON DELETE RESTRICT
business_id TEXT NOT NULL
source_position SMALLINT NOT NULL
name TEXT NOT NULL
name_en TEXT
name_type_code TEXT NOT NULL
name_type_label TEXT
name_type_label_en TEXT
version INTEGER
registered_on DATE
ended_on DATE
source_code TEXT
is_current BOOLEAN NOT NULL DEFAULT false
is_primary BOOLEAN NOT NULL DEFAULT false
is_auxiliary BOOLEAN NOT NULL DEFAULT false
source_item_hash TEXT NOT NULL
raw_name_payload JSONB NOT NULL DEFAULT '{}'::jsonb
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
metadata JSONB NOT NULL DEFAULT '{}'::jsonb
```

Uniqueness:

```sql
UNIQUE (company_id, source_item_hash)
```

Indexes:

```sql
CREATE INDEX idx_countrydata_finland_company_names_company
  ON countrydata_finland.company_names (company_id);

CREATE INDEX idx_countrydata_finland_company_names_name
  ON countrydata_finland.company_names (name);

CREATE INDEX idx_countrydata_finland_company_names_current
  ON countrydata_finland.company_names (company_id, is_current, is_primary);
```

Current legal name selection:

- `type = '1'`
- `endDate` is null
- if multiple qualify, use the latest `registrationDate`

Auxiliary names:

- `type = '3'` means auxiliary trade name.
- `type = '2'` and `type = '4'` should be preserved as source name types even when
  the exact business meaning is not needed by the main company row.

## `legal_forms`

Stores current and historical legal-form values from `companyForms[]`.

Important fields:

```sql
company_id UUID NOT NULL REFERENCES countrydata_finland.companies(id) ON DELETE CASCADE
raw_record_id UUID NOT NULL REFERENCES countrydata_finland_prh_ytj.raw_records(id) ON DELETE RESTRICT
business_id TEXT NOT NULL
source_position SMALLINT NOT NULL
legal_form_code TEXT NOT NULL
legal_form_label TEXT
legal_form_label_en TEXT
legal_form_label_fi TEXT
legal_form_label_sv TEXT
version INTEGER
registered_on DATE
ended_on DATE
source_code TEXT
is_current BOOLEAN NOT NULL DEFAULT false
source_item_hash TEXT NOT NULL
raw_legal_form_payload JSONB NOT NULL DEFAULT '{}'::jsonb
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
metadata JSONB NOT NULL DEFAULT '{}'::jsonb
```

`legal_form_label_en` should use PRH `descriptions[languageCode=3]` when present.
If PRH does not provide English, the translation pipeline can fill it.

Current legal form selection:

- `endDate` is null
- if multiple qualify, use the latest `registrationDate`

## `industries`

Stores the PRH `mainBusinessLine` as a queryable industry row and links it to NACE
when possible.

Important fields:

```sql
company_id UUID NOT NULL REFERENCES countrydata_finland.companies(id) ON DELETE CASCADE
raw_record_id UUID NOT NULL REFERENCES countrydata_finland_prh_ytj.raw_records(id) ON DELETE RESTRICT
business_id TEXT NOT NULL
classification_type TEXT NOT NULL DEFAULT 'main_business_line'
source_field TEXT NOT NULL DEFAULT 'mainBusinessLine'
position SMALLINT NOT NULL DEFAULT 1
source_industry_code TEXT NOT NULL
source_industry_code_set TEXT
source_industry_label TEXT
source_industry_label_en TEXT
source_industry_label_fi TEXT
source_industry_label_sv TEXT
registered_on DATE
source_code TEXT
nace_code_id UUID REFERENCES nace_codes(id) ON DELETE RESTRICT
mapped_nace_code TEXT
nace_revision TEXT
nace_title TEXT
nace_title_en TEXT
mapping_method TEXT
mapping_confidence REAL
is_primary BOOLEAN NOT NULL DEFAULT true
source_item_hash TEXT NOT NULL
raw_industry_payload JSONB NOT NULL DEFAULT '{}'::jsonb
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
metadata JSONB NOT NULL DEFAULT '{}'::jsonb
```

NACE mapping:

- Store PRH/TOL code exactly in `source_industry_code`.
- Store PRH code set exactly in `source_industry_code_set`.
- Resolve to `nace_codes.id` only when a deterministic mapping is available.
- For a five-digit TOL code, attempt a NACE class candidate from the first four
  digits formatted as `NN.NN`.
- For a four-digit numeric code, attempt `NN.NN`.
- Prefer an explicit `nace_code_aliases` match when such aliases exist.
- Only link active NACE class rows.
- Leave `nace_code_id` null when the mapping is uncertain.

Mapping metadata:

- `mapping_method = 'tol5_to_nace_class'` for a five-digit TOL truncation.
- `mapping_method = 'nace_exact'` for exact NACE-shaped source codes.
- `mapping_method = 'nace_alias'` for alias-table matches.
- `mapping_confidence = 1.0` for exact or alias matches.
- `mapping_confidence = 0.85` for deterministic TOL five-digit truncation until a
  Finland-specific TOL mapping table is added.

This mirrors existing Corpscout NACE patterns while keeping the original Finnish
classification code available.

## `addresses`

Stores address rows from `addresses[]`.

Important fields:

```sql
company_id UUID NOT NULL REFERENCES countrydata_finland.companies(id) ON DELETE CASCADE
raw_record_id UUID NOT NULL REFERENCES countrydata_finland_prh_ytj.raw_records(id) ON DELETE RESTRICT
business_id TEXT NOT NULL
source_position SMALLINT NOT NULL
address_type_code INTEGER NOT NULL
address_type TEXT NOT NULL
address_type_label TEXT
address_type_label_en TEXT
address_country_id UUID REFERENCES countries(id) ON DELETE RESTRICT
address_country_code TEXT
address_country_label TEXT
address_country_label_en TEXT
street TEXT
street_en TEXT
building_number TEXT
entrance TEXT
apartment_number TEXT
post_office_box TEXT
care_of TEXT
care_of_en TEXT
post_code TEXT
city TEXT
city_en TEXT
city_fi TEXT
city_sv TEXT
municipality_code TEXT
normalized_full_address TEXT
normalized_full_address_en TEXT
registered_on DATE
source_code TEXT
is_current BOOLEAN NOT NULL DEFAULT true
source_item_hash TEXT NOT NULL
raw_address_payload JSONB NOT NULL DEFAULT '{}'::jsonb
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
metadata JSONB NOT NULL DEFAULT '{}'::jsonb
```

Address type mapping:

- `1` = `visiting`
- `2` = `postal`
- unknown values are stored as `other`

Country reference:

- `companies.country_id` always references the Finland row in `countries`.
- `addresses.address_country_id` references `countries` when the PRH address
  country value can be resolved.
- If PRH omits the address country and the record is clearly domestic, use Finland
  as the resolved address country and record that derivation in `evidence`.

City language handling:

- `city_fi` from `postOffices[languageCode=1]`
- `city_sv` from `postOffices[languageCode=2]`
- `city_en` from translation or from the best English rendering available
- `city` stores the preferred source display value, usually Finnish first, then
  Swedish, then any available city value

`normalized_full_address` is assembled from source columns and should not hide the
individual address fields.

## `registered_entries`

Stores register-entry history from `registeredEntries[]`.

Important fields:

```sql
company_id UUID NOT NULL REFERENCES countrydata_finland.companies(id) ON DELETE CASCADE
raw_record_id UUID NOT NULL REFERENCES countrydata_finland_prh_ytj.raw_records(id) ON DELETE RESTRICT
business_id TEXT NOT NULL
source_position SMALLINT NOT NULL
register_code TEXT NOT NULL
register_label TEXT
register_label_en TEXT
entry_type_code TEXT NOT NULL
entry_type_label TEXT
entry_type_label_en TEXT
entry_type_label_fi TEXT
entry_type_label_sv TEXT
authority_code TEXT
authority_label TEXT
authority_label_en TEXT
registered_on DATE
ended_on DATE
is_current BOOLEAN NOT NULL DEFAULT false
source_item_hash TEXT NOT NULL
raw_registered_entry_payload JSONB NOT NULL DEFAULT '{}'::jsonb
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
metadata JSONB NOT NULL DEFAULT '{}'::jsonb
```

Known register-code interpretation from the Finland data-model analysis:

- `1` = Business Information System
- `4` = Trade Register
- `5` = Employer Register
- `6` = VAT Register
- `7` = Prepayment Register

These labels should be treated as derived metadata and recorded in `evidence`.
The raw `register_code`, `entry_type_code`, and `authority_code` are always stored
verbatim.

## `tax_registrations`

Stores derived current tax-register status from `registeredEntries[]`.

Recommended one row per company and registration type:

```sql
company_id UUID NOT NULL REFERENCES countrydata_finland.companies(id) ON DELETE CASCADE
raw_record_id UUID NOT NULL REFERENCES countrydata_finland_prh_ytj.raw_records(id) ON DELETE RESTRICT
business_id TEXT NOT NULL
registration_type TEXT NOT NULL
register_code TEXT NOT NULL
current_registered BOOLEAN NOT NULL
current_registered_entry_id UUID REFERENCES countrydata_finland.registered_entries(id) ON DELETE SET NULL
first_registered_on DATE
last_registered_on DATE
ended_on DATE
source_item_hash TEXT NOT NULL
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
metadata JSONB NOT NULL DEFAULT '{}'::jsonb
```

Allowed `registration_type` values:

- `vat`
- `employer`
- `prepayment_register`

Derivation rule:

- A registration is current when a matching `registeredEntries[]` row has the
  expected `register` code and null `endDate`.

Derived values are also denormalized onto `companies` as:

- `has_vat_registration`
- `has_employer_registration`
- `has_prepayment_register`

## `websites`

Stores website values from `website`.

Important fields:

```sql
company_id UUID NOT NULL REFERENCES countrydata_finland.companies(id) ON DELETE CASCADE
raw_record_id UUID NOT NULL REFERENCES countrydata_finland_prh_ytj.raw_records(id) ON DELETE RESTRICT
business_id TEXT NOT NULL
url TEXT NOT NULL
normalized_url TEXT NOT NULL
host TEXT
path TEXT
registered_on DATE
ended_on DATE
is_current BOOLEAN NOT NULL DEFAULT true
is_primary BOOLEAN NOT NULL DEFAULT true
source_item_hash TEXT NOT NULL
raw_website_payload JSONB NOT NULL DEFAULT '{}'::jsonb
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
metadata JSONB NOT NULL DEFAULT '{}'::jsonb
```

`companies.website_url`, `companies.website_normalized_url`, and
`companies.website_host` store the current primary website for fast list/detail
queries. The `websites` table preserves the item-level evidence and dates.

## `company_situations`

Stores rows from `companySituations[]`.

The sample data had no populated situations, so this table is intentionally simple
and keeps the raw item payload for future validation.

Important fields:

```sql
company_id UUID NOT NULL REFERENCES countrydata_finland.companies(id) ON DELETE CASCADE
raw_record_id UUID NOT NULL REFERENCES countrydata_finland_prh_ytj.raw_records(id) ON DELETE RESTRICT
business_id TEXT NOT NULL
source_position SMALLINT NOT NULL
situation_type_code TEXT NOT NULL
situation_label TEXT
situation_label_en TEXT
situation_label_fi TEXT
situation_label_sv TEXT
registered_on DATE
ended_on DATE
is_current BOOLEAN NOT NULL DEFAULT false
source_item_hash TEXT NOT NULL
raw_situation_payload JSONB NOT NULL DEFAULT '{}'::jsonb
evidence JSONB NOT NULL DEFAULT '{}'::jsonb
metadata JSONB NOT NULL DEFAULT '{}'::jsonb
```

## Projection Flow

The source package keeps downloading and processing PRH records as it does today.
Database projection is scheduler/database work.

Recommended flow:

1. PRH downloader stores an NDJSON snapshot.
2. PRH processor decodes each record.
3. Scheduler DB store upserts into `countrydata_finland_prh_ytj.raw_records`.
4. When the raw payload hash is new or the current raw record changed, a Finland
   projector updates `countrydata_finland`.
5. The projector runs in the same database transaction as the raw-record store for
   that chunk.
6. If profile source text changed, the projector sets `companies.is_translated =
   false`.

The projector should be concrete and source-specific:

```text
FinlandPRHYTJDBStore.StoreCompanies
  -> upsert raw records
  -> project changed records into countrydata_finland
```

Do not add a generic interface before another real Finland source needs the same
country projector.

## Projection Idempotency

Projection must be safe to run multiple times for the same raw record.

Rules:

- `companies` is upserted by `business_id`.
- Child rows are upserted by `(company_id, source_item_hash)` where possible.
- If an array item disappears from the current raw payload, the projector can delete
  or mark the old child row inactive for that company. The preferred first
  implementation is delete-and-recreate for rows whose `source_item_hash` is no
  longer present, while preserving rows whose hash still exists.
- If a child row has the same `source_item_hash`, keep existing `_en` translated
  values.
- If a child row has a new hash, set the parent company `is_translated = false`.
- If the company-level `payload_hash` is unchanged, skip profile projection except
  for `last_seen_at` bookkeeping.

## Text And `_en` Column Rules

Every local-language descriptive text column should have an English counterpart.

Examples:

- `legal_name` / `legal_name_en`
- `legal_form_label` / `legal_form_label_en`
- `primary_industry_label` / `primary_industry_label_en`
- `street` / `street_en`
- `city` / `city_en`
- `register_label` / `register_label_en`
- `entry_type_label` / `entry_type_label_en`
- `situation_label` / `situation_label_en`

When PRH provides English directly through `descriptions[languageCode=3]`, use that
value as the `_en` value and record that it came from source data.

When PRH only provides Finnish or Swedish, leave `_en` null until the translation
pipeline fills it.

Company names are special:

- The official legal name must remain in `legal_name`.
- `legal_name_en` is only a translated/display rendering.
- `legal_name_en` must not be used as a legal identifier.

## Countries Reference

`countrydata_finland.companies.country_id` is required and references
`countries(id)`.

The migration should resolve it with:

```sql
SELECT id FROM countries WHERE iso_alpha2 = 'FI'
```

If the Finland country row is missing, the migration should fail rather than create
profile rows without country linkage.

Address rows can also reference `countries(id)` through `address_country_id` when
the address country is known or safely derivable.

## NACE Reference

Industry rows and the main company row should reference `nace_codes(id)` when the
source industry code can be mapped.

The stored fields should include both source and mapped forms:

- `source_industry_code`
- `source_industry_code_set`
- `source_industry_label`
- `source_industry_label_en`
- `nace_code_id`
- `mapped_nace_code`
- `nace_revision`
- `nace_title`
- `nace_title_en`
- `mapping_method`
- `mapping_confidence`

This avoids losing the original Finnish code while still allowing standardized NACE
queries.

## sqlc Boundary

The database queries should be owned by sqlc:

- Add a query file for `countrydata_finland`.
- Generate insert/upsert/query parameter structs with sqlc.
- Let the Finland projection code build sqlc params from PRH records.
- Avoid mirror DTOs that copy sqlc row fields into identical Go structs.

Recommended query groups:

- `UpsertFinlandCompany`
- `UpsertFinlandCompanyName`
- `DeleteMissingFinlandCompanyNames`
- `UpsertFinlandLegalForm`
- `DeleteMissingFinlandLegalForms`
- `UpsertFinlandIndustry`
- `DeleteMissingFinlandIndustries`
- `UpsertFinlandAddress`
- `DeleteMissingFinlandAddresses`
- `UpsertFinlandRegisteredEntry`
- `DeleteMissingFinlandRegisteredEntries`
- `UpsertFinlandTaxRegistration`
- `UpsertFinlandWebsite`
- `DeleteMissingFinlandWebsites`
- `UpsertFinlandCompanySituation`
- `DeleteMissingFinlandCompanySituations`
- `ListFinlandCompaniesNeedingTranslation`
- `MarkFinlandCompanyTranslated`

## Testing

Migration tests:

- Verify `countrydata_finland` schema exists.
- Verify all tables exist.
- Verify `companies.country_id` references `countries`.
- Verify `companies.raw_record_id` references
  `countrydata_finland_prh_ytj.raw_records`.
- Verify `industries.nace_code_id` and `companies.primary_nace_code_id` reference
  `nace_codes`.
- Verify `companies.is_translated` defaults to `false`.
- Verify JSONB columns are constrained to objects.

Projection unit tests:

- Project the real Finland PRH sample record from
  `companies/analysis/finland/data_model/sources/prh_ytj_v3/sample_record.json`.
- Verify `companies.business_id`, `vat_id`, `euid`, active status, legal name,
  legal form, website, and source timestamps.
- Verify current legal name is selected from `names[]`.
- Verify auxiliary names are inserted into `company_names`.
- Verify current legal form is selected from `companyForms[]`.
- Verify PRH `status` is preserved as `raw_status_code` and not used as liveness.
- Verify `tradeRegisterStatus='1'` and null `endDate` produce active lifecycle.
- Verify `registeredEntries[]` derive VAT/employer/prepayment flags.
- Verify addresses preserve Finnish/Swedish city values and have `_en` columns.
- Verify industry mapping populates `nace_code_id` only when a NACE match exists.
- Verify a changed source text hash resets `companies.is_translated` to `false`.
- Verify unchanged source item hashes preserve existing `_en` values.

Integration tests:

- Run a local database transaction using migrated schemas.
- Insert or upsert a PRH raw record.
- Project it into `countrydata_finland`.
- Query the country profile tables and assert row counts and key joins.

Live tests:

- Keep live PRH sync tests opt-in.
- A live test can run with `--max-pages 20` and verify that at least one projected
  profile row exists.
- Live tests should not be required in normal CI because they depend on PRH network
  availability.

## Migration Strategy

Implement as a new database migration after the existing countrydata source-storage
migrations.

The migration should:

1. `CREATE SCHEMA IF NOT EXISTS countrydata_finland`.
2. Create `companies`.
3. Create child tables.
4. Add indexes and constraints.
5. Grant schema usage and read access consistently with existing countrydata
   schemas.

Do not backfill in the migration itself. Backfill should be performed by running the
Finland PRH sync/projector command after the migration. This keeps migrations fast
and deterministic.

## Backfill Strategy

After the migration:

1. Run the existing Finland PRH sync to populate current raw records.
2. Run the profile projector over current raw records.
3. Verify counts:
   - current PRH raw records
   - projected Finland company rows
   - child rows by table
   - companies needing translation
4. Start translation on rows where `companies.is_translated = false`.

The first implementation can project only records processed in the current sync.
A later maintenance command can project all current raw records to rebuild
`countrydata_finland` from raw data.

## Open Decisions Resolved In This Design

- The schema is country-level: `countrydata_finland`.
- PRH raw/audit data stays source-level: `countrydata_finland_prh_ytj`.
- The main country profile row is `countrydata_finland.companies`.
- `companies.is_translated` is the main translation queue flag.
- Repeatable source arrays are stored in child tables.
- `_en` columns are included for local-language display text.
- NACE links are nullable and only populated when mapping is reliable.
- Backfill is done by a command, not inside the migration.

## Future Extensions

Future Finland sources can add data to the same `countrydata_finland` schema.

Examples:

- PRH financial-statement API can add `financial_statements`.
- Municipality or geography reference data can add stronger address joins.
- A Finland-specific TOL mapping table can improve `industries.nace_code_id`
  confidence.
- A later global profile layer can read from `countrydata_finland` and other
  country schemas once several country-level schemas prove stable.
