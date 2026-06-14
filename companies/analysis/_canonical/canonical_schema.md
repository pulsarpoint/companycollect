# Canonical Company Schema — Cross-Country Contract

The single target every source dossier and every country's conformance output
resolves into. **Company-primary, country-shaped only for collection.** ClickHouse
holds eight tables and knows nothing about sources.

## Spine

Two country-independent entity tables (`company`, `persons`); everything else is a
per-(company, country) fact anchored to a country record, partitioned by country,
rolled up to the entity via `company_uid`.

```
company   persons        ← two country-independent entity spines
  ▲ company_uid              ▲ person_uid
  ├── registrations          one per (company, country)            PARTITION BY country
  ├── financials             per (company, country, period)         PARTITION BY country
  ├── company_websites       1 entity-main + N per-registration     PARTITION BY country   (multi-valued)
  ├── company_contacts       phone / email / fax / social           PARTITION BY country   (multi-valued)
  ├── company_people ────────person↔registration: owners/officers   PARTITION BY country   (→ persons)
  └── company_relationships  company↔company edges                  PARTITION BY reporting_country
```

- **Company-primary serving:** query by `company_uid`, roll up all of a company's
  registrations / financials / relationships regardless of where collected.
- **Country-shaped collection:** each country's conformance writes/replaces its own
  partition across the three child tables; rebuilding a country is one operation.

## Identity & resolution

`company_uid` is country-independent and stable across re-runs. Minting precedence:

1. **LEI** if the entity has one (globally unique, stable) → `uid = lei`.
2. else **surrogate** over the primary registration → `uid = "c:" + sha1(country + ':' + registration_number)`.

`country:registration_number` is the **registration** key, never the company key.

**Cross-country match precedence** (registrations → same entity), highest first:

| Rank | Key | Confidence |
|---|---|---|
| 1 | LEI | exact, global |
| 2 | EUID / BRIS (`euId`) | exact, EU branch↔parent |
| 3 | (country, national registration_number) | exact, within country |
| 4 | VAT number | strong |
| 5 | name + address + jurisdiction (+ directors) | probabilistic fallback |

Ranks 1–4 are deterministic; rank 5 is the fuzzy long tail. The single-registration
majority never reaches rank 5 (1 registration → 1 company).

## Provenance & licensing

CC-BY sources require attribution, and merges must stay debuggable, so origin
travels **inside the rows** even though ClickHouse isn't structured by source:

- `company.field_provenance` — `Map(field → source_slug)`: which source won each
  resolved field (survivorship audit + attribution).
- `company.sources` — every source slug that contributed to the entity.
- child tables carry `registry_source` (one source per row) + `source_run_id`.

## `extras` & promotion

Country/registry-specific fields that aren't (yet) canonical land in an `extras`
column (ClickHouse native `JSON`; `Map(String,String)` fallback). Never lost,
~free when sparse. Promote `extras` → typed column once a field proves prevalent
**or** product-central, driven by:

```sql
SELECT key, count() AS rows, uniq(country) AS countries
FROM registrations ARRAY JOIN JSONExtractKeys(extras) AS key
GROUP BY key ORDER BY rows DESC;
```

---

## Tables

All tables `ReplacingMergeTree(updated_at)` — re-runs supersede, query with `FINAL`.

### `company` — entity spine (country-independent)

```sql
CREATE TABLE company (
  company_uid          String,                          -- LEI or "c:<sha1>"
  uid_scheme           LowCardinality(String),          -- 'lei' | 'surrogate'
  lei                  Nullable(String),
  primary_name         String,                          -- resolved current legal name
  primary_name_latin   Nullable(String),                -- transliterated, for search/match
  name_aliases         Array(String),                   -- auxiliary + historical, for matching
  status               LowCardinality(String),          -- active | inactive | dissolved | unknown
  legal_form_code      Nullable(String),                -- EU ELF code (canonical)
  legal_form_label     Nullable(String),
  home_country         LowCardinality(String),          -- best-guess domicile ISO2, 'XX' if unknown
  incorporation_date   Nullable(Date),
  dissolution_date     Nullable(Date),
  registration_count   UInt16,                           -- number of country records
  operating_countries  Array(LowCardinality(String)),   -- derived from registrations
  identifiers          Array(Tuple(                      -- every known id across countries
                         scheme  LowCardinality(String), -- 'lei'|'euid'|'national'|'vat'
                         value   String,
                         country LowCardinality(String))),
  ultimate_parent_uid  Nullable(String),                -- convenience from the relationship graph
  primary_website      Nullable(String),                -- entity-main site (denormalized from company_websites)
  total_employees      Nullable(UInt32),                -- best entity-level estimate (rolled up)
  employee_count_band  LowCardinality(String),          -- size class, '' if unknown
  primary_email        Nullable(String),                -- denormalized from company_contacts
  primary_phone        Nullable(String),
  field_provenance     Map(String, String),             -- field -> winning source_slug
  sources              Array(LowCardinality(String)),   -- all contributing sources (attribution)
  extras               JSON,                             -- rare/unresolved entity-level attributes
  resolution_version   String,                           -- entity-resolution logic version
  first_seen_at        DateTime64(3, 'UTC'),
  updated_at           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY home_country                                -- coarse; 'XX' bucket for unknown
ORDER BY company_uid;
```

`home_country` partitioning is coarse and single-valued (domicile, **not**
registration country). Alternative if domicile churns during resolution:
`PARTITION BY cityHash64(company_uid) % 64`. Point lookups hit `ORDER BY company_uid`.

### `registrations` — one per (company, country); uniform schema

The "countries connected to a company". Same shape for all 150 countries;
country-specific registry fields go to `extras`.

```sql
CREATE TABLE registrations (
  registration_uid     String,                          -- "<country>:<registration_number>"
  company_uid          String,                          -- FK -> company
  country              LowCardinality(String),          -- ISO2 (partition + collection boundary)
  registration_number  String,                          -- national id (Y-tunnus, SIREN, HRB…)
  registry_source      LowCardinality(String),          -- source slug that produced this row
  is_primary           UInt8,                            -- the home/incorporation registration
  entity_role          LowCardinality(String),          -- domestic | branch | representative_office
  legal_name           String,                          -- name as recorded by THIS registry
  legal_form_code      Nullable(String),
  legal_form_label     Nullable(String),
  status_code          Nullable(String),                -- raw national status
  lifecycle_status     LowCardinality(String),          -- active | ceased | intermediate | unknown
  is_active            UInt8,
  incorporation_date   Nullable(Date),
  dissolution_date     Nullable(Date),
  addr_street          Nullable(String),
  addr_post_code       Nullable(String),
  addr_city            Nullable(String),
  addr_region          Nullable(String),
  addr_municipality_code Nullable(String),
  addr_country         LowCardinality(String),
  activity_code        Nullable(String),                -- canonical NACE
  activity_scheme      LowCardinality(String),          -- e.g. 'NACE2' | 'TOL2008'
  activity_code_national Nullable(String),              -- pre-crosswalk
  vat_number           Nullable(String),
  eu_id                Nullable(String),                -- EUID / BRIS
  lei                  Nullable(String),
  primary_website      Nullable(String),                -- denormalized; full set in company_websites
  employee_count       Nullable(UInt32),                -- headcount of THIS registration (branch has its own)
  employee_count_band  LowCardinality(String),          -- size class
  employee_count_date  Nullable(Date),
  extras               JSON,                             -- country-specific registry fields
  source_run_id        String,
  source_payload_hash  Nullable(String),
  ingested_at          DateTime64(3, 'UTC'),
  updated_at           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY country
ORDER BY (country, registration_number);
```

### `financials` — tall, per (company, country, period, metric)

New metrics are new rows, never new columns — schema is identical across all
countries. Registration-anchored: a branch files its own accounts; consolidated
group accounts have `registration_uid = NULL`.

```sql
CREATE TABLE financials (
  company_uid          String,                          -- FK -> company (roll-up key)
  registration_uid     Nullable(String),                -- which registration filed it (NULL = group)
  country              LowCardinality(String),          -- filing country (partition)
  statement_id         String,                           -- one filed statement
  period_start         Nullable(Date),
  period_end           Date,
  period_type          LowCardinality(String),          -- duration | instant
  period_reference     LowCardinality(String),          -- current | prior  (XBRL comparative)
  basis                LowCardinality(String),          -- individual | consolidated
  currency             LowCardinality(String),          -- ISO 4217
  metric_code          LowCardinality(String),          -- canonical: revenue|total_assets|equity…
  value                Decimal(38, 6),
  source_metric_id     Nullable(String),                -- original concept (e.g. 'fi_met:md103/fi_MC:x673')
  registry_source      LowCardinality(String),
  mapping_version      String,
  source_run_id        String,
  ingested_at          DateTime64(3, 'UTC'),
  updated_at           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY country
ORDER BY (company_uid, statement_id, basis, period_reference, metric_code);
```

### `company_relationships` — edges, reporting-country anchored

Company-to-company only (person/officer relationships are a future separate
table). Asserted by a registry about a subject registered there; tolerates an
unresolved counterparty until more countries land.

```sql
CREATE TABLE company_relationships (
  relationship_uid     String,                           -- sha1(reporting_country, from, to|raw, type)
  reporting_country    LowCardinality(String),           -- registry that asserts it (partition)
  from_registration_uid String,                          -- subject in reporting_country (always resolved)
  from_company_uid     String,                           -- rolled up
  to_company_uid       Nullable(String),                 -- resolved head; NULL if dangling
  to_registration_uid  Nullable(String),
  to_raw_name          Nullable(String),                 -- counterparty as named by the registry
  to_raw_identifier    Nullable(String),                 -- any id the registry gave
  to_country           Nullable(String),
  relationship_type    LowCardinality(String),           -- owns|controls|parent_of|subsidiary_of|branch_of|ultimate_parent_of
  ownership_pct        Nullable(Decimal(9, 6)),
  valid_from           Nullable(Date),
  valid_to             Nullable(Date),
  resolution_status    LowCardinality(String),           -- resolved | dangling
  registry_source      LowCardinality(String),
  source_run_id        String,
  ingested_at          DateTime64(3, 'UTC'),
  updated_at           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY reporting_country
ORDER BY (reporting_country, from_registration_uid, relationship_type, to_company_uid, to_raw_name);
```

> **Owners come in two kinds.** A *corporate* owner (one company owns another) is a
> `company_relationships` edge (`owns`/`controls`). A *person* owner / UBO is a
> `company_people` row (below). Both together answer "who owns this company".

### `company_websites` — sites (multi-valued; corpscout's core output)

One company has many sites: one **entity-main** flagship plus a separate site per
registration. Sourced from registry fields *and* from corpscout's own crawl/search
discovery — `source_kind` distinguishes them.

```sql
CREATE TABLE company_websites (
  website_uid          String,                           -- sha1(scope, registration_uid|company_uid, normalized_url)
  company_uid          String,                           -- FK -> company (roll-up)
  registration_uid     Nullable(String),                 -- FK -> registrations; NULL when scope='entity_main'
  country              LowCardinality(String),           -- registration country (partition); home country for entity_main
  scope                LowCardinality(String),           -- entity_main | registration
  url                  String,                            -- as found
  normalized_url       String,                            -- canonical form
  host                 String,                            -- registrable domain / host
  is_primary           UInt8,                             -- the main site within its scope
  source_kind          LowCardinality(String),           -- registry | crawl_discovery | search
  discovery_method     LowCardinality(String),           -- registry_field | homepage_crawl | search_engine | …
  registry_source      Nullable(String),                 -- set when source_kind='registry'
  confidence           Float32,                           -- discovery confidence 0..1 (1.0 for registry)
  http_status          Nullable(UInt16),                  -- last verification
  is_live              UInt8,
  first_seen_at        DateTime64(3, 'UTC'),
  last_seen_at         DateTime64(3, 'UTC'),
  updated_at           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY country
ORDER BY (country, company_uid, registration_uid, normalized_url);
```

`scope='entity_main'` (one, `registration_uid` NULL) is the corporate flagship
(`ikea.com`); `scope='registration'` (many) are the per-registration sites
(`ikea.fi`, `ikea.de`). `company.primary_website` denormalizes the entity-main.

### `company_contacts` — phone / email / fax / social (multi-valued)

```sql
CREATE TABLE company_contacts (
  contact_uid          String,                           -- sha1(registration_uid|company_uid, contact_type, value)
  company_uid          String,                           -- FK -> company (roll-up)
  registration_uid     Nullable(String),                 -- NULL = entity-level
  country              LowCardinality(String),           -- partition
  contact_type         LowCardinality(String),           -- phone | email | fax | social
  channel              LowCardinality(String),           -- 'linkedin'|'x'|'facebook'… for social, else ''
  value                String,                            -- normalized
  is_primary           UInt8,
  source_kind          LowCardinality(String),           -- registry | crawl_discovery | search
  registry_source      Nullable(String),
  first_seen_at        DateTime64(3, 'UTC'),
  last_seen_at         DateTime64(3, 'UTC'),
  updated_at           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY country
ORDER BY (country, company_uid, contact_type, value);
```

Some registries exclude email/phone (e.g. Finland), so contacts are often
corpscout-discovered, not registry-provided — hence `source_kind`.

### `persons` — person spine (PERSONAL DATA — GDPR)

Deliberately minimal: identity (`first_name`, `last_name`) plus an **outbound**
social link, nothing demographic — no DOB, nationality, or residence held
internally. `linkedin_url`, when present, is also the strongest person identifier,
so `person_uid` keys on it, falling back to a name surrogate.

```sql
CREATE TABLE persons (
  person_uid           String,                           -- linkedin-url hash if present, else "p:<sha1(name)>"
  first_name           String,
  last_name            String,
  name_aliases         Array(String),                    -- name variants, for matching only
  linkedin_url         Nullable(String),                 -- outbound link + strong identifier
  social_links         Array(Tuple(                      -- other networks (we link out, don't profile)
                         network LowCardinality(String), -- 'linkedin'|'x'|'github'|…
                         url     String)),
  role_count           UInt16,                            -- companies this person is linked to
  field_provenance     Map(String, String),
  sources              Array(LowCardinality(String)),
  resolution_version   String,
  first_seen_at        DateTime64(3, 'UTC'),
  updated_at           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY cityHash64(person_uid) % 32                 -- erasure is by person_uid; hash buckets keep parts even
ORDER BY person_uid;
```

We hold only name + public outbound links; the raw registry name stays on
`company_people.person_raw_name`. A company's *own* LinkedIn page is a
`company_contacts` row (`contact_type='social'`), distinct from a person's.

### `company_people` — person↔registration roles (PERSONAL DATA — GDPR)

Ownership / officer roles asserted by a national registry about a registered
subject. Reporting-country anchored; tolerates an unresolved person.

```sql
CREATE TABLE company_people (
  role_uid             String,                           -- sha1(reporting_country, registration, person|raw, role)
  reporting_country    LowCardinality(String),           -- registry asserting it (partition)
  registration_uid     String,                           -- the registered subject
  company_uid          String,                           -- rolled up
  person_uid           Nullable(String),                 -- resolved person; NULL if dangling
  person_raw_name      Nullable(String),                 -- person as named by the registry
  role                 LowCardinality(String),           -- beneficial_owner|shareholder|director|board_member|ceo|signatory
  is_beneficial_owner  UInt8,
  ownership_pct        Nullable(Decimal(9, 6)),
  voting_pct           Nullable(Decimal(9, 6)),
  valid_from           Nullable(Date),
  valid_to             Nullable(Date),
  resolution_status    LowCardinality(String),           -- resolved | dangling
  registry_source      LowCardinality(String),
  source_run_id        String,
  ingested_at          DateTime64(3, 'UTC'),
  updated_at           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY reporting_country
ORDER BY (reporting_country, registration_uid, role, person_uid, person_raw_name);
```

## Personal data (GDPR)

`persons` and `company_people` (and personal emails in `company_contacts`) hold
personal data and are regulated. Before populating them, settle:

- **Lawful basis** for processing owner/officer data (usually legitimate interest
  for public-registry data — document it per source; some registries restrict reuse).
- **Minimisation** — already baked in: only name + public outbound social link, no
  demographics. Keep it that way; resist adding DOB / address / nationality later.
- **Retention** — a TTL/erasure policy; `ReplacingMergeTree` + delete-by-`person_uid`
  supports right-to-erasure. Partition `persons` by `person_uid` hash, not by a
  personal attribute, so erasure is a keyed delete.
- **Residency** — child tables already partition by country; keep personal rows
  inside the partition model so residency constraints are enforceable.

These are decisions, not code — resolve them before the entity zone writes persons.

---

## How conformance populates it

```
SOURCE zone   raw → structured Parquet (per source)
COUNTRY zone  merge a country's sources → that country's:
                registrations[country]  + financials[country]  + relationships[country]
              (source-merged within the country; company_uid not yet final)
ENTITY zone   cross-country resolution:
                - fuse registrations across countries → company rows
                - assign final company_uid back onto registrations/financials/relationships
                - resolve dangling relationship heads (to_company_uid)
SERVING       load the four Parquet datasets → ClickHouse (dumb, source-agnostic)
```

- COUNTRY zone owns per-country partitions and is what a country's pipeline rebuilds.
- ENTITY zone is the only cross-country step; trivial for single-registration
  companies, real work only for LEI/EUID-linkable or fuzzy-matchable multinationals.

## Query patterns (company-primary)

```sql
-- Everything about one company, across all countries:
SELECT * FROM company            FINAL WHERE company_uid = :uid;
SELECT * FROM registrations      FINAL WHERE company_uid = :uid;        -- all its countries
SELECT * FROM financials         FINAL WHERE company_uid = :uid;        -- rolled up from filings
SELECT * FROM company_relationships FINAL WHERE from_company_uid = :uid; -- corporate owners/subsidiaries
SELECT * FROM company_people      FINAL WHERE company_uid = :uid;        -- person owners / officers
SELECT * FROM company_websites    FINAL WHERE company_uid = :uid;        -- all its sites
SELECT * FROM company_websites    FINAL WHERE company_uid = :uid AND scope = 'entity_main'; -- flagship
SELECT * FROM company_contacts    FINAL WHERE company_uid = :uid;        -- phone/email/social

-- A country's whole registered population (collection view):
SELECT * FROM registrations FINAL WHERE country = 'FI';
```

## Open decisions (resolve as data lands)

- **`company` partition:** `home_country` (coarse, readable, may churn) vs
  `cityHash64(company_uid)` buckets (stable, opaque). Default `home_country`.
- **`legal_form_code` canonical set:** EU ELF code list is the strongest candidate;
  confirm coverage for non-EU countries before locking.
- **`metric_code` vocabulary:** seed from the Finland XBRL mapping (revenue,
  total_assets, equity, liabilities, …); grow as financial sources land. Versioned
  by `mapping_version`.
- **Person resolution + GDPR:** key `person_uid` on `linkedin_url` when present
  (near-unique), else a name surrogate — name-only matching is fuzzy. We hold only
  name + outbound links; still settle lawful basis / retention before populating
  `persons` (see the GDPR section).
- **Website verification cadence:** corpscout-discovered sites carry `http_status`/
  `is_live`/`confidence`; decide how often they're re-verified and the confidence
  threshold for promoting a discovered site to `is_primary`/`entity_main`.
- **Employee counts:** modelled as a per-registration snapshot (`employee_count` +
  band + date). If a source gives a *time series*, add a tall non-monetary measures
  table rather than widening `registrations`.
- **Redomiciliation:** rare; keep `company_uid` stable and add a `succeeded_by`
  relationship rather than re-keying.
```
