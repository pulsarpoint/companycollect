# Canonical Per-Source Contact & Domain Tables — Standard

**Date:** 2026-07-04
**Status:** Standard spec (phase 1 of a multi-phase program), pending user review
**Scope of THIS document:** the two canonical table shapes, vocabularies, and
the per-source conversion inventory. Each conversion phase gets its own plan
executed against this standard.

## Problem

Company contact/domain data lives in five incompatible shapes:

| Shape | Tables | Company key | Notes |
|---|---|---|---|
| website-era | `fi_websites`, `no_websites` | `business_id` / `org_number` | URL columns, no contacts |
| wikidata variant | `wikidata_company_websites` | `wikidata_id` | string confidence, kinds |
| Estonia pair | `ee_company_contacts` + `ee_company_domains` | `reg_code` | closest to the target model |
| Brazil pair | `br_company_contact_info` + `br_websites` | `cnpj`/`cnpj_basico` | `br_websites` misnamed: email-derived domains, URL columns always `''` |
| name-extraction | `cz_company_contacts`, `lv_company_contacts` | `ico` / `regcode` | facts and derived domain fused in one row |

The domain graph (`domains/assets.py`) unions these through five hand-written
SQL adapters with per-source special-casing. Every new source adds another
shape and another adapter. Decision (user): standardize NOW, before more
sources land ("better now than when we add 5 more sources").

## Model: contacts are facts, domains are conclusions

Every source gets the same two tables:

- **`<src>_company_contacts`** — contact facts as found in the register,
  one row per (company, contact). No inference. A website URL, an email, a
  phone number, and a domain-looking company name are all facts.
- **`<src>_company_domains`** — derived company↔domain associations with
  provenance and confidence. This is the ONLY thing the domain graph reads,
  through one uniform query per source.

`domain_in_name` and `email` fact rows are unvalidated observations; consumers
wanting company↔domain associations must join `<src>_company_domains`, never
derive them from the contacts table.

A source with no contact data still gets both tables (empty) — uniformity is
the point; consumers never special-case.

## Canonical DDL

### `<src>_company_contacts`

```sql
CREATE TABLE IF NOT EXISTS corpscout.<src>_company_contacts
(
    country_iso2      LowCardinality(String),
    source_slug       LowCardinality(String),
    source_run_id     String,          -- '' when not applicable
    source_record_id  String,
    registry_id       String,          -- STANDARDIZED company key (see below)
    contact_type      LowCardinality(String),  -- canonical vocabulary
    contact_type_raw  LowCardinality(String),  -- source's original label, '' if same
    contact_value     String,          -- normalized value (full phone incl. area code)
    source_field      LowCardinality(String),  -- register field of origin
    is_current        UInt8,
    valid_to          Nullable(Date),
    source_url        String,          -- '' when not applicable
    resolved_at       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (registry_id, contact_type, contact_value);
```

`contact_type` vocabulary (closed; extend only via this spec):
`email` | `phone` | `mobile` | `fax` | `website` | `domain_in_name` | `other`.
`source_field` examples: `legal_name`, `name`, `correio_eletronico`,
`sidevahendid`, `website`, `website_url`.

### `<src>_company_domains`

```sql
CREATE TABLE IF NOT EXISTS corpscout.<src>_company_domains
(
    country_iso2           LowCardinality(String),
    source_slug            LowCardinality(String),
    source_run_id          String,
    source_record_id       String,
    registry_id            String,
    domain                 String,     -- registrable domain, lowercase unicode
    domain_source          LowCardinality(String),  -- website | email | name_embedded
    validation_method      LowCardinality(String),  -- '' | commoncrawl | dns
    confidence             Float32,
    website_url            String,     -- '' unless domain_source = website
    website_normalized_url String,
    website_host           String,
    is_current             UInt8,
    is_primary             UInt8,      -- at most one per registry_id (election rule below)
    resolved_at            DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (registry_id, domain);
```

Confidence defaults (spec-owned constants in the shared
`contact_extraction.py`): explicit website field **1.0**; unique-email
inference **0.9**; name-embedded + CommonCrawl **0.95**; name-embedded + DNS
**0.70**. `is_primary` election (from Estonia, now standard): prefer
website-sourced, then current, then highest confidence, then
shortest/alphabetical domain — exactly one winner per `registry_id` that has
any row.

## Key decisions

1. **`registry_id` replaces per-country id column names** (`ico`,
   `regcode`, `reg_code`, `business_id`, `org_number`, `cnpj_basico`,
   `wikidata_id`). Cross-source UNION is the point of the standard; the
   source's id semantics are recorded once per source (a
   `registry_id_type` constant in each source's module, carried into the
   graph's `company_id_type` column). For Brazil, `registry_id =
   cnpj_basico` (the company), `source_record_id` keeps the full
   establishment `cnpj`.
2. **Websites are facts AND conclusions**: a register website field yields
   a `contact_type='website'` fact row and a `domain_source='website'`
   domain row. The duplication is deliberate — the contacts table preserves
   what the register said; the domains table is the joinable distillation.
3. **cz/lv shape splits**: their current fused rows (fact + validated
   domain + confidence in one) become a fact row (`domain_in_name` /
   `email`, `source_field='name'`/`'legal_name'`) plus a domains row
   (`domain_source='name_embedded'`, `validation_method`, confidence).
4. **Brazil's phone data survives**: `br_company_contact_info` phone rows
   (with `contact_area_code`) become canonical `phone` facts with the area
   code folded into `contact_value`.
5. **Wikidata joins the standard** (`registry_id = wikidata_id`,
   `country_iso2` filled from `wikidata_companies` at build time, '' when
   unknown). Its extra columns (`website_kind`, string confidence,
   `validation_status`) map: `is_primary_candidate` feeds the election;
   string confidence maps high=1.0/medium=0.7/low=0.4.
6. **The shared `contact_extraction.py` owns the vocabularies** —
   contact_type / domain_source / validation_method constants, confidence
   defaults, `EMAIL_PROVIDER_DENYLIST` + `EMAIL_DOMAIN_MAX_COMPANIES`
   (today duplicated between Estonia and Brazil; diff-then-union the two
   denylists when moving).
7. **Domain graph reads only `<src>_company_domains`**: the five
   hand-written union branches collapse into one templated SELECT over a
   config list of `(table, registry_id_type)` pairs. `corpscout.domains`
   aggregation is unchanged.

## Addendum (2026-07-05, Phase D): decision 5 correction

`wikidata_company_websites.confidence` is a hardcoded literal `'wikidata'`
on every row (verified against the builder SQL and live data) — the
high/medium/low mapping described in decision 5 has nothing to map from.
Wikidata website rows take the standard explicit-website confidence **1.0**,
like every other register website field. `website_kind`/`validation_status`
are likewise constants and carry no signal; they do not survive into the
canonical shape. `is_primary_candidate` is constantly 1, so the canonical
`is_primary` comes from the standard election (one winner per wikidata_id).

## Per-source conversion inventory

| Source | Contacts | Domains | Notes |
|---|---|---|---|
| czech_ares | reshape `cz_company_contacts` | NEW `cz_company_domains` | split fused rows (decision 3) |
| latvia_ur | reshape `lv_company_contacts` | NEW `lv_company_domains` | same |
| estonia_ar | reshape `ee_company_contacts` (drop inline domain cols, `reg_code`→`registry_id`, map contact_type vocab) | reshape `ee_company_domains` (+confidence/validation_method) | closest to target |
| brazil_rfb | reshape `br_company_contact_info` | `br_websites` → `br_company_domains` (drop the lie: no fake URL columns filled with '' — they stay '' but honestly, `domain_source='email'`) | + denylist consolidation (decision 6) |
| norway_brreg | NEW `no_company_contacts` (website facts) | `no_websites` → `no_company_domains` | register has no phone/email (verified) |
| finland_ytj | NEW `fi_company_contacts` (website facts) | `fi_websites` → `fi_company_domains` | dbt model adjusts |
| wikidata | NEW (website facts) | `wikidata_company_websites` → `wikidata_company_domains` | decision 5 |
| sweden_company (future) | implements the pair from day one | | its design doc's deferred `contact_candidates` lands directly in this shape |

## Migration strategy (program, not one plan)

Additive and reversible at every step:

- **Phase A — cz/lv + shared vocab** (smallest, newest code): shared
  constants land in `contact_extraction.py`; cz/lv get canonical tables and
  writers. Validates the standard on the code we just built.
- **Phase B — Estonia** (rename-heavy, logic-light).
- **Phase C — Brazil** (+ denylist consolidation; DuckDB engine untouched —
  only output shapes change).
- **Phase D — Norway/Finland/wikidata** (mechanical website-fact sources).
- **Phase E — domain-graph switch**: `company_website_domains` build reads
  the uniform tables; verify `corpscout.domains` output is a superset of
  today's (row-count + spot-check parity gate); then deprecate
  `fi_websites`/`no_websites`/`wikidata_company_websites`/`br_websites`/
  old-shape tables (drop migrations one release later) — NOTE (Phase D): the
  no/fi/wikidata canonical pairs DERIVE from those websites tables; Phase E
  must first either demote them to internal stages or move the derivations
  upstream into each source's native pipeline.

Old tables keep working until Phase E — the graph switches last, so nothing
downstream breaks mid-program. Each phase is its own plan+review cycle
against this spec.

## Prerequisite carried in

The `dr.ing`/`_TITLE_LABELS` guard (final-review condition from the
lv-contacts project) must land before Phase E exposes name-embedded domains
to the graph — schedule it in Phase A while touching the shared module.

## Testing (per phase, standard-owned)

- A shared schema-conformance test helper: assert any `<src>_company_contacts`
  / `<src>_company_domains` migration matches the canonical DDL modulo table
  name (column names, types, engine, ORDER BY) — new sources can't drift.
- Vocabulary tests: contact_type/domain_source values written by each
  source ⊆ the closed vocabularies.
- Phase E parity gate: for each source, every (registry_id, domain) pair in
  the old graph input exists in the new one (allow strict supersets).

## Out of scope

- The CommonCrawl domain-signal cluster (`commoncrawl_*` tables) — domain-
  keyed enrichment, not company-registry contact data.
- `gleif_lei_issuers.website` (plain reference column).
- Changing extraction/inference logic itself (Phase C moves Brazil's rules,
  it doesn't change them; cz/lv pipelines only re-shape their output).
- The domain graph's aggregation semantics (`corpscout.domains` columns).
