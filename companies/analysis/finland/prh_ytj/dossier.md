# Finland PRH YTJ — Source Dossier

> Reference example. This source is already **live** in `corpscout/dagster_v2`
> (`sources/finland/prh_ytj`), so this dossier is partly retrospective: §1–§8 are
> the discovery/mapping a new source would do first; §9–§13 document the as-built
> pipeline. It is the `snapshot` archetype reference. Companion sources:
> financials live in a **separate** dossier (`finland/prh_xbrl`).

| | |
|---|---|
| **Source id** | `finland_prhytj` |
| **Country** | `finland` |
| **Date** | 2026-06-14 |
| **Author** | data-eng |
| **Status** | `live` |
| **Archetype** | `snapshot` (run-keyed full dump) |

## 1. Identity & access

| Field | Value |
|---|---|
| Publisher | Finnish Patent and Registration Office (PRH) + Tax Administration (YTJ / Business Information System) |
| Endpoint | `https://avoindata.prh.fi/opendata-ytj-api/v3/companies` |
| Auth | none |
| Cost | free |
| Format(s) | JSON (nested records) |
| License | CC-BY-4.0 — attribution required, redistribution ok |
| Freshness | daily (`lastModified` per record) |
| Entity coverage | registered companies (~819k). **Excludes** sole traders (*toiminimi*), officers, beneficial owners, financials, email, phone |

Expands `finland/source_inventory.md` row "PRH Open Data — YTJ API v3".

## 2. Acquisition

| Field | Value |
|---|---|
| Fetch mechanism | paginated API |
| Pagination | `page=N`, fixed 100 records/page; `maxResults` ignored. Full backfill ≈ `page=1…8191` |
| Volume | `totalResults=true` → **819,096** companies |
| Sample | worked example **Dynava Oy `0100130-4`** (saved raw record → `data_model/country_company_profile.example.json`) |
| Rate limits | none documented; human portal pages 403 bots — use the API, not scraping |
| Incremental signal | `lastModified` (daily) exists, **but the as-built pulls a full snapshot each run** (no delta yet — see §12) |

## 3. File / entity inventory

One nested JSON record per company; the normalizer explodes its arrays into 14
ClickHouse tables (all keyed by `business_id`).

| Logical entity | Grain (1 row =) | Source array | ClickHouse table |
|---|---|---|---|
| identifiers | one identifier | `businessId`, `euId` | `fi_prhytj_identifiers` |
| status | one company | `tradeRegisterStatus`, `status`, dates | `fi_prhytj_statuses` |
| names | one name version | `names[]` | `fi_prhytj_names` |
| business lines | one activity | `mainBusinessLine` | `fi_prhytj_business_lines` (+ `_descriptions`) |
| websites | one URL | `website` | `fi_prhytj_websites` |
| company forms | one legal-form version | `companyForms[]` | `fi_prhytj_company_forms` (+ `_descriptions`) |
| company situations | one situation | `companySituations[]` | `fi_prhytj_company_situations` (+ `_descriptions`) |
| registered entries | one register entry | `registeredEntries[]` | `fi_prhytj_registered_entries` (+ `_descriptions`) |
| addresses | one address | `addresses[]` | `fi_prhytj_addresses` (+ `_post_offices`) |

Reference code lists (`REK, REK_KDI, VIRANOM, TLAJI, YRMU, STATUS3, KIELI`) are a
separate pull → `fi_prhytj_code_lists`.

## 4. Schema profile

Run `profile_source.py` on the raw `source.ndjson` snapshot once landed:

```bash
uv run python ../_templates/profile_source.py 'samples/source.ndjson' --out .
```

Key observations (from sampled records / mapping work — see §6 source paths):

| Field | Null % | Distinct | Role | Notes |
|---|---:|---:|---|---|
| `businessId.value` | 0 | =rows | **primary key** | Y-tunnus |
| `tradeRegisterStatus` | 0 | ~3 | core (liveness) | 1=active, 4=ceased, 3=intermediate |
| `status` | 0 | 1 | **drop / keep raw** | constant `'2'` — NOT a liveness flag (see §12) |
| `euId.value` | ~82 | =present | extras→core | BRIS EUID, ~18/100 present |
| `website.url` | ~94 | — | extras→core | ~6/100 present; only contact field |
| `mainBusinessLine` | low | — | core | TOL/NACE code + `typeCodeSet` |
| `companySituations[]` | ~100 (empty) | — | extras (watch) | sub-schema unconfirmed |

## 5. Keys & joins (within this source)

| Purpose | Key |
|---|---|
| Company identity (PK) | `businessId.value` (Y-tunnus) |
| Names/forms/addresses ↔ company | `business_id` (all tables carry it) |
| Geography | `addresses[].postOffices[].municipalityCode` (Statistics Finland) |
| Cross-source identity (later entity resolution) | `eu_id` (BRIS EUID `FIFPRO.<business_id>`), derived `vat_id` |

> Single authoritative source → no within-source value conflicts. Cross-source
> entity resolution is a separate downstream layer; only the candidate keys are
> recorded here.

## 6. Source-to-target mapping — companies

Target = `finland/data_model/country_company_profile.schema.json`. Full detail in
`finland/data_model/country_company_profile_mapping.md`; the load-bearing rows:

| Target path | Source path | Tier | Transform | Notes |
|---|---|---|---|---|
| `registration.business_id` | `businessId.value` | core | direct | PK |
| `registration.eu_id` | `euId.value` | core | direct | sparse (~18%) |
| `registration.vat_id` | (derived) | core | `FI`+digits, dash removed | confirm liability via tax registers |
| `legal_identity.legal_name` | `names[?type=1 && endDate=null].name` | core | pick current (latest reg date) | |
| `legal_identity.name_history` | `names[]` | extras | verbatim array | full history |
| `legal_identity.legal_form` | `companyForms[?endDate=null]` | core | pick current | label = `descriptions[langCode=3]` |
| `status.is_active` | `tradeRegisterStatus` + `endDate` | core | `==1 && endDate==null` | **derived — never use `status`** |
| `status.incorporation_date` | `registrationDate` | core | direct | |
| `status.dissolution_date` | `endDate` | core | direct | null = active |
| `activity.*` | `mainBusinessLine` | core | direct | TOL/NACE; `code_set=typeCodeSet` |
| `addresses[]` | `addresses[]` | core | reshape | type 1=visiting, 2=postal |
| `tax_registrations.{vat,employer,prepayment}` | `registeredEntries[?register∈{6,5,7} && endDate=null]` | core | derive flags | "registered" = open entry exists |
| `online_presence.website` | `website.url` | core | normalize URL | sparse (~6%) |
| `register_entries[]` | `registeredEntries[]` | extras | verbatim | raw history preserved |
| `status.raw_status_code` | `status` | extras | verbatim | constant `'2'`; kept for audit only |

## 7. Source-to-target mapping — financials (tall)

**N/A for this source.** The YTJ companies endpoint carries no financial figures.
Financials are a separate Finland source — see the `finland/prh_xbrl` dossier
(PRH digital financial statement API → tall `financials` rows). Do not invent
financial fields here.

## 8. Coverage & promotion notes

| Field | Fill rate | Decision | Rationale |
|---|---|---|---|
| `business_id`, `tradeRegisterStatus`, `mainBusinessLine` | ~100% | core | universal, product-central |
| `eu_id`, `vat_id` | ~18% / derived | core | sparse but **identity keys** (centrality beats prevalence) |
| `website` | ~6% | core | sparse but the only contact signal; product needs it |
| `name_history`, `register_entries` | full arrays | extras | audit history, not queried hot |
| `companySituations` | ~0% (empty in sample) | extras (watch) | promote once a distressed entity confirms the sub-schema |

## 9. Ingestion & automation decision

| Field | Value | Rationale |
|---|---|---|
| Archetype | `snapshot` | full dump; no natural time-partition |
| Partitioning | none | run-keyed (`runs/<run_id>/source.ndjson`) |
| Cadence | weekly, Mon 03:00 (`0 3 * * 1`) | registry changes slowly; daily not needed yet |
| Trigger | `ScheduleDefinition`, default **STOPPED** | cron enters at raw only |
| Concurrency keys | `finland_prhytj` (raw), `finland_prhytj:clickhouse` (transforms) | isolate API vs CH pressure |
| New archetype needed? | no | this *is* the snapshot reference |

## 10. Dagster implementation (as-built)

```text
PRH YTJ API
  -> raw_snapshot            (full NDJSON + code lists → RustFS, manifest)   [bespoke]
  -> normalized_tables       (parse NDJSON → 14 fi_prhytj_* CH tables)       [bespoke]
  -> code_lists              (TSV code lists → CH)                           [bespoke]
  -> industry_nace_mappings  (TOL→NACE crosswalk)                            [bespoke]
  -> company_explorer_cache  (serving cache; feeds prh_xbrl eligibility)     [bespoke]
```

Cron triggers `raw_snapshot` only; `normalized/code_lists/mapping/cache` cascade
via `AutomationCondition.eager()`. Jobs: `pull` (raw), `pipeline` (all),
`transform_latest` (re-derive from latest snapshot without re-pull).

> **Deviation from the target three-tier pattern:** this source was ported from
> v1 and normalizes JSON **straight into 14 ClickHouse tables** — it predates the
> "structured → Parquet → thin canonical CH" convention. It works and is stable,
> so it stays as-is, but new sources should land structured **Parquet** and push
> only canonical `companies`/`financials`, not a per-source table family. Treat
> prh_ytj's 14-table normalized layer as legacy shape, not the model to copy.

## 11. Data-quality checks (asset_checks)

| Check | Asserts |
|---|---|
| `industry_nace_mappings_rows_present` | NACE crosswalk produced rows |
| `company_explorer_cache_matches_view` | serving cache row count matches its source view |

Gap worth adding: a `business_id` uniqueness check on the statuses table.

## 12. Open questions

- **`status` pitfall** — constant `'2'` for active *and* ceased; liveness must come
  from `tradeRegisterStatus`. The single biggest interpretation risk; keep it loud.
- **No incremental crawl yet** — `lastModified` supports delta ingestion, but the
  pipeline re-pulls the full ~819k snapshot weekly. Fine for now; revisit if cost/time grows.
- `companySituations` sub-schema unconfirmed (empty in sample) — validate on a
  known bankrupt/liquidating entity before relying on `special_situations`.
- Numeric `register`/`authority`/`source` code meanings inferred from sample
  descriptions — confirm against official PRH code lists before hard-coding.

## 13. Recommendation

**Live; keep as the `snapshot` archetype reference.** Mapping is high-confidence
(single authoritative source, no conflicts). Two follow-ups, neither blocking:
(1) add the `business_id` uniqueness check, (2) consider `lastModified` delta
ingestion if the weekly full pull becomes expensive. New snapshot sources should
copy this source's *archetype and automation shape* but adopt the Parquet-canonical
tier (§10 deviation) rather than its 14-table normalized layer.
