# Data-source ingestion guidelines

The standard for adding a country/open-data source to corpscout. Goal: at 100+ sources,
every pipeline looks the same, so any one can be understood, debugged, or handed off without
relearning. Deviations are allowed but **must be written down** in the source's design doc
(see §10). The day-to-day gotchas live in `dagster_v3/CLAUDE.md`; this is the shape and the
decision rules.

## 0. The golden path (one sentence)
**dlt-bounded bulk download → DuckDB staging (C++ reader, raw kept as text) → transform *only if
needed* (direct DuckDB→ClickHouse copy preferred; else set-based SQL; dbt only when earned) →
migration-owned, normalized ClickHouse tables (atomic full-refresh), with `*_usd` currency columns
and `_en` translation columns added as standard cross-cutting steps.**

Mirror the nearest existing module: `estonia_ar`/`latvia_ur` (bulk CSV + financials + EUR/USD),
`finland_ytj`+`finland_resolved` (register + dbt resolve), `norway_brreg` (registry + per-company
fetch + LLM translation).

## 1. Pipeline shape (always)
```
ingest (dlt)  →  <source>_source.duckdb (staging)  →  transform (SQL/dbt)  →  corpscout.<table> (ClickHouse)
```
- **One DuckDB file per source**, single-writer. Put `pool="<source>_duckdb"` on *every* asset that
  **opens** it — writers AND read-only exporters (a DuckDB writer excludes readers across
  processes, so an unpooled read-only step still collides with a concurrent write step's file
  lock). One shared pool across ALL of a source's chains (refresh, backfill, export) is what makes
  them safe to launch in any order and in parallel: Dagster interleaves the steps instead of
  letting two runs race on the file (see the sweden_financial 2026-07-20 incident). The file
  **stem must differ from the dlt dataset name**.
- **Per-source module** under `defs/<source>/` (own DuckDB, own tables, own pool). This is the unit
  of isolation — do **not** fold multiple countries into one partitioned asset.
- ClickHouse is the only `corpscout` database; the **migration owns the DDL**, code asserts the
  table exists then atomically replaces it (stage + `EXCHANGE TABLES`).

## 2. Choosing the ingest mode (decision tree)
Pick the **first** that applies. Record the choice + why in the design doc.
1. **Bulk file download available (CSV / Parquet / JSON-Lines / NDJSON)** → download the full
   snapshot, **non-partitioned full-refresh**. *Preferred — simplest, atomic, no backfill.*
   Format preference: **Parquet > CSV > JSON-Lines > nested JSON**.
2. **Only a paginated/record API** → **partitioned incremental** (see §4). This is the *only*
   case where we partition.
3. **API that returns the whole dataset in one (or a few) request(s)** → non-partitioned
   full-refresh, like `exchange_rates_v2` (don't partition just because it's an API).
- **Financial filings in XBRL/iXBRL** are a recurring special case with their own hierarchy (often
  pre-extracted facts exist → no parsing needed) — see **§5b**.

## 3. Loading (dlt + DuckDB's C++ reader — never row-by-row Python)
- **dlt owns the ingest boundary** (HTTP session with retry/backoff via
  `dlt.sources.helpers.requests`; `write_disposition="replace"` for full snapshots).
- **File → DuckDB with the multithreaded C++ reader**, not a Python-dict-per-row resource:
  - CSV: `read_csv(path, all_varchar=true, quote='"', escape='"')` (or, behind a `@dlt_assets`
    boundary, `dlt.sources.filesystem.read_csv_duckdb(use_pyarrow=True)` — **not** the pandas
    `read_csv`; `pandas` is not installed).
  - JSON: `read_json`/`read_ndjson`. For **nested JSON**, land the raw object as a `JSON`/`VARCHAR`
    column and unnest later with `json_extract`/`unnest` in SQL — do not flatten in Python.
  - Parquet: `read_parquet` (already Arrow — fastest).
  - A narrow Python-dict-per-row dlt resource is acceptable **only** for small registers where you
    compute per-row fields (e.g. static-map translation) — see `latvia_ur/resources.py`. For wide
    bulk files it's the slow path; don't.
- **Keep raw values as text in staging**; do all casts in the transform. Keep a `raw_*` JSON column
  + `source_payload_hash` in DuckDB for provenance (excluded from ClickHouse — see §6/CLAUDE.md).
- **Python-produced rows → typed Arrow or Polars relations**, registered with DuckDB and loaded
  through `INSERT … SELECT`. Flush parsed or wide records in batches of at most 50,000 rows.
  Small source catalogs and dimension metadata may use one typed relation. If a parser is
  unbounded, spool those batches to temporary Parquet files or a disk-backed staging table.
- **Never use DuckDB `executemany` in production loaders.** Full replacements must insert from a
  registered relation or native file scan inside an atomic staging/replacement transaction.
  Per-record changes must become one staged relation plus set-based `UPDATE … FROM`.
  `tests/test_duckdb_bulk_loading_contract.py` enforces this rule. The five existing TED
  procurement calls are an explicit temporary exception owned by the separate TED streaming
  rewrite; the guard fixes their exact package/file counts so the exception cannot spread.
- **Split big multi-file downloads into one raw-load asset per file** (checkpoints); resolve
  rotating filenames from the source's index at runtime, don't hardcode (see
  `estonia_ar/financials.py:resolve_financial_url`).
- **Refuse to replace on empty input** (`raise ValueError` on zero rows) so a bad fetch can't blank
  a populated table.

## 4. API-only sources → partition by the API's natural window
When there is no bulk file and you must page an API, partitioning earns its keep (each run fetches
one window instead of re-pulling everything):
- **Partition by the API's windowing key** — registration/report date, id range, page, or region.
  Use `MonthlyPartitionsDefinition` (not daily — daily-over-years floods the Postgres event log) with
  `end_offset=1` so the in-progress current window stays materializable.
- **`BackfillPolicy.multi_run(max_partitions_per_run=1)` + the per-source pool** → a UI/daemon
  backfill runs windows throttled, one small run each (no event-log connection spike). **Never**
  `single_run()`.
- **Incremental on schedule**: re-materialize only recent window(s); backfill old windows once from
  the UI. Use dlt incremental cursors for append-style feeds.
- **Cancel in-flight backfills before changing `partitions_def`** (a stale partition run leaks its
  pool slot — see CLAUDE.md Troubleshooting). Reference impl: `finland_xbrl` (registration-date window).
- Rule of thumb: **bulk file ⇒ non-partitioned full-refresh; per-window API ⇒ partitioned incremental.**

## 5. Transform — prefer **none → set-based SQL → dbt**, in that order
Pick the lightest that produces the export shape:
1. **No transform needed → direct DuckDB→ClickHouse copy.** If the dlt/staging table is already the
   export shape (typed, normalized columns), do **not** add a transform — export it straight with
   `export_duckdb_table_to_clickhouse` over the `*_EXPORT_COLUMNS` subset. Cheapest and most uniform;
   this is how a register/spine usually lands (e.g. `estonia_ar` entities → `ee_companies`). *Preferred.*
2. **Simple transform → set-based DuckDB SQL** in a `<source>/financials.py` module
   (`CREATE OR REPLACE TABLE … AS SELECT …`). One pivot/normalize is one statement — fast, no project
   overhead. EAV/long inputs → conditional-aggregate pivot (`max(value) filter (where element='…')`),
   see `estonia_ar`/`finland_xbrl`.
3. **Complex transform → dbt**, *only* when it's a genuine multi-model DAG, needs incremental/SCD2,
   or wants dbt tests/exposures (e.g. `finland_resolved`, `exchange_rates_v2`). dbt brings a
   per-source project + manifest (and the manifest-lock failure mode) — don't pay that for a copy or
   a single pivot.
- **Never transform with Python row loops** (the 40-min `latvia_ur` metrics regression). If you're
  iterating rows in Python, it belongs in SQL.

## 5b. XBRL / financial-document sources (a recurring special case)
Many registries publish financials as **XBRL** (XML business reporting; increasingly **inline XBRL /
iXBRL** under the EU ESEF mandate). Don't reach for a parser reflexively — follow the hierarchy, and
decouple the stages.

**Ingest hierarchy (best → worst):**
1. **Registry already publishes the extracted facts (CSV/JSON) → skip XBRL entirely** (bulk-file
   golden path). *Always check first.* Estonia is this — `elemendi_nimetus`/`vaartus` are already
   concept+value pairs, so we just pivot, no parser.
2. **Only raw XBRL/iXBRL → use an explicit parser stage** that is tested against real filings from
   the source. Run it as a **partitioned** extraction stage (§4).
3. **A lighter `lxml`/`py-xbrl` instance parser** → only for *plain* (non-inline) XBRL where the
   source files and expected concepts are verified. You give up full taxonomy validation.

**Decouple three stages — this is what scales XBRL across many countries:**
- **Extraction** (the parser's only job): XBRL → a generic flat fact table
  `(concept_qname, context, period, unit, value, dimensions)`. Swappable.
- **Mapping**: `concept_qname → canonical metric` as a **per-taxonomy lookup table (data, not code)**,
  applied in SQL/dbt (see `finland_xbrl`'s `xbrl_metric_map`). **A new XBRL country is mostly a new
  mapping table** — national GAAP / IFRS / ESEF taxonomies all flow through the same shape.
- **Normalize / USD**: downstream SQL/dbt per the standard (§5–§7). **Translation** is a separate
  out-of-graph step (§8) — not dbt/SQL.

**Make XBRL extraction a shared component**, not per-country code: a reusable `xbrl_common` extractor
(→ flat fact table) parameterized by `(source format, concept→metric map)`. A new XBRL country then =
configure it + supply a mapping table, rather than re-implementing a parser.

**Scaling XBRL parsing**:
- **Partition by registration window** (parse incrementally, never the whole corpus).
- **Parse once, cache the flat fact output** (S3/DuckDB) — never re-parse a filing.
- **Cache the taxonomy DTS locally** — per-doc cost is dominated by resolving/loading taxonomy files
  over the network; a warm cache is the big win.
- **Parallelize** — per-document parsing is embarrassingly parallel → N workers. (Textbook fit for the
  §4 partitioned-API pattern.)

## 6. ClickHouse export (normalized, migration-owned)
- DDL lives in `clickhouse/migrations/` (`.up.sql` + `.down.sql`, registered in
  `EXPECTED_MIGRATIONS`). Pin column order with a contract test that greps the migration.
- Export via `assert_clickhouse_tables_exist` + `export_duckdb_table_to_clickhouse`
  (`truncate=True` stage + `EXCHANGE`) or `replace_duckdb_tables_in_clickhouse`.
- **Don't ship `raw_*`/`source_payload_hash`** — keep them in DuckDB staging; export the
  `*_EXPORT_COLUMNS` subset (`CLICKHOUSE_EXCLUDED_COLUMNS`).
- **Non-nullable `String`/`LowCardinality(String)` columns must get `''`, never `NULL`** (the native
  driver `.encode()`s every value). `ORDER BY` keys must be non-nullable.
- Tables are **normalized**: one row per entity/statement, typed columns, English + USD companions
  (below). Don't dump wide raw payloads.

## 7. Cross-cutting standard A — currency (always, for any monetary amount)
- Store the **native value faithfully as `<metric>_amount_original`**; apply any source scaling
  (e.g. `rounded_to_nearest`) **before** FX.
- Add **`<metric>_amount_usd`** via the shared `ExchangeRateClient` (EUR-based, keyed on the report
  `period_end_date`) as a **separate, re-runnable step** — never inline with the native build, so
  metrics can land before rates exist. Fill `fx_rate_to_usd` / `fx_rate_date` / `fx_source`.
- **The metrics DDL always carries the `_original` + `_usd` pair per metric + the three fx columns.**
- **EVERY monetary figure the source publishes gets the pair — not just the one the view reads.**
  Storing four native amounts and converting one is the *same* loss as storing one, moved a layer
  down: the unconverted three can only ever answer a single-country question. Drive the conversion
  off a `VALUE_COLUMNS` tuple of `(native, usd)` pairs rather than naming a column inline, so
  "convert all of them" is structural instead of something to remember. `fi_hilma_notices` (four
  value fields, each with `_original` + `_usd`) and `br_pncp_contracts` are the reference shapes.
  One rate covers every figure on the same record, so the three fx columns stay **singular** rather
  than being repeated per figure.
- **Nothing is merged, coalesced, filtered or dropped on the way in.** No `coalesce(a, b)` picking a
  "best" amount, no excluding rows whose amounts are the wrong kind (Brazil's `receita` revenue
  contracts), no collapsing two grains into one. Which figure a reader is *shown*, and what a total
  sums, is decided in the **view and the UI**, where the choice can be labelled and where
  `value_source_field` can name the register field behind the number. A pipeline that picks silently
  makes the number uncheckable.
- **A figure the record omits stays NULL, never `0`.** Half of PNCP's contracts omit
  `valorAcumulado`; a zero there is indistinguishable from a real zero.
- **Batch the rate requests** (`_load_rates`, ≤50/call). Currencies absent from the ECB set (e.g.
  legacy LVL) keep native-only (`_usd` NULL) — document it. Mirror `latvia_ur`/`estonia_ar` metrics.

## 8. Cross-cutting standard B — translation (always, for non-English free text)
Non-English text/enum fields are translated to English by the **standalone Go translator service**
(`corpscout/translator` — see its README for the API contract). The flow is: **loader asset (this
repo) → Go translator service → `corpscout.text_translations`**. Translation is **not** done in
dbt/SQL; only the thin loader asset lives in the graph (full detail in `dagster_v3/README.md`
"# Translation").

The base ClickHouse table carries only `<field>_original` — **do not add `<field>_en` (or
`_translated_at`/`_provider`/`_model`) columns to it.** English values live in the shared
`corpscout.text_translations` cache, **keyed by `(source_table, source_column, source_text_hash)`** — a
cache row names its exact table and column. Results are exposed by a per-source `<source>_translated` join
view. The cache survives the wipe-and-replace export, so a refresh only translates genuinely new/changed text.

The loader pattern contract (translator HTTP resource in
`src/dagster_v3/defs/translator_load/resource.py`, ClickHouse SQL helpers in
`translator_load/loader.py`, and explicit per-source assets in `defs/<source>/translation.py`):
- **Anti-join scan**: `SELECT DISTINCT` untranslated texts per `(table, column)` by LEFT ANTI JOIN
  against `text_translations` — **loaders own dedup**, the service does not.
- **Hash in SQL**: `cityHash64(col)` computed in ClickHouse, never in Python, so hashes always
  agree with past runs and the join view.
- **Chunked POST**: at most 10k items per request to the service's `POST /v1/queue/items`, hashes
  as decimal strings; the first successful enqueue starts the service's Temporal workflow.
- **Completion gate**: after enqueueing, call `translator.wait_for_queue_completion()`. Failed
  queue items, workflow-start warnings, and timeouts must fail the Dagster asset; successful
  materialization means translated output was flushed to ClickHouse.
- **Static maps direct-insert**: closed enumerations skip the queue — the loader resolves them from
  an in-loader code→EN dict and inserts rows with `provider='static'`.

To make a source's fields translatable:
1. **Loader**: add source language constants, translated fields, and a loader asset accepting
   `translator: TranslatorResource` in `defs/<source>/translation.py` (mirror
   `defs/latvia_ur/translation.py`, or `defs/norway_brreg/assets/translation.py` for a source with
   static maps), downstream of the source's ClickHouse export. Keep the ClickHouse scan and
   `translator.enqueue_translation_rows(...)` / `translator.wait_for_queue_completion()` calls
   visible in the asset body.
   Pick the mechanism by field kind:
   - **Free text** (company description, activity text) → LLM (enqueue to the service).
   - **Finite enumeration** (legal form, status, size category) → static map + key column,
     direct-inserted. Deterministic and free. *Prefer this for closed sets.*
   - **Proper nouns** (company name, address) → not translated.
2. **View**: add a `<source>_translated` join view (mirror `corpscout.no_companies_translated`).
3. **Job wiring**: include the loader asset in the source's refresh job selection (mirror
   `norway_brreg_entities_full_snapshot_job`). No completion sensor is required: the loader itself
   waits. A translation failure fails the job after the upstream ingestion assets have already
   materialized; it does not roll back their ClickHouse writes.
- The design doc lists every translated field and its mechanism (LLM vs static dict).

Sources **without official industry codes** (see §8c) classify their free-text activity
description directly: add a `defs/<source>/classification.py` mirroring
`defs/latvia_ur/classification.py`, calling the shared embed-retrieve-adjudicate machinery in
`defs/classifier/lib.py`. Same anti-join/cache shape as translation — results are cached per
distinct text in `corpscout.text_classifications`, and a per-source view (mirror
`lv_companies_nace`) exposes the joined NACE code.

## 8b. Cross-cutting standard C — contact information (ALWAYS pull it)
**Connecting a company to its internet/contact presence is core to corpscout — capturing *any*
contact information is mandatory, not optional.** When you analyse a new source, the data inventory
**must explicitly check for contact data**: website/URL, email, phone, mobile, fax, social handles.
- It is frequently **not in the basic register** but in a richer "general data"/contacts dataset —
  *look for it.* Estonia: `lihtandmed` (register CSV) has **none**; `yldandmed` (general data JSON)
  has `sidevahendid` (`{liik, sisu}` = type, value) with `WWW`/`EMAIL`/`MOB`/`TEL`/`FAX`. If a source
  truly has no contact data, the design doc must say so explicitly.
- **Write the canonical pair, not a per-source shape.** Every source writes ONE
  **`<src>_company_contacts`** table (contact facts as found in the register — no inference; a
  website URL, an email, a phone number, and a domain-looking company name are all facts) and ONE
  **`<src>_company_domains`** table (derived company↔domain associations with provenance and
  confidence — the ONLY thing the domain graph reads). Column order/types for both tables are
  owned by the standard spec (`docs/superpowers/specs/2026-07-04-company-contacts-domains-standard-design.md`)
  and the shared `dagster_v3.contact_extraction.COMPANY_CONTACTS_COLUMNS`/`COMPANY_DOMAINS_COLUMNS`
  constants — don't invent a per-source shape. A source with no contact data still gets both tables
  (empty); consumers never special-case. Conformance is test-enforced: any
  `<src>_company_contacts`/`<src>_company_domains` migration must match the canonical DDL modulo
  table name, checked by the shared helper `tests/canonical_contact_tables.py`
  (`assert_canonical_contacts_ddl`/`assert_canonical_domains_ddl`).
- **Website AND email are domain signals.** A register website field yields a
  `contact_type='website'` fact row AND a `domain_source='website'` domain row (deliberate
  duplication — the contacts table preserves what the register said, the domains table is the
  joinable distillation):
  - **Website** → `root_domain(contact_value)` via the shared `dagster_v3.domains` tldextract UDFs
    (`root_domain`/`normalized_url`/`website_host`) — register them on the DuckDB connection.
  - **Email** → the email suffix, **but only if it is unique to one company.** Count *distinct
    companies* per suffix (not contact rows) and drop any suffix used by `> EMAIL_DOMAIN_MAX_COMPANIES`
    (default 1). This single rule auto-excludes mail providers (gmail, hot.ee) *and* shared
    accounting/formation-agent domains (one bookkeeper fronting hundreds of shell clients) — no magic
    threshold. Keep a small provider denylist only as a backstop (the shared
    `EMAIL_PROVIDER_DENYLIST` in `contact_extraction.py`). **Email matters**: far more companies
    have a same-domain email than a website (Estonia: ~340k vs ~21k).
- **`<src>_company_domains` feeds the cross-source graph**: one row per `(registry_id, domain)`,
  website preferred over email, exactly one `is_primary` per `registry_id` via the shared
  `elect_primary_domains()` election rule (website-sourced first, then current, then confidence,
  then shortest/alphabetical domain); website rows carry `website_url/_normalized_url/_host`. Add a
  UNION branch + the `domain_source` column to `domains/assets.py` so it lands in
  `company_website_domains` → `domains`.
- Pull contacts on the source's normal cadence; they sit alongside the register, not the financials.
- **Mandatory alongside currency (§7) and translation (§8).** Reference impl (canonical pair):
  `czech_ares` (`cz_company_contacts`/`cz_company_domains`) and `latvia_ur`
  (`lv_company_contacts`/`lv_company_domains`), both extracted from free-text legal names via the
  shared module below. Estonia reshaped in migration `000096` (data-preserving); Brazil/Norway/
  Finland/wikidata reshaped in Phase D. **All seven sources now write the canonical pair; Phase E
  (2026-07-05) switched the domain graph to read only the seven `<src>_company_domains` tables.**
- **The pre-standard `<src>_websites`-shaped tables (`fi_websites`, `no_websites`,
  `wikidata_company_websites`, `br_websites`) are demoted to internal stages** — the domain graph no
  longer reads them. `fi_websites`/`no_websites`/`wikidata_company_websites` are consumed only by
  their source's canonical derivation (`defs/finland_ytj/contacts.py`,
  `defs/norway_brreg/assets/contacts.py`, `defs/wikidata/contacts.py` respectively); `br_websites` has
  zero consumers (retire via a future migration). **Do not build new consumers on any of the four.**
  A source whose contacts already live in a `<src>_websites`-shaped table derives the canonical
  pair with a ClickHouse-native INSERT-SELECT (`dagster_v3.contact_extraction.replace_table_from_select`),
  not Python-materialized rows — see the three derivation modules above.
- **When a source has no structured contact fields but embeds domains/emails in free text** (e.g. a
  legal name like `SIA "cenuklubs.lv"`), use the shared `dagster_v3/contact_extraction.py` module
  (IDN-aware candidate parsing, CommonCrawl/DNS validation, atomic table replace, canonical-column
  builders `iter_contact_fact_rows`/`iter_company_domain_rows`/`elect_primary_domains`) instead of
  hand-rolling extraction — mirror the thin per-country orchestrators `defs/latvia_ur/contacts.py`
  or `defs/czech_ares/contacts.py`.

## 8c. Cross-cutting standard D — industry / NACE (ALWAYS connect it)
**Every company should connect to the unified NACE id.** The data inventory **must check for an
industry/activity classification** (national registers publish NACE, or a NACE-derived national scheme:
EE EMTAK, FI TOL2008, NO SN2007, LV NACE). If present, capture it; if absent from the register, look in
the general-data/financials datasets before concluding it's unavailable.
- **Store normalized**: one **`<cc>_industries`** table mirroring `no_industries`/`fi_industries` —
  `(<company_id>, source_industry_code, source_industry_code_set, description_original/_en (+ translation
  provenance), nace_revision, nace_code, nace_normalized_code, nace_mapping_method, nace_mapping_status,
  is_primary, …)`. One row per activity; flag the primary.
- **Map to NACE**: if the source provides the NACE code directly (EE `nace_kood`, NO codes are NACE),
  `nace_mapping_method='source_provided'`/`'direct_code'`. Otherwise map the national scheme → NACE and
  record `method`/`status` (`mapped`/`unmapped`). `nace_normalized_code` strips punctuation; pick
  `nace_revision` (`NACE_REV_2` / `NACE_REV_2_1`) to match the join.
- **Unified id target**: `corpscout.nace_categories` (EU SPARQL reference, Rev 2 + Rev 2.1) — must be
  materialized (`nace_categories_clickhouse`). Join on `nace_normalized_code`/`nace_revision`.
- **Per-source check**: EE/FI/NO/FR/GB/CZ/SK **yes** (FR = NAF Rev2 + NAF 2025; GB = UK SIC 2007;
  CZ = CZ-NACE + NACE2025; SK = SK-NACE Rev 2 from `statisticalCodes.mainActivity` — all strip to
  the first 4 digits → NACE); LV register + annual reports
  carry **no** per-company NACE
  (would need a separate CSP/VID source) — document that explicitly. Reference impl: `estonia_ar`
  (`ee_industries` from `yldandmed.teatatud_tegevusalad`).

## 8d. Cross-cutting standard E — Wikidata registry-number seed (check for every country)
**If Wikidata has a national registry-number property for the country, wire it into the
Wikidata registry-number seed** (`defs/wikidata/`), which seeds unlisted companies
(discovered by carrying the property) alongside the exchange-listed-company seed. Check
Wikidata for a property like SE `P6460`, NO `P2333`, DK `P1059` before concluding there
isn't one — most countries with a national company/organisation register have one.
- Declare **one `WikidataRegistrySeedSpec`** constant in the new source's own module
  (its `tables.py`, or a tiny `wikidata_seed.py` if the module has no `tables.py`) —
  see `defs/common/wikidata_registry_seed.py` for the dataclass and
  `defs/sweden_company/tables.py:WIKIDATA_REGISTRY_SEED_SPEC` for the pattern. Declaring
  it next to the source's own module (not in a central list under `defs/wikidata/`)
  means it's naturally added alongside everything else for the source — a central list
  would be forgotten.
- `defs/wikidata/registry_seed.py` aggregates every module's spec via an explicit
  import; add the new one there too. **The wikidata seed test
  (`tests/test_wikidata_assets.py::test_wikidata_registry_seed_specs_are_wired_into_seed_asset`)
  enforces the wiring** — it fails loudly if a spec is declared but not aggregated, or
  aggregated but its `spine_asset_key` isn't a real registered asset.

## 9. Scheduling (cadence-matched, non-partitioned full-refresh)
- **Match the schedule to the source's refresh rate**, and split chains that refresh differently into
  separate jobs (register daily/weekly; financials monthly). See CLAUDE.md "Scheduling".
- Select job assets with **`AssetSelection.assets(...).upstream()`** (full transitive chain — the
  `dg launch +leaf` CLI only resolves one hop). **Stagger** cron minutes across sources; leave
  schedules **default-STOPPED** until validated.
- Translation is **decoupled from ingestion**: the refresh job lands the source's tables in
  ClickHouse, then a **loader asset** scans for untranslated text and enqueues it to the Go
  translator service, returning immediately (no completion sensor, no gating). Mirror
  `norway_brreg_translation_load`.

## 10. Required: a per-source design doc
**Every source must ship `defs/<source>/docs/<source>-design.md`** using
`docs/source-design-doc-template.md`. It records the *decisions*, not the code: why this ingest mode,
where data is stored, the schema + any **DDL deviations from the norm and why**, the translation +
currency approach, the partitioning decision, and **issues hit during processing**. A reviewer
should be able to understand the source from this doc alone. Update it whenever a decision changes.
