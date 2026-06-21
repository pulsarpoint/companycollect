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
  writes it. The file **stem must differ from the dlt dataset name**.
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
2. **Only raw XBRL/iXBRL → parse with Arelle**, the regulator-grade reference processor (handles
   taxonomies/dimensions/units/validation and **iXBRL/ESEF**, which is impractical to parse by hand).
   Finland (`finland_xbrl`) is this. Run it as a **partitioned** extraction stage (§4).
3. **A lighter `lxml`/`py-xbrl` instance parser** → only for *plain* (non-inline) XBRL at extreme
   volume, and **only after** measuring Arelle as the bottleneck. You give up iXBRL + validation.

**Decouple three stages — this is what scales XBRL across many countries:**
- **Extraction** (the parser's only job): XBRL → a generic flat fact table
  `(concept_qname, context, period, unit, value, dimensions)`. Swappable.
- **Mapping**: `concept_qname → canonical metric` as a **per-taxonomy lookup table (data, not code)**,
  applied in SQL/dbt (see `finland_xbrl`'s `xbrl_metric_map`). **A new XBRL country is mostly a new
  mapping table** — national GAAP / IFRS / ESEF taxonomies all flow through the same shape.
- **Normalize / USD / translate**: downstream SQL/dbt per the standard (§5–§8).

**Make XBRL extraction a shared component**, not per-country code: a reusable `xbrl_common` Arelle
wrapper (→ flat fact table) parameterized by `(taxonomy entrypoint, concept→metric map)`. A new XBRL
country then = configure it + supply a mapping table, rather than re-implementing a parser.

**Scaling Arelle** (it's the heavy stage — mitigate, don't rewrite):
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
- **Batch the rate requests** (`_load_rates`, ≤50/call). Currencies absent from the ECB set (e.g.
  legacy LVL) keep native-only (`_usd` NULL) — document it. Mirror `latvia_ur`/`estonia_ar` metrics.

## 8. Cross-cutting standard B — translation (always, for non-English free text)
Every non-English text/enum field gets an `_en` companion column. Pick the mechanism by field kind:
- **Finite enumeration** (legal form, status, statement/size category) → **static lookup map** in
  Python, applied at row-build (`EE_LEGAL_FORM_EN_BY_NAME.get(x,'')`). Deterministic, no LLM, no
  Temporal. DDL: `<field>_original` + `<field>_en`. *Default — prefer this.*
- **Free text** (company description, activity text) → **LLM translation-service** (Temporal queue,
  start-or-reuse workflow, sensor-driven completion). DDL adds `<field>_en` +
  `<field>_translated_at` + `<field>_translation_provider` + `<field>_translation_model`. Mirror
  `norway_brreg`. Schedule it monthly (translation is slow/expensive); never re-trigger daily.
- **Proper nouns** (company name, address) → **not translated**.
- The design doc lists every translated field and its mechanism.

## 8b. Cross-cutting standard C — contact information (ALWAYS pull it)
**Connecting a company to its internet/contact presence is core to corpscout — capturing *any*
contact information is mandatory, not optional.** When you analyse a new source, the data inventory
**must explicitly check for contact data**: website/URL, email, phone, mobile, fax, social handles.
- It is frequently **not in the basic register** but in a richer "general data"/contacts dataset —
  *look for it.* Estonia: `lihtandmed` (register CSV) has **none**; `yldandmed` (general data JSON)
  has `sidevahendid` (`{liik, sisu}` = type, value) with `WWW`/`EMAIL`/`MOB`/`TEL`/`FAX`. If a source
  truly has no contact data, the design doc must say so explicitly.
- **Store normalized**: one **`<cc>_company_contacts`** table, one row per contact
  `(reg_code, contact_type, contact_type_en, contact_value, is_current, …)`, capturing **all** types.
  Don't fold contacts into the company row; a company has many.
- **Website AND email are domain signals.** Add **`domain` + `domain_source`** (`'website'|'email'|''`)
  to the contacts table, computed at build time:
  - **Website** → `root_domain(contact_value)` via the shared `dagster_v3.domains` tldextract UDFs
    (`root_domain`/`normalized_url`/`website_host`) — register them on the DuckDB connection.
  - **Email** → the email suffix, **but only if it is unique to one company.** Count *distinct
    companies* per suffix (not contact rows) and drop any suffix used by `> EMAIL_DOMAIN_MAX_COMPANIES`
    (default 1). This single rule auto-excludes mail providers (gmail, hot.ee) *and* shared
    accounting/formation-agent domains (one bookkeeper fronting hundreds of shell clients) — no magic
    threshold. Keep a small provider denylist only as a backstop. **Email matters**: far more companies
    have a same-domain email than a website (Estonia: ~340k vs ~21k).
- **Feed the cross-source graph.** Build a deduped **`<cc>_company_domains`** table (one row per
  `(reg_code, domain)`, website preferred over email, one `is_primary`/company; website rows carry
  `website_url/_normalized_url/_host`), then add a UNION branch + the `domain_source` column to
  `domains/assets.py` so it lands in `company_website_domains` → `domains`.
- Pull contacts on the source's normal cadence; they sit alongside the register, not the financials.
- **Mandatory alongside currency (§7) and translation (§8).** Reference impl: `estonia_ar`
  (`ee_company_contacts`/`ee_company_domains` from `yldandmed`).

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
- **Per-source check**: EE/FI/NO **yes**; LV register + annual reports carry **no** per-company NACE
  (would need a separate CSP/VID source) — document that explicitly. Reference impl: `estonia_ar`
  (`ee_industries` from `yldandmed.teatatud_tegevusalad`).

## 9. Scheduling (cadence-matched, non-partitioned full-refresh)
- **Match the schedule to the source's refresh rate**, and split chains that refresh differently into
  separate jobs (register daily/weekly; financials monthly). See CLAUDE.md "Scheduling".
- Select job assets with **`AssetSelection.assets(...).upstream()`** (full transitive chain — the
  `dg launch +leaf` CLI only resolves one hop). **Stagger** cron minutes across sources; leave
  schedules **default-STOPPED** until validated.
- Translation-gated sources (Norway) get a **coordinated** refresh (load once → kick off async
  translation + financials; companies land via the completion sensor), not the naive template.

## 10. Required: a per-source design doc
**Every source must ship `defs/<source>/docs/<source>-design.md`** using
`docs/source-design-doc-template.md`. It records the *decisions*, not the code: why this ingest mode,
where data is stored, the schema + any **DDL deviations from the norm and why**, the translation +
currency approach, the partitioning decision, and **issues hit during processing**. A reviewer
should be able to understand the source from this doc alone. Update it whenever a decision changes.
