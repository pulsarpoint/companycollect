# ESEF Filings Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest all EU/EEA listed-company ESEF annual reports from filings.xbrl.org into country-agnostic `esef_*` ClickHouse tables (filings, facts, IFRS metrics), matched to national registry ids via gleif — closing the listed-company financials blind spot for every country we cover (Finland's 0/298 first).

**Architecture:** Shared cross-country module `defs/esef_filings/` mirroring `ted_procurement` (one ingest, per-country consumers). Non-partitioned weekly index crawl (25k filings total — one paginated sweep); fact xBRL-JSON downloads cached in S3 keyed by filing id, year-partitioned for backfill throttling only; facts parsed from OIM xBRL-JSON (no XBRL parser needed — Level 1 of the ingest hierarchy); metrics derived **in ClickHouse** from exported facts (mirrors `sweden_financial` metrics), with `scope='consolidated_ifrs'` on every derived row and LEI→registry-id matching via `gleif_lei_records`.

**Tech Stack:** dlt requests session (retries), DuckDB staging (`data/esef_filings_source.duckdb`, dataset `esef_filings`), ClickHouse migration 000149, clickhouse-driver exports with stage+EXCHANGE atomic replace, `corpscout.exchange_rates` for USD conversion.

**Reference material (read before implementing):**
- `docs/esef-filings-research.md` — the agreed design this plan implements
- `defs/ted_procurement/` + `docs/ted-procurement.md` — module shape to mirror
- `defs/sweden_financial/metrics.py` — CH-derived metrics pattern
- `defs/clickhouse/resolved.py` — shared exporter (refuse-on-empty, stage+exchange)
- Live API (verified 2026-07-20): `GET https://filings.xbrl.org/api/filings?page[size]=N&page[number]=M&include=entity` — JSON:API; filing attrs: `fxo_id, country, date_added, period_end, processed, json_url, package_url, report_url, viewer_url, sha256, error_count, warning_count, inconsistency_count`; included `entity` carries `identifier` (LEI) and `name`; `meta.count` = 25,061 total (FI = 1,168). All URLs are relative to `https://filings.xbrl.org`.

## Global Constraints

- Migration number **000149** (`000149_corpscout_esef_filings`); register in `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py`. Migration owns ALL DDL; Dagster only stage-tables during export.
- Every ESEF-derived metrics row carries `scope = 'consolidated_ifrs'` (LowCardinality(String), NOT NULL). `company_financials_latest` does **NOT** consume ESEF rows in v1 (research doc open question 4 — deferred until the scope-preference rule is agreed).
- Entity key is **LEI** everywhere in `esef_*` tables. National registry ids appear only in the match layer (`esef_entity_registry_map`) and are *never* a filter on ingestion — unmatched LEIs are kept.
- S3 bucket `source-esef-filings`; DuckDB file `data/esef_filings_source.duckdb` (stem ≠ dataset name rule: dataset is `esef_filings`, file stem `esef_filings_source`); every asset writing it uses `pool="esef_filings_duckdb"`.
- HTTP via `dlt.sources.helpers.requests` Client (never plain `requests`); whole-download retry around streamed downloads.
- Non-nullable CH String columns get `''` never NULL; `raw_*` payloads and `source_payload_hash` stay in DuckDB, never exported to CH.
- Filings without a usable `json_url` are **skipped and counted** in v1 (no Arelle fallback yet); the skip count must be loud in materialization metadata and stored per filing (`has_json_facts` flag) so coverage is queryable.
- Commit by explicit path after each task (working tree carries unrelated WIP). Conventional Commits format.
- All commands via `uv run` from `corpscout/services/dagster_v3/`; validate with `uv run dg check defs` + the task's pytest files.
- ClickHouse migrations are applied by the USER (`make clickhouse-migrate-up` from `corpscout/`) — implementers must not run migrate commands or CH DDL; write the files, tests against the files, and note the pending apply in the task report.

---

### Task 1: ClickHouse migration 000149 (four tables)

**Files:**
- Create: `corpscout/clickhouse/migrations/000149_corpscout_esef_filings.up.sql`
- Create: `corpscout/clickhouse/migrations/000149_corpscout_esef_filings.down.sql`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py` (add `"000149_corpscout_esef_filings"` to `EXPECTED_MIGRATIONS`; add a column-contract test mirroring the existing per-table ones)

**Interfaces:**
- Produces: `corpscout.esef_filings`, `corpscout.esef_facts`, `corpscout.esef_financial_metrics`, `corpscout.esef_entity_registry_map` — exact DDL below; later tasks' export column tuples must match these orders exactly.

- [ ] **Step 1: Write the failing registry test** — add the migration name to `EXPECTED_MIGRATIONS` and run `uv run pytest tests/test_clickhouse_migrations.py -q`; expect FAIL (file missing).
- [ ] **Step 2: Write the up migration**

```sql
-- 000149_corpscout_esef_filings.up.sql
CREATE TABLE IF NOT EXISTS corpscout.esef_filings
(
    lei                 String,
    entity_name         String,
    fxo_id              String,
    country             LowCardinality(String),
    period_end          Date32,
    date_added          Date32,
    processed_at        Nullable(DateTime64(6)),
    json_url            String,
    package_url         String,
    report_url          String,
    viewer_url          String,
    package_sha256      String,
    error_count         UInt32,
    warning_count       UInt32,
    inconsistency_count UInt32,
    has_json_facts      UInt8,
    source_url          String,
    source_run_id       String,
    resolved_at         DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (lei, period_end, fxo_id);

CREATE TABLE IF NOT EXISTS corpscout.esef_facts
(
    lei                 String,
    fxo_id              String,
    period_end          Date32,
    fact_id             String,
    concept_qname       String,
    concept_namespace   LowCardinality(String),
    concept_local_name  String,
    period_start        Nullable(Date32),
    period_instant      Nullable(Date32),
    unit                LowCardinality(String),
    currency            LowCardinality(String),
    value_kind          LowCardinality(String),
    raw_value           String,
    amount_original     Nullable(Decimal128(2)),
    decimals            Nullable(Int32),
    dimensions          String,
    language            LowCardinality(String),
    source_run_id       String,
    resolved_at         DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (lei, period_end, fxo_id, fact_id);

CREATE TABLE IF NOT EXISTS corpscout.esef_financial_metrics
(
    lei                              String,
    entity_name                      String,
    fxo_id                           String,
    country                          LowCardinality(String),
    scope                            LowCardinality(String),
    fiscal_year                      Int32,
    period_start                     Nullable(Date32),
    period_end                       Date32,
    currency                         LowCardinality(String),
    revenue_amount_original          Nullable(Decimal128(2)),
    revenue_amount_usd               Nullable(Decimal128(2)),
    operating_profit_amount_original Nullable(Decimal128(2)),
    operating_profit_amount_usd      Nullable(Decimal128(2)),
    profit_loss_amount_original      Nullable(Decimal128(2)),
    profit_loss_amount_usd           Nullable(Decimal128(2)),
    total_assets_amount_original     Nullable(Decimal128(2)),
    total_assets_amount_usd          Nullable(Decimal128(2)),
    equity_amount_original           Nullable(Decimal128(2)),
    equity_amount_usd                Nullable(Decimal128(2)),
    liabilities_amount_original      Nullable(Decimal128(2)),
    liabilities_amount_usd           Nullable(Decimal128(2)),
    cash_amount_original             Nullable(Decimal128(2)),
    cash_amount_usd                  Nullable(Decimal128(2)),
    employees                        Nullable(Int64),
    mapped_fact_count                UInt32,
    source_fact_count                UInt32,
    mapping_version                  LowCardinality(String),
    fx_rate_to_usd                   Nullable(Float64),
    fx_rate_date                     Nullable(Date32),
    fx_source                        LowCardinality(String),
    viewer_url                       String,
    source_run_id                    String,
    resolved_at                      DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (lei, period_end, fxo_id);

CREATE TABLE IF NOT EXISTS corpscout.esef_entity_registry_map
(
    lei                    String,
    country_iso2           LowCardinality(String),
    registry_id_raw        String,
    registry_id            String,
    match_source           LowCardinality(String),
    source_run_id          String,
    resolved_at            DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (country_iso2, registry_id, lei);
```

- [ ] **Step 3: Write the down migration** — `DROP TABLE IF EXISTS` for the four tables in reverse order.
- [ ] **Step 4: Run** `uv run pytest tests/test_clickhouse_migrations.py -q` → PASS.
- [ ] **Step 5: Commit** the two migration files + the test by explicit path: `feat(clickhouse): esef filings/facts/metrics/entity-map tables (000149)`. Note in the task report: **user must run `make clickhouse-migrate-up`** before Task 8 validation.

---

### Task 2: Module skeleton — tables contract + API client

**Files:**
- Create: `src/dagster_v3/defs/esef_filings/__init__.py` (empty)
- Create: `src/dagster_v3/defs/esef_filings/tables.py`
- Create: `src/dagster_v3/defs/esef_filings/client.py`
- Test: `tests/test_esef_filings_client.py`

**Interfaces:**
- Produces: `EsefFilingsClient.iter_filings() -> Iterator[EsefFilingRecord]`; `EsefFilingsClient.download_json_facts(json_url: str, target: Path) -> None`; `tables.py` constants `ESEF_FILINGS_EXPORT_COLUMNS`, `ESEF_FACTS_EXPORT_COLUMNS`, `ESEF_ENTITY_MAP_EXPORT_COLUMNS` (tuples matching the 000149 column orders exactly, minus `resolved_at` which CH defaults).

`client.py` core (complete the class around this skeleton; JSON:API traversal is the whole trick — entities arrive in `included`, filings reference them via `relationships.entity.data.id`):

```python
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dlt.sources.helpers import requests as dlt_requests

ESEF_API_BASE = "https://filings.xbrl.org"
ESEF_INDEX_URL = f"{ESEF_API_BASE}/api/filings"
PAGE_SIZE = 200


@dataclass(frozen=True)
class EsefFilingRecord:
    lei: str
    entity_name: str
    fxo_id: str
    country: str
    period_end: str | None
    date_added: str | None
    processed_at: str | None
    json_url: str | None       # absolute URL or None
    package_url: str | None
    report_url: str | None
    viewer_url: str | None
    package_sha256: str | None
    error_count: int
    warning_count: int
    inconsistency_count: int


def _absolute(url: str | None) -> str | None:
    if url is None or url == "":
        return None
    return url if url.startswith("http") else f"{ESEF_API_BASE}{url}"


class EsefFilingsClient:
    def __init__(self, session: Any | None = None) -> None:
        self._session = session or dlt_requests.Client(
            request_timeout=120, request_max_attempts=5
        ).session

    def iter_filings(self) -> Iterator[EsefFilingRecord]:
        page = 1
        while True:
            resp = self._session.get(
                ESEF_INDEX_URL,
                params={
                    "page[size]": PAGE_SIZE,
                    "page[number]": page,
                    "include": "entity",
                },
            )
            resp.raise_for_status()
            payload = resp.json()
            entities = {
                inc["id"]: inc["attributes"]
                for inc in payload.get("included", [])
                if inc.get("type") == "entity"
            }
            data = payload.get("data", [])
            if not data:
                return
            for item in data:
                attrs = item["attributes"]
                rel = item.get("relationships", {}).get("entity", {}).get("data")
                ent = entities.get(rel["id"], {}) if rel else {}
                yield EsefFilingRecord(
                    lei=str(ent.get("identifier", "") or ""),
                    entity_name=str(ent.get("name", "") or ""),
                    fxo_id=str(attrs.get("fxo_id", "") or ""),
                    country=str(attrs.get("country", "") or ""),
                    period_end=attrs.get("period_end"),
                    date_added=attrs.get("date_added"),
                    processed_at=attrs.get("processed"),
                    json_url=_absolute(attrs.get("json_url")),
                    package_url=_absolute(attrs.get("package_url")),
                    report_url=_absolute(attrs.get("report_url")),
                    viewer_url=_absolute(attrs.get("viewer_url")),
                    package_sha256=attrs.get("sha256"),
                    error_count=int(attrs.get("error_count") or 0),
                    warning_count=int(attrs.get("warning_count") or 0),
                    inconsistency_count=int(attrs.get("inconsistency_count") or 0),
                )
            page += 1

    def download_json_facts(self, json_url: str, target: Path) -> None:
        # streamed download with whole-file retry, mirroring
        # latvia_ur/resources.py:_download_to_path (truncate per attempt,
        # verify Content-Length when present)
        ...
```

- [ ] **Step 1: Capture fixtures** — save two real index pages (`page[size]=3`) and one real `json_url` response snippet under `tests/fixtures/esef_filings/` (small, committed). Use `curl` once; do not hit the network in tests.
- [ ] **Step 2: Write failing tests** — `iter_filings` pagination (two fixture pages then empty page → stops; entity join yields lei+name; relative URLs absolutized; missing `json_url` → None), then run to see FAIL.
- [ ] **Step 3: Implement `client.py` + `tables.py`** (export-column tuples copied from the migration file order, minus `resolved_at`).
- [ ] **Step 4: Add a contract test** that greps `../../clickhouse/migrations/000149_corpscout_esef_filings.up.sql` and asserts each `*_EXPORT_COLUMNS` tuple matches the migration's column order (mirror the existing pattern in `tests/test_clickhouse_migrations.py`).
- [ ] **Step 5: Run** `uv run pytest tests/test_esef_filings_client.py -q` → PASS. Commit: `feat(dagster): esef_filings module skeleton — index client + table contracts`.

---

### Task 3: Index asset — full crawl → DuckDB

**Files:**
- Create: `src/dagster_v3/defs/esef_filings/assets.py` (index asset + non-empty check; jobs/schedule come in Task 7)
- Test: `tests/test_esef_filings_assets.py`

**Interfaces:**
- Produces: asset `esef_filings_index_duckdb` (non-partitioned, `pool="esef_filings_duckdb"`) writing DuckDB table `esef_filings.filings_index` — full replace each run, one row per `EsefFilingRecord` plus `source_run_id`, `has_json_facts` (`json_url is not None`), `source_url` (the index API URL). Raises `ValueError` if the crawl yields 0 filings (refuse-to-replace-on-empty).
- Non-partitioned by design: the full index is ~25k rows / ~125 pages — one sweep, no per-window bookkeeping (deviation from the research doc's monthly-partition sketch, justified by `CLAUDE.md` partitioning guidance; the expensive part — fact downloads — is incremental via S3 skip-existing in Task 4).

- [ ] **Step 1: Write failing asset test** — client stubbed with 3 records (one without `json_url`); materialize via `dg.materialize` into a tmp DuckDB path; assert row count 3, `has_json_facts` false for the one, full-replace semantics on second run; empty crawl → `ValueError`.
- [ ] **Step 2: Implement the asset** (mirror `ted_procurement/assets.py` structure for resource wiring; DuckDB path helper `esef_filings_source_duckdb_path()` under the standard `data/` root).
- [ ] **Step 3: Add asset check** `filings_index_non_empty` + a `country` distribution entry in materialization metadata (count by country, top 10 + total).
- [ ] **Step 4: Run tests + `uv run dg check defs`** → PASS. Commit: `feat(dagster): esef filings index crawl asset`.

---

### Task 4: Fact download + OIM xBRL-JSON parse (year-partitioned)

**Files:**
- Create: `src/dagster_v3/defs/esef_filings/facts.py` (parser)
- Modify: `src/dagster_v3/defs/esef_filings/assets.py` (download+parse asset)
- Test: `tests/test_esef_filings_facts.py`

**Interfaces:**
- Consumes: `esef_filings.filings_index` (Task 3), `EsefFilingsClient.download_json_facts` (Task 2).
- Produces: asset `esef_filing_facts_duckdb`, `StaticPartitionsDefinition([str(y) for y in range(2019, 2028)])` keyed by `toYear(period_end)`, `deps=["esef_filings_index_duckdb"]` (AllPartitionMapping not needed — index is unpartitioned), pool `esef_filings_duckdb`. Per partition run: select its year's filings from the local index, for each with `has_json_facts`: download `json_url` to S3 `esef_filings/fact_json/fxo_id=<fxo_id>/facts.json` (HEAD/skip if the object already exists — fxo_id is versioned upstream so the key is stable per filing version), parse, and **replace only that year's rows** in DuckDB table `esef_filings.facts` (`delete where period_end_year = ?` then insert — partition-scoped, mirroring the sweden lesson). Filings without JSON are counted in metadata (`skipped_no_json`).
- Parser: `parse_oim_facts(payload: dict, *, lei: str, fxo_id: str, period_end: str) -> list[EsefFact]`. OIM xBRL-JSON shape: top-level `"facts"` dict of `fact_id -> {"value": ..., "decimals": ..., "dimensions": {"concept": "ifrs-full:Revenue", "entity": "scheme:LEI", "period": "2022-01-01T00:00:00/2022-12-31T00:00:00" (duration) or "2022-12-31T00:00:00" (instant), "unit": "iso4217:EUR", "language": "fi", <taxonomy dims...>}}`. Rules: `concept_namespace`/`concept_local_name` split on `:`; `value_kind` = `monetary` when unit starts `iso4217:` (currency = suffix), `numeric` for other units, `text` otherwise; `amount_original` = Decimal(value) for monetary/numeric else None; non-core dimensions (anything besides concept/entity/period/unit/language/noteId) serialized as sorted-key JSON into `dimensions` (else `''`); period split into `period_start`/`period_instant`/derived end.

- [ ] **Step 1: Write failing parser tests from the Task 2 fixture** — monetary fact (value/currency/decimals), instant vs duration period, text fact, extension-taxonomy dimension serialization, facts entry missing `dimensions.concept` → skipped + counted.
- [ ] **Step 2: Implement `facts.py`** until parser tests pass.
- [ ] **Step 3: Write failing asset test** — stub object store + client (serving fixture JSON); materialize partition `"2022"` twice → second run: 0 downloads (skip-existing), row counts stable (scoped replace); a filing without json_url increments `skipped_no_json`.
- [ ] **Step 4: Implement the asset**; `BackfillPolicy.multi_run(max_partitions_per_run=1)`.
- [ ] **Step 5: Run** `uv run pytest tests/test_esef_filings_facts.py -q` + `uv run dg check defs` → PASS. Commit: `feat(dagster): esef fact json download + OIM parse assets`.

---

### Task 5: ClickHouse exports (filings, facts) + entity map

**Files:**
- Create: `src/dagster_v3/defs/esef_filings/publish.py`
- Modify: `src/dagster_v3/defs/esef_filings/assets.py` (three export assets)
- Modify: `src/dagster_v3/defs/common/clickhouse_checks.py` (add the new leaves)
- Test: `tests/test_esef_filings_publish.py`, modify `tests/test_clickhouse_leaf_checks.py`

**Interfaces:**
- Produces: `esef_filings_clickhouse` (from `filings_index`, full replace via the shared exporter in `defs/clickhouse/resolved.py`: assert-exists, stage+EXCHANGE, refuse-on-empty, shrink guard), `esef_facts_clickhouse` (same, full table from DuckDB `esef_filings.facts` — full replace is CORRECT here, unlike sweden: one DuckDB file holds the entire dataset, there is no split-file hazard; keep the shrink guard), and `esef_entity_registry_map_clickhouse` — built **in ClickHouse** from `gleif_lei_records` (no DuckDB input): stage table + `INSERT ... SELECT lei, primary_country_iso2, registered_as, <normalized>, 'gleif_registered_as', ... FROM corpscout.gleif_lei_records WHERE registered_as != '' AND lei IN (SELECT DISTINCT lei FROM corpscout.esef_filings)` + EXCHANGE. Normalization v1 (document in the SQL): `FI` → lowercase, strip spaces, ensure `NNNNNNN-N` (insert dash before last digit if 8 bare digits); `SE` → digits only, 10 digits; all other countries → `trim` only (raw passthrough; extend per country as backoffice consumers appear).
- `esef_facts` volume estimate ~15–40M rows — well inside full-replace comfort.

- [ ] **Step 1: Failing tests** — export column tuples flow through the shared exporter (stub CH client records SQL; assert explicit column lists match `tables.py`), entity-map SQL contains the scope subquery + both normalizers, leaf-check registration test updated.
- [ ] **Step 2: Implement `publish.py` + assets** (`deps`: facts export needs ALL fact partitions → use `dg.AllPartitionMapping` on the dep as in `sweden_financial/assets.py` derived wave).
- [ ] **Step 3: Run the three test files + `uv run dg check defs`** → PASS. Commit: `feat(dagster): esef clickhouse exports + gleif entity map`.

---

### Task 6: IFRS metrics derived in ClickHouse

**Files:**
- Create: `src/dagster_v3/defs/esef_filings/metrics.py`
- Modify: `src/dagster_v3/defs/esef_filings/assets.py` (derived asset)
- Test: `tests/test_esef_filings_metrics.py`

**Interfaces:**
- Consumes: `corpscout.esef_facts`, `corpscout.esef_filings`, `corpscout.exchange_rates`.
- Produces: asset `esef_financial_metrics_clickhouse` building `corpscout.esef_financial_metrics` fully in CH (stage+EXCHANGE, shrink guard), one row per filing (`fxo_id`). `MAPPING_VERSION = "esef-ifrs-v1"`. Concept mapping (data constant `IFRS_METRIC_CONCEPTS` in `metrics.py`):

```python
IFRS_METRIC_CONCEPTS: dict[str, tuple[str, ...]] = {
    "revenue": ("ifrs-full:Revenue", "ifrs-full:RevenueFromContractsWithCustomers"),
    "operating_profit": ("ifrs-full:ProfitLossFromOperatingActivities",),
    "profit_loss": ("ifrs-full:ProfitLoss",),
    "total_assets": ("ifrs-full:Assets",),
    "equity": ("ifrs-full:Equity",),
    "liabilities": ("ifrs-full:Liabilities",),
    "cash": ("ifrs-full:CashAndCashEquivalents",),
    "employees": ("ifrs-full:AverageNumberOfEmployees",),
}
```

- Selection rules (encode in one SQL statement, mirror `sweden_financial/metrics.py` style): undimensioned facts only (`dimensions = ''`); current period only (duration facts: `period_start..period_end` matching the filing's fiscal year, instant facts: `period_instant = period_end`); when a concept repeats, prefer highest `decimals` then deterministic `argMax(..., (decimals, fact_id))`; first concept in the tuple wins over fallbacks (`coalesce` of `argMaxIf` per concept in declared order); `liabilities` falls back to `total_assets - equity` when unmapped. USD via `corpscout.exchange_rates` keyed on `period_end` (join pattern from sweden metrics SQL — currency-aware, `fx_source`/`fx_rate_date` recorded; facts are multi-currency: EUR/SEK/NOK/GBP/CHF/DKK…).
- `scope` literal `'consolidated_ifrs'` on every row; `fiscal_year = toYear(period_end)`; `country`/`entity_name`/`viewer_url` joined from `esef_filings`.

- [ ] **Step 1: Failing tests** — SQL contains scope literal, all eight concept groups, the balance-equation fallback, deterministic tiebreak tuple (never bare `argMax(x, decimals)`), `mutations`-free (pure INSERT SELECT into stage), explicit column list matching migration order.
- [ ] **Step 2: Implement**; register leaf in `clickhouse_checks.py`.
- [ ] **Step 3: Run tests + `uv run dg check defs`** → PASS. Commit: `feat(dagster): esef IFRS metrics derived table`.

---

### Task 7: Jobs, schedule, design doc

**Files:**
- Modify: `src/dagster_v3/defs/esef_filings/assets.py` (jobs + schedule)
- Create: `src/dagster_v3/defs/esef_filings/docs/esef_filings-design.md`
- Modify: `docs/esef-filings-research.md` (status header → "implemented, see module design doc"), `docs/finland-data-sources.md` ("Planned" section: ESEF → live), `docs/sweden-data-sources.md` (roadmap item 9 note: ESEF module live, SE consumption via entity map)

**Interfaces:**
- `esef_filings_refresh_job` = index + current-year facts partition + current-year report-XHTML archive (Task 9's `esef_report_xhtml_s3`) + the three exports + metrics (selection via `AssetSelection.assets(...)`; the facts asset is partitioned — job targets the partition matching `toYear(now)` via a schedule-time partition fn, mirroring `sweden_financial_current_year_weekly`'s resolver).
- `esef_filings_backfill_job` = facts partitions for UI-launched backfill (2019–2027) with exports EXCLUDED (run exports once after all partitions land, like sweden).
- Schedule `esef_filings_refresh_weekly`: `50 5 * * 0` Europe/Belgrade (staggered from every existing source), `DefaultScheduleStatus.STOPPED` until first validated live run (Task 8 flips it in the UI).
- Design doc: as-built rewrite of the research doc following `docs/source-design-doc-template.md` (source, resource, assets, jobs, scope flag, matching, v1 skips: Arelle fallback, `company_financials_latest` integration).

- [ ] **Step 1:** Jobs + schedule + `uv run dg check defs` PASS.
- [ ] **Step 2:** Write the design doc; update the three docs.
- [ ] **Step 3:** Commit: `feat(dagster): esef jobs/schedule + as-built design doc`.

---

### Task 8: Server backfill + validation (controller-led, like SI Task 3)

No new code. Sequence (controller runs it, GraphQL launch pattern):

- [ ] **Step 1:** Confirm user ran migration 000149 (`schema_migrations` at 149, clean).
- [ ] **Step 2:** Deploy via light_sync (clean tree — commit everything first).
- [ ] **Step 3:** Materialize `esef_filings_index_duckdb` on the server; record `meta.count` vs stored rows; record per-country distribution and the **json_url coverage fraction** (answers research open question 2; if <90% have JSON, log the gap prominently in the run report — the Arelle fallback decision goes back to the user with real numbers).
- [ ] **Step 4:** UI-launched backfill of `esef_filing_facts_duckdb` partitions 2019→2027 (multi_run policy throttles; expect ~25k downloads total on first pass, hours — monitor with the background poll pattern).
- [ ] **Step 5:** Run exports + entity map + metrics. Validate: `esef_filings` ≈ 25k rows; FI filings ≈ 1.17k; **`SELECT count(DISTINCT m.lei) FROM esef_financial_metrics m JOIN esef_entity_registry_map e ON m.lei=e.lei WHERE e.country_iso2='FI'`** — target: majority of the 298 listed FI companies now have consolidated financials (measure and record the exact number); spot-check one household name per FI/SE (e.g. Nokia `0112038-9`, Ericsson) against published annual-report figures.
- [ ] **Step 6:** Enable `esef_filings_refresh_weekly` in the UI; append ledger entries; report coverage numbers + open items (redistribution-terms confirmation — user decision before any external exposure; backoffice consumption is the follow-up plan).

---

### Task 9: Report XHTML archive to S3 (added 2026-07-20 mid-execution, user request; runs between Tasks 5 and 6 in execution order)

**Files:**
- Modify: `src/dagster_v3/defs/esef_filings/assets.py` (new asset)
- Test: `tests/test_esef_filings_assets.py` (extend)

**Interfaces:**
- Consumes: `esef_filings.filings_index` (Task 3), `EsefFilingsClient.download_json_facts` (Task 2 — the download helper is URL-agnostic streamed GET with retries; reuse it for XHTML).
- Produces: asset `esef_report_xhtml_s3`, same `StaticPartitionsDefinition` 2019–2027 keyed by `toYear(period_end)`, `deps=["esef_filings_index_duckdb"]`, `pool="esef_filings_duckdb"` (opens the DuckDB file read-only for scope resolution — pool required per CLAUDE.md), `BackfillPolicy.multi_run(1)`. Per partition run: select the year's filings from the local index; for each with a non-null `report_url`: download the filing's rendered XHTML to S3 `esef_filings/report_xhtml/fxo_id=<fxo_id>/report.xhtml` in bucket `source-esef-filings`, skip when the object already exists (fxo_id is version-stable — verified live). No parsing. Metadata: `filings_in_scope`, `downloaded_count`, `reused_count`, `skipped_no_report_url`, `skipped_out_of_range`.
- Purpose: archive the full human-readable report documents now, as the corpus for a **future pass** building text extraction → embeddings → LLM-assisted search over annual reports (separate plan; see deferred). Volume estimate ~25k filings × 2–8 MB ≈ 50–200 GB on RustFS — flagged to the user.
- Jobs wiring (Task 7 must include): the archive asset joins the backfill job's partitioned set and the weekly refresh job's current-year selection.

- [ ] **Step 1: Failing asset test** — stub client+object store; partition run downloads only missing XHTMLs for its year, skips existing, counts filings without report_url; second run downloads nothing.
- [ ] **Step 2: Implement the asset** (mirror the facts asset's scope resolution; all fallible work before/outside the write path — this asset never writes DuckDB, read-only connection only).
- [ ] **Step 3: Run** `uv run pytest tests/test_esef_filings_assets.py -q` + `uv run dg check defs` → PASS. Commit: `feat(dagster): esef report xhtml archive to s3`.

---

## Explicitly deferred (log in ledger, do not build)

- **Embeddings + LLM search over archived report XHTML** (user, 2026-07-20): next pass after this plan — text extraction from the S3 XHTML corpus, embedding generation (existing embedder infra: Qwen3-Embedding-8B), vector search + LLM answering over annual-report content. Needs its own brainstorm/plan; the Task 9 archive is its data prerequisite.

- Arelle/package fallback for filings without `json_url` (decide after Task 8 coverage numbers).
- `company_financials_latest` consuming ESEF rows (scope-preference rule first).
- Backoffice "Group (IFRS consolidated)" section — separate follow-up plan once data is live (mirrors the public-contracts generic-section pattern; needs `consolidatedFinancialsQuery` per country joining `esef_financial_metrics` × `esef_entity_registry_map`).
- Dual-listed dedup rule (one LEI filing in two countries) — v1 keeps both filings; metrics are per-filing so nothing double-counts until a cross-country aggregate exists.
- Registry-id normalizers beyond FI/SE.
