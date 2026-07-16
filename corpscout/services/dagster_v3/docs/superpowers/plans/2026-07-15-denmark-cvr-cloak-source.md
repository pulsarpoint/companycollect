# Denmark CVR Cloak Source Implementation Plan

> **Superseded partition design (2026-07-16):** the original one-character substring partitions below were replaced after live testing proved DataCVR caps accessible results at 3,000. The current implementation uses completed monthly partitions from January 2015, a generic monthly count, and a fixed list of region/municipality filters for oversized months. Each run writes one merged complete or incomplete JSON object. The current source contract is documented in `src/dagster_v3/defs/denmark_cvr/docs/denmark_cvr-design.md`.

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate the browser-backed CVR search behavior from the standalone `cloack` prototype into `dagster_v3` as a Denmark source resource and one search-term-partitioned raw asset.

**Architecture:** `DenmarkCvrSearchResource` owns the CloakBrowser lifecycle, browser-session bootstrap, paginated POST requests, response validation, and polite request delay. The single `denmark_cvr_search_results_s3` asset maps each static partition to a one-character substring search term, writes every validated raw response page to a run-scoped object-store prefix, and writes a manifest only after the partition completes. Overlap is expected; the next DuckDB phase will normalize and deduplicate entity rows. This first slice intentionally stops at durable raw JSON: no dlt/DuckDB staging, normalization, ClickHouse migration, schedule, contacts derivation, translation, or currency work.

**Tech Stack:** Dagster `ConfigurableResource`, `StaticPartitionsDefinition`, `BackfillPolicy.multi_run`, CloakBrowser, Pydantic v2 discriminated unions, the shared `ObjectStoreResource`, pytest, Ruff, and `dg`.

## Design decisions locked in

- **Module identity:** use `defs/denmark_cvr/`, not the prototype typo `cloack`; retain `cloakbrowser` only as the dependency/import name.
- **Partition contract:** use lowercase static keys `0`-`9`, `a`-`z`, `æ`, `ø`, and `å`. Each key is passed as the CVR free-text substring search term. Results can occur in multiple partitions and are deliberately not deduplicated in this raw phase.
- **Search-semantics gate:** the bounded live probe confirmed substring semantics. The user accepted overlapping raw partitions and assigned normalization/deduplication to the next DuckDB phase.
- **Raw fidelity:** store the exact response body returned by CVR after it passes `SearchResponse` validation. Preserve all three discriminated entity types: `virksomhed`, `person`, and `produktionsenhed`.
- **Durable boundary:** use S3-compatible object storage because each partition is a raw browser/API retrieval. Do not introduce DuckDB merely as a marker table and do not create an imperative local `data/` output directory in production code.
- **Object layout:** write pages under `denmark_cvr/search/search_term=<partition>/run_id=<run_id>/page=<zero-padded-index>.json` and the completion manifest beside them. A run-scoped prefix prevents concurrent or retried runs from overwriting prior evidence.
- **Failure behavior:** non-2xx and schema-invalid responses fail the asset. A schema-invalid raw body is written to an `.invalid.json` object before raising, without putting the body, names, addresses, emails, phones, cookies, or browser state in logs or exception messages.
- **Pagination:** stop when the response has no entities or when the page range reaches `response.total`; refuse to report success when the first page is empty but advertises a positive total.
- **Concurrency:** use `BackfillPolicy.multi_run(max_partitions_per_run=1)` and `pool="denmark_cvr_search"`. A full backfill therefore creates one throttled run per search term rather than one browser-heavy run containing all partitions.
- **No schedule/job yet:** the user requested one asset for now. Manual partition materialization/backfill is the operator entry point.
- **No custom interface/facade layer:** the concrete Dagster resource owns the concrete CloakBrowser behavior. Tests use a small fake at the browser/page protocol edge through explicit launcher/sleep arguments.
- **Prototype retirement:** copy/migrate behavior first. The standalone `/Users/graovic/pulsarpoint/ppoint/cloack` project has uncommitted and untracked work plus nested `.git` history, so preserve it rather than deleting user state during this migration.

## Global constraints

- Work from `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3`; all repository paths below are relative to it.
- Use `uv run` for Python, pytest, Ruff, and Dagster commands.
- Do not add `from __future__ import annotations` to `assets.py`; Dagster must see the real `AssetExecutionContext` type.
- Do not log raw bodies or entity fields. Logs and materialization metadata may contain only partition key, page index, aggregate counts, object keys, and byte counts.
- Keep defaults at the Dagster resource/operator boundary. Internal page/result models carry explicit values.
- Preserve the unrelated existing modification in `tests/test_clickhouse_migrations.py`.
- Commit or stage only explicit Denmark/CVR paths; never use `git add -A`.

## File structure

- Create `src/dagster_v3/defs/denmark_cvr/__init__.py` — package marker only; no re-exports.
- Create `src/dagster_v3/defs/denmark_cvr/models.py` — migrated Pydantic response models.
- Create `src/dagster_v3/defs/denmark_cvr/resources.py` — concrete CloakBrowser search resource, response-page value, payload builder, and safe errors.
- Create `src/dagster_v3/defs/denmark_cvr/assets.py` — partition definition, object-key/manifest helpers, one raw S3 asset, and local `Definitions` resource registration.
- Create `src/dagster_v3/defs/denmark_cvr/docs/denmark_cvr-design.md` — mandatory source design record for this deliberately raw-only slice.
- Create `tests/test_denmark_cvr.py` — model, resource, storage, partition, and definitions tests.
- Modify `pyproject.toml` and `uv.lock` — add the compatible CloakBrowser dependency.
- Preserve `/Users/graovic/pulsarpoint/ppoint/cloack`; it contains uncommitted/untracked user work and nested Git history.

---

### Task 0: Baseline and source-semantics gate

**Files:** none modified.

- [ ] **Step 1: Confirm the current Dagster definitions baseline**

Run:

```bash
uv run dg check defs
```

Expected: exit 0. If it fails for a pre-existing reason, record the failure before changing Denmark code.

- [ ] **Step 2: Confirm the focused test namespace does not already exist**

Run:

```bash
uv run pytest tests/test_denmark_cvr.py -q
```

Expected: pytest reports that the file does not exist.

- [ ] **Step 3: Verify CloakBrowser under the project Python version**

Run the dependency update early:

```bash
uv add 'cloakbrowser>=0.4.10'
uv run python -c 'from cloakbrowser import launch; print(launch.__module__)'
```

Expected: dependency resolution and import both succeed under Python 3.14. If CloakBrowser cannot resolve/import, stop and report the compatibility blocker instead of building an un-runnable resource.

- [x] **Step 4: Run a bounded live search-semantics probe**

Use the migrated payload shape in a temporary one-page probe (page size 5) for at least `a`, `æ`, and `0`. Record whether every returned `senesteNavn` begins with the requested character after `strip().casefold()`.

Observed: DataCVR uses substring semantics. Raw partitions intentionally overlap; future DuckDB normalization will deduplicate by source identifiers.

---

### Task 1: Migrate and test the CVR response models

**Files:**

- Create `src/dagster_v3/defs/denmark_cvr/__init__.py`
- Create `src/dagster_v3/defs/denmark_cvr/models.py`
- Create `tests/test_denmark_cvr.py`

- [ ] **Step 1: Write failing model tests**

Add representative JSON fixtures for each discriminator value and tests that prove:

- `SearchResponse.model_validate_json()` selects `CompanySearchResult`, `PersonSearchResult`, and `ProductionUnitSearchResult` correctly.
- camelCase CVR fields populate the snake_case model fields.
- `ophoersDato: ""` becomes `None` for companies and production units.
- totals reject negative values.
- unknown `enhedstype` values fail validation.

Use only synthetic names, addresses, emails, and phone numbers in fixtures.

- [ ] **Step 2: Run the tests and confirm the expected failure**

Run:

```bash
uv run pytest tests/test_denmark_cvr.py -k model -v
```

Expected: import failure because `dagster_v3.defs.denmark_cvr.models` does not exist.

- [ ] **Step 3: Port the models**

Move the model behavior from `/Users/graovic/pulsarpoint/ppoint/cloack/models.py` into `models.py` with these canonical symbols:

- `CompanySearchResult`
- `PersonSearchResult`
- `ProductionUnitSearchResult`
- `SearchResultUnit`
- `SearchResponse`

Keep the Pydantic discriminated union on `enhedstype` and the empty cessation-date normalization. Use absolute imports only and do not add re-exports in `__init__.py`.

- [ ] **Step 4: Run the model tests**

Run:

```bash
uv run pytest tests/test_denmark_cvr.py -k model -v
```

Expected: all model tests pass.

---

### Task 2: Implement the concrete CloakBrowser search resource

**Files:**

- Create `src/dagster_v3/defs/denmark_cvr/resources.py`
- Modify `tests/test_denmark_cvr.py`

**Resource API:**

```python
class DenmarkCvrSearchResource(dg.ConfigurableResource):
    search_base_url: str = "https://datacvr.virk.dk"
    page_size: int = 100
    min_delay_ms: int = 100
    max_delay_ms: int = 800

    def iter_search_pages(
        self,
        search_term: str,
        *,
        start_page_index: int = 0,
        launcher: Callable[[], Any] = launch,
        sleep: Callable[[float], None] = time.sleep,
    ) -> Iterator[DenmarkCvrSearchPage]: ...
```

`DenmarkCvrSearchPage` is a frozen dataclass containing explicit `page_index`, `raw_body`, `response`, `status`, and response-header metadata needed for audit/debugging. Do not expose a second service or protocol abstraction.

- [ ] **Step 1: Write failing pure payload/URL tests**

Assert that:

- `build_search_payload("æ", page_index=2, size=100)` matches the prototype CVR request contract, including `sideIndex` as a string.
- `search_results_url("æ", page_index=2, size=100)` URL-encodes the term and includes the page/size query parameters.
- empty search terms, negative page indices, non-positive page sizes, and invalid delay ranges raise `ValueError` with safe messages.

- [ ] **Step 2: Write failing browser-boundary tests**

Build small concrete fakes for `browser`, `page`, and `page.evaluate` in the test file. Verify that the resource:

- launches one browser and closes it in `finally` on success and failure;
- navigates to the public search-result URL before using the same-page `/gateway/soeg/fritekst` fetch;
- sends sequential page indices and the configured page size;
- yields validated pages without changing the exact raw JSON body;
- stops at `response.total` without requesting an unnecessary extra page;
- also stops on an empty result page when total is zero;
- raises a safe request error for a non-2xx result without embedding the body;
- raises a dedicated validation error that retains the invalid raw body as an attribute but not in its message;
- invokes the injected delay only between requests, not after the final page.

- [ ] **Step 3: Run the resource tests and confirm failure**

Run:

```bash
uv run pytest tests/test_denmark_cvr.py -k 'payload or url or resource or browser' -v
```

Expected: failures because the resource module and helpers do not exist.

- [ ] **Step 4: Implement the resource directly**

Port the relevant `cloack/main.py` behavior into the concrete resource:

- build the bootstrap URL;
- `launch()` and `browser.new_page()`;
- `page.goto(..., wait_until="networkidle")`;
- execute the same credentialed JSON POST in `page.evaluate`;
- validate with `SearchResponse.model_validate_json`;
- return typed pages;
- close the browser in `finally`.

Keep the browser-evaluation JavaScript as one named module constant so the request contract is visible and testable. Do not print response headers or bodies; the asset boundary will provide structured aggregate logging.

- [ ] **Step 5: Run the resource tests**

Run:

```bash
uv run pytest tests/test_denmark_cvr.py -k 'payload or url or resource or browser' -v
```

Expected: all resource tests pass.

---

### Task 3: Add raw-object layout and manifest behavior

**Files:**

- Create `src/dagster_v3/defs/denmark_cvr/assets.py`
- Modify `tests/test_denmark_cvr.py`

**Storage API:**

- `DENMARK_CVR_BUCKET = "source-denmark-cvr"`
- `page_object_key(search_term, run_id, page_index) -> str`
- `invalid_page_object_key(search_term, run_id, page_index) -> str`
- `manifest_object_key(search_term, run_id) -> str`
- `write_denmark_cvr_search_partition(...) -> DenmarkCvrPartitionSummary`

- [ ] **Step 1: Write failing object-key tests**

Assert stable keys, including:

```text
denmark_cvr/search/search_term=æ/run_id=test-run/page=000000.json
denmark_cvr/search/search_term=æ/run_id=test-run/page=000000.invalid.json
denmark_cvr/search/search_term=æ/run_id=test-run/manifest.json
```

Reject path separators, blank run IDs, unsupported search terms, and negative page indices before any object-store call.

- [ ] **Step 2: Write failing storage-boundary tests**

Use an in-memory fake object store implementing only `ensure_bucket`, `write_bytes`, and `write_json`. Use a fake concrete search resource that yields typed `DenmarkCvrSearchPage` values. Prove that `write_denmark_cvr_partition`:

- writes each exact raw page body to the expected bucket/key;
- writes the manifest only after all pages succeed;
- records source URL, search term, run ID, retrieval timestamp, page keys, page count, entity count, and company/person/production-unit totals;
- returns the same aggregate counts for materialization metadata;
- writes an invalid response to the `.invalid.json` key and does not write a completion manifest;
- never places entity data in the log callback.

- [ ] **Step 3: Run the storage tests and confirm failure**

Run:

```bash
uv run pytest tests/test_denmark_cvr.py -k 'object_key or partition_storage or invalid_response' -v
```

Expected: failures because storage helpers are not implemented.

- [ ] **Step 4: Implement storage as semantic functions**

Keep object-key construction, manifest construction, and the asset wrapper separate. The storage function accepts all runtime inputs explicitly and returns a frozen summary; it does not reach into Dagster context. This makes raw persistence testable without materializing a Dagster run.

Write page objects as they arrive so a late failure still leaves page-level evidence. Write `manifest.json` last as the completion marker. Do not write a mutable `latest.json` pointer.

- [ ] **Step 5: Run the storage tests**

Run:

```bash
uv run pytest tests/test_denmark_cvr.py -k 'object_key or partition_storage or invalid_response' -v
```

Expected: all storage tests pass.

---

### Task 4: Define and register the partitioned Dagster asset

**Files:**

- Modify `src/dagster_v3/defs/denmark_cvr/assets.py`
- Modify `tests/test_denmark_cvr.py`

- [ ] **Step 1: Write failing asset-definition tests**

Assert that:

- `DENMARK_CVR_SEARCH_PARTITIONS` is a `StaticPartitionsDefinition` with exactly `0`-`9`, `a`-`z`, `æ`, `ø`, `å`, without duplicates;
- `denmark_cvr_search_results_s3.partitions_def` is that exact definition;
- the asset name is noun-based, group is `denmark_cvr`, and kinds include `python`, `browser`, `json`, and `s3`;
- backfill policy is multi-run with one partition per run;
- pool is `denmark_cvr_search`;
- the asset function passes `context.partition_key`, `context.run_id`, and `context.log.info` into the storage function and returns `MaterializeResult` metadata with counts and manifest location;
- local `defs` registers only this asset plus `DenmarkCvrSearchResource` under `denmark_cvr_search`; it relies on the already-shared `object_store` resource rather than duplicating the binding.

- [ ] **Step 2: Run the asset tests and confirm failure**

Run:

```bash
uv run pytest tests/test_denmark_cvr.py -k 'partitions or asset_definition or asset_materialization or definitions' -v
```

Expected: failures because the asset/definitions are incomplete.

- [ ] **Step 3: Implement the one asset**

Create `denmark_cvr_search_results_s3` with:

- `group_name="denmark_cvr"`;
- `partitions_def=DENMARK_CVR_SEARCH_PARTITIONS`;
- `backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1)`;
- `pool="denmark_cvr_search"`;
- a description stating the partition grain and preserved entity types;
- injected `DenmarkCvrSearchResource` and `ObjectStoreResource` parameters;
- `MaterializeResult` metadata for bucket, manifest key, search term, page count, entity count, and per-type totals.

Register it in a module-level `defs = dg.Definitions(...)`. Do not add a job or schedule.

- [ ] **Step 4: Run all Denmark tests**

Run:

```bash
uv run pytest tests/test_denmark_cvr.py -v
```

Expected: all tests pass.

---

### Task 5: Write the mandatory Denmark source design doc

**Files:**

- Create `src/dagster_v3/defs/denmark_cvr/docs/denmark_cvr-design.md`

- [ ] **Step 1: Fill the source design template with verified facts**

Document:

- Virk/DataCVR search page and `/gateway/soeg/fritekst` browser-session dependency;
- JSON response shape and the three entity types;
- chosen search-term partitioning, substring-semantics evidence, page size, delay, and one-partition-per-run policy;
- raw S3 bucket/key/manifest shape;
- why dlt/DuckDB/ClickHouse are deferred rather than represented by empty marker assets;
- company key (`cvr`), person/production-unit keys (`enhedsnummer`/`pNummer`), and which totals are exposed;
- contact fields observed (`email`, `telefonnummer`) and that canonical contact/domain extraction is deferred to the normalization slice;
- Danish fields requiring future static/LLM translation analysis; proper names/addresses remain untranslated;
- no financial amounts in this slice, so currency conversion is not applicable yet;
- no schedule in the first slice;
- privacy/logging constraints for person, address, email, and phone data;
- the configured one-character search-term set and expected overlap between raw partitions.

- [ ] **Step 2: Review every deviation explicitly**

The doc must state that this is a browser-gateway raw capture rather than the repository's full dlt → DuckDB → ClickHouse golden path, and that the deviation is temporary and bounded by the user's requested first slice.

---

### Task 6: Verification and prototype preservation

**Files:**

- All Denmark files above
- `pyproject.toml`
- `uv.lock`
- Preserve `/Users/graovic/pulsarpoint/ppoint/cloack` because it contains user-owned uncommitted work

- [ ] **Step 1: Format and lint only touched Python files**

Run:

```bash
uv run ruff format src/dagster_v3/defs/denmark_cvr tests/test_denmark_cvr.py
uv run ruff check src/dagster_v3/defs/denmark_cvr tests/test_denmark_cvr.py
```

Expected: both commands exit 0.

- [ ] **Step 2: Run the focused suite**

Run:

```bash
uv run pytest tests/test_denmark_cvr.py -v
```

Expected: all Denmark tests pass.

- [ ] **Step 3: Validate discovered definitions**

Run:

```bash
uv run dg check defs
uv run dg list defs --json
```

Expected: definitions validation succeeds and the listing contains `denmark_cvr_search_results_s3` with the expected partition definition/resource requirements.

- [ ] **Step 4: Run repository contract tests affected by the new asset**

Run:

```bash
uv run pytest tests/test_backfill_policy_contracts.py tests/test_heavy_job_tags.py tests/test_schedule_cron_contracts.py -q
```

Expected: all tests pass; no schedule contract changes are introduced.

- [ ] **Step 5: Perform one manual smoke materialization**

Materialize one low-volume search term from Dagster after confirming the object-store environment. Inspect only aggregate metadata and object keys; do not paste raw person/company payloads into logs or the handoff.

Expected: page objects and one final manifest exist under the run-scoped prefix, and counts match the manifest.

- [ ] **Step 6: Check the diff without touching unrelated work**

Run:

```bash
git status --short
git diff --check -- pyproject.toml uv.lock src/dagster_v3/defs/denmark_cvr tests/test_denmark_cvr.py docs/superpowers/plans/2026-07-15-denmark-cvr-cloak-source.md
```

Expected: no whitespace errors; the pre-existing `tests/test_clickhouse_migrations.py` modification remains untouched.

- [x] **Step 7: Preserve the standalone prototype safely**

Inspection found a nested Git repository with uncommitted and untracked prototype work and no configured remote. Leave it untouched. Do not copy its `.git`, `.venv`, caches, local `data/`, or lockfile into `dagster_v3`.

---

## Completion criteria

- The Dagster definitions load with exactly one new Denmark raw asset.
- Every configured search term is a visible materializable partition and full backfills are throttled one partition per run.
- The concrete resource reproduces the prototype's browser-session POST and pagination behavior.
- Raw responses are schema-validated, persisted exactly, and completed by an auditable manifest.
- Invalid/non-2xx data cannot be mistaken for a successful partition.
- No raw entity data, cookies, response bodies, or browser state are logged.
- No DuckDB/ClickHouse/schedule abstractions are introduced before the next requested slice.
- The standalone prototype remains preserved because its nested repository contains uncommitted user work.
