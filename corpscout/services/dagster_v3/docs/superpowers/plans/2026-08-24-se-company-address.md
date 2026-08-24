# Sweden Company Address Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `address` datatype on the `se_company` machinery the info pilot proved: two artifact tables (`se_company_address_bolagsverket`, `se_company_address_scb`) with the standard envelope, one merged final `se_company_address` with SEVERAL rows per company, geocode augmentation from the existing shared-identity chain, a correction ledger + sensor, and a backoffice Address tab that reviews the final. **No model anywhere in this datatype** — no observation table, no `LlmProfileConfig`, no model columns.

**Architecture:** `sweden_company_addresses_clickhouse` (existing source layer) → `corpscout.se_company_addresses_current` → artifact assets in `se_company/bolagsverket.py` and `se_company/scb.py` (groups `se_company_bolagsverket` / `se_company_scb`, append-only versions keyed by evidence hash, `observed_at` = append time) → `se_company/address.py` final asset (group `se_company`): changed companies → normalize each artifact row → group by `address_key` → copy every field from its owning source in precedence order `bolagsverket › scb` → augment with the geocode the existing chain already computed → **set replacement** (keys that disappeared are republished `is_current = false`) → ledger corrections → stage/validate/publish with provenance. Merge rules are pure functions in `se_company/address_rules.py`. Backoffice: validator + writer + server module + Address-tab corrections UI + a ledger list page, all mirroring the info corrections stack.

**Tech Stack:** ClickHouse 26.5 (golang-migrate SQL under `corpscout/clickhouse/migrations/`), Dagster 1.13 (`uv run`), pytest + clickhouse-local harness, React Router 8 + vitest in `corpscout/services/backoffice`.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-08-23-se-company-address-design.md` (binding). Structural facts about the existing address tables/assets/geocoder: `.superpowers/sdd/2026-08-22-se-company-info-pilot/address-pipeline-map.md`. Pattern reference (do not re-derive, read): `defs/se_company/{common.py,scb.py,info_rules.py,info.py}`, `tests/se_company_ddl.py`, `tests/test_se_company_info_clickhouse_local.py`, migrations 000297–000306.

## Global Constraints

- **Naming.** Artifact tables `se_company_address_<source>` with assets `<table>_clickhouse` in group `se_company_<source>`; final `se_company_address`, asset `se_company_address_clickhouse`, group `se_company`; ledger `se_company_address_correction`. No observation table exists for this datatype and none is created.
- **Envelope.** Every artifact table starts with `company_id String, source_record_uid String, observed_at DateTime64(3,'UTC'), source_run_id String, evidence_hash FixedString(64) MATERIALIZED …`, then the source's typed payload; `ENGINE = ReplacingMergeTree(observed_at) ORDER BY (company_id, source_record_uid)`; `CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')` (10-digit orgnr and 12-digit sole-trader ids, as migration 000299 settled for info).
- **`observed_at` is APPEND time** — `now64(3, 'UTC')` inside each artifact SELECT, never a register stamp. `se_company_addresses_current.updated_from_raw_at` is one constant for the whole weekly bulk load and is older than every `resolved_at`; a version appended under it would never look newer than the row it replaces and the final's change scan would never select the company again. The info pilot learned this the hard way (see `scb.py`'s comment).
- **Publish is anti-join.** Artifacts use `publish_with_stage(..., new_versions_only=True)` as in `common.py`: the stage is `CREATE TABLE stage AS target`, so the target's MATERIALIZED `evidence_hash` is computed **by ClickHouse on the stage** and never re-expressed in Python; the copy is a `LEFT ANTI JOIN` on `(company_id, source_record_uid, evidence_hash)`. The final publishes with `new_versions_only=False` (a new version per resolution is the point).
- **Set replacement is the one mechanism `info` does not have.** Re-resolving a company recomputes its full address set: keys it produces are published `is_current = true`; keys that were current in the previous resolution and are no longer produced are republished `is_current = false` (a versioned tombstone carrying the previous row's own provenance, so `has_evidence` still holds). Readers always filter `FINAL … WHERE is_current`. **This needs harness proof:** an address disappearing from a source publishes a tombstone, and reappearing publishes `is_current = true` again (Task 4 proves the decision, Task 6 proves the storage round-trip).
- **Change detection.** A company is re-resolved when: it has never been published; **or** an artifact's `max(observed_at)` is newer than its published `resolved_at`; **or** the ledger gained a row after it; **or** the geocode snapshot (`max(se_address_geocodes_current.matched_at)` over the company's linked identities) is newer than its published `resolved_at`. Plus `resolve_all` / `resolve_all_before` for rules-only changes, with the same cutoff semantics as `SECompanyInfoConfig` (the cutoff is ALWAYS bound — `parseDateTime64BestEffort` is parsed whether or not the flag is on).
- **`execute` gate.** The asset writes nothing unless the run config says `execute: true`. A bare "Materialize" click in the Dagster UI is a preview that runs the change scan and reports what a real run would do (selection reasons, per-reason counts, `stopped_at_cap`). The schedule and the sensor both spell `execute: true` out.
- **ClickHouse 26.5 rules.** Project columns explicitly, never `SELECT alias.*` after a second `USING` join. Guard every LEFT-JOIN miss of a **Nullable** column with `ifNull(...)`; gate every **non-Nullable** joined column behind a hit flag computed as `ifNull(<run id column>, '') != ''` — a bare `!= ''` is NULL under `join_use_nulls = 1`, and a bare read is the column's *type default* (0, 1970-01-01) under `join_use_nulls = 0`, which is exactly the Task 18 lesson the backoffice address query already carries in its own comment. Named parameters only (`%(name)s` in Dagster SQL, `{name:Type}` in backoffice SQL). No `;` inside `--` comments.
- **Executed-SQL harness is a phase gate.** Every SQL constant this plan adds runs in `clickhouse-local` on the Docker 26.5 image, under BOTH `join_use_nulls = 0` and `join_use_nulls = 1`, and both settings must answer identically. Substring tests do not close phase 3.
- **Dagster.** No `from __future__ import annotations`; `uv run` for every command from `corpscout/services/dagster_v3`; `uv run dg check defs` green and `uv run ruff check` clean before each commit.
- **Migrations.** First line `CREATE DATABASE IF NOT EXISTS corpscout;` (grant-only migrations exempt), a `.down.sql` twin for every migration, registered in `EXPECTED_MIGRATIONS` / `EXPECTED_ACCESS_MIGRATIONS` in `tests/test_clickhouse_migrations.py`, and compatible with the DDL replay in `tests/se_company_ddl.py` (one clause per line; `ADD COLUMN … AFTER <column>` ends its line). Next numbers: **000307** (tables), **000308** (grants), and for retirements (phase 8 only) the next free number (000312 at time of writing — esef took 000309–000311; re-check `ls corpscout/clickhouse/migrations | tail -1` before creating).
- **Backoffice.** Named ClickHouse params only; route components never import values from `~/lib/*.server`; gates before each commit are `pnpm typecheck`, `npx react-router build`, and `rg -l clickhouse build/client` returning nothing. Run vitest **filtered to the files this plan touches** (`npx vitest run tests/<file> …`) — never unfiltered. `app/routes.ts`, `app/components/admin/admin-sidebar.tsx` and `app/routes/admin-layout.tsx` are tracked (committed cd1fcc00): edit AND commit them by explicit path like any other file.
- **Commits** by explicit path only (the shared tree carries unrelated uncommitted work), with the trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- **Production is controller-only.** Applying migrations, deploying Dagster, launching backfills, starting the sensor/schedule and applying the retirement migration are Tasks 10a–10e and are marked **(controller)**. An implementing agent stops at the end of its phase and reports.
- **Out of scope, by design:** other countries; the `address_resolution` / golden-corpus geocoder itself; Lantmäteriet; the public company page's address section (switch-over is a later, separate decision, same rule as descriptions); the legacy per-company geocoder pair (`se_company_address_geocodes`, `se_company_address_geocode_results`) is explicitly **NOT** dropped by this plan — the owner decides that separately.

## Phases (execute one at a time; each ends in a verifiable, stoppable state)

| phase | tasks | deliverable | stop/verify |
|---|---|---|---|
| **1 — Tables** | Task 1 | migrations 000307/000308 + DDL contract tests committed | `uv run pytest tests/test_se_company_address_layout.py tests/test_se_company_layout.py tests/test_clickhouse_migrations.py` green |
| **2 — Apply migrations** *(controller)* | Task 10a | 000307/000308 applied on the ClickHouse host | four `se_company_address*` tables exist; INSERT grant visible |
| **3 — Assets** | Tasks 2–6 | `bolagsverket.py`, `scb.py`'s second asset, `address_rules.py`, `address.py`, jobs, sensor (STOPPED), weekly schedule (STOPPED), harness | all `test_se_company_address*` green, `dg check defs` green, harness green on Docker under both `join_use_nulls` settings |
| **4 — Deploy** *(controller)* | Task 10b | Dagster host synced and reloaded | groups `se_company_bolagsverket` / `se_company_scb` / `se_company` visible; sensor + schedule present and STOPPED |
| **5 — Initial load** *(controller)* | Task 10c | artifacts backfilled, final resolved for every company | artifact counts equal the per-source source counts; final rows ≥ companies; geocode coverage ≈ 2.09M identities; a second run is quiet |
| **6 — Backoffice** | Tasks 7–8 | validator, writer, server module, Address tab with corrections, ledger list page | needs only phase 2; may run in parallel with phases 3–5 |
| **7 — Switch on + e2e** *(controller)* | Task 10d | sensor + weekly schedule RUNNING; override / reject / undo verified on a real company | closes the datatype |
| **8 — Retirements** *(controller-gated, LAST)* | Task 9 → Task 10e | retirement migration (the next free number — 000312 at time of writing — esef took 000309–000311; re-check `ls corpscout/clickhouse/migrations | tail -1` before creating) drops `se_company_addresses_canonical_current` and `se_company_address_display_current`; dbt model removed | each drop carries a fresh `rg` zero-reader proof and a row-count snapshot in the migration comment |

---

### Task 1: Migrations 000307/000308 with envelope contract tests

**Files:**
- Create: `corpscout/clickhouse/migrations/000307_corpscout_se_company_address.up.sql`, `.down.sql`
- Create: `corpscout/clickhouse/migrations/000308_corpscout_se_company_address_writer_grants.up.sql`, `.down.sql`
- Modify: `corpscout/services/dagster_v3/tests/se_company_ddl.py` (find the creating migration per table instead of reading one constant; add `address_artifact_tables()`)
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS` tail after `"000306_corpscout_se_company_info_legal_form_label"`, and `EXPECTED_ACCESS_MIGRATIONS`)
- Test: `corpscout/services/dagster_v3/tests/test_se_company_address_layout.py`

**Interfaces:**
- Produces: the four tables in 000307 and the grant in 000308. No Python registry of tables/columns: each asset module declares its own table name and insert list, and the tests read the migration file.
- Produces (tests, reused by Tasks 2–6): `tests/se_company_ddl.py` gains `ADDRESS_MIGRATION`, `_migration_for(table) -> str` and `address_artifact_tables() -> list[str]`; `table_block(table)` and `declared_columns(table)` keep their signatures and now work for any `se_company*` table.
- Consumes: nothing.

- [ ] **Step 1: Write the failing contract test**

```python
# tests/test_se_company_address_layout.py
"""The address datatype's DDL contract, read from the migration itself.

Mirrors tests/test_se_company_layout.py for the second datatype. What is pinned
here is the ENVELOPE and the final's provenance tail, not every column -- the
per-module tests (Tasks 2, 3, 5) pin each module's own insert list against
declared_columns(), so a column added in a later migration is picked up by the
replay rather than hand-copied into three places.
"""
import pytest

from tests.se_company_ddl import (
    ADDRESS_MIGRATION,
    ENVELOPE,
    MIGRATIONS_DIR,
    address_artifact_tables,
    declared_columns,
    table_block,
)

ADDRESS_FINAL_PROVENANCE = ("sources", "source_record_uids", "evidence_hashes",
                            "evidence_set_hash", "correction_ids", "source_run_id", "resolved_at")


def test_the_datatype_declares_two_artifact_tables() -> None:
    assert address_artifact_tables() == ["se_company_address_bolagsverket", "se_company_address_scb"]


@pytest.mark.parametrize("table", address_artifact_tables())
def test_artifact_table_starts_with_the_envelope(table: str) -> None:
    columns = declared_columns(table)
    block = table_block(table)

    assert tuple(columns[: len(ENVELOPE)]) == ENVELOPE
    assert len(columns) > len(ENVELOPE)  # a payload exists
    assert "evidence_hash FixedString(64) MATERIALIZED" in block
    assert "ENGINE = ReplacingMergeTree(observed_at)" in block
    assert "ORDER BY (company_id, source_record_uid)" in block
    assert "CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')" in block


@pytest.mark.parametrize("table", address_artifact_tables())
def test_every_artifact_payload_column_is_hashed_into_the_evidence(table: str) -> None:
    """A payload column outside evidence_hash would change silently: the anti-join
    would never append a version for it and no downstream run would ever see it."""
    block = table_block(table)
    payload = [column for column in declared_columns(table)
               if column not in ENVELOPE]
    hashed = block[block.index("MATERIALIZED") : block.index("CONSTRAINT")]
    for column in payload:
        assert column in hashed, f"{table}.{column} is not part of evidence_hash"


def test_the_final_carries_the_geocode_augmentation_and_the_tombstone_flag() -> None:
    columns = declared_columns("se_company_address")
    block = table_block("se_company_address")

    assert columns[:2] == ["company_id", "address_key"]
    for column in ("address_id", "latitude", "longitude", "geocode_status", "geocoded_at", "is_current"):
        assert column in columns
    assert tuple(columns[-len(ADDRESS_FINAL_PROVENANCE):]) == ADDRESS_FINAL_PROVENANCE
    assert "evidence_set_hash FixedString(64) MATERIALIZED" in block
    assert "arraySort(arrayMap(x -> toString(x), evidence_hashes))" in block
    assert "is_current Bool DEFAULT true" in block
    assert "ENGINE = ReplacingMergeTree(resolved_at)" in block
    assert "ORDER BY (company_id, address_key)" in block
    assert "CONSTRAINT has_evidence CHECK notEmpty(source_record_uids)" in block


def test_no_model_columns_exist_anywhere_in_this_datatype() -> None:
    """Nothing here is model-written -- the spec's one hard negative."""
    sql = (MIGRATIONS_DIR / ADDRESS_MIGRATION).read_text(encoding="utf-8")
    for forbidden in ("suggestion_id", "model_provider", "model_name", "prompt_version",
                      "llm_enhanced", "enrichment_observation"):
        assert forbidden not in sql


def test_the_ledger_twins_the_info_one_with_its_own_kinds() -> None:
    ledger = table_block("se_company_address_correction")
    for column in ("correction_id", "company_id", "correction_kind", "payload", "evidence_hash",
                   "reason", "decided_by", "supersedes_correction_id", "created_at"):
        assert f"    {column} " in ledger
    assert "ORDER BY (company_id, created_at, correction_id)" in ledger
    assert "CONSTRAINT valid_payload CHECK isValidJSON(payload)" in ledger


def test_writer_grant_is_insert_only() -> None:
    name = "000308_corpscout_se_company_address_writer_grants"
    up = (MIGRATIONS_DIR / f"{name}.up.sql").read_text(encoding="utf-8")
    down = (MIGRATIONS_DIR / f"{name}.down.sql").read_text(encoding="utf-8")
    assert ("GRANT INSERT ON corpscout.se_company_address_correction\n"
            "TO corpscout_person_correction_writer") in up
    assert "GRANT SELECT" not in up and "GRANT ALL" not in up
    assert "REVOKE INSERT ON corpscout.se_company_address_correction" in down
```

- [ ] **Step 2: Run to verify failure**

Run (from `corpscout/services/dagster_v3`): `uv run pytest tests/test_se_company_address_layout.py -q`
Expected: FAIL with `ImportError` (`ADDRESS_MIGRATION` does not exist yet).

- [ ] **Step 3: Generalize `tests/se_company_ddl.py`**

The helpers currently read one hard-coded migration (`000297`). Two datatypes now declare `se_company*` tables, so the block/column helpers find the file that CREATEs the table instead. Every existing call site keeps working unchanged — `_migration_for("se_company_info")` resolves to 000297.

Replace the `MIGRATION` constant, `_sql`, `table_block` and the head of `_column_changes`/`declared_columns` with:

```python
from functools import lru_cache

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000297_corpscout_se_company_info.up.sql"
ADDRESS_MIGRATION = "000307_corpscout_se_company_address.up.sql"
ENVELOPE = ("company_id", "source_record_uid", "observed_at", "source_run_id", "evidence_hash")
FINAL_PROVENANCE = ("source_record_uids", "evidence_hashes", "evidence_set_hash", "correction_ids",
                    "suggestion_id", "model_provider", "model_name", "prompt_version", "source_run_id", "resolved_at")


def _sql() -> str:
    return (MIGRATIONS_DIR / MIGRATION).read_text(encoding="utf-8")


@lru_cache(maxsize=None)
def _migration_for(table: str) -> str:
    """The migration file whose CREATE TABLE declares `table`.

    The se_company layer no longer lives in one migration (000297 declares the info
    tables, 000307 the address ones), so the helpers below locate the creating file
    instead of reading a single constant -- every existing caller keeps its signature.
    Exactly one migration may create a given table; two would mean a rename-swap, which
    these helpers do not model and which would break the ALTER replay below.
    """
    matches = [
        path.name
        for path in sorted(MIGRATIONS_DIR.glob("[0-9]*.up.sql"))
        if f"CREATE TABLE IF NOT EXISTS corpscout.{table}\n" in path.read_text(encoding="utf-8")
    ]
    if len(matches) != 1:
        raise AssertionError(f"expected one migration creating corpscout.{table}, found {matches}")
    return matches[0]


def table_block(table: str) -> str:
    """The one CREATE TABLE statement for `table`, up to and including its semicolon."""
    sql = (MIGRATIONS_DIR / _migration_for(table)).read_text(encoding="utf-8")
    start = sql.index(f"CREATE TABLE IF NOT EXISTS corpscout.{table}\n")
    return sql[start : sql.index(";", start) + 1]
```

In `_column_changes`, replace the `if path.name <= MIGRATION:` guard with `created = _migration_for(table)` computed once before the loop and `if path.name <= created: continue` inside it (same rule, per-table baseline). Everything else in that function — the ADD/DROP replay, the `AFTER` handling, the docstring's format contract — is unchanged. Append:

```python
def address_artifact_tables() -> list[str]:
    """The address datatype's artifact tables (the final and the ledger are not artifacts)."""
    sql = (MIGRATIONS_DIR / ADDRESS_MIGRATION).read_text(encoding="utf-8")
    return sorted(set(re.findall(
        r"CREATE TABLE IF NOT EXISTS corpscout\.(se_company_address_(?!correction)[a-z0-9_]+)\n", sql)))
```

- [ ] **Step 4: Write migration 000307**

`000307_corpscout_se_company_address.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Sweden company addresses: one artifact table per source (standard envelope first, then
-- the source's own typed payload), one merged final with SEVERAL rows per company, and a
-- correction ledger. There is no observation table and there are no model columns: nothing
-- in this datatype is model-written.
--
-- observed_at is APPEND time (now64 in each artifact SELECT), never the register's
-- bulk-load stamp. se_company_addresses_current.updated_from_raw_at is one constant for
-- the whole weekly load and is older than every resolved_at, so a version appended under
-- it would never look newer than the row it replaces and the final's change scan would
-- never select the company again -- the info pilot proved this in production.
--
-- ORDER BY (company_id, source_record_uid) is the standard envelope key, and it is unique
-- for both of today's sources: the register normalizer picks exactly one address row per
-- company per source (address_rank = 1). A future artifact whose source carries SEVERAL
-- addresses for one company must add its own discriminator to that table's ORDER BY, or
-- versions of different addresses would collapse into one.
--
-- address_fingerprint is payload, not decoration: it is the source observation's own key
-- (se_company_addresses_current.address_fingerprint), and it is what
-- se_company_address_members_current.address_key holds, so it is the ONLY way from a
-- merged address back into the shared-identity geocode chain.

CREATE TABLE IF NOT EXISTS corpscout.se_company_address_bolagsverket
(
    company_id String,
    source_record_uid String,
    observed_at DateTime64(3, 'UTC'),
    source_run_id String,
    evidence_hash FixedString(64) MATERIALIZED lower(hex(SHA256(concat(
        'se-company-address-bolagsverket-v1\n',
        address_type, '\n', address_fingerprint, '\n',
        ifNull(care_of, ''), '\n', ifNull(street_address, ''), '\n',
        ifNull(normalized_address, ''), '\n', ifNull(postal_code, ''), '\n',
        ifNull(city, ''), '\n', ifNull(country_code, '')
    )))),
    address_type LowCardinality(String),
    address_fingerprint String,
    care_of Nullable(String),
    street_address Nullable(String),
    normalized_address Nullable(String),
    postal_code Nullable(String),
    city Nullable(String),
    country_code Nullable(String),

    CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (company_id, source_record_uid);

CREATE TABLE IF NOT EXISTS corpscout.se_company_address_scb
(
    company_id String,
    source_record_uid String,
    observed_at DateTime64(3, 'UTC'),
    source_run_id String,
    evidence_hash FixedString(64) MATERIALIZED lower(hex(SHA256(concat(
        'se-company-address-scb-v1\n',
        address_type, '\n', address_fingerprint, '\n',
        ifNull(care_of, ''), '\n', ifNull(street_address, ''), '\n',
        ifNull(normalized_address, ''), '\n', ifNull(postal_code, ''), '\n',
        ifNull(city, ''), '\n', ifNull(country_code, '')
    )))),
    address_type LowCardinality(String),
    address_fingerprint String,
    care_of Nullable(String),
    street_address Nullable(String),
    normalized_address Nullable(String),
    postal_code Nullable(String),
    city Nullable(String),
    country_code Nullable(String),

    CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (company_id, source_record_uid);

-- Final: SEVERAL rows per company, one per address_key. address_key is sha256 of the
-- normalized (address_type, care_of, street, postal digits, city, country) tuple, computed
-- in address_rules.py and nowhere else -- deterministic, and identical across sources that
-- agree on both the type and the address.
--
-- is_current is the versioned tombstone: re-resolving a company republishes a key it no
-- longer produces with is_current = false, carrying that row's own provenance forward so
-- has_evidence still holds and a reviewer can still see what the address was. Readers
-- always filter FINAL ... WHERE is_current.
--
-- The geocode columns are an augmentation, not a source: they are read at resolve time
-- from the existing shared-identity chain (members -> links -> se_address_geocodes_current)
-- and stored, the same way the Swedish description augments the English one in
-- se_company_info. geocode_status '' means the address never reached the geocoder.
CREATE TABLE IF NOT EXISTS corpscout.se_company_address
(
    company_id String,
    address_key FixedString(64),
    address_type LowCardinality(String),
    care_of Nullable(String),
    street_address Nullable(String),
    normalized_address Nullable(String),
    postal_code Nullable(String),
    city Nullable(String),
    country_code Nullable(String),
    address_id Nullable(FixedString(64)),
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    geocode_status LowCardinality(String) DEFAULT '',
    geocoded_at Nullable(DateTime64(3, 'UTC')),
    is_current Bool DEFAULT true,
    sources Array(String),
    source_record_uids Array(String),
    evidence_hashes Array(String),
    evidence_set_hash FixedString(64) MATERIALIZED lower(hex(SHA256(arrayStringConcat(
        arraySort(arrayMap(x -> toString(x), evidence_hashes)), '\n'
    )))),
    correction_ids Array(UUID) DEFAULT [],
    source_run_id String,
    resolved_at DateTime64(3, 'UTC'),

    CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT has_evidence CHECK notEmpty(source_record_uids)
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (company_id, address_key);

-- Ledger: identical shape to se_company_info_correction. Kinds: override_field (payload is
-- address_key plus any subset of the address text fields), reject_address (payload is
-- address_key alone -- the row is published is_current = false), undo (supersedes another
-- correction and carries the zero evidence hash). Every payload names the address_key it
-- decides: a company has several rows, so a correction without one has no subject.
CREATE TABLE IF NOT EXISTS corpscout.se_company_address_correction
(
    correction_id UUID,
    company_id String,
    correction_kind LowCardinality(String),
    payload String,
    evidence_hash FixedString(64),
    reason String,
    decided_by String,
    supersedes_correction_id Nullable(UUID),
    created_at DateTime64(3, 'UTC'),

    CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_payload CHECK isValidJSON(payload)
)
ENGINE = MergeTree
ORDER BY (company_id, created_at, correction_id);
```

`000307_corpscout_se_company_address.down.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.se_company_address_correction;
DROP TABLE IF EXISTS corpscout.se_company_address;
DROP TABLE IF EXISTS corpscout.se_company_address_scb;
DROP TABLE IF EXISTS corpscout.se_company_address_bolagsverket;
```

- [ ] **Step 5: Write migration 000308**

`000308_corpscout_se_company_address_writer_grants.up.sql` — the backoffice writes corrections under the same role the info and person ledgers use:

```sql
GRANT INSERT ON corpscout.se_company_address_correction
TO corpscout_person_correction_writer;
```

`.down.sql`:

```sql
REVOKE INSERT ON corpscout.se_company_address_correction
FROM corpscout_person_correction_writer;
```

- [ ] **Step 6: Register in `tests/test_clickhouse_migrations.py`**

Append `"000307_corpscout_se_company_address",` after `"000306_corpscout_se_company_info_legal_form_label",` in `EXPECTED_MIGRATIONS`; append `"000308_corpscout_se_company_address_writer_grants",` to `EXPECTED_ACCESS_MIGRATIONS`.

- [ ] **Step 7: Run**

Run: `uv run pytest tests/test_se_company_address_layout.py tests/test_se_company_layout.py tests/test_clickhouse_migrations.py -q`
Expected: PASS (the info layout test is in the list because Step 3 changed the helper it shares).

- [ ] **Step 8: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/clickhouse/migrations/000307_corpscout_se_company_address.up.sql \
        corpscout/clickhouse/migrations/000307_corpscout_se_company_address.down.sql \
        corpscout/clickhouse/migrations/000308_corpscout_se_company_address_writer_grants.up.sql \
        corpscout/clickhouse/migrations/000308_corpscout_se_company_address_writer_grants.down.sql \
        corpscout/services/dagster_v3/tests/se_company_ddl.py \
        corpscout/services/dagster_v3/tests/test_se_company_address_layout.py \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(se_company): address artifact, final and correction tables"
```

---

### Task 2: `bolagsverket.py` — the `se_company_address_bolagsverket` artifact

**Files:**
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/bolagsverket.py`
- Test: `corpscout/services/dagster_v3/tests/test_se_company_address_bolagsverket.py`

**Interfaces:**
- Consumes: `publish_with_stage`, `SE_COMPANY_ID_PATTERN` (`se_company/common.py`); `assert_clickhouse_tables_exist` (`defs/clickhouse/resolved.py`); source table `corpscout.se_company_addresses_current` (columns used: `company_id, address_type, source, street_address, care_of, postal_code, post_town, country_code, normalized_address, source_record_uid, has_address, address_fingerprint`); asset `sweden_company_addresses_clickhouse` as `deps`.
- Produces: `TABLE = "se_company_address_bolagsverket"`, `SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS: tuple[str, ...]`, `SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL: str` (one `%(source_run_id)s` parameter), asset `se_company_address_bolagsverket_clickhouse`, `defs`.

- [ ] **Step 1: Failing tests**

```python
# tests/test_se_company_address_bolagsverket.py
import dagster as dg

from dagster_v3.defs.se_company.bolagsverket import (
    SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS,
    SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL,
)
from tests.se_company_ddl import declared_columns, projection_aliases


def test_columns_are_the_migration_order_minus_the_materialized_hash() -> None:
    assert list(SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS) == [
        column for column in declared_columns("se_company_address_bolagsverket")
        if column != "evidence_hash"
    ]


def test_the_trailing_projection_binds_positionally_to_those_columns() -> None:
    """publish_with_stage inserts positionally: a swapped pair of same-typed columns
    here would transpose values with an otherwise-green suite."""
    assert projection_aliases(SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL) == list(
        SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS
    )


def test_the_select_reads_only_this_source_and_only_real_addresses() -> None:
    sql = SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL
    assert "FROM corpscout.se_company_addresses_current AS addresses" in sql
    assert "addresses.source = 'bolagsverket'" in sql
    assert "addresses.has_address = 1" in sql
    assert "addresses.post_town AS city" in sql
    assert "toString(addresses.address_fingerprint) AS address_fingerprint" in sql
    assert "match(addresses.company_id, '^([0-9]{10}|[0-9]{12})$')" in sql


def test_observed_at_is_append_time_not_the_bulk_load_stamp() -> None:
    """updated_from_raw_at is one constant per weekly load; a version stamped with it
    would never look newer than the final row it replaces."""
    sql = SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL
    assert "now64(3, 'UTC') AS observed_at" in sql
    assert "updated_from_raw_at" not in sql


def test_the_asset_reads_the_source_layer_and_writes_its_own_table() -> None:
    from dagster_v3.definitions import defs as load_defs

    asset = load_defs().get_repository_def().asset_graph.get(
        dg.AssetKey("se_company_address_bolagsverket_clickhouse"))
    assert asset.parent_keys == {dg.AssetKey("sweden_company_addresses_clickhouse")}
    assert asset.group_name == "se_company_bolagsverket"
    assert asset.metadata["table"] == "corpscout.se_company_address_bolagsverket"
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/test_se_company_address_bolagsverket.py -q` → `ModuleNotFoundError`

- [ ] **Step 3: Implement**

```python
"""Swedish company addresses as Bolagsverket registered them.

Input (source layer): sweden_company_addresses_clickhouse ->
corpscout.se_company_addresses_current, the rename-swap snapshot of the append-only
se_company_addresses history: one row per (company, address_type, source), with the
register's own provenance (source_run_id, source_record_id, source_payload_hash and the
derived source_record_uid that joins company_source_records).

This module keeps the Bolagsverket half of that snapshot -- address_type 'postal', the
registered postal address, the register being the registration authority for it -- and
writes the standard envelope followed by the address payload. One artifact row per
company: the normalizer picks exactly one address row per company per source.

address_fingerprint travels with the payload because it is the key the rest of the
address pipeline uses: se_company_address_members_current.address_key IS this
fingerprint, so it is the only way from a merged final address back to a canonical
address, a shared address_id and its geocode. Without it the final could not augment
anything.

Assets
  se_company_address_bolagsverket_clickhouse -> corpscout.se_company_address_bolagsverket
Downstream: address.py (field precedence bolagsverket > scb).
"""

from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.common import SE_COMPANY_ID_PATTERN, publish_with_stage

GROUP_NAME = "se_company_bolagsverket"
DATABASE = "corpscout"
TABLE = "se_company_address_bolagsverket"
SOURCE = "bolagsverket"
SOURCE_TABLE = "se_company_addresses_current"
# Positional insert list: the envelope (evidence_hash is MATERIALIZED, so omitted) then
# this module's payload, in the order the migration declares them -- pinned by the test.
SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS = (
    "company_id", "source_record_uid", "observed_at", "source_run_id",
    "address_type", "address_fingerprint", "care_of", "street_address",
    "normalized_address", "postal_code", "city", "country_code",
)

# New versions only: publish_with_stage stages these candidates and keeps only rows whose
# (company_id, source_record_uid, evidence_hash) is not already in the target -- the
# target's MATERIALIZED evidence_hash computes the hash on the stage, so it is never
# re-expressed here.
#
# observed_at is now64 at append time. The register's own updated_from_raw_at is a single
# constant for the whole weekly bulk load and is older than every resolved_at the final
# writes, so a version stamped with it would never look newer than the row it replaces and
# the change scan would never select the company again. Rows the anti-join skips are never
# rewritten, so an unchanged company keeps its original stamp instead of looking new every
# week.
#
# has_address = 0 rows are dropped: the snapshot carries one row per (company, type,
# source) whether or not the register recorded anything, and a row with no address at all
# is not an address. They are also exactly the rows whose MATERIALIZED normalized_address
# is '' by construction (migration 000265).
#
# post_town is the register's name for the column; the datatype publishes it as `city`,
# which is what every other country's address shape calls it. country_code is
# LowCardinality(Nullable(String)) in the source and Nullable(String) in the artifact, so
# it is CAST explicitly rather than left to an implicit conversion.
SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL = """WITH candidates AS (
    SELECT
        addresses.company_id AS company_id,
        addresses.source_record_uid AS source_record_uid,
        now64(3, 'UTC') AS observed_at,
        %(source_run_id)s AS source_run_id,
        toString(addresses.address_type) AS address_type,
        toString(addresses.address_fingerprint) AS address_fingerprint,
        addresses.care_of AS care_of,
        addresses.street_address AS street_address,
        nullIf(addresses.normalized_address, '') AS normalized_address,
        addresses.postal_code AS postal_code,
        addresses.post_town AS city,
        CAST(addresses.country_code AS Nullable(String)) AS country_code
    FROM corpscout.se_company_addresses_current AS addresses
    WHERE addresses.source = '{SOURCE}'
      AND addresses.has_address = 1
      AND match(addresses.company_id, '{SE_COMPANY_ID_PATTERN}')
)
SELECT
    company_id AS company_id, source_record_uid AS source_record_uid,
    observed_at AS observed_at, source_run_id AS source_run_id,
    address_type AS address_type, address_fingerprint AS address_fingerprint,
    care_of AS care_of, street_address AS street_address,
    normalized_address AS normalized_address, postal_code AS postal_code,
    city AS city, country_code AS country_code
FROM candidates
WHERE source_record_uid != ''""".replace(
    "{SE_COMPANY_ID_PATTERN}", SE_COMPANY_ID_PATTERN
).replace("{SOURCE}", SOURCE)


@dg.asset(
    name="se_company_address_bolagsverket_clickhouse",
    deps=[dg.AssetKey("sweden_company_addresses_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": f"{DATABASE}.{TABLE}"},
    description=(
        "The postal address Bolagsverket has registered for each Swedish company, as an "
        "append-only artifact; a new version is written only when the evidence hash "
        "changes and the latest per (company, source record) survives merges."
    ),
)
def se_company_address_bolagsverket_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    """Select the Bolagsverket rows of the address snapshot -> stage -> validate -> append."""
    assert_clickhouse_tables_exist(
        clickhouse, database=DATABASE, tables=(SOURCE_TABLE, TABLE)
    )
    counts = publish_with_stage(
        clickhouse=clickhouse,
        target=TABLE,
        insert_columns=SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS,
        select_sql=SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL,
        select_parameters={"source_run_id": context.run_id},
        invalid_condition=(
            "trim(company_id) = '' OR trim(source_record_uid) = '' OR trim(address_type) = ''"
        ),
        new_versions_only=True,
    )
    context.log.info(
        "se_company_address_bolagsverket: appended=%s total=%s", counts.inserted, counts.total
    )
    return dg.MaterializeResult(
        metadata={"appended_count": counts.inserted, "total_count": counts.total,
                  "table": f"{DATABASE}.{TABLE}", "resolved_at": datetime.now(UTC).isoformat()}
    )


defs = dg.Definitions(assets=[se_company_address_bolagsverket_clickhouse])
```

- [ ] **Step 4: Run** — `uv run pytest tests/test_se_company_address_bolagsverket.py -q && uv run dg check defs && uv run ruff check src/dagster_v3/defs/se_company` → PASS / green / clean

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/bolagsverket.py \
        corpscout/services/dagster_v3/tests/test_se_company_address_bolagsverket.py
git commit -m "feat(se_company): se_company_address_bolagsverket artifact"
```

---

### Task 3: `scb.py` — a second asset for `se_company_address_scb`

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/scb.py` (add the address constants, the address asset, extend `defs`; the info asset is untouched)
- Test: `corpscout/services/dagster_v3/tests/test_se_company_address_scb.py`

**Interfaces:**
- Consumes: same as Task 2.
- Produces: `ADDRESS_TABLE = "se_company_address_scb"`, `SE_COMPANY_ADDRESS_SCB_COLUMNS`, `SE_COMPANY_ADDRESS_SCB_SQL`, asset `se_company_address_scb_clickhouse`, and `defs = dg.Definitions(assets=[se_company_info_scb_clickhouse, se_company_address_scb_clickhouse])`. The module's existing `TABLE` (the info artifact) keeps its name and meaning — `address.py` imports `ADDRESS_TABLE`.

- [ ] **Step 1: Failing tests**

```python
# tests/test_se_company_address_scb.py
import dagster as dg

from dagster_v3.defs.se_company.scb import (
    SE_COMPANY_ADDRESS_SCB_COLUMNS,
    SE_COMPANY_ADDRESS_SCB_SQL,
)
from tests.se_company_ddl import declared_columns, projection_aliases


def test_columns_and_projection_match_the_migration() -> None:
    assert list(SE_COMPANY_ADDRESS_SCB_COLUMNS) == [
        column for column in declared_columns("se_company_address_scb") if column != "evidence_hash"
    ]
    assert projection_aliases(SE_COMPANY_ADDRESS_SCB_SQL) == list(SE_COMPANY_ADDRESS_SCB_COLUMNS)


def test_the_select_reads_only_the_scb_rows() -> None:
    sql = SE_COMPANY_ADDRESS_SCB_SQL
    assert "addresses.source = 'scb'" in sql
    assert "addresses.has_address = 1" in sql
    assert "now64(3, 'UTC') AS observed_at" in sql
    # SCB does not distinguish visiting from postal; the type travels as the register
    # recorded it rather than being renamed here.
    assert "toString(addresses.address_type) AS address_type" in sql


def test_the_two_scb_assets_are_separate_and_write_separate_tables() -> None:
    from dagster_v3.definitions import defs as load_defs

    graph = load_defs().get_repository_def().asset_graph
    address = graph.get(dg.AssetKey("se_company_address_scb_clickhouse"))
    info = graph.get(dg.AssetKey("se_company_info_scb_clickhouse"))
    assert address.parent_keys == {dg.AssetKey("sweden_company_addresses_clickhouse")}
    assert address.group_name == info.group_name == "se_company_scb"
    assert address.metadata["table"] == "corpscout.se_company_address_scb"
    assert info.metadata["table"] == "corpscout.se_company_info_scb"
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/test_se_company_address_scb.py -q` → `ImportError`

- [ ] **Step 3: Implement** — append to `scb.py` (and extend the module docstring's `Assets` block with the new asset line):

```python
ADDRESS_TABLE = "se_company_address_scb"
ADDRESS_SOURCE = "scb"
ADDRESS_SOURCE_TABLE = "se_company_addresses_current"
SE_COMPANY_ADDRESS_SCB_COLUMNS = (
    "company_id", "source_record_uid", "observed_at", "source_run_id",
    "address_type", "address_fingerprint", "care_of", "street_address",
    "normalized_address", "postal_code", "city", "country_code",
)

# Same shape as bolagsverket.py's SELECT, one source value apart -- deliberately NOT
# factored into a shared builder: each artifact module owns its own table name, its own
# insert list and its own SELECT, and a shared generator would make a payload change in
# one source silently rewrite the other's evidence.
#
# SCB records a single address per company and does not say whether it is the visiting or
# the postal one, hence the register's own 'visiting_or_postal' type, which travels
# unchanged: the address_type is part of address_key, so renaming it here would silently
# re-key every SCB address.
#
# SCB also marks foreign addresses with the placeholders PostOrt='Utlandet' and
# PostNr='00000'. Those are the normalizer's business (migration 000265 already drops both
# from normalized_address), so they arrive here as ordinary text and are neither
# special-cased nor repaired.
SE_COMPANY_ADDRESS_SCB_SQL = """WITH candidates AS (
    SELECT
        addresses.company_id AS company_id,
        addresses.source_record_uid AS source_record_uid,
        now64(3, 'UTC') AS observed_at,
        %(source_run_id)s AS source_run_id,
        toString(addresses.address_type) AS address_type,
        toString(addresses.address_fingerprint) AS address_fingerprint,
        addresses.care_of AS care_of,
        addresses.street_address AS street_address,
        nullIf(addresses.normalized_address, '') AS normalized_address,
        addresses.postal_code AS postal_code,
        addresses.post_town AS city,
        CAST(addresses.country_code AS Nullable(String)) AS country_code
    FROM corpscout.se_company_addresses_current AS addresses
    WHERE addresses.source = '{ADDRESS_SOURCE}'
      AND addresses.has_address = 1
      AND match(addresses.company_id, '{SE_COMPANY_ID_PATTERN}')
)
SELECT
    company_id AS company_id, source_record_uid AS source_record_uid,
    observed_at AS observed_at, source_run_id AS source_run_id,
    address_type AS address_type, address_fingerprint AS address_fingerprint,
    care_of AS care_of, street_address AS street_address,
    normalized_address AS normalized_address, postal_code AS postal_code,
    city AS city, country_code AS country_code
FROM candidates
WHERE source_record_uid != ''""".replace(
    "{SE_COMPANY_ID_PATTERN}", SE_COMPANY_ID_PATTERN
).replace("{ADDRESS_SOURCE}", ADDRESS_SOURCE)


@dg.asset(
    name="se_company_address_scb_clickhouse",
    deps=[dg.AssetKey("sweden_company_addresses_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": f"{DATABASE}.{ADDRESS_TABLE}"},
    description=(
        "The address SCB holds for each Swedish company (visiting or postal -- the register "
        "does not distinguish), as an append-only artifact; a new version is written only "
        "when the evidence hash changes."
    ),
)
def se_company_address_scb_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    """Select the SCB rows of the address snapshot -> stage -> validate -> append."""
    assert_clickhouse_tables_exist(
        clickhouse, database=DATABASE, tables=(ADDRESS_SOURCE_TABLE, ADDRESS_TABLE)
    )
    counts = publish_with_stage(
        clickhouse=clickhouse,
        target=ADDRESS_TABLE,
        insert_columns=SE_COMPANY_ADDRESS_SCB_COLUMNS,
        select_sql=SE_COMPANY_ADDRESS_SCB_SQL,
        select_parameters={"source_run_id": context.run_id},
        invalid_condition=(
            "trim(company_id) = '' OR trim(source_record_uid) = '' OR trim(address_type) = ''"
        ),
        new_versions_only=True,
    )
    context.log.info("se_company_address_scb: appended=%s total=%s", counts.inserted, counts.total)
    return dg.MaterializeResult(
        metadata={"appended_count": counts.inserted, "total_count": counts.total,
                  "table": f"{DATABASE}.{ADDRESS_TABLE}", "resolved_at": datetime.now(UTC).isoformat()}
    )


defs = dg.Definitions(assets=[se_company_info_scb_clickhouse, se_company_address_scb_clickhouse])
```

- [ ] **Step 4: Run** — `uv run pytest tests/test_se_company_address_scb.py tests/test_se_company_scb.py -q && uv run dg check defs && uv run ruff check src/dagster_v3/defs/se_company`

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/scb.py \
        corpscout/services/dagster_v3/tests/test_se_company_address_scb.py
git commit -m "feat(se_company): se_company_address_scb artifact beside the info one"
```

---

### Task 4: `address_rules.py` — pure merge rules, set replacement and the ledger

**Files:**
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address_rules.py`
- Test: `corpscout/services/dagster_v3/tests/test_se_company_address_rules.py`

**Interfaces:**
- Consumes: `LedgerRow`, `effective_ledger` (`se_company/common.py`); `ArtifactRow` and `evidence_set_hash_for` (`se_company/info_rules.py` — one artifact-row shape and one evidence-set-hash convention for the whole `se_company` layer; both are datatype-agnostic and are imported rather than re-declared).
- Produces:
  - `ADDRESS_SOURCE_PRIORITY = ("bolagsverket", "scb")`, `ADDRESS_KIND_ORDER = {"reject_address": 0, "override_field": 1}`, `OVERRIDABLE_FIELDS`, `ZERO_HASH`.
  - `address_components(values: Mapping[str, Any]) -> tuple[str, str, str, str, str, str]` and `address_key_for(components: Sequence[str]) -> str`.
  - `GeocodeFact(address_id: str, latitude: float | None, longitude: float | None, geocode_status: str, geocoded_at: datetime | None, has_geocode: bool)`.
  - `AddressOutcome` (frozen dataclass, fields below) and `merge_company_addresses(company_id: str, rows: Sequence[ArtifactRow]) -> tuple[AddressOutcome, ...]`.
  - `augment_with_geocodes(outcomes: Sequence[AddressOutcome], geocodes: Mapping[str, GeocodeFact]) -> tuple[AddressOutcome, ...]`.
  - `with_set_replacement(outcomes: Sequence[AddressOutcome], published: Sequence[AddressOutcome]) -> tuple[AddressOutcome, ...]`.
  - `apply_address_ledger(outcomes: Sequence[AddressOutcome], ledger: Sequence[LedgerRow]) -> tuple[tuple[AddressOutcome, ...], tuple[uuid.UUID, ...]]` — returns `(outcomes, stale_correction_ids)`.

- [ ] **Step 1: Failing tests**

```python
# tests/test_se_company_address_rules.py
import uuid
from datetime import UTC, datetime

from dagster_v3.defs.se_company.address_rules import (
    AddressOutcome,
    GeocodeFact,
    address_components,
    address_key_for,
    apply_address_ledger,
    augment_with_geocodes,
    merge_company_addresses,
    with_set_replacement,
)
from dagster_v3.defs.se_company.common import LedgerRow
from dagster_v3.defs.se_company.info_rules import ArtifactRow, evidence_set_hash_for

NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)
COMPANY = "5565200028"


def _row(source: str, *, uid: str = "", hash_: str = "", seconds: int = 0, **values) -> ArtifactRow:
    payload = {"address_type": "postal", "address_fingerprint": f"fp-{source}",
               "care_of": None, "street_address": "Storgatan 1", "normalized_address": "storgatan 1|11122|stockholm",
               "postal_code": "111 22", "city": "Stockholm", "country_code": None}
    payload.update(values)
    return ArtifactRow(source=source, source_record_uid=uid or f"uid-{source}",
                       evidence_hash=hash_ or f"{source[0]}" * 64,
                       observed_at=NOW.replace(second=seconds), values=payload)


def test_normalization_folds_case_whitespace_and_postal_punctuation() -> None:
    assert address_components({"address_type": "Postal", "care_of": "  c/o   Anna  ",
                               "street_address": "STORGATAN  1", "normalized_address": None,
                               "postal_code": "111-22", "city": " Stockholm ", "country_code": "SE"}) == (
        "postal", "c/o anna", "storgatan 1", "11122", "stockholm", "se")


def test_the_pipeline_normalized_address_wins_over_the_raw_street() -> None:
    """se_company_addresses_current.normalized_address (migration 000265) already strips
    floor suffixes and the foreign placeholders, so two sources that agree on the address
    agree on the key even when their raw street text differs."""
    with_normalized = address_components({"address_type": "postal", "street_address": "Storgatan 1, 3 tr",
                                          "normalized_address": "storgatan 1|11122|stockholm"})
    without = address_components({"address_type": "postal", "street_address": "storgatan 1|11122|stockholm",
                                  "normalized_address": None})
    assert with_normalized[2] == without[2] == "storgatan 1|11122|stockholm"


def test_the_key_is_stable_and_separates_address_types() -> None:
    postal = address_key_for(address_components({"address_type": "postal", "street_address": "A 1"}))
    visiting = address_key_for(address_components({"address_type": "visiting_or_postal", "street_address": "A 1"}))
    assert len(postal) == 64 and postal != visiting
    assert postal == address_key_for(address_components({"address_type": "POSTAL", "street_address": " a  1 "}))


def test_two_sources_agreeing_on_type_and_address_produce_one_row_in_precedence_order() -> None:
    outcomes = merge_company_addresses(COMPANY, [
        _row("scb", care_of="c/o SCB"),
        _row("bolagsverket", care_of=None, street_address="Storgatan 1"),
    ])
    assert len(outcomes) == 1
    outcome = outcomes[0]
    assert outcome.sources == ("bolagsverket", "scb")
    assert outcome.source_record_uids == ("uid-bolagsverket", "uid-scb")
    assert outcome.address_fingerprints == ("fp-bolagsverket", "fp-scb")
    # bolagsverket wins where it says something; scb fills what bolagsverket left empty.
    assert outcome.street_address == "Storgatan 1" and outcome.care_of == "c/o SCB"
    assert outcome.is_current is True


def test_differing_addresses_stay_separate_rows() -> None:
    outcomes = merge_company_addresses(COMPANY, [
        _row("bolagsverket", street_address="A 1", normalized_address="a 1"),
        _row("scb", address_type="visiting_or_postal", street_address="B 2", normalized_address="b 2"),
    ])
    assert len(outcomes) == 2
    assert [outcome.sources for outcome in outcomes].count(("bolagsverket",)) == 1
    assert sorted(outcome.address_key for outcome in outcomes) == [o.address_key for o in outcomes]


def test_only_the_newest_version_per_source_and_key_is_used() -> None:
    outcomes = merge_company_addresses(COMPANY, [
        _row("bolagsverket", seconds=1, hash_="1" * 64, care_of="old"),
        _row("bolagsverket", seconds=2, hash_="2" * 64, care_of="new"),
    ])
    assert len(outcomes) == 1 and outcomes[0].care_of == "new"
    assert outcomes[0].evidence_hashes == ("2" * 64,)


def test_geocodes_attach_by_source_fingerprint_and_prefer_a_coordinate() -> None:
    outcomes = merge_company_addresses(COMPANY, [_row("bolagsverket"), _row("scb")])
    augmented = augment_with_geocodes(outcomes, {
        "fp-bolagsverket": GeocodeFact("a" * 64, None, None, "unmatched", NOW, True),
        "fp-scb": GeocodeFact("b" * 64, 59.3, 18.1, "matched_exact", NOW, True),
    })
    assert augmented[0].address_id == "b" * 64 and augmented[0].latitude == 59.3
    assert augmented[0].geocode_status == "matched_exact"


def test_an_address_with_no_link_at_all_publishes_without_a_geocode() -> None:
    outcomes = augment_with_geocodes(merge_company_addresses(COMPANY, [_row("bolagsverket")]), {})
    assert outcomes[0].address_id is None and outcomes[0].geocode_status == ""


def _published(key: str, *, is_current: bool = True) -> AddressOutcome:
    return AddressOutcome(company_id=COMPANY, address_key=key, address_type="postal",
                          care_of=None, street_address="Gone 1", normalized_address="gone 1",
                          postal_code="11122", city="Stockholm", country_code=None,
                          sources=("scb",), source_record_uids=("uid-scb",),
                          evidence_hashes=("s" * 64,), address_fingerprints=("fp-scb",),
                          is_current=is_current)


def test_a_key_that_disappears_is_republished_as_a_tombstone_with_its_own_provenance() -> None:
    live = merge_company_addresses(COMPANY, [_row("bolagsverket")])
    rows = with_set_replacement(live, [_published("d" * 64), *live])
    tombstones = [row for row in rows if not row.is_current]
    assert [row.address_key for row in tombstones] == ["d" * 64]
    assert tombstones[0].source_record_uids == ("uid-scb",)  # has_evidence still holds


def test_an_address_that_comes_back_is_current_again_and_a_tombstone_is_not_republished() -> None:
    live = merge_company_addresses(COMPANY, [_row("bolagsverket")])
    rows = with_set_replacement(live, [_published(live[0].address_key, is_current=False)])
    assert [(row.address_key, row.is_current) for row in rows] == [(live[0].address_key, True)]


def _correction(index: int, kind: str, payload: dict, *, evidence: str, supersedes: int | None = None) -> LedgerRow:
    return LedgerRow(correction_id=uuid.UUID(int=index), company_id=COMPANY, kind=kind,
                     payload=payload, evidence_hash=evidence,
                     supersedes_correction_id=None if supersedes is None else uuid.UUID(int=supersedes),
                     created_at=NOW.replace(second=index))


def test_override_rewrites_the_named_row_only_and_keeps_its_key() -> None:
    outcomes = merge_company_addresses(COMPANY, [_row("bolagsverket")])
    key = outcomes[0].address_key
    evidence = evidence_set_hash_for(outcomes[0].evidence_hashes)
    updated, stale = apply_address_ledger(outcomes, [
        _correction(1, "override_field", {"address_key": key, "care_of": "c/o Reviewer", "city": None},
                    evidence=evidence)])
    assert stale == ()
    assert updated[0].care_of == "c/o Reviewer" and updated[0].city is None
    # The key is identity: an override must never move the row to a different address_key.
    assert updated[0].address_key == key
    assert updated[0].correction_ids == (uuid.UUID(int=1),)


def test_reject_address_publishes_the_row_as_a_tombstone() -> None:
    outcomes = merge_company_addresses(COMPANY, [_row("bolagsverket")])
    key = outcomes[0].address_key
    updated, _ = apply_address_ledger(outcomes, [
        _correction(2, "reject_address", {"address_key": key},
                    evidence=evidence_set_hash_for(outcomes[0].evidence_hashes))])
    assert updated[0].is_current is False and updated[0].correction_ids == (uuid.UUID(int=2),)


def test_override_outranks_a_reject_decided_after_it() -> None:
    outcomes = merge_company_addresses(COMPANY, [_row("bolagsverket")])
    key, evidence = outcomes[0].address_key, evidence_set_hash_for(outcomes[0].evidence_hashes)
    updated, _ = apply_address_ledger(outcomes, [
        _correction(1, "override_field", {"address_key": key, "care_of": "kept"}, evidence=evidence),
        _correction(2, "reject_address", {"address_key": key}, evidence=evidence)])
    assert updated[0].is_current is False and updated[0].care_of == "kept"


def test_stale_malformed_undone_and_unknown_corrections_never_abort_a_run() -> None:
    outcomes = merge_company_addresses(COMPANY, [_row("bolagsverket")])
    key, evidence = outcomes[0].address_key, evidence_set_hash_for(outcomes[0].evidence_hashes)
    updated, stale = apply_address_ledger(outcomes, [
        _correction(1, "override_field", {"address_key": key, "care_of": "x"}, evidence="f" * 64),
        _correction(2, "override_field", {"address_key": key, "legal_name": "nope"}, evidence=evidence),
        _correction(3, "override_field", {"address_key": key, "care_of": 7}, evidence=evidence),
        _correction(4, "override_field", {"care_of": "no key"}, evidence=evidence),
        _correction(5, "reject_address", {"address_key": "z" * 64}, evidence=evidence),
        _correction(6, "override_field", {"address_key": key, "care_of": "undone"}, evidence=evidence),
        _correction(7, "undo", {}, evidence="0" * 64, supersedes=6),
        _correction(8, "bogus_kind", {"address_key": key}, evidence=evidence),
    ])
    assert updated[0].care_of is None and updated[0].is_current is True
    # Stale: evidence moved on (1), or the key it names is not published (5).
    assert [item.int for item in stale] == [1, 5]
    assert updated[0].correction_ids == ()
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/test_se_company_address_rules.py -q` → `ModuleNotFoundError`

- [ ] **Step 3: Implement `address_rules.py`**

```python
"""Deterministic merge rules for Swedish company addresses.

Pure functions only -- no ClickHouse, no network -- so every rule is a table test.
address.py wires these to the artifacts, the geocode chain and the ledger. There is no
model step anywhere in this datatype, so unlike info_rules there is nothing here that
defers a decision to a suggestion.

The one mechanism info does not have is SET REPLACEMENT: a company has several addresses,
so re-resolving it is not "write one row" but "write this set and tombstone whatever left
it" -- see with_set_replacement.
"""

import hashlib
import re
import uuid
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from dagster_v3.defs.se_company.common import LedgerRow, effective_ledger
from dagster_v3.defs.se_company.info_rules import ArtifactRow, evidence_set_hash_for

# Bolagsverket is the registration authority for a company's postal address, so its text
# wins wherever both sources describe the same address. A source not named here sorts last,
# alphabetically -- a new artifact is never silently promoted above these two.
ADDRESS_SOURCE_PRIORITY = ("bolagsverket", "scb")
# override_field ranks AFTER reject_address, so a live override's field values survive a
# reject decided in the same batch while the reject still tombstones the row: the two
# decide different things and both are honoured. Within one kind, later (by created_at) wins.
ADDRESS_KIND_ORDER = {"reject_address": 0, "override_field": 1}
# The text fields a reviewer may decide. address_type is NOT among them: it is part of
# address_key, so overriding it would move the row to a different identity -- reject the
# address and let the corrected one arrive from a source instead. The backoffice validator
# (app/lib/se-address-corrections.ts) keeps the same list; a key this list does not own is
# skipped silently rather than applied, so a drift between the two fails safe.
OVERRIDABLE_FIELDS = ("care_of", "street_address", "normalized_address",
                      "postal_code", "city", "country_code")
ZERO_HASH = "0" * 64
_WHITESPACE = re.compile(r"\s+")
_NON_DIGIT = re.compile(r"[^0-9]")

__all__ = ["ADDRESS_KIND_ORDER", "ADDRESS_SOURCE_PRIORITY", "OVERRIDABLE_FIELDS", "ZERO_HASH",
           "AddressOutcome", "GeocodeFact", "address_components", "address_key_for",
           "apply_address_ledger", "augment_with_geocodes", "evidence_set_hash_for",
           "merge_company_addresses", "with_set_replacement"]


@dataclass(frozen=True)
class GeocodeFact:
    """What the shared-identity chain knows about one SOURCE observation's address.

    Keyed by that observation's ``address_fingerprint``: the chain runs
    ``se_company_addresses_current.address_fingerprint`` ->
    ``se_company_address_members_current.address_key`` -> ``canonical_address_key`` ->
    ``se_company_address_links_current.address_id`` -> ``se_address_geocodes_current``.
    ``has_geocode`` is the geocoder's hit flag, not "has coordinates": an address can be
    classified (unmatched, foreign, postal-box) without a point.
    """

    address_id: str
    latitude: float | None
    longitude: float | None
    geocode_status: str
    geocoded_at: datetime | None
    has_geocode: bool


@dataclass(frozen=True)
class AddressOutcome:
    """One published row: one address of one company.

    ``address_fingerprints`` is provenance the final table does not store -- it is the
    per-source key the geocode lookup needs, in the same order as ``sources``, and it is
    carried on the outcome only between the merge and the augmentation.
    """

    company_id: str
    address_key: str
    address_type: str
    care_of: str | None
    street_address: str | None
    normalized_address: str | None
    postal_code: str | None
    city: str | None
    country_code: str | None
    sources: tuple[str, ...]
    source_record_uids: tuple[str, ...]
    evidence_hashes: tuple[str, ...]
    address_fingerprints: tuple[str, ...] = ()
    address_id: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    geocode_status: str = ""
    geocoded_at: datetime | None = None
    is_current: bool = True
    correction_ids: tuple[uuid.UUID, ...] = ()


def _norm(value: object) -> str:
    return _WHITESPACE.sub(" ", str(value or "").strip()).lower()


def _digits(value: object) -> str:
    return _NON_DIGIT.sub("", str(value or ""))


def _text(value: object) -> str | None:
    cleaned = str(value or "").strip()
    return cleaned or None


def address_components(values: Mapping[str, Any]) -> tuple[str, str, str, str, str, str]:
    """The six normalized components an ``address_key`` is built from.

    ``(address_type, care_of, street, postal digits, city, country_code)``, each lowered,
    trimmed and whitespace-collapsed; the postal code keeps its digits only, so "111 22"
    and "111-22" are one address.

    The street component is the pipeline's own ``normalized_address`` when the source has
    one and the raw ``street_address`` otherwise. ``normalized_address`` (migration 000265)
    is a MATERIALIZED fold of street + postal code + post town + country that already
    strips floor suffixes (" 3 tr") and SCB's foreign placeholders, so two sources that
    describe the same address agree on it even when their raw text differs -- which is
    exactly what the key has to see. It repeats the postal code and city that follow it in
    the tuple; that is harmless (the key is an identity, not a display) and worth the
    stability.
    """
    street = _text(values.get("normalized_address")) or _text(values.get("street_address"))
    return (
        _norm(values.get("address_type")),
        _norm(values.get("care_of")),
        _norm(street),
        _digits(values.get("postal_code")),
        _norm(values.get("city")),
        _norm(values.get("country_code")),
    )


def address_key_for(components: Sequence[str]) -> str:
    """sha256 of the components joined by newlines -- the final's ``address_key``.

    Computed here and nowhere else: no SQL expression mirrors it, so there is no second
    definition that can drift (unlike the artifacts' evidence_hash, which is the DDL's).
    """
    return hashlib.sha256("\n".join(components).encode()).hexdigest()


def _priority(source: str) -> tuple[int, str]:
    index = ADDRESS_SOURCE_PRIORITY.index(source) if source in ADDRESS_SOURCE_PRIORITY else len(
        ADDRESS_SOURCE_PRIORITY)
    return (index, source)


def _pick(group: Sequence[ArtifactRow], field: str) -> str | None:
    """The first non-empty value for ``field``, in source precedence order."""
    for row in group:
        value = _text(row.values.get(field))
        if value is not None:
            return value
    return None


def merge_company_addresses(company_id: str, rows: Sequence[ArtifactRow]) -> tuple[AddressOutcome, ...]:
    """Merge one company's artifact rows into one outcome per ``address_key``.

    Newest version per (source, key) first -- the artifacts are append-only, so a page can
    legitimately carry several versions of the same row and only the newest is evidence.
    Rows that normalize to the same key are then one address: field values are copied from
    the highest-precedence source that offers each one, and every contributing source, uid
    and evidence hash is recorded. Rows that normalize differently stay separate addresses;
    with today's two sources that is the ordinary case, because Bolagsverket's 'postal' and
    SCB's 'visiting_or_postal' are different address types and the type is part of the key.

    A company with no artifact rows at all returns no outcomes -- and, in address.py, that
    is what tombstones its whole published set.
    """
    newest: dict[tuple[str, str], ArtifactRow] = {}
    for row in rows:
        key = address_key_for(address_components(row.values))
        seen = newest.get((row.source, key))
        if seen is None or (row.observed_at, row.source_record_uid) > (seen.observed_at, seen.source_record_uid):
            newest[(row.source, key)] = row

    grouped: dict[str, list[ArtifactRow]] = defaultdict(list)
    for (_, key), row in newest.items():
        grouped[key].append(row)

    outcomes = []
    for key, group in grouped.items():
        group.sort(key=lambda row: _priority(row.source))
        outcomes.append(AddressOutcome(
            company_id=company_id,
            address_key=key,
            # Every row in the group normalizes to the same type; the highest-precedence
            # source's own spelling is published.
            address_type=str(group[0].values.get("address_type") or ""),
            care_of=_pick(group, "care_of"),
            street_address=_pick(group, "street_address"),
            normalized_address=_pick(group, "normalized_address"),
            postal_code=_pick(group, "postal_code"),
            city=_pick(group, "city"),
            country_code=_pick(group, "country_code"),
            sources=tuple(row.source for row in group),
            source_record_uids=tuple(row.source_record_uid for row in group),
            evidence_hashes=tuple(row.evidence_hash for row in group),
            address_fingerprints=tuple(str(row.values.get("address_fingerprint") or "") for row in group),
        ))
    return tuple(sorted(outcomes, key=lambda outcome: outcome.address_key))


def augment_with_geocodes(
    outcomes: Sequence[AddressOutcome], geocodes: Mapping[str, GeocodeFact]
) -> tuple[AddressOutcome, ...]:
    """Attach the shared-identity geocode to each merged address.

    ``geocodes`` is keyed by SOURCE observation fingerprint, so a merged address that folds
    two observations may see two facts: the first (in source precedence order) that carries
    a coordinate wins, failing that the first that the geocoder answered at all, failing
    that the first that merely reached an address identity -- so a linked-but-ungeocoded
    address still publishes its ``address_id`` and an empty status. An address the chain
    has never seen keeps ``address_id = None`` and ``geocode_status = ''``.
    """
    augmented = []
    for outcome in outcomes:
        facts = [geocodes[fingerprint] for fingerprint in outcome.address_fingerprints
                 if fingerprint in geocodes]
        chosen = next((fact for fact in facts if fact.has_geocode and fact.latitude is not None), None)
        if chosen is None:
            chosen = next((fact for fact in facts if fact.has_geocode), None)
        if chosen is None:
            chosen = facts[0] if facts else None
        if chosen is None:
            augmented.append(outcome)
            continue
        augmented.append(replace(
            outcome,
            address_id=chosen.address_id or None,
            latitude=chosen.latitude,
            longitude=chosen.longitude,
            geocode_status=chosen.geocode_status,
            geocoded_at=chosen.geocoded_at,
        ))
    return tuple(augmented)


def with_set_replacement(
    outcomes: Sequence[AddressOutcome], published: Sequence[AddressOutcome]
) -> tuple[AddressOutcome, ...]:
    """The rows to publish for one company: this resolution's set, plus a tombstone for
    every key that WAS current and is no longer produced.

    A tombstone republishes the last published row with ``is_current = False``, keeping
    that row's own provenance: the final's ``has_evidence`` CHECK requires non-empty
    ``source_record_uids``, and a reviewer looking at a disappeared address needs to see
    which source once carried it. Its applied ``correction_ids`` are cleared -- the
    corrections decided a live address, and replaying them onto a tombstone would claim
    they were applied to this resolution.

    A key already published as a tombstone is not republished: nothing about it changed,
    and re-writing it every week would make the table grow for no reason. A key that comes
    BACK is simply produced again, so it publishes with ``is_current = True`` and the newer
    ``resolved_at`` wins in the ReplacingMergeTree.
    """
    live = {outcome.address_key for outcome in outcomes}
    tombstones = tuple(
        replace(row, is_current=False, correction_ids=())
        for row in published
        if row.is_current and row.address_key not in live
    )
    return tuple(outcomes) + tombstones


def _payload_key(payload: Mapping[str, Any]) -> str | None:
    key = payload.get("address_key")
    return key if isinstance(key, str) and key else None


def apply_address_ledger(
    outcomes: Sequence[AddressOutcome], ledger: Sequence[LedgerRow]
) -> tuple[tuple[AddressOutcome, ...], tuple[uuid.UUID, ...]]:
    """Apply live corrections, in step then time order, on top of ``outcomes``.

    Every payload names the ``address_key`` it decides -- a company has several rows, so a
    correction without one has no subject. Staleness is per row: a correction is compared
    against the ``evidence_set_hash`` of the row it names, computed here from that row's own
    evidence hashes exactly as the final table's MATERIALIZED column computes it.

    Never raises on a bad correction:

    - stale (its evidence has moved on, or it names a key this company no longer publishes)
      -> its id is returned in the second element, not applied.
    - malformed (no ``address_key``, an unknown field, a non-string/non-null value, an
      ``override_field`` that names no field at all) -> silently skipped: neither applied
      nor counted as stale, exactly as ``apply_info_ledger`` treats a malformed payload.

    ``override_field`` rewrites the named text fields of one row; an explicit ``null``
    clears a field and an ABSENT key leaves it as computed. It never touches
    ``address_key`` or ``address_type``: the key is the row's identity, so a corrected
    address is a different address.

    ``reject_address`` publishes the row ``is_current = False``. It ranks BEFORE
    ``override_field``, so a live override still decides the text of a row a reject
    tombstones -- the two answer different questions, and a reviewer who does both means both.
    """
    by_key = {outcome.address_key: outcome for outcome in outcomes}
    applied: dict[str, list[uuid.UUID]] = defaultdict(list)
    stale: list[uuid.UUID] = []
    evidence_by_key = {key: evidence_set_hash_for(outcome.evidence_hashes)
                       for key, outcome in by_key.items()}

    for correction in effective_ledger(ledger, ADDRESS_KIND_ORDER):
        key = _payload_key(correction.payload)
        if key is None:
            continue  # malformed: nothing to decide
        outcome = by_key.get(key)
        if outcome is None:
            stale.append(correction.correction_id)  # the address is no longer published
            continue
        if correction.evidence_hash not in (ZERO_HASH, evidence_by_key[key]):
            stale.append(correction.correction_id)
            continue
        if correction.kind == "override_field":
            fields = {name: value for name, value in correction.payload.items() if name != "address_key"}
            if not fields or any(name not in OVERRIDABLE_FIELDS for name in fields):
                continue  # malformed: no field, or a field this ledger does not own
            if any(value is not None and not isinstance(value, str) for value in fields.values()):
                continue  # malformed: every field is a string or null
            by_key[key] = replace(outcome, **{name: _text(value) for name, value in fields.items()})
        else:  # reject_address -- the only other kind effective_ledger lets through
            if set(correction.payload) != {"address_key"}:
                continue  # malformed: a reject decides nothing but the key
            by_key[key] = replace(outcome, is_current=False)
        applied[key].append(correction.correction_id)

    resolved = tuple(
        replace(outcome, correction_ids=tuple(sorted(applied[key], key=str))) if applied[key] else outcome
        for key, outcome in by_key.items()
    )
    return tuple(sorted(resolved, key=lambda outcome: outcome.address_key)), tuple(sorted(stale, key=str))
```

- [ ] **Step 4: Run** — `uv run pytest tests/test_se_company_address_rules.py -q && uv run ruff check src/dagster_v3/defs/se_company` → PASS / clean

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address_rules.py \
        corpscout/services/dagster_v3/tests/test_se_company_address_rules.py
git commit -m "feat(se_company): pure address merge, set-replacement and ledger rules"
```

---

### Task 5: `address.py` — the `se_company_address` final asset, ledger sensor and weekly schedule

**Files:**
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address.py`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/common.py` (add `normalized_se_company_ids`)
- Test: `corpscout/services/dagster_v3/tests/test_se_company_address.py`, `corpscout/services/dagster_v3/tests/test_se_company_common.py` (one added test)

**Interfaces:**
- Consumes: `publish_with_stage`, `build_ledger_sql`, `ledger_row_from_row`, `ledger_sensor`, `SE_COMPANY_ID_PATTERN` (`common.py`); `merge_company_addresses`, `augment_with_geocodes`, `with_set_replacement`, `apply_address_ledger`, `GeocodeFact`, `AddressOutcome` (`address_rules.py`); `ArtifactRow` (`info_rules.py`); `SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS` + `TABLE` (`bolagsverket.py`), `SE_COMPANY_ADDRESS_SCB_COLUMNS` + `ADDRESS_TABLE` (`scb.py`); `assert_clickhouse_tables_exist`. Read-only source tables: `se_company_address_members_current`, `se_company_address_links_current`, `se_address_geocodes_current`.
- Produces: `INSERT_COLUMNS`, `ARTIFACT_TABLES`, `ARTIFACT_READS`, `SELECTION_REASONS`, `build_changed_companies_sql()`, `build_artifact_rows_sql()`, `build_geocodes_sql()`, `build_published_rows_sql()`, `materialize_se_company_address(...) -> dict[str, object]`, `SECompanyAddressConfig`, asset `se_company_address_clickhouse`, jobs `se_company_address_job` / `se_company_address_review_job`, sensor `se_company_address_correction_sensor` (STOPPED), schedule `se_company_address_weekly` (STOPPED, `55 6 * * 1`), `defs`.
- Produces (`common.py`): `normalized_se_company_ids(company_ids: Sequence[str]) -> tuple[str, ...]`.

- [ ] **Step 1: Failing tests**

```python
# tests/test_se_company_address.py
import dagster as dg
import pytest

from dagster_v3.defs.se_company.address import (
    INSERT_COLUMNS,
    SELECTION_REASONS,
    build_artifact_rows_sql,
    build_changed_companies_sql,
    build_geocodes_sql,
    build_published_rows_sql,
    materialize_se_company_address,
)
from dagster_v3.defs.se_company.common import normalized_se_company_ids
from tests.se_company_ddl import declared_columns


def test_insert_columns_are_the_final_ddl_minus_the_materialized_hash() -> None:
    assert list(INSERT_COLUMNS) == [
        column for column in declared_columns("se_company_address") if column != "evidence_set_hash"
    ]


def test_the_change_scan_reads_every_left_join_miss_through_ifnull() -> None:
    """Bare comparisons work only while join_use_nulls = 0; under 1 a miss is NULL, the
    WHERE is NULL for every never-published company and the scan returns nothing."""
    sql = build_changed_companies_sql()
    assert "ifNull(published.company_id, '') = '' AS never_published" in sql
    assert "ifNull(published.resolved_at, toDateTime64('1970-01-01 00:00:00', 3, 'UTC'))" in sql
    assert "ifNull(ledger.latest_correction_at," in sql
    assert "ifNull(geocodes.latest_geocoded_at," in sql


def test_the_change_scan_has_one_term_per_reason_including_the_geocode_one() -> None:
    sql = build_changed_companies_sql()
    assert SELECTION_REASONS == ("never_published", "new_evidence_bolagsverket",
                                 "new_evidence_scb", "new_geocode", "ledger_pending")
    for reason in SELECTION_REASONS:
        assert f" AS {reason}" in sql
    assert "%(resolve_all)s = 1" in sql
    assert "parseDateTime64BestEffort(%(resolve_all_before)s, 3, 'UTC')" in sql
    assert "artifacts.company_id > %(after_company_id)s" in sql and "LIMIT %(page_size)s" in sql


def test_the_change_scan_needs_final_only_on_the_final_table() -> None:
    """observed_at and matched_at ARE the version columns, so max() over the raw parts is
    already the newest; the final is keyed by (company_id, address_key) and appends a row
    per resolution, but max(resolved_at) per company is likewise version-safe."""
    sql = build_changed_companies_sql()
    assert "FROM corpscout.se_company_address_bolagsverket GROUP BY company_id" in sql
    assert "FINAL" not in sql


def test_the_geocode_query_gates_every_non_nullable_joined_column() -> None:
    sql = build_geocodes_sql()
    assert "toUInt8(ifNull(geocodes.geocode_run_id, '') != '') AS has_geocode" in sql
    assert "INNER JOIN corpscout.se_company_address_links_current AS links" in sql
    assert "LEFT JOIN corpscout.se_address_geocodes_current AS geocodes" in sql
    assert "toString(members.address_key) AS address_fingerprint" in sql
    # Nullable source columns are ifNull'd, never gated; joined non-Nullable ones are gated.
    assert "ifNull(toString(geocodes.latitude), '') AS latitude" in sql
    assert "toString(geocodes.match_status), '') AS geocode_status" in sql


def test_the_artifact_read_contract_names_its_columns() -> None:
    sql = build_artifact_rows_sql()
    for source in ("bolagsverket", "scb"):
        assert f"'{source}' AS source" in sql
        assert f"FROM corpscout.se_company_address_{source} FINAL" in sql
    assert "'address_fingerprint', ifNull(toString(address_fingerprint), '')" in sql
    assert "*" not in sql


def test_published_rows_are_read_final_and_carry_the_tombstone_flag() -> None:
    sql = build_published_rows_sql()
    assert "FROM corpscout.se_company_address AS published FINAL" in sql
    assert "published.is_current AS is_current" in sql
    assert "WHERE published.company_id IN %(company_ids)s" in sql


def test_a_preview_writes_nothing_and_reads_nothing_but_the_scan() -> None:
    """A bare Materialize click in the Dagster UI carries no config at all."""
    executed: list[str] = []

    class _Client:
        def execute(self, sql: str, parameters: object = None) -> list[tuple]:
            executed.append(sql)
            if "system.tables" in sql:
                return [(name,) for name in (
                    "se_company_address_bolagsverket", "se_company_address_scb",
                    "se_company_address", "se_company_address_correction",
                    "se_company_address_members_current", "se_company_address_links_current",
                    "se_address_geocodes_current")]
            return []

    class _Clickhouse:
        def get_connection(self):
            from contextlib import contextmanager

            @contextmanager
            def _cm():
                yield _Client()

            return _cm()

    from datetime import UTC, datetime

    metadata = materialize_se_company_address(
        clickhouse=_Clickhouse(), source_run_id="run-1", resolved_at=datetime.now(UTC),
        company_ids=[], max_companies=10, company_batch_size=10, execute=False, log=None)
    assert metadata["preview"] is True and metadata["selected_company_count"] == 0
    assert all("INSERT" not in sql for sql in executed)
    for reason in SELECTION_REASONS:
        assert metadata[reason] == 0


def test_company_ids_accept_sole_traders() -> None:
    """se_companies carries 12-digit personnummer-based ids for enskild firma, and the
    final's has_company CHECK admits them -- so a scoped run must too."""
    assert normalized_se_company_ids(["196408233412", "5565200028"]) == ("196408233412", "5565200028")
    with pytest.raises(ValueError):
        normalized_se_company_ids(["55652000"])


def test_the_asset_the_jobs_the_sensor_and_the_schedule_are_wired() -> None:
    from dagster_v3.definitions import defs as load_defs

    repository = load_defs().get_repository_def()
    asset = repository.asset_graph.get(dg.AssetKey("se_company_address_clickhouse"))
    assert asset.parent_keys == {dg.AssetKey("se_company_address_bolagsverket_clickhouse"),
                                 dg.AssetKey("se_company_address_scb_clickhouse")}
    assert asset.group_name == "se_company"
    assert asset.metadata["table"] == "corpscout.se_company_address"

    sensor = repository.get_sensor_def("se_company_address_correction_sensor")
    assert sensor.default_status == dg.DefaultSensorStatus.STOPPED

    schedule = repository.get_schedule_def("se_company_address_weekly")
    assert schedule.cron_schedule == "55 6 * * 1"  # offset from se_company_info_weekly's 50 6
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    # An automated run must never fall back to the asset's own defaults, which are a preview.
    assert schedule.run_config["ops"]["se_company_address_clickhouse"]["config"]["execute"] is True
```

Add to `tests/test_se_company_common.py`:

```python
def test_normalized_se_company_ids_sorts_dedupes_and_rejects_non_ids() -> None:
    assert normalized_se_company_ids([" 5565200028 ", "5565200028", "196408233412"]) == (
        "196408233412", "5565200028")
    with pytest.raises(ValueError, match="10 or 12 digits"):
        normalized_se_company_ids(["not-an-id"])
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/test_se_company_address.py -q` → `ModuleNotFoundError`

- [ ] **Step 3: Add `normalized_se_company_ids` to `common.py`**

`defs/company_people/draft.py::normalized_company_ids` validates `\d{10}` exactly, so a scoped run naming a 12-digit sole trader raises — a latent bug in `info.py`, which this datatype must not inherit (its `has_company` CHECK admits both widths). Add beside `SE_COMPANY_ID_PATTERN`, with `import re` at the top of the module:

```python
_SE_COMPANY_ID_RE = re.compile(SE_COMPANY_ID_PATTERN)


def normalized_se_company_ids(company_ids: Sequence[str]) -> tuple[str, ...]:
    """Sorted, de-duplicated, validated Swedish company ids.

    Accepts both widths the se_company tables publish: a 10-digit organisationsnummer and
    a 12-digit personnummer-based sole-trader id (the has_company CHECK, migration 000299).
    company_people.draft.normalized_company_ids predates the sole traders and validates
    10 digits only -- it is deliberately not reused here.
    """
    normalized = tuple(sorted({company_id.strip() for company_id in company_ids}))
    invalid = [company_id for company_id in normalized if _SE_COMPANY_ID_RE.fullmatch(company_id) is None]
    if invalid:
        raise ValueError(f"Sweden company ids must be 10 or 12 digits: {invalid[:5]}")
    return normalized
```

- [ ] **Step 4: Implement `address.py` — module header, contracts and the SQL builders**

```python
"""Final Swedish company addresses: several rows per company, merged from the per-source
artifacts and augmented with the geocode the shared-identity chain already computed.

Inputs: se_company_address_bolagsverket (the registered postal address -- authoritative
for the fields both sources describe), se_company_address_scb.
Rules: address_rules (pure). Every field is copied from its owning source; nothing here is
model-written, so there is no observation table, no model profile and no model columns.
Geocode: se_company_address_members_current -> se_company_address_links_current ->
se_address_geocodes_current, keyed by the source observation's address_fingerprint, read
at resolve time and stored on the row.
Set replacement: a resolution publishes the company's whole address set; a key it no
longer produces is republished is_current = false. Readers filter FINAL ... WHERE is_current.
Ledger: se_company_address_correction -- override_field / reject_address / undo; stale by
the named row's evidence_set_hash; corrections never abort a run.
Trigger: se_company_address_weekly after the artifacts; se_company_address_correction_sensor
(ledger rows -> scoped review job); manual runs scoped by company_ids.
Gate: the asset writes nothing unless the run config says execute: true -- a bare
"Materialize" click in the Dagster UI is a preview that runs the change scan and reports
what a real run would select.

Assets
  se_company_address_clickhouse -> corpscout.se_company_address
"""

import json
import uuid
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.address_rules import (
    AddressOutcome,
    GeocodeFact,
    apply_address_ledger,
    augment_with_geocodes,
    merge_company_addresses,
    with_set_replacement,
)
from dagster_v3.defs.se_company.bolagsverket import SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS
from dagster_v3.defs.se_company.bolagsverket import TABLE as BOLAGSVERKET_TABLE
from dagster_v3.defs.se_company.common import (
    build_ledger_sql,
    ledger_row_from_row,
    ledger_sensor,
    normalized_se_company_ids,
    publish_with_stage,
)
from dagster_v3.defs.se_company.info_rules import ArtifactRow
from dagster_v3.defs.se_company.scb import ADDRESS_TABLE as SCB_TABLE
from dagster_v3.defs.se_company.scb import SE_COMPANY_ADDRESS_SCB_COLUMNS

DATABASE = "corpscout"
GROUP_NAME = "se_company"
SE_COMPANY_ADDRESS = "se_company_address"
SE_COMPANY_ADDRESS_CORRECTION = "se_company_address_correction"
MEMBERS_TABLE = "se_company_address_members_current"
LINKS_TABLE = "se_company_address_links_current"
GEOCODES_TABLE = "se_address_geocodes_current"
# A LEFT JOIN miss reads as this instant, not as a bare NULL comparison.
EPOCH_SQL = "toDateTime64('1970-01-01 00:00:00', 3, 'UTC')"

# This module's READ contract: the artifact modules' own positional insert lists (each
# pinned to the migration by its own test) minus the envelope this module reads by name.
# A renamed or dropped artifact column therefore fails loudly here instead of silently
# shifting values, and no column list is ever hand-copied.
ARTIFACT_ENVELOPE = ("company_id", "source_record_uid", "observed_at", "source_run_id")
ARTIFACT_TABLES: dict[str, str] = {"bolagsverket": BOLAGSVERKET_TABLE, "scb": SCB_TABLE}
ARTIFACT_READS: dict[str, tuple[str, ...]] = {
    "bolagsverket": tuple(column for column in SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS
                          if column not in ARTIFACT_ENVELOPE),
    "scb": tuple(column for column in SE_COMPANY_ADDRESS_SCB_COLUMNS
                 if column not in ARTIFACT_ENVELOPE),
}

# Why the scan picked a company, projected beside its id and counted per page. The reasons
# OVERLAP by construction (a never-published company also has evidence newer than its epoch
# resolved_at), so they are counters, never a partition. Derived from ARTIFACT_TABLES so a
# third artifact adds its reason to the SQL and to the metadata at once.
SELECTION_REASONS = (
    "never_published", *(f"new_evidence_{source}" for source in ARTIFACT_TABLES),
    "new_geocode", "ledger_pending",
)
SELECTION_COLUMNS = ("company_id", *SELECTION_REASONS)

# This module's WRITE contract: se_company_address insert columns in DDL order (the
# MATERIALIZED evidence_set_hash is omitted) -- pinned against the migration by the test.
INSERT_COLUMNS = (
    "company_id", "address_key", "address_type", "care_of", "street_address",
    "normalized_address", "postal_code", "city", "country_code",
    "address_id", "latitude", "longitude", "geocode_status", "geocoded_at", "is_current",
    "sources", "source_record_uids", "evidence_hashes", "correction_ids",
    "source_run_id", "resolved_at",
)
# The columns build_published_rows_sql projects, in order -- the tombstone read contract.
PUBLISHED_COLUMNS = (
    "company_id", "address_key", "address_type", "care_of", "street_address",
    "normalized_address", "postal_code", "city", "country_code", "is_current",
    "sources", "source_record_uids", "evidence_hashes",
)


def build_changed_companies_sql() -> str:
    """Companies whose address set is missing, older than their evidence, older than the
    geocode snapshot, or touched by a correction.

    Reasons to resolve a company again: it has never been published; an artifact carries an
    observation newer than the published resolution; the geocode snapshot moved after it;
    or the correction ledger gained a row after it. There is no model in this datatype, so
    there is no "still owed something" term -- a resolution is complete the moment it is
    written.

    ``max(observed_at)`` per artifact and ``max(matched_at)`` per company need no FINAL --
    both ARE their table's version column (ReplacingMergeTree for the artifacts, a
    rebuilt-per-run snapshot for the geocodes), so an unmerged older duplicate can never be
    the maximum. ``max(resolved_at)`` over the final is version-safe for the same reason,
    which is why nothing in this query is FINAL: a full-table dedup pass over a 4.7M-row
    final would be paid on every page for a value that cannot change.

    Every LEFT JOIN miss is read explicitly through ``ifNull``. Bare comparisons work only
    while ``join_use_nulls = 0``; under ``join_use_nulls = 1`` a miss is NULL, the WHERE is
    NULL for every never-published company, and the scan returns zero rows -- the pipeline
    would silently stop resolving anything.

    THE GEOCODE TERM IS DELIBERATELY BROAD. ``se_address_geocodes_current`` is rebuilt
    whole by the weekly geocoding job, so ``matched_at`` moves for every identity even when
    the outcome is unchanged, and this term therefore re-selects the geocoded population
    (~2.09M identities) once a week. That is accepted: the resolution is deterministic,
    model-free and cheap, republishing an unchanged address changes nothing a reader sees,
    and the ``max_companies`` cap bounds any single run. What it buys is the guarantee the
    spec asks for -- a re-geocode is evidence, and no company keeps a stale coordinate.

    ``resolve_all`` re-selects every in-scope company even though nothing moved -- for
    rules-only changes (new merge logic, a new artifact column) that no ``observed_at`` and
    no ledger row reflects. It carries a CUTOFF (``resolve_all_before``) because the scan
    has no memory of its own: it is ordered by ``company_id`` and every run starts from the
    first id again, so a pass capped below the table size would re-select the SAME slice
    forever (observed in production on the info final). A company whose published
    ``resolved_at`` is already at or after the cutoff has been rewritten by this pass and is
    skipped. The cutoff is ALWAYS bound -- ``parseDateTime64BestEffort`` is parsed whether
    or not the flag beside it is on, so an empty string would be an error, not a no-op.

    One page per call: the LIMIT is the page size and the caller resumes from
    ``after_company_id``. Each selected row also carries WHY it was selected -- the same
    expressions the WHERE is built from, spelled twice from one Python constant because a
    SELECT-list alias is not guaranteed visible to WHERE at the same level in ClickHouse.
    """
    artifact_union = "\n        UNION ALL\n        ".join(
        f"SELECT '{source}' AS source, company_id, max(observed_at) AS source_observed_at"
        f" FROM {DATABASE}.{table} GROUP BY company_id"
        for source, table in ARTIFACT_TABLES.items())
    published_at = f"ifNull(published.resolved_at, {EPOCH_SQL})"
    # maxIf over no rows returns the type's default -- 1970-01-01 for DateTime64 -- which is
    # exactly the instant an unpublished company is compared against, so a source the
    # company has no row in never reads as new evidence.
    per_source = ",\n        ".join(
        f"maxIf(source_observed_at, source = '{source}') AS {source}_observed_at"
        for source in ARTIFACT_TABLES)
    geocoded_at = f"ifNull(geocodes.latest_geocoded_at, {EPOCH_SQL})"
    correction_at = f"ifNull(ledger.latest_correction_at, {EPOCH_SQL})"
    reasons = ",\n    ".join((
        "ifNull(published.company_id, '') = '' AS never_published",
        *(f"artifacts.{source}_observed_at > {published_at} AS new_evidence_{source}"
          for source in ARTIFACT_TABLES),
        f"{geocoded_at} > {published_at} AS new_geocode",
        f"{correction_at} > {published_at} AS ledger_pending",
    ))
    return f"""WITH artifacts AS (
    SELECT company_id, max(source_observed_at) AS latest_observed_at,
        {per_source}
    FROM (
        {artifact_union}
    )
    WHERE (%(all_companies)s = 1 OR company_id IN %(company_ids)s)
    GROUP BY company_id
),
ledger AS (
    SELECT company_id, max(created_at) AS latest_correction_at
    FROM {DATABASE}.{SE_COMPANY_ADDRESS_CORRECTION}
    WHERE (%(all_companies)s = 1 OR company_id IN %(company_ids)s)
    GROUP BY company_id
),
geocodes AS (
    SELECT links.company_id AS company_id, max(geocodes.matched_at) AS latest_geocoded_at
    FROM {DATABASE}.{LINKS_TABLE} AS links
    INNER JOIN {DATABASE}.{GEOCODES_TABLE} AS geocodes ON geocodes.address_id = links.address_id
    WHERE (%(all_companies)s = 1 OR links.company_id IN %(company_ids)s)
    GROUP BY links.company_id
),
published AS (
    SELECT final.company_id AS company_id, max(final.resolved_at) AS resolved_at
    FROM {DATABASE}.{SE_COMPANY_ADDRESS} AS final
    WHERE (%(all_companies)s = 1 OR final.company_id IN %(company_ids)s)
    GROUP BY final.company_id
)
SELECT artifacts.company_id AS company_id,
    {reasons}
FROM artifacts
LEFT JOIN published ON published.company_id = artifacts.company_id
LEFT JOIN ledger ON ledger.company_id = artifacts.company_id
LEFT JOIN geocodes ON geocodes.company_id = artifacts.company_id
WHERE (
        ifNull(published.company_id, '') = ''
     OR (%(resolve_all)s = 1 AND {published_at} < parseDateTime64BestEffort(%(resolve_all_before)s, 3, 'UTC'))
     OR artifacts.latest_observed_at > {published_at}
     OR {geocoded_at} > {published_at}
     OR {correction_at} > {published_at}
      )
  AND artifacts.company_id > %(after_company_id)s
ORDER BY artifacts.company_id
LIMIT %(page_size)s"""


def build_artifact_rows_sql() -> str:
    """One SELECT per artifact naming exactly the columns this module reads, as a JSON map.

    No ORDER BY: merge_company_addresses picks the newest row per (source, address_key) by
    explicit keys, so arrival order never changes the outcome (and a trailing ORDER BY after
    UNION ALL binds to the last SELECT in ClickHouse anyway).
    """
    selects = []
    for source, columns in ARTIFACT_READS.items():
        pairs = ", ".join(f"'{column}', ifNull(toString({column}), '')" for column in columns)
        selects.append(f"""SELECT '{source}' AS source, company_id, source_record_uid, toString(evidence_hash) AS evidence_hash,
        observed_at, toJSONString(map({pairs})) AS payload_json
    FROM {DATABASE}.{ARTIFACT_TABLES[source]} FINAL
    WHERE company_id IN %(company_ids)s""")
    return "\n    UNION ALL\n    ".join(selects)


def build_geocodes_sql() -> str:
    """What the shared-identity chain knows about each of this page's source observations.

    members.address_key IS se_company_addresses_current.address_fingerprint (see
    sweden_company/address_canonicalization.py, which selects it as exactly that), which is
    why the artifacts carry the fingerprint: it is the only join key from a company's own
    observation into the cross-company address identity and its geocode.

    The members -> links join is INNER: an observation that never reached an address
    identity has nothing to say here and is simply absent, which augment_with_geocodes
    reads as "no geocode". The geocode join is LEFT, because an identity can exist before
    the geocoder has answered for it -- and that miss is GATED, not ifNull'd: ClickHouse
    fills a LEFT JOIN miss with each column's TYPE DEFAULT, so match_status would read ''
    and matched_at 1970-01-01 as if the geocoder had answered. The gate itself is
    ``ifNull(geocodes.geocode_run_id, '') != ''`` rather than a bare ``!= ''``, because
    under join_use_nulls = 1 the miss really is NULL and a bare comparison would be NULL
    too. ifNull stays only where the SOURCE column is genuinely Nullable (latitude,
    longitude), which is a different question -- and both are projected as text so every
    column of this query is a plain String on both settings.
    """
    hit = "ifNull(geocodes.geocode_run_id, '') != ''"
    return f"""SELECT
    members.company_id AS company_id,
    toString(members.address_key) AS address_fingerprint,
    toString(links.address_id) AS address_id,
    toUInt8({hit}) AS has_geocode,
    ifNull(toString(geocodes.latitude), '') AS latitude,
    ifNull(toString(geocodes.longitude), '') AS longitude,
    if({hit}, toString(geocodes.match_status), '') AS geocode_status,
    if({hit}, toString(geocodes.matched_at), '') AS geocoded_at
FROM {DATABASE}.{MEMBERS_TABLE} AS members
INNER JOIN {DATABASE}.{LINKS_TABLE} AS links
    ON links.company_id = members.company_id
   AND links.canonical_address_key = members.canonical_address_key
LEFT JOIN {DATABASE}.{GEOCODES_TABLE} AS geocodes ON geocodes.address_id = links.address_id
WHERE members.company_id IN %(company_ids)s"""


def build_published_rows_sql() -> str:
    """This page's already-published rows, as the tombstone decision needs them.

    FINAL is required here and only here: the final is keyed by (company_id, address_key)
    and appends a version per resolution, so without it a key's older version could be read
    as still current. The projection is PUBLISHED_COLUMNS in order -- the geocode columns
    are deliberately absent, because a tombstone republishes the address, not a coordinate
    this resolution did not verify.
    """
    return f"""SELECT
    published.company_id AS company_id,
    toString(published.address_key) AS address_key,
    toString(published.address_type) AS address_type,
    published.care_of AS care_of,
    published.street_address AS street_address,
    published.normalized_address AS normalized_address,
    published.postal_code AS postal_code,
    published.city AS city,
    published.country_code AS country_code,
    published.is_current AS is_current,
    published.sources AS sources,
    published.source_record_uids AS source_record_uids,
    published.evidence_hashes AS evidence_hashes
FROM {DATABASE}.{SE_COMPANY_ADDRESS} AS published FINAL
WHERE published.company_id IN %(company_ids)s"""
```

- [ ] **Step 5: Implement `address.py` — row mappers, the page resolver and the entry point**

```python
def _artifact_row_from_row(row: Sequence[Any]) -> ArtifactRow:
    """payload_json is a name->string map, so typed NULLs arrive as '' and numbers as text;
    address_rules treats '' as missing."""
    return ArtifactRow(source=str(row[0]), source_record_uid=str(row[2]), evidence_hash=str(row[3]),
                       observed_at=row[4], values=json.loads(str(row[5])))


def _float(value: object) -> float | None:
    text = str(value or "").strip()
    return float(text) if text else None


def _geocode_fact_from_row(row: Sequence[Any]) -> tuple[str, str, GeocodeFact]:
    """(company_id, address_fingerprint, fact) -- every column arrives as text."""
    stamp = str(row[7] or "").strip()
    return (str(row[0]), str(row[1]), GeocodeFact(
        address_id=str(row[2]), latitude=_float(row[4]), longitude=_float(row[5]),
        geocode_status=str(row[6]), has_geocode=bool(int(row[3])),
        geocoded_at=datetime.fromisoformat(stamp).replace(tzinfo=UTC) if stamp else None))


def _published_outcome_from_row(row: Sequence[Any]) -> AddressOutcome:
    """A published row as an AddressOutcome, so with_set_replacement compares like with like."""
    return AddressOutcome(
        company_id=str(row[0]), address_key=str(row[1]), address_type=str(row[2]),
        care_of=row[3], street_address=row[4], normalized_address=row[5],
        postal_code=row[6], city=row[7], country_code=row[8], is_current=bool(row[9]),
        sources=tuple(str(value) for value in row[10]),
        source_record_uids=tuple(str(value) for value in row[11]),
        evidence_hashes=tuple(str(value) for value in row[12]))


def _final_row(outcome: AddressOutcome, *, source_run_id: str, resolved_at: datetime) -> tuple[Any, ...]:
    return (
        outcome.company_id, outcome.address_key, outcome.address_type, outcome.care_of,
        outcome.street_address, outcome.normalized_address, outcome.postal_code, outcome.city,
        outcome.country_code, outcome.address_id, outcome.latitude, outcome.longitude,
        outcome.geocode_status, outcome.geocoded_at, outcome.is_current,
        list(outcome.sources), list(outcome.source_record_uids), list(outcome.evidence_hashes),
        list(outcome.correction_ids), source_run_id, resolved_at,
    )


def _resolve_page(
    *, clickhouse: ClickhouseResource, companies: Sequence[str], metrics: dict[str, int],
    source_run_id: str, resolved_at: datetime, log: Callable[..., object] | None,
) -> None:
    """Read one page's evidence, resolve every company in it and publish the results."""
    params = {"company_ids": tuple(companies)}
    with clickhouse.get_connection() as client:
        rows_by_company: dict[str, list[ArtifactRow]] = defaultdict(list)
        for row in client.execute(build_artifact_rows_sql(), params):
            rows_by_company[str(row[1])].append(_artifact_row_from_row(row))
        geocodes_by_company: dict[str, dict[str, GeocodeFact]] = defaultdict(dict)
        for row in client.execute(build_geocodes_sql(), params):
            company_id, fingerprint, fact = _geocode_fact_from_row(row)
            # One fingerprint can appear under several canonical addresses in the member
            # bridge; the first geocoded one wins, so a later ungeocoded duplicate never
            # overwrites a coordinate.
            existing = geocodes_by_company[company_id].get(fingerprint)
            if existing is None or (fact.has_geocode and existing.latitude is None):
                geocodes_by_company[company_id][fingerprint] = fact
        published_by_company: dict[str, list[AddressOutcome]] = defaultdict(list)
        for row in client.execute(build_published_rows_sql(), params):
            outcome = _published_outcome_from_row(row)
            published_by_company[outcome.company_id].append(outcome)
        ledger_by_company: dict[str, list[Any]] = defaultdict(list)
        for row in client.execute(build_ledger_sql(SE_COMPANY_ADDRESS_CORRECTION), params):
            item = ledger_row_from_row(row)
            ledger_by_company[item.company_id].append(item)

    final_rows: list[tuple[Any, ...]] = []
    for company_id in companies:
        outcomes = merge_company_addresses(company_id, rows_by_company.get(company_id, []))
        outcomes = augment_with_geocodes(outcomes, geocodes_by_company.get(company_id, {}))
        # Set replacement BEFORE the ledger: a reject_address decides a row this resolution
        # produced, and a tombstone the set replacement wrote is not something a correction
        # can be applied to (its correction_ids are cleared by design).
        outcomes = with_set_replacement(outcomes, published_by_company.get(company_id, []))
        outcomes, stale = apply_address_ledger(outcomes, ledger_by_company.get(company_id, []))
        metrics["address_count"] += sum(1 for outcome in outcomes if outcome.is_current)
        metrics["tombstone_count"] += sum(1 for outcome in outcomes if not outcome.is_current)
        metrics["geocoded_count"] += sum(1 for outcome in outcomes
                                         if outcome.is_current and outcome.latitude is not None)
        metrics["applied_correction_count"] += sum(len(outcome.correction_ids) for outcome in outcomes)
        metrics["stale_correction_count"] += len(stale)
        if stale and log is not None:
            log("Stale corrections skipped: company=%s ids=%s", company_id, [str(item) for item in stale])
        final_rows.extend(_final_row(outcome, source_run_id=source_run_id, resolved_at=resolved_at)
                          for outcome in outcomes)

    if final_rows:
        # new_versions_only stays off: the final is keyed by (company_id, address_key) and a
        # new version per resolution is the point -- ReplacingMergeTree(resolved_at) keeps
        # the newest, tombstones included.
        counts = publish_with_stage(
            clickhouse=clickhouse, target=SE_COMPANY_ADDRESS, insert_columns=INSERT_COLUMNS,
            rows=final_rows,
            invalid_condition="trim(company_id) = '' OR empty(source_record_uids) OR trim(address_type) = ''",
            new_versions_only=False)
        metrics["inserted_count"] += counts.inserted
        metrics["total_count"] = counts.total
    if log is not None:
        log("se_company_address page: companies=%s rows=%s tombstones=%s geocoded=%s",
            len(companies), len(final_rows), metrics["tombstone_count"], metrics["geocoded_count"])


def materialize_se_company_address(
    *, clickhouse: ClickhouseResource, source_run_id: str, resolved_at: datetime,
    company_ids: Sequence[str], max_companies: int, company_batch_size: int, execute: bool,
    log: Callable[..., object] | None, resolve_all: bool = False, resolve_all_before: str = "",
) -> dict[str, object]:
    """Resolve the changed companies -- or, with ``execute`` false, only say which.

    A preview runs the change scan exactly as a real run does (every chunk, every page, the
    same flags) and reports what it selected. It reads nothing else: no artifact rows, no
    geocodes, no published rows, no ledger -- and it writes nothing. That is the whole point
    of the flag: a "Materialize" click in the Dagster UI, which carries no config at all,
    must be free and harmless.
    """
    # One cutoff for the whole run: every chunk and every page binds this same value, so a
    # row this run publishes can never be re-selected by a later page of the same run. Empty
    # config -> the run's own resolved_at. Always bound, resolve_all or not: the scan's
    # parseDateTime64BestEffort is parsed regardless of the flag beside it.
    resolve_all_cutoff = resolve_all_before.strip() or resolved_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    scope = normalized_se_company_ids(company_ids)
    assert_clickhouse_tables_exist(clickhouse, database=DATABASE, tables=(
        *ARTIFACT_TABLES.values(), SE_COMPANY_ADDRESS, SE_COMPANY_ADDRESS_CORRECTION,
        MEMBERS_TABLE, LINKS_TABLE, GEOCODES_TABLE))
    # The scan embeds %(company_ids)s four times and clickhouse-driver substitutes them
    # client-side, so the rendered statement grows by ~12 bytes per id per copy: an explicit
    # scope is split into chunks of at most company_batch_size ids, each paged on its own,
    # and company_batch_size is capped at 5,000 by the config (ClickHouse's default
    # max_query_size is 262,144 bytes).
    chunks = [tuple(scope[start : start + company_batch_size])
              for start in range(0, len(scope), company_batch_size)] or [()]
    metrics: dict[str, int] = defaultdict(int)
    # Seeded rather than left to the defaultdict: "no company was selected for this reason"
    # must read as 0 in the metadata, not as a missing key.
    for reason in SELECTION_REASONS:
        metrics[reason] = 0
    stopped_at_cap = False

    for chunk in chunks:
        if stopped_at_cap:
            break
        base = {"all_companies": int(not chunk), "company_ids": chunk or ("",),
                "resolve_all": int(resolve_all), "resolve_all_before": resolve_all_cutoff}
        after_company_id = ""
        while True:
            remaining = max_companies - metrics["selected_company_count"]
            if remaining <= 0:
                # Reachable only after a FULL page (a short page breaks below), so the scan
                # may well have more to give: this flag means "the cap stopped us".
                stopped_at_cap = True
                break
            page_size = min(company_batch_size, remaining)
            with clickhouse.get_connection() as client:
                page = client.execute(build_changed_companies_sql(),
                                      {**base, "after_company_id": after_company_id, "page_size": page_size})
            companies = [str(row[0]) for row in page]
            if not companies:
                break
            after_company_id = companies[-1]
            for row in page:
                for offset, reason in enumerate(SELECTION_REASONS, start=1):
                    if row[offset]:
                        metrics[reason] += 1
            metrics["selected_company_count"] += len(companies)
            if execute:
                _resolve_page(clickhouse=clickhouse, companies=companies, metrics=metrics,
                              source_run_id=source_run_id, resolved_at=resolved_at, log=log)
            if len(companies) < page_size:
                break  # a short page means the scan is exhausted
    if stopped_at_cap and log is not None:
        log("se_company_address stopped at the max_companies cap (%s): changed companies may "
            "remain, the next run resumes from the start of the scan", max_companies)
    if not execute:
        return {**metrics, "preview": True, "stopped_at_cap": stopped_at_cap,
                "source_run_id": source_run_id, "company_scope": list(scope)}
    return {**metrics, "stopped_at_cap": stopped_at_cap, "source_run_id": source_run_id,
            "company_scope": list(scope)}
```

- [ ] **Step 6: Implement `address.py` — config, asset, jobs, sensor, schedule**

```python
class SECompanyAddressConfig(dg.Config):
    # False = preview: run the change scan, report what a real run would select, write
    # nothing. The default is False so that a "Materialize" click in the Dagster UI -- which
    # sends no config at all -- can never rewrite every company's address set. Every real
    # run says execute: true explicitly: the schedule, the correction sensor and any manual
    # launch all do.
    execute: bool = False
    company_ids: list[str] = Field(default_factory=list)
    max_companies: int = Field(default=1_000_000, ge=1, le=1_000_000)
    # Capped at 5,000: this is both the scan page size and the chunk size for an explicit
    # company_ids scope, and the scan embeds the id list four times client-side.
    company_batch_size: int = Field(default=5_000, ge=1, le=5_000)
    # True = re-resolve every in-scope company even though no evidence moved. For rules-only
    # changes (new merge logic, a new artifact column): nothing marks those companies as
    # changed, so an ordinary run would resolve none of them.
    resolve_all: bool = False
    # The cutoff resolve_all resumes from: a company whose published resolved_at is already
    # at or after it has been rewritten by this pass and is skipped. ISO-8601 UTC, e.g.
    # "2026-08-24 18:30:00". Empty means "this pass is one run". A pass that CANNOT fit in
    # one run (max_companies below the company count) must give an EXPLICIT cutoff -- the
    # instant before the first run started -- and reuse it for every run of the pass.
    resolve_all_before: str = ""


@dg.asset(
    name="se_company_address_clickhouse",
    deps=[dg.AssetKey("se_company_address_bolagsverket_clickhouse"),
          dg.AssetKey("se_company_address_scb_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": f"{DATABASE}.{SE_COMPANY_ADDRESS}"},
    description=("Every registered address of a Swedish company, merged across sources with "
                 "full provenance, augmented with the shared-identity geocode, tombstoned "
                 "when a source stops carrying it, and overridable from the backoffice. A UI "
                 "materialization without execute=true is a preview that writes nothing."),
)
def se_company_address_clickhouse(context: dg.AssetExecutionContext, config: SECompanyAddressConfig,
                                  clickhouse: ClickhouseResource) -> dg.MaterializeResult:
    """changed companies -> artifact rows -> rules -> geocodes -> set replacement -> ledger -> publish."""
    metadata = materialize_se_company_address(
        clickhouse=clickhouse, source_run_id=context.run_id, resolved_at=datetime.now(UTC),
        company_ids=config.company_ids, max_companies=config.max_companies,
        company_batch_size=config.company_batch_size, execute=config.execute,
        log=context.log.info, resolve_all=config.resolve_all,
        resolve_all_before=config.resolve_all_before)
    return dg.MaterializeResult(metadata={**metadata, "table": f"{DATABASE}.{SE_COMPANY_ADDRESS}"})


se_company_address_job = dg.define_asset_job("se_company_address_job", selection=dg.AssetSelection.assets(
    "se_company_address_bolagsverket_clickhouse", "se_company_address_scb_clickhouse",
    "se_company_address_clickhouse"))
se_company_address_review_job = dg.define_asset_job(
    "se_company_address_review_job", selection=dg.AssetSelection.assets("se_company_address_clickhouse"))
# Both automated triggers must resolve for real, so both spell execute out: a sensor-launched
# or scheduled run carries only the config written here, and anything left out falls back to
# the asset's own defaults -- which for this asset means resolving nothing.
AUTOMATED_RUN_CONFIG: dict[str, Any] = {"execute": True}
se_company_address_correction_sensor = ledger_sensor(
    name="se_company_address_correction_sensor", table=SE_COMPANY_ADDRESS_CORRECTION,
    job=se_company_address_review_job, asset_names=("se_company_address_clickhouse",),
    extra_config=AUTOMATED_RUN_CONFIG)
# 06:55 Monday, five minutes after se_company_info_weekly's 06:50: the (minute, hour) slot
# must be unique across every schedule in this deployment, and both datatypes read the same
# weekly register load.
se_company_address_weekly = dg.ScheduleDefinition(
    name="se_company_address_weekly", job=se_company_address_job, cron_schedule="55 6 * * 1",
    execution_timezone="UTC", default_status=dg.DefaultScheduleStatus.STOPPED,
    run_config={"ops": {"se_company_address_clickhouse": {"config": dict(AUTOMATED_RUN_CONFIG)}}})

defs = dg.Definitions(assets=[se_company_address_clickhouse],
                      jobs=[se_company_address_job, se_company_address_review_job],
                      sensors=[se_company_address_correction_sensor],
                      schedules=[se_company_address_weekly])
```

- [ ] **Step 7: Run** — `uv run pytest tests/test_se_company_address.py tests/test_se_company_common.py -q && uv run dg check defs && uv run ruff check src/dagster_v3/defs/se_company`
Expected: PASS / green / clean. `dg check defs` also proves the new schedule's cron slot does not collide.

- [ ] **Step 8: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/common.py \
        corpscout/services/dagster_v3/tests/test_se_company_address.py \
        corpscout/services/dagster_v3/tests/test_se_company_common.py
git commit -m "feat(se_company): se_company_address final asset, ledger sensor and weekly schedule"
```

---

### Task 6: Executed-SQL harness for the address pipeline (clickhouse-local, both `join_use_nulls` settings)

**Files:**
- Create: `corpscout/services/dagster_v3/tests/test_se_company_address_clickhouse_local.py`
- Reuse (import, do not modify): `_clickhouse_local_command()`, `_literal()`, `_render()` from `tests/test_se_company_person_clickhouse_local.py`; the whole shape (marked sections, `_publish_pass`, the parametrized `sections` fixture) from `tests/test_se_company_info_clickhouse_local.py`.

**Interfaces:** consumes `SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL/_COLUMNS`, `SE_COMPANY_ADDRESS_SCB_SQL/_COLUMNS`, `build_changed_companies_sql`, `build_artifact_rows_sql`, `build_geocodes_sql`, `build_published_rows_sql`, `INSERT_COLUMNS`. Produces nothing importable.

- [ ] **Step 1: Write the harness**

```python
# tests/test_se_company_address_clickhouse_local.py
"""Executes the address artifact SELECTs, the change scan, the geocode lookup and the
final's read-back against the migrations' DDL in a disposable clickhouse-local. Proves the
SQL runs on the deployed ClickHouse version -- substring tests cannot.

Two companies. ALPHA has both sources: a Bolagsverket 'postal' row and an SCB
'visiting_or_postal' row for the SAME street, which is the ordinary Swedish case and the
one that shows why both rows are published (the address types differ, so the keys differ).
Its Bolagsverket observation is linked and geocoded; its SCB observation is linked but NOT
geocoded, which is the LEFT JOIN miss build_geocodes_sql has to gate rather than ifNull.
BETA has only an SCB row and no address identity at all, so it exercises the "no link"
path through the INNER JOIN.

The publish sequence (stage -> validate -> LEFT ANTI JOIN copy -> drop stage) mirrors
publish_with_stage(..., new_versions_only=True); common.py has no separate SQL-string
builder for that anti-join -- it is inlined in the function -- so _publish_pass copies the
shape verbatim, exactly as the info harness does.

Set replacement is proved as a STORAGE round-trip here (the rules' decision is a table test
in tests/test_se_company_address_rules.py): a key published is_current = true, then
republished is_current = false with a newer resolved_at, disappears from
`FINAL ... WHERE is_current`; published true again with a newer stamp still, it comes back.

The whole script runs twice, once under default settings and once with
`SET join_use_nulls = 1` prepended: every LEFT JOIN miss in the scan and in the geocode
lookup is read through ifNull, so both settings must answer identically.
"""

import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dagster_v3.defs.se_company.address import (
    INSERT_COLUMNS,
    build_artifact_rows_sql,
    build_changed_companies_sql,
    build_geocodes_sql,
    build_published_rows_sql,
)
from dagster_v3.defs.se_company.bolagsverket import (
    SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS,
    SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL,
)
from dagster_v3.defs.se_company.scb import (
    SE_COMPANY_ADDRESS_SCB_COLUMNS,
    SE_COMPANY_ADDRESS_SCB_SQL,
)
from tests.se_company_ddl import declared_columns
from tests.test_se_company_person_clickhouse_local import _clickhouse_local_command, _literal, _render

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
# Every migration that creates or alters one of NEEDED_TABLES, in migration order. The
# address history table (se_company_addresses) is deliberately absent: the artifacts read
# the _current snapshot only.
MIGRATIONS = (
    "000256_corpscout_se_company_address_current_snapshot.up.sql",
    "000265_corpscout_se_company_address_normalization.up.sql",
    "000273_corpscout_se_company_canonical_addresses.up.sql",
    "000274_corpscout_se_shared_addresses.up.sql",
    "000275_corpscout_se_address_geocodes_current.up.sql",
    "000277_corpscout_se_address_geocode_spread.up.sql",
    "000278_corpscout_se_address_components.up.sql",
    "000307_corpscout_se_company_address.up.sql",
)
NEEDED_TABLES = frozenset({
    "se_company_addresses_current",
    "se_company_address_members_current",
    "se_company_address_links_current",
    "se_addresses_current",
    "se_address_geocodes_current",
    "se_company_address_bolagsverket",
    "se_company_address_scb",
    "se_company_address",
    "se_company_address_correction",
})
_TABLE_RE = re.compile(r"^(?:CREATE TABLE(?: IF NOT EXISTS)?|ALTER TABLE)\s+corpscout\.(\w+)", re.IGNORECASE)
# 000256 builds se_company_addresses_current under a staging name and RENAMEs it -- a
# rename the per-statement filter cannot replay -- so its CREATE is taken and renamed here.
# Nothing is hand-copied: a column added to the deployed table lands here on the next run.
STAGING_NAME = "se_company_addresses_current_snapshot_000256"

RUN_ID = "fixture-run-1"
ALPHA = "5565200028"
BETA = "196408233412"  # sole trader: 12-digit id, admitted by the has_company CHECK
T_SEED = _literal(datetime(2026, 8, 1, tzinfo=UTC))
T_GEOCODE = _literal(datetime(2026, 8, 2, tzinfo=UTC))
# DateTime64(3) has millisecond resolution and consecutive statements can share one, so
# every point where a stamp must be strictly newer than the previous one is separated by a
# real pause. FORMAT Null keeps sleep's own row out of the marked-section stream.
SETTLE = "SELECT sleep(0.05) FORMAT Null;\n"
ALPHA_FP_BOLAGSVERKET, ALPHA_FP_SCB, BETA_FP = "a" * 64, "b" * 64, "c" * 64
CANONICAL_KEY, ADDRESS_ID = "d" * 64, "e" * 64
CANONICAL_KEY_SCB, ADDRESS_ID_SCB = "f" * 64, "1" * 64


def _schema_statements(migrations: tuple[str, ...]) -> list[str]:
    """CREATE/ALTER TABLE statements for NEEDED_TABLES only, in migration order.

    Several of these files also touch tables this pipeline never reads
    (se_company_addresses_canonical_current, se_company_address_geocode_results); applying
    those blindly would work but would grow the script for nothing, and 000277's ALTER on
    the legacy results table would fail since it is never created here.
    """
    statements: list[str] = []
    for name in migrations:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8").replace(STAGING_NAME,
                                                                          "se_company_addresses_current")
        for raw in text.split(";"):
            statement = "\n".join(line for line in raw.splitlines()
                                  if not line.strip().startswith("--")).strip()
            if not statement:
                continue
            if statement.upper().startswith("CREATE DATABASE"):
                statements.append(statement)
                continue
            match = _TABLE_RE.match(statement)
            if match and match.group(1) in NEEDED_TABLES:
                statements.append(statement)
    return statements


FIXTURE = f"""
INSERT INTO corpscout.se_company_addresses_current
    (company_id, address_type, source, raw_address, street_address, care_of, postal_code,
     post_town, country_code, source_run_id, source_record_id, source_payload_hash,
     source_record_uid, updated_from_raw_at, has_address, address_fingerprint,
     observation_fingerprint, observed_at, has_observation)
VALUES
    ('{ALPHA}', 'postal', 'bolagsverket', 'Storgatan 1, 111 22 Stockholm', 'Storgatan 1', NULL,
     '111 22', 'Stockholm', NULL, 'fixture', 'bv-1', 'hash-bv-1', 'uid-bv-1', {T_SEED}, 1,
     '{ALPHA_FP_BOLAGSVERKET}', '{ALPHA_FP_BOLAGSVERKET}', {T_SEED}, 1),
    ('{ALPHA}', 'visiting_or_postal', 'scb', 'Storgatan 1', 'Storgatan 1', 'c/o Anna',
     '111-22', 'Stockholm', NULL, 'fixture', 'scb-1', 'hash-scb-1', 'uid-scb-1', {T_SEED}, 1,
     '{ALPHA_FP_SCB}', '{ALPHA_FP_SCB}', {T_SEED}, 1),
    ('{BETA}', 'visiting_or_postal', 'scb', 'Lillgatan 2', 'Lillgatan 2', NULL, '22100',
     'Lund', NULL, 'fixture', 'scb-2', 'hash-scb-2', 'uid-scb-2', {T_SEED}, 1,
     '{BETA_FP}', '{BETA_FP}', {T_SEED}, 1),
    ('{BETA}', 'postal', 'bolagsverket', NULL, NULL, NULL, NULL, NULL, NULL, 'fixture',
     'bv-2', 'hash-bv-2', 'uid-bv-2', {T_SEED}, 0, '{"0" * 64}', '{"0" * 64}', {T_SEED}, 1);

INSERT INTO corpscout.se_company_address_members_current
    (company_id, canonical_address_key, address_key, address_type, address_source, raw_address,
     display_address, street_address, care_of, postal_code, post_town, country_code,
     registry_source_record_uid, registry_source_run_id, source_observed_at,
     normalization_run_id, normalized_at)
VALUES
    ('{ALPHA}', '{CANONICAL_KEY}', '{ALPHA_FP_BOLAGSVERKET}', 'postal', 'bolagsverket', '', '',
     'Storgatan 1', '', '11122', 'Stockholm', 'SE', 'uid-bv-1', 'fixture', {T_SEED}, 'norm-1', {T_SEED}),
    ('{ALPHA}', '{CANONICAL_KEY_SCB}', '{ALPHA_FP_SCB}', 'visiting_or_postal', 'scb', '', '',
     'Storgatan 1', 'c/o Anna', '11122', 'Stockholm', 'SE', 'uid-scb-1', 'fixture', {T_SEED}, 'norm-1', {T_SEED});

INSERT INTO corpscout.se_company_address_links_current
    (company_id, address_id, canonical_address_key, address_types, address_sources, evidence_count,
     first_observed_at, last_observed_at, address_identity_run_id, address_identity_built_at)
VALUES
    ('{ALPHA}', '{ADDRESS_ID}', '{CANONICAL_KEY}', ['postal'], ['bolagsverket'], 1, {T_SEED}, {T_SEED}, 'ident-1', {T_SEED}),
    ('{ALPHA}', '{ADDRESS_ID_SCB}', '{CANONICAL_KEY_SCB}', ['visiting_or_postal'], ['scb'], 1, {T_SEED}, {T_SEED}, 'ident-1', {T_SEED});

INSERT INTO corpscout.se_address_geocodes_current
    (address_id, address_identity_run_id, normalized_match_key, match_status, candidate_count,
     candidate_record_ids, candidate_record_urls, match_method, match_confidence, latitude, longitude,
     geocode_provider, geocode_precision, coordinate_supporting_point_count, geocode_run_id, matched_at)
VALUES
    ('{ADDRESS_ID}', 'ident-1', 'storgatan 1|11122|stockholm', 'matched_exact', 1, [], [], 'exact',
     0.99, 59.33, 18.06, 'osm', 'building', 1, 'geo-1', {T_GEOCODE});
"""

ARTIFACTS = (
    ("se_company_address_bolagsverket", SE_COMPANY_ADDRESS_BOLAGSVERKET_COLUMNS,
     SE_COMPANY_ADDRESS_BOLAGSVERKET_SQL),
    ("se_company_address_scb", SE_COMPANY_ADDRESS_SCB_COLUMNS, SE_COMPANY_ADDRESS_SCB_SQL),
)
COUNTS_SQL = (
    "SELECT 'bolagsverket', count() FROM corpscout.se_company_address_bolagsverket"
    " UNION ALL SELECT 'scb', count() FROM corpscout.se_company_address_scb")


def _marked(label: str, query: str) -> str:
    return f"SELECT '@@{label}';\n{query} FORMAT TSV;\n"


def _publish_pass(table: str, columns: tuple[str, ...], select_sql: str, params: dict[str, object]) -> str:
    """Mirrors publish_with_stage(..., new_versions_only=True) in se_company/common.py:
    stage <- SELECT, then copy into the target only the rows whose (company_id,
    source_record_uid, evidence_hash) is not already there, via a LEFT ANTI JOIN. The
    anti-join text is copied verbatim from that function, which has no SQL-string builder
    to import."""
    col_list = ", ".join(columns)
    stage_cols = ", ".join(f"stage.{column}" for column in columns)
    stage = f"corpscout._tmp_{table}"
    anti_join = (
        f"FROM {stage} AS stage\n"
        f"LEFT ANTI JOIN corpscout.{table} AS existing\n"
        "ON existing.company_id = stage.company_id "
        "AND existing.source_record_uid = stage.source_record_uid "
        "AND existing.evidence_hash = stage.evidence_hash")
    return (
        f"DROP TABLE IF EXISTS {stage};\n"
        f"CREATE TABLE {stage} AS corpscout.{table};\n"
        f"INSERT INTO {stage} ({col_list})\n{_render(select_sql, params)};\n"
        f"INSERT INTO corpscout.{table} ({col_list})\nSELECT {stage_cols} {anti_join};\n"
        f"DROP TABLE {stage};\n")


def _changed_params(*, resolve_all: int = 0, resolve_all_before: str = "2099-12-31 23:59:59") -> dict[str, object]:
    """The scan's parameters. resolve_all_before is ALWAYS bound, resolve_all or not: the
    predicate's parseDateTime64BestEffort is parsed regardless of the flag beside it, so an
    empty string would be a query error -- which is exactly what this harness holds
    address.py to."""
    return {"all_companies": 1, "company_ids": ("",), "resolve_all": resolve_all,
            "resolve_all_before": resolve_all_before, "after_company_id": "", "page_size": 10}


def _final_row_values(company: str, key: str, *, is_current: str, resolved_at: str) -> str:
    """One final row, positionally bound to INSERT_COLUMNS."""
    return (f"('{company}', '{key}', 'postal', NULL, 'Storgatan 1', 'storgatan 1|11122|stockholm', "
            f"'111 22', 'Stockholm', NULL, '{ADDRESS_ID}', 59.33, 18.06, 'matched_exact', {T_GEOCODE}, "
            f"{is_current}, ['bolagsverket'], ['uid-bv-1'], ['{'9' * 64}'], [], '{RUN_ID}', {resolved_at})")


def _script(*, join_use_nulls: int) -> str:
    render_params = {"source_run_id": RUN_ID}
    parts: list[str] = []
    if join_use_nulls:
        parts.append("SET join_use_nulls = 1;")
    parts.append(";\n".join(_schema_statements(MIGRATIONS)) + ";")
    parts.append(FIXTURE)

    for table, columns, sql in ARTIFACTS:
        parts.append(_publish_pass(table, columns, sql, render_params))
    parts.append(_marked("counts", COUNTS_SQL))
    parts.append(SETTLE)
    # Second pass, same evidence: the anti-join must append nothing.
    for table, columns, sql in ARTIFACTS:
        parts.append(_publish_pass(table, columns, sql, render_params))
    parts.append(_marked("counts_after_rerun", COUNTS_SQL))

    parts.append(_marked("changed_empty_final",
                         _render(build_changed_companies_sql(), _changed_params())))
    parts.append(_marked("artifact_rows",
                         "SELECT source, company_id, source_record_uid, payload_json FROM ("
                         + _render(build_artifact_rows_sql(), {"company_ids": (ALPHA, BETA)})
                         + ") ORDER BY source, company_id"))
    parts.append(_marked("geocodes",
                         "SELECT company_id, address_fingerprint, address_id, has_geocode, latitude,"
                         " geocode_status FROM ("
                         + _render(build_geocodes_sql(), {"company_ids": (ALPHA, BETA)})
                         + ") ORDER BY company_id, address_fingerprint"))

    # Set replacement, as a storage round-trip. ALPHA_KEY is published current, then
    # tombstoned, then published current again -- each with a strictly newer resolved_at.
    columns = ", ".join(INSERT_COLUMNS)
    live_sql = ("SELECT company_id, toString(address_key), is_current FROM corpscout.se_company_address"
                " FINAL WHERE is_current ORDER BY company_id, address_key")
    parts.append(f"INSERT INTO corpscout.se_company_address ({columns}) VALUES "
                 + _final_row_values(ALPHA, "7" * 64, is_current="true", resolved_at="now64(3, 'UTC')") + ";")
    parts.append(_marked("live_after_first_publish", live_sql))
    parts.append(SETTLE)
    parts.append(f"INSERT INTO corpscout.se_company_address ({columns}) VALUES "
                 + _final_row_values(ALPHA, "7" * 64, is_current="false", resolved_at="now64(3, 'UTC')") + ";")
    parts.append(_marked("live_after_tombstone", live_sql))
    parts.append(_marked("published_rows",
                         "SELECT company_id, address_key, is_current FROM ("
                         + _render(build_published_rows_sql(), {"company_ids": (ALPHA,)})
                         + ") ORDER BY address_key"))
    parts.append(SETTLE)
    parts.append(f"INSERT INTO corpscout.se_company_address ({columns}) VALUES "
                 + _final_row_values(ALPHA, "7" * 64, is_current="true", resolved_at="now64(3, 'UTC')") + ";")
    parts.append(_marked("live_after_reappearance", live_sql))
    parts.append(_marked("changed_after_publish",
                         _render(build_changed_companies_sql(), _changed_params())))
    parts.append(_marked("changed_resolve_all",
                         _render(build_changed_companies_sql(),
                                 _changed_params(resolve_all=1))))
    return "\n".join(parts) + "\n"


@pytest.fixture(scope="module", params=(0, 1), ids=("join_use_nulls_off", "join_use_nulls_on"))
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    command = _clickhouse_local_command()
    try:
        completed = subprocess.run(command, input=_script(join_use_nulls=request.param),
                                   capture_output=True, text=True, timeout=900)
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    result: dict[str, list[list[str]]] = {}
    current = ""
    for line in completed.stdout.splitlines():
        if line.startswith("@@"):
            current = line[2:]
            result[current] = []
        elif current and line.strip():
            result[current].append(line.split("\t"))
    return result


def test_artifacts_publish_only_addressed_rows_and_are_idempotent(sections) -> None:
    """BETA's Bolagsverket row has has_address = 0 -- one row per (company, type, source)
    exists whether or not the register recorded anything, and a row with no address is not
    an address."""
    assert sorted(sections["counts"]) == [["bolagsverket", "1"], ["scb", "2"]]
    assert sections["counts_after_rerun"] == sections["counts"]


def test_the_artifact_payload_carries_the_fingerprint_the_geocode_join_needs(sections) -> None:
    payloads = {(row[0], row[1]): row[3] for row in sections["artifact_rows"]}
    assert ALPHA_FP_BOLAGSVERKET in payloads[("bolagsverket", ALPHA)]
    assert '"city":"Stockholm"' in payloads[("bolagsverket", ALPHA)]


def test_the_geocode_lookup_gates_the_miss_instead_of_reading_a_type_default(sections) -> None:
    rows = {row[1]: row for row in sections["geocodes"]}
    # Linked AND geocoded.
    assert rows[ALPHA_FP_BOLAGSVERKET][2] == ADDRESS_ID
    assert rows[ALPHA_FP_BOLAGSVERKET][3] == "1" and rows[ALPHA_FP_BOLAGSVERKET][5] == "matched_exact"
    # Linked, NOT geocoded: address_id present, status EMPTY -- not the type default.
    assert rows[ALPHA_FP_SCB][2] == ADDRESS_ID_SCB
    assert rows[ALPHA_FP_SCB][3] == "0" and rows[ALPHA_FP_SCB][5] == ""
    assert rows[ALPHA_FP_SCB][4] == ""  # latitude, a genuinely Nullable source column
    # Never linked: absent entirely (the members -> links join is INNER).
    assert BETA_FP not in rows


def test_the_scan_selects_every_company_before_anything_is_published(sections) -> None:
    assert [row[0] for row in sections["changed_empty_final"]] == sorted([ALPHA, BETA])
    never_published = {row[0]: row[1] for row in sections["changed_empty_final"]}
    assert set(never_published.values()) == {"1"}


def test_a_tombstone_hides_the_address_and_a_reappearance_brings_it_back(sections) -> None:
    """The set-replacement contract at the storage layer: ReplacingMergeTree(resolved_at)
    keeps the newest version, and readers filter FINAL ... WHERE is_current."""
    assert [row[1] for row in sections["live_after_first_publish"]] == ["7" * 64]
    assert sections["live_after_tombstone"] == []
    assert [row[1] for row in sections["live_after_reappearance"]] == ["7" * 64]


def test_the_published_read_back_sees_the_tombstone_not_the_older_live_version(sections) -> None:
    assert sections["published_rows"] == [[ALPHA, "7" * 64, "false"]]


def test_a_published_company_is_quiet_until_resolve_all_asks_for_it(sections) -> None:
    """ALPHA's final rows are newer than every artifact and than the geocode snapshot, so
    only the resolve_all disjunct can select it again."""
    assert ALPHA not in [row[0] for row in sections["changed_after_publish"]]
    assert ALPHA in [row[0] for row in sections["changed_resolve_all"]]


def test_the_deployed_final_columns_are_what_the_ddl_replay_says(sections) -> None:
    """The INSERT above binds INSERT_COLUMNS positionally; had the migration and the module
    disagreed, the script would have failed before any section was produced."""
    assert list(INSERT_COLUMNS) == [column for column in declared_columns("se_company_address")
                                    if column != "evidence_set_hash"]
```

- [ ] **Step 2: Run** — `uv run pytest tests/test_se_company_address_clickhouse_local.py -q`
Expected: PASS on both parametrizations with a `clickhouse-local` binary or Docker (`skipped` otherwise — if it skips on your machine, say so in the report; the controller verifies on a machine with Docker). Fix any SQL the engine rejects and re-run Tasks 2–5's unit tests afterwards.

- [ ] **Step 3: Commit**

```bash
git add corpscout/services/dagster_v3/tests/test_se_company_address_clickhouse_local.py
git commit -m "test(se_company): execute the address pipeline queries against clickhouse-local"
```

**Phase 3 ends here.** Report: test results, `dg check defs` output, and whether the harness ran or skipped.

---

### Task 7: Backoffice — validator, writer and server module for address corrections

**Files:**
- Create: `corpscout/services/backoffice/app/lib/se-address-corrections.ts` (client-safe)
- Modify: `corpscout/services/backoffice/app/lib/clickhouse.server.ts` (add `chInsertSeCompanyAddressCorrections` next to `chInsertSeCompanyInfoCorrections`)
- Rewrite: `corpscout/services/backoffice/app/lib/se-company-address.server.ts` (reads the final instead of the raw chain; adds the ledger reads and the append)
- Test: `corpscout/services/backoffice/tests/se-address-corrections.test.ts`, `tests/se-company-address.server.test.ts`; extend `tests/clickhouse-writer.server.test.ts`

**Interfaces:**
- Produces (`se-address-corrections.ts`): `SE_ADDRESS_CORRECTION_KINDS = ["override_field", "reject_address", "undo"] as const`; `SE_ADDRESS_CORRECTION_STATUSES = ["pending", "applied", "stale", "undone"] as const`; `SeAddressCorrectionValidationError`; `SeAddressCorrectionInput { companyId; kind; payload?; evidenceHash; reason; supersedesCorrectionId? }`; `SeAddressCorrectionDraft { company_id; correction_kind; payload: string; evidence_hash; reason; supersedes_correction_id: string | null }`; `validateSeAddressCorrection(input): SeAddressCorrectionDraft`; `liveOverrideCorrectionId(corrections, addressKey): string | null`. Reuses `ZERO_EVIDENCE_HASH` from `~/lib/se-person-corrections`, as `se-info-corrections.ts` does.
- Produces (`se-company-address.server.ts`): `ADDRESSES_SQL`, `CORRECTIONS_SQL` (exported for tests); `SeCompanyAddressRow`, `SeCompanyAddressCorrectionRow`, `SeCompanyAddressDetail { addresses; corrections }`; `loadSeCompanyAddresses(companyId): Promise<SeCompanyAddressDetail>`; `appendSeCompanyAddressCorrection(input): Promise<{ correctionId: string }>`.
- Consumes: `chQuery`, `getWriteClient` (`clickhouse.server.ts`); the Dagster-side contract from Task 4 (payload keys, kind ranking, staleness by the named row's `evidence_set_hash`).

- [ ] **Step 1: Failing tests**

```ts
// tests/se-address-corrections.test.ts
import { describe, expect, it } from "vitest";
import {
  SeAddressCorrectionValidationError,
  validateSeAddressCorrection,
  ZERO_EVIDENCE_HASH,
} from "~/lib/se-address-corrections";

const KEY = "a".repeat(64);
const HASH = "b".repeat(64);
const base = { companyId: "5565200028", evidenceHash: HASH, reason: "Fixed the care-of line." };

describe("validateSeAddressCorrection", () => {
  it("accepts a partial override and keeps absent fields absent", () => {
    const draft = validateSeAddressCorrection({
      ...base, kind: "override_field", payload: { address_key: KEY, care_of: "c/o Anna" },
    });
    expect(JSON.parse(draft.payload)).toEqual({ address_key: KEY, care_of: "c/o Anna" });
  });

  it("passes an explicit null through as a decision to clear the field", () => {
    const draft = validateSeAddressCorrection({
      ...base, kind: "override_field", payload: { address_key: KEY, care_of: null },
    });
    expect(JSON.parse(draft.payload)).toEqual({ address_key: KEY, care_of: null });
  });

  it("refuses an override that names no field, an unknown field, or the key itself", () => {
    for (const payload of [{ address_key: KEY }, { address_key: KEY, legal_name: "x" },
                           { address_key: KEY, address_type: "postal" }, { care_of: "x" }]) {
      expect(() => validateSeAddressCorrection({ ...base, kind: "override_field", payload }))
        .toThrow(SeAddressCorrectionValidationError);
    }
  });

  it("accepts a reject that names only the address key", () => {
    const draft = validateSeAddressCorrection({ ...base, kind: "reject_address", payload: { address_key: KEY } });
    expect(draft.correction_kind).toBe("reject_address");
    expect(JSON.parse(draft.payload)).toEqual({ address_key: KEY });
  });

  it("requires the zero hash and a superseded id for undo, and forbids both elsewhere", () => {
    const undo = validateSeAddressCorrection({
      ...base, kind: "undo", evidenceHash: ZERO_EVIDENCE_HASH,
      supersedesCorrectionId: "3f2b1c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
    });
    expect(undo.supersedes_correction_id).not.toBeNull();
    expect(() => validateSeAddressCorrection({ ...base, kind: "undo", evidenceHash: HASH,
      supersedesCorrectionId: "3f2b1c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d" })).toThrow();
    expect(() => validateSeAddressCorrection({ ...base, kind: "reject_address",
      payload: { address_key: KEY }, supersedesCorrectionId: "3f2b1c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d" })).toThrow();
  });

  it("accepts both Swedish company id widths and rejects anything else", () => {
    expect(validateSeAddressCorrection({ ...base, companyId: "196408233412", kind: "reject_address",
      payload: { address_key: KEY } }).company_id).toBe("196408233412");
    expect(() => validateSeAddressCorrection({ ...base, companyId: "55652000",
      kind: "reject_address", payload: { address_key: KEY } })).toThrow();
  });

  it("requires a 64-hex address key on every non-undo kind", () => {
    expect(() => validateSeAddressCorrection({ ...base, kind: "reject_address", payload: { address_key: "nope" } }))
      .toThrow(/address key/i);
  });
});
```

```ts
// tests/se-company-address.server.test.ts
import { describe, expect, it } from "vitest";
import { ADDRESSES_SQL, CORRECTIONS_SQL } from "~/lib/se-company-address.server";

describe("ADDRESSES_SQL", () => {
  it("reads the final, newest version only, live rows only", () => {
    expect(ADDRESSES_SQL).toContain("FROM corpscout.se_company_address AS a FINAL");
    expect(ADDRESSES_SQL).toContain("WHERE a.company_id = {companyId:String}");
    expect(ADDRESSES_SQL).toContain("AND a.is_current");
    // The raw chain is gone: the geocode now travels on the published row.
    expect(ADDRESSES_SQL).not.toContain("se_company_addresses_current");
    expect(ADDRESSES_SQL).not.toContain("se_company_address_display_current");
  });

  it("exposes the evidence hash the correction form has to echo back", () => {
    expect(ADDRESSES_SQL).toContain("toString(a.evidence_set_hash) AS evidence_set_hash");
    expect(ADDRESSES_SQL).toContain("toString(a.address_key) AS address_key");
  });

  it("collapses genuinely Nullable columns to text rather than mapping null in TypeScript", () => {
    expect(ADDRESSES_SQL).toContain("ifNull(a.care_of, '') AS care_of");
    expect(ADDRESSES_SQL).toContain("ifNull(toString(a.latitude), '') AS latitude");
  });
});

describe("CORRECTIONS_SQL", () => {
  it("computes status against the published rows of this company", () => {
    expect(CORRECTIONS_SQL).toContain("FROM corpscout.se_company_address_correction AS c");
    expect(CORRECTIONS_SQL).toContain("{evidenceSetHashes:Array(String)}");
    expect(CORRECTIONS_SQL).toContain("AS is_current");
    expect(CORRECTIONS_SQL).toContain("AS is_stale");
    expect(CORRECTIONS_SQL).toContain("AS is_applied");
  });
});
```

Extend `tests/clickhouse-writer.server.test.ts` with the twin of its info case: `chInsertSeCompanyAddressCorrections` inserts into `se_company_address_correction` with `format: "JSONEachRow"` and no-ops on an empty array.

- [ ] **Step 2: Run to verify failure** — `npx vitest run tests/se-address-corrections.test.ts tests/se-company-address.server.test.ts` → module not found

- [ ] **Step 3: Implement `app/lib/se-address-corrections.ts`**

```ts
/**
 * Client-safe validator for the Sweden company-address correction ledger
 * (se_company_address_correction). Mirrors se-info-corrections.ts, with two
 * differences that come from the datatype rather than from taste:
 *
 * - every payload names the `address_key` it decides, because a company has
 *   several published addresses and a correction without one has no subject;
 * - there is no approve/reject of a model suggestion, because nothing in this
 *   datatype is model-written. `reject_address` is not that: it says "this is
 *   not an address of this company", and Dagster publishes the row
 *   is_current = false.
 *
 * Reuses ZERO_EVIDENCE_HASH from se-person-corrections since all three ledgers
 * share the "undo carries no evidence" convention.
 */
import { ZERO_EVIDENCE_HASH } from "~/lib/se-person-corrections";

export { ZERO_EVIDENCE_HASH };

export const SE_ADDRESS_CORRECTION_KINDS = [
  "override_field",
  "reject_address",
  "undo",
] as const;

export type SeAddressCorrectionKind = (typeof SE_ADDRESS_CORRECTION_KINDS)[number];

/**
 * A ledger row's status relative to the published rows, as the
 * `/admin/se/company-address/corrections` list computes it in SQL. Declared
 * here (client-safe) so the list's filter can import the value set instead of
 * keeping a second copy.
 */
export const SE_ADDRESS_CORRECTION_STATUSES = ["pending", "applied", "stale", "undone"] as const;

export type SeAddressCorrectionStatus = (typeof SE_ADDRESS_CORRECTION_STATUSES)[number];

export class SeAddressCorrectionValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SeAddressCorrectionValidationError";
  }
}

export interface SeAddressCorrectionInput {
  companyId: string;
  kind: string;
  payload?: Record<string, unknown>;
  evidenceHash: string;
  reason: string;
  supersedesCorrectionId?: string | null;
}

export interface SeAddressCorrectionDraft {
  company_id: string;
  correction_kind: SeAddressCorrectionKind;
  payload: string;
  evidence_hash: string;
  reason: string;
  supersedes_correction_id: string | null;
}

// Legal entities carry a 10-digit organisationsnummer; sole traders carry a
// 12-digit personnummer-based id. Mirrors has_company in migration 000307.
const COMPANY_ID_PATTERN = /^([0-9]{10}|[0-9]{12})$/;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const HASH_PATTERN = /^[0-9a-f]{64}$/;

// The text fields a reviewer may decide. address_type is NOT among them: it is
// part of address_key, so overriding it would move the row to a different
// identity -- Dagster's OVERRIDABLE_FIELDS says exactly the same thing, and the
// two lists must stay in step.
const OVERRIDABLE_FIELDS = [
  "care_of",
  "street_address",
  "normalized_address",
  "postal_code",
  "city",
  "country_code",
] as const;

const ALLOWED_PAYLOAD_KEYS: Record<SeAddressCorrectionKind, readonly string[]> = {
  override_field: ["address_key", ...OVERRIDABLE_FIELDS],
  reject_address: ["address_key"],
  undo: [],
};

function fail(message: string): never {
  throw new SeAddressCorrectionValidationError(message);
}

function isKind(value: string): value is SeAddressCorrectionKind {
  return (SE_ADDRESS_CORRECTION_KINDS as readonly string[]).includes(value);
}

function addressKeyOrFail(payload: Record<string, unknown>): string {
  const value = typeof payload.address_key === "string" ? payload.address_key.trim().toLowerCase() : "";
  if (!HASH_PATTERN.test(value)) fail("The address key is missing or malformed.");
  return value;
}

export function validateSeAddressCorrection(
  input: SeAddressCorrectionInput,
): SeAddressCorrectionDraft {
  const companyId = input.companyId.replace(/[^0-9]/g, "");
  if (!COMPANY_ID_PATTERN.test(companyId) || companyId !== input.companyId.trim()) {
    fail("Company must be a 10-digit or 12-digit Swedish company id.");
  }
  if (!isKind(input.kind)) fail("Unknown correction kind.");
  const kind = input.kind;

  const evidenceHash = input.evidenceHash.trim().toLowerCase();
  if (!HASH_PATTERN.test(evidenceHash)) fail("The evidence hash is missing or malformed.");

  const reason = input.reason.trim();
  if (reason === "" || reason.length > 1000) fail("Reason is required (max 1000 characters).");

  // Scope supersedes_correction_id to undo only.
  if (kind !== "undo" && input.supersedesCorrectionId) {
    fail("Only undo may supersede a correction.");
  }

  const payload = input.payload ?? {};
  for (const key of Object.keys(payload)) {
    if (!ALLOWED_PAYLOAD_KEYS[kind].includes(key)) {
      fail(`Payload key "${key}" is not allowed for ${kind}.`);
    }
  }

  const cleanPayload: Record<string, unknown> = {};
  let supersedesCorrectionId: string | null = null;

  switch (kind) {
    case "override_field": {
      if (evidenceHash === ZERO_EVIDENCE_HASH) fail("The evidence hash is missing or malformed.");
      cleanPayload.address_key = addressKeyOrFail(payload);
      let named = 0;
      for (const field of OVERRIDABLE_FIELDS) {
        if (!(field in payload)) continue; // absent means "leave it as computed"
        const value = payload[field];
        if (value !== null && typeof value !== "string") {
          fail(`Override ${field} must be a string or null.`);
        }
        const trimmed = typeof value === "string" ? value.trim() : null;
        // "" is not a decision: clearing a field is an explicit null.
        cleanPayload[field] = trimmed === "" ? null : trimmed;
        named += 1;
      }
      if (named === 0) fail("Override needs at least one address field.");
      break;
    }
    case "reject_address": {
      if (evidenceHash === ZERO_EVIDENCE_HASH) fail("The evidence hash is missing or malformed.");
      cleanPayload.address_key = addressKeyOrFail(payload);
      break;
    }
    case "undo": {
      if (!input.supersedesCorrectionId) fail("Undo needs the correction it supersedes.");
      const superseded = input.supersedesCorrectionId.trim().toLowerCase();
      if (!UUID_PATTERN.test(superseded)) fail("Superseded correction must be a UUID.");
      supersedesCorrectionId = superseded;
      if (evidenceHash !== ZERO_EVIDENCE_HASH) fail("Undo must carry the zero evidence hash.");
      break;
    }
  }

  return {
    company_id: companyId,
    correction_kind: kind,
    payload: JSON.stringify(cleanPayload),
    evidence_hash: evidenceHash,
    reason,
    supersedes_correction_id: supersedesCorrectionId,
  };
}

/**
 * Dagster's `apply_address_ledger` drops a stale correction before ranking, and
 * among what is left ADDRESS_KIND_ORDER ranks `override_field` after
 * `reject_address` -- so a live override's field values always survive, whatever
 * order the reviewer decided in. That is fine for the two kinds to coexist (they
 * decide different things), but a SECOND override of the same row is not: the
 * later one wins by created_at and the earlier is invisible. The page uses this
 * to show "this address is already overridden" and offer undo instead.
 *
 * A "live" override is one no later `undo` supersedes. Sorts the array itself by
 * `created_at DESC, correction_id DESC`, so arrival order does not matter.
 */
export function liveOverrideCorrectionId(
  corrections: ReadonlyArray<{
    correction_id: string;
    correction_kind: string;
    address_key: string;
    supersedes_correction_id: string | null;
    is_current: number;
    is_stale: number;
    created_at: string;
  }>,
  addressKey: string,
): string | null {
  const supersededIds = new Set(
    corrections
      .filter((row) => row.correction_kind === "undo" && row.supersedes_correction_id)
      .map((row) => row.supersedes_correction_id as string),
  );
  const live = corrections.filter(
    (row) =>
      row.correction_kind === "override_field" &&
      row.address_key === addressKey &&
      row.is_current === 1 &&
      row.is_stale === 0 &&
      !supersededIds.has(row.correction_id),
  );
  if (live.length === 0) return null;
  live.sort((a, b) => {
    if (a.created_at !== b.created_at) return a.created_at > b.created_at ? -1 : 1;
    return a.correction_id > b.correction_id ? -1 : 1;
  });
  return live[0].correction_id;
}
```

- [ ] **Step 4: Add the writer to `app/lib/clickhouse.server.ts`**

Directly after `chInsertSeCompanyInfoCorrections`, with the same shape and a one-line doc comment ("Append reviewer decisions to the Sweden company-address correction ledger; Dagster's sensor picks them up"):

```ts
export async function chInsertSeCompanyAddressCorrections<T extends object>(
  values: T[],
): Promise<void> {
  if (values.length === 0) return;
  await getWriteClient().insert({
    table: "se_company_address_correction",
    values,
    format: "JSONEachRow",
  });
}
```

- [ ] **Step 5: Rewrite `app/lib/se-company-address.server.ts`**

The module keeps its name and its exported loader name (`loadSeCompanyAddresses`) so the route need not move; what changes is where it reads. Structure and required contents (the tests above pin each clause; `se-company-info.server.ts` is the literal pattern for the corrections block and the append):

1. **`ADDRESSES_SQL`** — `SELECT` from `corpscout.se_company_address AS a FINAL WHERE a.company_id = {companyId:String} AND a.is_current ORDER BY a.address_type, a.address_key LIMIT 100`. Projects, all as text so "absent" is one value (`""`) rather than null/0/undefined: `toString(a.address_key) AS address_key`, `toString(a.address_type) AS address_type`, `ifNull(a.care_of, '') AS care_of` (and the same for `street_address`, `normalized_address`, `postal_code`, `city`, `country_code`), `ifNull(toString(a.address_id), '') AS address_id`, `ifNull(toString(a.latitude), '') AS latitude`, `ifNull(toString(a.longitude), '') AS longitude`, `toString(a.geocode_status) AS geocode_status`, `ifNull(toString(a.geocoded_at), '') AS geocoded_at`, `a.sources AS sources`, `a.source_record_uids AS source_record_uids`, `toString(a.evidence_set_hash) AS evidence_set_hash`, `arrayMap(x -> toString(x), a.correction_ids) AS correction_ids`, `toString(a.resolved_at) AS resolved_at`. **No joins at all** — the geocode travels on the published row, which is the point of the datatype. A header comment says so and names what it replaced (the six-table LEFT JOIN chain, kept in git history), so nobody re-adds it.
2. **`CORRECTIONS_SQL`** — the twin of the info one, with `superseded` as a `WITH` clause, `is_current` = not superseded, `is_applied` = `has({appliedIds:Array(String)}, toString(c.correction_id))`, and `is_stale` = current AND `toString(c.evidence_hash) != {zeroHash:String}` AND `toString(c.evidence_hash) NOT IN {evidenceSetHashes:Array(String)}` — a correction is stale when its hash matches no LIVE row of the company, which covers both "the evidence moved" and "the address it named is gone". Also projects `JSONExtractString(c.payload, 'address_key') AS address_key`, which is what groups a correction under its address card.
3. **`loadSeCompanyAddresses(companyId)`** — one `chQuery` for the addresses, then one for the corrections, passing `evidenceSetHashes` (every live row's hash), `appliedIds` (the union of the live rows' `correction_ids`) and `zeroHash`. Returns `{ addresses, corrections }`. A company with no published address returns `{ addresses: [], corrections: [] }` — a normal pipeline state, not an error.
4. **`appendSeCompanyAddressCorrection(input)`** — validates, then (except for `undo`) re-reads the named row's hash:
   ```ts
   const [current] = await chQuery<{ evidence_set_hash: string }>(
     `SELECT toString(a.evidence_set_hash) AS evidence_set_hash
      FROM corpscout.se_company_address AS a FINAL
      WHERE a.company_id = {companyId:String} AND a.address_key = {addressKey:String} AND a.is_current
      LIMIT 1`,
     { companyId: draft.company_id, addressKey: JSON.parse(draft.payload).address_key },
   );
   if (!current) throw new SeAddressCorrectionValidationError("This address is not published.");
   if (current.evidence_set_hash !== draft.evidence_hash) {
     throw new SeAddressCorrectionValidationError(
       "The evidence changed while you were reviewing. Reload and decide again.");
   }
   ```
   then inserts one row through `chInsertSeCompanyAddressCorrections` with `correction_id: randomUUID()`, `decided_by: CORRECTION_ACTOR`, `created_at` formatted `YYYY-MM-DD HH:MM:SS.mmm` — all exactly as `appendSeCompanyInfoCorrection` does.

- [ ] **Step 6: Run**

```bash
cd corpscout/services/backoffice
npx vitest run tests/se-address-corrections.test.ts tests/se-company-address.server.test.ts tests/clickhouse-writer.server.test.ts
pnpm typecheck && npx react-router build && rg -l clickhouse build/client
```
Expected: tests PASS, typecheck clean, build green, `rg` prints nothing (a non-empty result means a `.server` module reached the client bundle — fix before committing).

- [ ] **Step 7: Commit**

```bash
git add corpscout/services/backoffice/app/lib/se-address-corrections.ts \
        corpscout/services/backoffice/app/lib/se-company-address.server.ts \
        corpscout/services/backoffice/app/lib/clickhouse.server.ts \
        corpscout/services/backoffice/tests/se-address-corrections.test.ts \
        corpscout/services/backoffice/tests/se-company-address.server.test.ts \
        corpscout/services/backoffice/tests/clickhouse-writer.server.test.ts
git commit -m "feat(backoffice): read se_company_address and write its correction ledger"
```

---

### Task 8: Backoffice — Address tab corrections UI and the ledger list page

**Files:**
- Create: `corpscout/services/backoffice/app/lib/se-address-review-form.ts` (client-safe)
- Modify: `corpscout/services/backoffice/app/components/admin/se-company-address.tsx`
- Modify: `corpscout/services/backoffice/app/routes/admin-se-company-address.tsx` (add `action`)
- Create: `corpscout/services/backoffice/app/lib/se-company-address-lists.server.ts`
- Create: `corpscout/services/backoffice/app/components/admin/se-company-address-corrections-table.tsx`
- Create: `corpscout/services/backoffice/app/routes/admin-se-company-address-corrections.tsx`
- Modify (owner WIP — edit, do NOT commit, list in the report): `corpscout/services/backoffice/app/routes.ts`, `app/components/admin/admin-sidebar.tsx`, `app/routes/admin-layout.tsx`
- Test: `corpscout/services/backoffice/tests/se-address-review-form.test.ts`, `tests/admin-se-company-address.test.tsx`, `tests/se-company-address-lists.server.test.ts`

**Interfaces:**
- Produces (`se-address-review-form.ts`): `buildCorrectionInput(form: FormData, context: { companyId: string }): { ok: true; input: SeAddressCorrectionInput } | { ok: false; error: string }`; `liveOverrideRefusal(kind: string, addressKey: string, corrections): string | null`.
- Produces (`se-company-address-lists.server.ts`): `ADDRESS_CORRECTION_LIST_SQL`, `ADDRESS_CORRECTION_COUNTS_SQL`, `resolveCorrectionsSort`, `listSeCompanyAddressCorrectionsPage(query): Promise<SeCompanyAddressCorrectionListPage>`, `loadSeCompanyAddressCorrectionFilterOptions()`. Mirrors `se-company-info-lists.server.ts` clause for clause, including `PAGE_LIMIT_OFFSET_SQL` and the status expression.
- Consumes: Task 7's validator and server module.

- [ ] **Step 1: Failing tests**

```ts
// tests/se-address-review-form.test.ts
import { describe, expect, it } from "vitest";
import { buildCorrectionInput } from "~/lib/se-address-review-form";

const KEY = "a".repeat(64);

function form(entries: Record<string, string>): FormData {
  const data = new FormData();
  for (const [name, value] of Object.entries(entries)) data.append(name, value);
  return data;
}

describe("buildCorrectionInput", () => {
  it("sends only the fields the reviewer actually changed", () => {
    const built = buildCorrectionInput(
      form({ kind: "override_field", address_key: KEY, evidence_hash: "b".repeat(64),
             reason: "Care-of was wrong.", care_of: "c/o Anna", original_care_of: "c/o Bo",
             city: "Stockholm", original_city: "Stockholm" }),
      { companyId: "5565200028" });
    expect(built.ok).toBe(true);
    if (built.ok) expect(built.input.payload).toEqual({ address_key: KEY, care_of: "c/o Anna" });
  });

  it("turns a clear checkbox into an explicit null", () => {
    const built = buildCorrectionInput(
      form({ kind: "override_field", address_key: KEY, evidence_hash: "b".repeat(64),
             reason: "No care-of on this address.", care_of: "c/o Bo", original_care_of: "c/o Bo",
             clear_care_of: "yes" }),
      { companyId: "5565200028" });
    if (built.ok) expect(built.input.payload).toEqual({ address_key: KEY, care_of: null });
  });

  it("refuses an override where nothing moved", () => {
    const built = buildCorrectionInput(
      form({ kind: "override_field", address_key: KEY, evidence_hash: "b".repeat(64),
             reason: "No change.", care_of: "c/o Bo", original_care_of: "c/o Bo" }),
      { companyId: "5565200028" });
    expect(built.ok).toBe(false);
  });
});
```

```tsx
// tests/admin-se-company-address.test.tsx -- rendering contract, not styling
// Renders <SeCompanyAddressTab detail={...} result={null} /> with two addresses, one
// geocoded and one not, one carrying a stale correction. Asserts:
//   - one card per published address, each showing its address_type label
//   - a Sources badge per contributing source, in the order the row lists them
//   - the geocode block renders exactly as it does today for a geocoded row
//     (status, coordinates, the OpenStreetMap link) and says so plainly when absent
//   - an "evidence changed" marker on the card whose correction is stale
//   - an override form per card carrying hidden address_key + evidence_hash inputs
//   - a Reject address control per card, and an Undo control on a card with a live
//     override (the form for the second is disabled with the refusal text instead)
```

```ts
// tests/se-company-address-lists.server.test.ts -- SQL contract, mirrors
// tests/se-company-info-lists.server.test.ts: the list reads
// corpscout.se_company_address_correction, computes the four statuses, filters by
// kind/status/company, sorts by an allow-listed column, and pages with
// {limit:UInt32}/{offset:UInt32}.
```

- [ ] **Step 2: Run to verify failure** — `npx vitest run tests/se-address-review-form.test.ts tests/admin-se-company-address.test.tsx tests/se-company-address-lists.server.test.ts`

- [ ] **Step 3: Implement `se-address-review-form.ts`**

Client-safe on purpose: React Router only strips `loader`/`action`/`middleware`/`headers` from a route module, so any other export that reaches a `.server` module drags it into the client bundle and fails the production build. Mirrors `se-info-review-form.ts`:

- `overrideField(form, name)` — `{ value, original, cleared, changed }`, where `changed` is `cleared || (value !== "" && value !== original)`. Diffing against the text the reviewer was SHOWN is not cosmetic: a correction is replayed on every Dagster run, so sending an untouched field would pin it forever.
- `payloadFor(form, kind)` — for `override_field`, `{ address_key }` plus every changed field (`null` when cleared); for `reject_address`, `{ address_key }`; for `undo`, `{}`.
- `buildCorrectionInput(form, { companyId })` — reads `kind`, `evidence_hash` (the zero hash for undo), `reason`, `supersedes_correction_id`, returns `{ ok: false, error }` when the kind is unknown or an override changed nothing, otherwise `{ ok: true, input }`.
- `liveOverrideRefusal(kind, addressKey, corrections)` — returns the message to show when a second `override_field` is attempted on a row that already carries a live one ("This address already has a live override — undo it before overriding again."), otherwise `null`.

- [ ] **Step 4: Rewrite `se-company-address.tsx`**

Keep what already works: `ADDRESS_TYPE_LABELS`/`addressTypeLabel`, `openStreetMapHref`, the `Card` + `DefinitionList` layout, the `Empty` state. Change the props to `{ detail, result }`, render one card per `detail.addresses` row and, per card:

- header: the address-type label, one `<Badge variant="outline">` per entry of `row.sources` (in order — precedence is visible), and a `<Badge variant="secondary">evidence changed</Badge>` when a correction on this key is stale;
- body: the `DefinitionList` over care-of / street / postal code / city / country / normalized address, then the **unchanged** geocode block (status, coordinates, the OpenStreetMap link at zoom 18, "not geocoded" when `geocode_status === ""`), then a collapsible "Provenance" line listing `source_record_uids` and `resolved_at`;
- a `<Form method="post">` with hidden `kind`, `address_key`, `evidence_hash`, one input per overridable field plus its `original_*` mirror and `clear_*` checkbox, a required `reason`, and a submit; a second small form posting `kind=reject_address`; and, on the corrections list beneath, an Undo button per current correction posting `kind=undo` + `supersedes_correction_id` + the zero hash.

Export `SeCompanyAddressTab` (props `{ detail, result }`) and keep a `SeCompanyAddressEmpty` for the not-published case.

- [ ] **Step 5: Add the route `action` and the ledger list page**

`admin-se-company-address.tsx` gains an `action` that is the twin of `admin-se-company-info.tsx`'s: build the input from the form, refuse a second live override via `liveOverrideRefusal`, call `appendSeCompanyAddressCorrection`, map `SeAddressCorrectionValidationError` to `{ ok: false, error }` and re-throw anything else. Only `loader`, `action`, `meta` and the component live in that file.

`admin-se-company-address-corrections.tsx` + `se-company-address-corrections-table.tsx` mirror the info corrections page and table, reading `se_company_address_correction` and adding one column the info list does not have: the `address_key` the correction names (rendered short, linking to the company's Address tab).

Register in `app/routes.ts` beside the info ones (owner WIP file — leave uncommitted):

```ts
route("se/company-address/corrections", "routes/admin-se-company-address-corrections.tsx"),
```

- [ ] **Step 6: Run**

```bash
cd corpscout/services/backoffice
npx vitest run tests/se-address-review-form.test.ts tests/admin-se-company-address.test.tsx \
              tests/se-company-address-lists.server.test.ts tests/admin-se-company-area.test.tsx
pnpm typecheck && npx react-router build && rg -l clickhouse build/client
```
Expected: PASS / clean / green / no output from `rg`.

- [ ] **Step 7: Commit** (by explicit path; `routes.ts` and the sidebar/breadcrumb files stay uncommitted and are listed in the report)

```bash
git add corpscout/services/backoffice/app/lib/se-address-review-form.ts \
        corpscout/services/backoffice/app/lib/se-company-address-lists.server.ts \
        corpscout/services/backoffice/app/components/admin/se-company-address.tsx \
        corpscout/services/backoffice/app/components/admin/se-company-address-corrections-table.tsx \
        corpscout/services/backoffice/app/routes/admin-se-company-address.tsx \
        corpscout/services/backoffice/app/routes/admin-se-company-address-corrections.tsx \
        corpscout/services/backoffice/tests/se-address-review-form.test.ts \
        corpscout/services/backoffice/tests/admin-se-company-address.test.tsx \
        corpscout/services/backoffice/tests/se-company-address-lists.server.test.ts
git commit -m "feat(backoffice): address review UI and the address correction ledger page"
```

**Phase 6 ends here.**

---

### Task 9 (Phase 8): Retirement migration (the next free number — 000312 at time of writing — esef took 000309–000311; re-check `ls corpscout/clickhouse/migrations | tail -1` before creating) with inline zero-reader gates

**Do not start this task before Phase 7 has closed.** It is written last, reviewed by the controller, and applied by the controller (Task 10e). Its whole safety argument is that the final has been serving for a while and the tables below have no reader left.

Two tables retire: `se_company_addresses_canonical_current` (the per-company canonical merge) and `se_company_address_display_current` (+ its dbt build model, whose geocode columns were never wired to anything). The legacy geocoder pair (`se_company_address_geocodes`, `se_company_address_geocode_results`) and the whole shared-identity chain (`se_addresses_current`, links, members, `se_address_geocodes_current`) **STAY** — the chain is the geocode augmentation source, and the legacy pair is the parity baseline the owner decides about separately.

**Files:**
- Create: `corpscout/clickhouse/migrations/<NNNNNN>_corpscout_retire_se_address_serving_tables.up.sql`, `.down.sql` (`<NNNNNN>` = the next free number — 000312 at time of writing — esef took 000309–000311; re-check `ls corpscout/clickhouse/migrations | tail -1` before creating)
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/tables.py` (remove the `ADDRESSES` `CurrentTable`)
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/company_section_presence_current_build.sql` (its `addresses` branch `ref`s the model being deleted)
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/schema.yml`
- Delete: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/se_company_address_display_current_build.sql`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_geocoding_assets.py` (stop publishing the canonical table to ClickHouse) and `corpscout/services/dagster_v3/tests/test_sweden_company_address_geocoding.py`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py`, `tests/test_company_serving_dbt.py`

**Interfaces:** consumes nothing; produces two fewer tables and one fewer dbt model. `sweden_company_canonical_addresses_duckdb` keeps building the canonical addresses in DuckDB — the members bridge is derived from them — so only the ClickHouse *publish* of the per-company canonical table goes.

- [ ] **Step 1: Re-verify zero readers, freshly, and record the output**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg -n "se_company_addresses_canonical_current" corpscout --glob '!*.pyc'
rg -n "se_company_address_display_current" corpscout --glob '!*.pyc'
```

Known hits as of 2026-08-24 (each must be gone or accounted for before the drop):

| hit | table | action |
|---|---|---|
| `clickhouse/migrations/000267,000273,000278` (up/down) | both | historical DDL — leave, they are the ledger |
| `defs/company_serving/tables.py` (`ADDRESSES`) | display | remove the `CurrentTable` entry |
| `defs/company_serving/dbt/models/se_company_address_display_current_build.sql` | display | delete the model |
| `defs/company_serving/dbt/models/company_section_presence_current_build.sql` | display | repoint (Step 2) |
| `defs/company_serving/dbt/models/schema.yml` | display | remove the model's entry |
| `defs/sweden_company/address_canonicalization.py` | canonical | it BUILDS the table — drop only the ClickHouse publish, keep the DuckDB build |
| `defs/sweden_company/address_geocoding_assets.py` | canonical | remove the paired ClickHouse output + its AssetCheck |
| `services/backoffice/app/lib/se-company-address.server.ts` | display | already gone after Task 7 — confirm the rg returns nothing here |
| `services/backoffice/tests/company-serving-sections.test.ts`, `tests/test_company_serving_dbt.py`, `tests/test_sweden_company_address_geocoding.py` | both | update with the code |

If the `rg` shows a hit not in this table, **stop and report** — a new reader appeared since this plan was written and the drop is not safe.

- [ ] **Step 2: Repoint the section-presence model, do not delete its addresses branch**

`company_section_presence_current_build.sql` decides which sections the public company page shows, and the public page is explicitly out of scope. Preserve today's behaviour exactly by reading the same source the deleted model read, instead of the model:

```sql
    SELECT '{{ var("country_code") }}', company_id, 'addresses',
           toString(address_fingerprint), observed_at
    FROM {{ source('corpscout', 'se_company_addresses_current') }}
    WHERE has_address = 1 AND has_observation = 1
```

That is the deleted model's own filter and its own key (`address_key` in that model IS `address_fingerprint`), so presence is unchanged row for row. **Repointing it at the new `se_company_address` final would change what the public page shows and is a separate owner decision** — do not do it here.

- [ ] **Step 3: Snapshot the row counts (controller runs these; paste the output into the migration comment)**

```sql
SELECT 'canonical', count() FROM corpscout.se_company_addresses_canonical_current
UNION ALL SELECT 'display', count() FROM corpscout.se_company_address_display_current
UNION ALL SELECT 'final_live', count() FROM corpscout.se_company_address FINAL WHERE is_current;
```

- [ ] **Step 4: Write the retirement migration** (the next free number — 000312 at time of writing — esef took 000309–000311; re-check `ls corpscout/clickhouse/migrations | tail -1` before creating)

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- RETIREMENT, phase 8 of the se_company_address plan. Applied only after
-- corpscout.se_company_address has been serving the backoffice Address tab and the
-- correction sensor has been RUNNING (phase 7 closed on <DATE>).
--
-- Gate 1 -- zero readers, re-verified immediately before writing this file:
--   rg -n "se_company_addresses_canonical_current" corpscout  -> only migrations 000273/000278
--     (historical DDL) and the DuckDB build, whose ClickHouse publish is removed in the same
--     commit as this migration.
--   rg -n "se_company_address_display_current" corpscout      -> only migrations 000267/000278.
--     The dbt model that built it is deleted in the same commit; company_section_presence
--     now reads corpscout.se_company_addresses_current directly with the same filter and the
--     same key, so the public page's section presence is unchanged row for row.
--
-- Gate 2 -- row counts at drop time (SELECT run on the host, <DATE>):
--   se_company_addresses_canonical_current  <N> rows
--   se_company_address_display_current      <N> rows
--   se_company_address FINAL WHERE is_current <N> rows
--
-- NOT dropped, deliberately: se_addresses_current, se_company_address_links_current,
-- se_company_address_members_current and se_address_geocodes_current are the geocode
-- augmentation source the final reads on every resolution. The legacy per-company geocoder
-- pair (se_company_address_geocodes, se_company_address_geocode_results) is the parity
-- baseline and is the owner's separate decision.
--
-- UNDROP window is about 480 seconds: if this turns out to be wrong, UNDROP TABLE within it.

DROP TABLE IF EXISTS corpscout.se_company_address_display_current;

DROP TABLE IF EXISTS corpscout.se_company_addresses_canonical_current;
```

`.down.sql` recreates both tables **empty**, from the CREATE statements in 000267 and 000273 + 000278's ALTERs (copy them verbatim, `IF NOT EXISTS`), with a leading comment: *"Structure only — the rows are not restored. Re-materializing `company_serving_current` / `sweden_company_canonical_addresses_clickhouse` (after reverting the code change that removed them) refills both."*

- [ ] **Step 5: Register and run the suite**

Append `"<NNNNNN>_corpscout_retire_se_address_serving_tables",` (`<NNNNNN>` = the next free number — 000312 at time of writing — esef took 000309–000311; re-check `ls corpscout/clickhouse/migrations | tail -1` before creating) to `EXPECTED_MIGRATIONS`. Then:

```bash
cd corpscout/services/dagster_v3
uv run pytest tests/test_clickhouse_migrations.py tests/test_company_serving_dbt.py \
              tests/test_sweden_company_address_geocoding.py -q
uv run dg check defs && uv run ruff check src/dagster_v3/defs
cd ../backoffice && npx vitest run tests/company-serving-sections.test.ts && pnpm typecheck
```

- [ ] **Step 6: Commit** (the migration is applied by the controller in Task 10e, not here)

```bash
# <NNNNNN> = the next free migration number (000312 at time of writing —
# esef took 000309-000311; re-check `ls corpscout/clickhouse/migrations | tail -1` before creating)
git add corpscout/clickhouse/migrations/<NNNNNN>_corpscout_retire_se_address_serving_tables.up.sql \
        corpscout/clickhouse/migrations/<NNNNNN>_corpscout_retire_se_address_serving_tables.down.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/tables.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/company_section_presence_current_build.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/schema.yml \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_geocoding_assets.py \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py \
        corpscout/services/dagster_v3/tests/test_company_serving_dbt.py \
        corpscout/services/dagster_v3/tests/test_sweden_company_address_geocoding.py
git rm corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/se_company_address_display_current_build.sql
git commit -m "chore(se_company): retire the canonical and display address serving tables"
```

---

### Task 10a (Phase 2): Apply migrations on the ClickHouse host — **controller**

- [ ] Apply **000307** and **000308** with the same golang-migrate path used for 000297–000306. Do this BEFORE deploying the Dagster code, which asserts these tables exist.
- [ ] Verify:
  ```sql
  SELECT name FROM system.tables WHERE database = 'corpscout' AND name LIKE 'se_company_address%' ORDER BY name;
  SELECT name, type FROM system.columns WHERE database = 'corpscout' AND table = 'se_company_address' ORDER BY position;
  SHOW GRANTS FOR corpscout_person_correction_writer;
  ```
  Expected: `se_company_address`, `se_company_address_bolagsverket`, `se_company_address_correction`, `se_company_address_scb` (plus the pre-existing `se_company_address_display_current`, `se_company_address_geocodes`, `se_company_address_geocode_results`, `se_company_address_links_current`, `se_company_address_members_current`); `is_current Bool`, `address_id Nullable(FixedString(64))`, `evidence_set_hash FixedString(64)` present; an INSERT grant on `se_company_address_correction` beside the existing ones.
- [ ] Stop and report counts. Nothing else runs until phase 3 is reviewed.

### Task 10b (Phase 4): Deploy and reload Dagster — **controller**

- [ ] `cd corpscout/services/dagster_v3/ansible && ansible-playbook -i inventory.ini light_sync.yml`.
- [ ] In the Dagster UI confirm: `se_company_address_bolagsverket_clickhouse` (group `se_company_bolagsverket`), `se_company_address_scb_clickhouse` (group `se_company_scb`) and `se_company_address_clickhouse` (group `se_company`) exist; `se_company_address_correction_sensor` and `se_company_address_weekly` are present and **STOPPED**; `se_company_info_*` is untouched; no code-location load errors.
- [ ] Stop here.

### Task 10c (Phase 5): Initial load — **controller**

The artifact assets only copy from `corpscout.se_company_addresses_current`, which is already materialized; nothing upstream is re-run, and there is no model, so there is no model pass and nothing to restore. Each artifact run is idempotent (a second run appends 0 rows).

- [ ] **Step 1 — smoke, scoped.** Launch `se_company_address_job` with run config
  `{"ops": {"se_company_address_clickhouse": {"config": {"execute": true, "company_ids": ["5592990765", "5560125220"]}}}}`.
  The artifact assets take no scope: this first run loads every company into both artifact tables (that IS the backfill, Step 2); only the final is scoped. Expected: the final publishes one row per (company, address type) with a live address, `tombstone_count = 0`, and a non-zero `geocoded_count` if either company is geocoded.
- [ ] **Step 2 — artifacts, full (the first materialization is the backfill).** Verify counts against the source rather than against a number written here:
  ```sql
  SELECT source, count() FROM corpscout.se_company_addresses_current WHERE has_address = 1 GROUP BY source;
  SELECT count() FROM corpscout.se_company_address_bolagsverket FINAL;  -- = the 'bolagsverket' count above
  SELECT count() FROM corpscout.se_company_address_scb FINAL;           -- = the 'scb' count above
  ```
  Reference from the pipeline map: `se_company_addresses_current` holds ~4.67M rows across both sources before the `has_address` filter, of which the Bolagsverket postal side is ~2.85M. Record the actual split — it is the first time anyone has measured it per source, and phase 5's report is where it belongs. Re-launch one artifact asset: `appended_count` must be 0.
- [ ] **Step 3 — final, full pass.** Launch `se_company_address_review_job` with `{"execute": true, "company_batch_size": 5000}` and no `company_ids`. Then:
  ```sql
  SELECT count(), uniqExact(company_id), countIf(latitude IS NOT NULL), countIf(geocode_status = '')
  FROM corpscout.se_company_address FINAL WHERE is_current;
  SELECT uniqExact(address_id) FROM corpscout.se_company_address FINAL
  WHERE is_current AND address_id IS NOT NULL;
  ```
  Expected: `count()` **≥** `uniqExact(company_id)` (several addresses per company is the point, and equal would mean the two sources are collapsing when they should not); `uniqExact(company_id)` close to the number of companies with any address; `uniqExact(address_id)` ≈ **2.09M** — the geocode coverage the map records for `se_address_geocodes_current`. A materially lower number means the fingerprint join is missing rows: check `build_geocodes_sql` against `se_company_address_members_current` before going on.
  ```sql
  SELECT countIf(NOT is_current) FROM corpscout.se_company_address FINAL;  -- 0 on a first load
  ```
- [ ] **Step 4 — steady state.** Launch `se_company_address_job` again with `{"execute": true}`: artifacts append 0 and the final's `selected_company_count` is 0 **unless the geocoding job has re-run since Step 3**, in which case every geocoded company is selected again — that is the geocode term working as designed (see `build_changed_companies_sql`). Note which of the two happened in the report; a non-zero count with no geocode run in between is a bug.
- [ ] **Step 5 — preview is free.** Click "Materialize" on `se_company_address_clickhouse` with no config: it must report `preview: true` and write nothing.

### Task 10d (Phase 7): Switch on and verify end to end — **controller**

- [ ] Open `/admin/se/company/5592990765/address`. Confirm the cards render from the final (sources badges, the geocode block, provenance).
- [ ] Submit an `override_field` on one card; start `se_company_address_correction_sensor`; within ~2 minutes the page shows the corrected text with the correction id in that row's `correction_ids`.
- [ ] Submit `reject_address` on a second card; confirm the card disappears from the tab (the row is `is_current = false`) and still appears in `/admin/se/company-address/corrections`.
- [ ] Submit `undo` for both; confirm both revert on the next sensor run.
- [ ] Start `se_company_address_weekly`.
- [ ] Record the date, the counts from Task 10c and the per-source split in the spec's §2, and commit that doc change by explicit path.

### Task 10e (Phase 8): Apply the retirement migration — **controller**

- [ ] Re-run Task 9 Step 1's `rg` commands one final time; confirm the migration's comment still matches reality.
- [ ] Deploy the code change (Task 9) FIRST, so nothing reads the tables at the moment they go.
- [ ] Apply the retirement migration (the next free number — 000312 at time of writing — esef took 000309–000311; re-check `ls corpscout/clickhouse/migrations | tail -1` before creating). Watch for ~10 minutes: `UNDROP TABLE` is available for about 480 seconds.
- [ ] Verify `SELECT name FROM system.tables WHERE database = 'corpscout' AND name IN ('se_company_addresses_canonical_current', 'se_company_address_display_current')` returns nothing, and that the company page's addresses section still renders.

---

## Self-review

**Spec coverage (`2026-08-23-se-company-address-design.md`):** §1 decision — Tasks 2–5 reuse `publish_with_stage(new_versions_only=True)`, the `execute` gate, `resolve_all`/`resolve_all_before`, the ledger sensor and a weekly schedule, in `defs/se_company/`. §2 sources — Tasks 2–3, one artifact per source, provenance carried by `source_record_uid`. §3 artifacts — Task 1's migration (envelope, `observed_at` = append time, `ReplacingMergeTree(observed_at)`, `has_company`) + Tasks 2–3's SELECTs (`deps` on `sweden_company_addresses_clickhouse`, anti-join append). §4 final — Task 1's `se_company_address` DDL, Task 4's `address_key` normalization + merge + set replacement + geocode augmentation, Task 5's change scan with the geocode term. §5 ledger — Task 1's table, Task 4's kinds and staleness, Task 5's sensor/job/schedule. §6 backoffice — Tasks 7–8. §7 retirements — Task 9 + Task 10e, with the chain and the legacy geocoder pair explicitly kept. §8 out of scope — restated in Global Constraints and enforced in Task 9 Step 2 (the presence model is repointed at the raw source, not at the new final).

**Placeholder scan:** Task 7 Step 5 and Task 8 Steps 3–5 describe the server module's SQL and the components by clause and by section rather than as full text — the exported-constant tests in each Step 1 pin what must be present, and `se-company-info.server.ts` / `se-company-info-review-workspace.tsx` / `se-company-info-lists.server.ts` are the literal patterns beside them. Task 9's migration carries `<DATE>` / `<N>` placeholders **by design**: they are the gate output, filled in by the controller at Step 3 and reviewed before the drop. No TBD/TODO anywhere else.

**Schema ownership:** no Python registry of tables or columns. Each artifact module declares its own `TABLE` and positional insert list; `address.py` declares its own `INSERT_COLUMNS` (write contract), `ARTIFACT_READS` (read contract, derived from the artifact modules' lists) and `PUBLISHED_COLUMNS` (tombstone read contract). Every one is pinned against the migration by `tests/se_company_ddl.declared_columns`, which now finds the creating migration per table and replays later ALTERs.

**Type consistency:** `ArtifactRow(source, source_record_uid, evidence_hash, observed_at, values)` and `evidence_set_hash_for` are imported from `info_rules` rather than re-declared — one artifact-row shape and one evidence-set-hash convention for the whole `se_company` layer. `PublishCounts(staged, inserted, total)` from `common.py` in Tasks 2, 3 and 5. `LedgerRow` + `effective_ledger(rows, kind_order)` in Task 4. `AddressOutcome` fields match `_final_row`'s order and `INSERT_COLUMNS` (minus the MATERIALIZED `evidence_set_hash`); `PUBLISHED_COLUMNS` matches `_published_outcome_from_row`'s positional read. `GeocodeFact` is produced by `_geocode_fact_from_row` and consumed only by `augment_with_geocodes`. `SeAddressCorrectionInput`/`validateSeAddressCorrection` in Tasks 7–8; `ZERO_EVIDENCE_HASH` imported from `se-person-corrections` in both, as the info stack does. The overridable-field list exists twice by necessity (Python `OVERRIDABLE_FIELDS`, TypeScript `OVERRIDABLE_FIELDS`) — both files carry a comment naming the other, and Dagster silently skips a payload key it does not own, so a drift fails safe.

**Known deliberate costs, called out where they are paid:** (1) the geocode change term re-selects the geocoded population weekly because the geocoding job rebuilds `se_address_geocodes_current` whole — argued in `build_changed_companies_sql`'s docstring and checked in Task 10c Step 4; (2) with today's two sources the address types never coincide, so cross-source merging is implemented but dormant — `merge_company_addresses`'s docstring says so, and Task 10c Step 3 asserts `count() ≥ uniqExact(company_id)` precisely to catch the opposite mistake; (3) the artifact ORDER BY `(company_id, source_record_uid)` is unique only while each source yields one address per company — stated in the migration comment for whoever adds the third source.
