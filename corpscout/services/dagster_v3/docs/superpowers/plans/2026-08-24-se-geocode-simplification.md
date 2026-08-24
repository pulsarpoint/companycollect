# Swedish Address Geocoding Simplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the weekly Swedish geocoding job from a whole-universe rematch into a demand-driven one. A permanent, versioned, append-only outcome store (`corpscout.se_address_geocodes`) replaces the weekly-swapped `se_address_geocodes_current` as the source of truth; every stored outcome carries the matcher that produced it (`policy_version`) and the OSM snapshot it was computed against (`reference_md5`); "current outcome per identity" becomes ONE read rule instead of a rebuild artifact. Two of the three matchers are retired, the 19,413 legacy-exact decisions the resolver refuses are imported as auditable `legacy_adopted_v1` outcomes before their tables go, and three ClickHouse tables retire.

**Architecture:** `sweden_osm_addresses_duckdb` (reference) + `sweden_shared_addresses_duckdb` (identities) → a new **demand** asset that loads the store's current outcomes into DuckDB and computes the pending identity set (no resolver outcome / policy bumped / non-geocoded at a stale reference) → the golden gate → the resolver shadow, now scoped to the pending set → promotion, which **appends** the pending set's outcomes to the store (`ReplacingMergeTree(matched_at) ORDER BY (address_id, policy_version, reference_md5)`) instead of replacing a serving table → `se_address_geocodes_current` derived from the store by the versioned read (stage + `EXCHANGE TABLES`) during the transition → `se_company_address`'s geocode read repointed at the same versioned read. The read rule and the pending rule are each ONE SQL fragment plus ONE pure Python twin in `sweden_company/geocode_store.py` and `sweden_company/geocode_demand.py`; no consumer inlines its own ranking.

**Tech Stack:** ClickHouse 26.5 (golang-migrate SQL under `corpscout/clickhouse/migrations/`), DuckDB (`sweden_company_enrichment` schema, pool `sweden_address_osm_duckdb`), Dagster 1.13 (`uv run`), pytest + a `clickhouse-local` harness on the Docker `clickhouse/clickhouse-server:26.5` image.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-08-24-se-geocode-simplification-design.md` (binding). Structural facts with file:line evidence for every claim: `.superpowers/sdd/2026-08-24-se-company-address/geocode-subsystem-map.md`. Pattern reference (read, do not re-derive): the address plan `docs/superpowers/plans/2026-08-24-se-company-address.md` (its Task 9/10e retirement discipline is the model for Tasks 8, 9, 12e and 12f), `defs/company_financials_latest/assets.py:52-70` (the CH-native stage + `INSERT … SELECT` + `EXCHANGE TABLES` shape Task 5 copies), `defs/sweden_company/address_canonicalization.py:465-519` (the streaming ClickHouse→DuckDB load Task 4 copies), migration `000314_corpscout_retire_se_address_display_table.up.sql` (the gate-comment shape Tasks 8 and 9 copy).

## Global Constraints

- **Migrations.** First line `CREATE DATABASE IF NOT EXISTS corpscout;` in every `.up.sql`; a `.down.sql` twin for every migration; **no `;` anywhere inside a `--` comment** (golang-migrate splits the file on `;`, so a semicolon in prose truncates the statement); registered in `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py` (this plan adds no grants, so `EXPECTED_ACCESS_MIGRATIONS` is untouched). **Next free number is 000316 at time of writing** — 000315 (`retire_esef_source_documents`) has landed; re-check with `ls corpscout/clickhouse/migrations | tail -1` before creating any file and shift the whole run if another session took a number.
- **Reviewers execute SQL.** Every SQL constant this plan adds runs in `clickhouse-local` on the Docker `clickhouse/clickhouse-server:26.5` image, under BOTH `join_use_nulls = 0` and `join_use_nulls = 1`, and both settings must answer identically. Substring tests do not close a task that ships SQL.
- **ClickHouse 26.5 rules.** Project columns explicitly, never `SELECT alias.*` after a second `USING` join. Guard every LEFT-JOIN miss of a **Nullable** column with `ifNull(...)`; gate every **non-Nullable** joined column behind a hit flag computed as `ifNull(<run id column>, '') != ''` — a bare `!= ''` is NULL under `join_use_nulls = 1` and reads as the column's *type default* under `join_use_nulls = 0`. Named parameters only (`%(name)s`).
- **Dagster.** No `from __future__ import annotations` in any file this plan creates or rewrites (`clickhouse/resolved.py` and `sweden_company/address_geocoding.py` carry one today; `resolved.py` is not rewritten here and `address_geocoding.py` is deleted). `uv run` for every command from `corpscout/services/dagster_v3`; `uv run dg check defs` green and `uv run ruff check src/dagster_v3/defs` clean before each commit.
- **TDD.** Every task writes its failing test first, runs it to see it fail for the stated reason, then implements. Reviewers re-run mutations: a check or a rule that still passes with its predicate inverted has not been tested.
- **Versioning contract (spec §5), enforced everywhere.** No outcome is ever written without a real `policy_version` and the `reference_md5` of the snapshot it matched against. `matched_at` is append time for the outcomes an append actually computed — never a run-wide constant restamped over unchanged rows (the mistake 000300 fixed for SCB observations). Re-running with an identical (policy, reference) may replace rows but must be a no-op in content.
- **Drop migrations follow the address plan's gate discipline.** Each drop carries, inline in the migration comment: (1) a **fresh** zero-reader proof, including a grep for the **qualified-constant indirection** and not only the literal table name — the six-reader lesson from that plan's Task 9 is why `000314` had to walk back a drop; (2) a **row-count snapshot** taken on the host at drop time; (3) **code deployed before the drop**, never after; (4) an **UNDROP watch** of about 480 seconds after applying.
- **Commits** by explicit path only (the shared tree carries unrelated uncommitted work from other sessions), conventional-commit subject, with the trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- **Production is controller-only.** Applying migrations, deploying Dagster, the backfill append, the adoption import run and the two drop applies are Tasks 12a–12g and are marked **(controller)**. An implementing agent stops at the end of its phase and reports.
- **Touch nothing outside the listed files.** Other sessions are live in this tree.
- **Out of scope, by design:** the identity chain's weekly whole rebuild (canonical → members → links → `se_addresses_current`) keeps it — spec §3; Lantmäteriet stays parked; the resolver's matching semantics (policy v5 taxonomy, score-margin ambiguity) do not change — this plan changes WHEN matching runs, not HOW it decides; the DuckDB canonical build stays (only its ClickHouse publish retires); the public company page. **Spec §6 step 6 — dropping `se_address_geocodes_current` itself — is deliberately NOT in this plan.** It needs a fresh zero-reader gate taken after the final has been reading the store for a while, and the transition parity check (Task 5) is what keeps the derived table honest until then. It is a short follow-up plan, not a task here.

## Phases (execute one at a time; each ends in a verifiable, stoppable state)

| phase | tasks | deliverable | stop/verify |
|---|---|---|---|
| **1 — Store DDL** | Task 1 | migration 000316 + layout tests committed | `uv run pytest tests/test_sweden_company_address_geocoding.py tests/test_clickhouse_migrations.py` green |
| **2 — Apply 000316** *(controller)* | Task 12a | `corpscout.se_address_geocodes` exists, empty | `SELECT count() FROM corpscout.se_address_geocodes` returns 0 |
| **3 — Rules** | Task 2 | `geocode_store.py`: versioned-read SQL + pure twin + taxonomy | `uv run pytest tests/test_sweden_geocode_store.py` green, mutations caught |
| **4 — Write path** | Task 3 | promotion threads `policy_version`/`reference_md5`, appends to the store, dual-writes `_current` unchanged | `uv run pytest tests/test_sweden_company_address_geocoding.py tests/test_address_resolution.py` green |
| **5 — Deploy + backfill** *(controller)* | Tasks 12b, 12c | code deployed; one full append of today's serving rows stamped policy v5 | store row count ≈ 2.09M, one `policy_version`, one `reference_md5` |
| **6 — Demand** | Tasks 4, 5 | demand asset + short-circuit + `rematch_all`; join matcher retired; `_current` derived from the store + parity check | `uv run dg check defs` green, all geocoding tests green |
| **7 — Deploy + one cycle** *(controller)* | Task 12d | one weekly run under demand-driven matching | second consecutive run does zero index building |
| **8 — Adoption** | Task 6 | the one-shot import asset + job, owner-gated, not scheduled | `uv run pytest tests/test_sweden_geocode_legacy_adoption.py` green |
| **9 — Import run** *(controller)* | Task 12e | the import executed, sample verified, count recorded | adopted identities all read `legacy_adopted` through the versioned read |
| **10 — Checks** | Task 7 | §4.5's six-check disposition + the stats-helper split | all seven checks pass on the host after 12d |
| **11 — Legacy retirement** | Task 8 → Task 12f | code removed, then migration 000317 drops the legacy pair | fresh zero-reader proof, row counts in the comment, UNDROP watch |
| **12 — Final repoint** | Tasks 9, 10, 11 | canonical publish retired (migration 000318), `se_company_address` reads the store, harness extended | `npm`-free: `uv run pytest tests/test_se_company_address*.py` green |
| **13 — Canonical drop + close** *(controller)* | Task 12g | 000318 applied, one weekly cycle observed | Monday address run shrinks to churn-sized selection |

**Ordering note (deliberate deviation from the spec's §6 numbering).** §6 lists the legacy retirement before the check relocations. That order is not executable: checks 5 and 6 read the legacy pair through `fetch_sweden_address_geocode_stats` (`address_geocoding_assets.py:56-86`, used at `:1090` and `:1135`), so the pair cannot be dropped while they still read it. Task 7 (all six checks + the stats split) therefore lands **before** Task 8 (legacy retirement), and Task 9 (canonical publish retirement) after both — which is also the order that makes each drop's zero-reader grep come out clean.

---

### Task 1: Migration 000316 — the versioned outcome store

**Files:**
- Create: `corpscout/clickhouse/migrations/000316_corpscout_se_address_geocodes_store.up.sql`, `.down.sql`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py` (append to `EXPECTED_MIGRATIONS` after `"000315_corpscout_retire_esef_source_documents"`)
- Test: `corpscout/services/dagster_v3/tests/test_sweden_company_address_geocoding.py` (append; this file already owns the migration-shape tests for 000275/000277/000278 at `:2249-2320`, and the store is the same subsystem)

**Interfaces:**
- Produces: ClickHouse table `corpscout.se_address_geocodes`, engine `ReplacingMergeTree(matched_at)`, `ORDER BY (address_id, policy_version, reference_md5)`. Its columns are migration 000275's shape plus 000277's `coordinate_spread_meters`, plus the two new key columns `policy_version` and `reference_md5` placed immediately after `address_id` so the leading columns and the sorting key read as one tuple.
- Consumes: nothing. No Python touches the store yet — Task 2 introduces the module that names it.

**Why the two key columns are non-Nullable while `source_md5` stays Nullable.** `source_md5` is provenance copied from the OSM reference table and 000275 declared it `Nullable(String)`; `reference_md5` is the same value **promoted to a key role**, and a NULL in a sorting key is a trap. The append path fills it with `ifNull(source_md5, '')` and the promotion's existing provenance invariant (`address_resolution_promotion.py:392-398` fails on a NULL `source_md5`) already makes `''` unreachable in practice. Task 2's check 3 rewrite asserts `reference_md5 != ''` so the guarantee is enforced on the stored side too, not just inferred from an upstream raise.

- [ ] **Step 1: Write the failing migration-shape test**

Append to `tests/test_sweden_company_address_geocoding.py`, directly after `test_sweden_shared_address_geocode_migration_keeps_complete_outcomes` (which ends at `:2278`):

```python
def test_sweden_address_geocode_store_migration_is_versioned_and_replacing() -> None:
    """The store's whole point is that one identity can hold several attributable outcomes.

    Engine and sorting key are asserted as exact strings: a ReplacingMergeTree without
    matched_at as its version column silently keeps an arbitrary row per key, and a sorting
    key missing policy_version or reference_md5 would collapse two different matchers'
    answers into one row -- both are the failure this table exists to make impossible.
    """
    migration_directory = (
        Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
    )
    up = (
        migration_directory / "000316_corpscout_se_address_geocodes_store.up.sql"
    ).read_text(encoding="utf-8")
    down = (
        migration_directory / "000316_corpscout_se_address_geocodes_store.down.sql"
    ).read_text(encoding="utf-8")

    assert up.startswith("CREATE DATABASE IF NOT EXISTS corpscout;")
    assert "CREATE TABLE IF NOT EXISTS corpscout.se_address_geocodes\n" in up
    assert "ENGINE = ReplacingMergeTree(matched_at)" in up
    assert "ORDER BY (address_id, policy_version, reference_md5)" in up
    assert "DROP TABLE IF EXISTS corpscout.se_address_geocodes;" in down
    # The store is NOT the serving table under another name.
    assert "se_address_geocodes_current" not in up

    for column in (
        "address_id FixedString(64)",
        "policy_version LowCardinality(String)",
        "reference_md5 String",
        "address_identity_run_id String",
        "match_status LowCardinality(String)",
        "candidate_record_urls Array(String)",
        "match_confidence Float32",
        "latitude Nullable(Float64)",
        "coordinate_supporting_point_count UInt32",
        "coordinate_spread_meters Nullable(Float64)",
        "source_md5 Nullable(String)",
        "source_snapshot_at Nullable(DateTime64(3, 'UTC'))",
        "geocode_run_id String",
        "matched_at DateTime64(3, 'UTC')",
    ):
        assert column in up
    # The two key columns are never Nullable -- a NULL in a sorting key is a trap.
    assert "policy_version Nullable" not in up and "reference_md5 Nullable" not in up


def test_sweden_address_geocode_store_carries_every_serving_column() -> None:
    """The store is 000275's shape plus 000277's spread plus the two version columns.

    Read out of the two migration files rather than hand-listed: a column added to the
    serving table by a later migration and forgotten here would leave the store unable to
    derive `_current`, and this test is the only place that would notice.
    """
    migration_directory = (
        Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
    )
    store = (
        migration_directory / "000316_corpscout_se_address_geocodes_store.up.sql"
    ).read_text(encoding="utf-8")
    serving = (
        migration_directory / "000275_corpscout_se_address_geocodes_current.up.sql"
    ).read_text(encoding="utf-8")

    serving_columns = re.findall(r"^    (\w+) ", serving, re.MULTILINE)
    store_columns = re.findall(r"^    (\w+) ", store, re.MULTILINE)
    assert serving_columns, "the 000275 column parser needs updating"
    assert set(serving_columns) | {"coordinate_spread_meters"} | {
        "policy_version",
        "reference_md5",
    } == set(store_columns)
    assert store_columns[:3] == ["address_id", "policy_version", "reference_md5"]
```

`re` and `Path` are already imported at the top of this file; if `re` is not, add `import re` beside the existing imports.

- [ ] **Step 2: Run to verify failure**

```bash
cd corpscout/services/dagster_v3
uv run pytest tests/test_sweden_company_address_geocoding.py -k geocode_store -q
```

Expected: two failures, both `FileNotFoundError` on `000316_corpscout_se_address_geocodes_store.up.sql`.

- [ ] **Step 3: Re-check the next free number, then write the migration**

```bash
ls /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse/migrations | tail -1
```

Expected `000315_corpscout_retire_esef_source_documents.up.sql` — if a higher number appears, another session took 000316: shift this file and every later migration in this plan by the same amount and say so in the report.

`corpscout/clickhouse/migrations/000316_corpscout_se_address_geocodes_store.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- The permanent, versioned Sweden address geocode store.
--
-- One row per (address identity, matcher, reference snapshot). An address_id is a
-- fingerprint of normalized address text, so the text cannot change under it -- matching an
-- identity again with the same matcher against the same OSM snapshot must reproduce the
-- same answer, and ReplacingMergeTree(matched_at) makes that re-append idempotent instead
-- of duplicative. A new policy_version or a new reference_md5 appends BESIDE the old row
-- rather than overwriting it, which is what makes a stored coordinate attributable.
--
-- policy_version is the matcher: se-address-resolution-policy-v5 today
-- (address_resolution_policy.py), or legacy_adopted_v1 for the one-time import of the
-- decisions the retired per-company matcher made and the resolver refuses.
-- reference_md5 is the Geofabrik MD5 of the OSM snapshot the outcome was computed against
-- -- the same value carried as source_md5 provenance since 000275, promoted to a key role.
-- matched_at is APPEND time for the outcomes a run actually computed, never a run-wide
-- constant restamped over unchanged rows.
--
-- corpscout.se_address_geocodes_current is NOT retired by this migration. It keeps its
-- readers and is derived from this store by the versioned read during the transition.

CREATE TABLE IF NOT EXISTS corpscout.se_address_geocodes
(
    address_id FixedString(64),
    policy_version LowCardinality(String),
    reference_md5 String,
    address_identity_run_id String,
    normalized_match_key String,
    match_status LowCardinality(String),
    candidate_count UInt16,
    candidate_record_ids Array(String),
    candidate_record_urls Array(String),
    match_method LowCardinality(String),
    match_confidence Float32,
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    geocode_provider LowCardinality(String),
    geocode_precision LowCardinality(String),
    coordinate_method Nullable(String),
    coordinate_locality Nullable(String),
    coordinate_supporting_point_count UInt32,
    coordinate_spread_meters Nullable(Float64),
    source_record_id Nullable(String),
    source_record_url Nullable(String),
    source_url Nullable(String),
    source_object_key Nullable(String),
    source_md5 Nullable(String),
    source_snapshot_at Nullable(DateTime64(3, 'UTC')),
    source_retrieved_at Nullable(DateTime64(3, 'UTC')),
    geocode_run_id String,
    matched_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(matched_at)
ORDER BY (address_id, policy_version, reference_md5);
```

`corpscout/clickhouse/migrations/000316_corpscout_se_address_geocodes_store.down.sql`:

```sql
-- Reverts 000316. The store is append-only and holds outcomes no other table keeps once
-- the transition is complete -- re-materializing the geocoding job after reverting the code
-- refills it for the CURRENT policy and reference only. Adopted legacy_adopted_v1 rows are
-- NOT reproducible by any asset and would have to be re-imported by hand.

CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.se_address_geocodes;
```

- [ ] **Step 4: Register the migration**

In `tests/test_clickhouse_migrations.py`, append to `EXPECTED_MIGRATIONS` after `"000315_corpscout_retire_esef_source_documents",` (line 328):

```python
    "000316_corpscout_se_address_geocodes_store",
```

- [ ] **Step 5: Run**

```bash
cd corpscout/services/dagster_v3
uv run pytest tests/test_sweden_company_address_geocoding.py tests/test_clickhouse_migrations.py -q
uv run ruff check src/dagster_v3/defs
```

Expected: all green, ruff clean.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/clickhouse/migrations/000316_corpscout_se_address_geocodes_store.up.sql \
        corpscout/clickhouse/migrations/000316_corpscout_se_address_geocodes_store.down.sql \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py \
        corpscout/services/dagster_v3/tests/test_sweden_company_address_geocoding.py
git commit -m "feat(sweden_company): add the versioned Sweden address geocode store

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

**STOP.** Phase 2 is the controller's (Task 12a).

---

### Task 2: `geocode_store.py` — the store contract, the versioned read, and its pure twin

Resolves spec §9 open item 1 (exact versioned-read SQL and its Python twin) and item 4 (the `matched_at` tie-break).

**Files:**
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/geocode_store.py`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_resolution_promotion.py` (import the status tuples from their new home instead of declaring them; keep the module-level names so existing importers and tests are unaffected)
- Test: `corpscout/services/dagster_v3/tests/test_sweden_geocode_store.py`

**Interfaces (Tasks 3–11 consume these exact names):**
- `GEOCODE_STORE_TABLE = "se_address_geocodes"`, `QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE`, `QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE`, `QUALIFIED_DUCKDB_PREVIOUS_OUTCOMES_TABLE`, `CLICKHOUSE_DATABASE`, `ENRICHMENT_SCHEMA`, `GEOCODE_APPEND_TABLE`, `PREVIOUS_OUTCOMES_TABLE`
- `STORE_COLUMNS: tuple[str, ...]` (28, DDL order), `STORE_KEY_COLUMNS`, `SERVING_COLUMNS` (26 = `STORE_COLUMNS` minus the two version columns, and equal to `shared_address_geocoding.ADDRESS_GEOCODE_COLUMNS`), `RANK_INPUT_COLUMNS`
- `GEOCODED_STATUSES`, `VALID_STATUSES`, `LEGACY_ADOPTED_POLICY_VERSION = "legacy_adopted_v1"`, `LEGACY_ADOPTED_MATCH_METHOD = "legacy_adopted"`, `RESOLVER_ONLY_FILTER_SQL`, `IS_ADOPTED_SQL`
- `NEWEST_PER_FAMILY_RANK_SQL: str`, `CURRENT_OUTCOME_CHOICE_RANK_SQL: str`
- `build_current_geocodes_sql(*, columns=STORE_COLUMNS, address_filter_sql="") -> str` — the full two-stage rule, for the serving consumers
- `build_current_resolver_geocodes_sql(*, columns=STORE_COLUMNS, address_filter_sql="") -> str` — stage 1 over the resolver family only, for the demand scan
- `StoredOutcome` dataclass, `is_adopted(outcome) -> bool`, `is_geocoded(status) -> bool`, `family_rank(outcome)`, `choice_rank(outcome)`, `current_resolver_outcome(outcomes)`, `current_adopted_outcome(outcomes)`, `current_outcome(outcomes) -> StoredOutcome | None`, `current_outcomes_by_address(outcomes) -> dict[str, StoredOutcome]`, `current_resolver_outcomes_by_address(outcomes) -> dict[str, StoredOutcome]`

**The read rule, stated once — and why it is TWO stages, not one rank.**

Spec §4.1 says two things: (a) the current outcome is the newest `matched_at` per `address_id` across versions, and (b) `legacy_adopted_v1` is outranked only by a **same-or-newer** resolver outcome that is GEOCODED — a resolver `ambiguous` never takes a coordinate away from an adopted exact.

Those two sentences **cannot** be expressed as one lexicographic rank tuple. Take an identity holding three rows: an adopted exact at T1, a resolver `matched_exact` at T2 (T2 > T1), and a resolver `ambiguous` at T3 (T3 > T2). Rule (b) says the resolver exact beats the adopted row; rule (b) also says the adopted row beats the resolver ambiguous; rule (a) says the ambiguous (newest) beats the resolver exact. That is a cycle, and any flat rank silently resolves it by dropping one of the three sentences. The demotion has to apply between *matcher families*, not between arbitrary pairs.

So the rule runs in two stages, and each stage is a plain total order:

1. **Per family, newest wins.** Rank every row by `(matched_at, reference_md5, policy_version)` descending, keep the first row per `(address_id, is_adopted)`. This is rule (a), applied where it belongs — among a single matcher family's own outcomes. At most two rows per identity survive: its best resolver outcome and its best adopted outcome.
2. **Choose between the two survivors.** Rank by `(servable, matched_at, is_resolver, reference_md5, policy_version)` descending and keep the first row per `address_id`, where `servable` is 1 for an adopted row or a GEOCODED resolver row and 0 for a non-geocoded resolver row.

Stage 2 reads out exactly as rule (b): a non-geocoded resolver outcome loses to an adopted row whatever its date (`servable`); a geocoded resolver outcome beats the adopted row when it is newer, loses when it is older, and wins on an exact tie (`is_resolver`, the "same" half of "same-or-newer"). With no adopted row, the single resolver survivor wins by default and `servable` is inert — so the ordinary 2.09M identities are ranked by nothing but recency, which is rule (a).

**The `matched_at` tie-break (spec §9 item 4), decided.** Two rows can share a `matched_at` in three ways. (a) **Same family, different key triples** — one run appending a new-identity outcome and a retry-pool outcome for the same identity at two references. Stage 1's `reference_md5` then `policy_version` components break it deterministically: arbitrary but stable, which is what matters, because two reads of an unchanged store must answer identically. (b) **Different families, same instant** — stage 2's `is_resolver` component decides for the resolver, which is the spec's "same-or-newer". (c) **The same key triple twice at the same instant** — a replace racing an append inside one run. Those rows are content-identical by the §5 versioning contract (same identity, same matcher, same reference ⇒ same answer), so no tie-break can pick wrongly. This is why the read never uses `FINAL`: `FINAL` hands the choice to ReplacingMergeTree's part order, which is not deterministic before a merge, and the read would be reproducible only by luck. The append path additionally refuses to let a replaced row keep a newer `matched_at` than the run replacing it (Task 3, `build_store_append_regression_sql`), and the harness proves the byte-identical re-append (Task 11).

- [ ] **Step 1: Write the failing tests**

`corpscout/services/dagster_v3/tests/test_sweden_geocode_store.py`:

```python
"""The store's ONE read rule, pinned on both sides.

The rule lives twice by necessity -- once as SQL for the two ClickHouse consumers, once as
Python for the demand scan that has to reason about outcomes in memory. Both halves are
generated from the same constants and both are pinned here, because a divergence between
them is invisible at runtime: the SQL would serve one coordinate and the demand scan would
believe another, and neither would raise.
"""
import re
from datetime import UTC, datetime

import pytest

from dagster_v3.defs.sweden_company import shared_address_geocoding
from dagster_v3.defs.sweden_company.geocode_store import (
    CURRENT_OUTCOME_CHOICE_RANK_SQL,
    GEOCODED_STATUSES,
    IS_ADOPTED_SQL,
    LEGACY_ADOPTED_MATCH_METHOD,
    LEGACY_ADOPTED_POLICY_VERSION,
    NEWEST_PER_FAMILY_RANK_SQL,
    QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE,
    RANK_INPUT_COLUMNS,
    RESOLVER_ONLY_FILTER_SQL,
    SERVING_COLUMNS,
    STORE_COLUMNS,
    STORE_KEY_COLUMNS,
    VALID_STATUSES,
    StoredOutcome,
    build_current_geocodes_sql,
    build_current_resolver_geocodes_sql,
    choice_rank,
    current_adopted_outcome,
    current_outcome,
    current_outcomes_by_address,
    current_resolver_outcome,
    current_resolver_outcomes_by_address,
    is_adopted,
    is_geocoded,
)

POLICY = "se-address-resolution-policy-v5"
OLD_POLICY = "se-address-resolution-policy-v4"
MD5_A, MD5_B = "aaaaaaaa", "bbbbbbbb"
T1 = datetime(2026, 8, 1, tzinfo=UTC)
T2 = datetime(2026, 8, 8, tzinfo=UTC)
T3 = datetime(2026, 8, 15, tzinfo=UTC)
ADDRESS = "f" * 64
ADOPTED = LEGACY_ADOPTED_POLICY_VERSION


def _outcome(policy: str, md5: str, status: str, matched_at: datetime,
             address_id: str = ADDRESS) -> StoredOutcome:
    return StoredOutcome(address_id=address_id, policy_version=policy, reference_md5=md5,
                         match_status=status, matched_at=matched_at)


def test_the_store_columns_are_the_serving_columns_plus_the_two_version_columns() -> None:
    assert STORE_KEY_COLUMNS == ("address_id", "policy_version", "reference_md5")
    assert STORE_COLUMNS[:3] == STORE_KEY_COLUMNS
    assert SERVING_COLUMNS == tuple(
        column for column in STORE_COLUMNS if column not in ("policy_version", "reference_md5"))
    # The serving contract is not re-typed here: it IS the shipped export list.
    assert SERVING_COLUMNS == shared_address_geocoding.ADDRESS_GEOCODE_COLUMNS
    # Everything the two ranks read must be projectable even when the caller did not ask
    # for it -- SERVING_COLUMNS omits both version columns, and the choice rank needs them.
    assert set(RANK_INPUT_COLUMNS) == {
        "address_id", "policy_version", "reference_md5", "match_status", "matched_at"}
    assert set(RANK_INPUT_COLUMNS) <= set(STORE_COLUMNS)


def test_the_module_agrees_with_the_canonicalization_module_on_names() -> None:
    """Two literals, one meaning -- geocode_store spells them itself to stay import-light."""
    from dagster_v3.defs.sweden_company import address_canonicalization
    from dagster_v3.defs.sweden_company import geocode_store

    assert geocode_store.CLICKHOUSE_DATABASE == address_canonicalization.CLICKHOUSE_DATABASE
    assert geocode_store.ENRICHMENT_SCHEMA == address_canonicalization.ENRICHMENT_SCHEMA


def test_the_taxonomy_has_one_home() -> None:
    """address_resolution_promotion imported these; re-declaring them anywhere would let a
    status be geocoded on one side of the pipeline and not on the other."""
    from dagster_v3.defs.sweden_company import address_resolution_promotion

    assert address_resolution_promotion.GEOCODED_STATUSES is GEOCODED_STATUSES
    assert address_resolution_promotion.VALID_STATUSES is VALID_STATUSES
    assert set(GEOCODED_STATUSES) < set(VALID_STATUSES)
    assert len(VALID_STATUSES) == 11
    assert LEGACY_ADOPTED_POLICY_VERSION not in VALID_STATUSES
    assert LEGACY_ADOPTED_MATCH_METHOD == "legacy_adopted"


@pytest.mark.parametrize("status", VALID_STATUSES)
def test_is_geocoded_agrees_with_the_geocoded_tuple(status: str) -> None:
    assert is_geocoded(status) == (status in GEOCODED_STATUSES)


def test_the_two_rank_expressions_spell_their_components_in_order() -> None:
    """Parsed out of the expressions, not substring-matched.

    Reordering `servable` and `matched_at` in the choice rank is the mutation that matters:
    it would make a newer resolver `ambiguous` outrank an adopted exact -- exactly the
    regression the import exists to prevent, and one no downstream assertion would catch.
    """
    assert NEWEST_PER_FAMILY_RANK_SQL == "tuple(matched_at, reference_md5, policy_version)"
    components = [line.strip().rstrip(",")
                  for line in CURRENT_OUTCOME_CHOICE_RANK_SQL.splitlines()[1:]]
    assert components == [
        f"toUInt8(is_adopted = 1 OR match_status IN ({', '.join(repr(s) for s in GEOCODED_STATUSES)}))",
        "matched_at",
        "1 - is_adopted",
        "reference_md5",
        "policy_version)",
    ]
    assert IS_ADOPTED_SQL == f"toUInt8(policy_version = '{LEGACY_ADOPTED_POLICY_VERSION}')"
    assert RESOLVER_ONLY_FILTER_SQL == f"policy_version != '{LEGACY_ADOPTED_POLICY_VERSION}'"


def test_the_read_runs_both_stages_and_keeps_one_row_per_identity() -> None:
    sql = build_current_geocodes_sql()
    # Stage 1: newest per (identity, matcher family).
    assert f"FROM {QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}\n" in sql
    assert f"{IS_ADOPTED_SQL} AS is_adopted" in sql
    assert f"ORDER BY address_id, is_adopted, {NEWEST_PER_FAMILY_RANK_SQL} DESC" in sql
    assert "LIMIT 1 BY address_id, is_adopted" in sql
    # Stage 2: choose between the at most two survivors.
    assert ") AS candidates\n" in sql
    assert f"ORDER BY address_id, {CURRENT_OUTCOME_CHOICE_RANK_SQL} DESC" in sql
    assert sql.rstrip().endswith("LIMIT 1 BY address_id")
    assert sql.index("LIMIT 1 BY address_id, is_adopted") < sql.index(") AS candidates")
    # FINAL is deliberately absent -- see the module docstring.
    assert "FINAL" not in sql
    # The outer projection IS the requested column list, in order, and nothing else.
    assert re.findall(r"^    (\w+),?$", sql[: sql.index("\nFROM (")], re.MULTILINE) == list(STORE_COLUMNS)


def test_the_read_projects_the_rank_inputs_even_when_they_were_not_requested() -> None:
    """SERVING_COLUMNS has no policy_version and no reference_md5, and the derived
    `_current` table asks for exactly those 26 columns. If the inner SELECT projected only
    what was asked for, the outer ORDER BY would reference columns that do not exist and the
    derivation would fail at run time on the host, not here."""
    sql = build_current_geocodes_sql(columns=SERVING_COLUMNS)
    inner = sql[sql.index("FROM (") : sql.index(") AS candidates")]
    for column in RANK_INPUT_COLUMNS:
        assert re.search(rf"^        {column},?$", inner, re.MULTILINE), column
    outer = sql[: sql.index("\nFROM (")]
    assert "policy_version" not in outer and "reference_md5" not in outer


def test_the_read_filters_before_ranking() -> None:
    sql = build_current_geocodes_sql(
        columns=("address_id", "match_status"),
        address_filter_sql="address_id IN (SELECT address_id FROM corpscout.se_company_address_links_current)")
    # The filter sits in the INNER query: it prunes on the sorting key's leading column, so
    # a page-sized read touches parts, not all 2.09M identities. Filtering the ranked result
    # would be correct and would pay for the whole store on every page.
    assert sql.index("WHERE address_id IN (") < sql.index("ORDER BY address_id, is_adopted")


def test_the_resolver_only_read_is_stage_one_over_the_resolver_family() -> None:
    """What the demand scan loads. It is NOT the served answer: an identity whose served
    answer is an adopted exact still has a resolver `ambiguous`, and that ambiguous is what
    decides whether the identity is due for a rematch."""
    sql = build_current_resolver_geocodes_sql(columns=("address_id", "match_status"))
    assert f"WHERE {RESOLVER_ONLY_FILTER_SQL}" in sql
    assert f"ORDER BY address_id, {NEWEST_PER_FAMILY_RANK_SQL} DESC" in sql
    assert sql.rstrip().endswith("LIMIT 1 BY address_id")
    # One stage only -- no candidates subquery, no servable component.
    assert "candidates" not in sql and "is_adopted" not in sql
    filtered = build_current_resolver_geocodes_sql(address_filter_sql="address_id = 'x'")
    assert f"WHERE address_id = 'x'\n  AND {RESOLVER_ONLY_FILTER_SQL}" in filtered


@pytest.mark.parametrize(
    ("name", "outcomes", "expected"),
    [
        ("one outcome wins by default", [_outcome(POLICY, MD5_A, "ambiguous", T1)], (POLICY, MD5_A)),
        # Rule (a) within the resolver family: newest wins, geocoded or not. A reference
        # update that turns an exact into an ambiguous IS honoured -- the identity then sits
        # in the retry pool, which is where it belongs.
        ("a newer resolver outcome replaces an older one",
         [_outcome(POLICY, MD5_A, "matched_exact", T1), _outcome(POLICY, MD5_B, "matched_exact", T2)],
         (POLICY, MD5_B)),
        ("a newer resolver ambiguous replaces an older resolver exact",
         [_outcome(POLICY, MD5_A, "matched_exact", T1), _outcome(POLICY, MD5_B, "ambiguous", T2)],
         (POLICY, MD5_B)),
        # Rule (b): the adopted row survives a newer resolver non-answer.
        ("an adopted exact outranks a newer resolver ambiguous",
         [_outcome(ADOPTED, MD5_A, "matched_exact", T1), _outcome(POLICY, MD5_B, "ambiguous", T2)],
         (ADOPTED, MD5_A)),
        ("an adopted exact outranks a newer resolver unmatched",
         [_outcome(ADOPTED, MD5_A, "matched_exact", T1), _outcome(POLICY, MD5_B, "unmatched", T2)],
         (ADOPTED, MD5_A)),
        # ... and yields to a newer resolver answer.
        ("a newer geocoded resolver outcome outranks an adopted row",
         [_outcome(ADOPTED, MD5_A, "matched_exact", T1), _outcome(POLICY, MD5_B, "matched_site", T2)],
         (POLICY, MD5_B)),
        ("an older geocoded resolver outcome does NOT outrank an adopted row",
         [_outcome(ADOPTED, MD5_B, "matched_exact", T2), _outcome(POLICY, MD5_A, "matched_exact", T1)],
         (ADOPTED, MD5_B)),
        ("at an exact tie the resolver wins -- 'same-or-newer'",
         [_outcome(ADOPTED, MD5_A, "matched_exact", T1), _outcome(POLICY, MD5_A, "matched_exact", T1)],
         (POLICY, MD5_A)),
        # The three-row state that a flat rank cannot order: the adopted row is protected
        # from the newest ambiguous, and among resolver rows the newest still wins -- so the
        # served answer is the adopted exact, not either resolver row.
        ("adopted, a newer resolver exact and a newest resolver ambiguous",
         [_outcome(ADOPTED, MD5_A, "matched_exact", T1),
          _outcome(POLICY, MD5_A, "matched_exact", T2),
          _outcome(POLICY, MD5_B, "ambiguous", T3)],
         (ADOPTED, MD5_A)),
        ("same three rows with the adopted row newest",
         [_outcome(ADOPTED, MD5_A, "matched_exact", T3),
          _outcome(POLICY, MD5_A, "matched_exact", T1),
          _outcome(POLICY, MD5_B, "ambiguous", T2)],
         (ADOPTED, MD5_A)),
        # Same instants, different references: stable, not merge-order-dependent.
        ("equal instants in one family break on reference_md5",
         [_outcome(POLICY, MD5_A, "matched_exact", T1), _outcome(POLICY, MD5_B, "matched_exact", T1)],
         (POLICY, MD5_B)),
    ],
)
def test_current_outcome_ranks_the_way_the_rule_says(
    name: str, outcomes: list[StoredOutcome], expected: tuple[str, str]
) -> None:
    chosen = current_outcome(outcomes)
    assert chosen is not None, name
    assert (chosen.policy_version, chosen.reference_md5) == expected, name
    # Order of arrival never decides.
    reversed_choice = current_outcome(list(reversed(outcomes)))
    assert reversed_choice == chosen, name


def test_the_resolver_view_ignores_adopted_rows_entirely() -> None:
    """What the demand scan reads. An adopted row must never make an identity look matched
    or make it look due for a rematch -- it is not a resolver answer at all."""
    outcomes = [_outcome(ADOPTED, MD5_A, "matched_exact", T3),
                _outcome(POLICY, MD5_A, "ambiguous", T1)]
    resolver = current_resolver_outcome(outcomes)
    assert resolver is not None
    assert resolver.policy_version == POLICY and resolver.match_status == "ambiguous"
    adopted = current_adopted_outcome(outcomes)
    assert adopted is not None and adopted.policy_version == ADOPTED
    assert current_resolver_outcome([_outcome(ADOPTED, MD5_A, "matched_exact", T1)]) is None
    assert current_adopted_outcome([_outcome(POLICY, MD5_A, "ambiguous", T1)]) is None
    assert is_adopted(_outcome(ADOPTED, MD5_A, "matched_exact", T1))
    assert not is_adopted(_outcome(POLICY, MD5_A, "matched_exact", T1))


def test_choice_rank_components_are_the_five_the_rule_names() -> None:
    """Named separately from the table above so a reviewer mutating one component sees a
    direct failure rather than a scenario name."""
    assert choice_rank(_outcome(POLICY, MD5_A, "matched_exact", T1)) == (1, T1, 1, MD5_A, POLICY)
    assert choice_rank(_outcome(POLICY, MD5_A, "ambiguous", T1)) == (0, T1, 1, MD5_A, POLICY)
    assert choice_rank(_outcome(ADOPTED, MD5_A, "matched_exact", T1)) == (1, T1, 0, MD5_A, ADOPTED)
    # An adopted row is servable even if some future import stamps a non-exact status on it.
    assert choice_rank(_outcome(ADOPTED, MD5_A, "unmatched", T1))[0] == 1


def test_outcomes_are_grouped_by_identity() -> None:
    other = "e" * 64
    rows = [
        _outcome(POLICY, MD5_A, "ambiguous", T1),
        _outcome(POLICY, MD5_A, "matched_exact", T1, address_id=other),
        _outcome(POLICY, MD5_B, "matched_exact", T2),
        _outcome(ADOPTED, MD5_A, "matched_exact", T3, address_id=other),
    ]
    grouped = current_outcomes_by_address(rows)
    assert set(grouped) == {ADDRESS, other}
    assert grouped[ADDRESS].reference_md5 == MD5_B
    assert grouped[other].policy_version == ADOPTED
    resolver_only = current_resolver_outcomes_by_address(rows)
    assert set(resolver_only) == {ADDRESS, other}
    assert resolver_only[other].policy_version == POLICY


def test_current_outcome_is_none_for_an_identity_with_no_rows() -> None:
    assert current_outcome([]) is None
    assert current_outcomes_by_address([]) == {}
    assert current_resolver_outcomes_by_address([]) == {}
```

- [ ] **Step 2: Run to verify failure** — `cd corpscout/services/dagster_v3 && uv run pytest tests/test_sweden_geocode_store.py -q` → `ModuleNotFoundError: dagster_v3.defs.sweden_company.geocode_store`

- [ ] **Step 3: Implement `geocode_store.py`**

```python
"""The Sweden address geocode store: its ClickHouse contract and its ONE read rule.

The store (`corpscout.se_address_geocodes`, migration 000316) holds one row per
(address identity, matcher, reference snapshot). "The current outcome for an identity" is
therefore a READ RULE over several rows, not a table -- and that rule lives here exactly
once, as SQL for the ClickHouse consumers and as a pure function for the demand scan.
Nothing else may re-express it: a consumer that inlined its own ranking would serve a
different coordinate from the one the demand scan believes is stored, and neither side
would raise.

THE RULE, IN TWO STAGES.

Stage 1 -- per matcher family, newest wins. Rank by (matched_at, reference_md5,
policy_version) descending and keep the first row per (address_id, is_adopted). At most two
rows per identity survive: its best resolver outcome and its best imported outcome.

Stage 2 -- choose between the survivors. Rank by (servable, matched_at, is_resolver,
reference_md5, policy_version) descending and keep the first row per address_id, where
`servable` is 1 for an adopted row or a GEOCODED resolver row and 0 for a resolver row that
did not geocode.

Read out, stage 2 says: a resolver `ambiguous` never takes a coordinate away from an
imported `legacy_adopted_v1` exact, however recent it is (that is what the import in spec
section 4.4 is for); a resolver outcome that DOES geocode takes over as soon as it is as new
as the adopted row, and does not take over if it is older.

WHY TWO STAGES AND NOT ONE RANK. The spec's two sentences -- "newest matched_at per
address_id across versions" and "legacy_adopted_v1 outranked by any same-or-newer resolver
outcome that is GEOCODED" -- are cyclic over three rows. Adopted exact at T1, resolver
matched_exact at T2, resolver ambiguous at T3: the resolver exact beats the adopted row, the
adopted row beats the resolver ambiguous, and the ambiguous (newest) beats the resolver
exact. Any flat rank breaks that cycle by silently dropping one of the sentences. Splitting
the demotion out to a comparison BETWEEN families, after each family has already been
reduced to its newest row, makes both stages plain total orders and keeps all three
sentences.

WHY NO `FINAL`. ReplacingMergeTree(matched_at) collapses rows sharing the full key triple,
but before a merge `FINAL` picks among equal-version rows in part order, which is not
deterministic. Ranking explicitly makes two reads of an unchanged store answer identically,
which the transition parity check and the harness both depend on. Rows sharing the whole key
AND matched_at are content-identical by the versioning contract, so the trailing rank
components never choose between differing content -- they only make the choice stable.
"""

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime

# Mirrors address_canonicalization.CLICKHOUSE_DATABASE / ENRICHMENT_SCHEMA. Spelled here so
# this module stays import-light: defs/se_company/address.py imports it, and
# address_canonicalization pulls in pyarrow and libpostal. tests/test_sweden_geocode_store.py
# asserts the two spellings agree.
CLICKHOUSE_DATABASE = "corpscout"
ENRICHMENT_SCHEMA = "sweden_company_enrichment"

GEOCODE_STORE_TABLE = "se_address_geocodes"
QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE = (
    f"{CLICKHOUSE_DATABASE}.{GEOCODE_STORE_TABLE}"
)
# What promotion hands the ClickHouse append asset ...
GEOCODE_APPEND_TABLE = "se_address_geocodes_append"
QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE = f"{ENRICHMENT_SCHEMA}.{GEOCODE_APPEND_TABLE}"
# ... and what the demand asset loads back out of ClickHouse for the run to reason about.
PREVIOUS_OUTCOMES_TABLE = "se_address_geocodes_previous"
QUALIFIED_DUCKDB_PREVIOUS_OUTCOMES_TABLE = (
    f"{ENRICHMENT_SCHEMA}.{PREVIOUS_OUTCOMES_TABLE}"
)

LEGACY_ADOPTED_POLICY_VERSION = "legacy_adopted_v1"
LEGACY_ADOPTED_MATCH_METHOD = "legacy_adopted"
RESOLVER_ONLY_FILTER_SQL = f"policy_version != '{LEGACY_ADOPTED_POLICY_VERSION}'"
IS_ADOPTED_SQL = f"toUInt8(policy_version = '{LEGACY_ADOPTED_POLICY_VERSION}')"

GEOCODED_STATUSES = (
    "matched_exact",
    "matched_corrected",
    "matched_site",
    "matched_area",
    "matched_street",
)
VALID_STATUSES = (
    *GEOCODED_STATUSES,
    "ambiguous",
    "unmatched",
    "invalid_address",
    "foreign_address",
    "postal_box",
    "property_identifier",
)

STORE_KEY_COLUMNS = ("address_id", "policy_version", "reference_md5")
# Migration 000316's declaration order. The append binds these positionally.
STORE_COLUMNS = (
    *STORE_KEY_COLUMNS,
    "address_identity_run_id",
    "normalized_match_key",
    "match_status",
    "candidate_count",
    "candidate_record_ids",
    "candidate_record_urls",
    "match_method",
    "match_confidence",
    "latitude",
    "longitude",
    "geocode_provider",
    "geocode_precision",
    "coordinate_method",
    "coordinate_locality",
    "coordinate_supporting_point_count",
    "coordinate_spread_meters",
    "source_record_id",
    "source_record_url",
    "source_url",
    "source_object_key",
    "source_md5",
    "source_snapshot_at",
    "source_retrieved_at",
    "geocode_run_id",
    "matched_at",
)
# What se_address_geocodes_current holds: the store minus the two version columns. Equal to
# shared_address_geocoding.ADDRESS_GEOCODE_COLUMNS, asserted rather than imported so this
# module keeps no dependency on the matcher-era module.
SERVING_COLUMNS = tuple(
    column
    for column in STORE_COLUMNS
    if column not in ("policy_version", "reference_md5")
)
# Columns both ranks read. The inner SELECT projects these whatever the caller asked for --
# SERVING_COLUMNS omits both version columns, and the choice rank needs them.
RANK_INPUT_COLUMNS = (
    "address_id",
    "policy_version",
    "reference_md5",
    "match_status",
    "matched_at",
)


def _quoted(values: Sequence[str]) -> str:
    return ", ".join(f"'{value}'" for value in values)


NEWEST_PER_FAMILY_RANK_SQL = "tuple(matched_at, reference_md5, policy_version)"

CURRENT_OUTCOME_CHOICE_RANK_SQL = (
    "tuple(\n"
    f"        toUInt8(is_adopted = 1 OR match_status IN ({_quoted(GEOCODED_STATUSES)})),\n"
    "        matched_at,\n"
    "        1 - is_adopted,\n"
    "        reference_md5,\n"
    "        policy_version)"
)


def _inner_columns(columns: Sequence[str]) -> list[str]:
    inner = list(columns)
    for column in RANK_INPUT_COLUMNS:
        if column not in inner:
            inner.append(column)
    return inner


def build_current_geocodes_sql(
    *,
    columns: Sequence[str] = STORE_COLUMNS,
    address_filter_sql: str = "",
) -> str:
    """The current outcome per address identity, as a self-contained SELECT.

    ``address_filter_sql`` is inserted as the INNER query's WHERE and must constrain
    ``address_id`` -- the store's sorting key leads with it, so a page-sized read touches a
    few parts instead of ranking all 2.09M identities. It never goes on the outer query:
    that would be correct and would pay for the whole store on every page.

    The caller wraps the result in ``FROM ( ... ) AS <alias>``. There is no join here and no
    Nullable comparison, so the fragment answers identically under both join_use_nulls
    settings by construction.
    """
    outer_projection = ",\n    ".join(columns)
    inner_projection = ",\n        ".join(_inner_columns(columns))
    where = f"\n    WHERE {address_filter_sql}" if address_filter_sql else ""
    return (
        f"SELECT\n    {outer_projection}\n"
        "FROM (\n"
        f"    SELECT\n        {inner_projection},\n        {IS_ADOPTED_SQL} AS is_adopted\n"
        f"    FROM {QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}{where}\n"
        f"    ORDER BY address_id, is_adopted, {NEWEST_PER_FAMILY_RANK_SQL} DESC\n"
        "    LIMIT 1 BY address_id, is_adopted\n"
        ") AS candidates\n"
        f"ORDER BY address_id, {CURRENT_OUTCOME_CHOICE_RANK_SQL} DESC\n"
        "LIMIT 1 BY address_id"
    )


def build_current_resolver_geocodes_sql(
    *,
    columns: Sequence[str] = STORE_COLUMNS,
    address_filter_sql: str = "",
) -> str:
    """Stage 1 alone, over the resolver family: the newest resolver outcome per identity.

    This is what the demand scan reasons about, and it is deliberately NOT the served
    answer. An identity whose served answer is an imported adopted exact still has a
    resolver `ambiguous` behind it, and that ambiguous is what decides whether the identity
    belongs in the retry pool. Ranking the served answer here would make every adopted
    identity look permanently settled and the resolver would never try it again.
    """
    projection = ",\n    ".join(columns)
    filters = [RESOLVER_ONLY_FILTER_SQL]
    if address_filter_sql:
        filters.insert(0, address_filter_sql)
    where = "\nWHERE " + "\n  AND ".join(filters)
    return (
        f"SELECT\n    {projection}\n"
        f"FROM {QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}{where}\n"
        f"ORDER BY address_id, {NEWEST_PER_FAMILY_RANK_SQL} DESC\n"
        "LIMIT 1 BY address_id"
    )


def is_geocoded(match_status: str) -> bool:
    return match_status in GEOCODED_STATUSES


@dataclass(frozen=True)
class StoredOutcome:
    """One stored row, reduced to what the ranks and the demand scan need."""

    address_id: str
    policy_version: str
    reference_md5: str
    match_status: str
    matched_at: datetime


def is_adopted(outcome: StoredOutcome) -> bool:
    return outcome.policy_version == LEGACY_ADOPTED_POLICY_VERSION


def family_rank(outcome: StoredOutcome) -> tuple[datetime, str, str]:
    """Stage 1: the Python twin of NEWEST_PER_FAMILY_RANK_SQL."""
    return (outcome.matched_at, outcome.reference_md5, outcome.policy_version)


def choice_rank(outcome: StoredOutcome) -> tuple[int, datetime, int, str, str]:
    """Stage 2: the Python twin of CURRENT_OUTCOME_CHOICE_RANK_SQL."""
    servable = 1 if is_adopted(outcome) or is_geocoded(outcome.match_status) else 0
    return (
        servable,
        outcome.matched_at,
        0 if is_adopted(outcome) else 1,
        outcome.reference_md5,
        outcome.policy_version,
    )


def _newest(outcomes: Iterable[StoredOutcome]) -> StoredOutcome | None:
    best: StoredOutcome | None = None
    for outcome in outcomes:
        if best is None or family_rank(outcome) > family_rank(best):
            best = outcome
    return best


def current_resolver_outcome(
    outcomes: Iterable[StoredOutcome],
) -> StoredOutcome | None:
    """Stage 1 over the resolver family. This is what the demand scan reasons about."""
    return _newest(outcome for outcome in outcomes if not is_adopted(outcome))


def current_adopted_outcome(
    outcomes: Iterable[StoredOutcome],
) -> StoredOutcome | None:
    return _newest(outcome for outcome in outcomes if is_adopted(outcome))


def current_outcome(outcomes: Iterable[StoredOutcome]) -> StoredOutcome | None:
    rows = list(outcomes)
    candidates = [
        candidate
        for candidate in (
            current_resolver_outcome(rows),
            current_adopted_outcome(rows),
        )
        if candidate is not None
    ]
    if not candidates:
        return None
    return max(candidates, key=choice_rank)


def current_outcomes_by_address(
    outcomes: Iterable[StoredOutcome],
) -> dict[str, StoredOutcome]:
    return _by_address(outcomes, current_outcome)


def current_resolver_outcomes_by_address(
    outcomes: Iterable[StoredOutcome],
) -> dict[str, StoredOutcome]:
    return _by_address(outcomes, current_resolver_outcome)


def _by_address(
    outcomes: Iterable[StoredOutcome],
    select: "Callable[[list[StoredOutcome]], StoredOutcome | None]",
) -> dict[str, StoredOutcome]:
    grouped: dict[str, list[StoredOutcome]] = {}
    for outcome in outcomes:
        grouped.setdefault(outcome.address_id, []).append(outcome)
    selected: dict[str, StoredOutcome] = {}
    for address_id, rows in grouped.items():
        chosen = select(rows)
        if chosen is not None:
            selected[address_id] = chosen
    return selected
```

`Callable` comes from `collections.abc` — add it to the existing import line (`from collections.abc import Callable, Iterable, Sequence`) and drop the quotes on the annotation; the quoted form above is only to keep the snippet readable in isolation.

- [ ] **Step 4: Give the status tuples one home**

In `address_resolution_promotion.py`, delete the `GEOCODED_STATUSES` and `VALID_STATUSES` literals at `:17-32` and import them instead. The module-level names stay, so every existing `_quoted(GEOCODED_STATUSES)` call site (`:157`, `:278`, `:376`, `:380`, `:389`) is unchanged:

```python
from dagster_v3.defs.sweden_company.geocode_store import (
    GEOCODED_STATUSES,
    VALID_STATUSES,
)
```

Place it with the other `dagster_v3.defs.sweden_company` imports at `:6-14`. Leave `POSTCODE_CONFLICT_STREET_STRATEGIES` and `BUILDING_MATCH_STATUSES` where they are — they are promotion-gate specifics, not store taxonomy.

- [ ] **Step 5: Run**

```bash
cd corpscout/services/dagster_v3
uv run pytest tests/test_sweden_geocode_store.py tests/test_sweden_company_address_geocoding.py tests/test_address_resolution.py -q
uv run dg check defs && uv run ruff check src/dagster_v3/defs/sweden_company
```

Expected: all green, `dg check defs` green, ruff clean.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/geocode_store.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_resolution_promotion.py \
        corpscout/services/dagster_v3/tests/test_sweden_geocode_store.py
git commit -m "feat(sweden_company): add the geocode store's versioned read rule

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Promotion appends to the store — version threading, dual write, and the backfill

Spec §6 step 1. The serving table is still rebuilt exactly as today; the store gains the same outcomes with their provenance. Nothing reads the store yet, so this task is reversible by deleting one asset.

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/address_resolution_promotion.py` (thread `policy_version` and `reference_md5` through the stage, split the stage into the serving table and the append table, extend the invariants)
- Modify: `src/dagster_v3/defs/sweden_company/address_resolution_assets.py` (pass the policy version into the promotion call — it already has it at `:126`)
- Modify: `src/dagster_v3/defs/sweden_company/address_geocoding_assets.py` (new asset `sweden_address_geocode_store_clickhouse`, new asset `sweden_address_geocode_store_backfill_clickhouse` with its `execute` gate and its own job, both registered in `defs` and in the four job selections)
- Test: `corpscout/services/dagster_v3/tests/test_sweden_geocode_store_append.py` (new), and `tests/test_sweden_company_address_geocoding.py` (job/graph shape)

**Interfaces (Tasks 4–6 consume):**
- `address_resolution_promotion.replace_current_geocodes_from_address_resolution_shadow(..., expected_policy_version: str)` — unchanged signature; it now also writes `sweden_company_enrichment.se_address_geocodes_append` (`STORE_COLUMNS` order) beside `se_address_geocodes_current` (`SERVING_COLUMNS` order), and returns two new keys, `"reference_md5"` and `"appended_rows"`.
- Asset key `sweden_address_geocode_store_clickhouse` (group `sweden_company`, pool `sweden_address_osm_duckdb`), deps `sweden_address_resolution_current_duckdb`.
- Asset key `sweden_address_geocode_store_backfill_clickhouse` + `SwedenGeocodeStoreBackfillConfig(execute: bool = False)` + job `sweden_address_geocode_store_backfill_job`.
- `address_geocoding_assets.GEOCODE_STORE_ASSET_KEY`, `GEOCODE_STORE_BACKFILL_ASSET_KEY` constants.

- [ ] **Step 1: Write the failing tests**

`corpscout/services/dagster_v3/tests/test_sweden_geocode_store_append.py`:

```python
"""The append path: every stored outcome is attributable, and a re-append cannot go backwards.

The promotion's DuckDB half is exercised against a real in-memory DuckDB (the pattern
tests/test_address_resolution.py already uses for this module), because the thing under
test is a projection over three joined tables, not a Python branch.
"""
from datetime import UTC, datetime

import duckdb
import pytest

from dagster_v3.defs.sweden_company.address_geocoding_assets import (
    GEOCODE_STORE_BACKFILL_SQL,
    build_geocode_store_backfill_sql,
    build_store_append_regression_sql,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE,
    SERVING_COLUMNS,
    STORE_COLUMNS,
)

POLICY = "se-address-resolution-policy-v5"


def test_the_backfill_stamps_the_policy_and_promotes_source_md5_to_the_key() -> None:
    sql = build_geocode_store_backfill_sql()
    assert sql.startswith(
        "INSERT INTO corpscout.se_address_geocodes (" + ", ".join(STORE_COLUMNS) + ")")
    assert "%(policy_version)s AS policy_version" in sql
    assert "ifNull(source_md5, '') AS reference_md5" in sql
    assert "FROM corpscout.se_address_geocodes_current" in sql
    # matched_at is COPIED, never restamped: the serving row's own instant is what that
    # outcome claims, and copying it is what makes a second backfill run a no-op in content
    # rather than a version bump on 2.09M rows.
    assert "now64" not in sql and "now(" not in sql
    assert GEOCODE_STORE_BACKFILL_SQL is sql or GEOCODE_STORE_BACKFILL_SQL == sql
    # Every serving column is carried across, none dropped.
    for column in SERVING_COLUMNS:
        assert column in sql


def test_the_append_regression_query_looks_for_rows_that_would_swallow_this_run() -> None:
    """ReplacingMergeTree keeps the row with the LARGEST matched_at per key. If a row for a
    key this run just appended already carries a newer instant, this run's outcome is
    invisible from the moment it lands -- a silent no-op that no row count would reveal."""
    sql = build_store_append_regression_sql()
    assert "FROM corpscout.se_address_geocodes" in sql
    assert "geocode_run_id = %(geocode_run_id)s" in sql
    assert "matched_at > %(matched_at)s" in sql
    assert "(address_id, policy_version, reference_md5) IN (" in sql


@pytest.fixture()
def connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("create schema if not exists sweden_company_enrichment")
    yield connection
    connection.close()


def test_promotion_writes_both_tables_with_their_own_shapes(connection) -> None:
    """Built on the same fixture set tests/test_address_resolution.py uses for promotion --
    see that file's `_seed_promotable_shadow` helper, which this test imports rather than
    re-inventing so the two stay in step."""
    from tests.test_address_resolution import seed_promotable_shadow
    from dagster_v3.defs.sweden_company.address_resolution_promotion import (
        replace_current_geocodes_from_address_resolution_shadow,
    )

    seed_promotable_shadow(connection, source_md5="md5-alpha")
    counts = replace_current_geocodes_from_address_resolution_shadow(
        connection=connection,
        geocode_run_id="run-1",
        matched_at=datetime(2026, 8, 24, tzinfo=UTC),
        expected_policy_version=POLICY,
        log=None,
    )

    assert counts["reference_md5"] == "md5-alpha"
    assert counts["appended_rows"] == counts["rows"]
    # DuckDB's `describe` returns (column_name, column_type, ...), so index 0 is the name.
    serving = [row[0] for row in connection.execute(
        "describe sweden_company_enrichment.se_address_geocodes_current").fetchall()]
    appended = [row[0] for row in connection.execute(
        f"describe {QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE}").fetchall()]
    assert serving == list(SERVING_COLUMNS)
    assert appended == list(STORE_COLUMNS)
    [(policies, references)] = connection.execute(
        f"select count(distinct policy_version), count(distinct reference_md5)"
        f" from {QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE}").fetchall()
    assert int(policies) == 1 and int(references) == 1


def test_promotion_refuses_an_outcome_with_no_reference_identity(connection) -> None:
    """The versioning contract's hard half: an outcome with no reference_md5 is not
    attributable, and the store's sorting key would carry an empty string forever."""
    from tests.test_address_resolution import seed_promotable_shadow
    from dagster_v3.defs.sweden_company.address_resolution_promotion import (
        replace_current_geocodes_from_address_resolution_shadow,
    )

    seed_promotable_shadow(connection, source_md5=None)
    with pytest.raises(ValueError, match="reference"):
        replace_current_geocodes_from_address_resolution_shadow(
            connection=connection,
            geocode_run_id="run-1",
            matched_at=datetime(2026, 8, 24, tzinfo=UTC),
            expected_policy_version=POLICY,
            log=None,
        )
```

> **Note on the third and fourth tests.** `tests/test_address_resolution.py` currently seeds its promotion fixtures inline (`:780-800` builds `se_address_geocodes_current`, and the promotion tests around `:280-430` seed the shadow tables). Step 3 extracts that seeding into a module-level `seed_promotable_shadow(connection, *, source_md5)` helper in that file — `source_md5` threaded into the OSM address-points fixture so the `None` case is reachable — and rewrites the existing promotion tests to call it. Same fixtures, one definition, and that file is listed in this task's commit for exactly that reason.

Also append to `tests/test_sweden_company_address_geocoding.py`, inside the existing job-shape test (`:1990-2127`), after the `weekly_job` assertion block:

```python
    store = repo.asset_graph.get(dg.AssetKey("sweden_address_geocode_store_clickhouse"))
    assert store.group_name == "sweden_company"
    assert store.parent_keys == {dg.AssetKey("sweden_address_resolution_current_duckdb")}
    assert store.pools == {"sweden_address_osm_duckdb"}
    # The store append rides in every job that promotes, so a promotion is never published
    # to the serving table without the attributable row landing beside it.
    for job_name in ("sweden_company_address_geocoding_job",
                     "sweden_shared_address_geocoding_job",
                     "sweden_address_resolution_publish_job",
                     "sweden_company_address_geocoding_weekly_job"):
        assert "sweden_address_geocode_store_clickhouse" in {
            key.path[-1] for key in repo.get_job(job_name).asset_layer.executable_asset_keys}
    # The backfill is a one-shot: its own job, no schedule, and it is in no other job.
    backfill_job = repo.get_job("sweden_address_geocode_store_backfill_job")
    assert {key.path[-1] for key in backfill_job.asset_layer.executable_asset_keys} == {
        "sweden_address_geocode_store_backfill_clickhouse"}
    for job_name in ("sweden_company_address_geocoding_weekly_job",
                     "sweden_company_address_geocoding_job"):
        assert "sweden_address_geocode_store_backfill_clickhouse" not in {
            key.path[-1] for key in repo.get_job(job_name).asset_layer.executable_asset_keys}
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/test_sweden_geocode_store_append.py -q` → `ImportError` on `build_geocode_store_backfill_sql`

- [ ] **Step 3: Thread the version columns through promotion**

In `address_resolution_promotion.py`, import the store module beside the others and change three places.

`_replace_promotion_stage` gains the policy version and projects the two key columns. Change its signature and the two inserted projection lines (the stage's `select` starts at `:266`):

```python
def _replace_promotion_stage(
    connection: Any,
    *,
    geocode_run_id: str,
    matched_at: datetime,
    policy_version: str,
) -> None:
```

and inside the second `connection.execute`, immediately after the `cast(address.address_id as varchar) as address_id,` line, insert:

```sql
            ?::varchar as policy_version,
            coalesce(provenance.source_md5, '') as reference_md5,
```

then extend the bound parameter list at the end of that call from `[geocode_run_id, matched_at]` to `[policy_version, geocode_run_id, matched_at]` — DuckDB binds `?` positionally in statement order, and `policy_version` now appears first.

`replace_current_geocodes_from_address_resolution_shadow` splits the stage into the two tables (replacing the single `create or replace table ... as select * from stage` at `:67-74`):

```python
        _replace_promotion_stage(
            connection,
            geocode_run_id=geocode_run_id,
            matched_at=matched_at,
            policy_version=expected_policy_version,
        )
        _assert_promoted_geocode_invariants(
            connection,
            PROMOTION_STAGE_TABLE,
            expected_policy_version=expected_policy_version,
        )
        # The serving table keeps its 26-column shape: se_address_geocodes_current is
        # published to ClickHouse by column list, and a stray column would break the export.
        connection.execute(
            f"""
            create or replace table {
                shared_address_geocoding.QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE
            } as
            select {", ".join(geocode_store.SERVING_COLUMNS)}
            from {PROMOTION_STAGE_TABLE}
            """
        )
        # ... and the append table carries the two version columns as well. It is a separate
        # persistent table, not the temporary stage, because the ClickHouse append asset
        # opens its own DuckDB connection and a temporary table would not be there.
        connection.execute(
            f"""
            create or replace table {
                geocode_store.QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE
            } as
            select {", ".join(geocode_store.STORE_COLUMNS)}
            from {PROMOTION_STAGE_TABLE}
            """
        )
        connection.execute("commit")
```

The return dict at `:100-107` gains two keys:

```python
    [(reference_md5, appended_rows)] = connection.execute(
        f"""
        select first(reference_md5), count(*)
        from {geocode_store.QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE}
        """
    ).fetchall()
    return {
        "rows": int(rows),
        "geolocated": int(geolocated),
        "evaluation_run_id": str(evaluation_run_id),
        "policy_version": expected_policy_version,
        "reference_md5": str(reference_md5),
        "appended_rows": int(appended_rows),
        "status_counts": status_counts,
        "table": shared_address_geocoding.QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE,
        "append_table": geocode_store.QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE,
    }
```

`_assert_promoted_geocode_invariants` gains the version terms. Change its signature to `(connection: Any, table_name: str, *, expected_policy_version: str) -> None`, add two aggregates to the existing single SELECT — `count(*) filter (where reference_md5 = '')` as `missing_reference` and `count(*) filter (where policy_version != '{expected_policy_version}')` as `wrong_policy` — and two raises beside the existing ones:

```python
    if int(missing_reference) != 0:
        raise ValueError("Promoted geocodes are missing the OSM reference identity")
    if int(wrong_policy) != 0:
        raise ValueError(
            "Promoted geocodes do not carry the expected address-resolution policy "
            f"{expected_policy_version}"
        )
```

The `expected_policy_version` interpolation is safe here for the same reason `_quoted` is: it comes from `SWEDEN_ADDRESS_RESOLUTION_POLICY.version`, a module constant, never from data.

- [ ] **Step 4: Add the ClickHouse append asset and the backfill**

In `address_geocoding_assets.py`, add the import and two key constants beside the existing ones at `:22-38`:

```python
from dagster_v3.defs.sweden_company import geocode_store

GEOCODE_STORE_ASSET_KEY = "sweden_address_geocode_store_clickhouse"
GEOCODE_STORE_BACKFILL_ASSET_KEY = "sweden_address_geocode_store_backfill_clickhouse"
```

Then the two SQL builders and the two assets (place them directly after `sweden_address_geocodes_clickhouse`, which ends at `:486`):

```python
def build_store_append_regression_sql() -> str:
    """Rows that would swallow the outcomes this run just appended.

    ReplacingMergeTree(matched_at) keeps the LARGEST matched_at per key. A pre-existing row
    for one of this run's key triples carrying a newer instant makes this run's outcome
    invisible the moment it lands -- a silent no-op no row count would show. Equal instants
    are this run's own rows and are content-identical by the versioning contract, so the
    comparison is strictly greater-than.
    """
    return f"""SELECT count()
FROM {geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}
WHERE (address_id, policy_version, reference_md5) IN (
        SELECT address_id, policy_version, reference_md5
        FROM {geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}
        WHERE geocode_run_id = %(geocode_run_id)s
      )
  AND matched_at > %(matched_at)s"""


def build_geocode_store_backfill_sql() -> str:
    """The one-time append of today's serving rows as policy-v5 outcomes.

    matched_at is COPIED from the serving row, never restamped. That instant is what the
    outcome actually claims, and copying it is what makes a second backfill run replace each
    row with byte-identical content instead of bumping 2.09M versions. source_md5 is
    promoted to the reference_md5 key role -- the pre-flight below refuses to run if any
    serving row is missing it, because an empty key column is not attributable.
    """
    carried = ",\n    ".join(
        column
        for column in geocode_store.SERVING_COLUMNS
        if column != "address_id"
    )
    columns = ", ".join(geocode_store.STORE_COLUMNS)
    return f"""INSERT INTO {
        geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE
    } ({columns})
SELECT
    address_id,
    %(policy_version)s AS policy_version,
    ifNull(source_md5, '') AS reference_md5,
    {carried}
FROM {shared_address_geocoding.QUALIFIED_CLICKHOUSE_ADDRESS_GEOCODES_TABLE}"""


GEOCODE_STORE_BACKFILL_SQL = build_geocode_store_backfill_sql()

BACKFILL_PREFLIGHT_SQL = f"""SELECT
    count(),
    countIf(isNull(source_md5) OR source_md5 = '')
FROM {shared_address_geocoding.QUALIFIED_CLICKHOUSE_ADDRESS_GEOCODES_TABLE}"""


@dg.asset(
    name=GEOCODE_STORE_ASSET_KEY,
    deps=[dg.AssetKey(ADDRESS_RESOLUTION_CURRENT_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "openstreetmap"},
    pool=osm_tables.DUCKDB_POOL,
    metadata={"table": geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE},
    description=(
        "Appends this run's resolved Sweden address outcomes to the permanent "
        "versioned geocode store, stamped with the resolver policy and the OSM "
        "reference snapshot they were computed against."
    ),
)
def sweden_address_geocode_store_clickhouse(
    context: dg.AssetExecutionContext,
    sweden_address_osm_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=geocode_store.CLICKHOUSE_DATABASE,
        tables=(geocode_store.GEOCODE_STORE_TABLE,),
    )
    with sweden_address_osm_duckdb.get_connection() as connection:
        [(appended, matched_at, policy_versions, references)] = connection.execute(
            f"""
            select
                count(*),
                max(matched_at),
                count(distinct policy_version),
                count(distinct reference_md5)
            from {geocode_store.QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE}
            """
        ).fetchall()
        if int(policy_versions) > 1 or int(references) > 1:
            raise ValueError(
                "One promotion appends outcomes for one policy and one reference"
            )
        with clickhouse.get_connection() as clickhouse_client:
            rows = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=connection,
                clickhouse_client=clickhouse_client,
                duckdb_schema=geocode_store.ENRICHMENT_SCHEMA,
                duckdb_table=geocode_store.GEOCODE_APPEND_TABLE,
                clickhouse_database=geocode_store.CLICKHOUSE_DATABASE,
                clickhouse_table=geocode_store.GEOCODE_STORE_TABLE,
                columns=geocode_store.STORE_COLUMNS,
                truncate=False,
                log=context.log.info,
            )
            [(regressions,)] = clickhouse_client.execute(
                build_store_append_regression_sql(),
                {"geocode_run_id": context.run_id, "matched_at": matched_at},
            )
            [(store_rows, store_identities)] = clickhouse_client.execute(
                f"""
                SELECT count(), uniqExact(address_id)
                FROM {geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}
                """
            )
    if int(regressions) != 0:
        raise ValueError(
            f"{regressions} stored outcomes are newer than this run's appended rows "
            "for the same identity, policy and reference"
        )
    return dg.MaterializeResult(
        metadata={
            "appended_rows": rows,
            "expected_appended_rows": int(appended),
            "store_rows": int(store_rows),
            "store_identities": int(store_identities),
            "table": geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE,
        }
    )


class SwedenGeocodeStoreBackfillConfig(dg.Config):
    """A bare Materialize click is a preview -- it reports what a real run would append."""

    execute: bool = False


@dg.asset(
    name=GEOCODE_STORE_BACKFILL_ASSET_KEY,
    deps=[dg.AssetKey(SHARED_GEOCODE_CLICKHOUSE_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse"},
    metadata={"table": geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE},
    description=(
        "One-time append of the whole current Sweden serving geocode table into the "
        "versioned store, stamped with the resolver policy that produced it and each "
        "row's own OSM snapshot MD5. Idempotent: a second run replaces each row with "
        "identical content."
    ),
)
def sweden_address_geocode_store_backfill_clickhouse(
    context: dg.AssetExecutionContext,
    config: SwedenGeocodeStoreBackfillConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=geocode_store.CLICKHOUSE_DATABASE,
        tables=(
            geocode_store.GEOCODE_STORE_TABLE,
            shared_address_geocoding.ADDRESS_GEOCODES_TABLE,
        ),
    )
    policy_version = SWEDEN_ADDRESS_RESOLUTION_POLICY.version
    with clickhouse.get_connection() as client:
        [(serving_rows, missing_reference)] = client.execute(BACKFILL_PREFLIGHT_SQL)
        if int(serving_rows) == 0:
            raise ValueError("The Sweden serving geocode table is empty")
        if int(missing_reference) != 0:
            raise ValueError(
                f"{missing_reference} serving rows carry no OSM snapshot MD5 and "
                "cannot be given a reference identity"
            )
        if not config.execute:
            return dg.MaterializeResult(
                metadata={
                    "preview": True,
                    "serving_rows": int(serving_rows),
                    "policy_version": policy_version,
                }
            )
        client.execute(
            GEOCODE_STORE_BACKFILL_SQL, {"policy_version": policy_version}
        )
        [(store_rows, store_identities, policies)] = client.execute(
            f"""
            SELECT count(), uniqExact(address_id), uniqExact(policy_version)
            FROM {geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}
            """
        )
    context.log.info(
        "Backfilled %s serving rows into the geocode store as %s",
        serving_rows,
        policy_version,
    )
    return dg.MaterializeResult(
        metadata={
            "preview": False,
            "serving_rows": int(serving_rows),
            "store_rows": int(store_rows),
            "store_identities": int(store_identities),
            "store_policy_versions": int(policies),
            "policy_version": policy_version,
        }
    )
```

`SWEDEN_ADDRESS_RESOLUTION_POLICY` is not imported in this module today — add it to the imports:

```python
from dagster_v3.defs.sweden_company.address_resolution_policy import (
    SWEDEN_ADDRESS_RESOLUTION_POLICY,
)
```

- [ ] **Step 5: Register the assets and the job**

Add `GEOCODE_STORE_ASSET_KEY` to the selections of `sweden_company_address_geocoding_job` (`:1155`), `sweden_shared_address_geocoding_job` (`:1186`), `sweden_company_address_geocoding_weekly_job` (`:1202`) and — in `address_resolution_assets.py` — `sweden_address_resolution_publish_job` (`:239`, add the string `"sweden_address_geocode_store_clickhouse"` to its `dg.AssetSelection.assets(...)` call). Add both new assets to `defs.assets` at `:1241-1251`, and the new job:

```python
sweden_address_geocode_store_backfill_job = dg.define_asset_job(
    name="sweden_address_geocode_store_backfill_job",
    selection=dg.AssetSelection.assets(GEOCODE_STORE_BACKFILL_ASSET_KEY),
    tags={"country": "SE", "pipeline": "address_geocode_store_backfill"},
    description=(
        "One-time append of the current Sweden serving geocode table into the "
        "versioned store. Requires execute: true -- a bare materialization previews."
    ),
)
```

registered in `defs.jobs` at `:1261-1266`.

- [ ] **Step 6: Run**

```bash
cd corpscout/services/dagster_v3
uv run pytest tests/test_sweden_geocode_store_append.py tests/test_sweden_geocode_store.py \
              tests/test_sweden_company_address_geocoding.py tests/test_address_resolution.py -q
uv run dg check defs && uv run ruff check src/dagster_v3/defs/sweden_company
```

Expected: all green. In the Dagster UI after the controller deploys, `sweden_address_geocode_store_clickhouse` sits between `sweden_address_resolution_current_duckdb` and nothing.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_resolution_promotion.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_resolution_assets.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_geocoding_assets.py \
        corpscout/services/dagster_v3/tests/test_sweden_geocode_store_append.py \
        corpscout/services/dagster_v3/tests/test_sweden_company_address_geocoding.py \
        corpscout/services/dagster_v3/tests/test_address_resolution.py
git commit -m "feat(sweden_company): append attributable geocode outcomes to the store

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

**STOP.** Phase 5 is the controller's (Tasks 12b, 12c).

---

### Task 4: Demand-driven matching, the MD5 short-circuit, and the join matcher's retirement

Spec §6 step 2, §4.2, §4.3. This is the task that delivers goal 1 (matching cost proportional to change) and goal 2 (one matcher).

**Files:**
- Create: `src/dagster_v3/defs/sweden_company/geocode_demand.py`
- Modify: `src/dagster_v3/defs/sweden_company/address_resolution_shadow.py` (scope the query documents to the pending set, short-circuit an empty set BEFORE any reference index is built, repoint the comparison off the join matcher's output)
- Modify: `src/dagster_v3/defs/sweden_company/address_resolution_promotion.py` (short-circuit, repoint the postcode-conflict gate, relax the coverage invariants from "every current address" to "every pending identity")
- Modify: `src/dagster_v3/defs/sweden_company/address_resolution_assets.py` (shadow's dep, promotion's short-circuit metadata)
- Modify: `src/dagster_v3/defs/sweden_company/address_geocoding_assets.py` (the demand asset; remove `sweden_shared_address_osm_matches_duckdb` and `SHARED_GEOCODE_DUCKDB_ASSET_KEY` from three job selections and from `defs`)
- Modify: `src/dagster_v3/defs/sweden_company/shared_address_geocoding.py` (delete the matcher; keep the table names and `ADDRESS_GEOCODE_COLUMNS`)
- Create: `corpscout/services/dagster_v3/tests/test_sweden_geocode_demand.py`
- Modify: `corpscout/services/dagster_v3/tests/test_sweden_company_address_geocoding.py` (delete the join matcher's seven scenario blocks; update the job-shape test)

**Interfaces (Tasks 5–11 consume):**
- `geocode_demand.PENDING_IDENTITIES_TABLE = "se_address_pending_identities"`, `QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE`
- `geocode_demand.PENDING_REASONS = ("rematch_all", "no_outcome", "policy_changed", "reference_changed")`
- `geocode_demand.pending_reason(outcome, *, policy_version, reference_md5, rematch_all) -> str`
- `geocode_demand.replace_pending_address_identities(*, connection, policy_version, reference_md5, rematch_all, log) -> dict[str, object]`
- `geocode_demand.load_current_resolver_outcomes(*, connection, clickhouse_client, log) -> int`
- `geocode_demand.fresh_reference_md5(connection) -> str`
- `geocode_demand.pending_identity_count(connection) -> int`
- Asset key `sweden_address_geocode_demand_duckdb`, config `SwedenGeocodeDemandConfig(rematch_all: bool = False)`, constant `address_geocoding_assets.GEOCODE_DEMAND_ASSET_KEY`

- [ ] **Step 1: THE GATE — verify the join matcher's output is dead, and act on what it says**

Spec §4.3 makes this a hard precondition: "nothing reads its output table between its write and the promotion overwrite — including the shadow's comparison tables and diagnostics". Run it, freshly:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg -n "QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE|se_address_geocodes_current" \
   corpscout/services/dagster_v3/src --glob '!*.pyc'
```

**The gate was run at plan time (2026-08-24) and the precondition is FALSE as written.** Two sites read the join matcher's freshly written output between its write and the promotion overwrite:

| # | site | what it reads | disposition |
|---|---|---|---|
| 1 | `address_resolution_shadow.py:526-553` `_replace_comparison` | `INNER JOIN … current ON current.address_id = shadow.query_document_id`, projecting `current.match_status AS current_status`. Feeds `..._comparison_shadow`, the `largest_transitions` metadata (`_shadow_counts`, `:602-611`) and the `comparisons == results` invariant (`:587-588`). | Repoint at the store's previous **resolver** outcome (Step 5). The comparison then reports resolver-vs-previously-served instead of resolver-vs-join-matcher, which is the more meaningful transition report and the only one that still exists once the join matcher is gone. |
| 2 | `address_resolution_promotion.py:162-215` `_assert_shadow_is_promotable`'s postcode-conflict gate | `INNER JOIN … current`, projecting `current.match_status`, `current.match_method`, `current.match_confidence`, `current.candidate_record_ids`. A real safety gate: it can REFUSE promotion. | Repoint at the same previous-resolver-outcome table (Step 6). The gate's question — "would this postcode-conflict street fallback replace a still-supported building match or a stronger street result?" — becomes a question about what is actually being served, which is what it was always trying to ask. |

The remaining hits (`promotion.py:70,85,106`, `shared_address_geocoding.py:86,98,106,111,189,594`, `address_geocoding_assets.py:407`) are the promotion's own write of the serving table and the join matcher's writes and self-reads — not reads of the join matcher's values by anything else.

**STOP AND REPORT if the `rg` shows a reader that is not in the table above.** A new one appeared since 2026-08-24 and the deletion is not safe. Otherwise proceed: this task deletes the matcher AND repoints its two readers in the same commit, which is what "delete" has to mean here.

**Also record what the repoint changes, because it is a real semantic change, not a refactor.** Today `current_status` in the comparison table is the JOIN matcher's answer for this run; after Step 5 it is the previous run's RESOLVER answer. The `largest_transitions` metadata therefore stops reporting "resolver vs the old matcher" and starts reporting "resolver vs last week" — week one after the deploy will show a large, one-off transition table, and that is expected, not a regression.

- [ ] **Step 2: Write the failing tests for the pending rule**

`corpscout/services/dagster_v3/tests/test_sweden_geocode_demand.py`:

```python
"""What the weekly resolver run is allowed to match, and nothing else.

The rule lives twice -- as DuckDB SQL over the loaded previous outcomes, and as a pure
function. Both are pinned here; the SQL half is EXECUTED against a real in-memory DuckDB,
because the thing under test is a LEFT JOIN and a CASE, and a substring test cannot tell a
correct CASE from one whose branches are in the wrong order.
"""
from datetime import UTC, datetime

import duckdb
import pytest

from dagster_v3.defs.sweden_company.geocode_demand import (
    PENDING_REASONS,
    QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE,
    pending_reason,
    replace_pending_address_identities,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    LEGACY_ADOPTED_POLICY_VERSION,
    StoredOutcome,
)

POLICY = "se-address-resolution-policy-v5"
OLD_POLICY = "se-address-resolution-policy-v4"
MD5_NOW, MD5_OLD = "md5-current", "md5-previous"
T1 = datetime(2026, 8, 1, tzinfo=UTC)


def _outcome(policy: str, md5: str, status: str) -> StoredOutcome:
    return StoredOutcome(address_id="a" * 64, policy_version=policy, reference_md5=md5,
                         match_status=status, matched_at=T1)


@pytest.mark.parametrize(
    ("name", "outcome", "rematch_all", "expected"),
    [
        # 1. New identities. Register churn keeps its fingerprint, so an unchanged address
        #    already has an outcome and is never selected.
        ("an identity with no resolver outcome at all", None, False, "no_outcome"),
        # 2. A policy bump is a full rematch: every stored outcome carries the old version.
        ("a bumped policy wakes a geocoded identity",
         _outcome(OLD_POLICY, MD5_NOW, "matched_exact"), False, "policy_changed"),
        ("a bumped policy wakes a non-geocoded identity",
         _outcome(OLD_POLICY, MD5_NOW, "ambiguous"), False, "policy_changed"),
        # 3. The retry pool: non-geocoded outcomes, and only when the reference moved.
        ("a stale reference wakes an ambiguous",
         _outcome(POLICY, MD5_OLD, "ambiguous"), False, "reference_changed"),
        ("a stale reference wakes an unmatched",
         _outcome(POLICY, MD5_OLD, "unmatched"), False, "reference_changed"),
        ("a stale reference wakes an invalid_address",
         _outcome(POLICY, MD5_OLD, "invalid_address"), False, "reference_changed"),
        # ... and NOT when it did not.
        ("an unchanged reference leaves an ambiguous alone",
         _outcome(POLICY, MD5_NOW, "ambiguous"), False, ""),
        # 4. A geocoded identity at a stale reference is NOT retried. This is the whole
        #    saving: a reference bump costs the non-geocoded population, not 2.09M rows.
        ("a stale reference does not wake a geocoded identity",
         _outcome(POLICY, MD5_OLD, "matched_exact"), False, ""),
        ("a settled identity is not selected",
         _outcome(POLICY, MD5_NOW, "matched_exact"), False, ""),
        # 5. The explicit operator action.
        ("rematch_all takes precedence over everything",
         _outcome(POLICY, MD5_NOW, "matched_exact"), True, "rematch_all"),
        ("rematch_all selects an identity with no outcome", None, True, "rematch_all"),
    ],
)
def test_pending_reason(name: str, outcome: StoredOutcome | None, rematch_all: bool,
                        expected: str) -> None:
    assert pending_reason(outcome, policy_version=POLICY, reference_md5=MD5_NOW,
                          rematch_all=rematch_all) == expected, name
    assert expected == "" or expected in PENDING_REASONS, name


def test_an_adopted_outcome_is_not_a_resolver_outcome() -> None:
    """The demand scan reads the RESOLVER view of the store, so an adopted row is simply
    absent from its input. Spelled out here because passing the SERVED outcome in would make
    every adopted identity look settled forever and the resolver would never try it again --
    which would quietly freeze 19,413 identities at the imported answer."""
    adopted = _outcome(LEGACY_ADOPTED_POLICY_VERSION, MD5_NOW, "matched_exact")
    # If an adopted row ever reached this function it would look like a policy bump, which
    # is loud rather than silent -- but the loader is what keeps it out.
    assert pending_reason(adopted, policy_version=POLICY, reference_md5=MD5_NOW,
                          rematch_all=False) == "policy_changed"


@pytest.fixture()
def connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("create schema if not exists sweden_company_enrichment")
    connection.execute("""
        create table sweden_company_enrichment.se_addresses_current (
            address_id varchar, address_kind varchar)
    """)
    connection.execute("""
        create table sweden_company_enrichment.se_address_geocodes_previous (
            address_id varchar, policy_version varchar, reference_md5 varchar,
            match_status varchar, match_method varchar, match_confidence double,
            candidate_record_ids varchar[], matched_at timestamptz)
    """)
    yield connection
    connection.close()


def _seed(connection, rows: list[tuple[str, str | None, str | None, str | None]]) -> None:
    for address_id, policy, md5, status in rows:
        connection.execute(
            "insert into sweden_company_enrichment.se_addresses_current values (?, 'physical')",
            [address_id])
        if policy is None:
            continue
        connection.execute(
            "insert into sweden_company_enrichment.se_address_geocodes_previous"
            " values (?, ?, ?, ?, 'exact', 1.0, [], ?)",
            [address_id, policy, md5, status, T1])


def test_the_pending_table_is_the_rule_executed(connection) -> None:
    _seed(connection, [
        ("new", None, None, None),
        ("settled", POLICY, MD5_NOW, "matched_exact"),
        ("stale-geocoded", POLICY, MD5_OLD, "matched_exact"),
        ("stale-ambiguous", POLICY, MD5_OLD, "ambiguous"),
        ("old-policy", OLD_POLICY, MD5_NOW, "matched_exact"),
    ])
    counts = replace_pending_address_identities(
        connection=connection, policy_version=POLICY, reference_md5=MD5_NOW,
        rematch_all=False, log=None)

    rows = dict(connection.execute(
        f"select address_id, pending_reason from {QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE}"
    ).fetchall())
    assert rows == {"new": "no_outcome", "stale-ambiguous": "reference_changed",
                    "old-policy": "policy_changed"}
    assert counts["pending_identities"] == 3
    assert counts["reason_counts"] == {"no_outcome": 1, "reference_changed": 1,
                                       "policy_changed": 1}
    assert counts["short_circuit"] is False


def test_an_unchanged_week_selects_nothing_at_all(connection) -> None:
    """Goal 1, executed: unchanged snapshot plus unchanged policy plus no new identities
    means the resolver has nothing to do and the reference index is never built."""
    _seed(connection, [("settled", POLICY, MD5_NOW, "matched_exact"),
                       ("known-ambiguous", POLICY, MD5_NOW, "ambiguous")])
    counts = replace_pending_address_identities(
        connection=connection, policy_version=POLICY, reference_md5=MD5_NOW,
        rematch_all=False, log=None)
    assert counts["pending_identities"] == 0
    assert counts["short_circuit"] is True
    assert connection.execute(
        f"select count(*) from {QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE}").fetchone()[0] == 0


def test_rematch_all_selects_every_identity(connection) -> None:
    _seed(connection, [("settled", POLICY, MD5_NOW, "matched_exact"),
                       ("new", None, None, None)])
    counts = replace_pending_address_identities(
        connection=connection, policy_version=POLICY, reference_md5=MD5_NOW,
        rematch_all=True, log=None)
    assert counts["pending_identities"] == 2
    assert counts["reason_counts"] == {"rematch_all": 2}
    assert counts["short_circuit"] is False
```

- [ ] **Step 3: Run to verify failure** — `uv run pytest tests/test_sweden_geocode_demand.py -q` → `ModuleNotFoundError: dagster_v3.defs.sweden_company.geocode_demand`

- [ ] **Step 4: Implement `geocode_demand.py`**

```python
"""What the weekly Sweden resolver run is allowed to match -- and nothing else.

The resolver used to score every one of the ~2.09M address identities every Tuesday. An
address_id is a fingerprint of normalized address text, so the text cannot change under it:
for an unchanged matcher and an unchanged OSM snapshot that work reproduces answers the
store already holds. This module computes the set that is genuinely due.

THE RULE, over each identity's CURRENT RESOLVER OUTCOME (geocode_store's stage 1 restricted
to the resolver family -- an imported legacy_adopted_v1 row is not a resolver answer and
never enters here):

  rematch_all         the operator asked for it, explicitly, in the run config
  no_outcome          the identity has no resolver outcome at all -- register churn
  policy_changed      the stored outcome was produced by a different policy version.
                      A policy bump therefore IS a full rematch, and it routes through the
                      golden corpus gate that already sits upstream of the shadow.
  reference_changed   the stored outcome did not geocode AND was computed against a
                      different OSM snapshot than the one this run holds -- the retry pool.

A GEOCODED outcome at a stale reference is deliberately NOT retried. That is where the whole
saving lives: a Geofabrik refresh costs the non-geocoded population (hundreds of thousands),
not the whole universe. An operator who wants the geocoded population re-examined against a
new snapshot passes rematch_all, which is loud and deliberate.

WHY THE FRESH MD5 COMES OUT OF THE DUCKDB REFERENCE TABLE and not from the OSM asset's
Dagster metadata: the promotion stamps reference_md5 from exactly this expression
(`first(source_md5 order by source_record_id)` over sweden_address_osm.address_points), so
reading it the same way here makes it impossible for the demand scan and the stamp to
disagree about which snapshot this run is matching against.
"""

import time
from collections.abc import Callable, Iterator, Sequence
from typing import Any

from dagster_v3.defs.sweden_address_osm import tables as osm_tables
from dagster_v3.defs.sweden_company import shared_addresses
from dagster_v3.defs.sweden_company.geocode_store import (
    ENRICHMENT_SCHEMA,
    GEOCODED_STATUSES,
    QUALIFIED_DUCKDB_PREVIOUS_OUTCOMES_TABLE,
    PREVIOUS_OUTCOMES_TABLE,
    StoredOutcome,
    build_current_resolver_geocodes_sql,
    is_geocoded,
)

PENDING_IDENTITIES_TABLE = "se_address_pending_identities"
QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE = (
    f"{ENRICHMENT_SCHEMA}.{PENDING_IDENTITIES_TABLE}"
)
PENDING_REASONS = (
    "rematch_all",
    "no_outcome",
    "policy_changed",
    "reference_changed",
)
# Everything the demand rule and the two repointed readers (the shadow's comparison and the
# promotion's postcode-conflict gate) need from a previous outcome. Deliberately narrow: the
# whole store's 28 columns for 2.09M identities is not worth streaming.
PREVIOUS_OUTCOME_COLUMNS = (
    "address_id",
    "policy_version",
    "reference_md5",
    "match_status",
    "match_method",
    "match_confidence",
    "candidate_record_ids",
    "matched_at",
)
QUERY_BATCH_SIZE = 100_000
PROGRESS_LOG_ROW_INTERVAL = 500_000

PREVIOUS_OUTCOMES_SQL = build_current_resolver_geocodes_sql(
    columns=PREVIOUS_OUTCOME_COLUMNS
)


def pending_reason(
    outcome: StoredOutcome | None,
    *,
    policy_version: str,
    reference_md5: str,
    rematch_all: bool,
) -> str:
    """The Python twin of the CASE in replace_pending_address_identities."""
    if rematch_all:
        return "rematch_all"
    if outcome is None:
        return "no_outcome"
    if outcome.policy_version != policy_version:
        return "policy_changed"
    if not is_geocoded(outcome.match_status) and outcome.reference_md5 != reference_md5:
        return "reference_changed"
    return ""


def fresh_reference_md5(connection: Any) -> str:
    """The OSM snapshot identity this run holds, read exactly as the promotion stamps it."""
    [(reference_md5,)] = connection.execute(
        f"""
        select coalesce(first(source_md5 order by source_record_id), '')
        from {osm_tables.QUALIFIED_ADDRESS_TABLE}
        """
    ).fetchall()
    if not str(reference_md5):
        raise ValueError(
            "The Sweden OSM reference table carries no snapshot MD5 -- refusing to "
            "compute matching demand against an unidentifiable reference"
        )
    return str(reference_md5)


def load_current_resolver_outcomes(
    *,
    connection: Any,
    clickhouse_client: Any,
    log: Callable[..., object] | None = None,
) -> int:
    """Stream the store's current resolver outcome per identity into DuckDB."""
    connection.execute(f"create schema if not exists {ENRICHMENT_SCHEMA}")
    connection.execute(
        f"""
        create or replace table {QUALIFIED_DUCKDB_PREVIOUS_OUTCOMES_TABLE} (
            address_id varchar,
            policy_version varchar,
            reference_md5 varchar,
            match_status varchar,
            match_method varchar,
            match_confidence double,
            candidate_record_ids varchar[],
            matched_at timestamptz
        )
        """
    )
    started_at = time.monotonic()
    loaded_rows = 0
    batch: list[Sequence[object]] = []
    for row in _iter_clickhouse_rows(clickhouse_client):
        batch.append(row)
        if len(batch) < QUERY_BATCH_SIZE:
            continue
        _insert_previous_outcome_batch(connection, batch)
        loaded_rows += len(batch)
        batch.clear()
        if loaded_rows % PROGRESS_LOG_ROW_INTERVAL == 0:
            _log(
                log,
                "Loading current Sweden resolver outcomes: rows=%d elapsed_seconds=%.1f",
                loaded_rows,
                time.monotonic() - started_at,
            )
    _insert_previous_outcome_batch(connection, batch)
    loaded_rows += len(batch)
    _log(
        log,
        "Loaded current Sweden resolver outcomes: rows=%d elapsed_seconds=%.1f",
        loaded_rows,
        time.monotonic() - started_at,
    )
    return loaded_rows


def replace_pending_address_identities(
    *,
    connection: Any,
    policy_version: str,
    reference_md5: str,
    rematch_all: bool,
    log: Callable[..., object] | None = None,
) -> dict[str, object]:
    """Build the identity set this run will match, with the reason for each."""
    started_at = time.monotonic()
    connection.execute(
        f"""
        create or replace table {QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE} as
        select address_id, pending_reason
        from (
            select
                cast(address.address_id as varchar) as address_id,
                case
                    when ?::boolean then 'rematch_all'
                    when previous.address_id is null then 'no_outcome'
                    when previous.policy_version != ?::varchar then 'policy_changed'
                    when previous.match_status not in ({_quoted(GEOCODED_STATUSES)})
                     and previous.reference_md5 != ?::varchar then 'reference_changed'
                    else ''
                end as pending_reason
            from {shared_addresses.QUALIFIED_SHARED_ADDRESSES_TABLE} address
            left join {QUALIFIED_DUCKDB_PREVIOUS_OUTCOMES_TABLE} previous
                on previous.address_id = cast(address.address_id as varchar)
        ) candidates
        where pending_reason != ''
        """,
        [rematch_all, policy_version, reference_md5],
    )
    reason_counts = {
        str(reason): int(count)
        for reason, count in connection.execute(
            f"""
            select pending_reason, count(*)
            from {QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE}
            group by pending_reason
            order by pending_reason
            """
        ).fetchall()
    }
    pending = pending_identity_count(connection)
    _log(
        log,
        "Sweden geocoding demand: pending=%d policy=%s reference=%s elapsed_seconds=%.1f",
        pending,
        policy_version,
        reference_md5,
        time.monotonic() - started_at,
    )
    return {
        "pending_identities": pending,
        "reason_counts": reason_counts,
        "policy_version": policy_version,
        "reference_md5": reference_md5,
        "rematch_all": rematch_all,
        "short_circuit": pending == 0,
    }


def pending_identity_count(connection: Any) -> int:
    [(count,)] = connection.execute(
        f"select count(*) from {QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE}"
    ).fetchall()
    return int(count)


def _iter_clickhouse_rows(clickhouse_client: Any) -> Iterator[Sequence[object]]:
    execute_iter = getattr(clickhouse_client, "execute_iter", None)
    if callable(execute_iter):
        yield from execute_iter(
            PREVIOUS_OUTCOMES_SQL,
            settings={"max_block_size": QUERY_BATCH_SIZE},
        )
        return
    yield from clickhouse_client.execute(PREVIOUS_OUTCOMES_SQL)


def _insert_previous_outcome_batch(
    connection: Any,
    rows: Sequence[Sequence[object]],
) -> None:
    if not rows:
        return
    connection.executemany(
        f"insert into {QUALIFIED_DUCKDB_PREVIOUS_OUTCOMES_TABLE} "
        "values (?, ?, ?, ?, ?, ?, ?, ?)",
        [list(row) for row in rows],
    )


def _quoted(values: Sequence[str]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def _log(log: Callable[..., object] | None, message: str, *args: object) -> None:
    if log is not None:
        log(message, *args)
```

`PREVIOUS_OUTCOMES_TABLE` is imported for the asset's metadata only; drop it from the import list if ruff flags it unused.

- [ ] **Step 5: Scope the shadow to the pending set and repoint its comparison**

In `address_resolution_shadow.py`:

1. Import the demand module beside the others (`geocode_demand` — note it imports `shared_addresses` and `geocode_store` only, so there is no cycle) and the store module.
2. Short-circuit at the top of `replace_sweden_address_resolution_shadow`, before `_replace_query_documents` (which is before ANY reference index is built — that is the point):

```python
    connection.execute(
        f"create schema if not exists {address_canonicalization.ENRICHMENT_SCHEMA}"
    )
    pending = geocode_demand.pending_identity_count(connection)
    if pending == 0:
        # Nothing to match: no query documents, no OSM building or street reference index,
        # no candidate generation. The shadow tables keep the last matching run's contents,
        # which is what they already do between runs -- they have never been a per-run
        # artefact of a run that matched nothing.
        _log(log, "Sweden address resolution: no pending identities, skipping matching")
        return {"pending_identities": 0, "short_circuit": True,
                "shadow_status_counts": {}, "largest_transitions": []}
```

The caller (`address_resolution_assets.py:86-102`) spreads `counts` into metadata and reads `counts["shadow_status_counts"]` and `counts["largest_transitions"]`, so both keys must be present in the short-circuit dict — they are.

3. Scope the query documents. In `_replace_query_documents`, add the pending predicate to the `source_sql` (after `from {shared_addresses.QUALIFIED_SHARED_ADDRESSES_TABLE}` at `:177`):

```sql
            where cast(address_id as varchar) in (
                select address_id
                from {geocode_demand.QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE}
            )
```

4. Repoint the comparison. In `_replace_comparison` (`:526-553`) replace the `inner join … QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE current` block with:

```sql
        left join {
            geocode_store.QUALIFIED_DUCKDB_PREVIOUS_OUTCOMES_TABLE
        } current
            on current.address_id = shadow.query_document_id
```

and change the projected `current.match_status as current_status` to
`coalesce(current.match_status, '') as current_status`. The join becomes LEFT because a
new identity has no previous outcome at all, and `_assert_shadow_invariants` requires one
comparison row per result (`:587-588`) — an INNER join would drop exactly the new
identities, which are the rows a demand-driven run cares most about. `''` is the honest
value for "there was nothing here before", and `_shadow_counts`'s transition query
(`:602-611`) reports it as its own transition class.

5. Point `_assert_shadow_invariants`'s coverage term at the pending set instead of every
address. Its current `results == queries` term (`:585-586`) already holds — the query
documents ARE the pending set now — so add one term rather than changing one:

```python
    [(pending,)] = connection.execute(
        f"select count(*) from {geocode_demand.QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE}"
    ).fetchall()
    if int(queries) != int(pending):
        raise ValueError(
            "Shadow query documents must be exactly the pending Sweden identities"
        )
```

- [ ] **Step 6: Short-circuit promotion, repoint its gate, and scope its invariants**

In `address_resolution_promotion.py`:

1. Short-circuit at the top of `replace_current_geocodes_from_address_resolution_shadow`, before `_assert_shadow_is_promotable`:

```python
    pending = geocode_demand.pending_identity_count(connection)
    if pending == 0:
        _log(log, "Sweden address resolution: nothing pending, nothing to promote")
        connection.execute(
            f"""
            create or replace table {
                geocode_store.QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE
            } as
            select {", ".join(geocode_store.STORE_COLUMNS)}
            from {geocode_store.QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE}
            where false
            """
        )
        return {"rows": 0, "geolocated": 0, "evaluation_run_id": "",
                "policy_version": expected_policy_version, "reference_md5": "",
                "appended_rows": 0, "status_counts": {}, "short_circuit": True,
                "table": shared_address_geocoding.QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE,
                "append_table": geocode_store.QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE}
```

The `where false` rewrite empties the append table in place rather than dropping it, so the ClickHouse append asset downstream finds a correctly-typed empty table and appends 0 rows. On the very first run after deploy the table does not exist yet — guard it with `create table if not exists` semantics by wrapping in a `try` is NOT the pattern here; instead create it from the store's column list when it is absent:

```python
        _ensure_empty_append_table(connection)
```

with

```python
def _ensure_empty_append_table(connection: Any) -> None:
    """An empty, correctly-typed append table for a run that promoted nothing.

    Derived from the shadow-era stage projection rather than hand-typed: building the stage
    with a `where false` predicate costs nothing and cannot drift from the real shape.
    """
    connection.execute(
        f"""
        create or replace table {geocode_store.QUALIFIED_DUCKDB_GEOCODE_APPEND_TABLE} as
        select {", ".join(geocode_store.STORE_COLUMNS)}
        from {PROMOTION_STAGE_TABLE}
        where false
        """
    )
```

and call `_replace_promotion_stage(...)` before it in the short-circuit branch as well — with an empty pending set the stage's inner joins yield zero rows, so building it is free and the schema comes from the same projection the real path uses. Replace the `_ensure_empty_append_table` call site accordingly: build the stage, then create the append table from it, then return.

2. Repoint the postcode-conflict gate. In `_assert_shadow_is_promotable` (`:162-215`) replace

```sql
            inner join {
                shared_address_geocoding.QUALIFIED_DUCKDB_ADDRESS_GEOCODES_TABLE
            } current
                on cast(current.address_id as varchar) = shadow.query_document_id
```

with

```sql
            inner join {
                geocode_store.QUALIFIED_DUCKDB_PREVIOUS_OUTCOMES_TABLE
            } current
                on current.address_id = shadow.query_document_id
```

The `cast(... as varchar)` goes because the loaded table already stores `address_id` as
`varchar`. The gate keeps its INNER join: an identity with no previous outcome has no
served answer to protect, which is exactly what "no row" should mean here.

3. Scope the two coverage invariants from "every current address" to "every pending
identity". In `_assert_shadow_is_promotable`, the block at `:127-135` counts
`shared_addresses.QUALIFIED_SHARED_ADDRESSES_TABLE`; keep those counts (the uniqueness and
one-identity-run terms at `:216-221` still want them) but change the two comparisons that
used `address_rows` as the expected result count:

```python
    [(pending_rows,)] = connection.execute(
        f"select count(*) from {geocode_demand.QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE}"
    ).fetchall()
    ...
    if int(result_rows) != int(unique_results) or int(result_rows) != int(pending_rows):
        raise ValueError("Shadow results must contain one row per pending identity")
```

and the `set_mismatches` query's `address` side becomes the pending table rather than
`se_addresses_current`:

```sql
            from (
                select address_id
                from {geocode_demand.QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE}
            ) address
```

with its error message updated to "Shadow results and pending Sweden identities must have
equal IDs".

4. The same substitution in `_assert_promoted_geocode_invariants`: its `address_rows` query
(`:402-404`) reads the pending table, and its message becomes "Promoted geocodes must
contain one row per pending identity".

- [ ] **Step 7: Add the demand asset and delete the join matcher**

In `address_geocoding_assets.py`, add the constant and the asset (place it where
`sweden_shared_address_osm_matches_duckdb` was, `:377-411`, so the diff reads as a swap):

```python
GEOCODE_DEMAND_ASSET_KEY = "sweden_address_geocode_demand_duckdb"


class SwedenGeocodeDemandConfig(dg.Config):
    """`rematch_all` is the explicit operator action of spec 4.2 item 3 -- the same
    spelled-out-in-config pattern the se_company finals use for `resolve_all`."""

    rematch_all: bool = False


@dg.asset(
    name=GEOCODE_DEMAND_ASSET_KEY,
    deps=[
        dg.AssetKey(SHARED_ADDRESSES_CLICKHOUSE_ASSET_KEY),
        dg.AssetKey("sweden_osm_addresses_duckdb"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "openstreetmap"},
    pool=osm_tables.DUCKDB_POOL,
    metadata={"table": geocode_demand.QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE},
    description=(
        "Decides which Sweden address identities this run must match: those with no "
        "resolver outcome, those whose outcome came from a different policy version, "
        "and non-geocoded outcomes whose OSM reference snapshot has moved."
    ),
)
def sweden_address_geocode_demand_duckdb(
    context: dg.AssetExecutionContext,
    config: SwedenGeocodeDemandConfig,
    sweden_address_osm_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=geocode_store.CLICKHOUSE_DATABASE,
        tables=(geocode_store.GEOCODE_STORE_TABLE,),
    )
    with sweden_address_osm_duckdb.get_connection() as connection:
        reference_md5 = geocode_demand.fresh_reference_md5(connection)
        with clickhouse.get_connection() as clickhouse_client:
            loaded = geocode_demand.load_current_resolver_outcomes(
                connection=connection,
                clickhouse_client=clickhouse_client,
                log=context.log.info,
            )
        counts = geocode_demand.replace_pending_address_identities(
            connection=connection,
            policy_version=SWEDEN_ADDRESS_RESOLUTION_POLICY.version,
            reference_md5=reference_md5,
            rematch_all=config.rematch_all,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={
            **{
                key: value
                for key, value in counts.items()
                if not isinstance(value, dict | list)
            },
            "loaded_previous_outcomes": loaded,
            "reason_counts": dg.MetadataValue.json(counts["reason_counts"]),
            "table": geocode_demand.QUALIFIED_DUCKDB_PENDING_IDENTITIES_TABLE,
        }
    )
```

Then delete `sweden_shared_address_osm_matches_duckdb` (`:377-411`), delete the
`SHARED_GEOCODE_DUCKDB_ASSET_KEY` constant (`:29`), and replace it with
`GEOCODE_DEMAND_ASSET_KEY` in the three job selections that name it (`:1162`, `:1189`,
`:1211`) and in `defs.assets` (`:1246`). Add `geocode_demand` and `geocode_store` to the
imports.

In `address_resolution_assets.py`, change the shadow's dep at `:57` from
`dg.AssetKey("sweden_shared_address_osm_matches_duckdb")` to
`dg.AssetKey("sweden_address_geocode_demand_duckdb")`, and pass the short-circuit through to
the metadata by leaving the existing `**{k: v for ...}` spread alone (`short_circuit` and
`pending_identities` are scalars and flow through as they are).

In `shared_address_geocoding.py`, delete `replace_sweden_shared_address_osm_matches`
(`:48-122`), `_create_osm_match_reference_tables` (`:125-168`),
`_create_shared_address_geocode_results` (`:171-501`),
`_assert_shared_address_geocode_invariants` (`:504-606`) and
`_assert_shared_addresses_available` (`:609-614`). What stays is the module's first 45 lines
— the table names and `ADDRESS_GEOCODE_COLUMNS` — plus nothing else; delete the now-unused
`time`, `datetime`, `Callable`, `address_matching`, `osm_tables` and `shared_addresses`
imports and the `_count`/`_log` helpers. Replace the module's (absent) docstring with:

```python
"""The Sweden shared-address serving table's names and column contract.

This module used to hold a second OSM matcher whose output was overwritten by the
resolver's promotion before anything read it. The matcher is gone (spec section 4.3); what
survives is the naming of se_address_geocodes_current, which is now DERIVED from
corpscout.se_address_geocodes by geocode_store's versioned read.
"""
```

Its geocode invariants are NOT ported wholesale: every one of them is re-asserted on the
promoted stage by `_assert_promoted_geocode_invariants`, which runs on the authoritative
matcher's output rather than on a discarded one. The two the promotion does not carry —
the `matched_site`/`matched_area` spread bounds against `SITE_MAX_SPREAD_METERS` and
`AREA_MAX_SPREAD_METERS` — belong to the join matcher's own spread ladder, which the
resolver's policy replaces (`address_resolution_policy.py:12-13` carries the same two
thresholds and `resolution.py:711-738` applies them), and they are pinned in
`tests/test_address_resolution.py`. Say so in the deleting commit.

- [ ] **Step 8: Delete the join matcher's tests**

In `tests/test_sweden_company_address_geocoding.py`, seven tests call
`replace_sweden_shared_address_osm_matches` and then assert on
`sweden_company_enrichment.se_address_geocodes_current`: at `:1018-…` (inside
`test_sweden_company_address_matching_only_accepts_unique_exact_osm_rows`), `:1263`,
`:1406`, `:1561`, `:1716`, `:1849` and `:1938-1990`. In each, delete the import, the
`replace_sweden_shared_address_osm_matches(...)` call, its `assert … counts == {...}` block
and every subsequent query against `se_address_geocodes_current`. The legacy-matcher half
of each test stays (it retires in Task 8) and every canonical/members/links assertion stays.

**These assertions are not being relocated, they are being retired with the code they
covered.** They pin the JOIN matcher's classification ladder — count-and-spread only, no
`matched_corrected`, no `property_identifier` (subsystem map §7) — which is not the
resolver's ladder. The resolver's own semantics are pinned in `tests/test_address_resolution.py`
(`:280-430` for promotion, and the policy tests for the ladder). Deleting a matcher deletes
its tests; keeping them would mean keeping the matcher.

In the job-shape test (`:1996-2127`), replace every
`"sweden_shared_address_osm_matches_duckdb"` with
`"sweden_address_geocode_demand_duckdb"`, and change the parent-key assertions:

```python
    demand = repo.asset_graph.get(dg.AssetKey("sweden_address_geocode_demand_duckdb"))
    assert demand.parent_keys == {
        dg.AssetKey("sweden_shared_addresses_clickhouse"),
        dg.AssetKey("sweden_osm_addresses_duckdb"),
    }
    assert demand.pools == {"sweden_address_osm_duckdb"}
    assert resolution_shadow.parent_keys == {
        dg.AssetKey("sweden_address_resolution_golden_evaluation"),
        dg.AssetKey("sweden_address_geocode_demand_duckdb"),
    }
```

- [ ] **Step 9: Run**

```bash
cd corpscout/services/dagster_v3
uv run pytest tests/test_sweden_geocode_demand.py tests/test_sweden_geocode_store.py \
              tests/test_sweden_geocode_store_append.py tests/test_address_resolution.py \
              tests/test_sweden_company_address_geocoding.py -q
uv run dg check defs && uv run ruff check src/dagster_v3/defs/sweden_company
rg -n "replace_sweden_shared_address_osm_matches|sweden_shared_address_osm_matches_duckdb" \
   /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout --glob '!*.pyc'
```

Expected: all green; the final `rg` returns **nothing** except this plan and the spec.

- [ ] **Step 10: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/geocode_demand.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/geocode_store.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/shared_address_geocoding.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_resolution_shadow.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_resolution_promotion.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_resolution_assets.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_geocoding_assets.py \
        corpscout/services/dagster_v3/tests/test_sweden_geocode_demand.py \
        corpscout/services/dagster_v3/tests/test_sweden_company_address_geocoding.py
git commit -m "feat(sweden_company): match only the Sweden address identities that are due

The weekly resolver run now scores new identities, everything after a policy bump, and
non-geocoded outcomes whose OSM snapshot moved. An unchanged snapshot with no new
identities builds no reference index at all.

Retires the join matcher, whose output the resolver's promotion overwrote before anything
read it. Its two in-run readers -- the shadow comparison and the promotion's
postcode-conflict gate -- now read the store's previous resolver outcome instead.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: `se_address_geocodes_current` derived from the store, plus the transition parity check

Spec §6 step 2's tail and §8's "transition drift" mitigation. After this task the serving table is a projection of the store, not a rebuild artefact — and crucially it stops re-stamping `matched_at` on 2.09M unchanged rows every Tuesday, which is what makes the `se_company_address` Monday scan shrink even before Task 10 repoints it.

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/address_geocoding_assets.py` (rewrite `sweden_address_geocodes_clickhouse` as a ClickHouse-native derivation; add the parity check)
- Modify: `corpscout/services/dagster_v3/tests/test_sweden_company_address_geocoding.py` (job-shape test: the asset's parent and pool change)
- Test: `corpscout/services/dagster_v3/tests/test_sweden_geocode_derivation.py`

**Interfaces (Tasks 7, 10, 11 consume):**
- `address_geocoding_assets.build_derived_current_geocodes_sql() -> str` — the `INSERT … SELECT` body that fills the stage
- `address_geocoding_assets.DERIVED_PARITY_SQL: str` — the check's single two-sided query
- Asset `sweden_address_geocodes_clickhouse` keeps its key and its table; its dep becomes `sweden_address_geocode_store_clickhouse` and it no longer takes a DuckDB connection or the DuckDB pool
- Asset check `derived_current_matches_the_store`

- [ ] **Step 1: Write the failing tests**

`corpscout/services/dagster_v3/tests/test_sweden_geocode_derivation.py`:

```python
"""The serving table is a projection of the store, and something says so out loud.

The derivation is a stage + INSERT-SELECT + EXCHANGE, copied from
defs/company_financials_latest/assets.py:52-70 -- the house's ClickHouse-native replace.
"""
import re

from dagster_v3.defs.sweden_company.address_geocoding_assets import (
    DERIVED_PARITY_SQL,
    build_derived_current_geocodes_sql,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    SERVING_COLUMNS,
    build_current_geocodes_sql,
)


def test_the_derivation_is_the_versioned_read_and_nothing_else() -> None:
    """No second ranking may exist. If this SELECT ever stops being byte-identical to
    geocode_store's fragment, the serving table and the final would disagree about which
    outcome is current, and both would look internally consistent."""
    sql = build_derived_current_geocodes_sql()
    assert sql == build_current_geocodes_sql(columns=SERVING_COLUMNS)
    # The 26 serving columns, in the order the target declares them.
    assert re.findall(r"^    (\w+),?$", sql[: sql.index("\nFROM (")], re.MULTILINE) == list(
        SERVING_COLUMNS)


def test_the_parity_check_compares_content_and_not_only_counts() -> None:
    """A stale EXCHANGE leaves the previous week's table in place with the RIGHT row count
    and the wrong rows. Counts alone would pass; the checksum is what makes the check able
    to fail."""
    assert "cityHash64" in DERIVED_PARITY_SQL
    assert "UNION ALL" in DERIVED_PARITY_SQL
    assert "'derived'" in DERIVED_PARITY_SQL and "'store'" in DERIVED_PARITY_SQL
    for column in ("address_id", "match_status", "latitude", "longitude", "matched_at"):
        assert DERIVED_PARITY_SQL.count(column) >= 2, column
    # Nullable columns are read through ifNull on BOTH sides, so the two checksums are
    # comparable under either join_use_nulls setting.
    assert DERIVED_PARITY_SQL.count("ifNull(toString(latitude), '')") == 2
```

Update the job-shape test in `tests/test_sweden_company_address_geocoding.py`:

```python
    assert shared_geocode_clickhouse.parent_keys == {
        dg.AssetKey("sweden_address_geocode_store_clickhouse")
    }
    # It no longer touches DuckDB, so it no longer holds the serialising OSM pool -- which
    # is the point: the derivation runs beside the DuckDB stages rather than behind them.
    assert shared_geocode_clickhouse.pools == set()
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/test_sweden_geocode_derivation.py -q` → `ImportError` on `build_derived_current_geocodes_sql`

- [ ] **Step 3: Rewrite the publish asset as a derivation**

In `address_geocoding_assets.py`, replace `sweden_address_geocodes_clickhouse` (`:414-486`) with:

```python
def build_derived_current_geocodes_sql() -> str:
    """The serving table's contents: the store's versioned read, projected to 26 columns.

    Deliberately not a second expression of the rule -- it IS geocode_store's fragment,
    asked for the serving column list. A separate ranking here would let the serving table
    and the se_company_address final disagree about which outcome is current while both
    looked internally consistent.
    """
    return geocode_store.build_current_geocodes_sql(
        columns=geocode_store.SERVING_COLUMNS
    )


def _parity_side_sql(label: str, table: str) -> str:
    return f"""SELECT
    '{label}' AS side,
    count() AS rows,
    uniqExact(address_id) AS identities,
    sum(cityHash64(
        toString(address_id),
        toString(match_status),
        ifNull(toString(latitude), ''),
        ifNull(toString(longitude), ''),
        toString(matched_at)
    )) AS content_hash
FROM {table}"""


DERIVED_PARITY_SQL = (
    _parity_side_sql(
        "derived", shared_address_geocoding.QUALIFIED_CLICKHOUSE_ADDRESS_GEOCODES_TABLE
    )
    + "\nUNION ALL\n"
    + _parity_side_sql("store", f"(\n{build_derived_current_geocodes_sql()}\n)")
)


@dg.asset(
    deps=[dg.AssetKey(GEOCODE_STORE_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "openstreetmap"},
    metadata={
        "table": (shared_address_geocoding.QUALIFIED_CLICKHOUSE_ADDRESS_GEOCODES_TABLE)
    },
    description=(
        "Derives the Sweden shared-address serving table from the versioned geocode "
        "store: the current outcome per identity, staged and swapped atomically. "
        "Transitional -- consumers migrate to the store's versioned read on their own "
        "schedule and this table retires when the last one has."
    ),
)
def sweden_address_geocodes_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=geocode_store.CLICKHOUSE_DATABASE,
        tables=(
            geocode_store.GEOCODE_STORE_TABLE,
            shared_address_geocoding.ADDRESS_GEOCODES_TABLE,
        ),
    )
    target = shared_address_geocoding.QUALIFIED_CLICKHOUSE_ADDRESS_GEOCODES_TABLE
    stage = (
        f"{geocode_store.CLICKHOUSE_DATABASE}."
        f"_tmp_{shared_address_geocoding.ADDRESS_GEOCODES_TABLE}_{uuid.uuid4().hex}"
    )
    columns = ", ".join(geocode_store.SERVING_COLUMNS)
    with clickhouse.get_connection() as client:
        client.execute(f"CREATE TABLE {stage} AS {target}")
        try:
            client.execute(
                f"INSERT INTO {stage} ({columns})\n"
                f"{build_derived_current_geocodes_sql()}"
            )
            [(rows, identities)] = client.execute(
                f"SELECT count(), uniqExact(address_id) FROM {stage}"
            )
            if int(rows) == 0:
                raise ValueError(
                    f"{stage} has 0 rows -- refusing to replace {target}. The geocode "
                    "store is empty or the versioned read returned nothing."
                )
            if int(rows) != int(identities):
                raise ValueError(
                    f"{stage} holds {rows} rows for {identities} identities -- the "
                    "versioned read must yield exactly one outcome per identity"
                )
            client.execute(f"EXCHANGE TABLES {stage} AND {target}")
        finally:
            client.execute(f"DROP TABLE IF EXISTS {stage}")
        status_counts = {
            str(status): int(count)
            for status, count in client.execute(
                f"""
                SELECT match_status, count()
                FROM {target}
                GROUP BY match_status
                ORDER BY match_status
                """
            )
        }
        [(geolocated,)] = client.execute(
            f"""
            SELECT count()
            FROM {target}
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            """
        )
    context.log.info("Derived %s from the geocode store: %s rows", target, rows)
    return dg.MaterializeResult(
        metadata={
            "rows": int(rows),
            "geolocated": int(geolocated),
            "exact_match_rate_percent": (
                100.0 * status_counts.get("matched_exact", 0) / int(rows)
                if int(rows) > 0
                else 0.0
            ),
            **status_counts,
            "table": target,
        }
    )
```

`import uuid` goes at the top of the module. The asset no longer takes
`sweden_address_osm_duckdb`, and its `pool=osm_tables.DUCKDB_POOL` is removed — it does no
DuckDB work, and holding the serialising pool would keep 11 other assets waiting for a
ClickHouse-only step.

- [ ] **Step 4: Add the transition parity check**

Directly after the asset:

```python
@dg.asset_check(
    asset=sweden_address_geocodes_clickhouse,
    name="derived_current_matches_the_store",
    description=(
        "Fails when the derived Sweden serving geocode table disagrees with the "
        "versioned store read it is supposed to be a projection of."
    ),
)
def sweden_address_geocodes_derived_parity_check(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    """The transition's one safety net.

    The derivation IS this read, so the interesting failure is not an arithmetic mistake --
    it is a stage that never swapped, leaving last week's table in place. That table has a
    plausible row count and the wrong rows, so the content hash is what makes this check
    able to fail at all. It retires with the serving table.
    """
    with clickhouse.get_connection() as client:
        sides = {
            str(side): (int(rows), int(identities), int(content_hash))
            for side, rows, identities, content_hash in client.execute(
                DERIVED_PARITY_SQL
            )
        }
    derived, store = sides["derived"], sides["store"]
    return dg.AssetCheckResult(
        passed=derived == store,
        metadata={
            "derived_rows": derived[0],
            "store_rows": store[0],
            "derived_identities": derived[1],
            "store_identities": store[1],
            "content_hashes_agree": derived[2] == store[2],
        },
    )
```

Register it in `defs.asset_checks` (`:1252-1260`).

- [ ] **Step 5: Run**

```bash
cd corpscout/services/dagster_v3
uv run pytest tests/test_sweden_geocode_derivation.py tests/test_sweden_company_address_geocoding.py \
              tests/test_sweden_geocode_store.py -q
uv run dg check defs && uv run ruff check src/dagster_v3/defs/sweden_company
```

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_geocoding_assets.py \
        corpscout/services/dagster_v3/tests/test_sweden_geocode_derivation.py \
        corpscout/services/dagster_v3/tests/test_sweden_company_address_geocoding.py
git commit -m "refactor(sweden_company): derive the Sweden serving geocodes from the store

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

**STOP.** Phase 7 is the controller's (Task 12d) — one weekly cycle under demand-driven matching before anything else lands.

---

### Task 6: The legacy-adoption import — one shot, versioned, auditable, owner-gated

Spec §4.4 and §6 step 3. The resolver refuses ~19,413 companies' addresses as `ambiguous` that the retired per-company matcher resolved `matched_exact` at confidence 1.0 on identical street text. That signal is trapped in a pair of tables about to be dropped; this task moves it into the store as attributable `legacy_adopted_v1` outcomes that the read rule already knows how to rank.

**Files:**
- Create: `src/dagster_v3/defs/sweden_company/geocode_legacy_adoption.py`
- Modify: `src/dagster_v3/defs/sweden_company/address_geocoding_assets.py` (the asset, its config and its own job)
- Test: `corpscout/services/dagster_v3/tests/test_sweden_geocode_legacy_adoption.py`

**Interfaces (Tasks 8, 12e consume):**
- `geocode_legacy_adoption.ADOPTION_CANDIDATES_SQL: str` — the dry run's measurement
- `geocode_legacy_adoption.ADOPTION_INSERT_SQL: str` — the write
- `geocode_legacy_adoption.ADOPTION_SAMPLE_SQL: str` — the controller's verification sample
- Asset key `sweden_address_geocode_legacy_adoption_clickhouse`, config `SwedenGeocodeLegacyAdoptionConfig(execute: bool = False)`, job `sweden_address_geocode_legacy_adoption_job`

**Spec correction — the join path.** §4.4 says "map via `se_company_address_members_current` (canonical_address_key → address_id)". That table does not carry `address_id`: its 20 columns are `(company_id, canonical_address_key, address_key, address_type, address_source, …)` (`address_canonicalization.py:83-104`, migration 000273). The only place the `canonical_address_key → address_id` map exists is **`se_company_address_links_current`** (`shared_addresses.COMPANY_ADDRESS_LINK_COLUMNS`, `(company_id, address_id, canonical_address_key, …)`), which is also the table the retiring baseline check joins on (`address_geocoding_assets.py:1040-1042` uses `result.address_key = canonical.canonical_address_key`). The import joins through links. Everything else in §4.4 stands.

**Two things the spec leaves implicit, decided here.**
1. **Several companies share one identity.** The legacy table is keyed per company, the store per identity. Companies at the same address produce several legacy rows for one `address_id`. The import groups by `address_id` and adopts only where **every** contributing legacy row agrees on the coordinate pair exactly (`uniqExact(tuple(latitude, longitude)) = 1`). Identities whose legacy rows disagree are not adopted; they are counted and reported, because "we had several different answers" is not a decision worth freezing into a versioned store.
2. **`matched_at` is the import instant**, one stamp for the whole import, bound as a parameter so the metadata can report it and a re-run of the same import is a content-identical replace rather than a version bump. That is the one place in this plan where a run-wide stamp is correct: every adopted row genuinely was decided at that instant, by that import.

- [ ] **Step 1: Write the failing tests**

`corpscout/services/dagster_v3/tests/test_sweden_geocode_legacy_adoption.py`:

```python
"""The one-time import: what it adopts, what it refuses, and what the read rule then serves.

Executed against clickhouse-local rather than substring-tested -- the selection is a
three-way join with a HAVING clause, and a substring test cannot tell a correct join from
one that adopts an identity the resolver already geocoded.
"""
import subprocess
from datetime import UTC, datetime

import pytest

from dagster_v3.defs.sweden_company.geocode_legacy_adoption import (
    ADOPTION_CANDIDATES_SQL,
    ADOPTION_INSERT_SQL,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    LEGACY_ADOPTED_MATCH_METHOD,
    LEGACY_ADOPTED_POLICY_VERSION,
    STORE_COLUMNS,
)
from tests.test_se_company_person_clickhouse_local import _clickhouse_local_command, _literal, _render

pytestmark = pytest.mark.integration

POLICY = "se-address-resolution-policy-v5"
IMPORTED_AT = _literal(datetime(2026, 8, 24, 12, tzinfo=UTC))
GEOCODED_AT = _literal(datetime(2026, 8, 20, tzinfo=UTC))
# One identity per scenario, one 64-hex id each.
ADOPTABLE = "1" * 64        # resolver ambiguous, one legacy exact
ALREADY_GEOCODED = "2" * 64  # resolver matched_exact -- must NOT be adopted
DISAGREEING = "3" * 64       # two legacy rows, two coordinates -- must NOT be adopted
NOT_EXACT = "4" * 64         # legacy matched_area -- must NOT be adopted
SHARED = "5" * 64            # three companies, one agreed coordinate -- adopted ONCE


def test_the_selection_names_its_three_tables_and_its_two_predicates() -> None:
    for table in ("corpscout.se_company_address_geocode_results",
                  "corpscout.se_company_address_links_current",
                  "corpscout.se_address_geocodes"):
        assert table in ADOPTION_CANDIDATES_SQL
    assert "match_status = 'matched_exact'" in ADOPTION_CANDIDATES_SQL
    assert "match_confidence = 1.0" in ADOPTION_CANDIDATES_SQL
    assert "uniqExact(tuple(legacy.latitude, legacy.longitude)) = 1" in ADOPTION_CANDIDATES_SQL
    # It is joined through LINKS, not members -- members carries no address_id at all.
    assert "se_company_address_members_current" not in ADOPTION_CANDIDATES_SQL


def test_the_insert_stamps_the_adoption_version_and_writes_every_store_column() -> None:
    assert ADOPTION_INSERT_SQL.startswith(
        "INSERT INTO corpscout.se_address_geocodes (" + ", ".join(STORE_COLUMNS) + ")")
    assert f"'{LEGACY_ADOPTED_POLICY_VERSION}' AS policy_version" in ADOPTION_INSERT_SQL
    assert f"'{LEGACY_ADOPTED_MATCH_METHOD}' AS match_method" in ADOPTION_INSERT_SQL
    assert "%(imported_at)s" in ADOPTION_INSERT_SQL
    # The adopted row carries the LEGACY provenance, not the resolver's.
    for column in ("source_url", "source_object_key", "source_md5", "source_snapshot_at",
                   "source_retrieved_at"):
        assert f"legacy.{column}" in ADOPTION_INSERT_SQL or f"any(legacy.{column})" in ADOPTION_INSERT_SQL


@pytest.fixture(scope="module")
def sections() -> dict[str, list[list[str]]]:
    """Runs the fixture, the import and the read rule end to end in clickhouse-local."""
    command = _clickhouse_local_command()
    try:
        completed = subprocess.run(command, input=_script(), capture_output=True,
                                   text=True, timeout=600)
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


def test_only_the_trapped_decisions_are_adopted(sections) -> None:
    adopted = {row[0] for row in sections["adopted"]}
    assert adopted == {ADOPTABLE, SHARED}
    assert ALREADY_GEOCODED not in adopted, "the resolver already answered for this one"
    assert DISAGREEING not in adopted, "two legacy coordinates is not a decision"
    assert NOT_EXACT not in adopted, "only matched_exact at confidence 1.0 is adopted"


def test_a_shared_identity_is_adopted_exactly_once(sections) -> None:
    """Three companies, one address identity, one store row -- the grain change the whole
    import has to get right."""
    assert [row[0] for row in sections["adopted"]].count(SHARED) == 1


def test_the_read_rule_serves_the_adopted_coordinate(sections) -> None:
    served = {row[0]: (row[1], row[2]) for row in sections["served"]}
    assert served[ADOPTABLE] == (LEGACY_ADOPTED_POLICY_VERSION, "matched_exact")
    # ... and the resolver's own answer still wins where it had one.
    assert served[ALREADY_GEOCODED][0] == POLICY


def test_a_later_resolver_success_outranks_the_adopted_row(sections) -> None:
    """Spec 4.4's reversibility claim, executed: nothing is merged, the adopted row simply
    stops being the newest servable answer."""
    served = {row[0]: (row[1], row[2]) for row in sections["served_after_resolver_success"]}
    assert served[ADOPTABLE] == (POLICY, "matched_exact")
```

`_script()` builds: migration 000271 + 000277 (the legacy results table), 000274 (links),
000316 (the store), the five fixtures above, an `INSERT` of the resolver outcomes into the
store, `_render(ADOPTION_INSERT_SQL, {...})`, then three marked sections — `adopted`
(the store's `legacy_adopted_v1` rows), `served` (`build_current_geocodes_sql` projected to
`address_id, policy_version, match_status`), an append of a later resolver `matched_exact`
for `ADOPTABLE`, and `served_after_resolver_success`. Follow
`tests/test_se_company_address_clickhouse_local.py:120-144` for the per-statement migration
replay and `:260-262` for `_marked`.

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/test_sweden_geocode_legacy_adoption.py -q` → `ModuleNotFoundError`

- [ ] **Step 3: Implement `geocode_legacy_adoption.py`**

```python
"""The one-time import of the retired per-company matcher's trapped exact decisions.

The resolver refuses ~19,413 companies' addresses as `ambiguous` that the legacy matcher
resolved `matched_exact` at confidence 1.0 on identical street text (measured on prod
2026-08-24). Those decisions live only in se_company_address_geocode_results, which retires
with the matcher. This module moves them into the versioned store as `legacy_adopted_v1`
outcomes so they stay attributable, rankable and reversible.

REVERSIBLE, NOT MERGED. An adopted row is distinguishable by its policy_version forever.
geocode_store's read rule serves it only while the identity's resolver outcome is
non-geocoded -- the moment a resolver run answers, the resolver's row outranks it and the
adopted row simply stops being served. Nothing is deleted and nothing is overwritten.

THE GRAIN CHANGE. The legacy table is keyed (company_id, canonical_address_key); the store
is keyed by address identity. Companies sharing an address produce several legacy rows for
one address_id, so the import groups by address_id and adopts only where every contributing
row agrees on the coordinate exactly. An identity whose legacy rows disagree is not adopted
-- "we had several different answers" is not a decision worth freezing into a store.

THE JOIN PATH is legacy -> se_company_address_links_current -> address_id. The design
document names se_company_address_members_current, which carries no address_id column at
all -- links is the only table holding that map.
"""

from dagster_v3.defs.sweden_company import shared_addresses
from dagster_v3.defs.sweden_company.address_geocoding import (
    QUALIFIED_CLICKHOUSE_RESULTS_TABLE,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    GEOCODED_STATUSES,
    LEGACY_ADOPTED_MATCH_METHOD,
    LEGACY_ADOPTED_POLICY_VERSION,
    QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE,
    STORE_COLUMNS,
    build_current_resolver_geocodes_sql,
)

_GEOCODED = ", ".join(f"'{status}'" for status in GEOCODED_STATUSES)

# The identity's current RESOLVER answer -- adopted rows are irrelevant here, and an
# identity the resolver has already geocoded is never adopted.
_NON_GEOCODED_RESOLVER_SQL = build_current_resolver_geocodes_sql(
    columns=("address_id", "match_status")
)

_SELECTION_SQL = f"""FROM {QUALIFIED_CLICKHOUSE_RESULTS_TABLE} AS legacy
INNER JOIN {
    shared_addresses.QUALIFIED_CLICKHOUSE_COMPANY_ADDRESS_LINKS_TABLE
} AS links
    ON links.company_id = legacy.company_id
   AND links.canonical_address_key = legacy.address_key
INNER JOIN (
{_NON_GEOCODED_RESOLVER_SQL}
) AS resolver ON resolver.address_id = links.address_id
WHERE legacy.match_status = 'matched_exact'
  AND legacy.match_confidence = 1.0
  AND isNotNull(legacy.latitude)
  AND isNotNull(legacy.longitude)
  AND resolver.match_status NOT IN ({_GEOCODED})
GROUP BY links.address_id
HAVING uniqExact(tuple(legacy.latitude, legacy.longitude)) = 1"""

ADOPTION_CANDIDATES_SQL = f"""SELECT
    links.address_id AS address_id,
    count() AS legacy_rows,
    uniqExact(legacy.company_id) AS companies
{_SELECTION_SQL}"""

# Identities the rule REFUSES, reported beside the adoption count so the number is
# explainable rather than merely large.
ADOPTION_DISAGREEMENT_SQL = f"""SELECT
    count()
FROM (
    SELECT links.address_id
    {_SELECTION_SQL.replace("HAVING uniqExact(tuple(legacy.latitude, legacy.longitude)) = 1",
                            "HAVING uniqExact(tuple(legacy.latitude, legacy.longitude)) > 1")}
)"""

ADOPTION_INSERT_SQL = f"""INSERT INTO {
    QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE
} ({", ".join(STORE_COLUMNS)})
SELECT
    links.address_id AS address_id,
    '{LEGACY_ADOPTED_POLICY_VERSION}' AS policy_version,
    ifNull(any(legacy.source_md5), '') AS reference_md5,
    any(links.address_identity_run_id) AS address_identity_run_id,
    any(legacy.normalized_match_key) AS normalized_match_key,
    'matched_exact' AS match_status,
    toUInt16(any(legacy.candidate_count)) AS candidate_count,
    any(legacy.candidate_record_ids) AS candidate_record_ids,
    any(legacy.candidate_record_urls) AS candidate_record_urls,
    '{LEGACY_ADOPTED_MATCH_METHOD}' AS match_method,
    toFloat32(any(legacy.match_confidence)) AS match_confidence,
    any(legacy.latitude) AS latitude,
    any(legacy.longitude) AS longitude,
    any(legacy.geocode_provider) AS geocode_provider,
    any(legacy.geocode_precision) AS geocode_precision,
    any(legacy.coordinate_method) AS coordinate_method,
    any(legacy.coordinate_locality) AS coordinate_locality,
    toUInt32(any(legacy.coordinate_supporting_point_count))
        AS coordinate_supporting_point_count,
    any(legacy.coordinate_spread_meters) AS coordinate_spread_meters,
    any(legacy.source_record_id) AS source_record_id,
    any(legacy.source_record_url) AS source_record_url,
    any(legacy.source_url) AS source_url,
    any(legacy.source_object_key) AS source_object_key,
    any(legacy.source_md5) AS source_md5,
    any(legacy.source_snapshot_at) AS source_snapshot_at,
    any(legacy.source_retrieved_at) AS source_retrieved_at,
    %(geocode_run_id)s AS geocode_run_id,
    %(imported_at)s AS matched_at
{_SELECTION_SQL}"""

ADOPTION_SAMPLE_SQL = f"""SELECT
    toString(store.address_id),
    store.match_status,
    store.match_method,
    store.latitude,
    store.longitude,
    store.geocode_precision,
    toString(store.reference_md5)
FROM {QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE} AS store
WHERE store.policy_version = '{LEGACY_ADOPTED_POLICY_VERSION}'
ORDER BY store.address_id
LIMIT %(sample_size)s"""
```

`legacy.geocode_precision` for a `matched_exact` legacy row is `'building'`, which is what
`_assert_promoted_geocode_invariants`'s status-precision rule requires and what check 3
(Task 7) re-asserts on the store — the import inherits the constraint rather than restating
it, and the harness scenario proves it holds.

- [ ] **Step 4: Add the asset and its job**

In `address_geocoding_assets.py`:

```python
LEGACY_ADOPTION_ASSET_KEY = "sweden_address_geocode_legacy_adoption_clickhouse"


class SwedenGeocodeLegacyAdoptionConfig(dg.Config):
    """One shot, owner-gated. A bare Materialize click measures and writes nothing."""

    execute: bool = False
    sample_size: int = 20


@dg.asset(
    name=LEGACY_ADOPTION_ASSET_KEY,
    deps=[
        dg.AssetKey(GEOCODE_STORE_ASSET_KEY),
        dg.AssetKey(GEOCODE_RESULT_ASSET_KEY),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse"},
    metadata={"table": geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE},
    description=(
        "One-time import of the retired per-company matcher's exact decisions for "
        "Sweden address identities the resolver refuses, as versioned "
        "legacy_adopted_v1 outcomes. Requires execute: true."
    ),
)
def sweden_address_geocode_legacy_adoption_clickhouse(
    context: dg.AssetExecutionContext,
    config: SwedenGeocodeLegacyAdoptionConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    imported_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        [(adoptable, legacy_rows, companies)] = client.execute(
            f"""
            SELECT count(), sum(legacy_rows), sum(companies)
            FROM (
            {geocode_legacy_adoption.ADOPTION_CANDIDATES_SQL}
            )
            """
        )
        [(disagreeing,)] = client.execute(
            geocode_legacy_adoption.ADOPTION_DISAGREEMENT_SQL
        )
        if not config.execute:
            return dg.MaterializeResult(
                metadata={
                    "preview": True,
                    "adoptable_identities": int(adoptable),
                    "contributing_legacy_rows": int(legacy_rows or 0),
                    "contributing_companies": int(companies or 0),
                    "refused_disagreeing_identities": int(disagreeing),
                }
            )
        if int(adoptable) == 0:
            raise ValueError(
                "No Sweden legacy exact decisions are adoptable -- refusing to run an "
                "import that would write nothing"
            )
        client.execute(
            geocode_legacy_adoption.ADOPTION_INSERT_SQL,
            {"geocode_run_id": context.run_id, "imported_at": imported_at},
        )
        [(adopted_rows, adopted_identities)] = client.execute(
            f"""
            SELECT count(), uniqExact(address_id)
            FROM {geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}
            WHERE policy_version = '{geocode_store.LEGACY_ADOPTED_POLICY_VERSION}'
            """
        )
        sample = client.execute(
            geocode_legacy_adoption.ADOPTION_SAMPLE_SQL,
            {"sample_size": config.sample_size},
        )
    if int(adopted_rows) != int(adopted_identities):
        raise ValueError(
            "The adoption import wrote more than one row per address identity"
        )
    return dg.MaterializeResult(
        metadata={
            "preview": False,
            "adoptable_identities": int(adoptable),
            "adopted_identities": int(adopted_identities),
            "contributing_companies": int(companies or 0),
            "refused_disagreeing_identities": int(disagreeing),
            "imported_at": imported_at.isoformat(),
            "sample": dg.MetadataValue.json([list(map(str, row)) for row in sample]),
        }
    )


sweden_address_geocode_legacy_adoption_job = dg.define_asset_job(
    name="sweden_address_geocode_legacy_adoption_job",
    selection=dg.AssetSelection.assets(LEGACY_ADOPTION_ASSET_KEY),
    tags={"country": "SE", "pipeline": "address_geocode_legacy_adoption"},
    description=(
        "One-time, owner-gated import of the legacy per-company matcher's exact "
        "Sweden decisions into the versioned geocode store."
    ),
)
```

Register the asset in `defs.assets` and the job in `defs.jobs`. It goes in **no other job**
and in no schedule — assert that in the job-shape test the way Task 3 asserts it for the
backfill.

- [ ] **Step 5: Run**

```bash
cd corpscout/services/dagster_v3
uv run pytest tests/test_sweden_geocode_legacy_adoption.py tests/test_sweden_company_address_geocoding.py -q
uv run dg check defs && uv run ruff check src/dagster_v3/defs/sweden_company
```

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/geocode_legacy_adoption.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_geocoding_assets.py \
        corpscout/services/dagster_v3/tests/test_sweden_geocode_legacy_adoption.py \
        corpscout/services/dagster_v3/tests/test_sweden_company_address_geocoding.py
git commit -m "feat(sweden_company): import the legacy exact Sweden geocodes as adopted outcomes

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

**STOP.** Phase 9 is the controller's (Task 12e) — the import runs once, with a verified sample, and the measured identity count goes into Task 8's migration comment.

---

### Task 7: The six checks, per spec §4.5's disposition table, and the stats-helper split

Resolves spec §9 open item 3 (whether checks 1–2's DuckDB relocations assert inside the build functions or as separate assets). **Decision: inside the existing build functions.** That is what every other invariant in this subsystem already does — `address_canonicalization._assert_canonical_address_invariants` (`:415-462`), `shared_addresses._assert_shared_address_invariants` (`:287-347`), `shared_address_geocoding`'s now-deleted suite and `address_resolution_promotion._assert_promoted_geocode_invariants` (`:350-416`) are all in-function raises inside the build's transaction, which is strictly stronger than an asset check: they abort before the bad snapshot is committed, instead of reporting on it afterwards. There is no separate-assertion-asset pattern anywhere in `sweden_company/` to follow.

**This task lands BEFORE Task 8** — checks 5 and 6 read the legacy pair through `fetch_sweden_address_geocode_stats`, so the pair cannot be dropped while they still do.

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/address_canonicalization.py` (check 1's terms move into `_assert_canonical_address_invariants`)
- Modify: `src/dagster_v3/defs/sweden_company/shared_addresses.py` (check 2's authority already lives here; add the review-status term)
- Modify: `src/dagster_v3/defs/sweden_company/address_geocoding_assets.py` (narrow check 2's ClickHouse half, rewrite check 3 for the store, delete check 4, repoint checks 5 and 6, split the stats helper)
- Modify: `corpscout/services/dagster_v3/tests/test_sweden_company_address_geocoding.py`
- Test: `corpscout/services/dagster_v3/tests/test_sweden_geocode_checks.py`

**Interfaces (Tasks 8, 9 consume):**
- `fetch_sweden_address_geocode_stats` and `SwedenAddressGeocodeStats` are **deleted**. In their place: `fetch_sweden_geocode_exact_match_stats(client) -> SwedenGeocodeExactMatchStats` and `fetch_sweden_geocode_snapshot_freshness(client) -> datetime | None`.
- `EXACT_MATCH_RATE_SQL`, `SNAPSHOT_FRESHNESS_SQL`, `STORE_INVARIANTS_SQL`, `STORE_COVERAGE_SQL` module constants (so the harness can execute them).
- Checks 3, 5 and 6 hang off `sweden_address_geocode_store_clickhouse`; check 2 stays on `sweden_shared_addresses_clickhouse`; check 1's ClickHouse half stays on `sweden_company_canonical_addresses_clickhouse` until Task 9 deletes both together.

| # | check | after this task |
|---|---|---|
| 1 | `sweden_company_canonical_addresses_complete_check` | terms added to the DuckDB build; the ClickHouse check is untouched here and is deleted with the publish in Task 9 |
| 2 | `sweden_shared_addresses_complete_check` | narrowed to shared-vs-links — the canonical denominator goes, the DuckDB assertion is the authority |
| 3 | `sweden_shared_address_geocodes_complete_check` | rewritten against the store, re-hosted on the store asset |
| 4 | `sweden_shared_address_geocodes_baseline_check` | deleted |
| 5 | `sweden_company_address_exact_match_rate_check` | links denominator, versioned-read numerator, re-hosted on the store asset |
| 6 | `sweden_company_address_osm_snapshot_freshness_check` | reads only the store's `max(source_snapshot_at)`, re-hosted on the store asset |

- [ ] **Step 1: Write the failing tests**

`corpscout/services/dagster_v3/tests/test_sweden_geocode_checks.py`:

```python
"""What each check would have to stop seeing before it stopped failing.

Every SQL constant here is also executed by the harness (Task 11). These tests pin the
predicates, because a check whose predicate silently narrows keeps passing forever.
"""
from dagster_v3.defs.sweden_company.address_geocoding_assets import (
    EXACT_MATCH_RATE_SQL,
    SNAPSHOT_FRESHNESS_SQL,
    STORE_COVERAGE_SQL,
    STORE_INVARIANTS_SQL,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    GEOCODED_STATUSES,
    QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE,
    VALID_STATUSES,
)


def test_the_store_invariants_check_pins_the_new_grain() -> None:
    """Uniqueness is per (identity, matcher, reference) now, not per identity: two rows for
    one identity is the store working, and a check that still demanded one row per identity
    would fail every week from the first policy bump onwards."""
    assert "uniqExact(tuple(address_id, policy_version, reference_md5))" in STORE_INVARIANTS_SQL
    assert f"FROM {QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}" in STORE_INVARIANTS_SQL
    # The full 11-status allowlist, and the two key columns are never empty.
    for status in VALID_STATUSES:
        assert f"'{status}'" in STORE_INVARIANTS_SQL
    assert "reference_md5 = ''" in STORE_INVARIANTS_SQL
    assert "policy_version = ''" in STORE_INVARIANTS_SQL
    # Status/coordinate and status/precision agreement survive the grain change unchanged.
    for status in GEOCODED_STATUSES:
        assert f"'{status}'" in STORE_INVARIANTS_SQL
    assert "geocode_precision != 'building'" in STORE_INVARIANTS_SQL
    assert "isNull(source_md5)" in STORE_INVARIANTS_SQL


def test_the_coverage_check_asks_the_identity_side() -> None:
    """After a demand-driven run every identity must have an outcome -- a new identity is
    matched the week it appears. Anti-joining from the identity table is the only direction
    that can see a MISSING outcome."""
    assert "corpscout.se_addresses_current" in STORE_COVERAGE_SQL
    assert "LEFT ANTI JOIN" in STORE_COVERAGE_SQL


def test_the_exact_match_rate_counts_links_against_the_versioned_read() -> None:
    """Denominator and numerator must share a grain. The store is per identity and the
    denominator is per company-address link, so the rate is computed by joining -- counting
    identities over a link denominator would report a number that means nothing."""
    assert "corpscout.se_company_address_links_current" in EXACT_MATCH_RATE_SQL
    assert "LEFT JOIN" in EXACT_MATCH_RATE_SQL
    # The joined column is non-Nullable in the store, so the miss is read through ifNull --
    # a bare comparison is NULL under join_use_nulls = 1 and '' under 0.
    assert "ifNull(geocode.match_status, '') = 'matched_exact'" in EXACT_MATCH_RATE_SQL
    # No legacy table, no canonical table.
    assert "se_company_address_geocode" not in EXACT_MATCH_RATE_SQL
    assert "canonical" not in EXACT_MATCH_RATE_SQL


def test_the_freshness_query_reads_the_store_and_nothing_else() -> None:
    assert SNAPSHOT_FRESHNESS_SQL.strip().startswith("SELECT max(source_snapshot_at)")
    assert QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE in SNAPSHOT_FRESHNESS_SQL
    assert "canonical" not in SNAPSHOT_FRESHNESS_SQL
    assert "se_company_address_geocodes" not in SNAPSHOT_FRESHNESS_SQL


def test_the_baseline_check_is_gone() -> None:
    """It joined canonical to the legacy pair on a key that exists only on canonical, and
    its purpose -- parity between the two matchers -- ended when one of them did."""
    from dagster_v3.defs.sweden_company import address_geocoding_assets

    assert not hasattr(address_geocoding_assets, "sweden_shared_address_geocodes_baseline_check")
    assert not hasattr(address_geocoding_assets, "fetch_sweden_address_geocode_stats")
    assert not hasattr(address_geocoding_assets, "SwedenAddressGeocodeStats")
```

And, in `tests/test_sweden_company_address_geocoding.py`, add the DuckDB relocation tests
beside the existing canonical/shared scenario tests — after the canonical build in
`test_shared_address_link_collapses_canonical_care_of_variants` (`:854`), assert the new
terms raise on a mutated snapshot:

```python
def test_the_canonical_build_refuses_a_member_total_that_disagrees(connection) -> None:
    """Check 1's ClickHouse half compared member_count sums across two published tables.
    Asserting it in the build instead aborts before the bad snapshot is ever published --
    and it is the same arithmetic, on the same rows, one step earlier."""
    from dagster_v3.defs.sweden_company.address_canonicalization import (
        _assert_canonical_address_invariants,
    )

    connection.execute(
        "update sweden_company_enrichment.se_company_addresses_canonical_current "
        "set member_count = member_count + 1")
    with pytest.raises(ValueError, match="member"):
        _assert_canonical_address_invariants(connection)
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/test_sweden_geocode_checks.py -q` → `ImportError` on `EXACT_MATCH_RATE_SQL`

- [ ] **Step 3: Relocate check 1's terms into the canonical build**

In `address_canonicalization.py`, `_assert_canonical_address_invariants` (`:415-462`) already
covers "every observation maps to exactly one canonical address" and "canonical keys are
unique". Add the three terms check 1 held that it does not: the member-count total, the
canonical ≤ members bound, and the single-normalization-run identity across both tables.
Extend the second query's projection (`:430-451`):

```sql
            select
                count(*),
                count(distinct (company_id, canonical_address_key)),
                count(*) filter (where country_count > 1),
                sum(member_total),
                count(distinct normalization_run_id)
            from (
                select
                    canonical.company_id,
                    canonical.canonical_address_key,
                    canonical.normalization_run_id,
                    any_value(canonical.member_count) as member_total,
                    count(distinct members.country_code) as country_count
                from {QUALIFIED_CANONICAL_ADDRESSES_TABLE} canonical
                join {QUALIFIED_ADDRESS_MEMBERS_TABLE} members using (
                    company_id,
                    canonical_address_key
                )
                group by
                    canonical.company_id,
                    canonical.canonical_address_key,
                    canonical.normalization_run_id
            ) groups
```

and add, beside the existing raises:

```python
    if int(declared_member_total) != int(member_rows):
        raise ValueError(
            "Canonical Sweden member_count totals must equal the member rows they "
            "summarise"
        )
    if int(canonical_rows) > int(member_rows):
        raise ValueError(
            "Sweden canonical addresses cannot outnumber the members they group"
        )
    if int(canonical_normalization_runs) != 1:
        raise ValueError("Canonical Sweden addresses must belong to one normalization run")
```

The members table's own single-run term goes in the first query as
`count(distinct normalization_run_id)` over `QUALIFIED_ADDRESS_MEMBERS_TABLE`, with a raise
that names it, so the "same id on both" half of check 1 is covered by two single-run
assertions plus the join above — which is what "same id on both" means when the join is on
the key.

- [ ] **Step 4: Narrow check 2 and add its review-status term to the DuckDB build**

In `shared_addresses.py`, `_assert_shared_address_invariants` (`:287-347`) already asserts
every count term check 2 held except the review-status allowlist. Add it to the links query
(`:310-317`) as `count(*) filter (where review_status not in ('unreviewed', 'confirmed', 'rejected'))`
and raise `"Sweden company-address link review statuses must be known values"`.

In `address_geocoding_assets.py`, `sweden_shared_addresses_complete_check` (`:755-845`)
loses its first query entirely — the canonical read at `:767-781` — and with it the
`expected_link_rows` and `canonical_evidence` terms. What remains is shared-vs-links:

```python
    passed = (
        int(link_rows) == int(unique_link_rows)
        and int(address_rows) == int(unique_address_rows)
        and int(link_evidence) == int(address_evidence)
        and int(link_rows) == int(address_company_total)
        and int(link_runs) == int(address_runs) == 1
        and int(invalid_review_statuses) == 0
    )
```

with the canonical keys dropped from the metadata dict and the docstring saying where the
canonical half went:

```python
    """Fails when the published shared-address graph disagrees with its own links.

    The canonical denominator this check used to carry is asserted by
    shared_addresses._assert_shared_address_invariants inside the DuckDB build, which aborts
    before publishing rather than reporting afterwards -- and the canonical ClickHouse table
    it read is retiring (spec section 4.5).
    """
```

- [ ] **Step 5: Rewrite check 3 for the store's grain**

Replace `sweden_shared_address_geocodes_complete_check` (`:848-999`) — same invariants, new
grain, new host asset:

```python
STORE_INVARIANTS_SQL = f"""SELECT
    count(),
    uniqExact(tuple(address_id, policy_version, reference_md5)),
    uniqExact(address_id),
    countIf(match_status NOT IN ({", ".join(f"'{s}'" for s in geocode_store.VALID_STATUSES)})),
    countIf(reference_md5 = '' OR policy_version = ''),
    countIf(
        isNull(latitude) != isNull(longitude)
        OR (isNotNull(latitude) AND (latitude < -90 OR latitude > 90))
        OR (isNotNull(longitude) AND (longitude < -180 OR longitude > 180))
    ),
    countIf(
        match_status IN ({", ".join(f"'{s}'" for s in geocode_store.GEOCODED_STATUSES)})
        AND (isNull(latitude) OR isNull(longitude))
    ),
    countIf(
        match_status NOT IN ({", ".join(f"'{s}'" for s in geocode_store.GEOCODED_STATUSES)})
        AND (isNotNull(latitude) OR isNotNull(longitude))
    ),
    countIf(
        match_status IN ('matched_exact', 'matched_corrected')
            AND geocode_precision != 'building'
        OR match_status = 'matched_site' AND geocode_precision != 'site'
        OR match_status = 'matched_area' AND geocode_precision != 'area'
        OR match_status = 'matched_street' AND geocode_precision != 'street'
        OR match_status NOT IN ({", ".join(f"'{s}'" for s in geocode_store.GEOCODED_STATUSES)})
            AND geocode_precision != ''
    ),
    countIf(
        isNull(source_url)
        OR isNull(source_object_key)
        OR isNull(source_md5)
        OR isNull(source_snapshot_at)
        OR isNull(source_retrieved_at)
    )
FROM {geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}"""

STORE_COVERAGE_SQL = f"""SELECT count()
FROM {shared_addresses.QUALIFIED_CLICKHOUSE_SHARED_ADDRESSES_TABLE} AS address
LEFT ANTI JOIN {
    geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE
} AS store ON store.address_id = address.address_id"""


@dg.asset_check(
    asset=sweden_address_geocode_store_clickhouse,
    name="every_stored_outcome_is_attributable_and_consistent",
    description=(
        "Fails when the versioned Sweden geocode store holds a duplicate "
        "(identity, policy, reference) row, an unknown status, an outcome with no "
        "version identity, or a coordinate that disagrees with its status."
    ),
)
def sweden_address_geocode_store_complete_check(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    """Check 3, at the store's grain.

    The old check demanded ONE row per identity and ONE geocode run across the whole table.
    Both are now wrong by design: several attributable outcomes per identity is the store
    working, and a demand-driven run appends only what it matched, so a table spanning many
    runs is the normal state. What survives unchanged is everything about a single row --
    the status allowlist, the coordinate/precision agreement, the provenance -- plus the two
    new key columns, which must never be empty because an outcome that cannot be attributed
    is exactly what this design set out to eliminate.
    """
    with clickhouse.get_connection() as client:
        [
            (
                rows,
                unique_keys,
                identities,
                invalid_statuses,
                missing_versions,
                invalid_coordinates,
                missing_geocoded_coordinates,
                unexpected_coordinates,
                invalid_precision,
                missing_provenance,
            )
        ] = client.execute(STORE_INVARIANTS_SQL)
        [(identities_without_outcome,)] = client.execute(STORE_COVERAGE_SQL)
    passed = (
        int(rows) == int(unique_keys)
        and int(rows) >= int(identities)
        and int(identities_without_outcome) == 0
        and int(invalid_statuses) == 0
        and int(missing_versions) == 0
        and int(invalid_coordinates) == 0
        and int(missing_geocoded_coordinates) == 0
        and int(unexpected_coordinates) == 0
        and int(invalid_precision) == 0
        and int(missing_provenance) == 0
    )
    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "store_rows": int(rows),
            "unique_version_keys": int(unique_keys),
            "identities": int(identities),
            "identities_without_outcome": int(identities_without_outcome),
            "invalid_statuses": int(invalid_statuses),
            "outcomes_missing_a_version_identity": int(missing_versions),
            "invalid_coordinates": int(invalid_coordinates),
            "missing_geocoded_coordinates": int(missing_geocoded_coordinates),
            "unexpected_coordinates": int(unexpected_coordinates),
            "invalid_precision": int(invalid_precision),
            "missing_snapshot_provenance": int(missing_provenance),
        },
    )
```

`rows == unique_keys` is the ReplacingMergeTree contract stated as an assertion: duplicate
key triples mean the engine has un-merged parts the versioned read has to rank around, which
is safe but is worth knowing about; a persistent inequality means the append path is writing
the same key twice in one run, which is a bug.

- [ ] **Step 6: Delete check 4, split the stats helper, repoint checks 5 and 6**

Delete `sweden_shared_address_geocodes_baseline_check` (`:1002-1074`) and its entry in
`defs.asset_checks`. Delete `SwedenAddressGeocodeStats` (`:41-53`),
`fetch_sweden_address_geocode_stats` (`:56-86`) and `fetch_sweden_address_geocode_result_counts`
(`:89-98` — its only caller is the legacy results publish asset, which Task 8 deletes;
delete it here together with that asset's `**status_counts` metadata spread, leaving `rows`
and `table`). In `sweden_company_address_geocodes_clickhouse` (`:538-576`) delete the
`stats = fetch_sweden_address_geocode_stats(clickhouse_client)` call and the four metadata
keys it fed — that asset is deleted whole in Task 8 and only has to keep compiling until
then. Then:

```python
@dataclass(frozen=True)
class SwedenGeocodeExactMatchStats:
    company_address_links: int
    matched_exact_links: int
    geocoded_links: int

    @property
    def exact_match_rate_percent(self) -> float:
        if self.company_address_links == 0:
            return 0.0
        return 100.0 * self.matched_exact_links / self.company_address_links


EXACT_MATCH_RATE_SQL = f"""SELECT
    count(),
    countIf(ifNull(geocode.match_status, '') = 'matched_exact'),
    countIf(ifNull(geocode.match_status, '') IN (
        {", ".join(f"'{s}'" for s in geocode_store.GEOCODED_STATUSES)}
    ))
FROM {
    shared_addresses.QUALIFIED_CLICKHOUSE_COMPANY_ADDRESS_LINKS_TABLE
} AS link
LEFT JOIN (
{geocode_store.build_current_geocodes_sql(columns=("address_id", "match_status"))}
) AS geocode ON geocode.address_id = link.address_id"""

SNAPSHOT_FRESHNESS_SQL = f"""SELECT max(source_snapshot_at)
FROM {geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}"""


def fetch_sweden_geocode_exact_match_stats(client: Any) -> SwedenGeocodeExactMatchStats:
    """Company-address links, and how many of them the store currently geocodes.

    The denominator is LINKS, not canonical addresses: the canonical ClickHouse table is
    retiring, and a link is the thing a rate over company addresses is actually about. The
    numerator joins the versioned read, so the two share a grain -- counting identities
    against a link denominator would produce a number that means nothing.
    """
    [(links, matched_exact, geocoded)] = client.execute(EXACT_MATCH_RATE_SQL)
    return SwedenGeocodeExactMatchStats(
        company_address_links=int(links),
        matched_exact_links=int(matched_exact),
        geocoded_links=int(geocoded),
    )


def fetch_sweden_geocode_snapshot_freshness(client: Any) -> datetime | None:
    """The newest OSM snapshot any stored outcome was computed against."""
    [(snapshot_at,)] = client.execute(SNAPSHOT_FRESHNESS_SQL)
    return snapshot_at
```

Checks 5 and 6 then move onto the store asset and drop every term that referred to the
legacy pair:

```python
@dg.asset_check(
    asset=sweden_address_geocode_store_clickhouse,
    name="exact_match_rate_stable",
    description=(
        "Warns when exact OSM coverage of Sweden company-address links drops below "
        "the operational floor or moves by more than two percentage points from the "
        "previous materialization."
    ),
)
def sweden_company_address_exact_match_rate_check(
    context: dg.AssetCheckExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    with clickhouse.get_connection() as client:
        stats = fetch_sweden_geocode_exact_match_stats(client)
    previous_percent = previous_exact_match_rate_percent(
        context.instance,
        current_run_id=context.run.run_id,
    )
    current_percent = stats.exact_match_rate_percent
    change = (
        current_percent - previous_percent if previous_percent is not None else None
    )
    return dg.AssetCheckResult(
        passed=exact_match_rate_is_stable(
            current_percent=current_percent,
            previous_percent=previous_percent,
        ),
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "company_address_links": stats.company_address_links,
            "matched_exact_links": stats.matched_exact_links,
            "geocoded_links": stats.geocoded_links,
            "exact_match_rate_percent": current_percent,
            "previous_exact_match_rate_percent": previous_percent,
            "change_percentage_points": change,
            "minimum_match_rate_percent": MIN_EXACT_MATCH_RATE_PERCENT,
            "maximum_change_percentage_points": (
                MAX_EXACT_MATCH_RATE_CHANGE_PERCENTAGE_POINTS
            ),
        },
    )


@dg.asset_check(
    asset=sweden_address_geocode_store_clickhouse,
    name="osm_snapshot_fresh",
    description="Warns when stored Sweden coordinates come from an OSM snapshot over nine days old.",
)
def sweden_company_address_osm_snapshot_freshness_check(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    checked_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        snapshot_at = fetch_sweden_geocode_snapshot_freshness(client)
    snapshot_age_hours = (
        (checked_at - snapshot_at.astimezone(UTC)).total_seconds() / 3600
        if snapshot_at is not None and snapshot_at.tzinfo is not None
        else None
    )
    return dg.AssetCheckResult(
        passed=osm_snapshot_is_fresh(snapshot_at=snapshot_at, now=checked_at),
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "latest_osm_snapshot_at": (
                snapshot_at.isoformat() if snapshot_at is not None else None
            ),
            "snapshot_age_hours": snapshot_age_hours,
            "maximum_snapshot_age_hours": MAX_OSM_SNAPSHOT_AGE.total_seconds() / 3600,
        },
    )
```

`previous_exact_match_rate_percent` (`:127-146`) reads materializations of
`GEOCODE_ASSET_KEY` — repoint its `dg.AssetKey(GEOCODE_ASSET_KEY)` to
`dg.AssetKey(GEOCODE_STORE_ASSET_KEY)`. **The metric history does not carry over**: the
first run after the deploy sees `previous_percent = None` and the ±2pp term is inert for one
week. Say so in the deploy report rather than letting someone read the missing comparison as
a bug. The rate itself also steps — the denominator moves from canonical company addresses
to company-address links — so week one's absolute number is not comparable with the old
series either.

- [ ] **Step 7: Run**

```bash
cd corpscout/services/dagster_v3
uv run pytest tests/test_sweden_geocode_checks.py tests/test_sweden_company_address_geocoding.py \
              tests/test_sweden_geocode_store.py -q
uv run dg check defs && uv run ruff check src/dagster_v3/defs/sweden_company
rg -n "fetch_sweden_address_geocode_stats|SwedenAddressGeocodeStats" \
   /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout --glob '!*.pyc'
```

Expected: green; the final `rg` returns nothing but this plan and the spec.

- [ ] **Step 8: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_canonicalization.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/shared_addresses.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_geocoding_assets.py \
        corpscout/services/dagster_v3/tests/test_sweden_geocode_checks.py \
        corpscout/services/dagster_v3/tests/test_sweden_company_address_geocoding.py
git commit -m "refactor(sweden_company): move the Sweden address checks onto the geocode store

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: Retire the legacy per-company matcher and its pair (code, then migration 000317)

Spec §4.4's tail and §6 step 3. **Do not start this task before Task 12e has closed** — the adoption import must have run and been verified, because the rows it reads are the ones this task drops. Its measured identity count goes into the migration's comment.

**Files:**
- Delete: `src/dagster_v3/defs/sweden_company/address_geocoding.py`
- Delete: `src/dagster_v3/defs/sweden_company/geocode_legacy_adoption.py`
- Modify: `src/dagster_v3/defs/sweden_company/address_geocoding_assets.py` (three assets, one check, one config, one job, the imports and the job selections)
- Modify: `src/dagster_v3/defs/common/clickhouse_checks.py` (two freshness leaves go; one for the store arrives)
- Modify: `corpscout/services/dagster_v3/tests/test_sweden_company_address_geocoding.py`, `tests/test_se_company_address.py` (leaf assertions)
- Create: `corpscout/clickhouse/migrations/000317_corpscout_retire_se_company_address_geocode_pair.up.sql`, `.down.sql`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py`

**Interfaces:** consumes Task 6's measured counts (from the 12e report). Produces two fewer ClickHouse tables, three fewer assets, one fewer asset check.

- [ ] **Step 1: Re-verify zero readers, freshly, including the constant indirection**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg -n "se_company_address_geocodes|se_company_address_geocode_results" corpscout --glob '!*.pyc'
rg -n "address_geocoding\.(QUALIFIED_)?CLICKHOUSE" corpscout --glob '!*.pyc'
rg -n "from dagster_v3.defs.sweden_company import .*address_geocoding\b|sweden_company\.address_geocoding" corpscout --glob '!*.pyc'
```

Known hits as of 2026-08-24 — **each must be gone or accounted for before the drop**:

| hit | action |
|---|---|
| `clickhouse/migrations/000270, 000271, 000272, 000277, 000314` (up/down) | historical DDL — leave, they are the ledger |
| `defs/sweden_company/address_geocoding.py` | the module itself — deleted |
| `defs/sweden_company/address_geocoding_assets.py` | three assets + one check + the import — deleted (Step 2) |
| `defs/sweden_company/geocode_legacy_adoption.py` | reads the results table — deleted (Step 3) |
| **`defs/common/clickhouse_checks.py:206-215`** | **two freshness leaves, reached by asset key rather than by any `sweden_company` import — this is the constant-indirection class of hit the address plan's Task 9 was burned by. Remove both (Step 4)** |
| `services/backoffice/tests/company-serving-sections.test.ts:63,68` | `not.toContain` assertions — they assert the public page does NOT read these tables, so they stay and keep passing |
| `tests/test_sweden_company_address_geocoding.py`, `tests/test_clickhouse_migrations.py`, `tests/test_se_company_address.py` | updated with the code |
| `defs/sweden_address_geocoding/assets.py`, `tests/test_sweden_address_geocoding.py` | **false positives** — that is the Lantmäteriet module, matched on the path fragment `address_geocoding`. Nothing there touches these tables. Leave. |

If the `rg` shows a hit not in this table, **stop and report**.

- [ ] **Step 2: Delete the legacy matcher and its publish assets**

`git rm` `src/dagster_v3/defs/sweden_company/address_geocoding.py`. In
`address_geocoding_assets.py` delete: the `address_geocoding` import (`:17`), the
`GEOCODE_ASSET_KEY` and `GEOCODE_RESULT_ASSET_KEY` constants (`:23-24`),
`sweden_company_address_osm_matches_duckdb` (`:489-524`),
`sweden_company_address_geocodes_clickhouse` (`:527-576`),
`sweden_company_address_geocode_results_clickhouse` (`:579-624`) and
`sweden_company_address_geocode_results_complete_check` (`:627-665` — its canonical read
retires with it; the store's own completeness is check 3, rewritten in Task 7). Remove all
four names from `defs.assets` / `defs.asset_checks`, and remove
`"sweden_company_address_osm_matches_duckdb"`, `GEOCODE_ASSET_KEY` and
`GEOCODE_RESULT_ASSET_KEY` from `sweden_company_address_geocoding_job` (`:1155`) and
`sweden_company_address_geocoding_weekly_job` (`:1202`).

The weekly job's asset list goes from 15 to 12: `sweden_osm_pbf_s3`,
`sweden_osm_addresses_duckdb`, canonical DuckDB, canonical ClickHouse (which Task 9 narrows
to members but does NOT remove — it keeps its asset key), shared addresses DuckDB, shared
addresses ClickHouse, **demand**, golden, shadow, promotion, **store append**, derived
`_current`. Update the job-shape test's four expected sets accordingly.

- [ ] **Step 3: Delete the adoption import**

`git rm` `src/dagster_v3/defs/sweden_company/geocode_legacy_adoption.py`, delete
`sweden_address_geocode_legacy_adoption_clickhouse`, `SwedenGeocodeLegacyAdoptionConfig`,
`LEGACY_ADOPTION_ASSET_KEY` and `sweden_address_geocode_legacy_adoption_job` from
`address_geocoding_assets.py` and from `defs`, and `git rm`
`tests/test_sweden_geocode_legacy_adoption.py`.

It is a one-shot that has run; its input tables are being dropped in this same commit, so
keeping it would leave an asset that fails the moment anyone clicks Materialize. **The
record it leaves behind is the migration comment below plus its own commit** — which is
where a reader looks for "where did these 19-odd-thousand `legacy_adopted_v1` rows come
from" anyway, because that is the question the ledger answers.

- [ ] **Step 4: Fix the freshness leaves**

In `defs/common/clickhouse_checks.py`, delete the two `ClickhouseLeaf` entries at `:206-215`
and add one for the store in their place:

```python
    ClickhouseLeaf(
        "sweden_address_geocode_store_clickhouse",
        ("se_address_geocodes",),
        WEEKLY,
    ),
```

Update `tests/test_se_company_address.py`'s leaf assertions (`:590-600`) with the same
change, adding:

```python
    assert leaves["sweden_address_geocode_store_clickhouse"].tables == ("se_address_geocodes",)
    assert "sweden_company_address_geocodes_clickhouse" not in leaves
    assert "sweden_company_address_geocode_results_clickhouse" not in leaves
```

- [ ] **Step 5: Delete the legacy matcher's tests**

In `tests/test_sweden_company_address_geocoding.py`, the six scenario tests at `:911`,
`:1255`, `:1398`, `:1553`, `:1708`, `:1841` and `:1933` exist to exercise
`address_geocoding.replace_sweden_company_address_osm_matches` against a hand-built OSM
fixture. After Task 4 stripped their shared-matcher halves, what remains in each is the
legacy matcher's own ladder. Delete the six tests whose only remaining subject is that
matcher; keep `test_shared_address_link_collapses_canonical_care_of_variants` (`:854`) and
the canonical/members/links assertions that Task 4 preserved inside
`test_sweden_company_address_matching_only_accepts_unique_exact_osm_rows` (`:911`) —
rename that one to `test_the_canonical_and_shared_address_chain_is_built_from_source_observations`
and delete everything in it below the links assertions.

Same reasoning as Task 4 Step 8: these tests pin a matcher's ladder, and the matcher is
gone. The resolver's ladder is pinned in `tests/test_address_resolution.py` and the store's
invariants in `tests/test_sweden_geocode_checks.py`.

- [ ] **Step 6: Snapshot the row counts (controller runs these — paste the output into the migration comment)**

```sql
SELECT 'legacy_geocodes', count() FROM corpscout.se_company_address_geocodes
UNION ALL SELECT 'legacy_results', count() FROM corpscout.se_company_address_geocode_results
UNION ALL SELECT 'store', count() FROM corpscout.se_address_geocodes
UNION ALL SELECT 'adopted', count() FROM corpscout.se_address_geocodes
    WHERE policy_version = 'legacy_adopted_v1';
```

- [ ] **Step 7: Write migration 000317**

Re-check the next free number first (`ls corpscout/clickhouse/migrations | tail -1`).

`000317_corpscout_retire_se_company_address_geocode_pair.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- RETIREMENT -- the Sweden legacy per-company address geocoder's output pair.
--
-- These two tables were the parity baseline for a second matcher that ran every week
-- alongside the resolver, keyed per company rather than per address identity. The matcher
-- is deleted in the same commit as this migration (defs/sweden_company/address_geocoding.py
-- and its three assets), so nothing writes them either.
--
-- Gate 0 -- the decisions worth keeping were imported FIRST. The resolver refused
-- <N_COMPANIES> companies' addresses as ambiguous that this matcher resolved matched_exact
-- at confidence 1.0 on identical street text. Those became <N_IDENTITIES> versioned
-- legacy_adopted_v1 rows in corpscout.se_address_geocodes on <DATE>, adopted only where
-- every contributing company row agreed on the coordinate -- <N_REFUSED> identities were
-- refused for disagreeing. geocode_store's read rule serves an adopted row only while the
-- identity's resolver outcome is non-geocoded, so a future resolver improvement outranks it
-- without anything being deleted or merged.
--
-- Gate 1 -- zero readers, re-verified immediately before writing this file with
--   rg -n "se_company_address_geocodes|se_company_address_geocode_results" corpscout --glob '!*.pyc'
--   rg -n "address_geocoding\.(QUALIFIED_)?CLICKHOUSE" corpscout --glob '!*.pyc'
-- The second grep is the one that matters -- every ClickHouse read of these tables went
-- through address_geocoding.QUALIFIED_CLICKHOUSE_TABLE and
-- address_geocoding.QUALIFIED_CLICKHOUSE_RESULTS_TABLE, which a grep for the table names
-- alone does not see. A third indirection was found and removed in the same commit:
-- defs/common/clickhouse_checks.py registered freshness leaves for both publish assets by
-- ASSET KEY, naming the tables in a list far from any sweden_company import.
-- Everything else is migrations 000270, 000271, 000272, 000277 and 000314 (historical DDL,
-- the ledger), two backoffice not.toContain assertions that assert the public page does NOT
-- read these tables, and the Lantmäteriet module, which matches only on the path fragment
-- "address_geocoding" and touches neither table.
--
-- Gate 2 -- row counts at drop time (SELECT run on the host, <DATE>):
--   se_company_address_geocodes         <N> rows
--   se_company_address_geocode_results  <N> rows
--   se_address_geocodes                 <N> rows
--   ... of which legacy_adopted_v1      <N> rows
--
-- NOT dropped, deliberately: corpscout.se_addresses_current,
-- se_company_address_links_current, se_company_address_members_current and
-- se_address_geocodes_current all stay. The first three are the identity chain, which keeps
-- its weekly whole rebuild by design. se_address_geocodes_current is now DERIVED from
-- se_address_geocodes by the versioned read and still has readers -- the backoffice public
-- page section server among them -- so it retires in its own change, behind its own gate.
--
-- UNDROP window is about 480 seconds: if this turns out to be wrong, UNDROP TABLE within it.

DROP TABLE IF EXISTS corpscout.se_company_address_geocode_results;

DROP TABLE IF EXISTS corpscout.se_company_address_geocodes;
```

`.down.sql` recreates both **empty**, copying the `CREATE TABLE` statements verbatim from
000270 and 000271 plus 000272's and 000277's `ALTER`s (with `IF NOT EXISTS`), under a
leading comment: *"Structure only — the rows are not restored, and no asset writes them any
more. Restoring them means reverting the code change that deleted
defs/sweden_company/address_geocoding.py and its three assets, then re-materializing the
weekly geocoding job."*

- [ ] **Step 8: Register and run**

Append `"000317_corpscout_retire_se_company_address_geocode_pair",` to `EXPECTED_MIGRATIONS`.

```bash
cd corpscout/services/dagster_v3
uv run pytest tests/test_clickhouse_migrations.py tests/test_sweden_company_address_geocoding.py \
              tests/test_se_company_address.py tests/test_clickhouse_leaf_checks.py \
              tests/test_sweden_geocode_checks.py -q
uv run dg check defs && uv run ruff check src/dagster_v3/defs
cd ../backoffice && npx vitest run tests/company-serving-sections.test.ts
```

- [ ] **Step 9: Commit** (the migration is applied by the controller in Task 12f, not here)

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/clickhouse/migrations/000317_corpscout_retire_se_company_address_geocode_pair.up.sql \
        corpscout/clickhouse/migrations/000317_corpscout_retire_se_company_address_geocode_pair.down.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_geocoding_assets.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/common/clickhouse_checks.py \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py \
        corpscout/services/dagster_v3/tests/test_sweden_company_address_geocoding.py \
        corpscout/services/dagster_v3/tests/test_se_company_address.py
git rm corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_geocoding.py \
       corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/geocode_legacy_adoption.py \
       corpscout/services/dagster_v3/tests/test_sweden_geocode_legacy_adoption.py
git commit -m "chore(sweden_company): retire the legacy Sweden per-company address geocoder

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 9: Retire the canonical ClickHouse publish (code, then migration 000318)

Spec §4.5's tail and §6 step 5. This is the "dedicated parity-baseline cleanup" that migration `000314`'s comment deferred this table to: *"Retiring the canonical table therefore belongs to the dedicated parity-baseline cleanup, where the cost of those two checks is weighed on purpose (controller ruling A19, 2026-08-24)."* The six readers 000314 enumerated are exactly the six this plan has been dismantling.

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/address_geocoding_assets.py` (the publish becomes members-only; check 1's ClickHouse half goes)
- Modify: `src/dagster_v3/defs/sweden_company/address_canonicalization.py` (delete the two now-unused ClickHouse constants)
- Create: `corpscout/clickhouse/migrations/000318_corpscout_retire_se_company_addresses_canonical.up.sql`, `.down.sql`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py`, `tests/test_sweden_company_address_geocoding.py`

**Interfaces:** `sweden_company_canonical_addresses_clickhouse` **keeps its asset key and its place in the graph** — it publishes members only. Renaming it would churn the weekly job, the freshness leaves and three tests for no gain, and members is what the downstream `se_company_address` final joins through. `address_canonicalization.QUALIFIED_CLICKHOUSE_CANONICAL_ADDRESSES_TABLE` and `CANONICAL_ADDRESS_COLUMNS` are deleted; `QUALIFIED_CANONICAL_ADDRESSES_TABLE` (the DuckDB one) stays, because the DuckDB canonical build stays — the members bridge derives from it (spec §3).

- [ ] **Step 1: Re-verify zero readers, freshly, including the constant indirection**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg -n "se_company_addresses_canonical_current" corpscout --glob '!*.pyc'
rg -n "QUALIFIED_CLICKHOUSE_CANONICAL_ADDRESSES_TABLE|CANONICAL_ADDRESS_COLUMNS" corpscout --glob '!*.pyc'
```

Expected after Tasks 7 and 8, and each accounted for:

| hit | action |
|---|---|
| `clickhouse/migrations/000273, 000278, 000314` (up/down) | historical DDL — leave |
| `address_canonicalization.py:14,16-17,22-23,57,258,401` | `CANONICAL_ADDRESSES_TABLE` and `QUALIFIED_CANONICAL_ADDRESSES_TABLE` are the **DuckDB** build, which stays. Delete only `QUALIFIED_CLICKHOUSE_CANONICAL_ADDRESSES_TABLE` (`:22-23`) and `CANONICAL_ADDRESS_COLUMNS` (`:57`) |
| `address_geocoding_assets.py:196, 216, 229-230, 242, 246` | the publish — narrowed to members (Step 2) |
| `address_geocoding_assets.py:725` | check 1's ClickHouse half — deleted (Step 3) |
| `services/backoffice/tests/company-serving-sections.test.ts:65` | a `not.toContain` assertion — it asserts the public page does NOT read this table. Stays, keeps passing |
| `tests/test_sweden_company_address_geocoding.py:2205-2226, 2302-2326` | migration-shape tests for 000273 and 000278 — historical DDL assertions, they stay |
| `tests/test_se_company_address_clickhouse_local.py:124-126` | a comment explaining why the harness does not replay this table's DDL. Update the comment's tense, nothing else |

The three sites 000314's comment called unrecoverable — `fetch_sweden_address_geocode_stats`,
`all_company_addresses_link_to_one_shared_address` and `shared_geocoding_matches_company_baseline`
— are gone: the first was split (Task 7 Step 6), the second narrowed onto links (Task 7
Step 4) with its canonical arithmetic moved into the DuckDB build where the canonical data
still lives, and the third deleted with the parity baseline it compared (Task 7 Step 6).
**If the `rg` shows a hit not in this table, stop and report.**

- [ ] **Step 2: Narrow the publish to members**

In `address_geocoding_assets.py`, `sweden_company_canonical_addresses_clickhouse`
(`:189-252`):

```python
@dg.asset(
    deps=[dg.AssetKey(CANONICAL_DUCKDB_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "bolagsverket", "scb"},
    pool=osm_tables.DUCKDB_POOL,
    metadata={
        "member_table": (
            address_canonicalization.QUALIFIED_CLICKHOUSE_ADDRESS_MEMBERS_TABLE
        ),
    },
    description=(
        "Publishes the complete source-observation membership of each canonical "
        "Sweden company address to ClickHouse. The canonical addresses themselves "
        "stay in DuckDB -- the members bridge is what downstream readers join."
    ),
)
def sweden_company_canonical_addresses_clickhouse(
    context: dg.AssetExecutionContext,
    sweden_address_osm_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=address_canonicalization.CLICKHOUSE_DATABASE,
        tables=(address_canonicalization.ADDRESS_MEMBERS_TABLE,),
    )
    with sweden_address_osm_duckdb.get_connection() as connection:
        with clickhouse.get_connection() as clickhouse_client:
            rows = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=connection,
                clickhouse_client=clickhouse_client,
                duckdb_schema=address_canonicalization.ENRICHMENT_SCHEMA,
                duckdb_table=address_canonicalization.ADDRESS_MEMBERS_TABLE,
                clickhouse_database=address_canonicalization.CLICKHOUSE_DATABASE,
                clickhouse_table=address_canonicalization.ADDRESS_MEMBERS_TABLE,
                columns=address_canonicalization.ADDRESS_MEMBER_COLUMNS,
                truncate=True,
                log=context.log.info,
            )
    return dg.MaterializeResult(
        metadata={
            "source_members": rows,
            "member_table": (
                address_canonicalization.QUALIFIED_CLICKHOUSE_ADDRESS_MEMBERS_TABLE
            ),
        }
    )
```

`replace_duckdb_connection_tables_in_clickhouse` becomes an unused import here if nothing
else in the module uses it — check and remove it. The single-table helper's `truncate=True`
selects the same stage + `EXCHANGE TABLES` path (subsystem map §2's note: the parameter
never issues a `TRUNCATE`).

- [ ] **Step 3: Delete check 1's ClickHouse half**

Delete `sweden_company_canonical_addresses_complete_check` (`:668-752`) and its entry in
`defs.asset_checks`. Its arithmetic lives in `address_canonicalization._assert_canonical_address_invariants`
since Task 7, one step earlier in the pipeline, where it aborts the build rather than
reporting on a published snapshot. The `source_rows` term it also carried — a count of
`corpscout.se_company_addresses_current` — is the "upstream observations since snapshot"
metric, which was informational only (`passed` never depended on it); it is not relocated,
and the deleting commit says so.

- [ ] **Step 4: Delete the two ClickHouse-only constants**

In `address_canonicalization.py` delete `QUALIFIED_CLICKHOUSE_CANONICAL_ADDRESSES_TABLE`
(`:22-23`) and `CANONICAL_ADDRESS_COLUMNS` (`:57-...`). Leave `CANONICAL_ADDRESSES_TABLE`
and `QUALIFIED_CANONICAL_ADDRESSES_TABLE` — the DuckDB build writes that table at `:258` and
asserts on it at `:401`.

- [ ] **Step 5: Snapshot the row counts (controller runs these — paste into the migration comment)**

```sql
SELECT 'canonical', count() FROM corpscout.se_company_addresses_canonical_current
UNION ALL SELECT 'members', count() FROM corpscout.se_company_address_members_current
UNION ALL SELECT 'links', count() FROM corpscout.se_company_address_links_current;
```

- [ ] **Step 6: Write migration 000318**

Re-check the next free number first.

`000318_corpscout_retire_se_company_addresses_canonical.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- RETIREMENT -- the Sweden per-company canonical address table's ClickHouse publish.
--
-- This is the cleanup migration 000314's comment deferred this table to (controller ruling
-- A19, 2026-08-24). 000314 found six readers reaching it through
-- address_canonicalization.QUALIFIED_CLICKHOUSE_CANONICAL_ADDRESSES_TABLE -- an indirection
-- a grep for the table name cannot see -- and walked the drop back. All six are gone:
--   1. fetch_sweden_address_geocode_stats, split into an exact-match-rate helper reading
--      se_company_address_links_current and a freshness helper reading se_address_geocodes
--   2. the sweden_company_canonical_addresses_clickhouse publish itself, narrowed in the
--      same commit as this migration to publish only se_company_address_members_current
--   3. all_current_addresses_classified, retired with the legacy geocode pair in 000317
--   4. all_source_observations_have_one_canonical_address, whose arithmetic now runs inside
--      address_canonicalization._assert_canonical_address_invariants against the DuckDB
--      canonical table -- which is NOT retired and still holds every row this check read
--   5. all_company_addresses_link_to_one_shared_address, narrowed to shared-vs-links, with
--      its canonical denominator asserted in shared_addresses._assert_shared_address_invariants
--   6. shared_geocoding_matches_company_baseline, retired with the parity baseline it
--      compared against
--
-- The DuckDB canonical build STAYS. se_company_address_members_current derives from it and
-- the se_company_address final joins through members on every resolution -- only the
-- ClickHouse copy of the per-company canonical rows retires.
--
-- Gate 1 -- zero readers, re-verified immediately before writing this file with
--   rg -n "se_company_addresses_canonical_current" corpscout --glob '!*.pyc'
--   rg -n "QUALIFIED_CLICKHOUSE_CANONICAL_ADDRESSES_TABLE" corpscout --glob '!*.pyc'
-- Remaining hits are migrations 000273, 000278 and 000314 (historical DDL, the ledger),
-- two migration-shape tests asserting that historical DDL, one backoffice not.toContain
-- assertion that the public page does not read this table, and the DuckDB constants, which
-- name a table in the sweden_company_enrichment schema, not in corpscout.
--
-- Gate 2 -- row counts at drop time (SELECT run on the host, <DATE>):
--   se_company_addresses_canonical_current  <N> rows
--   se_company_address_members_current      <N> rows
--   se_company_address_links_current        <N> rows
--
-- UNDROP window is about 480 seconds: if this turns out to be wrong, UNDROP TABLE within it.

DROP TABLE IF EXISTS corpscout.se_company_addresses_canonical_current;
```

`.down.sql` recreates the table **empty** from 000273's `CREATE TABLE` plus 000278's three
`ALTER … ADD COLUMN` statements (copy verbatim, `IF NOT EXISTS`), under a leading comment:
*"Structure only — the rows are not restored. Reverting the code change that narrowed
`sweden_company_canonical_addresses_clickhouse` to members and re-materializing it refills
the table from the DuckDB canonical build, which was never retired."*

- [ ] **Step 7: Register and run**

Append `"000318_corpscout_retire_se_company_addresses_canonical",` to `EXPECTED_MIGRATIONS`.

```bash
cd corpscout/services/dagster_v3
uv run pytest tests/test_clickhouse_migrations.py tests/test_sweden_company_address_geocoding.py \
              tests/test_sweden_geocode_checks.py -q
uv run dg check defs && uv run ruff check src/dagster_v3/defs
cd ../backoffice && npx vitest run tests/company-serving-sections.test.ts
```

- [ ] **Step 8: Commit** (applied by the controller in Task 12g)

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/clickhouse/migrations/000318_corpscout_retire_se_company_addresses_canonical.up.sql \
        corpscout/clickhouse/migrations/000318_corpscout_retire_se_company_addresses_canonical.down.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_geocoding_assets.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/address_canonicalization.py \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py \
        corpscout/services/dagster_v3/tests/test_sweden_company_address_geocoding.py \
        corpscout/services/dagster_v3/tests/test_se_company_address_clickhouse_local.py
git commit -m "chore(sweden_company): retire the Sweden canonical address ClickHouse publish

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 10: Repoint `se_company_address`'s geocode read at the store

Spec §4.6 and §6 step 4. The final's schema does not change — `address_id`, `latitude`, `longitude`, `geocode_status`, `geocoded_at` are already the distilled block and `geocoded_at` maps to the outcome's `matched_at`. What changes is which table those five come from, and what the `new_geocode` change-scan term watches.

**This file's tests are unusually strict and the plan must update them in this same task:** `tests/test_se_company_address.py` parses the change scan's WHERE into an exact disjunct list (`_where_disjuncts`, `:104-110`) and pins all **eight** geocode aliases to their source columns (`:243-259`). Both were written to catch exactly this kind of edit, so both must move with the code — and neither may be weakened while it moves.

**Files:**
- Modify: `src/dagster_v3/defs/se_company/address.py`
- Modify: `corpscout/services/dagster_v3/tests/test_se_company_address.py`

**Interfaces:** `build_geocodes_sql()` and `build_changed_companies_sql()` keep their names, signatures and output contracts (`GEOCODE_COLUMNS`, `SELECTION_REASONS`). `GEOCODES_TABLE` is replaced by an import of `geocode_store`.

**The one new cross-package import.** `defs/se_company/address.py` gains
`from dagster_v3.defs.sweden_company import geocode_store`. It is the first `se_company →
sweden_company` import in the tree, and it is deliberate: the alternative is re-expressing
the versioned read here, which spec §8 names as the design's sharpest risk ("centralized in
one SQL fragment + one pure function, both pinned; consumers never inline their own
ranking"). `geocode_store` was written import-light for exactly this — it pulls in nothing
but `collections.abc`, `dataclasses` and `datetime`, so no Dagster or pyarrow weight crosses
the boundary. There is no cycle: `sweden_company` imports nothing from `se_company`.

- [ ] **Step 1: Update the failing tests first**

In `tests/test_se_company_address.py`:

```python
# EXISTING_TABLES (:48-59): the store replaces the serving table in the asset's precondition.
EXISTING_TABLES = [
    (table,)
    for table in (
        "se_company_address_bolagsverket",
        "se_company_address_scb",
        "se_company_address",
        "se_company_address_correction",
        "se_company_address_members_current",
        "se_company_address_links_current",
        "se_address_geocodes",
    )
]
```

```python
def test_the_geocode_cte_reads_the_store_and_does_not_alias_a_table_with_its_own_name() -> None:
    """Self-shadowing a WITH name inside its own body is analyzer-dependent: the outer
    ``geocodes.latest_geocoded_at`` reads the CTE, so the joined table must be called
    something else or the two names are one identifier with two meanings.

    The CTE reads the store RAW -- max(matched_at) over every stored row for the identity,
    not over the versioned read's chosen row. That is deliberate and is argued in
    build_changed_companies_sql's docstring: the raw maximum is always greater than or equal
    to the current outcome's, so the scan can over-select but can never MISS a company whose
    served coordinate moved. Ranking 2.09M identities on every scan page to avoid an
    occasional harmless re-resolution would be the wrong trade.
    """
    sql = build_changed_companies_sql()
    assert "corpscout.se_address_geocodes AS geocodes" not in sql
    assert "corpscout.se_address_geocodes AS points" in sql
    assert "max(points.matched_at) AS latest_geocoded_at" in sql
    assert "se_address_geocodes_current" not in sql
    # Raw, not ranked: no second copy of the read rule lives in this module.
    assert "LIMIT 1 BY" not in sql


def test_the_geocode_query_reads_the_versioned_read_and_gates_every_joined_column() -> None:
    sql = build_geocodes_sql()
    assert "toUInt8(ifNull(geocodes.geocode_run_id, '') != '') AS has_geocode" in sql
    assert "INNER JOIN corpscout.se_company_address_links_current AS links" in sql
    assert "toString(members.address_key) AS address_fingerprint" in sql
    # The geocode side is the store's ONE read rule, pulled in whole -- byte-identical to
    # what geocode_store builds, so this module cannot drift into a second ranking.
    expected_read = build_current_geocodes_sql(
        columns=("address_id", "match_status", "latitude", "longitude", "matched_at",
                 "geocode_run_id"),
        address_filter_sql=GEOCODE_ADDRESS_FILTER_SQL)
    assert f"LEFT JOIN (\n{expected_read}\n) AS geocodes ON geocodes.address_id = links.address_id" in sql
    # The filter prunes the store on its sorting key's leading column before ranking, so a
    # page of companies never pays for the whole store.
    assert GEOCODE_ADDRESS_FILTER_SQL.strip().startswith("address_id IN (")
    assert "%(company_ids)s" in GEOCODE_ADDRESS_FILTER_SQL
    # Nullable source columns are ifNull'd, never gated; joined non-Nullable ones are gated.
    assert "ifNull(toString(geocodes.latitude), '') AS latitude" in sql
    assert "toString(geocodes.match_status), '') AS geocode_status" in sql
    assert re.findall(r"AS (\w+)", sql[: sql.index("\nFROM ")]) == list(GEOCODE_COLUMNS)
    # Every alias, fed by its own source column -- all eight, unchanged by the repoint. An
    # alias wired to a neighbour would transpose the fact silently, and half of these are
    # same-typed text, so nothing downstream would notice.
    sources = {
        "company_id": "members.company_id",
        # members.address_key IS the source observation's address_fingerprint -- the whole
        # reason the artifacts carry it.
        "address_fingerprint": "members.address_key",
        "address_id": "links.address_id",
        # The hit flag is the geocoder's RUN id, not a coordinate: an address can be
        # classified (unmatched, foreign, postal-box) without a point.
        "has_geocode": "geocodes.geocode_run_id",
        "latitude": "geocodes.latitude",
        "longitude": "geocodes.longitude",
        "geocode_status": "geocodes.match_status",
        "geocoded_at": "geocodes.matched_at",
    }
    assert set(sources) == set(GEOCODE_COLUMNS)
    assert len(sources) == 8
    for column, expression in GEOCODE_PROJECTION:
        assert sources[column] in expression, (column, expression)
```

`test_every_where_disjunct_of_the_change_scan_is_pinned_exactly` (`:158-185`) needs **no
change at all** — its five disjuncts are spelled from `PUBLISHED_AT_SQL` and `EPOCH_SQL` and
name no table. Run it and confirm it still passes rather than editing it; if it fails, the
repoint changed the scan's shape and that is a bug, not a test to update.

Add `build_current_geocodes_sql` and `GEOCODE_ADDRESS_FILTER_SQL` to the test's imports.

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/test_se_company_address.py -q` → failures on the two rewritten geocode tests and an `ImportError` for `GEOCODE_ADDRESS_FILTER_SQL`

- [ ] **Step 3: Repoint `address.py`**

Replace `GEOCODES_TABLE` (`:76`) with the import and the filter constant:

```python
from dagster_v3.defs.sweden_company import geocode_store
```

```python
# The geocode store, read through sweden_company's ONE versioned-read rule. Imported rather
# than re-expressed: a second ranking here would let this final and the serving table
# disagree about which outcome is current while both looked internally consistent, which
# spec section 8 names as this design's sharpest risk.
GEOCODE_STORE_TABLE = geocode_store.QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE
# What the versioned read is allowed to look at for one page of companies. It constrains
# address_id, the store's sorting-key leading column, so a page prunes to a few parts
# instead of ranking all 2.09M identities.
GEOCODE_ADDRESS_FILTER_SQL = f"""address_id IN (
        SELECT address_id
        FROM {DATABASE}.{LINKS_TABLE}
        WHERE company_id IN %(company_ids)s
    )"""
# Only the six columns this module actually reads -- the store has 28.
GEOCODE_READ_COLUMNS = (
    "address_id",
    "match_status",
    "latitude",
    "longitude",
    "matched_at",
    "geocode_run_id",
)
```

`build_geocodes_sql`'s body (`:352-359`) becomes:

```python
    versioned_read = geocode_store.build_current_geocodes_sql(
        columns=GEOCODE_READ_COLUMNS,
        address_filter_sql=GEOCODE_ADDRESS_FILTER_SQL,
    )
    return f"""SELECT
    {_projection_sql(GEOCODE_PROJECTION)}
FROM {DATABASE}.{MEMBERS_TABLE} AS members
INNER JOIN {DATABASE}.{LINKS_TABLE} AS links
    ON links.company_id = members.company_id
   AND links.canonical_address_key = members.canonical_address_key
LEFT JOIN (
{versioned_read}
) AS geocodes ON geocodes.address_id = links.address_id
WHERE members.company_id IN %(company_ids)s"""
```

and its docstring gains a paragraph after the existing LEFT-JOIN-gating one:

```
    The geocode side is no longer a table but the geocode store's versioned read, imported
    whole from sweden_company/geocode_store.py. An identity can hold several attributable
    outcomes -- one per (matcher, OSM snapshot) -- and which one is CURRENT is a rule, not a
    row. That rule is not restated here: this query asks for it. The subquery carries its
    own address filter so the store is pruned on its sorting key before it is ranked; the
    %(company_ids)s parameter is therefore bound twice in one statement, once inside the
    subquery and once in the outer WHERE, which ClickHouse's named parameters handle by
    name rather than by position.
```

`build_changed_companies_sql`'s geocode CTE (`:281-287`) swaps one identifier:

```sql
geocodes AS (
    SELECT links.company_id AS company_id, max(points.matched_at) AS latest_geocoded_at
    FROM {DATABASE}.{LINKS_TABLE} AS links
    INNER JOIN {GEOCODE_STORE_TABLE} AS points ON points.address_id = links.address_id
    WHERE (%(all_companies)s = 1 OR links.company_id IN %(company_ids)s)
    GROUP BY links.company_id
),
```

and its docstring's "THE GEOCODE TERM IS DELIBERATELY BROAD" paragraph (`:206-224`) is
rewritten — this is the paragraph the whole project exists to change:

```
    THE GEOCODE TERM IS NOW AS NARROW AS THE STORE MAKES IT.
    It used to be the widest term here: se_address_geocodes_current was rebuilt whole every
    week with one wall-clock matched_at, so every geocoded company looked changed every
    Monday and the weekly config had to run uncapped. corpscout.se_address_geocodes appends
    an outcome only for an identity a run actually matched, and matched_at is that append's
    instant -- so this term now selects register churn plus real geocode changes, which is
    what it always meant to say.

    It reads the store RAW, not through the versioned read. max(matched_at) over every
    stored row for an identity is always greater than or equal to the current outcome's, so
    this can over-select -- an identity whose newest row is outranked (an adopted row a
    later resolver success beat, say) wakes its companies once more than strictly needed --
    but it can never MISS a company whose served coordinate moved. Over-selection costs one
    republished version with identical content; under-selection would leave a stale
    coordinate served forever. Ranking 2.09M identities on every scan page to avoid the
    former would be the wrong trade, and it would put a second copy of the read rule in this
    module, which is the thing the design forbids.
```

Finally, the asset's precondition (`:567-569`) swaps `GEOCODES_TABLE` for
`geocode_store.GEOCODE_STORE_TABLE`:

```python
    assert_clickhouse_tables_exist(clickhouse, database=DATABASE, tables=(
        SE_COMPANY_ADDRESS, SE_COMPANY_ADDRESS_CORRECTION, *ARTIFACT_TABLES.values(),
        MEMBERS_TABLE, LINKS_TABLE, geocode_store.GEOCODE_STORE_TABLE))
```

- [ ] **Step 4: Run**

```bash
cd corpscout/services/dagster_v3
uv run pytest tests/test_se_company_address.py tests/test_se_company_address_rules.py \
              tests/test_sweden_geocode_store.py -q
uv run dg check defs && uv run ruff check src/dagster_v3/defs/se_company
rg -n "se_address_geocodes_current" src/dagster_v3/defs/se_company/
```

Expected: green; the final `rg` returns nothing.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address.py \
        corpscout/services/dagster_v3/tests/test_se_company_address.py
git commit -m "feat(se_company): read Sweden geocodes through the store's versioned read

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 11: Executed-SQL harnesses — the store, and the address final that now reads it

Spec §7. Two harnesses, because there are two SQL surfaces and they run on two engines. The store's read rule, its ReplacingMergeTree behaviour and the checks run in `clickhouse-local` on the Docker 26.5 image, under both `join_use_nulls` settings. The demand rule is DuckDB SQL and is already executed against a real in-memory DuckDB in Task 4 (`test_the_pending_table_is_the_rule_executed`, `test_an_unchanged_week_selects_nothing_at_all`, `test_rematch_all_selects_every_identity`) — this task ties the two halves together with a reference bump that is appended on the ClickHouse side and read back through the rule.

**Files:**
- Create: `corpscout/services/dagster_v3/tests/test_sweden_geocode_store_clickhouse_local.py`
- Modify: `corpscout/services/dagster_v3/tests/test_se_company_address_clickhouse_local.py`

**Interfaces:** consumes `geocode_store.build_current_geocodes_sql`, `build_current_resolver_geocodes_sql`, `STORE_COLUMNS`, `SERVING_COLUMNS`; `address_geocoding_assets.build_derived_current_geocodes_sql`, `STORE_INVARIANTS_SQL`, `STORE_COVERAGE_SQL`, `EXACT_MATCH_RATE_SQL`, `SNAPSHOT_FRESHNESS_SQL`, `build_store_append_regression_sql`; `se_company.address.build_geocodes_sql`, `build_changed_companies_sql`.

- [ ] **Step 1: Write the store harness**

`corpscout/services/dagster_v3/tests/test_sweden_geocode_store_clickhouse_local.py`:

```python
"""Executes the geocode store's read rule, its append semantics and its checks against the
migrations' own DDL in a disposable clickhouse-local. Proves the SQL runs on the deployed
ClickHouse version -- substring tests cannot, and the read rule is the one thing in this
design that no downstream failure would reveal if it were subtly wrong.

The fixture is five identities, each a scenario the rule has to get right:

  SETTLED    one resolver matched_exact at md5-A. The ordinary 2.09M case.
  RETRIED    resolver ambiguous at md5-A, then matched_exact at md5-B. The retry pool doing
             its job -- the newer row wins because it is newer AND servable.
  REGRESSED  resolver matched_exact at md5-A, then ambiguous at md5-B. Newest still wins:
             within the resolver family the rule is plain recency, and the identity goes
             back into the retry pool. This is the case a flat rank gets wrong.
  ADOPTED    resolver ambiguous at md5-A, plus a legacy_adopted_v1 exact imported later.
             The adopted row is served, and the ambiguous is still what the demand scan
             sees -- so the identity stays eligible for a rematch.
  RECLAIMED  the ADOPTED shape plus a later resolver matched_exact. The resolver takes over
             and the adopted row is neither deleted nor merged, just outranked.

The whole script runs twice, once under default settings and once with
`SET join_use_nulls = 1` prepended. The read rule has no joins, but the checks and the
address final's geocode query do, and every one of them must answer identically.
"""

import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dagster_v3.defs.sweden_company.address_geocoding_assets import (
    EXACT_MATCH_RATE_SQL,
    SNAPSHOT_FRESHNESS_SQL,
    STORE_COVERAGE_SQL,
    STORE_INVARIANTS_SQL,
    build_derived_current_geocodes_sql,
    build_store_append_regression_sql,
)
from dagster_v3.defs.sweden_company.geocode_store import (
    LEGACY_ADOPTED_POLICY_VERSION,
    SERVING_COLUMNS,
    STORE_COLUMNS,
    build_current_geocodes_sql,
    build_current_resolver_geocodes_sql,
)
from tests.test_se_company_person_clickhouse_local import _clickhouse_local_command, _literal, _render

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATIONS = (
    "000274_corpscout_se_shared_addresses.up.sql",
    "000275_corpscout_se_address_geocodes_current.up.sql",
    "000277_corpscout_se_address_geocode_spread.up.sql",
    "000278_corpscout_se_address_components.up.sql",
    "000316_corpscout_se_address_geocodes_store.up.sql",
)
NEEDED_TABLES = frozenset({
    "se_addresses_current",
    "se_company_address_links_current",
    "se_address_geocodes_current",
    "se_address_geocodes",
})

POLICY = "se-address-resolution-policy-v5"
MD5_A, MD5_B = "md5-alpha", "md5-beta"
SETTLED, RETRIED, REGRESSED, ADOPTED, RECLAIMED = ("1" * 64, "2" * 64, "3" * 64,
                                                   "4" * 64, "5" * 64)
T_A = _literal(datetime(2026, 8, 1, tzinfo=UTC))
T_B = _literal(datetime(2026, 8, 8, tzinfo=UTC))
T_IMPORT = _literal(datetime(2026, 8, 15, tzinfo=UTC))
T_RECLAIM = _literal(datetime(2026, 8, 22, tzinfo=UTC))
RUN_A, RUN_B, RUN_IMPORT, RUN_RECLAIM = "run-a", "run-b", "run-import", "run-reclaim"
```

`_schema_statements` copies `tests/test_se_company_address_clickhouse_local.py:120-144`
verbatim (per-statement filter on `NEEDED_TABLES`, comment lines stripped). `_marked` copies
`:260-262`. The fixture inserts one `se_addresses_current` row and one
`se_company_address_links_current` row per identity, then the store rows above, using a
`_store_row(address_id, policy, md5, status, precision, lat, lon, run, matched_at)` helper
that binds `STORE_COLUMNS` positionally so a column added to the migration and forgotten
here fails the script rather than silently shifting values.

The marked sections and what each proves:

| section | query | asserts |
|---|---|---|
| `served` | `build_current_geocodes_sql(columns=("address_id", "policy_version", "reference_md5", "match_status"))` | `SETTLED`→(v5, md5-A, matched_exact); `RETRIED`→(v5, md5-B, matched_exact); `REGRESSED`→(v5, md5-B, **ambiguous**) — newest wins inside the family; `ADOPTED`→(legacy_adopted_v1, matched_exact); `RECLAIMED`→(v5, matched_exact) |
| `resolver_view` | `build_current_resolver_geocodes_sql(columns=("address_id", "match_status", "reference_md5"))` | five rows, and `ADOPTED` reads **ambiguous** here — the adopted row is invisible to the demand scan, which is what keeps the identity eligible for a rematch |
| `served_after_reappend` | the same read after re-inserting the SETTLED row **byte-identically** | identical to `served`, row for row. Spec §5's idempotency: a re-append with the same (policy, reference) is a no-op in content |
| `store_rows_after_reappend` | `SELECT count() FROM corpscout.se_address_geocodes` | unchanged **after** `OPTIMIZE TABLE corpscout.se_address_geocodes FINAL` — the ReplacingMergeTree contract, made deterministic by forcing the merge rather than hoping for one |
| `regression_probe` | `build_store_append_regression_sql()` rendered with `RUN_B` and `T_A` | non-zero — an append stamped `T_A` for a key that already holds `T_B` WOULD be swallowed, and the guard sees it. Rendered with `T_B`, zero |
| `derived` | `build_derived_current_geocodes_sql()` | 26 columns, five rows, and the same `(address_id, match_status)` pairs as `served` — the derivation and the read are one expression |
| `invariants` | `STORE_INVARIANTS_SQL` | `rows == unique_keys` after the OPTIMIZE, zero for every violation counter |
| `coverage` | `STORE_COVERAGE_SQL` | 0 — every identity has an outcome |
| `rate` | `EXACT_MATCH_RATE_SQL` | links counted, and only the four servable-exact identities in the numerator |
| `freshness` | `SNAPSHOT_FRESHNESS_SQL` | the newest `source_snapshot_at` across every stored row, not just the current ones |
| `reference_bump` | append a `matched_exact` at a third md5 for `REGRESSED`, then `served` again | `REGRESSED` flips back to `matched_exact` — the retry pool's end-to-end payoff, with the DuckDB half proved in Task 4 |

Two mutation probes to write as their own tests, because they are the ones a reviewer will
try:

```python
def test_a_newer_resolver_ambiguous_does_not_unseat_an_adopted_exact(sections) -> None:
    """Delete the `servable` component from the choice rank and this is what breaks: the
    import's 19,413 recovered coordinates all revert to ambiguous at once, and every other
    section in this file still passes."""
    served = {row[0]: row[3] for row in sections["served"]}
    assert served[ADOPTED] == "matched_exact"
    adopted_row = [row for row in sections["served"] if row[0] == ADOPTED][0]
    assert adopted_row[1] == LEGACY_ADOPTED_POLICY_VERSION


def test_a_newer_resolver_ambiguous_DOES_unseat_an_older_resolver_exact(sections) -> None:
    """The other half, and the reason the rule is two stages: promote `servable` above
    `matched_at` for the resolver family too and REGRESSED would keep serving a coordinate
    the current snapshot no longer supports, for ever, with nothing selecting it again."""
    served = {row[0]: row[3] for row in sections["served"]}
    assert served[REGRESSED] == "ambiguous"
```

The module-scoped `sections` fixture and the `params=(0, 1)` / `ids=("join_use_nulls_off",
"join_use_nulls_on")` parametrization copy
`tests/test_se_company_address_clickhouse_local.py:398-415` exactly.

- [ ] **Step 2: Update the address final's harness for the store**

In `tests/test_se_company_address_clickhouse_local.py`:

1. `MIGRATIONS` (`:74-83`) gains `"000316_corpscout_se_address_geocodes_store.up.sql"` after
   `000278`, and `NEEDED_TABLES` (`:84-94`) gains `"se_address_geocodes"`.
   `se_address_geocodes_current` **stays in both** — the backoffice public-page section
   server still reads it (`services/backoffice/tests/company-serving-sections.test.ts:54`),
   so the table is not going anywhere in this plan and the harness keeps proving its DDL
   replays.
2. `FIXTURE`'s geocode insert (`:197-203`) writes the **store**, not the serving table:

```sql
INSERT INTO corpscout.se_address_geocodes
    (address_id, policy_version, reference_md5, address_identity_run_id, normalized_match_key,
     match_status, candidate_count, candidate_record_ids, candidate_record_urls, match_method,
     match_confidence, latitude, longitude, geocode_provider, geocode_precision,
     coordinate_supporting_point_count, geocode_run_id, matched_at)
VALUES
    ('{ADDRESS_ID}', 'se-address-resolution-policy-v5', 'md5-alpha', 'ident-1',
     'storgatan 1|11122|stockholm', 'matched_exact', 1, [], [], 'exact',
     0.99, 59.33, 18.06, 'osm', 'building', 1, 'geo-1', {T_GEOCODE});
```

3. `NEW_GEOCODE_SQL` (`:236-244`) likewise appends a store row for `ADDRESS_ID_SCB` at
   `now64(3, 'UTC')`. The docstring comment above it changes from "The weekly geocoding job
   answers for ALPHA's SCB identity" to note that the append is now what moves `matched_at`
   — the whole table is no longer restamped, which is precisely why this scenario is now the
   ONLY thing that can wake ALPHA.
4. The module docstring's paragraph at `:32-34` is rewritten:

```
And the guarantee address.py's scan docstring headlines -- "a re-geocode is evidence, and no
company keeps a stale coordinate" -- is closed end to end: a settled company is woken again
by nothing but a new APPEND to corpscout.se_address_geocodes. It is worth saying what
changed under this test: the serving table used to be rebuilt whole every week with one
wall-clock matched_at, so this scenario was indistinguishable from the ordinary weekly run
and the scan woke every geocoded company every Monday. Now only an identity a run actually
matched moves, and this test is the difference.
```

5. `test_a_re_geocode_wakes_a_published_company` (`:540-556`) needs no logic change — it
   asserts the `new_geocode` reason flag, which still fires. Confirm it passes rather than
   editing it.
6. `_final_row_values` (`:314-318`) is unaffected: the final's own schema does not change.

- [ ] **Step 3: Run both harnesses under both settings**

```bash
cd corpscout/services/dagster_v3
uv run pytest tests/test_sweden_geocode_store_clickhouse_local.py \
              tests/test_se_company_address_clickhouse_local.py -q
```

Expected: every test passes twice, once per `join_use_nulls` id. A skip means
`clickhouse-local` is unavailable in the environment — that is **not** a pass; report it and
stop, because this task's whole deliverable is executed SQL.

- [ ] **Step 4: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/services/dagster_v3/tests/test_sweden_geocode_store_clickhouse_local.py \
        corpscout/services/dagster_v3/tests/test_se_company_address_clickhouse_local.py
git commit -m "test(sweden_company): execute the geocode store's read rule on ClickHouse 26.5

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 12a (Phase 2): Apply migration 000316 — **controller**

- [ ] Apply **000316** with the same golang-migrate path used for 000307–000315. Do this BEFORE deploying any code — Task 3's store-append asset calls `assert_clickhouse_tables_exist` on it.
- [ ] Verify:
  ```sql
  SELECT name, engine, sorting_key FROM system.tables
  WHERE database = 'corpscout' AND name = 'se_address_geocodes';
  SELECT name, type FROM system.columns
  WHERE database = 'corpscout' AND table = 'se_address_geocodes' ORDER BY position;
  SELECT count() FROM corpscout.se_address_geocodes;
  ```
  Expected: engine `ReplacingMergeTree`, sorting key `address_id, policy_version, reference_md5`; 28 columns with `policy_version` and `reference_md5` in positions 2 and 3; 0 rows.
- [ ] Stop and report. Nothing else runs until phase 3 and 4 are reviewed.

### Task 12b (Phase 5): Deploy the dual-write code — **controller**

- [ ] `cd corpscout/services/dagster_v3/ansible && ansible-playbook -i inventory.ini light_sync.yml`.
- [ ] In the Dagster UI confirm: `sweden_address_geocode_store_clickhouse` exists in group `sweden_company` with `sweden_address_resolution_current_duckdb` upstream; `sweden_address_geocode_store_backfill_clickhouse` exists and is in exactly one job, `sweden_address_geocode_store_backfill_job`; the weekly schedule `sweden_company_address_geocoding_weekly` is still RUNNING and still names 15 assets (14 plus the store append); no code-location load errors.
- [ ] Stop here.

### Task 12c (Phase 5): Backfill the store — **controller**

- [ ] **Preview first.** Materialize `sweden_address_geocode_store_backfill_clickhouse` with **no config**. Expected metadata: `preview: true`, `serving_rows` ≈ **2.09M**, `policy_version: se-address-resolution-policy-v5`. A non-zero `missing_reference` raises instead — if it does, stop: some serving rows carry no OSM MD5 and the store cannot attribute them.
- [ ] **Then execute.** Launch `sweden_address_geocode_store_backfill_job` with
  `{"ops": {"sweden_address_geocode_store_backfill_clickhouse": {"config": {"execute": true}}}}`.
- [ ] Verify:
  ```sql
  SELECT count(), uniqExact(address_id), uniqExact(policy_version), uniqExact(reference_md5)
  FROM corpscout.se_address_geocodes;
  SELECT count() FROM corpscout.se_address_geocodes_current;
  ```
  Expected: the first count equals the second exactly; `uniqExact(address_id)` equals it too (one row per identity at this point); `uniqExact(policy_version) = 1`; `uniqExact(reference_md5) = 1` **or** small — a handful of distinct MD5s would mean the serving table is a mix of snapshots, which is worth reporting but is not a failure.
- [ ] **Prove idempotency on production data.** Run the backfill a second time with `execute: true`. Expected: the counts above are UNCHANGED. Then:
  ```sql
  OPTIMIZE TABLE corpscout.se_address_geocodes FINAL;
  SELECT count(), uniqExact(tuple(address_id, policy_version, reference_md5))
  FROM corpscout.se_address_geocodes;
  ```
  The two numbers must be equal. If they are not, the backfill is not writing the same content twice and Task 3's guarantee is broken — stop and report.
- [ ] **Let one weekly job run**, unchanged, and confirm the store gained a second generation of rows without the serving table's behaviour changing at all: `SELECT policy_version, reference_md5, count(), min(matched_at), max(matched_at) FROM corpscout.se_address_geocodes GROUP BY 1, 2 ORDER BY 3 DESC`.
- [ ] Stop and report the numbers. Phase 6 is reviewed before it is deployed.

### Task 12d (Phase 7): Deploy demand-driven matching and watch one cycle — **controller**

- [ ] Deploy (`light_sync.yml`). Confirm in the UI: `sweden_address_geocode_demand_duckdb` exists with `sweden_shared_addresses_clickhouse` and `sweden_osm_addresses_duckdb` upstream; `sweden_shared_address_osm_matches_duckdb` is **gone**; `sweden_address_resolution_shadow_duckdb`'s upstream is now the demand asset; `sweden_address_geocodes_clickhouse` has `sweden_address_geocode_store_clickhouse` upstream and holds no pool.
- [ ] **Watch the first weekly run.** Record from the demand asset's metadata: `reference_md5`, `pending_identities`, `reason_counts`, `loaded_previous_outcomes`. Expect a large `no_outcome` count only if the identity chain rebuilt fingerprints; a large `reference_changed` count if Geofabrik published a new snapshot; both near zero otherwise.
- [ ] Record the **wall-clock time** of the shadow and promotion assets and compare with the previous week's. This is the number the project exists to move — the previous baseline is ~85 minutes for the resolution run.
- [ ] Confirm `derived_current_matches_the_store` passed and that `SELECT count() FROM corpscout.se_address_geocodes_current` is unchanged in magnitude (~2.09M).
- [ ] **Then force the quiet case.** Re-launch `sweden_company_address_geocoding_weekly_job` immediately. Expected: the demand asset reports `pending_identities: 0` and `short_circuit: true`; the shadow asset logs *"no pending identities, skipping matching"* and finishes in seconds with no reference index built; promotion appends 0 rows; the derivation still runs and the parity check still passes. **This is goal 1, observed.** If the shadow instead spends minutes building an index, stop and report — the short-circuit is not where it needs to be.
- [ ] Note in the report that `sweden_address_resolution_unmatched_diagnostics_duckdb` now describes only the identities the last matching run touched, not the whole unmatched population. That is expected; it becomes a partial view by design.
- [ ] Stop and report.

### Task 12e (Phase 9): Run the legacy-adoption import — **controller, owner-gated**

- [ ] Deploy Task 6's code (`light_sync.yml`) and confirm `sweden_address_geocode_legacy_adoption_clickhouse` exists and is in exactly one job.
- [ ] **Preview.** Materialize it with no config. Record `adoptable_identities`, `contributing_companies`, `refused_disagreeing_identities`. The company-level figure measured on prod 2026-08-24 was **~19,413**; the identity-level figure is smaller (companies share addresses) and is what the spec's §9 item 2 leaves to be measured here. **Take the owner's go-ahead on these numbers before executing** — this is the plan's one owner gate.
- [ ] **Execute.** Launch `sweden_address_geocode_legacy_adoption_job` with
  `{"ops": {"sweden_address_geocode_legacy_adoption_clickhouse": {"config": {"execute": true}}}}`.
- [ ] **Verify the sample by hand.** Take 10 rows from the asset's `sample` metadata and, for each, run:
  ```sql
  SELECT legacy.company_id, legacy.match_status, legacy.match_confidence,
         legacy.latitude, legacy.longitude, legacy.geocode_precision
  FROM corpscout.se_company_address_geocode_results AS legacy
  INNER JOIN corpscout.se_company_address_links_current AS links
      ON links.company_id = legacy.company_id
     AND links.canonical_address_key = legacy.address_key
  WHERE links.address_id = {address_id:String};
  ```
  Every contributing legacy row must be `matched_exact` at confidence 1.0 with the same coordinate the adopted store row carries.
- [ ] **Verify the read rule serves them.** For the same ten:
  ```sql
  SELECT address_id, policy_version, match_status, latitude, longitude
  FROM corpscout.se_address_geocodes_current
  WHERE address_id IN (...);
  ```
  Expected: `matched_exact` with the adopted coordinate. The derived serving table is computed by the versioned read, so this is the read rule answering on production data.
- [ ] Record `adopted_identities`, `contributing_companies`, `refused_disagreeing_identities` and the date — Task 8's migration comment needs all four, and they replace its `<N_IDENTITIES>` / `<N_COMPANIES>` / `<N_REFUSED>` / `<DATE>` placeholders.
- [ ] Stop and report.

### Task 12f (Phase 11): Apply migration 000317 — **controller**

- [ ] Re-run Task 8 Step 1's three `rg` commands one final time and confirm the migration's comment still matches reality.
- [ ] **Deploy the code change first** (`light_sync.yml`), so nothing reads the tables at the moment they go. Confirm the three legacy assets and the adoption asset are gone from the UI and that the weekly job has 12 assets.
- [ ] Take the Task 8 Step 6 row-count snapshot and paste it into the migration comment; commit that edit by explicit path before applying.
- [ ] Apply **000317**. Watch for ~10 minutes: `UNDROP TABLE` is available for about 480 seconds.
- [ ] Verify:
  ```sql
  SELECT name FROM system.tables WHERE database = 'corpscout'
    AND name IN ('se_company_address_geocodes', 'se_company_address_geocode_results');
  SELECT count() FROM corpscout.se_address_geocodes WHERE policy_version = 'legacy_adopted_v1';
  ```
  Expected: no rows from the first; the adopted count unchanged from 12e.
- [ ] Stop and report.

### Task 12g (Phase 13): Apply migration 000318 and close the project — **controller**

- [ ] Re-run Task 9 Step 1's two `rg` commands. Deploy Tasks 9, 10 and 11's code **first** (`light_sync.yml`).
- [ ] Confirm in the UI: `sweden_company_canonical_addresses_clickhouse` publishes members only (its metadata has `source_members` and no `canonical_addresses`); `se_company_address_clickhouse` loads without error.
- [ ] Take the Task 9 Step 5 snapshot, paste it into the migration comment, commit by explicit path, then apply **000318**. UNDROP watch, ~10 minutes.
- [ ] Verify:
  ```sql
  SELECT name FROM system.tables WHERE database = 'corpscout'
    AND name = 'se_company_addresses_canonical_current';
  SELECT count(), uniqExact(company_id) FROM corpscout.se_company_address FINAL WHERE is_current;
  ```
  Expected: nothing from the first; the second unchanged from before the deploy.
- [ ] **Watch one Monday `se_company_address` run.** Record `selected_company_count` and the per-reason counts. Expected: `new_geocode` collapses from "every geocoded company" to the size of that week's actual matching work — which is the payoff spec §4.6 promises and the number this whole project is measured by. Compare against the run recorded in the address plan's Task 10c Step 4.
- [ ] Record the final numbers in the spec's §2 (goals, with measured outcomes) and commit that doc change by explicit path.
- [ ] **Open the follow-up.** Spec §6 step 6 — retiring `se_address_geocodes_current` and the transition parity check — is not in this plan. Its blocker is the backoffice public-page section server, which still reads the table (`services/backoffice/tests/company-serving-sections.test.ts:54`). Note that as the one remaining reader so the follow-up starts from a known gate.

---

## Self-review

### Spec coverage walk (`2026-08-24-se-geocode-simplification-design.md`)

| spec section | where it lands |
|---|---|
| §1 problem — whole-universe rematch, run-wide `matched_at`, three matchers, trapped legacy signal, canonical publish with no serving reader | Tasks 4 (demand + join matcher), 3+5 (`matched_at` per append, `_current` derived), 6+8 (trapped signal, then the pair), 7+9 (canonical publish) |
| §2 goal 1 — cost proportional to change | Task 4's pending rule + short-circuit; observed in Task 12d's forced quiet run |
| §2 goal 2 — one matcher | Task 4 (join matcher), Task 8 (legacy matcher) |
| §2 goal 3 — permanent versioned store, "current" is a read rule | Tasks 1 (DDL), 2 (the rule), 3 (the write path), 5 (`_current` becomes a projection of it) |
| §2 goal 4 — the legacy exacts preserved as auditable `legacy_adopted` outcomes | Task 6, run in Task 12e, before Task 8 drops the source |
| §2 goal 5 — the Monday scan sees only real changes | Task 10 + Task 12g's measured Monday run |
| §2 goal 6 — three ClickHouse tables retire | Task 8 (two), Task 9 (one). The fourth, `se_address_geocodes_current`, is §6 step 6 and is explicitly out of scope — see Global Constraints |
| §3 non-goals | Restated in Global Constraints; the identity chain's rebuild, Lantmäteriet, the resolver's semantics, the DuckDB canonical build and the public page are untouched. Task 9 keeps the DuckDB canonical build explicitly, in both the code step and the migration comment |
| §4.1 the store | Task 1 (DDL exactly as specified: `ReplacingMergeTree(matched_at)`, `ORDER BY (address_id, policy_version, reference_md5)`, 000275's shape + the two version columns), Task 2 (the read rule), Task 5 (`_current` derived during the transition) |
| §4.2 demand-driven matching, the MD5 short-circuit, `rematch_all` | Task 4 |
| §4.3 matcher retirements + the dead-output gate | Task 4 Step 1 (the gate, with the plan-time finding), Task 8 (the legacy matcher) |
| §4.4 the adoption import | Task 6, with the members→links join corrected |
| §4.5 the six checks and the canonical publish | Task 7 (all six), Task 9 (the publish + the drop) |
| §4.6 the `se_company_address` final | Task 10 |
| §5 the versioning contract | Task 1's comment, Task 3's promotion invariants + `build_store_append_regression_sql`, Task 7's check 3 (`reference_md5 = '' OR policy_version = ''`), Task 11's idempotency section |
| §6 rollout order | The phases table, with the one documented deviation (checks before the legacy drop) |
| §7 testing | Pure-rule table tests in Tasks 2 and 4; executed DuckDB in Task 4; executed ClickHouse in Task 11; mutation probes named explicitly in Tasks 2, 7 and 11 |
| §8 risks | Dead-output → Task 4 Step 1's hard gate; store growth → accepted, one row per (identity, policy, reference), TTL deferred; read-rule subtlety → one fragment + one twin, imported by both consumers (Tasks 2, 5, 10), never re-expressed; transition drift → Task 5's parity check; partial failure → the demand scan picks up the remainder next run, and Task 3's regression guard proves a re-append cannot be swallowed |
| §9 open items | All four resolved: see below |

### The spec's §9 open items, resolved

1. **Exact versioned-read SQL and its Python twin** — Task 2. The rule is **two stages**, not one rank: per-family newest first, then a `servable`-led choice between the at most two survivors. A flat rank cannot express §4.1's two sentences without dropping one of them — the three-row case (adopted exact, newer resolver exact, newest resolver ambiguous) makes them cyclic. Both stages are total orders and both have a Python twin.
2. **Identity-level adoption count** — measured at execution, Task 12e, and it fills four named placeholders in Task 8's migration comment. This is the one place the plan defers a number, and the spec allows it explicitly.
3. **Where checks 1–2's DuckDB relocations assert** — **inside the existing build functions**, Task 7. Every invariant in `sweden_company/` already works that way, and it is strictly stronger: the raise aborts the transaction before the bad snapshot is committed instead of reporting on it afterwards. There is no separate-assertion-asset pattern in this subsystem to follow.
4. **The `matched_at` tie-break** — Task 2. Same family → `reference_md5` then `policy_version`; different families at the same instant → the resolver wins, which is §4.1's "same-or-newer"; the same key triple twice at one instant → content-identical by §5, so the tie cannot be lost. The read never uses `FINAL`, because `FINAL` would hand the choice to un-merged part order. Task 3's `build_store_append_regression_sql` additionally refuses an append that a newer stored row would swallow.

### Two more ambiguities found and resolved (not in §9)

5. **§4.4's join path is wrong.** It says to map `canonical_address_key → address_id` through `se_company_address_members_current`; that table has no `address_id` column (`address_canonicalization.ADDRESS_MEMBER_COLUMNS`, migration 000273). The map exists only in `se_company_address_links_current`. Task 6 joins through links and says so in the module docstring.
6. **§4.2's "new identities" rule is self-contradictory under a reference bump.** Read literally ("no outcome for the current `(policy_version, reference_md5)`"), a new Geofabrik snapshot makes EVERY identity new, which would defeat the retry-pool rule sitting directly beneath it. Task 4 resolves it into three disjoint terms over the identity's current **resolver** outcome — `no_outcome` (no resolver row at all), `policy_changed` (a bump is a full rematch, as §4.2 item 3 promises), `reference_changed` (non-geocoded only). A geocoded outcome at a stale reference is deliberately not retried, which is where the whole saving lives.

### The gate that came back false, and what the plan does about it

Spec §4.3 asserts nothing reads the join matcher's output between its write and the promotion overwrite. **Run at plan time, it is false**: `address_resolution_shadow.py:549` (`_replace_comparison`) and `address_resolution_promotion.py:174` (the postcode-conflict promotion gate) both `INNER JOIN` it. The plan does not delete-and-hope and does not stop dead: Task 4 Step 1 records both sites with file:line, repoints both at the store's previous **resolver** outcome in the same commit, and keeps the STOP-and-report instruction for any reader that is not one of those two. The semantic change is called out where it happens — the comparison stops reporting "resolver vs the old matcher" and starts reporting "resolver vs last week", which makes week one's transition table large and expected.

### Placeholder scan

No `TBD` and no `TODO` anywhere. Four deliberate placeholders, all of them gate output the controller fills at execution and reviews before a destructive step:

- Task 8's migration comment: `<N_COMPANIES>`, `<N_IDENTITIES>`, `<N_REFUSED>`, `<DATE>` (from Task 12e) and its `<N>` row counts (from Task 12f).
- Task 9's migration comment: `<DATE>` and three `<N>` row counts (from Task 12g).

Task 11 describes two of its three harness scaffolds by shape and by the line they copy from (`tests/test_se_company_address_clickhouse_local.py:120-144` for the migration replay, `:260-262` for `_marked`, `:398-415` for the two-settings fixture) rather than restating ~120 lines of verbatim boilerplate; every assertion those scaffolds feed is spelled out in the section table and in the two named mutation probes. Task 6's `_script()` is described the same way for the same reason.

### Interface consistency

Names flow forward and are never re-declared:

- `geocode_store` is the single home of the taxonomy (`GEOCODED_STATUSES`, `VALID_STATUSES` — `address_resolution_promotion` imports them in Task 2 and its five `_quoted(...)` call sites are untouched), of the column contracts (`STORE_COLUMNS`, `STORE_KEY_COLUMNS`, `SERVING_COLUMNS`, `RANK_INPUT_COLUMNS`) and of both read builders. `SERVING_COLUMNS` is asserted equal to `shared_address_geocoding.ADDRESS_GEOCODE_COLUMNS` rather than importing it, so the matcher-era module keeps no new dependents while it shrinks.
- Every ClickHouse consumer of "the current outcome" calls `build_current_geocodes_sql`: Task 5's derivation (`SERVING_COLUMNS`), Task 7's exact-match-rate query (two columns), Task 10's `build_geocodes_sql` (six columns). Every consumer of "the current resolver outcome" calls `build_current_resolver_geocodes_sql`: Task 4's demand loader, Task 6's adoption selection. **No third expression of either rule exists anywhere in the plan**, which is §8's central mitigation.
- `StoredOutcome(address_id, policy_version, reference_md5, match_status, matched_at)` is produced by Task 2 and consumed by Task 4's `pending_reason` — the same five fields the demand rule reasons about, and the same five the loaded DuckDB table carries as its first four columns plus `matched_at`.
- `PREVIOUS_OUTCOME_COLUMNS` (Task 4) is the superset needed by all three of its consumers: the demand rule (`policy_version`, `reference_md5`, `match_status`), the repointed comparison (`match_status`) and the repointed postcode gate (`match_status`, `match_method`, `match_confidence`, `candidate_record_ids`). The DuckDB `create table` in `load_current_resolver_outcomes` declares those eight columns in that order and `_insert_previous_outcome_batch` binds eight `?` — they must stay in step, and the fixture in `tests/test_sweden_geocode_demand.py` declares the same eight so a drift fails there first.
- The promotion's return dict grows `reference_md5`, `appended_rows` (Task 3) and `short_circuit`, `pending_identities` (Task 4); `address_resolution_assets.py:130-138` spreads every scalar into metadata, so all four surface without touching the asset — but `shadow_status_counts` and `largest_transitions` must exist in the shadow's short-circuit dict because `:93-96` reads them by key. Task 4 Step 5 supplies both.
- Asset keys added: `sweden_address_geocode_store_clickhouse`, `sweden_address_geocode_store_backfill_clickhouse` (Task 3), `sweden_address_geocode_demand_duckdb` (Task 4), `sweden_address_geocode_legacy_adoption_clickhouse` (Task 6, deleted in Task 8). Removed: `sweden_shared_address_osm_matches_duckdb` (Task 4), `sweden_company_address_osm_matches_duckdb`, `sweden_company_address_geocodes_clickhouse`, `sweden_company_address_geocode_results_clickhouse` (Task 8). `sweden_address_geocodes_clickhouse` and `sweden_company_canonical_addresses_clickhouse` keep their keys throughout — their bodies change, the graph does not churn, and the freshness leaves and job selections stay readable.

### Type consistency

- `SwedenAddressGeocodeStats` (three ints + a datetime + a rate) is replaced by `SwedenGeocodeExactMatchStats` (three ints + a rate) and a bare `datetime | None`. The split is the point: the old dataclass forced two unrelated checks to share a query that read three tables, one of which is being dropped.
- `reference_md5` is `String` in ClickHouse and `str` in Python, never `None`: the store's key column is non-Nullable, the promotion fills it with `coalesce(provenance.source_md5, '')` and then refuses `''`, the backfill refuses to run if any serving row lacks an MD5, and check 3 counts `reference_md5 = ''` as a violation. `source_md5` stays `Nullable(String)` because 000275 declared it so and the store keeps 000275's shape.
- `matched_at` is `DateTime64(3, 'UTC')` on both tables and `datetime` (tz-aware) in every Python signature; `family_rank` and `choice_rank` compare it directly, so a naive datetime reaching `StoredOutcome` would raise rather than compare wrongly.
- `pending_reason` returns `str` — `""` for "not pending" rather than `None`, so the DuckDB CASE and the Python twin have the same shape and the SQL can filter on `pending_reason != ''`.
- The demand asset's `rematch_all` is bound as `?::boolean` in DuckDB and typed `bool` in the config; `PENDING_REASONS` is a tuple of the four literals the CASE can emit, and Task 4's parametrized test asserts every returned reason is in it.
- `candidate_count` is `UInt16` in the store and `usmallint` in DuckDB (the promotion already clamps to 65535 at `:275-287`); the adoption import re-casts the legacy `UInt16` explicitly with `toUInt16(...)`, and `match_confidence` with `toFloat32(...)`, because `any()` over a `Float64` legacy column would otherwise widen the insert.

### Known deliberate costs, called out where they are paid

1. **The change scan reads the store raw, not ranked** (Task 10). It can over-select an identity whose newest stored row is outranked; it can never under-select. Argued in `build_changed_companies_sql`'s rewritten docstring and pinned by the `"LIMIT 1 BY" not in sql` assertion, which is what stops someone "fixing" it into a second copy of the read rule.
2. **The exact-match-rate metric's history does not carry over** (Task 7). The check moves to a new host asset and its denominator changes from canonical company addresses to company-address links, so week one has no previous value and no comparable absolute number. Task 12d's report says so rather than letting a missing comparison read as a bug.
3. **The unmatched diagnostics become a partial view** (Task 12d). They describe the identities the last matching run touched, not the whole unmatched population — an unavoidable consequence of matching only what is due, and worth stating before someone reads a shrunken diagnostics table as data loss.
4. **A geocoded resolver outcome at a stale reference is never retried without `rematch_all`** (Task 4). That is where the saving comes from, and it means a Geofabrik improvement to an already-matched address is not picked up until an operator asks. Stated in `geocode_demand`'s module docstring.
5. **The store grows without a compaction policy.** One row per (identity, policy, reference); references change weekly at most and policies rarely. Spec §8 defers a TTL until there are three versions in the wild, and nothing in this plan pretends otherwise.
