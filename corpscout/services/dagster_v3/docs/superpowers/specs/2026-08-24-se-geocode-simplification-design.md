# Swedish Address Geocoding Simplification — Design

Date: 2026-08-24. Status: approved by owner in discussion; this document is the binding record.
Companion facts document (subsystem map with file:line evidence for every claim below):
`.superpowers/sdd/2026-08-24-se-company-address/geocode-subsystem-map.md` (session artifact; the
claims it grounds are restated here with their locations so this spec stands alone).

## 1. Problem

The weekly geocoding job (`sweden_company_address_geocoding_weekly`, cron `5 4 * * 2`
Europe/Stockholm, RUNNING) recomputes the entire matching universe every run:

- `se_address_geocodes_current` (~2.09M rows) is rebuilt whole — DuckDB `CREATE OR REPLACE
  TABLE`, then ClickHouse stage + `EXCHANGE TABLES` (`clickhouse/resolved.py:103-142`).
  Matching is idempotent per address identity (an `address_id` is a fingerprint of normalized
  address text; the text cannot change under it), so for an unchanged matcher and reference
  this reproduces known results — usually against a byte-identical OSM snapshot.
- Because every row gets a fresh `matched_at` (wall clock at asset start —
  `address_geocoding_assets.py:395/:509`, `address_resolution_assets.py:120`), the
  `se_company_address` final's `new_geocode` change-scan term re-selects every geocoded
  identity every Monday: the ~85-minute weekly resolution run and the uncapped
  `max_companies` requirement (address plan ruling A17) are both downstream symptoms.
- THREE matchers run per weekly job, each building its own OSM candidate indexes (the
  dominant cost — reference-index construction happens three times):
  1. the **join matcher** (`shared_address_geocoding.py:48-122`) — whose output is
     **overwritten** by the resolver's promotion before anything reads it
     (`address_resolution_promotion.py:67-74`; the publish asset depends on the resolver,
     not on it — `address_geocoding_assets.py:414-415`);
  2. the **resolver** (`address_resolution/`, policy `se-address-resolution-policy-v5`,
     golden-corpus gate + shadow + promotion) — the authoritative matcher;
  3. the **legacy per-company matcher** (`address_geocoding.py:95-585`) → the parity pair
     `se_company_address_geocodes` (1,194,146 rows) / `se_company_address_geocode_results`
     (3,768,377 rows), kept only as a baseline.
- The resolver refuses to decide ~19,413 companies' addresses (`ambiguous`) that the legacy
  matcher resolved `matched_exact` at confidence 1.0 on identical street text (measured on
  prod 2026-08-24). That signal is trapped in a pair of tables slated for retirement.
- `corpscout.se_company_addresses_canonical_current` (3,768,377 rows) is published to
  ClickHouse solely so six asset checks can read it (`address_geocoding_assets.py:668-1152`);
  no serving path reads it (the backoffice and the `se_company_address` final read other
  tables; verified during the address plan's Task 9).

Reference facts that shape the design:
- The reference dataset is **OpenStreetMap/Geofabrik** (`sweden_address_osm/`), not
  Lantmäteriet. Its snapshot identity `source_md5` (+ `source_snapshot_at` etc.) is already
  stamped on every geocode row (migration 000275; `shared_address_geocoding.py:38-42`).
- Lantmäteriet (`sweden_address_geocoding/`) is an ingestion-only, STOPPED subsystem awaiting
  Geotorget legal approval (`assets.py:40-50`); nothing consumes it.
- The matcher version (`policy_version`, `address_resolution_policy.py:8`) never reaches
  ClickHouse — a stored coordinate cannot be attributed to a matcher version today.

## 2. Goals

1. Matching cost proportional to change: an unchanged OSM snapshot + unchanged policy +
   no new addresses ⇒ (near-)zero matching work.
2. One matcher (the resolver). The join matcher and the legacy per-company matcher are gone.
3. The geocode store is permanent and versioned; outcomes carry `policy_version` and the
   reference snapshot identity; "current" is a read rule, not a rebuild artifact.
4. The 19,413 legacy-exact decisions are preserved as auditable `legacy_adopted` outcomes
   before the legacy pair retires.
5. `se_company_address`'s Monday scan sees only genuinely new/changed geocode outcomes.
6. Three ClickHouse tables retire: the legacy pair and the canonical publish.

## 3. Non-goals (explicit owner decisions)

- The identity chain (canonical → members → links → `se_addresses_current`) KEEPS its weekly
  whole-rebuild. It is bounded DuckDB + `EXCHANGE` work and carries the human
  review-carry-forward logic (`address_geocoding_assets.py:279-292`,
  `shared_addresses.py:191-199`). Incrementalizing it is a separate future project.
- Lantmäteriet stays parked. When Geotorget approval lands it enters as a second reference
  with its own version stamp; the store design below accommodates that without change.
- The resolver's matching semantics (policy v5 status taxonomy, score-margin ambiguity rule)
  are unchanged. This project changes WHEN matching runs, not HOW it decides.
- The DuckDB canonical build stays (the members bridge derives from it); only its ClickHouse
  publish retires.
- The public company page is out of scope (unchanged since the address plan).

## 4. Design

### 4.1 The versioned outcome store

New ClickHouse table `corpscout.se_address_geocodes` (append-only; the permanent
store), replacing the weekly-swapped `se_address_geocodes_current` as the source of truth:

- Columns: everything `se_address_geocodes_current` carries today (identity, match fields,
  coordinates, candidates, provenance — migration 000275 shape) PLUS:
  - `policy_version LowCardinality(String)` — e.g. `se-address-resolution-policy-v5`, or
    `legacy_adopted_v1` for the imported decisions;
  - `reference_md5 String` — the OSM `source_md5` the outcome was computed against
    (already per-row today as `source_md5`; promoted to a key role);
  - `matched_at DateTime64(3,'UTC')` — append time (real, per-outcome — no longer a
    run-wide constant re-stamped over unchanged rows).
- Engine: `ReplacingMergeTree(matched_at)` `ORDER BY (address_id, policy_version,
  reference_md5)`. One row per (identity, matcher, reference); recomputation with the same
  triple replaces (idempotent); a new policy or reference version appends beside the old.
- The read rule ("current outcome per identity"): newest `matched_at` per `address_id`
  across versions, with `legacy_adopted_v1` outranked by any same-or-newer resolver outcome
  that is GEOCODED (a resolver `ambiguous` does not beat an adopted exact — that is the
  point of the import). Implemented as a small versioned-read SQL fragment used by both
  consumers (the `se_company_address` final's geocode read and the checks); exact expression
  is a plan-time decision, pinned by tests.
- `se_address_geocodes_current` (CH) is retired at the end of the rollout (§6). During
  transition it is derived FROM the store (same stage+EXCHANGE publish, but computed by the
  versioned read) so downstream consumers migrate on their own schedule.

DuckDB side: the shadow/promotion tables continue to exist per run, but promotion now emits
an APPEND of new/changed outcomes to the store instead of `create or replace` of the whole
serving table (`address_resolution_promotion.py:67-74` is the code that changes).

### 4.2 Demand-driven matching

The weekly resolver run matches exactly:

1. **New identities**: `address_id`s present in `se_addresses_current` (this week's identity
   rebuild) with NO outcome in the store for the current `(policy_version, reference_md5)`
   — for a stable reference this is register churn (thousands), because unchanged addresses
   keep their fingerprint and already have an outcome.
2. **The retry pool** — outcomes whose status is in the non-geocoded set (`ambiguous`,
   `unmatched`, `invalid_address`, …) — ONLY when the reference changed (`source_md5` of the
   fresh OSM snapshot differs from the outcome's `reference_md5`) or `policy_version`
   bumped. Unchanged reference + unchanged policy ⇒ retry pool is skipped entirely.
3. **Nothing else.** A full re-match is an explicit operator action: bump `policy_version`
   (which routes everything through the golden gate) or run a `rematch_all`-style config on
   the matching asset (the same explicit-pass pattern as the finals' `resolve_all`).

Short-circuit: the OSM refresh asset already knows the Geofabrik MD5
(`sweden_address_osm/assets.py:105-112`). When the MD5 is unchanged AND no new identities
exist AND the policy version is unchanged, the matching stage logs the decision and does no
index build at all. The OSM candidate-index construction (the dominant cost) runs only when
there is something to match, and then ONCE (see 4.3).

The golden-corpus gate (`address_resolution_assets.py:40-51`) runs on every job that will
match anything — it is cheap and is the safety net for policy changes.

### 4.3 Matcher retirements

- **Join matcher** (`shared_address_geocoding.py` matching path +
  `sweden_shared_address_osm_matches_duckdb` asset): DELETED. Precondition (verify during
  implementation, stop-and-report if false): nothing reads its output table between its
  write and the promotion overwrite — including the shadow's comparison tables
  (`address_resolution_shadow.py` `..._comparison_shadow`) and diagnostics. Its invariant
  suite (`shared_address_geocoding.py:504-606`) is ported to the store's append path where
  still meaningful.
- **Legacy per-company matcher** (`address_geocoding.py`, its asset, and the publish pair):
  retired AFTER the one-time import (4.4). With both gone, the weekly job builds OSM
  candidate indexes at most once (resolver only, and only when matching work exists).

### 4.4 The legacy-adoption import (one-time, versioned, auditable)

A one-shot asset/script (run once, kept in the repo as the record):

- Selection rule: legacy `se_company_address_geocode_results` rows with
  `match_status = 'matched_exact'` AND `match_confidence = 1.0`, joined to the current
  store/current table where the resolver outcome for the same identity is NON-geocoded
  (`ambiguous`/`unmatched`). Join path: legacy is keyed `(company_id, canonical_address_key)`;
  map via `se_company_address_members_current` (canonical_address_key → address_id).
  Measured population 2026-08-24: ~19,413 companies; the identity-level count is a plan-time
  measurement.
- Written as store rows with `policy_version = 'legacy_adopted_v1'`,
  `match_method = 'legacy_adopted'`, the legacy coordinates/precision/confidence, the legacy
  OSM provenance columns, `matched_at` = import time.
- Auditable and reversible: `DELETE`-free retirement — adopting rows are distinguishable by
  version; a future resolver improvement that geocodes the same identity outranks them by
  the read rule; nothing is silently merged.
- AFTER the import lands and is verified (spot-check a sample against the legacy pair), the
  legacy pair + matcher + publish assets retire (migration drops the two tables with the
  address plan's gate discipline: fresh zero-reader rg + row-count snapshot in the migration
  comment; the six-reader lesson from Task 9 applies — grep for the qualified-constant
  indirection, not just literals).

### 4.5 Canonical ClickHouse publish retirement and the six checks

Per-check disposition (numbering = subsystem map §6):

| # | Check | Disposition |
|---|---|---|
| 1 | `sweden_company_canonical_addresses_complete_check` | Moves to the DuckDB side of the canonical build (asserts there, where the table lives); the CH half is deleted with the publish |
| 2 | `sweden_shared_addresses_complete_check` | The equivalent DuckDB assertion (`shared_addresses.py:287-347`) becomes the authority; the CH check narrows to shared-vs-links consistency (no canonical denominator) |
| 3 | `sweden_shared_address_geocodes_complete_check` | Unaffected today; REWRITTEN for the store: uniqueness per (address_id, policy_version, reference_md5), status allowlist, status↔coordinate/precision agreement, provenance completeness — same invariants, new grain |
| 4 | `sweden_shared_address_geocodes_baseline_check` | Retires with the legacy pair (its join key exists only on canonical, and its purpose — parity — ends) |
| 5 | `sweden_company_address_exact_match_rate_check` | Denominator switches to `se_company_address_links_current` count; numerator switches to the store's versioned read |
| 6 | `sweden_company_address_osm_snapshot_freshness_check` | `fetch_sweden_address_geocode_stats` splits so the freshness path reads only the store's `max(source_snapshot_at)`; the canonical query is deleted |

Then `sweden_company_canonical_addresses_clickhouse` stops publishing the canonical table
(members continue to publish — the final's geocode join needs them), and
`corpscout.se_company_addresses_canonical_current` is dropped by migration with the standard
gates. NOTE the shared blast radius: `fetch_sweden_address_geocode_stats` also feeds the
legacy publish asset's metadata (`address_geocoding_assets.py:561-566`) — that asset retires
in 4.4, ordering matters (§6).

### 4.6 Downstream: the `se_company_address` final

The final's geocode read (`build_geocodes_sql()` in `defs/se_company/address.py`) repoints
from `se_address_geocodes_current` to the store's versioned read. The `new_geocode`
change-scan term keys on the store's `matched_at` — which now moves ONLY for genuinely new
or changed outcomes — so the Monday scan selects register churn plus real geocode changes.
Ruling A17's uncapped weekly config remains as pure defense. The final's schema does not
change (`address_id`, `latitude`, `longitude`, `geocode_status`, `geocoded_at` are already
the distilled block; `geocoded_at` maps to the outcome's `matched_at`).

## 5. Versioning contract (the one new invariant)

Every stored outcome is attributable: `(address_id, policy_version, reference_md5,
matched_at)` fully determines provenance. Consequences the implementation must honor:

- Promotion NEVER writes an outcome without a real `policy_version` and the `reference_md5`
  of the snapshot it matched against.
- Re-running with identical (policy, reference) may replace rows (ReplacingMergeTree) but
  must be a no-op in content — pinned by an idempotency test in the harness.
- A policy bump without a golden-gate pass must be impossible in the job graph (the gate
  asset sits upstream of matching, as today).
- `matched_at` is append time per outcome — never a run-wide constant restamped over
  unchanged rows (the exact mistake 000300 fixed for SCB observations; same lesson).

## 6. Rollout order (each step verifiable, stoppable, owner-gated where marked)

1. **Store + write path**: migration creates `se_address_geocodes`; promotion
   appends to it (while STILL rebuilding `se_address_geocodes_current` unchanged — dual
   write). Backfill: one full append of the current serving table's rows stamped
   `policy_version = 'se-address-resolution-policy-v5'` + their existing `source_md5`.
2. **Demand-driven matching**: the resolver's input narrows to new-identities + retry-pool
   rules; the short-circuit lands. The join matcher is deleted (after its dead-output
   verification). `se_address_geocodes_current` is now derived from the store.
3. **Legacy adoption import** (owner-gated run), verification sample, then legacy
   matcher/pair retirement (code first, then the drop migration — Task 9/10e discipline).
4. **Final repoint**: `se_company_address` reads the store; observe one weekly cycle
   (expect: Monday address run shrinks to churn-sized selection).
5. **Canonical publish retirement**: checks relocated per 4.5, then the drop migration.
6. **Cleanup**: retire the transitional `se_address_geocodes_current` derivation once
   nothing reads the table (fresh zero-reader gate), drop it by migration.

Steps 3, 5, 6 carry drop migrations: each follows the address plan's gate discipline
(zero-reader proof incl. constant-indirection grep, row-count snapshot, UNDROP watch,
code-deploy-before-drop ordering).

## 7. Testing

- Pure rules (versioned read, retry-pool selection, adoption ranking): table-tested pure
  functions, mirroring `address_rules.py` discipline.
- Store DDL + append path: extend the clickhouse-local harness pattern
  (`tests/test_se_company_address_clickhouse_local.py`) — idempotent re-append, version
  outranking, both `join_use_nulls` settings, ReplacingMergeTree FINAL semantics.
- Demand-driven selection: fixture with unchanged-md5 (expect zero work), new identity,
  md5 bump (retry pool wakes), policy bump (everything routes through the golden gate).
- The import: fixture proving adoption only where resolver is non-geocoded and legacy is
  exact/1.0, and that a later resolver success outranks the adopted row.
- Checks: each relocated/rewritten check gets the mutation-probe treatment (this plan's
  reviews found vacuous tests repeatedly; assume reviewers will re-run mutations).
- Every reviewer executes SQL against Docker ClickHouse 26.5 (house rule).

## 8. Risks and mitigations

- **Dead-output assumption wrong** (join matcher secretly read): verification step in 4.3 is
  a hard gate — stop and report, do not delete.
- **Store growth**: append-only with versions; bounded — one row per (identity, policy,
  reference); policies and references change rarely. A TTL/compaction decision is deferred
  until there are ≥3 versions in the wild.
- **Read-rule subtlety** (adopted vs resolver ranking): centralized in one SQL fragment +
  one pure function, both pinned; consumers never inline their own ranking.
- **Transition drift** (store vs derived `_current`): a temporary parity check compares them
  during steps 2–5 and retires in step 6.
- **Weekly-job partial failure** now leaves the store partially appended: safe by
  idempotency (§5); the next run's demand scan picks up the remainder (same property the
  address artifacts' anti-join gives).

## 9. Open items for the plan (not blockers)

- Exact versioned-read SQL and its Python twin.
- Identity-level count of the adoption population (measured at plan time, recorded in the
  import's comment).
- Whether checks 1–2's DuckDB relocations assert inside the existing build functions or as
  separate assets (follow the codebase's nearest pattern).
- The `matched_at` tie-break when a replace and an append race within one week (plan-time
  decision, pinned by a test).
