# Contract-level rollup Implementation Plan

> **For agentic workers:** Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Make `/countries/br/contracts` load, by precomputing the contract-level
aggregation the page currently performs on every request.

**Architecture:** A new `company_contract_rollup` table holds one row per
`(country_code, contract_ref)` — exactly the shape the contracts list renders.
A Dagster asset builds it from the two existing winner-level facts tables,
running the aggregation SQL that today lives in `contracts.server.ts`. The list
and facet queries become filtered, ordered reads instead of nested aggregations.

**Tech Stack:** ClickHouse (golang-migrate), Dagster (`uv run dg`), React Router
backoffice (TypeScript, vitest).

## Why

`/countries/br/contracts` returns HTTP 500. The query takes 31s and 13.2 GiB.

The source is not the problem — it already reads the precomputed
`company_contract_award_facts`. The **query shape** is. The page builds a
two-level aggregation over every row in the country:

```
GROUP BY contract_ref, source_slug   -- ~12 any()/argMin()/max() aggregates
GROUP BY contract_ref                -- ~12 argMax(..., priority)
```

All of it completes before `LIMIT 50` applies. For Brazil the aggregation
collapses nothing:

```
BR   4,605,018 contracts from 4,605,018 winner rows   1:1
NO      26,124 contracts from     55,493 winner rows   2:1
```

So it builds 4.6M groups holding a dozen aggregate states over wide strings to
emit 4.6M rows. That is the 13.2 GiB. Norway genuinely halves and was never in
trouble.

Measured 2026-08-01. Every other contracts page is already fast (se 0.90s,
no 0.59s, ee 0.86s, fi 0.44s, lv 0.17s, sk 0.13s, fr 3.85s).

## Global Constraints

- **The migration owns the schema.** Assets assert tables exist
  (`assert_clickhouse_tables_exist`) and replace contents. Never issue DDL from
  Python. Pin column order in a `tables.py` tuple and add the migration name to
  `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py`.
- **No semicolons inside SQL comments** in migration files — the migration
  splitter breaks on them and a test enforces it.
- **Replace via staging + `EXCHANGE TABLES`**, never truncate-then-insert.
- **Refuse to replace on empty input** — raise `ValueError` below a row floor,
  so a broken upstream cannot blank a populated table.
- **Schedules ship `default_status=STOPPED`** and must use a unique
  `(minute, hour)` cron pair — `tests/test_schedule_cron_contracts.py` enforces
  it. Currently taken and unavailable: (0,5) (15,6) (20,4) (20,5) (45,7) and
  every minute already used at hour 3, 4 and 5. `50 4` is used by
  `company_contract_facts_daily`.
- **Commit by explicit path.** The working tree carries unrelated WIP; never
  `git add -A`.
- Validate with `uv run dg check defs` and `uv run pytest tests/` from
  `corpscout/services/dagster_v3`, and `npm run typecheck && npx vitest run`
  from `corpscout/services/backoffice`.
- Three dagster tests fail before you start and are **not yours**:
  `test_every_partitioned_asset_uses_multi_run_backfill_policy`,
  `test_production_has_only_the_explicit_ted_executemany_debt`,
  `test_every_schedule_fires_on_a_unique_minute_hour_pair`. Leave them failing;
  do not "fix" them.

## File Structure

- Create: `corpscout/clickhouse/migrations/000238_corpscout_company_contract_rollup.{up,down}.sql`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_contracts/tables.py`
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_contracts/rollup_sql.py`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_contracts/assets.py`
- Create: `corpscout/services/dagster_v3/tests/test_contract_rollup.py`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py`
- Modify: `corpscout/services/backoffice/app/lib/contracts.server.ts`
- Modify: `corpscout/services/backoffice/app/lib/contract-filters.ts`

## Background you need

Two winner-level tables already exist and are populated, both one row per
(contract, winner):

| table | grain | rows |
| --- | --- | --- |
| `company_contract_facts` | 26 cols + `contract_ref`, `resolved_at` | 5,526,274 |
| `company_contract_award_facts` | 29 cols + `contract_ref`, `resolved_at` | 4,660,511 |

The award shape adds `winner_registered_id`, `winner_match_status`,
`winner_country`. Countries using it: **br, no**. All others use the plain
shape. `contracts.server.ts` picks between them in `contractsSource()`.

`contract_ref` is already a stored column in both.

**Filters are all contract-level** — `agreement`, `cpv` (prefix), `amountMin/Max`,
`from`/`to` dates. None are winner-level, which is what makes a rollup viable.

`contractFilterSql` (`app/lib/contract-filters.ts`) binds them to WINNER-level
column names: `value_amount_usd` for the amount range (USD, not the original —
a country can mix currencies) and `publication_date` for the dates. On the
rollup those are `amount_usd` and `publication_date`. It already takes an
`agreementExpr` option for exactly this reason; give it an amount-column option
in the same shape rather than rewriting the clauses at each call site.

**Measured semantic risk.** Within one `contract_ref`, how many contracts have
differing values in a filterable column:

```
        multi_cpv  multi_agreement  multi_date  multi_amount   contracts
BR              0                0           0             0   4,605,018
NO              2                0           0         1,097      26,124
```

Brazil: zero — single-source and single-winner throughout, so the rollup is
exactly equivalent there.

Norway is the behaviour change, and it is **larger than the 1,097 above**. That
column counted contracts whose winner rows carry *differing* amounts. But the
amount filter moves from "some winner ROW is in range" to "the SUMMED amount the
page displays is in range", and a sum differs from a row whenever there is more
than one row — identical values included. Norway holds 55,493 winner rows over
26,124 contracts, so this touches every multi-winner contract, not 4.2% of them.

The new behaviour is the correct one: today the page can show an amount outside
the range you filtered on, because the displayed figure was already a sum while
the filter was not. Record the change in the commit message — with the "every
multi-winner contract" framing, not the 1,097 — and do not try to preserve the
old behaviour.

---

### Task 1: The rollup table

**Files:**
- Create: `corpscout/clickhouse/migrations/000238_corpscout_company_contract_rollup.{up,down}.sql`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_contracts/tables.py`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py`

**Interfaces produced:** `ROLLUP_TABLE`, `ROLLUP_COLUMNS`, `ROLLUP_TABLES`,
`MIN_ROLLUP_ROWS` in `company_contracts/tables.py`.

- [x] **Step 1: Write the migration.**

Columns, in this order. The first fourteen are what the page selects; the rest
are what it filters and sorts on.

```sql
CREATE TABLE IF NOT EXISTS corpscout.company_contract_rollup
(
    country_code LowCardinality(String),
    contract_ref String,
    contract_date String,
    buyer_name String,
    title String,
    agreement_type String,
    cpv_code String,
    winner_name String,
    winner_registered_id String,
    winner_match_status String,
    supplier_count UInt32,
    amount_original Nullable(Float64),
    currency String,
    amount_usd Nullable(Float64),
    source_url String,
    publication_date Nullable(Date),
    resolved_at DateTime
)
ENGINE = MergeTree
ORDER BY (country_code, contract_date, contract_ref);
```

`contract_date` is a String because the page's aggregation produces
`coalesce(toString(argMax(source_date, priority)), '')` and sorts on it as text;
keep that exactly. `publication_date` is carried separately as a real Date so the
`from`/`to` filters can use a range rather than string comparison.

Write a comment block explaining WHY (the 31s / 13.2 GiB / 1:1 measurements
above). No semicolons inside it.

Down migration: `DROP TABLE IF EXISTS corpscout.company_contract_rollup;`

- [x] **Step 2: Add the contract to `tables.py`**, mirroring the existing
  `CONTRACT_FACTS_COLUMNS` block. `MIN_ROLLUP_ROWS = 500_000`.

- [x] **Step 3: Register the migration** by appending
  `"000238_corpscout_company_contract_rollup"` to `EXPECTED_MIGRATIONS`.

- [x] **Step 4: Apply and verify.**

```bash
cd corpscout && make clickhouse-migrate-up-one
cd services/dagster_v3 && uv run pytest tests/test_clickhouse_migrations.py -q
```
Expected: migration 238 applies, tests pass.

- [x] **Step 5: Commit.**

---

### Task 2: Move the aggregation SQL

**Files:**
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_contracts/rollup_sql.py`
- Create: `corpscout/services/dagster_v3/tests/test_contract_rollup.py`

**Interfaces produced:**
`build_rollup_select(*, source_table: str, has_supplier_detail: bool) -> str`

This is the heart of the task. The SQL below is copied verbatim from
`contracts.server.ts` (the `getCountryContractsPage` list query) with the
template holes filled. **Move it, do not paraphrase it** — every comment in it
records a decision that was got wrong once.

Substitutions to make when moving:
- `${REF}` → `contract_ref` (already a stored column in both facts tables)
- `${AGREEMENT_EXPR}` → `multiIf(startsWith(agreement_type, '{'), JSONExtractString(agreement_type, 'nome'), agreement_type)`
- `${supplierIdExpr}` → `winner_registered_id` when `has_supplier_detail`, else `''`
- `${supplierStatusExpr}` → `winner_match_status` when `has_supplier_detail`,
  else **`'exact'`** — a literal string, not a blank. Check
  `contracts.server.ts` before writing this one: a source with no supplier
  detail has nothing to disagree with, which the page renders as an exact match.
  Blanking it changes what six of the eight countries display.
- `${filterClause}` → **nothing.** Filters no longer apply here; they move to
  the read side. This is the whole point.
- The outer `ORDER BY` / `LIMIT` → **drop them.** The asset writes every row.

Three changes are NOT verbatim, each because the query's outer projection did
the work and there is no outer projection any more:

- `max(priority) AS amount_usd` → **`nullIf(max(priority), -1.0)`**. The page
  unwraps the sentinel one level up (`nullIf(amount_usd, -1.0)`), and a stored
  `-1.0` would leak into both the filter and the sort: `amount_usd <= X` is TRUE
  for `-1.0`, so every contract with no USD figure would join every max-amount
  filter, and `ORDER BY amount_usd ASC` would put them first instead of last.
  The column is already `Nullable(Float64)`. The sentinel stays in the inner
  query, where it is still doing its job.
- `argMax(winner_count, priority) AS winner_count_primary` →
  **`toUInt32(argMax(winner_count, priority)) AS supplier_count`**. The page
  casts it (`toUInt32(winner_count_primary) AS supplier_count`); the column is
  `UInt32` and `uniqExact` returns `UInt64`.
- `max(publication_date_in)` → **`argMax(publication_date_in, priority)`**.
  See the note below the SQL.

The outer SELECT list is also **reordered to match `ROLLUP_COLUMNS`** —
`source_url` moves from seventh to just before `publication_date`. The INSERT
names its columns, so the two lists must agree positionally; keeping the SELECT
in table order is what makes that checkable by eye.

```sql
SELECT
    contract_ref,
    coalesce(toString(argMax(source_date, priority)), '') AS contract_date,
    argMax(buyer_name_in, priority) AS buyer_name,
    argMax(title_in, priority) AS title,
    argMax(agreement_type_in, priority) AS agreement_type,
    argMax(cpv_code_in, priority) AS cpv_code,
    argMax(winner_name_in, priority) AS winner_name,
    argMax(winner_registered_id_in, priority) AS winner_registered_id,
    argMax(winner_match_status_in, priority) AS winner_match_status,
    toUInt32(argMax(winner_count, priority)) AS supplier_count,
    argMax(amount_original_in, priority) AS amount_original,
    argMax(currency_in, priority) AS currency,
    nullIf(max(priority), -1.0) AS amount_usd,
    argMax(source_url_in, priority) AS source_url,
    argMax(publication_date_in, priority) AS publication_date
FROM (
    SELECT
        contract_ref,
        source_slug AS source,
        max(publication_date) AS source_date,
        max(publication_date) AS publication_date_in,
        any(buyer_name) AS buyer_name_in,
        any(title) AS title_in,
        -- PNCP (Brazil) publishes agreement_type as a raw {"id":N,"nome":"..."}
        -- blob -- every other loaded source publishes plain text.
        any(<AGREEMENT_EXPR>) AS agreement_type_in,
        -- max(), not any(): a contract's rows within one source can carry CPV
        -- on some lots and '' on others, and any() would pick the blank often
        -- enough to look like the register publishes nothing.
        max(cpv_code) AS cpv_code_in,
        any(source_url) AS source_url_in,
        -- Alphabetically first, not source_winner_ordinal: that ordinal is just
        -- the order our parser happened to iterate the notice XML, so it encodes
        -- no rank -- arbitrary but stable, which looks meaningful and is not.
        -- argMin over the same expression min() uses, so the name, its id and
        -- its status all describe ONE supplier rather than three different ones.
        min(if(winner_name != '', winner_name, company_id)) AS winner_name_in,
        argMin(<SUPPLIER_ID>, if(winner_name != '', winner_name, company_id))
            AS winner_registered_id_in,
        argMin(<SUPPLIER_STATUS>, if(winner_name != '', winner_name, company_id))
            AS winner_match_status_in,
        uniqExact(if(company_id != '', company_id, winner_name)) AS winner_count,
        sum(value_amount_original) AS amount_original_in,
        any(value_currency) AS currency_in,
        -- -1 sentinel distinguishes "no source reported a USD figure" from a
        -- genuine zero once max() below collapses the per-source values.
        coalesce(toFloat64(sum(value_amount_usd)), -1.0) AS priority
    FROM <SOURCE_TABLE>
    GROUP BY contract_ref, source
)
GROUP BY contract_ref
```

Note `publication_date_in` is an addition — the page did not need a real Date,
the rollup does, for the `from`/`to` filters.

**`argMax(..., priority)`, not `max()`.** `contract_date` is
`argMax(source_date, priority)`, so it comes from the one source with the
largest USD amount. A plain `max()` over every source would let a multi-source
contract be FILTERED on one date and DISPLAY another — the same class of bug as
filtering the raw `agreement_type` while displaying the extracted one. Sharing
`priority` makes the filtered date and the displayed date the same date by
construction. Identical either way for Brazil, which is single-source.

- [x] **Step 1: Write the failing test** in `tests/test_contract_rollup.py`:

Assert the substituted EXPRESSION, not the absence of a substring — a test that
greps for a column name passes for the wrong reason the moment an alias happens
to contain it.

```python
import re

from dagster_v3.defs.company_contracts import tables
from dagster_v3.defs.company_contracts.rollup_sql import build_rollup_select


def test_supplier_columns_are_bound_only_where_the_source_has_them() -> None:
    """The plain facts table has no winner_registered_id -- selecting it would
    fail at query time for six of the eight countries."""
    plain = build_rollup_select(source_table="t", has_supplier_detail=False)
    awards = build_rollup_select(source_table="t", has_supplier_detail=True)
    assert "argMin('', if(" in plain
    # 'exact', NOT '': a source that publishes no match status has nothing to
    # disagree with, and the page renders that as an exact match. Blanking it
    # changes what six countries display.
    assert "argMin('exact', if(" in plain
    assert "argMin(winner_registered_id, if(" in awards
    assert "argMin(winner_match_status, if(" in awards


def test_carries_no_filter_or_limit() -> None:
    """Filters move to the read side and the asset writes every row -- a LIMIT
    here would silently truncate the table.

    Narrower than it reads: the per-country predicate is wrapped around the
    source table in assets.py, outside this function, so this proves the
    aggregation carries no filter of its OWN.
    """
    sql = build_rollup_select(source_table="t", has_supplier_detail=True)
    assert "LIMIT" not in sql.upper()
    assert " WHERE " not in sql.upper()


def test_unwraps_the_usd_sentinel_before_storing_it() -> None:
    """-1.0 distinguishes 'no source reported USD' from a genuine zero while
    max() collapses the per-source values. Stored, it stops being a sentinel and
    starts being a number: `amount_usd <= X` is true for it, so every contract
    without a USD figure would join every max-amount filter, and an ascending
    sort would lead with them instead of trailing.
    """
    sql = build_rollup_select(source_table="t", has_supplier_detail=True)
    assert "-1.0" in sql
    assert "nullIf(max(priority), -1.0)" in sql


def test_projects_columns_in_table_order() -> None:
    """The INSERT names its columns, so a SELECT in a different order writes
    source_url into publication_date -- and ClickHouse accepts it."""
    sql = build_rollup_select(source_table="t", has_supplier_detail=True)
    outer = sql.split("FROM")[0]
    expected = [
        column
        for column in tables.ROLLUP_COLUMNS
        # Literals the asset supplies at their own positions.
        if column not in ("country_code", "resolved_at")
    ]
    assert ["contract_ref", *re.findall(r"AS (\w+)", outer)] == expected
```

- [x] **Step 2: Run it and watch it fail** —
  `uv run pytest tests/test_contract_rollup.py -q`. Expected: ModuleNotFoundError.
- [x] **Step 3: Write `rollup_sql.py`** with the SQL above.
- [x] **Step 4: Run the tests to green.**
- [x] **Step 5: Commit.**

---

### Task 3: The asset

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_contracts/assets.py`

**Interfaces consumed:** `build_rollup_select` (Task 2), `ROLLUP_*` (Task 1).

Follow `company_contract_facts` in the same file exactly — it is the template:
staging table, per-country insert, floor check, `EXCHANGE TABLES`, `DROP` in
`finally`, per-country row counts in the log and in `MaterializeResult`.

- [x] **Step 1: Add `company_contract_rollup`.** For each country, pick the
  source by shape:

```python
for code in tables.CONTRACT_COUNTRIES:          # br, ee, fi, fr, lv, no, se, sk
    awards = code in tables.AWARD_COUNTRIES     # br, no
    source = (
        f"{RESOLVED_DATABASE}.{tables.AWARD_FACTS_TABLE}"
        if awards
        else f"{RESOLVED_DATABASE}.{tables.CONTRACT_FACTS_TABLE}"
    )
```

The source must be filtered to the country. Wrap it:
`(SELECT * FROM {source} WHERE country_code = '{code.upper()}')`.

`country_code` and `resolved_at` are not produced by the SELECT — add them to
the INSERT column list and select them as literals at their own positions, as
`facts_insert_sql` already does. Everything between them comes from
`build_rollup_select` in `ROLLUP_COLUMNS` order.

`MIN_ROLLUP_ROWS` is a floor on the **total across all countries**, checked once
after the loop — the same shape `company_contract_facts` uses. Per country it
would be nonsense: 500,000 is below Brazil and above every other country.

- [x] **Step 2: Add it to the existing job**, after both facts assets, so the
  grains cannot disagree. Add a `deps=[...]` on both facts asset keys.
- [x] **Step 3: `uv run dg check defs`.** Expected: loads successfully.
- [x] **Step 4: Materialize and verify equality against the live page query.**

```bash
uv run dg launch --assets company_contract_facts,company_contract_award_facts,company_contract_rollup
```

Then verify, for at least `br`, `no` and `se`, that the rollup agrees with what
the page computes today. Expected row counts: `br` 4,605,018 contracts,
`no` 26,124, `se` ~24,462 (a `_summary` view already reports Sweden's).

- [x] **Step 5: Commit.**

---

### Task 4: Read from the rollup

**Files:**
- Modify: `corpscout/services/backoffice/app/lib/contracts.server.ts`
- Modify: `corpscout/services/backoffice/app/lib/contract-filters.ts`

The read side is exactly four functions: `getCountryContractsPage`,
`getAgreementTypeFacet`, `getCpvChildren` and `getContractHeadlineStats`. There
is **no `buyer_name` or `winner_name` facet** — don't go looking for one. The
buyer and winner appear only as the headline's top-1, inside the fourth.

- [x] **Step 1: Teach `contractFilterSql` the rollup's column names.** It binds
  `value_amount_usd`; the rollup calls that `amount_usd`. Add an option beside
  the existing `agreementExpr` one — that option exists for precisely this
  reason and the comment above it explains why filtering a column the page does
  not display is a bug that looks correct.

  Against the rollup, **stop passing `agreementExpr: AGREEMENT_EXPR`.** The
  rollup stores the already-extracted value, so the JSON extraction would run
  over `Empenho` and find nothing to extract. Harmless today; a trap for the
  next reader. Filter the bare column and say so in a comment.

  `publication_date` and `cpv_code` keep their names, so `CPV_CODES` and the
  date clauses move unchanged.

- [x] **Step 2: Replace the list query** with a read of
  `company_contract_rollup` filtered by `country_code`, with the filter clause,
  `ORDER BY` and `LIMIT/OFFSET` applied directly. The count query above it
  (`SELECT count() FROM (SELECT ... GROUP BY contract_ref)`) collapses to a
  plain `count()` over the same filter — it is an aggregation too, and
  self-review item 1 means it.

  Drop `nullIf(amount_usd, -1.0)` and `toUInt32(winner_count_primary)` from the
  projection: the asset now stores both in their final form.

- [x] **Step 3: Move the facets with the filter — this is correctness, not
  speed.** `getAgreementTypeFacet` and `getCpvChildren` count contracts so that
  "the number beside a value matches what selecting it will show" (their own
  doc comments). Once the FILTER runs against the rollup's single
  `agreement_type` / `cpv_code`, a facet still counting over winner rows
  reports counts that selecting them cannot reproduce. They have to move.

  Moving them narrows what they see, and that is the intended trade: a
  multi-source contract now contributes only the CPV code and agreement type of
  its highest-USD source, so a code published solely by a lower-priority source
  leaves the tree. It also leaves the filter, which is why the counts still
  agree. `uniqExact(t.contract_ref)` becomes `count()`.

- [x] **Step 4: Leave `getContractHeadlineStats` on the facts tables.** Nothing
  measured says it is slow, and it does not agree with the rollup:

  - it takes `min(publication_date)` per contract where the rollup carries the
    highest-USD source's — repointing moves contracts between years in the
    histogram;
  - its top winner is a bare `min(winner_name)` where the rollup uses
    `min(if(winner_name != '', winner_name, company_id))` under an `argMax`. A
    contract with a blank winner name is dropped from the ranking today
    (`WHERE winner_name != ''`) and would rank under a company id from the
    rollup — and for a multi-source contract the two pick different winners
    outright.

  Both are defensible; neither is free. If a later change wants the rollup
  here, make it a deliberate step with its own before/after counts.

- [x] **Step 5:** Leave the DETAIL page and supplier lists on the winner-level
  facts tables. They need the per-winner grain, which the rollup does not have.
- [x] **Step 6:** `npm run typecheck && npx vitest run`. Expected: all pass. The
  Brazil test in `tests/country-contracts.queries.test.ts` currently exceeds its
  20s timeout — it should now pass comfortably. **Do not raise that timeout**;
  it has already been raised twice and is the signal this work exists to fix.
- [x] **Step 7: Measure every contracts page** and record the numbers:

```bash
for c in br no se fr ee fi lv sk; do
  printf "%s " $c
  curl -s -o /dev/null -w "%{http_code} %{time_total}s\n" \
    "http://localhost:5187/countries/$c/contracts"
done
```

Expected: `br` returns 200 in well under a second, having been a 500 at 32s.
Everything else stays at or improves on: se 0.90s, no 0.59s, ee 0.86s, fi 0.44s,
lv 0.17s, sk 0.13s, fr 3.85s.

Then measure Brazil **off** the default sort, which the loop above never leaves:

```bash
for q in "sort=buyer&dir=asc" "sort=amount_usd&dir=desc" "year=2025"; do
  printf "%s " "$q"
  curl -s -o /dev/null -w "%{http_code} %{time_total}s\n" \
    "http://localhost:5187/countries/br/contracts?$q"
done
```

The table's `ORDER BY (country_code, contract_date, contract_ref)` serves the
default page from the sort key. Any other sort reads and sorts all 4.6M rows —
still far cheaper than the aggregation it replaces (no aggregate states, narrow
columns), but it is the one remaining full scan and it should be measured rather
than assumed. If it is slow, the fix is a projection, not a wider sort key.

- [x] **Step 8: Commit**, recording the before/after numbers and the Norway
  filter-semantics change from the Background section.

---

## Self-Review

Before finishing, confirm:

1. The aggregation SQL exists in exactly ONE place. If `contracts.server.ts`
   still builds a nested aggregation for the list — its count query included —
   the move is incomplete and the two will drift.
2. `br` returns 200. That is the whole point.
3. The three pre-existing dagster failures are still exactly three.
4. No `LIMIT` reached the rollup asset.
5. No `-1.0` is stored. `SELECT count() FROM corpscout.company_contract_rollup
   WHERE amount_usd = -1.0` returns 0, and the USD-less contracts are NULL.
6. Nothing filters on a column it does not display: the agreement filter runs on
   the stored (extracted) value, the amount filter on `amount_usd`, and the
   date filter on the same `publication_date` the displayed `contract_date` was
   picked from.
7. Every facet count still equals the row count of selecting that facet. Pick
   one CPV node and one agreement value per country and check both numbers.
