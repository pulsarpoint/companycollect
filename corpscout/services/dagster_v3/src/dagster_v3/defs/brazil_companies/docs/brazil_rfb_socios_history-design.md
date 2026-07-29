# Brazil RFB Socios: connection history design doc

**Status:** built (branch `brazil-connection-history`, 2026-07-29). Migrations
`000210`/`000211` are applied on disk; the merge, guards and ClickHouse export
described below are implemented in `history.py` and `clickhouse.py`.

**What this adds:** `br_company_relations` stops being a snapshot of who is
connected to whom *now*, and becomes one row per **spell** of a connection —
company X to entity Y in role R, from a start date to an end date, with `NULL`
meaning still current.

Read alongside `brazil_rfb_socios-design.md`, which designed the edge table this
changes. That doc's §6 (privacy) is amended here: **retention moves from 90 days
to one year**, for a reason that did not exist when 90 was chosen.

## 0. Why

RFB republishes the whole register monthly and the pipeline replaces the serving
table each time (`truncate=True`). So the ninth run destroys what the eighth
learned. The user's objection, 2026-07-29: *"there is 0 reason that we just
ignore that if we already have data and we can show these informations."*

Correct. Ownership and control changing over time is the interesting signal, and
a full snapshot discarded is a month of history gone permanently — RFB's mirror
eventually drops it, so it cannot be recovered later.

**Scope is `br_company_relations` only.** The register tables (`br_companies`
68.6M, `br_establishments` 71.9M) stay current-state: they are ~140M rows a
month that barely move, they carry no `snapshot_year_month`, and company-status
history is a separate and larger decision. One table means "how the graph
changed"; the rest mean "what is true now."

## 1. Grain — one row per spell

Key: **`(cnpj_basico, related_entity_kind, related_tax_id, relation_code,
relation_since_key)`**.

`relation_code` is *in* the key, deliberately. When a partner becomes an
administrator, the old row closes and a new one opens. Going from shareholder to
administrator is precisely the control shift this table exists to show; holding
it in a mutable column would hide it. A `GROUP BY` on the first four columns
recovers the merged "how long has this person been involved at all" view;
nothing recovers the change if we collapse it.

### How a returning partner is detected — and why it is not by absence

RFB **never publishes a departure**. A partner who leaves simply stops
appearing. So "she left in March" is not in the source; it is inferred from
absence between two snapshots we happened to take.

But a *return* is in the source. `data_entrada_sociedade` is the date that
partner entered the company, so someone who leaves and rejoins carries a **new
entry date** — 2026-09, not 2019-03. That is a fact present in a single
snapshot, independent of whether we observed the gap.

Hence `relation_since_key` in the key:

```
same parties, role AND entry date   -> same spell, extend last_seen
same parties and role, NEW entry    -> genuine re-entry, new row
absent from the snapshot            -> close the open row
```

Spells are therefore defined by **what the source says**, not by our observation
luck. `relation_since_key` is the entry date as a non-nullable `String` (`''`
when RFB omits it) because `ORDER BY` cannot contain `Nullable`; the typed
`relation_since Date32` sits alongside it.

## 2. The two dates have different precision, and that must not be averaged

- **`start_at` is authoritative.** It comes from RFB's own
  `data_entrada_sociedade`, so a relationship first *observed* in 2026-06 can
  correctly report a true start of 2019.
- **`end_at` is bounded by cadence.** The source never says when a relationship
  ended, so `end_at` means **"gone by this snapshot"** — never "left on this
  date."

A row therefore carries one exact date and one approximate one. Any reader
treating them as equally precise will be wrong.

**This is the strongest argument for scheduling the RFB job.** `end_at`
precision *is* the run cadence: monthly runs give month-precision departures,
four runs a year give quarter-precision at best. The job is manual-only today
(deliberately — the user's call, 2026-07-29: *"we will create scheduler when
this system become more stable, this is still development version"*), which is
fine, and the cost lands here.

## 3. Schema

```
br_company_relations
  country_iso2, source_slug
  cnpj_basico            String                  -- subject of the edge
  related_entity_kind    LowCardinality(String)  -- '1' company | '2' person | '3' foreign
  related_tax_id         String                  -- CNPJ, or MASKED CPF
  relation_code          LowCardinality(String)  -- in the key: role change = new row
  relation_since_key     String                  -- entry date as text, '' when absent
  ---- attributes, from the most recent observation ----
  related_name           String
  related_country        String
  age_band               LowCardinality(String)
  representative_tax_id  String
  representative_name    String
  representative_code    LowCardinality(String)
  relation_since         Nullable(Date32)        -- typed twin of relation_since_key
  ---- the history ----
  first_seen_snapshot    LowCardinality(String)  -- 'YYYY-MM' first observed
  last_seen_snapshot     LowCardinality(String)  -- most recent observation
  start_at               Nullable(Date32)        -- authoritative, from the source
  end_at                 Nullable(Date32)        -- NULL while current; "gone by" semantics
  is_current             UInt8
  observations           UInt32                  -- snapshots this spell appeared in
  resolved_at            DateTime64(3, 'UTC')

ENGINE = MergeTree
ORDER BY (cnpj_basico, related_entity_kind, related_tax_id, relation_code,
          relation_since_key)
```

Every sort-key member is non-nullable (`allow_nullable_key` is off). Company
first, because "who is connected to this company" is the primary access path.

## 4. The merge

Each run rebuilds `br_company_relations` from **OPEN state only** plus the new
snapshot into a stage table, then `EXCHANGE TABLES` swaps it in for the
published table.

**Atomic, but NOT idempotent.** `EXCHANGE TABLES` is a single metadata swap —
no reader ever sees a half-built table — so "atomic" holds. "Idempotent" does
not: re-running the same month a second time does not reproduce the same
output. Every already-open spell's `observations` bumps again,
`last_seen_snapshot` re-advances, and a second ledger row gets written for a
month that was already recorded. This is exactly why the "already merged"
guard in §5 exists — it is not redundant belt-and-suspenders, it is the only
thing standing between a retried run and a silently double-counted history.
A maintainer who reads "idempotent" here and concludes that guard is
unnecessary would be wrong in a way that is easy to act on and hard to notice.

```
in both OPEN state and snapshot -> extend: keep first_seen and start_at,
                                last_seen = new snapshot, observations + 1,
                                attributes refreshed, end_at stays NULL
OPEN state only (absent now) -> close: is_current = 0,
                                end_at = the snapshot month's reference date
                                (meaning "gone by", see section 2)
only in the snapshot         -> open: first_seen = last_seen = new snapshot,
                                start_at = relation_since, end_at = NULL
already CLOSED state         -> untouched, column for column; never re-enters
                                the join (see below)
```

**The join is against OPEN state only** (`is_current = 1`); already-closed
spells bypass the join entirely and are unioned back in unchanged. This is
load-bearing, not an optimization — the code says so directly
(`history.build_merge_select_sql`'s docstring): if a closed spell re-entered
the join, a reappearing key would mutate the closed row in place instead of
the snapshot-only arm opening a new spell, and the edge would be silently
lost from current state.

**A re-entry is not the only way a reappearance can share a key with a closed
spell**, and the merge deliberately handles both:

- *Genuine re-entry* — RFB stamped a NEW `data_entrada_sociedade` —
  `relation_since_key` differs, so it lands in the snapshot-only arm as a new
  spell under a different key. Straightforward.
- *Same-key reappearance* — a partner missing from one month's snapshot for
  an unrelated reason (a missing socios part, an observed gap) who comes back
  with the **same** `relation_since_key`. This is not a re-entry by RFB's own
  data, but the closed row must stay closed and untouched while the
  reappearance opens a *new* spell sharing that key. `closed_state`
  partitions on `SPELL_KEY` plus `first_seen_snapshot` and `end_at` — not on
  `SPELL_KEY` alone — precisely so two closed spells sharing a key both
  survive as distinct rows (`test_two_distinct_closed_spells_sharing_a_key_...`
  pins this). Collapsing that grouping down to `SPELL_KEY` alone would
  silently merge those distinct historical spells into one.

**`join_use_nulls: 1` is mandatory, not tuning, and the code calls it "the
entire defence."** ClickHouse defaults to `join_use_nulls=0`, which fills the
unmatched side of the `FULL JOIN` with type defaults instead of `NULL` —
verified on a real ClickHouse 26.5 run to NOT error, and to blank the entire
spell identity (`country_iso2`, `source_slug`, `cnpj_basico`,
`related_entity_kind`, `related_tax_id`, `relation_code`,
`relation_since_key` — every column the spell is keyed by, not a couple of
incidental fields) on the first merge, collapsing every produced row onto one
identity. A bare `SELECT` cannot carry its own `SETTINGS` clause, so the
setting travels with the `INSERT` in `clickhouse.py`, which is the only place
it can be enforced — see `history.build_merge_select_sql`'s docstring for the
full failure mode.

**The ledger row is written BEFORE `EXCHANGE TABLES`, not after** —
deliberately the opposite of "record what you published." Proven on a real
ClickHouse by forcing the ledger insert to raise with the OLD ordering
(EXCHANGE first): the month was published (new spells present,
`last_seen_snapshot` advanced) but the ledger still listed only the prior
month, with the pre-merge copy already dropped and no rollback path —
published-but-unrecorded is invisible and permanent. Writing the ledger first
inverts the failure into a strictly better one: if `EXCHANGE` then fails, the
month is recorded but NOT published, which the next run's "already merged"
guard refuses loudly (§5) instead of silently corrupting history. One
consequence: **§8 step 1's assumption that the ledger and the published table
agree is not an invariant this implementation guarantees** — see §8.

Row count is ~distinct spells ever seen rather than months x edges — smaller
than retaining per-month observations, which is the next section's point.

## 5. Guards

There are **three** guards, not one, all run before the merge writes
anything, in this order:

1. **`assert_snapshot_is_newer`** — refuses an out-of-order `snapshot_year_month`
   against the ledger's merged months, and doubles as the "already merged"
   check: re-running a month already in the ledger is refused loudly,
   specifically because the merge is NOT idempotent (§4) — silently
   re-running it would double-bump `observations`. Its error message also
   covers the case where "already merged" means *recorded but not
   published* (§4's ledger-before-`EXCHANGE` ordering) and names the
   recovery: delete that ledger row, then re-run.
2. **`assert_snapshot_edge_count_is_plausible`** — refuses a snapshot whose
   edge count drops below `MIN_SNAPSHOT_EDGE_RATIO` (50%) of the previous
   merged month's. A coarse floor against a large-scale, mechanical failure;
   a no-op on the first-ever merge (nothing to compare against yet).
3. **`assert_snapshot_part_count_is_not_decreasing`** — compares this month's
   exact socios ZIP part count (from the snapshot manifest) against the
   previous merged month's, AND separately refuses any snapshot below
   `EXPECTED_SOCIOS_PART_COUNT` (10, RFB's measured `Socios0.zip`–
   `Socios9.zip`) regardless of whether a previous month exists. The ratio
   guard above cannot catch a single missing part — ~10% of ~10 parts is
   comfortably inside its 50% floor — and the previous-month comparison
   alone cannot guard the FIRST-ever merge, which is production's exact
   state today: 0 rows, an empty ledger. The absolute floor closes that gap.

**A ledger table makes all three guards possible.**
`corpscout.br_company_relations_snapshots` (created by migration `000210`,
extended by `000211`) is one row per merged month:

```
snapshot_year_month   LowCardinality(String)  -- 'YYYY-MM', the merge key
merged_at             DateTime64(3, 'UTC')
source_run_id         String
edges_in_snapshot     UInt64
spells_opened         UInt64
spells_closed         UInt64
spells_total          UInt64
socios_part_count     UInt32   -- added by 000211, DEFAULT 0 (existing rows)
```

Without it, none of the three guards above has anything to compare against, a
rebuild leaves no way to tell what the history is made of, and a gap is
invisible rather than detectable.

Considered and rejected: rebuilding from every S3 archive on every run. It is
self-healing and order-independent, but pays ~20-25M rows x up to 12 months of
reprocessing *every run, forever*, to insure against something that is rare once
runs are ordered — and the recovery it automates is available manually anyway
(§8). The user's objection settled it: *"why we need 2. option. If only reason
is irregular runs, we can add scheduler."*

The assertions do more than refuse: they make a skipped or out-of-order month
**visible**. Silently absorbing a late month would hide that the cadence had
slipped, and cadence is what `end_at` precision depends on.

## 6. No per-month observation table in ClickHouse

The alternative was keeping one row per edge per snapshot alongside the SCD2
table, so the history stays re-derivable. Rejected: the **S3 archives already
are** the observation record, and a second copy in ClickHouse buys nothing the
archives do not — at ~20-25M rows a month retained.

This is why retention changes.

## 7. Retention: one year, not 90 days — amends the socios design doc §6

`brazil_rfb_socios-design.md` §6 set socios raw archives to expire after **90
days**, chosen for "we keep the source file so ingest can be reprocessed."

That reason is now too narrow. The archives are the **only** rebuild path for
the connection history: SCD2 is derived state, and if the merge is wrong, the
archives are what a correct history gets re-derived from. Ninety days would make
the history unverifiable after a quarter.

**`RFB_SOCIOS_RETENTION_DAYS` moves from 90 to 365.** The other nine families
stay indefinite; they carry no personal data.

Consequences, stated rather than discovered:

- **One year is a hard limit, not a soft one.** RFB drops old snapshots, so
  "re-fetch from source" past the window is not actually available. Within a
  year the history is rebuildable; past it, the SCD2 table is the only record.
- **The decision stays reversible until roughly one year after the first
  materialization** — nothing can expire before then, so extending to indefinite
  costs nothing if taken in time. What closes the window is inattention: once
  archives begin crossing 365 days, extending does not bring back what already
  went. **Write the date down when the first run happens.**
- It is a longer personal-data window than 90 days, still bounded and still
  purpose-linked: *we retain the source for one year so relationship history can
  be rebuilt.*

The §6 non-goal is unchanged and still governs: no de-anonymization, ever.

## 8. Recovery procedure — write it down or it is folklore

Since §5 chose the cheap merge over the self-healing one, the recovery it gave
up must be executable by someone who was not in this conversation:

1. Read the ledger (`corpscout.br_company_relations_snapshots`, §5) to see
   which months are recorded — but see the caveat below before trusting that
   "recorded" means "published."
2. **Clear BOTH tables, not just the history table.** Run
   `000210_corpscout_br_company_relations_history.down.sql`, which drops
   `br_company_relations` **and** `br_company_relations_snapshots`, then
   re-apply `000210...up.sql` (and `000211...up.sql`, if it has run) to
   recreate both empty. Re-running only the `.up.sql` migration is **not**
   enough: its ledger statement is `CREATE TABLE IF NOT EXISTS`, so an
   existing ledger survives untouched even though the history table was just
   dropped and recreated — and every replayed month in step 3 then fails
   `assert_snapshot_is_newer`'s "already merged" guard against the ledger
   rows that never went away. This is not hypothetical: it is the same
   `IF NOT EXISTS` behaviour production relies on every month to avoid
   rewriting a table with real data in it, applied here to a table you
   actually needed dropped.
3. Re-derive by materializing the chain for each archived month **in ascending
   date order**, oldest first. Each run merges one snapshot; the ordering guard
   enforces the sequence, and now has an empty ledger to actually enforce it
   against.
4. Only months still in S3 can be replayed — one year, per §7. Anything older is
   gone and the rebuilt history starts from the oldest surviving archive.

**Caveat on step 1:** it assumes the ledger and the published table agree.
§4 records why that is not guaranteed — the ledger is written before
`EXCHANGE TABLES`, so a month can be recorded without ever being published.
If you suspect that state (a run failed after "merged" but before the next
scheduled run), don't trust step 1's read at face value: compare
`br_company_relations`'s actual row count against the ledger's
`spells_total` for the newest recorded month first. The export itself now
does this same comparison automatically right after every `EXCHANGE TABLES`
(see `clickhouse.py`), so a fresh divergence surfaces immediately rather than
waiting for someone to run this recovery procedure and be misled by step 1.

## 9. What this does not do

- **No back-history.** Only one partition has ever been materialized
  (`2026-06`), the S3 bucket does not exist yet, and RFB has likely already
  dropped 2026-04 and 05. History accrues from the next run forward and cannot
  be backfilled.
- **No register history.** `br_companies` and `br_establishments` stay
  current-state (§0).
- **No departure dates.** Only "gone by" dates (§2).

## 10. Things not to rediscover

```
RFB publishes no departure   a partner who leaves simply stops appearing; absence
                             between two snapshots is the only signal
returns ARE in the source    data_entrada_sociedade carries a NEW entry date on
                             re-entry, so a second spell is detectable from one
                             snapshot -- it does not depend on catching the gap
start_at vs end_at           start_at authoritative (from source), end_at bounded
                             by run cadence. Do not treat them alike
truncate=True                the current export replaces the table; that is the
                             single line this design removes
snapshot_year_month          NOT a column on br_company_relations -- migration
                             000210 dropped it, along with source_run_id and
                             source_record_id, when the table moved to SCD2.
                             The per-month values now live on the ledger
                             (br_company_relations_snapshots) instead: one
                             row per MERGED MONTH, not one row per edge.
one run only                 2026-06 materialized 2026-07-04/05; that is where
                             br_companies' 68.63M rows come from
no schedule                  RFB is manual-only by decision, so end_at precision
                             is whatever the operator's cadence happens to be
```
