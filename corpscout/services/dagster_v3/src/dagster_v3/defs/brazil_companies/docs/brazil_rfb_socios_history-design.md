# Brazil RFB Socios: connection history design doc

**Status:** designed 2026-07-29, not built.

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

Each run rebuilds the whole table from **existing state + the new snapshot**
into a stage table, then `EXCHANGE TABLES`. Atomic and idempotent; no ClickHouse
mutations, which it handles badly at this size.

```
in both state and snapshot   -> keep first_seen and start_at,
                                last_seen = new snapshot, observations + 1,
                                attributes refreshed, end_at stays NULL
only in state (absent now)   -> close: is_current = 0,
                                end_at = the snapshot month's reference date
                                (meaning "gone by", see section 2)
only in the snapshot         -> open: first_seen = last_seen = new snapshot,
                                start_at = relation_since, end_at = NULL
already closed               -> untouched; a re-entry arrives as a new key
                                because its relation_since_key differs
```

Row count is ~distinct spells ever seen rather than months x edges — smaller
than retaining per-month observations, which is the next section's point.

## 5. Ordering guard

**Refuse out-of-order snapshots.** The merge asserts the incoming
`snapshot_year_month` is newer than the newest already merged, and fails loudly
otherwise.

Considered and rejected: rebuilding from every S3 archive on every run. It is
self-healing and order-independent, but pays ~20-25M rows x up to 12 months of
reprocessing *every run, forever*, to insure against something that is rare once
runs are ordered — and the recovery it automates is available manually anyway
(§7). The user's objection settled it: *"why we need 2. option. If only reason
is irregular runs, we can add scheduler."*

The assertion does more than refuse: it makes a skipped or out-of-order month
**visible**. Silently absorbing a late July would hide that the cadence had
slipped, and cadence is what `end_at` precision depends on.

**A merged-snapshot record makes the assertion possible.** A small table listing
the months merged, with counts of edges opened and closed. Without it the guard
has nothing to compare against, a rebuild leaves no way to tell what the history
is made of, and a gap is invisible rather than detectable.

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

1. Read the merged-snapshot record (§5) to see which months are in the history.
2. `DROP` the SCD2 table and recreate it from the migration.
3. Re-derive by materializing the chain for each archived month **in ascending
   date order**, oldest first. Each run merges one snapshot; the ordering guard
   enforces the sequence.
4. Only months still in S3 can be replayed — one year, per §7. Anything older is
   gone and the rebuilt history starts from the oldest surviving archive.

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
snapshot_year_month          already a column on br_company_relations -- the
                             original design anticipated this
one run only                 2026-06 materialized 2026-07-04/05; that is where
                             br_companies' 68.63M rows come from
no schedule                  RFB is manual-only by decision, so end_at precision
                             is whatever the operator's cadence happens to be
```
