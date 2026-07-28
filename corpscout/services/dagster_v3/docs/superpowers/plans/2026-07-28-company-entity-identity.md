# What a company entity *is*: attributes, not a type flag

**Goal:** present an entity as what it actually is — legally, by ownership, by
what it does — so a municipal waste company reads differently from both a
ministry and a private firm, without any of the three being mislabelled.

**Status, 2026-07-28.** Step 1 is **built, committed and schema-deployed** —
`73880b75` carries the twelve columns, migration `000203` is applied. The
columns are live and **empty**: the UHM re-materialization has not run, so
102,785 rows hold `''` in every one of them. That run is the only thing between
here and real data, and it costs no requests.

Step 2's open design question is **answered** — the buyer join resolves 98.9%
and does not miss public bodies (§2). Steps 2 and 3 are designed, not built.

Read alongside `2026-07-27-procurement-sources-independent-first.md`, whose
§4.1c, §4.1d and §4.4 this expands. That plan's Phase 1 is complete **in code**;
several of its materializations are still outstanding (§4).

---

## 0. How this came up

A user question, and it was the right one to ask: on a procurement page, buyer
names link to `/company/...`, so *are the company registers full of government
institutions?*

They are, and the answer has two layers that must not be conflated:

- **Public form.** Brreg and Bolagsverket register legal *entities*, so
  municipalities and ministries hold organisation numbers beside businesses.
  Solved — `company_entity_types` (migration `000200`) classifies by legal form
  and drives a badge on the entity page.
- **Public ownership.** `Hässleholm Miljö AB` (SE `5565550349`) is a municipal
  waste company whose legal form is an ordinary aktiebolag. Classifying it as a
  Company is *correct* and still misses what it is. **Nearly half of Sweden's
  993 TED buyers are companies of this kind**, so this is not an edge case.

Legal form cannot answer the second. The data that can, we throw away.

---

## The principle this plan now follows

Decided 2026-07-28, and it governs every step below:

> **Ingest stores what the source said. The conclusion is computed at query
> time.** No mapping, bucketing or vocabulary normalisation happens on the way
> in — `Kommun` is stored as `Kommun`. What a company *is* is decided in a
> per-country view, where being wrong costs one DDL statement instead of a
> re-materialization.

This is §4.4's storage rule extended from amounts to descriptions, and it splits
the work cleanly in two:

| layer | table | size | changes when |
|---|---|---|---|
| **observation** | `company_attributes` | large, verbatim, append | a source publishes new data |
| **interpretation** | `company_entity_types` + per-country views | tiny | we change our mind |

The reason this matters is not tidiness. It is that **we cannot yet predict what
these attributes mean across countries.** Registers differ in what an entity can
be, what statuses exist, and how entities relate to each other and to the state.
Storing verbatim means the observation layer never needs revisiting when
understanding improves; only the view does. It also means the conclusion gets
sharper as sources accumulate with no re-ingest of anything — when a Swedish
ownership register arrives, the same view reads one more attribute and says
something better.

The corollary that made the earlier draft self-contradictory: materialising
register facts into `company_attributes` is **fine**, and does not undo
`000200`'s rationale, *provided the stored value is verbatim*. `000200` exists so
that correcting a classification does not re-materialize 1.2M Norwegian or 3.4M
Swedish rows. A verbatim row never needs correcting, so the hazard does not
arise. Row count is not the constraint here — `companies_all` is already 115.6M,
and `country_code`, `attribute` and `value` are all LowCardinality.

---

## 1. Step one — stop discarding UHM's sector columns

`sweden_uhm_procurement` lists these in `EXPECTED_SOURCE_COLUMNS`, parses them,
and the loader drops them:

```
Sektor för köpare          Kommun 61,927 · Region 20,324 · Stat 19,633 · Annat 783
Delsektor för köpare       e.g. "Kommunalt ägd organisation"
Juridisk form för köpare   e.g. "Övriga aktiebolag"
SNI-Avdelning för köpare
Sektor för leverantör      + Juridisk form, Företagsstorlek, 5 SNI columns
```

A plain breach of the storage rule (§4.4 of the other plan): received, parsed,
thrown away. It is also the **only** data we hold that distinguishes ownership
from form.

### Where the discard actually happens

Verified 2026-07-28, and it is later in the chain than "the loader drops them"
suggests. `replace_raw_table` does `select ..., *` from `read_csv`
(`normalize.py:37-54`), so **all 44 source columns are already in the DuckDB raw
table**. They are dropped by the two named projections downstream:

- the candidate `select` (`normalize.py:95-130`)
- `AWARDS_COLUMNS` (`tables.py:101`) and the insert that follows it

Two consequences:

- **Distributions can be measured today**, against the existing DuckDB file,
  with no re-materialization and no re-parse. Do that before writing the
  migration — check `Delsektor`'s cardinality and how often it is populated.
- The change is a **projection edit**, not a parser change.

### Scope — DONE in `73880b75`, migration `000203`

Twelve columns carried verbatim through the candidate projection, `AWARDS_COLUMNS`
and `se_uhm_procurement_awards`: four buyer (`sector`, `subsector`,
`legal_form`, `sni_division`) and eight supplier (`sector`, `legal_form`,
`size`, and all five SNI levels). Nothing is mapped, bucketed or renamed into a
normalised vocabulary — `Sektor för köpare` lands as whatever UHM wrote in it.

Two things surfaced while building it, both recorded in §5: the raw DuckDB
table already held all 44 columns, so this was a projection change rather than
a parser change; and the candidate **stage DDL was hand-written inline** while
the export shipped `CANDIDATE_COLUMNS` into it positionally — a drift that could
only fail inside a materialization, after the download. It is extracted to
`uhm_candidate_stage_ddl()` and pinned by a test.

**Still empty until the re-materialization runs** (§4).

That is the whole of Step 1, and it is why it ships alone: there is no
vocabulary to agree on, so there is nothing to get wrong and nothing downstream
that must be decided first.

**No new requests.** The CSV is snapshotted at
`s3://source-sweden-uhm-procurement/raw/retrieved_date=*/awards.csv` (115 MB),
`sweden_uhm_procurement_raw_duckdb` reads it back from S3 (`assets.py:82`), and
all three stored copies share one sha256 — so re-parsing is free and verifiably
reads the same bytes.

---

## 2. Step two — `company_attributes`

Sector is published **per award row**, so it describes an entity *in a role*. It
cannot become a scalar column on the company without inventing an answer for a
company that is both buyer and supplier, or that shows different sectors across
rows. Keep it as attributes:

```
company_attributes
  country_code     LowCardinality(String)
  company_id       String
  attribute        LowCardinality(String)  -- open vocabulary, see below
  value            String   -- VERBATIM as the source published it
  value_label      String   -- the source's own display text, also verbatim
  source_slug      LowCardinality(String)  -- se_companies | sweden_uhm_procurement | ...
  observed_in_role LowCardinality(String)  -- buyer | supplier | '' for register facts
  observations     UInt64   -- rows evidencing it
  first_seen, last_seen
  resolved_at      DateTime64(3, 'UTC')

ENGINE = MergeTree
PARTITION BY (country_code, source_slug)
ORDER BY (country_code, company_id, attribute, value, source_slug, observed_in_role)
```

**One row per (attribute, value, source, role).** A company seen as a municipal
buyer and a private supplier keeps both rows; a register fact and a procurement
observation that disagree sit side by side with their sources. Nothing picks a
winner — the §4.4 rule applied to descriptions rather than amounts.

### Why these engine choices

- **`PARTITION BY (country_code, source_slug)`**, not country alone. A source's
  re-run must replace exactly its own slice: partitioning on country only would
  mean the UHM loader replaces the `SE` partition and wipes the
  `se_companies`-sourced rows sitting in it. At this grain each loader owns its
  partitions, needs no coordination with any other loader, and a re-run is an
  atomic `REPLACE PARTITION` from a stage table. Precedent:
  `company_signal_coverage` (migration `000165`) is partitioned by country for
  exactly this reason, and `000192` spells out the atomicity argument. This is
  the property that lets 100 sources write one table without blocking each
  other.
- **Plain `MergeTree`, not `ReplacingMergeTree`.** The partition swap already
  gives atomicity, so readers are spared `FINAL`.
- **No `Nullable` in `ORDER BY`** (`allow_nullable_key` is off), and every
  String in the sort key is non-nullable — `observed_in_role` uses `''`, never
  NULL, per the native-driver rule in CLAUDE.md.
- **`attribute` is an open vocabulary**, documented but not closed. Known
  starters: `legal_form`, `sector`, `subsector`, `size`, `industry_*`, `status`.
  We cannot predict what the next 90 countries publish, and an open
  LowCardinality column costs nothing.

### Unbuilt work this depends on: buyer-side identity

**`se_uhm_procurement_awards.company_id` resolves suppliers only:**

```sql
LEFT ANY JOIN corpscout.se_companies AS c
    ON c.company_id = u.supplier_id_normalized     -- clickhouse.py:56
```

The worked example below is a **buyer**, and has no `company_id` anywhere in the
pipeline today. Two things follow, and neither is in Step 1's scope:

- **A buyer join must be added.** Cheap: `buyer_id_normalized` already goes
  through the same `sweden_identity_sql` as the supplier side
  (`normalize.py:84-85`), so it is join-ready against `se_companies.company_id`
  with no new normalisation.
- **The buyer side needs its own eligibility rule.** `match_eligibility`
  (`normalize.py:160`) is supplier semantics and marks every non-contracted row
  `not_contracted`. Buyer sector is published on *all* rows including those.
  Reusing the supplier gate would silently discard most buyer observations —
  the same class of invisible loss §4.1 of the other plan exists to complain
  about.

**Measured 2026-07-28, and it settles the design: the join works.** Buyer ids
are already stored, so this was answerable without the re-materialization:

```
distinct buyers               1,241
resolve to se_companies       1,227   98.9%
rows with a resolvable buyer  102,406 of 102,785
```

The question that mattered was not the headline rate but whether the residue is
skewed toward the public bodies this feature exists to describe. **It is not:**

```
prefix          entity              buyers  resolved
2120            municipality           277   277   100%
2321            region                  20    20   100%
2021            state agency           180   176  97.8%
2220            kommunalförbund         57    54  94.7%
```

`se_companies` holds public-prefix org numbers in bulk (257 statlig · 290
kommun · 186 kommunalförbund · 20 region), so Sweden has none of the structural
absence Finland's trade register has. The 14 misses are individual — ABs that
look deregistered, plus `LUMPARLANDS KOMMUN`, which is Ålandic and therefore a
Finnish municipality buying in the Swedish register (an instance of the
cross-border case §4.1 tracks, not a matching defect).

So `company_attributes` keyed on `company_id` with `observed_in_role = 'buyer'`
needs no fallback shape. Build the buyer join as described.

### What it yields on `5565550349`

```
legal_form  AB-ORGFO / Övriga aktiebolag   se_companies              role ''
sector      Kommun                         sweden_uhm_procurement    role buyer
subsector   Kommunalt ägd organisation     sweden_uhm_procurement    role buyer
industry    E Vattenförsörjning…           sweden_uhm_procurement    role buyer
```

Legally a company, municipally owned, running water and waste. None of the four
alone says what it is, which is why one type flag was the wrong shape. Note that
every value is the source's own string — the interpretation of `Kommun` happens
in §3, not here.

### Not this table: relationships

"Is this part of some other big company" is a **different grain**.
`attribute`/`value` describes an entity with a scalar; a parent/subsidiary link
points at *another entity* and carries its own facts — ownership percentage,
role, start and end dates, and a counterparty country that is frequently not the
subject's. Bending `value` to hold a foreign `company_id` means adding
`value_country_code` and then a pile of columns that are NULL for every
non-relationship row.

Relationships get their own table when they are built. The per-country view in
§3 reads both. **Do not half-encode them into `company_attributes` now.**

---

## 3. Step three — the conclusion, in one view per country

Compose from the attributes, not from a flag, so a municipal AB, a ministry and
a private AB each read as themselves.

The conclusion is a **column in a per-country view**, computed at query time:

```sql
-- se_company_identity
SELECT
    c.company_id,
    et.entity_type,                     -- form:      company_entity_types
    sec.value_label AS sector_label,    -- ownership: verbatim from UHM
    multiIf(
        et.is_public_sector = 1,  'public_body',
        sec.value = 'Kommun',     'municipally_owned_company',
        sec.value = 'Stat',       'state_owned_company',
        ...
    ) AS presentation
FROM se_companies AS c
LEFT JOIN company_entity_types AS et  ON ...
LEFT JOIN company_attributes   AS sec ON sec.attribute = 'sector' AND ...
```

One view per country, free to say something structurally different in Brazil
than in Sweden, exactly as the country contract views already do. There is no
universal schema to anticipate either.

**Why this is the right place for it:** changing what the product concludes
about an entity is a migration replacing a view — no re-parse, no
re-materialization, no touching 3.4M rows. We can be wrong about Sweden on
Tuesday and right on Wednesday for the cost of one DDL, which is the property
worth having while we are still learning what these attributes mean.

### The guard: one view owns the conclusion

Query-time logic drifts by duplication. If the `multiIf` is written once in the
country view, again in a backoffice loader, and a third time in some count, three
surfaces will disagree about the same company and **nothing will flag it** — the
same silent-miss failure mode as the `recordQuery` alias trap in §5.

**One view per country owns the conclusion. Everything else reads that column
and never recomputes it.** Same discipline `company_entity_types` already
applies: derived once, read everywhere.

---

## 4. Operational state to clear first

### Migrations — CLEARED 2026-07-28

`000199` through `000204` are committed and applied. Verified against the live
database rather than the ledger, which is the point of the story below:
`retrieval_method` is present on `procurement_registers`, and all twelve party
columns are present on `se_uhm_procurement_awards`.

**Why `000199` could not be fixed by re-deploying, kept because the shape of
this mistake recurs.** `b285033e` added `retrieval_method` to that migration's
CREATE after the version had already been applied. golang-migrate records
applied versions, so `migrate up` is a no-op at that version, and
`CREATE TABLE IF NOT EXISTS` would be inert even if it ran. The obvious remedy
therefore exits clean and changes nothing — a defect whose fix reports success
while the column stays missing, and whose next reader opens the CREATE, sees
the column, and concludes it is covered.

Repaired forward in `000204` (`7f8f1d0b`), idempotent by construction. The test
pins the **repair**, not the column's presence, because asserting presence
passes by reading `000199` — the exact reasoning that leaves prod without it.

**A migration that has shipped is history, not a draft.** Never edit one to
change what it built; add another.

The ledger is **append-only** — read it with `ORDER BY sequence DESC LIMIT 1`,
never `any(dirty)`, which returns an arbitrary row and produced a false
"migration failed" alarm. And "applied" is a status label: when the bug *is*
the label, check the column, not the ledger.

### Materializations outstanding — all independent

| what | assets | cost |
|---|---|---|
| **UHM re-parse** | `sweden_uhm_procurement_raw_duckdb → _awards_duckdb → _awards_clickhouse` | **0 API requests** — the CSV is in S3. **Blocks steps 2 and 3** |
| Brazil history | `brazil_pncp_contracts_duckdb → _usd → _clickhouse`, 2022-01…2025-01 | **0 API requests** — 37 months already in S3 |
| Brazil recent | full job incl. `raw_pages_s3`, 2025-02…2026-07 | ~11.4 h |
| TED re-parse | all **93** `ted_monthly_duckdb`, then `ted_publish_clickhouse` once | **0 API requests** |
| Doffin | `norway_doffin_backfill_job`, 103 partitions | ~4.5 h |

**TED's publish must wait for all 93** — a partial run fails on the missing
`lots` table. Brazil's `_duckdb` must be re-run for every month even where
Dagster shows it materialized: that asset `create or replace`s, so the DuckDB
holds one month at a time and the export reads from it.

---

## 5. Things not to rediscover

```
UHM sector          the only ownership signal we receive; per award row, not per company
UHM raw table       already holds ALL 44 source columns (`select *` at normalize.py:37).
                    The discard is in the candidate projection and AWARDS_COLUMNS, so
                    distributions are measurable today with no re-materialization
UHM company_id      SUPPLIERS ONLY (clickhouse.py:56). Buyers have no resolved company
                    at all, and the worked example is a buyer
UHM buyer join      VIABLE, measured 2026-07-28: 1,227 of 1,241 buyers resolve (98.9%),
                    102,406 of 102,785 rows. Municipalities 277/277 and regions 20/20 —
                    the public bodies are NOT the gap. Do not re-derive this before
                    building the join
UHM row count       102,785 as of 2026-07-28, not the 96,094 the earlier plan records
match_eligibility   supplier semantics; marks non-contracted rows ineligible. Buyer
                    sector is published on those rows too — needs its own gate
Hässleholm Miljö AB SE 5565550349 — the worked example; AB by form, Kommun by sector
SE public forms     81 statlig · 82 kommun · 83 kommunalförbund · 84 region (identified
                    from the entities carrying them; SE publishes no description column)
NO ORGL             ambiguous by design — a sub-unit whose parent decides. NOT flagged
                    public, though 144 procurement buyers are one
FI register         a TRADE register: municipalities are simply absent. Near-zero public
                    count is correct, not a bug. Its resolving buyers are state-OWNED
                    companies (Fortum, VR, Metsähallitus)
BR / DK             publish no legal form at all — nothing classifiable
coverage            NO 100% of rows classified · SE 99.995% · FI 99.997%
```

### Traps that cost time here
- **A custom `recordQuery` aliases its columns.** Sweden's joins translations
  and returns `c.legal_form_code`. An exact-key lookup found nothing, rendered
  no badge for *every* Swedish entity, and failed silently — a grep appeared to
  confirm it worked by matching other text on the page. Match the unqualified
  column name.
- **`companies_all` is 115.6M rows** and its country codes are **lowercase**.
  Adding a facet to it needs three lockstep changes (see `filters.ts`) plus a
  full rebuild. Deliberately skipped.
- **No `;` inside a `--` comment in a migration** — the driver splits on `;`
  without stripping comments, and the chunk fails as an empty query.
- **No `from __future__ import annotations`** in a module defining `@dg.asset`;
  it stringizes the context hint and breaks Dagster's validation.
