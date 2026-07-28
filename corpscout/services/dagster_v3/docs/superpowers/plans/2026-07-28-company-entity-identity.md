# What a company entity *is*: attributes, not a type flag

**Goal:** present an entity as what it actually is — legally, by ownership, by
what it does — so a municipal waste company reads differently from both a
ministry and a private firm, without any of the three being mislabelled.

**Status:** designed, not built. Step 1 is blocked on data we currently discard.

Read alongside `2026-07-27-procurement-sources-independent-first.md`, whose
§4.1c, §4.1d and §4.4 this expands. That plan's Phase 1 is **complete**.

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

**Work:** `defs/sweden_uhm_procurement/normalize.py` + `tables.py`, a migration
adding the columns to `se_uhm_procurement_awards`, and a UHM re-materialization.

**No new requests.** The CSV is snapshotted at
`s3://source-sweden-uhm-procurement/raw/retrieved_date=*/awards.csv` (115 MB),
and all three stored copies share one sha256 — so re-parsing is free and
verifiably reads the same bytes.

---

## 2. Step two — `company_attributes`

Sector is published **per award row**, so it describes an entity *in a role*. It
cannot become a scalar column on the company without inventing an answer for a
company that is both buyer and supplier, or that shows different sectors across
rows. Keep it as attributes:

```
company_attributes
  country_code, company_id
  attribute        legal_form | sector | subsector | size | industry_division
  value            normalised
  value_label      display
  source_slug      se_companies | sweden_uhm_procurement | ...
  observed_in_role buyer | supplier | ''      -- '' for register facts
  observations     rows evidencing it
  first_seen, last_seen
```

**One row per (attribute, value, source, role).** A company seen as a municipal
buyer and a private supplier keeps both rows; a register fact and a procurement
observation that disagree sit side by side with their sources. Nothing picks a
winner — the §4.4 rule applied to descriptions rather than amounts.

On `5565550349` that yields:

```
legal_form  Aktiebolag                   se_companies              role ''
sector      Kommun                       sweden_uhm_procurement    role buyer
subsector   Kommunalt ägd organisation   sweden_uhm_procurement    role buyer
industry    E Vattenförsörjning…         sweden_uhm_procurement    role buyer
```

Legally a company, municipally owned, running water and waste. None of the four
alone says what it is, which is why one type flag was the wrong shape.

---

## 3. Step three — the distinct page treatment

Compose from the attributes, not from a flag, so a municipal AB, a ministry and
a private AB each read as themselves. Only reachable after steps 1 and 2 —
today the page cannot even identify the example entity as government-owned.

---

## 4. Operational state to clear first

### Migrations applied locally, NOT deployed
- **`000199`** `procurement_registers` — **reshaped after it was deployed**; the
  deployed copy lacks `retrieval_method`. Re-deploy.
- **`000200`** `company_entity_types`.

Ledger was at 198 and clean. That ledger is **append-only** — read it with
`ORDER BY sequence DESC LIMIT 1`, never `any(dirty)`, which returns an arbitrary
row and produced a false "migration failed" alarm.

### Materializations outstanding — all independent

| what | assets | cost |
|---|---|---|
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
