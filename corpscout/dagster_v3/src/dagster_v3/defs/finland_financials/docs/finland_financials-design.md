# finland_financials design doc

Populate `corpscout.fi_financial_metrics` from Finnish PRH XBRL statements using the
**shared `xbrl_common` parser** — replacing the heavy, never-run Arelle pipeline in
the `finland_xbrl` module.

## 1. Why this exists
- Finland's financial statements are **plain (non-inline) dimensional XBRL** (the
  `suomi.fi/crr` taxonomy): facts are concept-tagged elements (`<fi_met:md103
  contextRef=.. unitRef=ISO4217_EUR>123</>`), and the **line item is concept +
  the context's explicit dimension member** (e.g. `fi_met:md103` + `fi_MC:x673` =
  revenue). Different shape from UK iXBRL (concept-only).
- The original `finland_xbrl` module parsed with **Arelle** (slow/heavy) + dbt and
  was never materialised (`fi_financial_metrics` was empty). The intended fix
  ("factor arelle_parser into shared xbrl_common") is realised here.

## 2. Source & ingest
- PRH XBRL API (open, no key): **discovery** `GET /all_financial_statements?
  registeredDateStart&registeredDateEnd&page` → `(businessId, financialDate)`;
  **statement** `GET /financial?businessId&financialDate` → the XBRL XML.
- Per-statement fetch → bounded by a registration-date **window + max_reports**
  (config). Full coverage = widen the window / run incrementally over time (the
  same annual-filing logic as UK; statements are per-business per-financial-year).

## 3. Parse & map (the shared parser)
- `xbrl_common.parse_xbrl(body)` → contexts (period/dimensions) + facts (concept
  qname + value + unit). Reuses the exact context/dimension code as iXBRL.
- The existing **dimensional seed** `finland_xbrl/.../xbrl_metric_map.csv`
  (`(concept_qname, mcy_member_code) -> metric`) maps facts to the 12 canonical
  metrics. Current-period facts only (`ctx.period_end == reporting_period_end`).
- Currency from `unitRef` (ISO4217_EUR → EUR).

## 4. ClickHouse
- Reuses the existing **`fi_financial_metrics`** (migration `000009`, no new
  migration). Native EUR + **EUR→USD** via the shared `ExchangeRateClient`
  (separate step, keyed on `period_end`). Export **appends** (ReplacingMergeTree
  dedups by `business_id/financial_date/statement_key`) so bounded runs accumulate.

## 5. Update strategy (registration-date driven)
PRH discovery is windowed by **registration date**, so updates are precise deltas —
no bulk archives (unlike UK). Two assets, both append (ReplacingMergeTree dedups by
`business_id/financial_date/statement_key`):
- **`finland_financials_incremental`** (non-partitioned, cursor) — a cursor (in the
  source DuckDB) holds the last registration date; each daily run fetches
  `(cursor, today]`, parses, EUR→USD, appends, then advances the cursor **after** a
  successful export. Keeps the table current at low volume.
- **`finland_financials_backfill`** (MonthlyPartitionsDefinition, registration month)
  — one month per partition. Launch a **Dagster backfill** over the historical range
  once; `BackfillPolicy.multi_run(1)` + the pool walk it one bounded month-run at a
  time (resumable, throttled — not a 25h marathon). `BACKFILL_MAX_REPORTS` caps a
  partition. A polite per-request delay avoids PRH rate-limiting (the cause of an
  early 17/169 → fixed to 167/169).
- Amendments: `statement_key` includes the XML hash, so a re-filing is a new row
  (full version history kept); "latest per company-year" = `argMax(.., resolved_at)`.

## 6. Status / deferred
- This supersedes the `finland_xbrl` Arelle metrics path; the old module's
  discovery/raw-download assets can be retired or repointed later.
- Validated live on a recent week of statements (real EUR figures + USD).
- Best-approach finding: **one shared `xbrl_common` extractor for UK iXBRL and FI
  plain dimensional XBRL**; the per-country difference is only the metric map
  (concept-only vs concept+member).
