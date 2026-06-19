# Latvia UR Financials (Module 2) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or executing-plans. Checkbox steps track progress.

**Goal:** Load Latvia's four structured financial CSVs (financial_statements, balance_sheets, income_statements, cash_flow_statements; ~2M reports, CC0) into DuckDB with per-file checkpoints, pivot them on `statement_id` into one wide `latvia_ur.financial_statements` table, and export it to ClickHouse `corpscout.lv_financial_statements`.

**Architecture / checkpoint strategy (validated live 2026-06-19):**
- **4 independent raw-load assets**, one per CSV. Each downloads its file and loads it into a DuckDB staging table (`latvia_ur.<name>_raw`) via DuckDB `read_csv` (`all_varchar=true`, `delim=';'`). Each is its own Dagster asset → a failure in one file re-materializes only that file, never the others (the user's explicit checkpoint requirement). Single-writer `latvia_ur_duckdb` pool.
- **1 pivot asset** `latvia_ur_financial_statements_duckdb`: `financial_statements_raw` is the spine; LEFT JOIN the other three on `statement_id` (= `financial_statements.id`); `regcode = legal_entity_registration_number`; `try_cast` numerics → `DECIMAL(38,2)` (signed), year→INT, period dates→DATE, employees→BIGINT. Preserve `rounded_to_nearest` and `currency` raw (values stored unscaled — scaling deferred; ONES verified, other units unconfirmed). Count + log orphan `statement_id`s present in parts but missing from the spine (R7 — don't silently drop).
- **1 ClickHouse export asset** → `corpscout.lv_financial_statements` (migration 000016 owns the schema), same assert-exists + atomic-replace pattern as `latvia_ur_clickhouse_companies`.

**Join keys / facts:** `financial_statements.id` = `{balance_sheets,income_statements,cash_flow_statements}.statement_id`; `financial_statements.legal_entity_registration_number` = register `regcode`. cash_flow is sparse/partial. EUR (pre-2014 LVL). Values can be negative.

**Download URLs (CKAN, verified):**
```
financial_statements: .../resource/27fcc5ec-c63b-4bfd-bb08-01f073a52d04/download/financial_statements.csv
balance_sheets:       .../resource/50ef4f26-f410-4007-b296-22043ca3dc43/download/balance_sheets.csv
income_statements:    .../resource/d5fd17ef-d32e-40cb-8399-82b780095af0/download/income_statements.csv
cash_flow_statements: .../resource/1a11fc29-ba7c-4e5a-8edc-7a28cea24988/download/cash_flow_statements.csv
```
(all under dataset `8d31b878-536a-44aa-a013-8bc6b669d477`)

**Tasks:**
1. Financials column schema in `tables.py` (raw table names, URLs, balance/income/cashflow column groups, `LV_FINANCIAL_STATEMENTS_COLUMNS` matching migration 000016) + test.
2. Raw CSV → DuckDB loader (`financials.py`: download to temp + `read_csv` into `*_raw` table) + 4 raw-load assets + tests.
3. Pivot asset building wide `latvia_ur.financial_statements` from the 4 raw tables (cast/map, orphan count) + test.
4. ClickHouse export asset (`export_latvia_ur_clickhouse_financial_statements`) + contract test.
5. Verify: `dg check`, discovery (6 latvia assets), full suite.

**Deferred:** value scaling by `rounded_to_nearest` (preserve column; confirm distinct units on first full load); cash_flow detail validation.
