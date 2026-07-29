# France BCE/INPI financial ratios design

## 1. Source overview

- **Country/source:** France — DGE financial ratios derived from INPI/RNCS.
- **Module:** `defs/france_financial/`
- **DuckDB:** `data/france_financial_source.duckdb`
- **Pool:** `france_financial_duckdb`
- **ClickHouse:** `corpscout.fr_financial_metrics` and the derived
  `corpscout.fr_company_financials_latest` summary, migration `000209`.
- **Dataset:** `ratios_inpi_bce`, public Parquet export, Open Licence 2.0,
  6,542,232 rows observed 2026-07-28, no authentication.
- **Entity key:** SIREN. Statement grain is
  `(siren, date_cloture_exercice, type_bilan)`.

## 2. Ingest mode

Non-partitioned full refresh. A full Parquet export exists, so pagination adds
no value. The source metadata was last updated 2026-06-01; the job runs monthly
on the 12th and remains stopped until the first live validation.

## 3. Loading

The dlt retry-capable HTTP session streams the Parquet file to a temporary path
with whole-download retries and Content-Length validation. DuckDB `read_parquet`
loads it in native code. Raw source values are cast to text in
`france_financial.ratios_raw`; no Python row loop is used. Empty snapshots fail.

## 4. Transform

One set-based DuckDB statement builds `financial_metrics`. The source monetary
fields remain separate: revenue, gross margin, EBE/EBITDA, EBIT and net income.
All source ratios are retained with names that expose percent/day units. Raw
JSON and its SHA-256 remain in DuckDB only.

## 5. ClickHouse schema

`fr_financial_metrics` retains all three source balance types: `K`
(consolidated), `C` (complete) and `S` (simplified). The latest-summary query
prefers K, then C, then S without deleting any underlying row. Date and fiscal
year are nullable because the source values are parsed rather than assumed.

## 6. Translation

None. The source is numeric and its balance-type codes are stable documented
codes. No free text is exported.

## 6b. Contacts

No contact fields exist in this financial dataset. Contacts remain outside this
source; `france_sirene` already documents their absence from Sirene.

## 6c. Industry and Wikidata

No activity classification exists in this dataset. The existing
`france_sirene` source owns NAF→NACE and the Wikidata P1616 registry seed.

## 7. Currency

All amounts are EUR. Each amount has `_amount_original` and `_amount_usd`.
`france_financial_metrics_usd_duckdb` performs the separate exchange-rate step
using the period-end date and stores `fx_rate_to_usd`, date and source.

## 8. Scheduling

`france_financial_job`, monthly at `10 7 12 * *`, Europe/Belgrade. The asset
selection uses `.upstream()` and every DuckDB opener uses the source pool.

## 9. Issues found

- A SIREN/date can have multiple balance types; omitting `type_bilan` would
  overwrite legitimate complete and consolidated statements.
- The source does not publish assets/equity. The cross-country latest summary
  therefore keeps those fields null rather than inventing values.

## 10. Verification

- `tests/test_france_financial.py`
- `tests/test_company_financials_latest.py`
- `uv run dg check defs`
- Apply migrations, materialize the explicit four-asset chain, then compare
  La Poste SIREN `356000000` against the source API.
