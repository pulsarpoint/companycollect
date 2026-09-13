# SE company financial entity — package notes

Spec: `docs/superpowers/specs/2026-09-11-se-company-financial-entity-design.md` (the binding
document; this note is the package's map and runbook).

## Shape (slice 1)

Five tables, migration 000401, keyed by company, accounting scope (`standalone` |
`consolidated`) and period end; `period_key` is `'<scope>:<period_end>'` and is what
suggestions, rules and the backoffice key on.

| Table | Grain | Written by |
|---|---|---|
| `se_company_financial_suggestion` | one current row per company, source and period | the extractors (slice 2), the backoffice's reviewer rows (slice 4) |
| `se_company_financial` | one row per company, scope and period end | the fold (slice 3) |
| `se_company_financial_history` | one row per change of a published period | the fold |
| `se_company_financial_precedence` | global rows (`company_id ''`, `period_key ''`) and company rules | the export asset; the backoffice |
| `se_company_financial_rule` | hide rules per company and period | the backoffice |

`tables.py` is the single place the column order lives and is pinned to the DDL by
`tests/test_se_company_financial_tables.py`. The twenty monetary fields are pairs
(`<field>_amount_original` in the source's currency at full units, `<field>_amount_usd` the
source's own conversion) with a `<field>_source` on the main row; `amount_scale` on the
suggestion row records the unit the source published in (Ratsit: 1000000).

## Precedence

`precedence.py` holds the spec's section-5 map: Ratsit 1000 first, the registers 900 (they
never share a scope), the restated Bolagsverket column 800, the reviewer 20000 above a
company rule's 10000. A source absent from a field's map cannot supply it.

Runbook: after changing the numbers, materialize `se_company_financial_precedence_clickhouse`
(it writes only when the stored global rows differ from the dictionary, so an idle re-run
moves no watermark). Its `stale_pairs` metadata counts the global pairs still in ClickHouse
that the dictionary no longer names; the export never deletes them. To retire such a pair by
hand, insert a new version at the same key with `removed = 1` and a newer `decided_at`, then
re-fold every bucket: the export never deletes, and any global-row change is a fold watermark.
Then re-fold every bucket with `changed_only: false` (slice 3): a precedence change is not a
per-company change and the fold's per-company watermarks will not notice it on their own.

## Notes for the extractors (slice 2)

- `period_key` must be built from the very same `period_end` value the row inserts (the CHECK
  compares them server-side; a mismatch aborts the whole INSERT block), and `makeDate32` returns
  1970-01-01 for years outside Date32's range rather than clamping, so an extractor guards the
  derived date before keying.
- `currency` is NULL when a source names none, never '' (the CHECK refuses '' and the fold's
  gate relies on NULL); `SUGGESTION_VALUE_COLUMNS` is both the tombstone definition (all NULL)
  and the state-hash column list.

## Extractors (slice 2)

| Module | Source | Reads | Key rule |
|---|---|---|---|
| `bolagsverket.py` | `bolagsverket` | `se_bolagsverket_financial_metrics` reported rows | one row per period end, the fuller statement wins, then the smaller statement key |
| `bolagsverket.py` | `bolagsverket_comparative` | the same table's comparative rows | revenue and total assets from the newest restating filing; `filing_fiscal_year` names it |
| `esef.py` | `esef` | `esef_financial_metrics` through `se_esef_filings` | `consolidated_ifrs` only; newest `fxo_id` version wins field by field, older versions fill gaps |
| `ratsit.py` | `ratsit` | `se_ratsit_financial_periods` for the latest `se_ratsit_financial_reports` hash | figures scaled from `monetary_unit`, USD twins copied, undated periods keyed on Dec 31 and flagged, the longer duplicate wins |

All four use `suggestions.py`'s target and the shared `se_company/state_scan.py` (the person
entity's per-company state hash, lifted in this slice): a company is visited when what the
source delivers now differs from its stored live rows, and a period the source stopped
delivering gets a tombstone that copies scope and period end so the table's CHECK holds.
The job `se_company_financial_extract_job` runs `se_ratsit_financial_periods_usd` first, then
the four extractors; the weekly `se_company_financial_weekly` (Monday 07:55 UTC) is defined
STOPPED until the fold (slice 3) exists. Every extractor previews by default (`execute: false`).

A live row must carry at least one figure or an employee count; a source row with none is
skipped, never written, on both sides of the state hash. The Ratsit extractor pins
`normalizer_version` (like the person extractor) so superseded normalizer generations never
compete for a period.

## Fold (slice 3)

`fold.py` is pure and decides ONE period: `fold_financial` picks the currency first (highest
effective precedence among the rows naming one; ties to the smaller source name, then uid),
lets each of the twenty figures compete only among rows in that currency (the winner brings
its own USD twin), lets employees and the period attributes compete ungated, and returns None
when no field has a winner. `resolve_rules` overlays a period's rules on the company-wide ones
on the global map. `fold_company_periods` walks a company's periods against its current main
rows: `created`, `updated`, `hidden`, `withdrawn` (no live row left; the last values stay with
`active 0`) and `reactivated`; an unchanged period is still returned so the batch can advance
its `folded_at`.

`batch.py` folds pages of 5,000 companies: five FINAL reads (live suggestions, main rows,
company rules, hide rules; the watermarks aside), the pure fold, then history BEFORE main.
`changed_only` selects a company when its newest suggestion, precedence decision or hide
decision (released versions included) is newer than its newest `folded_at`, or it was never
folded and has a live row. The global precedence export is NOT a watermark: after changing
the dictionary, export it and re-fold every bucket with `changed_only: false`.

Assets: `se_company_financial_fold` (64 hash buckets, one partition per run, pool
`se_company_financial_fold`, downstream of the four extractors) and
`se_company_financial_fold_companies` (`company_ids`, the backoffice's Fold now target).
Runbook for a full backfill while the run queue is held: run the 64 partitions in-process on
the dagster host, one after the other (the plan's Task 7 script), then re-run three buckets
with `changed_only: true` and expect `considered 0`.
