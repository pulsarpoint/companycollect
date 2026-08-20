# Finland Financial Display Parity With Sweden

Date checked: 2026-08-18

## Conclusion

Finland has the same two accounting scopes that the Sweden company page shows:

1. standalone legal-entity annual accounts from the national register; and
2. consolidated IFRS group accounts from ESEF.

The source data is already processed in source-owned tables. The missing piece
was the serving configuration: Finland did not declare those tables as separate
financial sources, so it had no dedicated Financials tab and its existing PRH
facts and XML documents could not be opened from a company page.

There is no `fi_financial_tables` table, and Sweden does not merge its two
accounting scopes into one table either. Keeping PRH standalone accounts and
ESEF consolidated accounts separate is intentional. Combining their values by
company and fiscal year would make unlike accounting entities look comparable.

## Implemented Source And Table Inventory

| Signal | Scope | Source-owned tables | Derived/serving table | Company join | Display decision |
|---|---|---|---|---|---|
| PRH digital annual accounts | Standalone legal entity | `fi_financial_statements`, `fi_xbrl_contexts`, `fi_xbrl_units`, `fi_xbrl_facts_raw`, `fi_xbrl_taxonomy_codes` | `fi_financial_metrics`; `fi_financial_facts_with_source` view | Finnish business ID (`business_id`) | Registry financial source with per-year standardized metrics, all tagged facts, and original XML |
| filings.xbrl.org ESEF | Consolidated IFRS group | `esef_filings`, `esef_facts` | `esef_financial_metrics` | LEI through `company_identifier`, then Finland business ID | Separate ESEF source card and report drill-down |
| Verohallinto corporate income tax | Taxpayer/tax assessment | `fi_tax_records` | none | Finnish business ID (`business_id`) | Separate tax-record section; never map taxable income to accounting profit |
| Virre financial statements | Paid document fallback | not implemented | none | Finnish business ID | Remains out of the open-data pipeline |

## Sweden Comparison

Sweden's serving path is source-preserving:

```text
Bolagsverket reports/facts -> se_financial_metrics -> registry source card
ESEF filings/facts         -> esef_financial_metrics -> ESEF source card
```

Finland now follows the same pattern:

```text
PRH statements/facts -> fi_financial_metrics -> registry source card
ESEF filings/facts    -> esef_financial_metrics -> ESEF source card
```

The dedicated company Financials page is the presentation-level composition.
It does not union source rows into a misleading country-wide financial table.

## Source Precedence And Deduplication

- PRH rows are deduplicated per fiscal year inside the PRH source by preferring
  the latest registration and resolution timestamp.
- ESEF amendments are resolved per metric from the newest filing version that
  actually reports that metric. ESEF remains consolidated scope.
- PRH and ESEF do not override each other. Both are displayed when both exist.
- Vero tax rows do not participate in accounting-metric precedence.

## Evidence And Drill-down

PRH statement XML now publishes into the shared `company_source_records`,
`company_source_record_origins`, and `company_source_record_links` tables with a
content-derived record identity. The company page uses the same identity for
the standardized metric evidence, and reads exact facts through
`fi_financial_facts_with_source`.

## Coverage And Risks

- PRH's open API covers digitally filed accounts, not all Finnish companies.
- ESEF principally covers listed issuers and reports consolidated group scope.
- `fi_company_financials_latest` continues to use PRH standalone metrics only;
  this avoids silently substituting group figures for a legal entity.
- filings.xbrl.org index access is open, but redistribution terms for underlying
  filing packages should remain under review.
- The formal Finland country-data-model handoff cannot be regenerated yet
  because `companies/data/finland/normalized/` is missing.
