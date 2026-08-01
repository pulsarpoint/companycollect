# France company detail: contracts, financials and Wikidata

**Status:** approved, ready for an implementation plan
**Date:** 2026-08-01
**Scope:** `corpscout/services/backoffice` only

## Goal

A French company page shows what France actually publishes about it: its
government contracts, its filed accounts with the ratio suite INPI carries, and
its Wikidata entry where one exists.

Today `/company/fr/<siren>` renders the register record, industries and address
and nothing else. All three sections it is missing **already exist as
components** — they render empty for France because France's config declares no
queries to fill them.

## Why now

France is the only large register in the backoffice with financial data this
rich and no way to see it. The numbers:

| source | table | rows | reach |
| --- | --- | --- | --- |
| Contracts | `fr_government_contracts` | 721,161 | 99,287 companies |
| Contract summary | `fr_government_contract_summary` | 99,287 | 1 per company |
| Financials (per year) | `fr_financial_metrics` | 6,542,232 | 1,586,046 companies |
| Financials (latest) | `fr_company_financials_latest` | 1,586,046 | 1 per company |
| Wikidata | `wikidata_company_identifiers` (`fr_siren`) | 169 | ~169 companies |

`fr_financial_metrics` is the reason this is not a five-line config change. It
carries revenue, gross margin, EBITDA, EBIT and net income **plus fourteen
ratio and working-capital-day columns**, and they are essentially fully
populated: revenue and EBITDA 100%, financial autonomy 99.96%, debt ratio
99.93%, interest coverage 76.5%. The generic `FinancialsSection` renders five
columns and would drop all of it.

Measured 2026-08-01.

## Decisions taken

1. **France gets its own financials section**, not the generic one — the ratio
   suite is the point. Consistent with the per-country detail sections already
   in `app/components/detail/countries/`.
2. **Wikidata is wired despite 169 matches.** The section hides itself when
   unmatched, so the cost is one query. Its coverage is separately
   investigated; see Out of scope.
3. **The contract summary header goes on the shared section**, because all nine
   contract countries have a `<cc>_government_contract_summary` table. France
   is the first to pass it, not the only one that could.
4. **The SQL lives in its own module.** `countries.ts` is 1,482 lines and mostly
   inline SQL literals; France's five queries would add ~180 more.

## Architecture

Data flows through the seam that already exists: `countries.ts` config →
`getCompanyDetail` fires one promise per declared query → the route hands each
result to a section that hides itself when empty.

Three of France's five queries use config keys that **already exist**
(`publicContractsQuery`, `wikidataQuery`, `wikidataPeopleQuery`), so contracts
and Wikidata need SQL only. Two keys are new.

### Files

| file | change |
| --- | --- |
| `app/lib/detail-queries/fr.ts` | **new** — France's five SQL strings as named consts |
| `app/lib/countries.ts` | France's `detail` block references them; two new optional keys on the `CountryDetail` type |
| `app/lib/queries.server.ts` | two new promises, two new fields on `CompanyDetail` |
| `app/components/detail/countries/fr-financials.tsx` | **new** — the France financials section |
| `app/components/detail/public-contracts-section.tsx` | optional `summary` prop |
| `app/routes/country-company-detail.tsx` | render `FrFinancialsSection`, pass `summary` |

### New config keys

```ts
/** Per-year financial metrics for registers that publish more than the five
 *  canonical figures. Rendered by a country-specific section, not the generic
 *  FinancialsSection. */
financialMetricsQuery?: string;
/** One row per company from <cc>_government_contract_summary, rendered as a
 *  header above the contracts table. */
contractSummaryQuery?: string;
```

France declares **no** `financialsQuery`, so the generic `FinancialsSection`
stays empty for it and the existing three-way financials conditional in the
route is untouched.

## Section 1 — France financials

One card, two tables.

**Figures, year per row:** revenue, gross margin, EBITDA, EBIT, net income.
EUR with the USD twin through the existing `MoneyPair`. Currency is `EUR` for
all 6,542,232 rows, so there is no mixed-currency case to handle.

**Ratios, metric per row and year per column:** EBITDA margin, debt ratio,
financial autonomy, liquidity, interest coverage, then the working-capital set
France uniquely publishes — customer payment days, supplier payment days,
inventory turnover days.

**Equity and total assets are NOT shown, because France does not have them.**

An earlier draft of this spec had them as a latest-year snapshot joined from
`fr_company_financials_latest`. Verified against the live table before writing
the plan: `equity_amount_original` and `total_assets_amount_original` are NULL
for **all 1,586,046 rows**. The columns exist because the table is shaped the
same for every country; France's loader fills neither.

So there is no join and no snapshot line. The section shows what France
actually publishes: revenue, gross margin, EBITDA, EBIT, net income and the
ratios. If the French loader later fills equity, adding it is a column, not a
redesign.

**Confidentiality is displayed.** 23.5% of filings are
`Partiellement confidentiel`, plus 50,273 `RAPCAC` and 2,178
`Publication simplifiee`, against 4,951,478 `Public`. French companies may
legally restrict publication, so a missing figure means **withheld, not zero**.
Non-public rows carry a badge and the card carries one line of explanation.

**One row per fiscal year, picked deterministically.** 41,055 `(siren,
fiscal_year)` pairs carry more than one filing because a company can file under
more than one balance type — `C` complete (4,924,259), `S` simplified
(1,586,394), `K` consolidated (31,579). The query takes one with:

```sql
ORDER BY fiscal_year DESC,
         multiIf(balance_type_code = 'C', 0, balance_type_code = 'S', 1, 2)
LIMIT 1 BY fiscal_year
LIMIT 15
```

`LIMIT 1 BY` is the idiom France's own `industryQuery` already uses. Consolidated
ranks last because it describes the group, not the entity whose page this is.
The chosen basis is labelled on the row.

**NOT `argMin` over the balance type.** `argMin` on a tied key picks
arbitrarily between runs — the same defect that made Swedish contract counts
flicker between 301 and 299 from identical data.

Data quality is sound: of 6.54M rows exactly one predates 2000 (1919) and one
is beyond 2026 (2029), so no year filter is warranted. Those two rows surface
only on their own companies' pages.

## Section 2 — Government contracts

France declares `publicContractsQuery` against `fr_government_contracts WHERE
company_id = {id:String}` in the same shape Brazil already uses. No component
change for the table itself. Sole source is `france_decp_procurement`.

Contract counts per company are median 2, p90 15, p99 69, max 1,507, so the
existing `LIMIT 100` covers more than 99% of companies completely.

**France publishes no per-winner values, and that is already handled.**
`value_amount_original` and `value_amount_usd` are NULL for all 721,161 rows,
while `notice_value_amount_original` and `notice_value_amount_usd` are
populated for all 721,161. DECP publishes what the whole procurement was worth,
not what each winner got.

`PublicContractsSection` already does the right thing with exactly this shape
(`public-contracts-section.tsx:89-104`): where there is no per-winner figure it
renders the notice total **labelled as such**, rather than passing it off as
this company's share. No component change, and no attempt to divide a notice
total by its winners.

The summary header, from `fr_government_contract_summary`, therefore shows
**award count, last award date and source list — and no value**.
`public_award_value_usd` is NULL and `public_award_valued_count` is 0 for all
99,287 companies, because both are derived from the per-winner figure France
does not publish. A "total value: —" would state that France awards nothing.
The header renders a value only when the summary carries one, so the countries
that do publish per-winner figures get it for free.

The prop is optional. Countries that declare no `contractSummaryQuery` render
exactly what they render today.

## Section 3 — Wikidata

Sweden's query adapted: match on the `fr_siren` identifier with a fallback to
an FR-jurisdiction LEI drawn from `gleif_lei_records`, plus the companion
people query. Both config keys already exist.

Coverage is 169 companies via SIREN and 168 via LEI, and the two overlap almost
entirely. The section hides itself for every other company, which is the
correct behaviour and the reason wiring it is cheap enough to be worth doing at
this coverage.

## Error handling

- Each new promise gets the no-op `.catch()` guard that closes the
  unhandled-rejection window; the `await Promise.all` still surfaces real
  failures (`queries.server.ts:759-773`). A missing table 500s the page exactly
  as it would for any other country.
- A null figure renders as an em-dash with its confidentiality badge, never as
  `0` or `€0`.
- Every section returns `null` on an empty result rather than an empty card.

## Testing

Live-ClickHouse integration tests, following `tests/public-contracts.queries.test.ts`.

- **`tests/fr-financials.queries.test.ts`** (new) — executes against the live
  schema with `id: "0"` expecting `[]`; then seeds a real SIREN known to have
  several filed years and asserts **exactly one row per fiscal year**. That
  assertion is what catches a broken balance-type pick, which is otherwise
  invisible until someone opens a company with duplicate filings.
- **`tests/public-contracts.queries.test.ts`** picks France up for free — its
  `withContracts` filter is derived from which countries declare the query. Add
  an assertion covering the new summary query in the same file.
- **`app/components/detail/countries/fr-financials.test.ts`** (new) — unit tests
  for the formatting rules: null versus zero, ratio rendering, basis label.
  Mirrors `no-financials.test.ts`.
- Gate: `npm run typecheck && npx vitest run`.

## Out of scope

- **The Wikidata coverage investigation.** French SIREN coverage is 169 against
  Sweden's 3,129 — twenty times worse for a larger economy, which suggests the
  seed extraction never targeted France. That lives in `dagster_v3`, not here.
  It produces a written finding; if it needs a pipeline change, that change gets
  its own spec.
- **`companies_all` and unified financials.** Frozen until 15 countries are
  processed. Nothing here adds a column to either.
- **France's `features` array**, which still reads `["industries"]`. Nothing
  consumes it — the company-list flags come from `COMPANY_FLAG_SOURCES`, where
  France's `financials` flag is already wired to `fr_company_financials_latest`.
  Left alone rather than changed speculatively.
- **France's country contracts page showing no amounts.** Noticed while
  verifying the above, and not caused by this work: `company_contract_rollup`
  derives `amount_original`/`amount_usd` from the per-winner
  `value_amount_original`, which is NULL for every French row, so all 589,291
  French contracts show a blank value on `/countries/fr/contracts`. The notice
  totals are right there in the facts table. Whether the country list should
  fall back to them the way the company detail section already does is a real
  question, and a separate one.
- **Generalising extended financials** to a country-agnostic type. Latvia also
  has a metrics table, but designing a shared shape from one example contradicts
  the "extract shared components only when a pattern repeats" rule this codebase
  already follows. Revisit when a second country needs it.
