# Finland — Financial Data Gap Analysis And Additional Sources

Date checked: 2026-07-19

## What we currently have (ClickHouse, prod)

Source pipelines: `finland_ytj` (register) and `finland_xbrl` (PRH digital
financial statements, registration dates 2023-07-01 onward).

| Table | Rows |
|---|---:|
| `fi_companies` | 460,988 |
| `fi_financial_statements` | 46,102 |
| `fi_financial_metrics` | 46,102 |
| `fi_company_financials_latest` | 21,315 |
| `fi_xbrl_facts_raw` | 3,461,944 |

Statements by financial year (registration-date ingestion window explains the
ramp): FY2022 1,689 · FY2023 11,462 · FY2024 15,412 · FY2025 16,328 ·
FY2026 1,076 (partial).

Metric fill rates inside the 46k statements are good: total assets 97.9%,
profit/loss 91.6%, operating profit 91.4%, equity 94.1%, revenue 79.7%,
personnel expenses 68.6%. `employees` is 0% — the PRH OYTP taxonomy facts we
map do not carry headcount.

### Coverage gap by legal form (active companies)

| Legal form | Companies | With financials | % |
|---|---:|---:|---:|
| Limited company | 316,737 | 20,499 | 6.5 |
| Housing corporation | 92,578 | 1 | ~0 |
| Limited partnership | 20,161 | 1 | ~0 |
| Mutual real estate ltd | 17,574 | 35 | 0.2 |
| Partnership | 6,929 | 0 | 0 |
| Cooperative | 3,139 | 82 | 2.6 |
| **Public limited company** | **298** | **0** | **0** |

Two distinct gaps:

1. **The long tail (~95% of companies)** — they file paper/PDF statements to
   the Trade Register; only digital IXBRL filers appear in the PRH open API.
2. **Listed companies (all 298 Oyj at 0%)** — they publish ESEF annual
   reports through the Nasdaq Helsinki OAM, which is a separate channel the
   PRH digital API does not expose.

## What more can be obtained from the internet

### 1. Verohallinto public corporate income tax data — recommended, free

- Page: `https://www.vero.fi/tietoa-verohallinnosta/tilastot/avoin_dat/`
- 2024 file: `https://www.vero.fi/contentassets/a7b04897ccd44feda446bd1894c1fd74/yhteis%C3%B6_tuloverotus_julk_2024.csv`
- CC-BY-4.0, annual CSV (published early November for prior tax year),
  years 2020–2024 live on vero.fi; 2011–2014 on avoindata.suomi.fi.
- Verified 2024 contents: **384,627 entities**, columns: tax year, business
  ID, name, municipality, **taxable income, total taxes assessed, refund,
  residual tax**. 124,650 entities with taxable income > 0.
- Overlap with `fi_companies`: **274,355 (60% of our register)** — an 18×
  jump over the 21k companies that have any financial signal today. The
  110k non-overlapping IDs are mostly associations/foundations outside YTJ
  open data.
- Limitation: it is a profitability/size *signal* (tax base), not a full
  P&L/balance sheet. Latin-1 encoding, `;` delimiter, decimal comma.
- Companion file: income tax amendments 2022–2024 (reassessments).

### 2. ESEF filings for listed companies via filings.xbrl.org — recommended, free

- API: `https://filings.xbrl.org/api/filings` (JSON:API; filter
  `country eq FI`), full iXBRL zip per filing via `package_url`.
- Verified live: **1,168 Finnish filings**, FY2020 onward (e.g. Citycon,
  F-Secure, Sanoma, Caverion, Enento).
- IFRS consolidated statements — far richer than the PRH OYTP metric set.
- Entities are keyed by **LEI**; join to Y-tunnus through the existing
  `gleif` Dagster module.
- Directly fixes the "0 of 298 public limited companies" gap.

### 3. Time fixes the long tail: mandatory iXBRL filing 2027/2028

Finland makes structured (Inline XBRL) filing of financial statements to PRH
**mandatory in 2027** for companies required to appoint an auditor, expanding
in **2028 to most limited companies and partnerships**; PRH targets all
trade-register accounts in structured form by 2028
(`https://www.xbrl.org/news/finland-moves-to-mandatory-xbrl-reporting-for-company-accounts/`).
The existing `finland_xbrl` pipeline therefore converges to near-full coverage
without new engineering — the FY2023→FY2025 growth (11.5k→16.3k) already shows
adoption rising.

### 4. Options considered and not recommended

- **Virre paid documents** (EUR 2.01–4.02/statement): only economic as a
  targeted backfill for specific high-value companies pre-2027; see
  `financials_and_virre_pricing.md`.
- **Nasdaq Helsinki OAM**: legal home of ESEF reports but no bulk/machine
  interface found; filings.xbrl.org supersedes it for ingestion.
- **Commercial aggregators** (Suomen Asiakastieto, Alma Talent/Kauppalehti,
  Finder.fi): paid and/or scraping-restricted; excluded per open-data policy.
- **Statistics Finland (StatFin)**: aggregate enterprise statistics only, no
  company-level financials.

## Recommended implementation order

1. **`finland/verotax`** — new small Dagster source (mirror `latvia_ur`
   bulk-CSV shape): annual CSV → DuckDB (DuckDB-native `read_csv`,
   `all_varchar`, Latin-1) → ClickHouse `fi_tax_records` keyed
   `(business_id, tax_year)`. One file per year 2020–2024, monthly schedule
   is overkill — annual refresh in November plus manual backfill.
2. **`finland/esef`** — index crawl of filings.xbrl.org (country=FI),
   download iXBRL zips to S3, parse with an IFRS-aware pass (Arelle or
   lxml + ESEF core taxonomy), LEI→business ID via `gleif`. This is a
   cross-country capability: the same repository serves every EU/EEA
   listed-company source.
3. **No change** to `finland_xbrl` — it becomes the primary full-coverage
   source automatically in 2027–2028.
4. Keep **Virre** documented as paid targeted backfill only.

## Data saved

- `data/finland/raw/bulk/vero_yhteiso_tuloverotus_julk_2024.csv` (+ metadata,
  sha256 recorded)
- `data/finland/raw/api/filings_xbrl_org_fi_page_1.json` (+ metadata)

## Open questions / risks

- filings.xbrl.org redistribution terms should be confirmed before
  republishing parsed ESEF content (index access is open; underlying filings
  are regulated public information).
- Vero data uses the taxpayer's *taxation* municipality and name at
  assessment time — names can lag the register; join on Y-tunnus only.
- Vero taxable income ≠ accounting profit (tax adjustments, group
  contributions); label the metric distinctly (`taxable_income`), do not map
  it onto `profit_loss`.
- ESEF consolidated group figures must not be mixed with PRH standalone
  statutory figures for the same business ID without a `scope` flag
  (consolidated vs standalone).
