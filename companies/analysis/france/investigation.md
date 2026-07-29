# Investigation — company data for France

Dates: initial research 2026-06-06; financial follow-up 2026-06-14; current
revalidation and expansion 2026-07-28.

## Summary

France supports a practical SIREN-centred company model. Sirene remains the
authoritative identity spine, while public financial, legal, lifecycle, ESG,
certification and intellectual-property sources can be joined to it. The
current project already ingests Sirene identity/address/activity data and DECP
contracts, so the highest-value missing layer is structured financial facts.

## Financial data

### 1. Ratios financiers BCE/INPI — preferred first source

The DGE publishes INPI-derived annual figures and calculated ratios through the
public data.economie.gouv.fr API and exports. At revalidation the API reported
6,542,232 records. Observed fields include:

- revenue, gross margin, EBE/EBITDA, EBIT and net income;
- debt, liquidity, age-of-assets and financial-autonomy ratios;
- working-capital, interest-coverage, cash-flow and repayment-capacity ratios;
- inventory, customer and supplier payment days;
- SIREN, closing date, balance-sheet type and confidentiality status.

The La Poste sample returned 17 records. It has separate complete (`C`) and
consolidated (`K`) rows for the same period, so the ingestion key must include
`type_bilan`.

### 2. Detailed financial Parquet — preferred full no-auth source

The Signaux Faibles dataset publishes a 2,820,473,022-byte Parquet snapshot with
SIREN, closing date, balance type and detailed base tax-form 2033/2050 fields.
It avoids account setup but needs a tax-form-code dictionary, schema profiling
and enough storage/memory for a multi-gigabyte load.

### 3. INPI RNE comptes annuels — authoritative filing source

INPI provides non-confidential annual accounts since 2017 through a free-account
API/SFTP service. The source includes balance-sheet assets/liabilities, income
statement, fixed assets, depreciation and provisions. INPI reports around
1.5 million filings per year and approximately 45% confidentiality, so open
coverage is materially incomplete and skewed away from smaller companies.

### 4. Recherche API — convenient lookup

The public search endpoint still returns `finances` keyed by year with `ca` and
`resultat_net`. It also returns officers, headquarters and many complements.
It is rate-limited to about seven requests per second and explicitly is not an
exhaustive company-base download, so it should not be used as the bulk spine.

## Additional data

### Enriched Annuaire bulk

Daily legal-unit and establishment Parquet files combine Sirene with official
enrichments: association/RNA, ESS, société à mission, EGAPRO, responsible
procurement, Alim'Confiance, spectacle licences, patrimoine vivant, training
organizations, Qualiopi, SIAE, FINESS, ADEME aid, lawyers and collective
agreements; establishment flags also include BIO, RGE and UAI.

### Legal and lifecycle

- INPI RNE adds capital, representatives, acts/statutes and richer legal data.
- BODACC is the lifecycle event stream for formations, changes, closures,
  insolvency/collective proceedings, business sales and account deposits.
- BALO adds notices from issuers, banks and companies making public offerings.

### ESG, public sector and innovation

- ADEME BEGES exposes SIREN/SIRET-linked greenhouse-gas reports, categories,
  reporting years, targets and action plans.
- BOAMP provides tender and award notices. It complements rather than replaces
  the existing DECP contract-award pipeline.
- INPI APIs expose trademarks, patents and designs after free registration.

## Restricted routes

- RBE beneficial-owner access is restricted to authorized parties or applicants
  demonstrating legitimate interest. General public access ended in 2024.
- API Entreprise is for authorized public-service use. Its DGFIP revenue and
  Banque de France balance endpoints are not general open-data sources.
- Missing financial data may mean confidentiality, no filing, an individual
  enterprise or an out-of-scope entity; it must never be interpreted as zero.

## Recommended ingestion

1. Keep Sirene legal units and establishments as dimensions.
2. Load Ratios BCE/INPI to a financial fact table at
   `(siren, closing_date, balance_type)`.
3. Preserve confidentiality and provenance on every financial row.
4. Add Annuaire enriched Parquet fields that are valuable for search/signals.
5. Model BODACC/BALO as events and ADEME BEGES as a separate ESG fact table.
6. Add detailed Parquet or authenticated INPI statements only after defining
   the desired line-item model.

## Risks and open questions

- Confirm whether ratio exports are full replacement snapshots and how revisions
  are represented before production scheduling.
- Confirm the detailed-Parquet refresh cadence; the observed resource was dated
  2026-02-10.
- Decide whether complete, simplified and consolidated statements should coexist
  or whether product views should prefer one `type_bilan`.
- Sirene and Annuaire stock file resource URLs rotate. Resolve them from stable
  data.gouv.fr pages.
- NAF Rev.2 and NAF2025 coexist during the transition; retain both codes.
- Respect `statutDiffusion=P` and avoid exposing personal data for opted-out
  individual entrepreneurs.
