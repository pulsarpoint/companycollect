# Company data sources for France

Last revalidated: 2026-07-28

## Summary

France has strong open company coverage keyed by the 9-digit SIREN. The existing
Sirene pipeline already provides identity, status, legal form, NAF activity and
head-office address. Financials and several useful enrichment layers can be
added without buying commercial data.

## Best financial sources

1. **Ratios financiers BCE/INPI** — best first ingestion. Public, no account,
   Open Licence 2.0, 6,542,232 rows observed. It exposes revenue, gross margin,
   EBITDA/EBE, EBIT, net income and calculated debt, liquidity, working-capital
   and payment-delay ratios. Query by SIREN or export from the Opendatasoft API.
   Grain is `(siren, date_cloture_exercice, type_bilan)`.
2. **Données financières détaillées des entreprises (Parquet)** — public
   2.82 GB snapshot containing detailed tax-form statement fields (2033/2050
   families). Best no-auth source for full structured statements.
3. **INPI RNE comptes annuels** — authoritative non-confidential annual-account
   filings since 2017, including balance sheet, income statement, fixed assets,
   depreciation and provisions. Free account required for API/SFTP.
4. **API Recherche d'Entreprises `finances`** — simplest lookup endpoint,
   no authentication, returning headline `ca` and `resultat_net` by year.
   It is a search/enrichment API, not an exhaustive bulk extraction route.

The main limitation is confidentiality: approximately 45% of annual-account
filings are confidential according to INPI. Missing figures must therefore be
stored as unknown, never as zero.

## Other useful data

- **Annuaire des Entreprises enriched bulk:** daily Parquet legal-unit and
  establishment data combining Sirene with association/RNA, ESS, société à
  mission, EGAPRO, Qualiopi, training, RGE, FINESS, BIO, SIAE, ADEME aid and
  other flags.
- **INPI RNE:** capital, legal representatives, acts and statutes, establishment
  details and non-confidential accounts.
- **BODACC:** lifecycle events such as creation, modification, radiation,
  insolvency/collective proceedings, sales and account deposits.
- **ADEME Bilans GES:** emissions reports, reporting year, scopes/categories,
  reduction targets and actions, joined by SIREN/SIRET.
- **BALO:** legal and financial notices for companies making public offerings,
  banks and other covered issuers.
- **INPI intellectual property:** trademarks, patents and designs after free
  account registration.
- **BOAMP:** public-procurement tender and award notices. The existing DECP
  contract pipeline remains the better supplier-award spine.

Beneficial-owner data is no longer generally open: RBE access requires an
authorized status or demonstrated legitimate interest. API Entreprise data
(including DGFIP revenue and Banque de France balances) is also restricted and
must not be treated as a general ingestion source.

## Recommended ingestion

1. Keep **Sirene** as the company/establishment spine; its current license is
   **Licence Ouverte 2.0**. Resolve rotating stock resources from the stable
   dataset page instead of hardcoding dated URLs.
2. Add **Ratios BCE/INPI** as a separate ClickHouse fact table keyed by SIREN,
   closing date and balance-sheet type.
3. Add the detailed financial Parquet only if the extra line items justify the
   2.82 GB snapshot and wider schema.
4. Use the daily **Annuaire enriched Parquet** for flags and classifications,
   and BODACC/ADEME/BALO as source-specific event or fact tables.
5. Add INPI RNE API/SFTP later for authoritative legal documents and daily
   annual-account detail; keep confidentiality/provenance fields.

See `source_inventory.md`, `schema_notes.md` and `investigation.md`. Current
bounded API samples and SHA-256 metadata are under
`../../data/france/raw/api/`.
