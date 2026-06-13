# Company data sources for France

## Status

### Company registry data
- Official bulk data: **found** (INSEE Sirene full stock; INPI RNE via SFTP)
- Official API: **found** (API Sirene, API Recherche d'Entreprises, RNE API, BODACC API)
- Open data portal: **found** (data.gouv.fr, official national portal)
- License: **known** — Sirene = ODbL; BODACC = Licence Ouverte 2.0; RNE = open data
- Recommended ingestion path: **bulk (Sirene + RNE) + daily API deltas**

### Financial data (annual accounts / comptes annuels) — OPEN
- Official bulk data: **found** — **INPI RNE comptes annuels** (non-confidential balance sheet +
  income statement + fixed assets/depreciation/provisions, since 2017-01-01; JSON since 2023) via
  data.inpi.fr SFTP/API (free account).
- Official API: **found, no auth** — the **API Recherche d'Entreprises** returns a `finances` block
  with **`ca` (revenue) + `resultat_net` (net income) per year** for companies with non-confidential
  accounts. **Verified live** (La Poste 2024: CA €34.569B, résultat net €1.722B).
- License: open (RNE open data; underlying figures inherit source terms).
- Coverage caveat: companies can opt for **confidentiality** of accounts (micro/small), and
  micro-entrepreneurs are excluded from DGFIP revenue — so financial coverage is **partial**.
- Recommended ingestion path: **`finances` block from Recherche API** for quick CA/résultat-net at
  scale (no auth) + **INPI RNE comptes annuels bulk** for full balance-sheet/income-statement detail.

## Best source

**INSEE Base Sirene** is the authoritative master list of every French legal
unit (SIREN, ~25M) and establishment (SIRET, ~36M), published as open data
under ODbL on data.gouv.fr. It is the canonical company identifier system in
France. For richer *legal* data (capital, directors, beneficial owners, acts,
annual accounts) complement it with the **INPI RNE** bulk feed.

For zero-friction prototyping, the **API Recherche d'Entreprises** (DINUM) is
public, needs no key, and already merges Sirene + RNE into one searchable index
— verified returning live data in this investigation. It also exposes an open
**`finances`** block (CA + résultat net per year), making France one of the few
countries where headline financials are available with **no auth and no payment**.

**Best financial source:** for full statements use **INPI RNE comptes annuels**
(open bulk/API, balance sheet + income statement since 2017); for fast headline
figures use the Recherche API `finances` block. Both are open; coverage is partial
because of the legal **confidentiality option** on small-company accounts.

## Next action

1. Prototype with the no-auth API Recherche d'Entreprises (already working).
2. For full ingestion: download `StockUniteLegale` (~960 MB) + `StockEtablissement`
   (~2.83 GB) Parquet/CSV from the Sirene stable landing page; load into Postgres/DuckDB.
3. Register a free account on `portail-api.insee.fr` for daily-delta API Sirene.
4. Register on `data.inpi.fr` for RNE SFTP bulk to enrich with legal/dirigeant data
   **and full comptes annuels (balance sheet + income statement)**.
5. **Financials:** harvest the `finances` block (CA + résultat net) from the no-auth Recherche API now;
   add INPI RNE comptes annuels bulk for full statements. Flag confidentiality gaps.

See `investigation.md` for full detail and `source_inventory.md` for the table.
