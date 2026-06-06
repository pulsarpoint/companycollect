# Company data sources for France

## Status

- Official bulk data: **found** (INSEE Sirene full stock; INPI RNE via SFTP)
- Official API: **found** (API Sirene, API Recherche d'Entreprises, RNE API, BODACC API)
- Open data portal: **found** (data.gouv.fr, official national portal)
- License: **known** — Sirene = ODbL; BODACC = Licence Ouverte 2.0; RNE = open data
- Recommended ingestion path: **bulk (Sirene + RNE) + daily API deltas**

## Best source

**INSEE Base Sirene** is the authoritative master list of every French legal
unit (SIREN, ~25M) and establishment (SIRET, ~36M), published as open data
under ODbL on data.gouv.fr. It is the canonical company identifier system in
France. For richer *legal* data (capital, directors, beneficial owners, acts,
annual accounts) complement it with the **INPI RNE** bulk feed.

For zero-friction prototyping, the **API Recherche d'Entreprises** (DINUM) is
public, needs no key, and already merges Sirene + RNE into one searchable index
— verified returning live data in this investigation.

## Next action

1. Prototype with the no-auth API Recherche d'Entreprises (already working).
2. For full ingestion: download `StockUniteLegale` (~960 MB) + `StockEtablissement`
   (~2.83 GB) Parquet/CSV from the Sirene stable landing page; load into Postgres/DuckDB.
3. Register a free account on `portail-api.insee.fr` for daily-delta API Sirene.
4. Register on `data.inpi.fr` for RNE SFTP bulk to enrich with legal/dirigeant data.

See `investigation.md` for full detail and `source_inventory.md` for the table.
