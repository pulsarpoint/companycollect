# License notes — France

> Public availability does NOT equal unrestricted reuse. Confirm before redistributing.

## INSEE Base Sirene — **ODbL (Open Database License)**

- Free to use, share, adapt, including commercially.
- **Attribution** required: credit INSEE / source Sirene.
- **Share-alike**: a derived *database* publicly distributed must be offered under
  ODbL as well. Internal use / producing non-database results is less constrained.
- INSEE applies a specific note: data on *physical persons* (entrepreneurs
  individuels) who opted out of public diffusion are excluded / masked
  (`statutDiffusionUniteLegale`). Respect the diffusion status field.
- Ref: https://www.insee.fr/fr/information/3591226

## INPI RNE (Data INPI) — open data, INPI conditions

- Enterprise legal data made available free of charge.
- Governed by INPI's open-data conditions of reuse; attribution expected.
- **Beneficial ownership (RBE)** is regulated separately — access is restricted
  and conditioned by law (not part of free open bulk). Do not assume open reuse.
- Annual accounts: only **non-confidential** filings are open; some companies
  file confidential accounts that are not redistributable.
- Ref: https://data.inpi.fr/ , https://www.inpi.fr/

## BODACC (DILA) — **Licence Ouverte / Open Licence v2.0**

- Free reuse including commercial, with attribution to the source (DILA / BODACC).
- One of the most permissive French public licenses.
- Ref: https://bodacc-datadila.opendatasoft.com/

## API Recherche d'Entreprises (DINUM)

- Public service API; underlying data inherits source licenses (Sirene ODbL,
  RNE open data, etc.). Attribution to original producers expected.
- Respect documented rate limit (~7 req/s) and fair-use.

## API Entreprise (DINUM) — **restricted**

- Reserved for administrations and legally authorized private bodies
  (habilitation required). **Not** open for general reuse. Excluded from
  ingestion recommendations.

## Practical guidance for ingestion

- Keep a `source_name` + `source_url` + `source_retrieved_at` on every record
  (already in the normalized schema) to satisfy attribution.
- If you publicly republish a derived *database* built on Sirene, publish it
  under ODbL and credit INSEE.
- Honor `statutDiffusion*` flags — never expose opted-out individual entrepreneurs.
- Treat RBE / beneficial ownership as restricted unless you obtain proper access.
