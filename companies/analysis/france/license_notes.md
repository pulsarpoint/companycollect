# License and access notes — France

Revalidated 2026-07-28. Public availability does not override privacy,
confidentiality or source-specific access rules.

## Licence Ouverte / Open Licence 2.0

The current official pages identify these sources as Open Licence 2.0:

- INSEE Base Sirene and API Sirene;
- Ratios financiers BCE/INPI;
- detailed financial Parquet;
- Annuaire enriched legal-unit/establishment bulk;
- BODACC and BALO;
- ADEME BEGES;
- Documents et comptes des entreprises.

Commercial reuse is allowed with source attribution. Retain `source_name`,
`source_url`, `source_retrieved_at` and the raw-response checksum.

## INPI RNE and intellectual-property data

INPI makes enterprise and IP data available under its reuse conditions, normally
after free account registration for API/SFTP access. Attribute INPI and review
the current conditions before redistribution.

Only non-confidential annual accounts are openly available. INPI reports that
approximately 45% of annual-account filings are confidential. Do not derive,
impute or expose protected figures.

## Privacy and diffusion status

Respect Sirene/RNE diffusion flags. In particular, `statutDiffusion=P` can
indicate partially diffused information for an individual entrepreneur. Avoid
publishing names, precise addresses or other protected personal data unless the
source explicitly permits it.

## Restricted sources

- **RBE beneficial owners:** since 31 July 2024, access is limited to authorized
  actors and applicants showing legitimate interest. It is not a general open
  company-enrichment source.
- **API Entreprise:** requires habilitation based on a public-service/legal
  mission. DGFIP revenue and Banque de France balance data accessed through it
  is not authorized for general open ingestion.

## Operational guidance

- Store missing financial figures as null/unknown, not zero.
- Keep `confidentiality` and `type_bilan` from the financial sources.
- Record resource version and retrieval time because bulk URLs and snapshots
  change.
- Recheck source terms before any public redistribution.
