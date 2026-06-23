# Israel investigation

## Conclusion

Israel is a strong Middle East candidate. The official Ministry of Justice
company registry is available through `data.gov.il` and the datastore API can be
used without authentication.

## Evidence

- `package_search` for the Hebrew Companies Registrar term found `ica_companies`,
  `ica_partnerships`, and change datasets.
- `datastore_search` for the company resource returned field metadata and records.
- Direct CSV download returned a challenge page from this environment, so API
  pagination is the safer first implementation path.

## Recommended ingestion

Use CKAN datastore pagination. Normalize Hebrew field names to stable English
internal names. Preserve raw Hebrew values as source truth.
