# Uruguay investigation

## Conclusion

Uruguay has useful official open company-like data, but the strongest verified
source is sector-scoped. It should stay after the full-register countries.

## Evidence

- `catalogodatos.gub.uy` CKAN API returned the DEI package with CSV, XLSX, XML,
  and JSON metadata resources.
- A bounded CSV sample was downloaded and normalized.
- INE enterprise-directory metadata exists, but a full operational registry API
  was not confirmed in this pass.

## Recommended ingestion

If partial coverage is acceptable, ingest MIEM DEI as a sector directory keyed on
RUT. Continue discovery for a full DGI or legal-entity registry before treating
Uruguay as a full country company source.
