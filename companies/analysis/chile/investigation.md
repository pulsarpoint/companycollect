# Chile investigation

## Conclusion

Chile is a high-priority source. The RES dataset is easy to access through CKAN
metadata and yearly CSV downloads. SII adds a useful tax-side enrichment layer.

## Evidence

- CKAN package API returned the RES package and resources from 2013 through a 2026
  file cut at 2026-05-31.
- The 2026 RES CSV sample was downloaded and normalized.
- SII public pages list ZIP files for legal entities, economic activities,
  addresses, company bands, and composition of companies.

## Recommended ingestion

Use a snapshot-style loader:

1. Fetch CKAN package metadata.
2. Download all RES yearly CSV resources.
3. Parse semicolon-delimited CSV with UTF-8 BOM handling.
4. Normalize RUT as the national registration/tax id.
5. Add SII ZIP enrichment as a second source package after licensing review.
