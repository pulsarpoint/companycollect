# Colombia investigation

## Conclusion

Colombia is a strong target. The Socrata endpoint is live, structured, and easy to
sample. It carries both legal entities and natural-person merchants.

## Evidence

- `https://www.datos.gov.co/resource/c82u-588k.json?$limit=1` returned HTTP 200.
- Metadata exposes field names/types and CC BY-SA 4.0 license.
- A legal-entity sample was downloaded with a Socrata `$where` filter.

## Recommended ingestion

Use Socrata API or CSV export as a snapshot. Preserve `matricula`, chamber code,
NIT/check digit, and legal status. Exclude or separately handle natural-person
records if the product scope is companies only.
