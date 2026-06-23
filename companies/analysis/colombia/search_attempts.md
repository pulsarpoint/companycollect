# Colombia search attempts

## Attempt 1

- Date/time: 2026-06-23
- Source: datos.gov.co Socrata API
- Query: `resource/c82u-588k.json?$limit=1`
- Result: API returned registry records and headers.
- Decision: Recommended.

## Attempt 2

- Date/time: 2026-06-23
- Source: Socrata metadata API
- Query: `/api/views/c82u-588k`
- Result: Found field list, update timestamp, and license.
- Decision: Save metadata and sample.

## Attempt 3

- Date/time: 2026-06-23
- Source: Socrata API filtered sample
- Query: legal entities where `organizacion_juridica != "PERSONA NATURAL"`
- Result: Downloaded legal-entity sample.
- Decision: Use filtered legal-entity sample for normalization.
