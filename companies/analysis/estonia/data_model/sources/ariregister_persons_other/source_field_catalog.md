# e-Business Register open data — persons on registry card + other datasets Field Catalog

## Source Summary

- Country: Estonia
- Source type: official_registry
- Organization: Registrite ja Infosüsteemide Keskus (RIK)
- URL: https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/ettevotja_rekvisiidid__kaardile_kantud_isikud.json.zip
- License: Creative Commons Attribution 4.0 (CC-BY 4.0)
- Access: public (free)
- Freshness: daily
- Record shape: JSON keyed by registrikood (persons on the registry card); plus registrikaardid / kommertspandid / maarused datasets
- Primary keys: `registrikood`
- Join keys: `registrikood`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| kaardile_kantud_isikud[].nimi | nimi | Person name | string | person | (not parsed) | **PII** |
| kaardile_kantud_isikud[].roll | roll | Role (e.g. board member) | string | relationship | (not parsed) | officers |
| registrikood | registrikood | Company id | string | identifier | (not parsed) | join key |

## Interpretation Notes

- **Officers and related datasets.** `kaardile_kantud_isikud` lists persons on the registry card — board
  members / representatives (officers). Several other open datasets share the same `registrikood` key:
  `registrikaardid` (registry cards), `kommertspandid` (commercial pledges), `maarused` (court rulings),
  `kandevalised_isikud`. All CC-BY 4.0, daily.
- **GDPR.** Officer names are personal data — lawful basis + retention; no direct marketing.
- Documented from the dataset list/schema (not parsed field-by-field here); confirm exact field names on first
  parse.
