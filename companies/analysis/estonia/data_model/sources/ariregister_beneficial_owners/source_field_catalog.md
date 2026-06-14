# e-Business Register open data — beneficial owners (kasusaajad) Field Catalog

## Source Summary

- Country: Estonia
- Source type: beneficial_ownership_register
- Organization: Registrite ja Infosüsteemide Keskus (RIK)
- URL: https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/ettevotja_rekvisiidid__kasusaajad.json.zip
- License: Creative Commons Attribution 4.0 (CC-BY 4.0)
- Access: public (free)
- Freshness: daily
- Record shape: JSON keyed by registrikood; each company has a list of beneficial owners
- Primary keys: `registrikood`
- Join keys: `registrikood`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| kasusaajad[].nimi | nimi | Beneficial owner name | string | ownership | (not parsed) | **PII** |
| kasusaajad[].kontrolli teostamise viis | kontrolli teostamise viis | Nature of control | string | ownership | (not parsed) | direct/indirect |
| kasusaajad[].registrikood | registrikood | Company id | string | identifier | (not parsed) | join key |

## Interpretation Notes

- **Open beneficial ownership — unusual.** Estonia publishes the beneficial-owner register as **open bulk data**
  under CC-BY 4.0, where many EU states restricted public access after the 2022 CJEU ruling. Verified reachable
  (HTTP 200, **27 MB JSON.zip**); not parsed field-by-field here, so no real values are copied and field
  confidence is `medium` (documented from the dataset description / schema notes).
- **GDPR.** Beneficial owners are **personal data** — apply a lawful basis + retention policy before persisting;
  no direct-marketing reuse. CC-BY governs IP, not data protection.
- **Distinct from shareholders.** Beneficial owners (ultimate natural persons) are a separate concept from the
  registered shareholders (`osanikud`). Keep both, do not conflate.
