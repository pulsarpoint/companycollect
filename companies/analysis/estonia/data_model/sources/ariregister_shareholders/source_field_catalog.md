# e-Business Register open data — shareholders (osanikud) Field Catalog

## Source Summary

- Country: Estonia
- Source type: official_registry
- Organization: Registrite ja Infosüsteemide Keskus (RIK)
- URL: https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/ettevotja_rekvisiidid__osanikud.json.zip
- License: Creative Commons Attribution 4.0 (CC-BY 4.0)
- Access: public (free)
- Freshness: daily
- Record shape: JSON keyed by registrikood; each company has a list of shareholders/members
- Primary keys: `registrikood`
- Join keys: `registrikood`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| osanikud[].nimi | nimi | Shareholder/member name | string | ownership | (not parsed) | **PII** if natural person |
| osanikud[].osa suurus | osa suurus | Shareholding size | decimal | ownership | (not parsed) | holding amount |
| osanikud[].registrikood | registrikood | Company id | string | identifier | (not parsed) | join key |

## Interpretation Notes

- **Open shareholders.** Estonia publishes registered shareholders/members (`osanikud`, for OÜ/AS) as **open
  bulk** under CC-BY 4.0. Verified reachable (HTTP 200, **33 MB JSON.zip**); not parsed field-by-field here, so
  field names/confidence are documented (`medium`) and no real values are copied.
- **Registered shareholders ≠ beneficial owners.** `osanikud` is the registered ownership; `kasusaajad` is the
  ultimate beneficial owner. Keep both as distinct `owners` vs `beneficial_owners` sections.
- **GDPR.** Natural-person shareholders are personal data — lawful basis + retention; no direct marketing.
