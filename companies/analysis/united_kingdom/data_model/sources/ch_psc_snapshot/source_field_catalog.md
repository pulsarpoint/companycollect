# Companies House — PSC Snapshot Field Catalog

> **PERSONAL DATA.** PSC records name individuals with month/year of birth,
> nationality, and partial address. Open (OGL) but redact person-level fields. No
> `sample_record.json` (would expose personal data).

## Source Summary

- Country: United Kingdom
- Source type: beneficial_ownership
- Organization: Companies House
- URL: http://download.companieshouse.gov.uk/en_pscdata.html (also REST API)
- License: Open Government Licence (OGL)
- Access: public (bulk snapshot, no key)
- Freshness: daily snapshot
- Record shape: NDJSON; PSC records per company
- Primary keys: `company_number` + `psc_id`
- Join keys: `company_number`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| company_number | company_number | Company | string | identifier | join key |
| data.name | name | PSC name | string | ownership | PII (individuals) |
| data.kind | kind | PSC kind | string | ownership | individual/corporate/legal |
| data.natures_of_control | natures_of_control | Control type | array | ownership | shares/voting % bands |
| data.notified_on | notified_on | Notified date | date | date | |
| data.nationality | nationality | Nationality | string | person | PII |
| data.date_of_birth | date_of_birth | Birth month/year | object | person | PII (day suppressed) |

## Interpretation Notes

- The **beneficial-ownership** layer (persons with significant control), free as a
  daily bulk snapshot (NDJSON) and via the REST API. Join on **company number**.
- `natures_of_control` gives the ownership/voting band (e.g.
  `ownership-of-shares-75-to-100-percent`) — the key signal; the PSC may be an
  **individual**, **corporate entity**, or **legal person**.
- **GDPR**: redact/minimise person data (name, DOB, nationality); keep corporate
  PSCs and control bands. Have a lawful basis before persisting individual PSCs.
