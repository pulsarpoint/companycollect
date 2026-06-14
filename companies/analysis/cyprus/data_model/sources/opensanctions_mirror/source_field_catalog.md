# OpenSanctions cy_companies (open data mirror) Field Catalog

## Source Summary

- Country: Cyprus
- Source type: open_data_mirror
- Organization: OpenSanctions / DRCIP
- URL: https://www.opensanctions.org/datasets/cy_companies/ (bulk: https://data.opensanctions.org/datasets/latest/cy_companies/)
- License: **CC-BY-NC 4.0** (commercial use needs a separate OpenSanctions licence)
- Access: public
- Freshness: derived from the data.gov.cy DRCIP CSV
- Record shape: FollowTheMoney entities (Company + Directorship)
- Primary keys: `id` (FTM entity id)
- Join keys: `registrationNumber`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| properties.registrationNumber | registrationNumber | HE registration number | array | identifier | (none) | join key |
| properties.name | name | Company name | array | legal_name | (none) | Greek/English |
| properties.status | status | Status | array | status | (none) | cross-check |
| properties.address | address | Registered address | array | address | (none) | cross-check |
| Directorship.director/.organization | Directorship | Officer ⇒ company link | object | relationship | (none) | PII; officers not shareholders |

## Interpretation Notes

- **Convenient cross-reference, not the authoritative source.** This is a FollowTheMoney mirror of the Cyprus
  DRCIP open CSV (~567,536 companies, ~2.75M entities). It **confirmed** during discovery that the open data
  **names officers but not shareholders**.
- **Licence matters.** The mirror is **CC-BY-NC 4.0** — commercial reuse needs a separate OpenSanctions
  licence. For commercial reuse of the company list, **use the data.gov.cy CSV directly** (`drcip_register`),
  under its open terms. Use this mirror only for cross-referencing/QA.
- No raw values are copied here (NC licence; treat as planning/QA cross-reference).
