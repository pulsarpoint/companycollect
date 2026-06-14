# GLEIF LEI (Italian holders) — Field Catalog

> **Open (CC0)** global LEI data; filter to Italy. Covers only **LEI-holding entities** (a subset — larger
> / regulated / internationally active firms). No financials, but it **bridges LEI ↔ Italian CF/REA** and
> carries (partial) ownership relationships. Cataloged from the documented GLEIF LEI-CDF / API schema.

## Source Summary

- Country: Italy
- Source type: open_identifier_registry
- Organization: GLEIF
- URL: https://api.gleif.org/api/v1/lei-records ; golden copy https://www.gleif.org/en/lei-data/gleif-golden-copy
- License: **CC0** (public domain)
- Access: public, no auth (fair-use API)
- Freshness: daily golden copy
- Record shape: JSON:API records (or LEI-CDF XML / CSV)
- Primary keys: `lei`
- Join keys: `lei`, `entity.registeredAs` (REA/CF), `legalName`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| data[].attributes.lei | lei | LEI (20-char) | string | identifier | global key |
| …entity.legalName.name | legalName | Legal name | string | legal_name | |
| …entity.registeredAs | registeredAs | National reg id | string | identifier | **REA/CF bridge** |
| …registationAuthority.registrationAuthorityID | registrationAuthorityID | RA code | string | metadata | RA000407 = IT register |
| …entity.legalAddress | legalAddress | Legal address | object | address | country=IT |
| …entity.legalForm.id | legalForm.id | ELF code | string | legal_form | ISO 20275 |
| …entity.status | entity.status | ACTIVE/INACTIVE | string | status | |
| …registration.status | registration.status | LEI status | string | status | LAPSED = stale |
| …relationships (L2) | parent/ultimate | Ownership (LEI↔LEI) | object | ownership | partial |

## Interpretation Notes

- **Cross-reference, not a master.** Use GLEIF to map **LEI ↔ Italian CF/REA** (`registeredAs` +
  `registrationAuthorityID`) and to enrich larger entities; it is a **subset** of all Italian companies.
- **Ownership (Level 2)** gives direct/ultimate parent links where both parties have LEIs and reported the
  relationship — a partial open corporate-group signal Italy otherwise lacks openly.
- **`registration.status=LAPSED`** flags records not renewed (potentially stale).
- No `sample_record.json` retrieved this run (follow-up: pull `?filter[entity.legalAddress.country]=IT`).
