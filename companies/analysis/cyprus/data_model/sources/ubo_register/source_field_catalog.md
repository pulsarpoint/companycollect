# UBO register (beneficial ownership) — DRCIP Field Catalog

> **Planning-only / restricted.** Access conditions and a fee apply (post-CJEU). Not open bulk. Fields are
> described from public documentation; no records or values are copied. No `sample_record.json`.

## Source Summary

- Country: Cyprus
- Source type: beneficial_ownership_register
- Organization: Department of Registrar of Companies and Intellectual Property (DRCIP)
- URL: https://www.companies.gov.cy/en/
- License: access conditions apply (legitimate interest / fee; post-CJEU)
- Access: restricted
- Freshness: continuous
- Record shape: access-controlled HTML register
- Primary keys: `registration_number`
- Join keys: `registration_number`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| beneficial_owners[].name | beneficial_owner | UBO natural person | string | ownership | (none — restricted) | GDPR; restricted |
| beneficial_owners[].ownership_percentage | ownership_percentage | Interest/control held | decimal | ownership | (none — restricted) | planning-only |

## Interpretation Notes

- **Restricted, not open.** Following the CJEU ruling on public BO access, the Cyprus UBO register is accessed
  under conditions (legitimate interest / fee). Treat the whole source as **planning-only**; do not attempt to
  bypass access controls.
- **Three distinct ownership/person concepts for Cyprus:** open **officers** (directors/secretary, in the DRCIP
  CSV), **shareholders** (on the paid HE32 annual return), and **beneficial owners** (this restricted register).
  Keep them separate; do not conflate.
- **GDPR.** Beneficial owners are personal data — planning-only.
