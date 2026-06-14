# RBE — Registre des bénéficiaires effectifs Field Catalog

> **Planning-only / restricted.** General public access withdrawn after the 2022 CJEU ruling (professionals /
> legitimate interest only). Not open bulk. No records/values copied. No `sample_record.json`.

## Source Summary

- Country: Luxembourg
- Source type: beneficial_ownership_register
- Organization: Luxembourg Business Registers (LBR)
- URL: https://www.lbr.lu/
- License: restricted (access conditions; post-CJEU)
- Access: restricted
- Freshness: continuous
- Record shape: access-controlled register
- Primary keys: `rcs_number`
- Join keys: `rcs_number`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| beneficial_owners[].name | bénéficiaire effectif | UBO natural person | string | ownership | (restricted) | GDPR; restricted |
| beneficial_owners[].nature_extent_control | nature et étendue du contrôle | Interest/control | string | ownership | (restricted) | planning-only |

## Interpretation Notes

- **Restricted, not open.** After the 2022 CJEU ruling, general public access to the RBE was withdrawn (access
  for professionals / those with a legitimate interest). Treat the whole source as **planning-only**; do not
  attempt to bypass access controls. Beneficial owners are **personal data (GDPR)**. Join on `rcs_number`.
- Distinct from **directors/officers** (which appear in the free filed RCS documents).
