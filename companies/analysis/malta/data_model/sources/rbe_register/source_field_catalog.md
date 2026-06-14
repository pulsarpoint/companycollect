# MBR Register of Beneficial Owners (UBO) Field Catalog

> **Planning-only / restricted.** Access for those demonstrating a legitimate interest (post-CJEU, since July
> 2025). Not open bulk. No records/values copied. No `sample_record.json`.

## Source Summary

- Country: Malta
- Source type: beneficial_ownership_register
- Organization: Malta Business Registry (MBR)
- URL: https://mbr.mt/
- License: restricted (legitimate interest; post-CJEU)
- Access: restricted
- Freshness: continuous
- Record shape: access-controlled register
- Primary keys: `registration_number`
- Join keys: `registration_number`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| beneficial_owners[].name | beneficial owner | UBO natural person | string | ownership | (restricted) | GDPR; restricted |
| beneficial_owners[].nature_extent | extent/nature of interest | Beneficial interest | string | ownership | (restricted) | planning-only |

## Interpretation Notes

- **Restricted, not open.** After the 2022 CJEU ruling, general public access was withdrawn; since **July 2025**
  access is for those demonstrating a **legitimate interest** (without alerting the company). Treat the whole
  source as **planning-only**; do not attempt to bypass access controls. Beneficial owners are **personal data
  (GDPR)**. Join on `registration_number`.
- **Distinct from registered shareholders** (which are in the register, paid). Beneficial owner = ultimate
  natural person; keep them as separate `owners` sub-concepts.
