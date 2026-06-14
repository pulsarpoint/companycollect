# RBO — Register of Beneficial Ownership Field Catalog

> **Planning-only / restricted.** Access conditions apply (post-CJEU). Not open bulk. No records/values copied.
> No `sample_record.json`.

## Source Summary

- Country: Ireland
- Source type: beneficial_ownership_register
- Organization: Registrar of Beneficial Ownership (RBO)
- URL: https://rbo.gov.ie/
- License: restricted (access conditions; post-CJEU)
- Access: restricted
- Freshness: continuous
- Record shape: access-controlled register
- Primary keys: `company_num`
- Join keys: `company_num`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| beneficial_owners[].name | beneficial owner | UBO natural person | string | ownership | (restricted) | GDPR; restricted |
| beneficial_owners[].nature_and_extent | nature and extent of control | Interest/control | string | ownership | (restricted) | planning-only |

## Interpretation Notes

- **Restricted, not open.** After the 2022 CJEU ruling, general public access to the RBO was withdrawn (access
  for designated persons / those with a legitimate interest). Treat the whole source as **planning-only**; do
  not attempt to bypass access controls. Beneficial owners are **personal data (GDPR)**. Join on `company_num`.
- Distinct from **directors/officers** (which appear in filed financial-statement documents, also paid).
