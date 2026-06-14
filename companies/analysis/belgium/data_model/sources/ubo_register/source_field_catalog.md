# UBO Register (Beneficial Ownership) — Field Catalog

> **PLANNING-ONLY (restricted).** Belgian beneficial-ownership register via MyMinfin. Access restricted to
> authorities / obliged entities / **legitimate interest** (fee) — **not open**. Cataloged from public
> documentation only; no records/values copied. Included because beneficial ownership is otherwise
> unavailable openly.

## Source Summary

- Country: Belgium
- Source type: beneficial_ownership_register
- Organization: FOD Financiën / SPF Finances
- URL: https://finances.belgium.be/fr/E-services/ubo-register
- License: restricted (legitimate interest / fee) — planning-only
- Access: restricted (MyMinfin)
- Freshness: continuous
- Record shape: per-company beneficial owners
- Primary keys: EnterpriseNumber + beneficial_owner
- Join keys: EnterpriseNumber

## Fields

| Path | Source field (NL/FR) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| enterpriseNumber | Ondernemingsnummer | Entity | string | identifier | join |
| beneficialOwner.name | uiteindelijke begunstigde | BO name | string | person | **sensitive PII** |
| beneficialOwner.ownership | percentage / control | Ownership nature | string | ownership | |
| category | category | Direct/indirect/SMO | string | ownership | |

## Interpretation Notes

- **Restricted, not open** (post-CJEU). Access only with legitimate interest / as an obliged entity, for a
  fee. Beneficial owners are natural persons → **sensitive PII**; GDPR.
- Joins to the spine via the **EnterpriseNumber** if/when lawfully obtained. Cataloged for completeness;
  do not ingest as open. No `sample_record.json`.
