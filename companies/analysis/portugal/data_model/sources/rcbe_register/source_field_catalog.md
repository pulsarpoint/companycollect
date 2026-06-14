# RCBE — Registo Central do Beneficiário Efetivo Field Catalog

> **Planning-only / restricted.** General public access restricted after the 2022 CJEU ruling (legitimate
> interest / obliged entities). Not open bulk. No records/values copied. No `sample_record.json`.

## Source Summary

- Country: Portugal
- Source type: beneficial_ownership_register
- Organization: IRN / Ministério da Justiça
- URL: https://rcbe.justica.gov.pt/
- License: restricted (access conditions; post-CJEU)
- Access: restricted
- Freshness: continuous
- Record shape: access-controlled register
- Primary keys: `nipc`
- Join keys: `nipc`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| beneficiarios[].nome | beneficiário efetivo | UBO natural person | string | ownership | (restricted) | GDPR; restricted |
| beneficiarios[].natureza_controlo | natureza e extensão do controlo | Interest/control | string | ownership | (restricted) | planning-only |

## Interpretation Notes

- **Restricted, not open.** Following the 2022 CJEU ruling, general public access to the RCBE was restricted
  (legitimate interest / obliged entities). Treat the whole source as **planning-only**; do not attempt to
  bypass access controls. Beneficial owners are **personal data (GDPR)**. Join on `nipc`.
- **Distinct from registered shareholders (sócios)** in the commercial register (paid). Keep beneficial owners
  and registered owners as separate sub-concepts.
