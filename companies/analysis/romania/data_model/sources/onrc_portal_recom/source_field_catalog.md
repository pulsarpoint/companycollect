# ONRC portal / RECOM online (portal.onrc.ro) Field Catalog

> **PLANNING-ONLY / PAID.** Cataloged from public ONRC product descriptions.
> Full extracts are **paid** per document and partly account/CAPTCHA-gated. No
> raw records or values retrieved. The open bulk CSV (OD_FIRME + companions)
> already supplies the company master data; this source matters only for
> **share capital, shareholders, and history**, which are not open.

## Source Summary

- Country: Romania
- Source type: official_registry
- Organization: ONRC (RECOM online)
- URL: https://portal.onrc.ro/
- License: paid extracts
- Access: paid (account; CAPTCHA on parts)
- Freshness: real-time
- Record shape: planning-only
- Primary keys: `cui`
- Join keys: `cui`, `cod_inmatriculare`

## Fields (from public docs)

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| capital_social | capital social | Share capital | decimal | financial | planning-only; ANAF I11 is a free proxy |
| asociati[] | asociati/actionari | Shareholders | array | ownership | planning-only; PII; NOT in open data |
| administratori[] | administratori | Administrators | array | person | planning-only; overlaps open reps |
| istoric[] | istoric mentiuni | Filing history | array | filing | planning-only |

## Interpretation Notes

- The interactive register portal provides full **certificates** (*furnizare de
  informații*, *certificat constatator*) including **share capital**,
  **shareholders**, **administrators**, and **filing history** — **paid** per
  extract.
- For the open pipeline, the only genuinely missing concepts vs the portal are
  **shareholders/ownership** and **detailed share capital** (a paid-up-capital
  proxy is free via ANAF `I11`). Keep this source **planning-only**.
