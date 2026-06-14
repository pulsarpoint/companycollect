# Publicações de atos societários (publicacoes.mj.pt) Field Catalog

> Field model documented from the portal. **No `sample_record.json`**: the search is reCAPTCHA-protected, so no
> per-company record was lawfully downloaded in bulk; no real values copied.

## Source Summary

- Country: Portugal
- Source type: official_gazette
- Organization: Ministério da Justiça
- URL: https://publicacoes.mj.pt/ (search /pesquisa.aspx — reCAPTCHA-gated)
- License: public (free to consult); reuse terms unclear
- Access: public (manual); search reCAPTCHA-protected
- Freshness: continuous
- Record shape: act publications (HTML)
- Primary keys: `publication_id`
- Join keys: `nipc`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| nipc | NIPC | Company id | string | identifier | (not copied) | join key |
| firma | firma | Company name | string | legal_name | (not copied) | free, manual |
| tipo_ato | tipo de ato | Act type | string | filing | (not copied) | constituição/alterações/dissolução |
| data_publicacao | data de publicação | Publication date | date | date | (not copied) | timeline |
| texto_ato | texto do ato | Act full text | string | document | (not copied) | capital/management/statutes |

## Interpretation Notes

- **The free per-company source — but manual.** publicacoes.mj.pt publishes company **acts** (incorporations,
  statute/capital/management changes, dissolutions) **for free**, searchable by **NIPC** / firma. It is the only
  free per-company source (the register itself is paid). However the search is **reCAPTCHA-protected** (verified)
  → **manual lookups only; automated/bulk access is blocked and must not be bypassed**.
- The **act text** can contain capital, management (gerência) and statute details — free per-company detail, but
  requiring manual extraction. May contain **personal data (GDPR)**. Join on **NIPC**. Reuse terms of the
  published acts are not clearly stated.
