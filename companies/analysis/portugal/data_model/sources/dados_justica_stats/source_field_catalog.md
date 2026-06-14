# dados.justica.gov.pt / dados.gov.pt — open statistical datasets Field Catalog

## Source Summary

- Country: Portugal
- Source type: open_data_portal
- Organization: Direção-Geral da Política de Justiça (DGPJ) / IRN
- URL: https://dados.justica.gov.pt/ (CKAN: /api/3/action/)
- License: CC-BY-SA / CC-BY (per dataset)
- Access: public (free)
- Freshness: monthly/quarterly
- Record shape: statistical time series (CSV); **not a per-company register**
- Primary keys: `Year-Month`
- Join keys: none

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| Year-Month | Year-Month | Period | string | date | 2016-01 | time index |
| Registos por transcrição requeridos online | … | Registrations by transcription | integer | metadata | 5399 | count |
| Registos por depósito requeridos online | … | Registrations by deposit | integer | metadata | 2066 | count |
| (other datasets) | constituições/extinções, certificados, insolvências | Other aggregates | integer | metadata | — | counts |

## Interpretation Notes

- **Statistical only — not the company register.** Verified: the `rco` dataset is a **120-row monthly time
  series** of online commercial registrations (by transcription / deposit, with accumulated totals); other
  datasets (`empresas`, `fcpc`, `insolvencia`, `eol`, `enh`) are likewise **aggregate counts** (incorporations/
  extinctions of companies, admissibility certificates, insolvency processes). **CC-BY-SA / CC-BY.** None
  contains per-company identity or financials.
- Useful for **statistics/context** (registration volumes, insolvency trends) — not for building a company
  profile. A real statistical `sample_record.json` is included.
