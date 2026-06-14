# Portugal — Search Attempts Log

## Attempt 1
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Portugal Registo Comercial empresas open data dados.gov.pt NIPC company register publicacoes.mj.pt IES financial statements certidão permanente API`
- Language: English + Portuguese
- Why this query was tried: Identify the register, any open bulk/API, and financials.
- Top relevant URLs:
  - https://dados.gov.pt/en/datasets/rco/
  - https://dados.gov.pt/en/datasets/atos-do-registo-comercial/
  - https://eportugal.gov.pt/servicos/aceder-a-certidao-permanente-de-registo-comercial
- Result: Register data via paid certidão permanente; IES = combined annual accounting/tax/statistics filing; dados.gov.pt has RCO + Atos datasets.
- Decision: Inspect the dados.gov.pt / dados.justica datasets; probe publicacoes.mj.pt + register.

## Attempt 2
- Date/time: 2026-06-14
- Source: curl (live) — dados.gov.pt / dados.justica APIs
- Query: datasets/rco, atos-do-registo-comercial; CKAN package_search?q=empresas; package_show empresas/fcpc/insolvencia
- Result: RCO = CC-BY-SA, on dados.justica.gov.pt (rco.csv/xlsx/xml). Atos = CC-BY statistical "número de actos". empresas/fcpc/insolvencia = statistical aggregates (counts). None per-company.
- Decision: Download RCO to confirm; classify open data as statistical only.

## Attempt 3
- Date/time: 2026-06-14
- Source: curl (live) — RCO download
- Query: GET rco.csv
- Result: 120-row monthly time series (online registrations by transcription/deposit, accumulated) - STATISTICAL, not a per-company register.
- Decision: dados_justica_stats = useful_secondary (statistics). No open per-company register/financials.

## Attempt 4
- Date/time: 2026-06-14
- Source: curl (live) — publicacoes.mj.pt + portals
- Query: publicacoes.mj.pt/pesquisa.aspx ; eportugal certidão ; bportugal.pt
- Result: publicacoes.mj.pt search page references reCAPTCHA + NIPC field -> free company-acts search but captcha-protected (automation blocked). Certidão permanente = paid (HTTP 200). bportugal.pt HTTP 403 (WAF).
- Decision: publicacoes_mj = recommended (manual; free company acts). registo_comercial = blocked_by_payment. ies_financials = blocked (not open). rcbe = blocked_by_authentication. vies_vat / commercial aggregators = secondary. Built a schematic normalized sample (no per-company open record lawfully downloadable).
