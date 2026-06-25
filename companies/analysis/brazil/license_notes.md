# Brazil — License Notes

## Receita Federal — Dados Públicos CNPJ — open

- The CNPJ public data is published under Brazil's open-data framework (**Lei de
  Acesso à Informação 12.527/2011** + the Política de Dados Abertos), as **open
  data free to reuse**, including commercial, with attribution to the Receita
  Federal. No authentication or payment.
- Access note: the historical Receita `arquivos.receitafederal.gov.br/dados/cnpj`
  path returned 404/timeout in headless checks. The same open RFB CNPJ ZIPs are
  available through the Casa dos Dados mirror at
  `https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/`, with dated
  monthly directories such as `2026-05-10/`.
- Treatment here: **open / reusable with attribution**. Company identity (CNPJ,
  razão social, address, CNAE, capital) is corporate data. **Sócios** (partners)
  names and (masked) CPF are **personal data (LGPD)** — redact in shared samples.

## CVM Dados Abertos — open

- CVM open data is published under the portal's open terms (CC-BY style, free reuse
  with attribution to the CVM). No key required. The financial statements are
  statutory public disclosure for listed companies.
- Treatment here: **open / reusable with attribution**. Verified and used for the
  real sample.

## Juntas Comerciais (DREI) — per-document / fee-based

- State commercial registries provide registration acts and certified extracts on a
  **per-document / fee** basis; terms vary by state. No single open bulk.
- Treatment here: **blocked_by_payment**. Cataloged from public documentation only.

## dados.gov.br — open (token for API)

- The national catalogue uses open terms; the **CKAN API now requires a free
  token** (401 without it). The CNPJ/CVM data are fetched from their own sites.

## Personal data

- **CPF** and **partner/owner names** (Sócios file) are **personal data** under
  Brazil's **LGPD (Lei Geral de Proteção de Dados)**. The RFB partially masks CPF,
  but names + masked CPF should still be **redacted** in committed/shared samples.
  Company CNPJs and corporate names are corporate identifiers.

## Tax identifiers

- The **CNPJ** is the company id and the **federal tax id**. Brazil has no single
  VAT; **ICMS** (state VAT-like) uses a separate **Inscrição Estadual** not present
  in the federal open data. Map `tax_id` to the CNPJ.
