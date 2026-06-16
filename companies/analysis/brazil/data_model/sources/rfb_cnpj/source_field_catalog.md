# Receita Federal — Dados Públicos CNPJ Field Catalog

> **Schema DOCUMENTED from the authoritative RFB published layout.** The data is
> fully open (no auth/payment), but the bulk ZIPs are now served via Receita's
> **SERPRO+ portal** (a JS app), so they were not fetchable headlessly in this run.
> No raw record is included for this source; the example record uses CVM data
> (which carries the CNPJ).

## Source Summary

- Country: Brazil
- Source type: official_registry
- Organization: Receita Federal do Brasil (RFB) / SERPRO
- URL: https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{YYYY-MM}/
- License: open (Lei de Acesso à Informação / CC-BY style)
- Access: public, no key (bulk ZIPs via the SERPRO+ portal)
- Freshness: monthly
- Record shape: multiple CSVs joined on `cnpj_basico` (Empresas / Estabelecimentos / Socios / Simples + reference tables)
- Primary keys: `cnpj_basico` (8-digit root)
- Join keys: `cnpj_basico`, full 14-digit `cnpj`

## Fields (modeled subset)

| File.field | Meaning | Semantic type | Notes |
|---|---|---|---|
| Empresas.cnpj_basico | 8-digit root (entity) | identifier | entity join key |
| Empresas.razao_social | Legal name | legal_name | |
| Empresas.natureza_juridica | Legal-nature code | legal_form | → Naturezas |
| Empresas.capital_social | Share capital (BRL) | financial | |
| Empresas.porte | Company size | metadata | 01 micro / 03 small / 05 other |
| Estabelecimentos.cnpj (basico+ordem+dv) | 14-digit CNPJ | identifier | 0001 = HQ; = tax id |
| Estabelecimentos.identificador_matriz_filial | 1 HQ / 2 branch | metadata | |
| Estabelecimentos.nome_fantasia | Trade name | legal_name | |
| Estabelecimentos.situacao_cadastral | Status | status | 02 ativa / 08 baixada |
| Estabelecimentos.data_inicio_atividade | Activity start | date | ≈ incorporation |
| Estabelecimentos.cnae_fiscal_principal | Activity (CNAE) | activity | → Cnaes |
| Estabelecimentos.address | Address | address | → Municipios |
| Estabelecimentos.correio_eletronico/telefone | Contact | raw_extension | **PII (LGPD) — redact** |
| Socios.nome_socio / cpf_cnpj_socio | Partner/owner | person/ownership | **PERSONAL DATA (LGPD) — redact** |
| Socios.qualificacao_socio | Partner role | ownership | → Qualificacoes |
| Simples.opcao_simples / opcao_mei | Tax-regime flags | license_or_terms | |
| Cnaes/Naturezas/Municipios/Paises/Qualificacoes/Motivos | Code lists | metadata | resolve coded fields |

(Full per-file layout is in `source_field_catalog.json` / `schema_notes.md`.)

## Interpretation Notes

- **CNPJ structure**: 8-digit root (`cnpj_basico` = the legal entity) + 4-digit
  establishment order (`0001` = matriz/HQ) + 2-digit check. Join the files on
  `cnpj_basico`; the full 14-digit CNPJ identifies an establishment **and is the
  federal tax id**.
- **One entity, many establishments**: Empresas is entity-level; Estabelecimentos
  is establishment-level (HQ + branches).
- **Status**: `situacao_cadastral` 02 = ativa (active), 08 = baixada (closed); map
  via the Motivos table for the reason.
- **Code lists**: resolve `natureza_juridica`, `cnae_fiscal`, `municipio`,
  `qualificacao_socio` against the bundled reference tables.
- **Encoding Latin-1**, `;`-delimited, **no header row** (map by position per the
  RFB layout).
- **Personal data**: the **Sócios** file (names, masked CPF) and establishment
  contact fields are **personal data (LGPD)** — redact in committed output.
- **CVM financials join here by CNPJ** (CVM `CNPJ_CIA` = this 14-digit CNPJ).
