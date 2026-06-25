# Brazil Company Data Investigation

## Conclusion

Brazil is a **top-tier open-data country**: the **complete company registry** and
**listed-company financial statements** are both openly available.

- **Identity (open bulk):** **Receita Federal — Dados Públicos CNPJ**. Every legal
  entity has a **CNPJ (14-digit)**. Published as monthly bulk ZIP CSVs: **Empresas**
  (legal entity), **Estabelecimentos** (establishments), **Sócios** (owners),
  **Simples**, plus reference tables (Cnaes, Naturezas, Municípios, Países,
  Qualificações, Motivos). ~50M+ entities, fully open.
- **Financials (open, verified):** **CVM Dados Abertos** publishes the standardized
  financial statements of **listed companies** (DFP annual + ITR quarterly): balance
  sheet (BPA/BPP), income (DRE), cash flow (DFC), equity (DMPL), DRA/DVA, capital
  composition, audit opinion — keyed on **`CNPJ_CIA`** + **`CD_CVM`**, in **BRL**.
  **Joins to the CNPJ registry by CNPJ.**

## What was verified live

- **CVM DFP works**: downloaded `dfp_cia_aberta_2025.zip` (12.5 MB), 19 CSVs. Real
  data: index row **BCO BRASIL S.A.**, CNPJ `00.000.000/0001-91`, CD_CVM 001023;
  balance-sheet "Ativo Total" (total assets) ≈ 2.46 billion (thousands of BRL).
  Built a real normalized sample (Banco do Brasil, Axia Energia, CEB, Caramuru).
- **CVM directory** lists `dfp_cia_aberta_{YYYY}.zip` (2022–2026) — reachable and
  downloadable, no key.
- **RFB CNPJ**: the dados-públicos-CNPJ landing page (gov.br) is live, and the
  dataset is on dados.gov.br. The historical
  `arquivos.receitafederal.gov.br/dados/cnpj/...` path returned 404/timeout
  headlessly, but the Casa dos Dados mirror is reachable and exposes dated
  snapshot folders such as `2026-05-10/` with direct ZIPs including
  `Cnaes.zip`, `Empresas0.zip`, and `Estabelecimentos0.zip`. The CNPJ layout
  below is the **authoritative published RFB spec**; the data itself is open
  (no auth/payment).
- **dados.gov.br** CKAN API now needs a **free token** (401 without it).

## Identifiers

- **CNPJ (Cadastro Nacional da Pessoa Jurídica)** — **14 digits**:
  `00.000.000/0001-91` = 8-digit **root** (the legal entity) + 4-digit
  **establishment order** (`0001` = headquarters) + 2-digit **check digit**. The
  CNPJ is the company id **and the federal tax id**. Universal join key.
- **CPF** — natural-person tax id (owners/partners); personal data (in the Sócios
  file it is partially masked).
- **NIRE** — the state commercial-registry (Junta Comercial) number.
- **No single VAT**: federal taxes use the CNPJ; **ICMS** (state VAT-like) uses a
  separate **Inscrição Estadual** (state-level, not in the federal open data).

## RFB CNPJ layout (authoritative published spec)

- **Empresas** (`*.EMPRECSV`): `cnpj_basico` (8-digit root), `razao_social`,
  `natureza_juridica` (→ Naturezas), `qualificacao_responsavel`, `capital_social`,
  `porte` (company size), `ente_federativo_responsavel`.
- **Estabelecimentos** (`*.ESTABELE`): `cnpj_basico` + `cnpj_ordem` + `cnpj_dv`,
  `identificador_matriz_filial` (1=HQ/2=branch), `nome_fantasia` (trade name),
  `situacao_cadastral` (2=active, 8=closed, …) + `data_situacao` + `motivo`,
  `data_inicio_atividade`, `cnae_fiscal_principal` + `cnae_fiscal_secundaria`
  (→ Cnaes), full address (`tipo_logradouro`, `logradouro`, `numero`, `bairro`,
  `cep`, `municipio` → Municípios, `uf`), `pais` (→ Países), phones, email.
- **Sócios** (`*.SOCIOCSV`): `cnpj_basico`, `identificador_socio`, `nome_socio`,
  `cpf_cnpj_socio` (CPF masked), `qualificacao_socio` (→ Qualificações),
  `data_entrada_sociedade`, `representante_legal` — **personal data (LGPD)**.
- **Simples** (`*.SIMPLES.CSV`): `cnpj_basico`, `opcao_simples`, `opcao_mei`, dates.
- **Reference**: Cnaes, Naturezas, Municípios, Países, Qualificações, Motivos
  (code → description). Encoding **Latin-1**, `;`-delimited, no header row.

## CVM DFP/ITR layout (verified)

- **Index** (`dfp_cia_aberta_{YYYY}.csv`): `CNPJ_CIA`, `DT_REFER`, `VERSAO`,
  `DENOM_CIA`, `CD_CVM`, `CATEG_DOC`, `ID_DOC`, `DT_RECEB`, `LINK_DOC`.
- **Statement files** (BPA/BPP/DRE/DFC/DMPL/DRA/DVA, each `_con` consolidated /
  `_ind` individual): `CNPJ_CIA`, `DT_REFER`, `DENOM_CIA`, `CD_CVM`, `GRUPO_DFP`,
  `MOEDA` (REAL), `ESCALA_MOEDA` (MIL = thousands), `ORDEM_EXERC` (ÚLTIMO/PENÚLTIMO),
  `DT_FIM_EXERC`, `CD_CONTA` (account code), `DS_CONTA` (account name),
  `VL_CONTA` (value), `ST_CONTA_FIXA`. Encoding **Latin-1**, `;`-delimited.

## What is NOT openly available

- **Private-company financial statements** — only listed issuers (CVM).
- **Inscrição Estadual / ICMS** data — state-level, not in the federal open data.
- **Junta Comercial acts / certified extracts** — per-document, fee-based.

## Recommended ingestion

1. **RFB CNPJ** ZIP CSVs (Empresas + Estabelecimentos + Sócios + Simples +
   reference tables), keyed on CNPJ; resolve code lists.
2. **CVM DFP + ITR** per-year ZIPs, joined on CNPJ for listed companies.
3. Treat Junta Comercial as a per-document enrichment.
4. Redact Sócios personal data (names, masked CPF) per **LGPD**.
