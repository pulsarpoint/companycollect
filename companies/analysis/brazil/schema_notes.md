# Brazil — Schema Notes

## Identifiers

- **CNPJ (Cadastro Nacional da Pessoa Jurídica)** — **14 digits**, formatted
  `00.000.000/0001-91`:
  - **8-digit root (`cnpj_basico`)** — the legal entity.
  - **4-digit establishment order (`cnpj_ordem`)** — `0001` = matriz (HQ), `0002+`
    = filiais (branches).
  - **2-digit check (`cnpj_dv`)**.
  The CNPJ is the company id **and the federal tax id**. The 8-digit root is the
  entity-level join key; the full 14-digit identifies an establishment.
- **CPF** — natural-person tax id (owners); **personal data (LGPD)**, masked in the
  Sócios file.
- **NIRE** — state commercial-registry (Junta Comercial) number.
- **CD_CVM** — CVM issuer code (links a listed company in the CVM datasets).
- **No single VAT** — federal taxes use the CNPJ; **ICMS** (state VAT-like) uses a
  separate **Inscrição Estadual** (not in the federal open data).

## RFB CNPJ bulk CSVs (Latin-1-compatible, `;`-delimited, NO header)

The Dagster pipeline preserves the raw ZIP members and writes UTF-8 normalized
CSV artifacts before DuckDB ingestion. This avoids DuckDB failures on dirty
control bytes present in some RFB snapshot rows while keeping CSV parsing
set-based.

### Empresas (`*.EMPRECSV`)
`cnpj_basico` (8-digit root), `razao_social`, `natureza_juridica` (→ Naturezas
code list), `qualificacao_responsavel` (→ Qualificações), `capital_social`,
`porte` (00 n/a, 01 micro, 03 small, 05 other), `ente_federativo_responsavel`.

### Estabelecimentos (`*.ESTABELE`)
`cnpj_basico` + `cnpj_ordem` + `cnpj_dv`, `identificador_matriz_filial` (1 HQ / 2
branch), `nome_fantasia` (trade name), `situacao_cadastral` (01 null, 02 ativa, 03
suspensa, 04 inapta, 08 baixada) + `data_situacao_cadastral` + `motivo_situacao`
(→ Motivos), `nome_cidade_exterior`, `pais` (→ Países), `data_inicio_atividade`,
`cnae_fiscal_principal` + `cnae_fiscal_secundaria` (→ Cnaes), address
(`tipo_logradouro`, `logradouro`, `numero`, `complemento`, `bairro`, `cep`,
`uf`, `municipio` → Municípios), `ddd_1/telefone_1`, `correio_eletronico`,
`situacao_especial`.

### Sócios (`*.SOCIOCSV`) — PERSONAL DATA (LGPD)
`cnpj_basico`, `identificador_socio` (1 PJ / 2 PF / 3 estrangeiro), `nome_socio`,
`cpf_cnpj_socio` (CPF masked `***NNNNNN**`), `qualificacao_socio` (→ Qualificações),
`data_entrada_sociedade`, `pais`, `representante_legal`, `nome_representante`,
`qualificacao_representante`, `faixa_etaria`.

### Simples (`*.SIMPLES.CSV`)
`cnpj_basico`, `opcao_simples` (S/N), `data_opcao_simples`, `data_exclusao_simples`,
`opcao_mei`, `data_opcao_mei`, `data_exclusao_mei`.

### Reference tables (code → description)
`Cnaes`, `Naturezas`, `Municipios`, `Paises`, `Qualificacoes`, `Motivos`.

## CVM DFP/ITR financial statements (verified; Latin-1, `;`-delimited, header row)

### Index (`dfp_cia_aberta_{YYYY}.csv`)
`CNPJ_CIA` (company CNPJ), `DT_REFER` (reference date), `VERSAO`, `DENOM_CIA`
(company name), `CD_CVM`, `CATEG_DOC` (DFP/ITR), `ID_DOC`, `DT_RECEB`, `LINK_DOC`.

### Statement files (BPA/BPP/DRE/DFC/DMPL/DRA/DVA; `_con` consolidated / `_ind` individual)
`CNPJ_CIA`, `DT_REFER`, `VERSAO`, `DENOM_CIA`, `CD_CVM`, `GRUPO_DFP` (statement
group, e.g. "DF Consolidado - Balanço Patrimonial Ativo"), `MOEDA` (REAL),
`ESCALA_MOEDA` (MIL = thousands / UNIDADE), `ORDEM_EXERC` (ÚLTIMO = current,
PENÚLTIMO = prior), `DT_FIM_EXERC`, `CD_CONTA` (account code, e.g. `1` Ativo Total,
`3.01` Receita), `DS_CONTA` (account name), `VL_CONTA` (value), `ST_CONTA_FIXA`.

- Statement types: **BPA** balance-sheet assets, **BPP** liabilities/equity,
  **DRE** income statement, **DFC_MD/MI** cash flow (direct/indirect), **DMPL**
  changes in equity, **DRA** comprehensive income, **DVA** value added, plus
  `composicao_capital` and `parecer` (audit opinion). Currency **BRL**.

## Dates, money, encoding

- Dates: `YYYY-MM-DD` (CVM); `YYYYMMDD` (RFB CNPJ).
- Money: **BRL**; CVM scale typically **MIL** (thousands) — multiply by 1000.
- Encoding: **Latin-1-compatible source CSVs** for RFB, normalized to UTF-8 in
  Dagster before DuckDB ingestion; **Latin-1 (ISO-8859-1)** for CVM CSVs.
  Both are `;`-delimited.

## Internal model mapping

```text
company_id          <- CNPJ (14-digit) / cnpj_basico (8-digit root for the entity)
registration_number <- CNPJ (or NIRE from the Junta, per-document)
tax_id              <- CNPJ (federal tax id)
vat_id              <- null (no single VAT; ICMS via Inscrição Estadual, not open here)
legal_name          <- razao_social (Empresas) / DENOM_CIA (CVM)
trade_name          <- nome_fantasia (Estabelecimentos)
status              <- situacao_cadastral (Estabelecimentos: 02 ativa, 08 baixada, …)
legal_form          <- natureza_juridica (Naturezas) + porte
incorporation_date  <- data_inicio_atividade (Estabelecimentos)
registered_address  <- Estabelecimentos address fields (+ Municípios)
activity_code       <- cnae_fiscal_principal (Cnaes)
capital             <- capital_social (Empresas)
financials          <- CVM DFP/ITR (BPA/BPP/DRE/…), listed only, BRL
owners              <- Socios (personal data, LGPD)
```
