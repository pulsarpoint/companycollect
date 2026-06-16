# CVM Dados Abertos — DFP/ITR Field Catalog

## Source Summary

- Country: Brazil
- Source type: financial_statements
- Organization: Comissão de Valores Mobiliários (CVM)
- URL: https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_{YYYY}.zip
- License: open (CC-BY style, free reuse with attribution)
- Access: public, **no key**
- Freshness: annual (DFP) + quarterly (ITR)
- Record shape: per-year ZIP (index + statement CSVs); **one row per account line**
- Primary keys: `CNPJ_CIA` + `CD_CVM` + `DT_REFER` + `GRUPO_DFP` + `CD_CONTA`
- Join keys: `CNPJ_CIA` (→ RFB CNPJ), `CD_CVM`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| index.CNPJ_CIA | CNPJ_CIA | Company CNPJ | string | identifier | 00.000.000/0001-91 | **join to RFB CNPJ** |
| index.DENOM_CIA | DENOM_CIA | Company name | string | legal_name | BCO BRASIL S.A. | |
| index.CD_CVM | CD_CVM | CVM issuer code | string | identifier | 001023 | |
| index.CATEG_DOC | CATEG_DOC | Doc category | string | document | DFP / ITR | |
| statement.GRUPO_DFP | GRUPO_DFP | Statement group | string | financial | DF Consolidado - Balanço Patrimonial Ativo | BPA/BPP/DRE/DFC/… |
| statement.MOEDA / ESCALA_MOEDA | MOEDA/ESCALA_MOEDA | Currency / scale | string | financial | REAL / MIL | BRL, thousands |
| statement.ORDEM_EXERC | ORDEM_EXERC | Exercise | string | metadata | ÚLTIMO / PENÚLTIMO | filter ÚLTIMO |
| statement.CD_CONTA / DS_CONTA | CD_CONTA/DS_CONTA | Account code/name | string | financial | 1 / Ativo Total | standardized chart |
| statement.VL_CONTA | VL_CONTA | Value (BRL) | decimal | financial | 2398719197 | × scale |

## Interpretation Notes

- **Verified from real data**: `dfp_cia_aberta_2025.zip` (12.5 MB, 19 CSVs). Real
  record: **BCO BRASIL S.A.**, CNPJ `00.000.000/0001-91`, CD_CVM 001023, "Ativo
  Total" ≈ 2.46 billion (thousands of BRL = ~R$2.46 trillion).
- **Granularity**: each row is **one account line** of one statement. To build a
  statement, **group by `CNPJ_CIA` + `DT_REFER` + `GRUPO_DFP` + `ORDEM_EXERC`** and
  pivot on `CD_CONTA`/`DS_CONTA`.
- **Statement files** (each `_con` consolidated / `_ind` individual): **BPA**
  balance-sheet assets, **BPP** liabilities/equity, **DRE** income statement,
  **DFC_MD/MI** cash flow (direct/indirect), **DMPL** changes in equity, **DRA**
  comprehensive income, **DVA** value added, plus `composicao_capital` and
  `parecer` (audit opinion).
- **Join**: `CNPJ_CIA` (strip formatting → 14-digit) joins to the **RFB CNPJ**
  registry. `CD_CVM` is the internal CVM issuer key.
- **Currency BRL**, scale usually **MIL** (multiply by 1000). `ORDEM_EXERC` ÚLTIMO =
  latest year, PENÚLTIMO = prior.
- **Coverage**: **listed companies only** (companhias abertas). Encoding Latin-1,
  `;`-delimited, header row.
