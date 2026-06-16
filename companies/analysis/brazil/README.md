# Company data sources for Brazil

## Status

- Official bulk data: **found** (RFB CNPJ open data — full national registry; CVM open data — listed financials)
- Official API: partial (bulk-first; per-CNPJ lookup APIs exist via third parties)
- Open data portal: **found** (dados.gov.br; dados.cvm.gov.br)
- License: **known** — Brazilian open-data / Lei de Acesso à Informação; CC-BY style
- Recommended ingestion path: **bulk** (CNPJ ZIP CSVs + CVM ZIP CSVs)

## Best source

**Receita Federal (RFB) — Dados Públicos CNPJ** — the **complete national company
registry**. Every legal entity has a **CNPJ (Cadastro Nacional da Pessoa Jurídica,
14-digit)**. Published openly as monthly bulk ZIP CSVs split into:

- **Empresas** — the legal entity (root CNPJ, razão social, legal nature, share
  capital, company size, responsible person).
- **Estabelecimentos** — each establishment (CNPJ root+order+DV, trade name, status,
  CNAE activity, address, phones, dates).
- **Sócios** — partners / owners (name, qualification, CPF mask / CNPJ).
- **Simples** — Simples Nacional / MEI tax-regime flags.
- Reference tables — **Cnaes**, **Naturezas** (legal natures), **Municipios**,
  **Paises**, **Qualificações**, **Motivos** (status reasons).

This covers **~50M+ entities** and is fully open. (Access note: the files are now
served through Receita's SERPRO+ portal — a JS app — so the bulk ZIPs could not be
fetched headlessly in this run; the layout below is the authoritative published
spec, and the dataset is listed on dados.gov.br.)

## Financial data — verified open

**CVM (Comissão de Valores Mobiliários) — Dados Abertos** publishes the **financial
statements of listed companies** (companhias abertas) openly:

- **DFP** (Demonstrações Financeiras Padronizadas, annual) and **ITR** (quarterly).
- Per year ZIP with **BPA/BPP** (balance sheet assets/liabilities), **DRE** (income
  statement), **DFC** (cash flow), **DMPL** (equity), **DRA/DVA**, capital
  composition, and audit opinion — all keyed on **`CNPJ_CIA`** (the company's CNPJ)
  + **`CD_CVM`**, in **BRL** (scale: thousands).

Verified live: downloaded `dfp_cia_aberta_2025.zip` (12.5 MB) — real data, e.g.
**Banco do Brasil S.A.** (CNPJ 00.000.000/0001-91), total assets ≈ R$2.46 trillion
(thousands). **CVM financials join to the CNPJ registry by CNPJ.**

> Private (non-listed) company financials are **not public** — only listed issuers
> via CVM.

## Identifiers & tax

- **CNPJ** (14-digit) — company id and **tax id**. Structure: 8-digit root + 4-digit
  establishment order + 2-digit check (`00.000.000/0001-91`). The 8-digit root is
  the legal entity; the full 14-digit identifies an establishment.
- **NIRE** — the state commercial-registry (Junta Comercial) number.
- Brazil has no single "VAT": federal taxes use the CNPJ; **ICMS** (state VAT-like)
  uses a separate **Inscrição Estadual**. The universal company key is the **CNPJ**.

## Next action

Ingest the RFB CNPJ ZIP CSVs (Empresas + Estabelecimentos + Sócios + reference
tables), keyed on CNPJ; join **CVM DFP/ITR** financials on CNPJ for listed
companies. Sample uses real CVM data.
