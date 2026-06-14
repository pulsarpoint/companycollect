# IES — Informação Empresarial Simplificada (annual accounts) Field Catalog

> **Planning-only / not openly published.** The IES financial statements are filed to AT/INE/Banco de Portugal
> and are not openly published per company. Fields described from public documentation; no records/values copied.
> No `sample_record.json`.

## Source Summary

- Country: Portugal
- Source type: official_financial_disclosure
- Organization: Autoridade Tributária (AT) / INE / Banco de Portugal / IRN
- URL: https://www.ies.gov.pt/
- License: not openly published (filed to AT/INE/Banco de Portugal)
- Access: restricted
- Freshness: annual filing
- Record shape: annual filing (SNC accounts: balance sheet + income statement + annexes)
- Primary keys: `nipc`, `fiscal_year`
- Join keys: `nipc`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| balanco.ativo | balanço — ativo | Total assets | decimal | financial | (not open) | EUR |
| balanco.capital_proprio | balanço — capital próprio | Equity | decimal | financial | (not open) | |
| balanco.passivo | balanço — passivo | Liabilities | decimal | financial | (not open) | |
| dr.volume_negocios | DR — volume de negócios | Turnover | decimal | financial | (not open) | |
| dr.resultado_liquido | DR — resultado líquido | Net result | decimal | financial | (not open) | |
| n_trabalhadores | n.º de trabalhadores | Employees | integer | employment | (not open) | |

## Interpretation Notes

- **A single combined annual filing — but not openly published.** The **IES (Informação Empresarial
  Simplificada)** lets a company meet four obligations in one act: **accounting** (SNC) delivery, **tax** (AT),
  **statistics** (INE), and **Banco de Portugal** reporting. It includes the **balanço** (balance sheet),
  **demonstração de resultados** (income statement), anexo, CAE and employee counts, and is the deposit of
  accounts ("depósito de contas") for the commercial register.
- **Not open per company.** The figures are **not openly published**; Banco de Portugal publishes only
  **aggregate sector statistics** (Central de Balanços). Structured per-company financials need the **paid
  register** or a **commercial provider** (which parse the IES). Currency **EUR**. Join on **NIPC** + fiscal year.
