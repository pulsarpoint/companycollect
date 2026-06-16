# Brazil — Source Inventory

| Source | Org | Type | Access | Formats | License | Status |
|---|---|---|---|---|---|---|
| RFB Dados Públicos CNPJ | Receita Federal / SERPRO | official registry | public (bulk; via SERPRO+ portal) | CSV, ZIP | open (LAI/CC-BY) | **recommended** |
| CVM DFP/ITR | CVM | financial statements | public, no key | CSV, ZIP | open (CC-BY) | **recommended** |
| Juntas Comerciais (NIRE) | Juntas estaduais / DREI | official registry | per-document (paid) | HTML, PDF | varies | blocked_by_payment |
| dados.gov.br | Governo Federal | open data portal | public (API token) | CSV, JSON | open | useful_secondary_source |

## Roles

- **rfb_cnpj** — the authoritative open **company registry** keyed on the 14-digit
  CNPJ: Empresas (entity), Estabelecimentos (establishments), Sócios (owners),
  Simples, + reference tables. ~50M+ entities. (Files now via the SERPRO+ portal;
  layout cataloged from the published RFB spec.)
- **cvm_dfp_itr** — open **financial statements** for listed companies (BPA/BPP/
  DRE/DFC/DMPL/DRA/DVA), keyed on CNPJ_CIA + CD_CVM, BRL. Verified live; joins to
  the CNPJ registry by CNPJ.
- **junta_comercial_nire** — legal incorporation acts (NIRE); per-document,
  fee-based; enrichment for officers/acts.
- **dados_gov_br** — discovery catalogue (API needs a token).

## Join keys

**CNPJ (14-digit)** is the universal key (8-digit root = legal entity). CVM joins on
**CNPJ_CIA**; the Junta uses **NIRE**. CNPJ is also the federal tax id; there is no
single VAT (ICMS uses a separate, non-open Inscrição Estadual).
