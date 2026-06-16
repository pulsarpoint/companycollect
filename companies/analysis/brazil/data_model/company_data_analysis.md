# Company Data Analysis For Brazil

## Summary

Brazil is a **top-tier open-data country**: it offers a **complete open company
registry** *and* **open financial statements for listed companies**, both keyed on
the **CNPJ**. The **Receita Federal Dados Públicos CNPJ** bulk data gives identity,
status, activity (CNAE), capital, address, partners, and tax regime for **every**
legal entity (~50M+). The **CVM Dados Abertos** (DFP/ITR) gives standardized
financial statements for **listed** companies, joined on the CNPJ. The example
uses real CVM data; financials were verified live.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| rfb_cnpj | Receita Federal — Dados Públicos CNPJ | recommended | public (bulk via SERPRO+ portal) | open (LAI/CC-BY) | Authoritative open registry (all entities) |
| cvm_dfp_itr | CVM Dados Abertos — DFP/ITR | recommended | public, no key | open (CC-BY) | Open financials (listed companies) |
| junta_comercial_nire | Juntas Comerciais (NIRE) | blocked_payment | per-document (paid) | varies | Incorporation acts / officers (enrichment) |

## What Each Source Contributes

- **rfb_cnpj** — the authoritative open registry keyed on the 14-digit CNPJ:
  Empresas (razão social, legal nature, capital, size), Estabelecimentos (trade
  name, status, CNAE, address, dates), Sócios (partners — personal data), Simples
  (tax regime), and reference code lists. Schema documented from the published RFB
  layout (bulk ZIPs now served via Receita's SERPRO+ JS portal — open, but not
  fetchable headlessly in this run).
- **cvm_dfp_itr** — open standardized financial statements (BPA/BPP balance sheet,
  DRE income, DFC cash flow, DMPL equity, DRA/DVA) for listed companies, keyed on
  CNPJ_CIA + CD_CVM, in BRL. **Verified live** (dfp_cia_aberta_2025.zip; real data
  incl. Banco do Brasil). Joins to the CNPJ registry by CNPJ.
- **junta_comercial_nire** — state commercial registries (NIRE): incorporation acts
  and full officer detail; per-document, fee-based; enrichment only.

## Proposed Country Company Profile

A single object keyed on `registration.cnpj` (+ cnpj_basico):

- `registration` — CNPJ, cnpj_basico, NIRE.
- `tax_identifiers` — tax_id = CNPJ; vat_id null (no single VAT).
- `legal_identity` — legal name, trade name, legal nature, company size.
- `status` — situacao_cadastral (active/closed) + date.
- `incorporation` — activity start date.
- `activity` — CNAE; `capital` — share capital (BRL).
- `registered_location` — address, municipality, UF.
- `tax_regime` — Simples / MEI.
- `owners[]` — partners (Sócios; personal data, LGPD).
- `financial_statements[]` — CVM (listed only, BRL).
- `source_provenance[]`.

## Join And Precedence Rules

- **Join keys**: CNPJ (14-digit) universal; cnpj_basico (8-digit root) for the
  entity; CVM `CNPJ_CIA` = the CNPJ; Junta uses NIRE.
- **Precedence**: RFB CNPJ (open registry) > CVM (open listed financials) > Junta
  (paid acts/officers).
- **CNPJ is also the federal tax id**; there is no separate VAT.

## Missing Or Restricted Data

- **Private-company financials** — not public (listed only via CVM).
- **VAT / Inscrição Estadual / ICMS** — state-level, not in the federal open data.
- **Incorporation acts / full officers** — fee-based Juntas.
- **Personal data** — Sócios names + masked CPF (LGPD), redacted.

## Common Mapper Notes

- Map `company_id`, `registration_number`, and `tax_id` all to the CNPJ; mark
  `vat_id` as not available.
- Map `financials` from CVM (listed, BRL) by aggregating per-account lines per
  period; join on CNPJ.
- Redact Sócios personal data (LGPD) in any committed output.
