# Brazil — Search Attempts

## Attempt 1

- Date/time: 2026-06-16
- Source: RFB CNPJ open data + CVM + dados.gov.br
- URL: arquivos.receitafederal.gov.br/dados/cnpj/... ; dados.cvm.gov.br ; dados.gov.br
- Language: Portuguese
- Why: RFB CNPJ is the national company registry; CVM holds listed financials.
- Result: RFB CNPJ path 404; CVM 200; dados.gov.br 200.
- Decision: Pursue CVM (works) and locate the current CNPJ file host.

## Attempt 2

- Date/time: 2026-06-16
- Source: RFB CNPJ file host discovery
- URL: several arquivos.receitafederal.gov.br paths; IP mirror 200.152.38.155; dados.gov.br dataset page
- Language: Portuguese
- Why: Find a working CNPJ bulk URL.
- Result: arquivos host is now a **SERPRO+ JS app** ("requer JavaScript"); month/file URLs 404; IP mirror timed out (000); dados.gov.br CKAN API 401 (needs token).
- Decision: CNPJ data is open but the ZIPs are not directly fetchable headlessly here; catalog from the authoritative RFB layout spec.

## Attempt 3

- Date/time: 2026-06-16
- Source: CVM Dados Abertos — DFP directory
- URL: https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/
- Language: Portuguese
- Why: Open financial statements for listed companies.
- Result: directory lists dfp_cia_aberta_{2022..2026}.zip — reachable, no key.
- Decision: RECOMMENDED (open financials).

## Attempt 4

- Date/time: 2026-06-16
- Source: CVM DFP 2025 download + inspect
- URL: https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_2025.zip
- Language: Portuguese
- Why: Capture the real financial schema and records.
- Result: 12.5 MB, 19 CSVs (index + BPA/BPP/DRE/DFC/DMPL/DRA/DVA + capital + parecer). Real data: BCO BRASIL S.A. (CNPJ 00.000.000/0001-91), total assets ≈ R$2.46T (thousands). Keyed on CNPJ_CIA + CD_CVM.
- Decision: Used as the real normalized sample; joins to CNPJ registry by CNPJ.

## Attempt 5

- Date/time: 2026-06-16
- Source: Juntas Comerciais (DREI) — state registries
- URL: https://www.gov.br/empresas-e-negocios/pt-br/drei
- Language: Portuguese
- Why: The legal incorporation registries (NIRE).
- Result: per-state portals; acts/extracts fee-based; no single open bulk (RFB CNPJ already covers core registry fields openly).
- Decision: blocked_by_payment (per-document enrichment).
