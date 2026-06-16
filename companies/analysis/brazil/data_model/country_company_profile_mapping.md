# Brazil Company Profile — Source Mapping

> **Top-tier open data.** Keyed on the **CNPJ (14-digit)** = company id = federal
> tax id (8-digit root = legal entity, 0001 = HQ). Identity + capital + partners
> come openly from the **RFB CNPJ** bulk data; **financial statements** for listed
> companies come openly from **CVM DFP/ITR**, joined on the CNPJ. No single VAT
> (ICMS via a separate Inscrição Estadual, not open). Partners are personal data
> (LGPD).

## Field mapping

| Profile path | Source | Source path | Join key | Freshness | License/Access | Precedence / Notes |
|---|---|---|---|---|---|---|
| registration.cnpj | rfb_cnpj | Estabelecimentos cnpj (basico+ordem+dv) | cnpj | monthly | open | Authoritative id + tax id. |
| registration.cnpj_basico | rfb_cnpj | Empresas.cnpj_basico | cnpj_basico | monthly | open | 8-digit entity root. |
| registration.nire | junta_comercial_nire | NIRE | nire/cnpj | live | paid | State registry id. |
| tax_identifiers.tax_id | rfb_cnpj | cnpj | — | monthly | open | = CNPJ. |
| tax_identifiers.vat_id | — | — | — | — | not available | No single VAT (ICMS = Inscrição Estadual, state, not open). |
| legal_identity.legal_name | rfb_cnpj | Empresas.razao_social | — | monthly | open | (CVM DENOM_CIA cross-check). |
| legal_identity.trade_name | rfb_cnpj | Estabelecimentos.nome_fantasia | — | monthly | open | |
| legal_identity.legal_nature | rfb_cnpj | Empresas.natureza_juridica → Naturezas | — | monthly | open | Resolve code. |
| legal_identity.company_size | rfb_cnpj | Empresas.porte | — | monthly | open | micro/small/other. |
| status.* | rfb_cnpj | Estabelecimentos.situacao_cadastral / data | — | monthly | open | 02 ativa→active, 08 baixada→closed. |
| incorporation.activity_start_date | rfb_cnpj | Estabelecimentos.data_inicio_atividade | — | monthly | open | ≈ incorporation. |
| activity.cnae_primary | rfb_cnpj | Estabelecimentos.cnae_fiscal_principal → Cnaes | — | monthly | open | |
| capital.share_capital_brl | rfb_cnpj | Empresas.capital_social | — | monthly | open | BRL. |
| registered_location.* | rfb_cnpj | Estabelecimentos address → Municipios | — | monthly | open | |
| tax_regime.simples/mei | rfb_cnpj | Simples.opcao_simples/opcao_mei | — | monthly | open | |
| owners[] | rfb_cnpj | Socios.* | cnpj_basico | monthly | open | PERSONAL DATA (LGPD) — redact. |
| financial_statements[] | cvm_dfp_itr | index + statement lines | CNPJ_CIA → cnpj | annual/quarterly | open | LISTED only; BRL; aggregate per period. |

## Source precedence

1. **rfb_cnpj** — authoritative open registry (identity, status, activity, capital,
   address, partners, tax regime) for **all** entities. Primary source.
2. **cvm_dfp_itr** — authoritative open **financials** for **listed** companies,
   joined on CNPJ.
3. **junta_comercial_nire** — incorporation acts / NIRE / officer detail; per-
   document, fee-based; enrichment only.

Conflict rules:
- **Legal name**: RFB `razao_social` is authoritative; CVM `DENOM_CIA` is a
  cross-check (may differ in abbreviation).
- **Financials**: CVM only (listed); private companies have none.

## Join keys

- **CNPJ (14-digit)** is the universal key; **cnpj_basico (8-digit root)** joins the
  RFB files at entity level. **CVM `CNPJ_CIA`** (strip formatting) = the CNPJ. The
  Junta uses **NIRE**. CNPJ is also the federal tax id.

## Missing / restricted data

- **Private-company financials** — not public (listed only via CVM).
- **VAT / Inscrição Estadual / ICMS** — state-level, not in the federal open data.
- **Incorporation acts / full officers** — fee-based Juntas (RFB Sócios covers
  partners openly).
- **Personal data** — Sócios names + masked CPF (LGPD) redacted.
