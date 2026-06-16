# Juntas Comerciais (NIRE) Field Catalog

> **PLANNING-ONLY.** The legal incorporation registries are the state Juntas
> Comerciais (NIRE), coordinated by DREI. Registration acts and certified extracts
> are **per-document / fee-based** per state; no single open bulk. Cataloged from
> public documentation only — no records fetched. The RFB CNPJ data already covers
> the core registry fields openly, so the Juntas are an officer/acts enrichment.

## Source Summary

- Country: Brazil
- Source type: official_registry
- Organization: Juntas Comerciais estaduais (e.g. JUCESP) / DREI
- URL: https://www.gov.br/empresas-e-negocios/pt-br/drei
- License: varies per state
- Access: public search / per-document (fee-based extracts)
- Freshness: live register
- Record shape: per-company registration acts
- Primary keys: `nire`
- Join keys: `nire`, `cnpj`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| junta.nire | NIRE | State registry id | string | identifier | complements CNPJ |
| junta.denominacao | Nome empresarial | Legal name | string | legal_name | |
| junta.tipo_juridico | Natureza/Tipo | Legal form | string | legal_form | LTDA / S.A. / EIRELI |
| junta.data_constituicao | Data de constituição | Incorporation date | date | date | authoritative |
| junta.administradores | Administradores | Officers/partners | array | person | **PERSONAL DATA (LGPD)** |
| junta.atos | Atos arquivados | Filed acts | array | document | certified copies fee-based |

## Interpretation Notes

- The **NIRE** is the state commercial-registry id, complementary to the CNPJ. Each
  state Junta runs its own portal (no single national open bulk).
- This is the source of the authoritative **incorporation acts** and the full
  **administrators/officers**, beyond what the RFB Sócios file carries.
- **Personal data**: administrators/partners are personal data under **LGPD** —
  redact.
- **Access**: per-document; certified extracts are fee-based. For the open layer,
  use the RFB CNPJ data (which already provides razão social, status, CNAE,
  capital, address, and partners). No raw sample record.
