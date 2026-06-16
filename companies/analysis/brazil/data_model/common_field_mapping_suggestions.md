# Brazil — Common Field Mapping Suggestions

> Suggestion layer only for a future cross-country mapper. It does **not**
> constrain the Brazil-specific profile. The registry (RFB CNPJ) and listed
> financials (CVM) are both open; the Juntas are paid enrichment.

| Common field | Brazil mapping | Status |
|---|---|---|
| company_id | registration.cnpj (14-digit) / cnpj_basico (8-digit root) | open (RFB) |
| registration_number | registration.cnpj (or NIRE from the Junta) | open (CNPJ); NIRE paid |
| tax_id | registration.cnpj | open — CNPJ is the federal tax id |
| vat_id | not_available_in_open_sources | no single VAT; ICMS = Inscrição Estadual (state, not open) |
| legal_name | legal_identity.legal_name (razao_social) | open (RFB) |
| status | status.status (situacao_cadastral) | open (RFB) |
| legal_form | legal_identity.legal_nature (natureza_juridica) + company_size (porte) | open (RFB) |
| incorporation_date | incorporation.activity_start_date (data_inicio_atividade) | open (RFB) |
| dissolution_date | status.status_date when status=closed (baixada) | open (RFB) |
| registered_address | registered_location.registered_address | open (RFB) |
| activity_code | activity.cnae_primary (CNAE) | open (RFB) |
| financials | financial_statements[] (CVM DFP/ITR) | open; LISTED only; BRL |
| officers | owners[] (RFB Sócios) | open but personal data (LGPD) — redact |
| owners | owners[] (RFB Sócios) | open but personal data (LGPD) — redact |
| source_provenance | source_provenance[] | available |

## Notes

- **Single strong anchor**: the **CNPJ** is `company_id`, `registration_number`,
  and `tax_id` all at once, and the join key for **CVM financials** (`CNPJ_CIA`).
  Do not expect a separate VAT id (Brazil has no single VAT; ICMS uses a state
  Inscrição Estadual not in the open data).
- **Financials are first-class and open** for **listed** companies (CVM DFP/ITR,
  BRL) — aggregate the per-account lines per period before mapping `financials`.
  Private companies have no public financials.
- **Owners/officers** are open in the RFB Sócios file but are **personal data
  (LGPD)** (names + masked CPF) — redact in any committed sample.
- **Entity vs establishment**: cnpj_basico (8-digit) = the entity; the full
  14-digit CNPJ = an establishment (0001 = HQ).
