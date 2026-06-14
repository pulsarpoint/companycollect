# Italy — Common Field Mapping Suggestions

> **Suggestion only.** Proposes how Italy's country-specific profile *could* map onto a future
> cross-country company schema. It does **not** constrain `country_company_profile.schema.json`. The
> country-specific model is authoritative.

| Common field | Italy source path | Notes |
|---|---|---|
| company_id | registration.codice_fiscale | Clean single key; open via startup/GLEIF/ANAC, authoritative via paid register. |
| registration_number | registration.numero_rea (+ provincia) | REA is province-scoped; or use codice_fiscale. |
| tax_id | registration.codice_fiscale | Fiscal code. |
| vat_id | registration.vat_id ("IT" + partita_iva) | Partita IVA often = CF, not always. |
| legal_name | legal_identity.denominazione | Open via startup/GLEIF. |
| status | status.stato_attivita / derived | **Paid** (Registro Imprese). |
| legal_form | legal_identity.forma_giuridica | Paid (SRL/SPA/...); ELF code open via GLEIF. |
| incorporation_date | (Registro Imprese data_iscrizione) | Paid. |
| dissolution_date | (from cessazione/fallimento event) | Paid. |
| registered_address | registered_location.* | comune open (startup subset); full address paid. |
| activity_code | activity.ateco | ATECO; open only for the startup subset, else paid. |
| financials | financial_statements[] | **Paid** exact (bilanci XBRL) or **open bands** (startup); no open exact figures. |
| officers | officers[] = paid (Registro Imprese) | PII; GDPR. |
| owners | ownership (GLEIF L2 partial) | Beneficial ownership not openly modeled. |
| source_provenance | source_provenance[] | Per-source provenance + access flag. |

## Cross-country notes for a future mapper

- **Italy is clean-key but mostly paid**: one `codice_fiscale` joins everything (reconcile with Partita
  IVA + REA; bridge LEI↔CF via GLEIF), but unlike France the authoritative spine and financials are
  **paid** — a cross-country mapper gets only a **subset** for free (innovative startups/PMI + GLEIF + ANAC).
- **Activity code (ATECO) exists** but openly only for the startup subset — mark `activity_code` partially
  available, fully available only via paid/aggregator.
- **Financials**: no open exact figures; open data gives **bands** (ranges). A `financials` mapper must
  tolerate band-only values and a `source`/`access` discriminator; exact figures imply paid/aggregator.
- **Ownership**: only partial open via GLEIF Level-2 LEI links; beneficial ownership not open.
- **Currency** effectively always EUR; store per figure.
