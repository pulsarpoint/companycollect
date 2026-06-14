# Registro Imprese (InfoCamere) — Field Catalog

> **PLANNING-ONLY (paid).** The authoritative Italian business register. Access is **paid/contractual**
> (InfoCamere Web Service/API + Telemaco) — **no open bulk**. Fields are cataloged from public
> documentation and `schema_notes.md`; **no records or values are copied**.

## Source Summary

- Country: Italy
- Source type: official_registry (authoritative spine)
- Organization: InfoCamere / Unioncamere (Camere di Commercio)
- URL: https://www.registroimprese.it/ ; API https://accessoallebanchedati.registroimprese.it/abdo/api
- License: paid/contractual — planning-only
- Access: **paid** (API key/contract or Telemaco per-document)
- Freshness: authoritative / continuous
- Record shape: per-company visura (JSON/XML via paid API, or document via Telemaco)
- Primary keys: `codice_fiscale`
- Join keys: `codice_fiscale`, `partita_iva`, `numero_rea + provincia`, `lei`

## Fields

| Path | Source field (IT) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| denominazione | denominazione | Legal name | string | legal_name | suffix embedded |
| codice_fiscale | codice_fiscale | **Fiscal code (PK)** | string | identifier | 11-digit; may ≠ P.IVA |
| partita_iva | partita_iva | VAT number | string | identifier | often = CF; VAT id = IT+P.IVA |
| numero_rea | numero_rea | REA registry number | string | identifier | **province-scoped** |
| forma_giuridica | forma_giuridica | Legal form | string | legal_form | SRL/SPA/SRLS/… |
| ateco | codice_ateco | Activity code | string | activity | ATECO 2025/2007 |
| capitale_sociale | capitale_sociale | Share capital | decimal | financial | register capital (EUR) |
| stato_attivita | stato_attivita | Status | string | status | attiva/in liquidazione/fallita/cessata |
| data_iscrizione | data_iscrizione | Registration date | date | date | incorporation |
| amministratori[] | amministratori | Directors | array | person | **PII**; Visura Amministratori |
| pec | pec | Certified email | string | metadata | |
| sede_legale | sede_legale | Registered office | object | address | comune/provincia/regione |

## Interpretation Notes

- **The authoritative spine** — every Italian company source keys back to the **Codice Fiscale**.
  Identifiers to reconcile: **CF** (primary), **Partita IVA** (often equal but not always), **numero REA**
  (province-scoped, e.g. `MI-1234567`), and **LEI** (for holders).
- **Access is paid** — modeled planning-only. The realistic open seed (CF/PIVA + name) comes from the
  startup/PMI innovative lists, ANAC suppliers, and GLEIF; this register then enriches authoritatively.
- **ATECO** is the Italian NACE implementation (transitioning 2007 → 2025).
- **PII**: amministratori are natural persons — GDPR.
- No `sample_record.json` (paid source; values not retrievable under planning-only terms).
