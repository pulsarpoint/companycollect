# Italy — Schema Notes

Italy has no open per-company master; fields below come from the documented Registro Imprese / bilanci
XBRL structure and the downloaded aggregate open data. Keep sources separate; join on **Codice Fiscale**.

## Identifiers
- **Codice Fiscale (CF)** — the company's fiscal code; the **primary key** in the Registro Imprese. For
  companies it is usually an 11-digit numeric (often equal to the Partita IVA) but can be alphanumeric.
- **Partita IVA (P.IVA)** — 11-digit VAT number. Often equals the CF but **not always** — keep both.
  VAT id form = `IT` + Partita IVA.
- **Numero REA** — Repertorio Economico Amministrativo number; **province-scoped** (e.g. `MI-1234567`).
  Local registry handle, not globally unique without the province.
- **ATECO** — Italian activity classification (= NACE-based; **ATECO 2025** current, formerly 2007).
- **LEI** — for entities that hold one (GLEIF), cross-references CF/REA.

## Registro Imprese (authoritative, paid) — observed/documented fields
```
denominazione            - legal name
codice_fiscale           - CF (primary key)
partita_iva              - VAT (often = CF)
numero_rea + provincia   - REA registry number (province-scoped)
forma_giuridica          - legal form (SRL, SPA, SRLS, SNC, SAS, SS, ...)
ateco                    - activity code (ATECO 2025/2007)
capitale_sociale         - share capital (EUR)
stato_attivita           - status (attiva / in liquidazione / fallita / cessata / sospesa / inattiva)
data_iscrizione          - registration date
amministratori           - administrators / directors (persons or companies) [PII]
pec                      - certified email
sede_legale              - registered office address (via, comune, provincia, cap, regione)
```

## Bilanci XBRL (financials, paid) — observed/documented concepts
Italian civil-code XBRL taxonomy (2018-11-04). Expect (per `esercizio`/fiscal year):
```
stato patrimoniale (balance sheet):
  totale attivo                    - total assets
  attivo: immobilizzazioni, attivo circolante
  patrimonio netto                 - equity
  debiti / totale passivo          - liabilities
conto economico (income statement):
  valore della produzione / ricavi - revenue / value of production
  costi della produzione
  risultato d'esercizio (utile/perdita) - net income (profit/loss)
tipologia bilancio                 - ordinario / abbreviato / micro (governs disclosure depth)
nota integrativa                   - notes; numero medio dipendenti (avg employees) often here
```
- Size types: **bilancio ordinario / abbreviato / micro** (art. 2435-bis/ter c.c.) — micro/abbreviato
  disclose fewer lines → expect nulls. Currency EUR.

## Open / aggregate data (downloaded) — NOT per-company
- `imprese-attive-ateco.csv` (CC-BY): columns `Settore Ateco 2025; Divisione Ateco 2025; <month1>; <month2>`
  = **counts** of active companies by ATECO division and month. Aggregate only.

## Startup/PMI innovative (open subset) — documented fields
```
denominazione, codice_fiscale, regione/provincia/comune, ateco, data_iscrizione,
classe_addetti (employee band), classe_valore_produzione (revenue band)
```
- Per-company but a small subset; financials only as **bands** (not exact figures).

## Mapping to internal company model
```
company_id          <- codice_fiscale
registration_number <- numero_rea (+ provincia)  [and/or codice_fiscale]
tax_id              <- codice_fiscale
vat_id              <- "IT" + partita_iva
legal_name          <- denominazione
normalized_name     <- lower(trim(denominazione)) minus forma giuridica suffix
company_type        <- forma_giuridica (SRL/SPA/...)
status              <- stato_attivita (attiva/in liquidazione/fallita/cessata/...)
incorporation_date  <- data_iscrizione
dissolution_date    <- (from cessazione/cancellazione event)
registered_address  <- sede_legale (via/comune/cap)
municipality        <- comune
region              <- regione (province from REA / sede)
activity_code       <- ateco
financials[]        <- bilanci XBRL (paid) | classe bands (startup open) | aggregator
country             <- "Italy"
source_url/name/at, raw_record
```
See `normalized/companies.sample.jsonl` (schematic — built from documented fields, not a downloaded
per-company record, since no open per-company file was retrievable).
