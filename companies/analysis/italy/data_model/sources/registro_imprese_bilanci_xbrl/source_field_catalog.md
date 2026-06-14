# Registro Imprese — Bilanci XBRL — Field Catalog

> **PLANNING-ONLY (paid).** Annual accounts deposited in **XBRL** (Italian civil-code taxonomy
> "2018-11-04", mandatory since 2019). Retrievable **per company for a fee** via Telemaco /
> registroimprese.it / the Bilancio XBRL API as PDF/HTML/XLS/CSV (+ EN/FR/DE). **No open bulk.** Fields
> from the documented taxonomy; **no records or values copied**.

## Source Summary

- Country: Italy
- Source type: official_financial_disclosure
- Organization: InfoCamere / Camere di Commercio
- URL: https://www.registroimprese.it/deposito-bilanci
- License: paid/contractual — planning-only
- Access: **paid** per document
- Freshness: annual; XBRL taxonomy 2018-11-04
- Record shape: per-company per-esercizio XBRL instance + prospetto
- Primary keys: `codice_fiscale + esercizio`
- Join keys: `codice_fiscale`

## Fields

| Path | Source field (IT) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| codice_fiscale | codice_fiscale | Company id | string | identifier | join |
| esercizio | esercizio | Fiscal year | date | date | per-statement key |
| tipologia_bilancio | tipologia bilancio | ordinario/abbreviato/micro | string | filing | drives nullability |
| statoPatrimoniale.totaleAttivo | totale attivo | Total assets | decimal | financial | EUR |
| statoPatrimoniale.immobilizzazioni | immobilizzazioni | Fixed assets | decimal | financial | |
| statoPatrimoniale.attivoCircolante | attivo circolante | Current assets | decimal | financial | |
| statoPatrimoniale.patrimonioNetto | patrimonio netto | Equity | decimal | financial | |
| statoPatrimoniale.debiti | debiti | Liabilities | decimal | financial | |
| contoEconomico.valoreDellaProduzione | valore della produzione | Revenue | decimal | financial | headline revenue |
| contoEconomico.risultatoEsercizio | utile/perdita d'esercizio | Net income | decimal | financial | neg = loss |
| notaIntegrativa.numeroMedioDipendenti | numero medio dipendenti | Avg employees | integer | employment | in notes |
| valuta | valuta | Currency | string | financial | usually EUR |

## Interpretation Notes

- **The financial source for the population — but paid.** XBRL is a real strength (systematic,
  machine-readable), yet access is **per-document paid** via InfoCamere/Telemaco; there is **no open bulk**.
  For financials at scale, a **commercial aggregator** (AIDA/BvD, Cerved, Atoka) that resells these is the
  realistic route.
- **Accounts type drives disclosure.** *micro* / *abbreviato* (art. 2435-bis/ter c.c.) disclose fewer
  lines than *ordinario* → `revenue`/`net_income`/`employees` are **nullable** for the smallest filers.
- **Revenue concept**: "valore della produzione" (item A of the conto economico) is the headline; "ricavi
  delle vendite e prestazioni" (A.1) is the narrower sales figure — pick consistently.
- **Currency** usually EUR; Italian number locale (`1.234.567,89`) in HTML prospetto, clean numerics in XBRL.
- No `sample_record.json` — paid source; values not retrievable under planning-only terms.
