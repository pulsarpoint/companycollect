# Austria — Schema Notes

No open per-company master was downloadable; fields below come from documented Firmenbuch / Jahresabschluss
structure and the GISA open dataset description. Keep sources separate; join on Firmenbuchnummer / UID.

## Identifiers
- **Firmenbuchnummer (FN)** — the company register number: digits + a **check letter**, written `FN 123456a`.
  Primary key in the Firmenbuch. Companies only (GmbH, AG, OG, KG, Genossenschaft, …).
- **UID (Umsatzsteuer-Identifikationsnummer)** — VAT id, format **`ATU` + 8 digits** (e.g. `ATU12345678`).
  Tax/VAT key. Validate via FinanzOnline/VIES.
- **GISA-Zahl** — trade-register number in GISA (per trade authorization). Not the same as the Firmenbuchnummer.
- **Stammzahl / ERsB** — supplementary-register id (used in e-government).
- **Steuernummer** — internal tax number (not public).

## Firmenbuch (authoritative, paid) — documented fields
```
firmenbuchnummer          - FN###### + check letter
firmenwortlaut            - registered company name
rechtsform               - legal form (GmbH, AG, OG, KG, eU, Genossenschaft, ...)
sitz                     - registered seat (Gemeinde)
geschaeftsanschrift      - business address
uid                      - VAT id (ATU########)
stammkapital             - share capital (EUR)
geschaeftszweig          - line of business (free text; not a coded ÖNACE in the public extract)
organe                   - representatives (Geschäftsführer/Vorstand/Prokuristen) [PII]
status                   - aufrecht (active) / gelöscht (deleted)
eintragungsdatum         - registration date
```

## Jahresabschluss (financials, paid) — documented concepts
Austrian UGB accounting; filed to the Urkundensammlung. Expect (per Geschäftsjahr):
```
bilanz (balance sheet):
  bilanzsumme               - total assets
  anlagevermögen / umlaufvermögen
  eigenkapital              - equity
  verbindlichkeiten/rückstellungen - liabilities/provisions
gewinn- und verlustrechnung (income statement):
  umsatzerlöse              - revenue (medium/large; small may be exempt)
  jahresüberschuss/-fehlbetrag - net income (profit/loss)
anhang                      - notes; durchschnittliche Mitarbeiterzahl (avg employees)
größenklasse                - Kleinst / klein / mittel / groß (§221 UGB) -> disclosure depth
```
- Size classes (Kleinstkapitalgesellschaft / klein / mittel / groß) govern what is filed -> expect nulls
  (small companies file only an abridged balance sheet; no P&L/revenue). Currency EUR. Documents are PDF /
  structured electronic filing — no open machine-readable bulk.

## GISA open dataset ("Gewerbe in Österreich") — documented fields
```
gisa_zahl                 - GISA trade-register number
name / firmenwortlaut     - business name (no personal data)
standort / adresse        - location/address
gewerbewortlaut           - wording of the trade authorization
gewerbeschluessel         - trade code (Gewerbeschlüssel; companion code list on data.gv.at)
status                    - active
```
- Trade licences, NOT a company master; **no guaranteed Firmenbuchnummer link**; excludes sole-trader PII.

## Mapping to internal company model
```
company_id          <- firmenbuchnummer (companies) else gisa_zahl (trade-licence holders)
registration_number <- firmenbuchnummer
tax_id / vat_id     <- uid (ATU########)
legal_name          <- firmenwortlaut / name
company_type        <- rechtsform (GmbH/AG/OG/KG/eU/...)
status              <- aufrecht/gelöscht (+ Ediktsdatei insolvency)
incorporation_date  <- eintragungsdatum
registered_address  <- geschaeftsanschrift / standort
municipality        <- sitz / Gemeinde
region              <- Bundesland (derive from address/Gemeinde)
activity_code       <- (no public ÖNACE; Geschäftszweig free text / GISA Gewerbeschlüssel as proxy)
financials[]        <- Jahresabschluss (paid) | aggregator
country             <- "Austria"
source_url/name/at, raw_record
```
See `normalized/companies.sample.jsonl` (schematic — no per-company open record was downloadable).
