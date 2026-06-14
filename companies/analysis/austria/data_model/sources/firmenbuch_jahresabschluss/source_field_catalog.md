# Jahresabschluss (Annual Accounts) — Field Catalog

> **PLANNING-ONLY (paid).** Annual accounts for GmbH/AG, filed to the Firmenbuch Urkundensammlung and
> publicly accessible **per document for a fee** via clearing houses. **No open bulk, no free API.** Fields
> from documented UGB accounting structure; **no records or values copied**.

## Source Summary

- Country: Austria
- Source type: official_financial_disclosure
- Organization: BMJ / Firmenbuchgerichte; clearing houses
- URL: https://justizonline.gv.at/ (filing since 1.1.2026); retrieval via Verrechnungsstellen
- License: publicly accessible for a fee; contractual → planning-only
- Access: **paid** per document
- Freshness: annual filing
- Record shape: per-company per-Geschäftsjahr filed document (PDF / structured electronic filing)
- Primary keys: `firmenbuchnummer + geschaeftsjahr`
- Join keys: `firmenbuchnummer`

## Fields

| Path | Source field (DE) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| firmenbuchnummer | Firmenbuchnummer | Company id | string | identifier | join |
| geschaeftsjahr | Geschäftsjahr / Bilanzstichtag | Fiscal year | date | date | per-statement key |
| groessenklasse | Größenklasse (§221 UGB) | Kleinst/klein/mittel/groß | string | filing | drives nullability |
| bilanz.bilanzsumme | Bilanzsumme | Total assets | decimal | financial | EUR |
| bilanz.anlagevermoegen | Anlagevermögen | Fixed assets | decimal | financial | |
| bilanz.umlaufvermoegen | Umlaufvermögen | Current assets | decimal | financial | |
| bilanz.eigenkapital | Eigenkapital | Equity | decimal | financial | |
| bilanz.verbindlichkeiten | Verbindlichkeiten/Rückstellungen | Liabilities | decimal | financial | |
| guv.umsatzerloese | Umsatzerlöse | Revenue | decimal | financial | **often absent (small)** |
| guv.jahresergebnis | Jahresüberschuss/-fehlbetrag | Net income | decimal | financial | neg = loss |
| anhang.mitarbeiter | durchschnittliche Mitarbeiterzahl | Avg employees | integer | employment | in notes |

## Interpretation Notes

- **The financial source — but paid.** Austria's annual accounts are systematic (UGB) but accessed **per
  document for a fee** via clearing houses; there is **no open bulk**. Realistic financials at scale →
  a **commercial aggregator** (Compass / KSV1870 / firmafind).
- **Size class drives disclosure (§221 UGB).** *Kleinst*/*klein* companies file only an **abridged balance
  sheet** — **no GuV, so no `umsatzerloese`/`net_income`**. Expect those nullable for the majority of
  Austrian GmbHs. *mittel*/*groß* add the income statement + notes.
- **Format**: filed documents are **PDF / structured electronic filing** — not an open machine-readable
  feed; extracting figures needs parsing (or an aggregator's structured API). Currency EUR; AT number locale.
- No `sample_record.json` — paid source; values not retrievable under planning-only terms.
