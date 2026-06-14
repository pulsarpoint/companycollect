# RDF — Financial Statements (e-Sprawozdania) — Field Catalog

> **OPEN, structured.** Free per-company download of filed financial statements as **XML** (MF logical
> schema) + PDF; XBRL for listed. Fields documented from the Ministry of Finance e-Sprawozdania schema;
> the per-company XML was not bulk-downloaded here (interactive per-document flow), so no sample_record.

## Source Summary

- Country: Poland
- Source type: official_financial_disclosure
- Organization: Ministerstwo Sprawiedliwości (KRS)
- URL: https://ekrs.ms.gov.pl/rdf/pd/search_df (free per-company); PRS-eKRS mass API (registration)
- License: open (free access/download)
- Access: public, no auth (per-company)
- Freshness: annual filing; continuous
- Record shape: per-company per-period **e-Sprawozdanie finansowe XML** (+ PDF; XBRL for listed)
- Primary keys: `krs + okresOd + okresDo`
- Join keys: `krs`

## Fields

| Path | Source field (PL) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| krs | numerKRS | Entity KRS | string | identifier | join |
| Naglowek.okresOd/okresDo | okres | Period | date | date | per-statement key |
| Naglowek.typJednostki | typ jednostki | mikro/małe/inne/banki | string | filing | drives nullability |
| Bilans.Aktywa.SumaBilansowa | suma bilansowa | Total assets | decimal | financial | PLN |
| Bilans.Aktywa.AktywaTrwale | aktywa trwałe | Fixed assets | decimal | financial | |
| Bilans.Aktywa.AktywaObrotowe | aktywa obrotowe | Current assets | decimal | financial | |
| Bilans.Pasywa.KapitalWlasny | kapitał własny | Equity | decimal | financial | |
| Bilans.Pasywa.ZobowiazaniaIRezerwy | zobowiązania i rezerwy | Liabilities | decimal | financial | |
| RachunekZyskowIStrat.PrzychodyNetto | przychody netto ze sprzedaży | Revenue | decimal | financial | primary revenue |
| RachunekZyskowIStrat.ZyskStrataOperacyjny | zysk/strata operacyjny | Operating result | decimal | financial | |
| RachunekZyskowIStrat.ZyskStrataNetto | zysk/strata netto | Net income | decimal | financial | neg = loss |
| InformacjaDodatkowa.przecietneZatrudnienie | przeciętne zatrudnienie | Avg employees | integer | employment | notes |
| Naglowek.waluta/jednostka | waluta/jednostka | Currency + unit | string | financial | PLN; whole/thousands |

## Interpretation Notes

- **Open machine-readable financials** — Poland's standout vs DE/IT/ES: the actual **bilans** + **rachunek
  zysków i strat** are downloadable as structured **XML** per company, **free**, keyed on KRS. Triggered by
  the KRS `wzmiankiOZlozonychDokumentach`.
- **Schema variants + versions**: the MF logical schema differs by entity type (mikro / małe / inne / banki
  / ubezpieczyciele) and is **versioned yearly** — the parser must handle multiple XSD versions and the two
  P&L variants (kalkulacyjny / porównawczy). Some entities file PDF-only; listed/consolidated may use XBRL.
- **Units**: figures may be in **whole PLN or thousands** — read `jednostka` and scale; store currency.
- **Nullability**: mikro/małe disclose fewer lines → revenue/net_income/employees nullable.
- **Mass access**: the PRS-eKRS automated module requires registration; the per-company download is open.
