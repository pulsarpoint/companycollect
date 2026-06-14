# RÚZ Financial Statements & Reports (Open API) Field Catalog

## Source Summary

- Country: Slovakia
- Source type: official_registry
- Organization: Register účtovných závierok (Ministry of Finance SR / DataCentrum)
- URL: https://www.registeruz.sk/cruz-public/api/ (`uctovna-zavierka`, `uctovny-vykaz`, `sablona`)
- License: CC0
- Access: public
- Freshness: continuous (incremental via `zmenene-od`)
- Record shape: statement metadata → report tables (positional) decoded via template
- Primary keys: `uctovna_zavierka_id`
- Join keys: `ico` (via the accounting unit `idUJ`)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| zavierka.id | id | Statement id | integer | identifier | 6500234 | |
| zavierka.idUJ | idUJ | Accounting-unit id | integer | identifier | 154048 | → IČO |
| zavierka.obdobieOd/Do | — | Reporting period | string | date | 2024-01 / 2024-12 | fiscal year |
| zavierka.datumZostaveniaK | datumZostaveniaK | Balance date | date | date | 2024-12-31 | |
| zavierka.typ | typ | Statement type | string | metadata | Riadna | |
| zavierka.idUctovnychVykazov[] | — | Report ids | array | filing | [9793753,…] | → uctovny-vykaz |
| vykaz.idSablony | idSablony | Template id | integer | metadata | 687 | decode key |
| vykaz.obsah.tabulky[].nazov.sk | nazov | Table name | string | financial | Strana aktív | |
| vykaz.obsah.tabulky[].data[] | data | Cell values (positional) | array | financial | ["5000","",…] | EUR; map to template |
| vykaz.prilohy[] | prilohy | PDF attachments | array | document | Vybrané údaje.PDF | |
| sablona…riadky[].text.sk | text | Line-item label | string | financial | SPOLU MAJETOK | decoder |
| sablona…riadky[].cisloRiadku/oznacenie | — | Row no./designation | string | financial | 1 / A.I. | |

## Interpretation Notes

- **Full structured financials**, CC0. Walk: accounting unit
  `idUctovnychZavierok[]` → `uctovna-zavierka?id=` (period, type,
  `idUctovnychVykazov[]`) → `uctovny-vykaz?id=` (`obsah.tabulky[]`).
- **Decoding `data[]`**: each table's `data[]` is a **positional** array of cell
  strings. Decode against the matching `sablona?id={idSablony}` —
  `tabulky[].riadky[]` give the line-item labels (`text.sk`), `cisloRiadku`, and
  `oznacenie`. Tables: **Strana aktív** (assets), **Strana pasív**
  (liabilities/equity), **Výkaz ziskov a strát** (income statement). Each row
  spans several columns (current/prior period; for assets gross/correction/net).
  Empty string = blank; monetary values are **EUR**.
- **Templates** vary by entity size (Úč MUJ micro = 687; small/large use other
  ids). **Cache** templates by id (they rarely change).
- **Large/consolidated filers** (e.g. ESET) may have empty `obsah` and only a PDF
  attachment ("Vybrané údaje") — fall back to the PDF or RPO equities.
- `sample_record.json` is a real `uctovny-vykaz` with populated tables (template
  687); `data[]` truncated for readability.
