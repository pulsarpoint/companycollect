# Slovenia — Search Attempts

## Attempt 1
- Date/time: 2026-06-15
- Source: WebSearch
- Query: `AJPES Poslovni register Slovenije PRS open data odprti podatki download API companies`
- Language: Slovenian/English
- Why: find the official business register open data + API.
- Result: PRS open CSV on OPSI (matična, name, address, legal form, registrar, HSMID); twice-monthly; CC-BY 4.0. `restPrsInfo` REST service (credentialed).
- Decision: get the OPSI CSV download URL; check restPrsInfo terms.

## Attempt 2
- Date/time: 2026-06-15
- Source: WebSearch
- Query: `AJPES letna poročila JOLP financial statements open data Slovenia companies financial data API`
- Why: find financial-statement data.
- Result: JOLP publishes annual reports free to view; no explicit open API. Fi=Po/S.BON are paid.
- Decision: confirm whether structured financials are open or paid.

## Attempt 3
- Date/time: 2026-06-15
- Source: WebFetch (OPSI PRS dataset + AJPES Ponovna_uporaba)
- Result: PRS CSV direct URL (120.6 MB, CC-BY 4.0, twice-monthly); `restPrsInfo` requires credentials and is "not for mass download".
- Decision: download the PRS CSV; treat restPrsInfo as credentialed.

## Attempt 4
- Date/time: 2026-06-15
- Source: curl
- Query: download opsiprs.csv
- Result: 127 MB, **UTF-16**, 293,222 rows. Columns: Matična številka, Popolno ime, HSEID, Pravnoorganizacijska oblika, Registrski organ, + address. No tax number / status / SKD / financials.
- Decision: need tax number + activity from another open source.

## Attempt 5
- Date/time: 2026-06-15
- Source: WebSearch + WebFetch (OPSI seznam-davcnih-zavezancev)
- Query: `FURS zavezanci za DDV davčni zavezanci seznam open data`
- Result: FURS tax-payer lists on OPSI, CC-BY 4.0, daily. Legal-entities CSV = `DURS_zavezanci_PO_csv.zip`.
- Decision: download and inspect.

## Attempt 6
- Date/time: 2026-06-15
- Source: curl + unzip
- Query: DURS_zavezanci_PO_csv.zip
- Result: `DURS_zavezanci_PO.csv` (UTF-8 BOM, semicolon), 144,537 legal entities. Columns: Zavezanost za DDV, **Davčna številka**, **Matična številka**, Datum registracije za DDV, **Šifra dejavnosti (SKD)**, Ime, Naslov, Finančni urad. VAT `*` flag.
- Decision: join FURS↔PRS on matična številka → identity + tax/VAT/activity. Verified join (ISTRA XLL d.o.o., MB 3282490000, SI10001310, SKD 49.410).

## Attempt 7
- Date/time: 2026-06-15
- Source: WebFetch (ajpes.si/FinancialData)
- Result: JOLP = free view-only; Fi=Po (structured financials DB) and S.BON (ratings) are **paid**. No open structured financial download.
- Decision: financials = view-only/paid; mark as a gap.
