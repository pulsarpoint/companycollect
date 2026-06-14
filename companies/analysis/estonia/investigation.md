# Estonia — Company Open Data Investigation

## Conclusion

Estonia is a **best-in-class fully-open** country for company data — including **structured financial
statements**, **beneficial owners** and **shareholders** as open bulk. Everything joins on the
**registrikood** (Äriregistri kood, 8-digit registry code). The single authoritative source is the
**e-Business Register open data** (Äriregister) published by **RIK** (Registrite ja Infosüsteemide Keskus) at
`avaandmed.ariregister.rik.ee`, free since **1 October 2022** under **CC-BY 4.0**.

## What was verified (live, with real downloads)

- Download page `avaandmed.ariregister.rik.ee/en/downloading-open-data` (saved) — enumerated every dataset URL.
- **Basic data** `ettevotja_rekvisiidid__lihtandmed.csv.zip` → HTTP 200, **18 MB zip**, **373,025 companies**.
  Fields: `nimi`, `ariregistri_kood`, `ettevotja_oiguslik_vorm`, `kmkr_nr` (VAT), `ettevotja_staatus(_tekstina)`,
  `ettevotja_esmakande_kpv` (first registration), address + `asukoha_ehak_kood` (EHAK), normalized address,
  `teabesysteemi_link`.
- **General data** `...__yldandmed.json.zip` → HTTP 200, **225 MB** (deeper fields; not fully downloaded).
- **Annual report financials** (verified, downloaded):
  - `1.aruannete_yldandmed_*.zip` → report metadata (18 MB zip / 228 MB csv): `report_id`, `registrikood`,
    `aruandeaasta`, `period_start/end`, `kas konsolideeritud?`, `kas auditeeritud?`, `audiitori otsuse tüüp`,
    audit firm.
  - `4.2024_aruannete_elemendid_*.zip` → **financial line items** (23 MB zip / 314 MB csv): `report_id`,
    `tabel` (e.g. Lõppbilanss = closing balance sheet), `elemendi_label` (Estonian), `elemendi_nimetus`
    (XBRL-like English name, e.g. `CurrentAssets`, `Equity`, `TotalAnnualPeriodProfitLoss`), `vaartus` (value).
    Years 2019–2025 available.
  - `2.EMTAK_myygitulu_*.zip` → revenue by activity (10 MB zip): `report_id`, `emtak`, `Jaotatud müügitulu`,
    `põhitegevusala`, `emtak_version`.
- **Beneficial owners** `...__kasusaajad.json.zip` → HTTP 200, **27 MB** (open BO data — reachability confirmed).
- **Shareholders** `...__osanikud.json.zip` → HTTP 200, **33 MB** (open shareholder data — confirmed).
- Other datasets present: `kaardile_kantud_isikud` (officers/persons on card), `registrikaardid`,
  `kommertspandid` (commercial pledges), `maarused` (court rulings), `kandevalised_isikud`, plus **Parquet**
  versions of lihtandmed/yldandmed.

## Identifiers

- **registrikood / ariregistri_kood** — 8-digit registry code; the universal join key (also the `report_id`
  bridge for financials).
- **KMKR** — VAT number, `EE` + 9 digits (e.g. `EE101335276`), present in the basic data (`kmkr_nr`).
- **EHAK** — administrative-unit classifier code for the address (`asukoha_ehak_kood`).

## Financial data model

Three joined layers, all CC-BY 4.0, updated monthly:

```
aruannete_yldandmed (report_id, registrikood, aruandeaasta, audited?, consolidated?, auditor)
   └─ {year}_aruannete_elemendid (report_id, tabel, elemendi_nimetus [XBRL], vaartus)   <- balance sheet + P&L line items
   └─ EMTAK_myygitulu (report_id, emtak, müügitulu)                                       <- revenue by activity
   └─ myygitulu_geograafiline (report_id, geography, revenue)                             <- revenue by geography
```

Join `report_id` → `aruannete_yldandmed.registrikood` → company. Currency **EUR**. This is genuine **structured
open financial data** (not PDF) — rare and excellent.

## Recommended ingestion

Bulk-first: load basic + general company data keyed on registrikood; load the per-year `elemendid` financial
line items joined via `report_id`; load beneficial owners + shareholders + officers. Use the XML/REST API for
real-time single-company refresh. Attribute RIK (CC-BY 4.0).

## Risks / open questions

- **GDPR**: beneficial owners, shareholders (natural persons) and officers are personal data — lawful basis +
  retention; no direct-marketing reuse. (CC-BY licence governs reuse, not data-protection law.)
- **Volume**: general data (225 MB JSON) and per-year financial elements (300+ MB CSV each) are large — stream/
  chunk, don't load whole.
- **Date format** `dd.mm.yyyy` in CSVs; **decimal** values are dot-decimal strings in the elements file.
- **report_id** is the financial bridge key — keep it to join the three financial layers before mapping to the
  company.
