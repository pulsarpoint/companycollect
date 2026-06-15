# Slovenia — Schema Notes

## Identifiers

- **matična številka** — registration number; the universal join key (PRS ↔ FURS).
  10-digit in the open feeds (7-digit base unit + 3-digit suffix, suffix `000`
  for the main entity).
- **davčna številka** — tax number (8-digit); from FURS. **VAT id = `SI` +
  davčna** when VAT-registered.
- **HSEID** (a.k.a. HSMID) — unique building/address identifier (PRS).
- **SKD** — Standardna klasifikacija dejavnosti (≈ NACE Rev.2); FURS `Šifra
  dejavnosti` (e.g. `49.410`).

## AJPES PRS — opsiprs.csv (UTF-16, comma-delimited, quoted)

| Column | Meaning |
|---|---|
| Matična številka | Registration number (join key) |
| Popolno ime | Full registered name |
| HSEID | Unique address/building identifier |
| Pravnoorganizacijska oblika | Legal form (text, e.g. "Družba z omejeno odgovornostjo d.o.o.") |
| Registrski organ | Registering authority (court / AJPES branch) |
| Ulica | Street |
| Hišna št / Hišna št dodatek | House number / addendum |
| Naselje | Settlement |
| Poštna št / Pošta | Postal code / post office |
| Država | Country |

293,222 rows; all entity types (d.o.o., s.p., društvo, poslovna enota, javni
zavod, sindikat, …). No tax number, status, SKD, or financials.

## FURS — DURS_zavezanci_PO.csv (UTF-8 BOM, semicolon-delimited)

| Column | Meaning |
|---|---|
| Omejen obseg identifikacije | Limited scope of identification flag |
| Zavezanost za DDV | VAT liability (`*` = VAT payer) |
| Davčna številka | Tax number (8-digit) |
| Matična številka | Registration number (join key) |
| Datum registracije za DDV | VAT registration date (DD.MM.YYYY) |
| Šifra dejavnosti | SKD activity code (e.g. 49.410) |
| Ime zavezanca | Name (note: trailing spaces) |
| Naslov zavezanca | Address (single string) |
| Finančni urad | Tax office code |

144,537 legal entities. VAT id = `SI` + Davčna številka when Zavezanost za DDV = `*`.

## Mapping to internal model

| Internal | Slovenia source |
|---|---|
| company_id | matična številka (PRS/FURS) |
| registration_number | matična številka |
| tax_id | FURS Davčna številka |
| vat_id | "SI" + Davčna številka (if VAT-registered) |
| legal_name | PRS Popolno ime |
| company_type / legal_form | PRS Pravnoorganizacijska oblika (text) |
| status | **not in open data** (restPrsInfo, credentialed) |
| incorporation_date | **not in open data** (restPrsInfo) |
| dissolution_date | **not in open data** |
| registered_address | PRS Ulica + Hišna št + Naselje + Poštna št + Pošta |
| activity_code | FURS Šifra dejavnosti (SKD) |
| financials | **not in open data** — JOLP view-only / Fi=Po paid |
| officers | **not in open data** (court register / restPrsInfo) |
| owners | **not in open data** |

## Encodings & gotchas

- PRS is **UTF-16** — convert on ingest. FURS is **UTF-8 BOM, semicolon**.
- FURS name/address have trailing spaces — trim.
- Status, incorporation date, officers, ownership, and financials are **absent
  from the open feeds** — only via credentialed restPrsInfo (status/SKD/history),
  the court register, or paid Fi=Po (financials).
