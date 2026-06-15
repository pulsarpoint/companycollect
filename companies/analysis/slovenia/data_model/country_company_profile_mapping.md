# Slovenia Company Profile — Mapping Report

Slovenia has **fully-open identity + tax/activity** but **no open structured
financials**. Two free CC-BY 4.0 datasets join on the **matična številka**:
AJPES PRS (identity/address) and FURS (tax number, VAT, SKD activity). Status,
incorporation date, officers, ownership, and financials sit behind the
credentialed restPrsInfo, the court register, or paid Fi=Po / view-only JOLP.

## Mapping Table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.maticna_stevilka | ajpes_prs | Matična številka | matična | PRS/FURS agree | company id + join key |
| tax_identifiers.davcna_stevilka | furs_zavezanci_po | Davčna številka | matična | FURS | tax id |
| tax_identifiers.vat_id | furs_zavezanci_po | "SI"+Davčna | matična | derived | if VAT-registered |
| tax_identifiers.vat_registered | furs_zavezanci_po | Zavezanost za DDV='*' | matična | FURS | |
| tax_identifiers.vat_registration_date | furs_zavezanci_po | Datum registracije za DDV | matična | FURS | |
| legal_identity.legal_name | ajpes_prs | Popolno ime | matična | PRS > FURS name | |
| legal_identity.legal_form | ajpes_prs | Pravnoorganizacijska oblika | matična | PRS | text label |
| status.status | ajpes_restprsinfo | status | matična | PLANNING-ONLY | credentialed |
| activity.skd_code | furs_zavezanci_po | Šifra dejavnosti | matična | FURS | SKD ≈ NACE |
| incorporation.registration_date | ajpes_restprsinfo | datumVpisa | matična | PLANNING-ONLY | credentialed |
| registered_location.* | ajpes_prs | Ulica/Hišna št/Naselje/Poštna št/Pošta/HSEID | matična | PRS | structured |
| registering_authority | ajpes_prs | Registrski organ | matična | PRS | court/AJPES |
| financial_statements[] | ajpes_jolp / ajpes_fipo | Bilanca stanja / Izkaz poslovnega izida | matična | PLANNING-ONLY | view-only / paid |
| officers[] | ajpes_restprsinfo | — | matična | PLANNING-ONLY | court register; PII |

## Source Precedence

1. **AJPES PRS** — authoritative for identity, legal form, address. CC-BY 4.0.
2. **FURS** — authoritative for tax number, VAT status/date, SKD activity. CC-BY 4.0.
3. **restPrsInfo** (credentialed) — status, registration date, full SKD, change
   list → planning-only.
4. **JOLP** (view-only) / **Fi=Po** (paid) — financials → planning-only.

On name conflict, prefer **PRS** `Popolno ime` (proper case) over FURS
`Ime zavezanca` (uppercase, padded).

## Join Keys

- **matična številka** joins PRS ↔ FURS (↔ restPrsInfo/JOLP/Fi=Po). `vat_id = "SI"
  + davčna številka` (davčna from FURS). The PRS open feed has **no** tax number,
  so FURS is required for any tax/VAT join.

## Missing / Restricted

- **Status, incorporation date, officers, ownership** — not open (restPrsInfo
  credentialed / court register).
- **Financials** — not openly downloadable: JOLP view-only, Fi=Po paid.
- **Encodings** — PRS UTF-16; FURS UTF-8 semicolon (trim trailing spaces).
