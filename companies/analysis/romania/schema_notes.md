# Romania — Schema Notes

## Identifiers

- **CUI** (`Cod Unic de Înregistrare`) — numeric fiscal/unique registration code;
  the **company id** used by ANAF. In OD_FIRME the value `0` marks entities
  without a CUI (e.g. some `PF`/`PFA`). VAT number = `RO` + CUI for VAT-registered
  firms.
- **COD_INMATRICULARE** — trade-register registration number. Two formats:
  - classic `J40/630/1992` = `<letter><county>/<serial>/<year>` (J = company,
    F = PFA/family, C = cooperative);
  - newer numeric `J2002000372404`.
  This is the **join key across all ONRC companion CSVs**.
- **EUID** — European Unique Identifier, e.g. `ROONRC.J2002000372404`.
- **Bridge**: OD_FIRME carries BOTH CUI and COD_INMATRICULARE → links companion
  CSVs (COD_INMATRICULARE) to ANAF financials/VAT (CUI).

## OD_FIRME.CSV (`^`-delimited, UTF-8 BOM)

| Column | Meaning |
|---|---|
| DENUMIRE | Legal name |
| CUI | Fiscal/unique code (company id); 0 = none |
| COD_INMATRICULARE | Registration number (J40/.../yyyy or numeric) |
| DATA_INMATRICULARE | Registration date (DD/MM/YYYY) |
| EUID | European Unique Identifier (ROONRC.<reg>) |
| FORMA_JURIDICA | Legal form (SRL, SA, PF, PFA, SCS, SNC, …) |
| ADR_TARA | Country |
| ADR_JUDET | County (județ) |
| ADR_LOCALITATE | Locality / Bucharest sector |
| ADR_DEN_STRADA / ADR_NR_STRADA / ADR_BLOC / ADR_SCARA / ADR_ETAJ / ADR_APARTAMENT | Street / number / block / staircase / floor / apartment |
| ADR_COD_POSTAL | Postal code |
| ADR_SECTOR | Bucharest sector |
| ADR_COMPLETARE | Address free-text addition |
| WEB | Website (often blank) |
| TARA_FIRMA_MAMA | Parent-company country (for branches) |

## OD_STARE_FIRMA.CSV

| Column | Meaning |
|---|---|
| COD_INMATRICULARE | Registration number (join key) |
| COD | Status code |

Observed status codes (need the ONRC nomenclator for full decode): **1048** =
funcţiune (active), **1084** = radiată (struck off / deregistered), **2069** =
dizolvare (dissolution). Map at ingestion time.

## OD_CAEN_AUTORIZAT.CSV

| Column | Meaning |
|---|---|
| COD_INMATRICULARE | Registration number (join key) |
| COD_CAEN_AUTORIZAT | Authorized activity code (CAEN) |
| VER_CAEN_AUTORIZAT | CAEN revision/version |

Multiple rows per company (one per authorized activity). CAEN ≈ NACE.

## OD_REPREZENTANTI_LEGALI.CSV — PERSONAL DATA (GDPR)

| Column | Meaning |
|---|---|
| COD_INMATRICULARE | Registration number (join key) |
| PERSOANA_IMPUTERNICITA | Representative name (person or legal entity, e.g. liquidator) |
| CALITATE | Capacity/role (administrator, lichidator, …) |
| DATA_NASTERE | Date of birth (PII; often blank for legal-entity reps) |
| LOCALITATE_NASTERE / JUDET_NASTERE / TARA_NASTERE | Birth place (PII) |
| LOCALITATE / JUDET / TARA | Current locality / county / country |

Redact birth fields and names in published outputs.

## OD_SUCURSALE_ALTE_STATE_MEMBRE.CSV

| Column | Meaning |
|---|---|
| COD_INMATRICULARE | Parent registration number (join key) |
| TIP_UNITATE | Unit type (Sucursală = branch) |
| DENUMIRE_SUCURSALA | Branch name |
| EUID | Branch EUID (often blank) |
| COD_FISCAL | Branch fiscal code (often 0/blank) |
| TARA | Country of the branch |

## ANAF /bilant JSON (financials)

Top level: `an` (year), `cui`, `deni` (name), `caen` (activity code),
`den_caen` (activity description), `i[]` (indicators).
Each indicator: `indicator` (I-code), `val_indicator` (number, **RON**),
`val_den_indicator` (Romanian label).

Key indicator codes (non-financial-sector entity):

| Code | Label (RO) | Meaning |
|---|---|---|
| I1 | ACTIVE IMOBILIZATE - TOTAL | Fixed assets total |
| I2 | ACTIVE CIRCULANTE - TOTAL | Current assets total |
| I3 | Stocuri | Inventories |
| I4 | Creante | Receivables |
| I5 | Casa si conturi la banci | Cash and bank |
| I6 | CHELTUIELI IN AVANS | Prepaid expenses |
| I7 | DATORII | Liabilities |
| I8 | VENITURI IN AVANS | Deferred income |
| I9 | PROVIZIOANE | Provisions |
| I10 | CAPITALURI - TOTAL | Equity total |
| I11 | Capital subscris varsat | Subscribed paid-up capital |
| I13 | Cifra de afaceri neta | Net turnover |
| I14 | VENITURI TOTALE | Total revenue |
| I15 | CHELTUIELI TOTALE | Total expenses |
| I16 | Profit brut | Gross profit |
| I17 | Pierdere bruta | Gross loss |
| I18 | Profit net | Net profit |
| I19 | Pierdere neta | Net loss |
| I20 | Numar mediu de salariati | Average number of employees |

(Insurance/financial entities use additional codes up to I33.) Currency RON;
values are plain RON (not thousands). Coverage verified **2014–2024**.

## Mapping to internal model

| Internal | Romania source |
|---|---|
| company_id | OD_FIRME.CUI |
| registration_number | OD_FIRME.COD_INMATRICULARE |
| tax_id | OD_FIRME.CUI |
| vat_id | "RO" + CUI (if VAT-registered; confirm via ws/tva.scpTVA) |
| legal_name | OD_FIRME.DENUMIRE |
| company_type / legal_form | OD_FIRME.FORMA_JURIDICA |
| status | OD_STARE_FIRMA.COD (decode) |
| incorporation_date | OD_FIRME.DATA_INMATRICULARE |
| dissolution_date | implied by status (radiată/dizolvare); not a dedicated field |
| registered_address | OD_FIRME ADR_* fields |
| municipality / region | OD_FIRME.ADR_LOCALITATE / ADR_JUDET |
| activity_code | OD_CAEN_AUTORIZAT.COD_CAEN_AUTORIZAT |
| financials | ANAF /bilant indicators (RON) |
| officers | OD_REPREZENTANTI_LEGALI (PII; redact) |
| owners | ONRC portal (paid) / RBR (restricted) — not open |
