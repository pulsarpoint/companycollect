# Mexico — Schema Notes

## Identifiers

- **RFC (Registro Federal de Contribuyentes)** — the tax id. **12 characters** for
  legal entities (3 letters + 6-digit date + 3 homoclave), **13** for individuals.
  Mexico has **IVA (VAT)** but **no separate VAT number** — the RFC is the tax id.
- **folio mercantil electrónico** — the Registro Público de Comercio (RPC) id for a
  company.
- **clee (Clave Única del Establecimiento)** — 28-char DENUE establishment code;
  **`id`** — DENUE numeric establishment id.
- **No shared open join key**: DENUE has no RFC/folio; the registry and SAT use
  RFC/folio. Cross-source joins are by **name** (fuzzy) unless RFC is sourced
  separately.

## INEGI DENUE CSV (42 columns, verified)

| Column | Meaning |
|---|---|
| id | DENUE establishment id |
| clee | Unique establishment code (28-char) |
| nom_estab | Trade / establishment name |
| raz_social | Legal / corporate name (when applicable; blank for many micro-units) |
| codigo_act | SCIAN activity code (6-digit) |
| nombre_act | SCIAN activity name |
| per_ocu | Employee size band (e.g. "0 a 5 personas") |
| tipo_vial, nom_vial, numero_ext, letra_ext, … | Street address components |
| tipo_asent, nomb_asent | Settlement type / name (colonia) |
| cod_postal | Postal code |
| cve_ent, entidad | State code / name |
| cve_mun, municipio | Municipality code / name |
| cve_loc, localidad | Locality code / name |
| ageb, manzana | Census geo (AGEB, block) |
| telefono, correoelec, www | Contact — **may be personal data; redact** |
| tipoUniEco | Type of economic unit (Fijo, Semifijo, …) |
| latitud, longitud | Geolocation |
| fecha_alta | Date added to DENUE (YYYY-MM) |

- Encoding: **Latin-1 (ISO-8859-1)**. Bulk: `denue_{EE}_csv.zip`, EE = 01..32 per
  state (00 = national). Updated ~twice a year.

## SAT 69-B CSV (verified)

| Column | Meaning |
|---|---|
| No | Row number |
| RFC | Taxpayer RFC (12-char companies) |
| Nombre del Contribuyente | Legal name |
| Situación del contribuyente | Presunto / Definitivo / Desvirtuado / Sentencia Favorable |
| oficio + fecha (presunción/definitivo/…) | SAT office references and dates |
| Publicación DOF | Official-gazette publication refs |

- 3 preamble lines precede the header row. Encoding Latin-1.

## RPC / PSM legal registry (per-document)

folio mercantil electrónico, denominación / razón social, tipo societario (S.A.
de C.V., S. de R.L. de C.V., S.A.P.I., S.C., …), fecha de constitución, domicilio,
capital social, objeto social. Search/per-document; fee-based extracts.

## Financials

Private companies: **not public**. Listed issuers: BMV (EMISNET) / CNBV (SITI) —
XBRL/Excel financial statements, **MXN**.

## Dates, money, encoding

- Dates: `YYYY-MM` (DENUE fecha_alta); `DD/MM/YYYY` (SAT).
- Money: **MXN** (financials, capital).
- Encoding: **Latin-1** for INEGI/SAT CSVs (convert to UTF-8).

## Internal model mapping

```text
company_id          <- DENUE id / clee (establishment) ; folio mercantil (legal, per-doc)
registration_number <- folio mercantil electrónico (RPC; not in DENUE)
tax_id              <- RFC (SAT; not in DENUE)
vat_id              <- RFC (no separate VAT id)
legal_name          <- DENUE raz_social (or registry denominación)
normalized_name     <- DENUE nom_estab (trade name)
status              <- DENUE presence (active in directory) ; SAT 69-B situation (risk)
legal_form          <- registry tipo societario (per-doc) ; inferable from raz_social suffix
activity_code       <- DENUE codigo_act (SCIAN)
registered_address  <- DENUE address fields (+ lat/long)
financials          <- BMV/CNBV (listed only) ; private not public
officers            <- registry/notarial docs (per-doc; personal data)
```
