# Spain — Schema Notes

Observed from the downloaded samples in `data/spain/raw/`. Spain has several distinct shapes; keep them
separate and join on **CIF/NIF** (sparse in open data) and the **Hoja registral** (registry sheet).

## 1. OpenMercantil company DB (CSV) — `raw/samples/openmercantil_muestra_empresas_100.csv`
```
slug         string  - URL slug, e.g. "mirador-de-bellavista-sl"
name         string  - legal name incl. form suffix, e.g. "MIRADOR DE BELLAVISTA SL"
cif          string  - Spanish tax id (CIF/NIF); OFTEN EMPTY (~18% populated overall)
province     string  - provincia, e.g. "Málaga", "Balears (Illes)"
first_seen   date    - first BORME appearance (YYYY-MM-DD)
last_seen    date    - last BORME appearance (YYYY-MM-DD)
acts_count   integer - number of BORME acts seen for the company
```
Full bulk CSV (per OpenMercantil docs) has 12 columns:
`Date, Section, Province, Company Name, CIF, Website, Capital, Address, Workers, Act Type, Details, ID`.

## 2. OpenMercantil "new companies" (JSON) — `raw/samples/openmercantil_muestra_nuevas_50.json`
```
[{ slug, name, cif, province, first_seen, last_seen, acts_count }]  // same fields as the CSV master
```

## 3. BORME via BOE API — summary — `raw/api/borme_sumario_20240115.json`
```
status.code / status.text
data.sumario.metadatos.{publicacion, fecha_publicacion}
data.sumario.diario[] -> { numero, seccion[] }
  seccion[].codigo   - "A" (Empresarios, actos inscritos) | "C" (Anuncios y avisos legales)
  seccion[].item[]   - one per province/registry:
     identificador   - "BORME-A-2024-10-04"  (A = section, 2024 = year, 10 = diario nº, 04 = province)
     titulo          - province, e.g. "ALMERÍA"
     url_pdf.texto / url_html / url_xml  - links to the act bulletin
```

## 4. BORME per-province act XML — `raw/api/borme_A_2024_10_04_almeria.xml`
```
documento.metadatos.{identificador, titulo(provincia), seccion, fecha_publicacion, url_pdf}
documento.texto:
  <p class="articulo">  -> "{acto_nº} - {COMPANY NAME}."        (one per company)
  <p class="parrafo">   -> free-text acts for that company, e.g.:
      "Nombramientos. Liquidador: ...  Disolución. Voluntaria. Extinción.  Datos registrales. T 2045 , F 33, S 8, H AL 52199, I/A 4 (8.01.24)."
```
Act types seen: **Constitución** (incorporation — carries Capital, Domicilio, Objeto social/CNAE,
administradores), **Nombramientos/Ceses/Dimisiones/Reelecciones** (officers), **Ampliación/Reducción de
capital** (with Capital + Suscrito), **Declaración de unipersonalidad** (sole shareholder),
**Modificaciones estatutarias**, **Disolución/Extinción**, **Cambio de domicilio**, **Auditor**.
- **Datos registrales** encode the registry sheet: `T`=Tomo, `F`=Folio, `S`=Sección, **`H`=Hoja**
  (e.g. `AL 52199`), `I/A`=Inscripción/Asiento. **The Hoja (province + number) is the stable per-company
  key** across acts. CIF is usually NOT in the act text.
- Parsing is **NLP/regex over Spanish prose** — use `bormeparser`; dedup acts into a company master.

## 5. Financials — annual accounts (Registro Mercantil XBRL) & CNMV XBRL  [paid / open-listed]
Spanish annual accounts (modelo PGC, XBRL taxonomy) concepts to expect:
```
balance:
  activo total (total assets), activo no corriente, activo corriente
  patrimonio neto (equity), pasivo no corriente, pasivo corriente
pérdidas y ganancias (P&L):
  importe neto de la cifra de negocios (revenue/turnover)
  resultado de explotación (operating result)
  resultado del ejercicio (net result)
otros:
  número medio de empleados (avg employees), memoria, informe de auditoría
  cuentas individuales + consolidadas; cuadro de posición económico-financiera por sector
```
- **Registro Mercantil cuentas anuales**: XBRL + XML + PDF, **paid ~€9–20/company**, no bulk.
- **CNMV (listed)**: IFA (annual) + IFI (intermediate) as **open XBRL+PDF**.
- Size class (PGC normal / abreviado / PYME / microempresa) governs how much is disclosed — expect
  nulls (e.g. no P&L detail) for the smallest filers.

## Identifiers & gotchas
- **CIF/NIF** = the Spanish tax id and de-facto company id (e.g. `B12345678`; letter = legal form).
  VAT id = `ES` + CIF. **Sparse in open BORME data** → enrichment needed to join to financials.
- **Hoja registral** (province + number) is the stable register key in BORME; use it for dedup when CIF
  is absent.
- **No clean CNAE/status** in BORME without parsing the Constitución act; DIRCE has CNAE but aggregate only.
- Names are uppercase with form suffixes: `SOCIEDAD LIMITADA`/`SL`, `SOCIEDAD ANÓNIMA`/`SA`, `SLU`, `SCP`.

## Mapping to internal company model
```
company_id          <- cif (when present) else hoja-registral key (province+Hoja)
registration_number <- Hoja registral (e.g. "AL 52199")  [+ Tomo/Folio]
tax_id              <- cif
vat_id              <- "ES" + cif
legal_name          <- name
normalized_name     <- lower(trim(name)) minus form suffix
company_type        <- derive from name suffix / CIF leading letter
status              <- derive from acts (Disolución/Extinción/En liquidación) ; else active/unknown
incorporation_date  <- BORME Constitución act date (first_seen as proxy)
dissolution_date    <- BORME Disolución/Extinción act date
registered_address  <- Domicilio (from Constitución/Cambio de domicilio act) | OpenMercantil Address
municipality        <- from address
region              <- province
country             <- "Spain"
financials[]        <- annual accounts (Registro Mercantil XBRL paid | CNMV open XBRL) keyed by year
source_url/source_name/source_retrieved_at
raw_record          <- original CSV row / act XML / XBRL
```
See `normalized/companies.sample.jsonl` and `normalized/companies.sample.csv` for the applied mapping.
