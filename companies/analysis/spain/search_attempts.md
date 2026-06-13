# Spain — Search Attempts Log

## Attempt 1
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `BORME Boletín Oficial Registro Mercantil open data API XML datos abiertos BOE`
- Language: Spanish/English
- Why this query was tried: Find the open path to the commercial-register gazette (BORME).
- Top relevant URLs:
  - https://datos.gob.es/en/catalogo/ea0040819-boletin-oficial-del-registro-mercantil-borme
  - https://www.boe.es/datosabiertos/documentos/APIsumarioBORME.pdf
  - https://www.boe.es/datosabiertos/faq/borme.php
  - https://github.com/PabloCastellano/bormeparser
  - https://openmercantil.es/fuentes
- Result: BORME is open via the BOE REST API (HTML/PDF/XML), Sección I from 2009. Tools: bormeparser, OpenMercantil.
- Decision: Deep-dive the BOE API + OpenMercantil as the open company sources.

## Attempt 2
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Registro Mercantil Central España depósito cuentas anuales acceso datos empresas bulk download`
- Language: Spanish
- Why this query was tried: Find financial data (annual accounts) access and any bulk option.
- Top relevant URLs:
  - https://sede.registradores.org/site/mercantil
  - https://www.registradores.org/en/el-colegio/registro-mercantil
  - https://www.einforma.com/informacion-empresas/informes-empresas/cuentas-anuales
- Result: Annual accounts deposited in **XBRL** (individual + consolidated) + PDF; search by NIF/name; retained 6 years; **no bulk**.
- Decision: Catalog cuentas anuales as the (paid, per-company) financial source.

## Attempt 3
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `CNMV información financiera empresas cotizadas open data API XBRL annual financial reports Spain`
- Language: Spanish/English
- Why this query was tried: Find an OPEN financial source (listed companies).
- Top relevant URLs:
  - https://www.cnmv.es/ipps/
  - https://www.cnmv.es/portal/xbrl/xbrl
  - https://datos.gob.es/en/catalogo?publisher_display_name=Comisi%C3%B3n+Nacional+del+Mercado+de+Valores
- Result: CNMV publishes IFA (annual) + IFI (intermediate) as **open XBRL+PDF**; also on datos.gob.es. Listed issuers only.
- Decision: Catalog CNMV as the open financial source for listed companies.

## Attempt 4
- Date/time: 2026-06-14
- Source: WebFetch (BOE API PDF — binary, unreadable) + WebFetch (OpenMercantil /fuentes) + WebSearch (DIRCE)
- Query/targets: BORME API spec; OpenMercantil sources/bulk; `datos.gob.es empresas DIRCE INE`
- Language: Spanish/English
- Why this query was tried: Get exact API/bulk specifics; check INE DIRCE as a possible master.
- Top relevant URLs:
  - https://openmercantil.es/fuentes
  - https://datos.gob.es/en/catalogo/a06004074-directorio-central-de-empresas-dirce
  - https://www.ine.es/dyngs/INEbase/operacion.htm?...DIRCE
- Result: OpenMercantil = CC-BY bulk (CSV/Parquet/API), ~2.8M companies, **excludes financials**. DIRCE = **aggregate only**, not per-company.
- Decision: OpenMercantil = open master; DIRCE = secondary (benchmarks only).

## Attempt 5
- Date/time: 2026-06-14
- Source: WebFetch (BOE API HTML page) + WebFetch (OpenMercantil /descargas) + WebSearch (registradores precios)
- Targets: BORME API endpoints; OpenMercantil download URLs/columns; cuentas anuales pricing
- Language: Spanish/English
- Why this query was tried: Pin down exact endpoints, bulk file URLs/columns, and financial pricing.
- Top relevant URLs:
  - https://www.boe.es/datosabiertos/api/api.php
  - https://openmercantil.es/descargas
  - https://economia3.com/2024/03/28/601937-obtener-cuentas-anuales-de-una-empresa-registro-mercantil/
- Result: BORME API = `/datosabiertos/api/borme/sumario/{YYYYMMDD}` (JSON/XML). OpenMercantil full CSV 210 MB/12 cols ('Próximamente'), samples live. Cuentas anuales **~€8.99–20/company**.
- Decision: Download BORME + OpenMercantil samples; confirm structures.

## Attempt 6 (direct download/probing, not a search engine)
- Date/time: 2026-06-14
- Source: curl against the BOE API and OpenMercantil sample URLs
- Result:
  - `borme/sumario/20240115` → HTTP 200 JSON (38 KB) — summary lists per-province act bulletins (Sección A/C).
  - `diario_borme/xml.php?id=BORME-A-2024-10-04` → HTTP 200 XML (7.7 KB) — Almería acts as `<p class="articulo">` (company) / `<p class="parrafo">` (acts), incl. Constitución/Nombramientos/Ampliación de capital/Disolución + Datos registrales (Tomo/Folio/Sección/**Hoja**).
  - OpenMercantil `muestra_empresas_100.csv` (9.5 KB; cols slug,name,cif,province,first_seen,last_seen,acts_count) and `muestra_nuevas_50.json` (11.7 KB) — HTTP 200.
- Decision: Saved all four to raw/ with metadata+SHA-256; built a normalized sample from OpenMercantil; confirmed BORME needs prose parsing and CIF coverage is sparse.
