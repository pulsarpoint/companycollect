# BORME via BOE open-data API — Field Catalog

## Source Summary

- Country: Spain
- Source type: official_gazette_api
- Organization: Agencia Estatal Boletín Oficial del Estado (BOE)
- URL: `https://www.boe.es/datosabiertos/api/borme/sumario/{YYYYMMDD}` (summary) →
  `https://www.boe.es/diario_borme/xml.php?id=BORME-A-{YYYY}-{N}-{PP}` (per-province acts)
- License: BOE open-data reuse (open; confirm attribution)
- Access: public, no auth
- Freshness: daily (business days); Sección I (Empresarios) 2009→, Sección II 2001→
- Record shape: summary (JSON/XML) → per-province bulletin XML with company acts as **prose**
- Primary keys: `bulletin_id + acto number`; `hoja_registral`
- Join keys: `hoja_registral`; `cif` (rarely present in act text)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| …metadatos.fecha_publicacion | fecha_publicacion | Issue date | date | date | `20240115` | YYYYMMDD |
| …seccion[].codigo | codigo | Section A/C | string | metadata | `A` | A = company acts |
| …item[].identificador | identificador | Province bulletin id | string | identifier | `BORME-A-2024-10-04` | fetch its XML |
| …item[].titulo | titulo | Province | string | geography | `ALMERÍA` | scope |
| …item[].url_xml | url_xml | Bulletin XML URL | string | document | `…xml.php?id=BORME-A-2024-10-04` | also pdf/html |
| p.articulo | p.articulo | `{n} - {COMPANY NAME}.` | string | legal_name | `19589 - PATRIMONIAL FAMILIA ZAMORA SOCIEDAD LIMITADA.` | split n/name |
| p.parrafo | p.parrafo | Acts prose | string | filing | `Nombramientos. Liquidador: … Datos registrales. T 2045, F 33, S 8, H AL 52199, I/A 4.` | **parse** |
| Datos registrales → H | Hoja (H) | Registry sheet | string | identifier | `AL 52199` | **stable key** |
| Datos registrales → T/F/S/I·A | T,F,S,I/A | Registry coordinates | string | metadata | `T 2045, F 33, S 8` | pinpoints entry |
| Constitución/Ampliación → Capital | Capital | Share capital | decimal | financial | `909.000,00 Euros` | register capital, es locale |
| Nombramientos/Ceses | Adm./Liquidador/Auditor | Officers | string | person | `Adm. Unico: MILLAN LUNA FRANCISCO` | **PII** |
| Declaración de unipersonalidad | Socio único | Sole shareholder | string | ownership | `Socio único: …` | only ownership signal |
| Disolución/Extinción | Disolución/Extinción | Dissolution | string | status | `Disolución. Voluntaria. Extinción.` | status+date |

## Interpretation Notes

- **The summary is an index, the company data is in the per-province XML.** Crawl: summary by date →
  list section-A `item`s → fetch each `url_xml` → parse the acts.
- **Acts are semi-structured Spanish prose.** `<p class="articulo">` names the company (with a sequence
  number), the following `<p class="parrafo">` lists the acts. Use **`bormeparser`** (or equivalent
  regex/NLP). Each act ends with **"Datos registrales"** encoding `T`(Tomo)/`F`(Folio)/`S`(Sección)/
  **`H`(Hoja)**/`I/A`(Inscripción·Asiento).
- **Hoja registral is the stable per-company key** (e.g. `AL 52199` = Almería, sheet 52199). BORME act
  text usually does **not** include the CIF, so dedup a company master on Hoja, not CIF.
- **Numbers are es-locale** (`909.000,00`). Parse `Capital` accordingly; it is **register share capital**,
  not annual-accounts data.
- **Personal data**: administrators, sole shareholders, liquidators, apoderados are named. Apply a GDPR
  lawful basis + retention policy before persisting beyond the raw zone.
- **Ownership** is limited to the **sole-shareholder** ("Declaración de unipersonalidad") case — there is
  no general cap table in BORME.
- A real summary (`borme_sumario_20240115.json`) and province XML (`borme_A_2024_10_04_almeria.xml`) are
  in `data/spain/raw/api/`. No per-company `sample_record.json` is added here (the act is prose, not a
  record); see the raw XML instead.
