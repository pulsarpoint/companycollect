# INEGI DENUE Field Catalog

## Source Summary

- Country: Mexico
- Source type: statistical_business_directory
- Organization: Instituto Nacional de Estadística y Geografía (INEGI)
- URL: https://www.inegi.org.mx/contenidos/masiva/denue/denue_{EE}_csv.zip
- License: INEGI "Términos de Libre Uso de la Información del INEGI" (free use, attribution)
- Access: public, **no token** for the masiva CSV
- Freshness: updated ~twice a year
- Record shape: flat CSV, 42 columns, one row per **establishment**
- Primary keys: `id` (DENUE id)
- Join keys: `id`, `clee`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| id | id | DENUE establishment id | string | identifier | 11845140 | key |
| clee | clee | Unique establishment code (28-char) | string | identifier | 01009112515000021000000000U8 | stable key |
| nom_estab | nom_estab | Trade name | string | legal_name | ABONO DE LOMBRIZ EL AGUILA | display |
| raz_social | raz_social | Legal name (when applicable) | string | legal_name | AGROPECUARIA CHARCOS DE QUEZADA | blank for many micro-units |
| codigo_act / nombre_act | codigo_act | SCIAN activity | string | activity | 112515 / Piscicultura… | 6-digit code |
| per_ocu | per_ocu | Employee band | string | employment | 0 a 5 personas | band, not exact |
| tipoUniEco | tipoUniEco | Unit type | string | metadata | Fijo | |
| address cols | tipo_vial… nomb_asent… cod_postal | Address | string | address | | concatenate |
| cve_ent/entidad, cve_mun/municipio | … | State / municipality | string | geography | Aguascalientes / Tepezalá | INEGI codes |
| latitud / longitud | latitud/longitud | Geolocation | decimal | geography | 22.195 / -102.257 | |
| fecha_alta | fecha_alta | DENUE listing date | string | date | 2025-05 | NOT incorporation |
| telefono/correoelec/www | … | Contact | string | raw_extension | | phone/email = **PII, redact** |

(Full 42-column layout is in the bundled `diccionario_de_datos`; the modeled
subset is in `source_field_catalog.json`.)

## Interpretation Notes

- **Verified from real data**: Aguascalientes file (`denue_inegi_01_.csv`), 71,871
  establishments, 42 columns. Real record: "AGROPECUARIA CHARCOS DE QUEZADA" (SCIAN
  "Otros servicios relacionados con la agricultura").
- **Establishment-level, not a legal-entity registry**: one row per physical
  establishment. A company with several locations appears multiple times.
- **No RFC, no folio mercantil** — so DENUE cannot be joined to the tax (SAT) or
  commercial (RPC) data by a shared key; only by **name** (`raz_social`/`nom_estab`).
- **Legal vs trade name**: `raz_social` is the corporate name (present for
  companies); `nom_estab` is the trade name (always present).
- **`fecha_alta` is the DENUE listing date**, not the company's incorporation date.
- **Encoding Latin-1 (ISO-8859-1)** — convert to UTF-8. Bulk file per state
  (`denue_{EE}_csv.zip`, EE 01..32; 00 national ~5M units).
- **Personal data**: `telefono` / `correoelec` may be personal data (LFPDPPP) —
  redact; `www` is fine.
