# SAT Listado 69-B Field Catalog

## Source Summary

- Country: Mexico
- Source type: tax_risk_list
- Organization: Servicio de Administración Tributaria (SAT)
- URL: http://omawww.sat.gob.mx/cifras_sat/Documents/Listado_Completo_69-B.csv
- License: public by statute (art. 69-B CFF)
- Access: public
- Freshness: periodically updated
- Record shape: flat CSV, **3 preamble lines** then the header row
- Primary keys: `RFC`
- Join keys: `RFC`, `Nombre del Contribuyente`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| No | No | Row number | integer | metadata | 1 | not a stable id |
| RFC | RFC | Taxpayer RFC (12-char companies) | string | identifier | AAA080808HL8 | join key; tax id |
| Nombre del Contribuyente | … | Legal name | string | legal_name | INGENIOS SANTOS, S.A. DE C.V. | incl. form suffix |
| Situación del contribuyente | … | 69-B status | string | status | Presunto / Definitivo / Desvirtuado / Sentencia Favorable | risk level |
| oficio + fecha | … | SAT oficio refs | string | document | 500-05-2018-16632 de fecha 01 de junio de 2018 | audit trail |
| Publicación DOF | … | Gazette refs | string | document | | |

## Interpretation Notes

- **Verified from real data**: 14,247 taxpayers. The CSV has **3 preamble lines**
  (disclaimer + title) before the `No,RFC,Nombre del Contribuyente,Situación…`
  header — skip them when parsing. Encoding **Latin-1**.
- This is a **risk list** (art. 69-B CFF — taxpayers that issued invoices for
  presumed **non-existent operations**, i.e. likely shell companies / EFOS), **not
  a company master**. Use it as a compliance/risk overlay.
- **`Situación`**: `Definitivo` = confirmed (highest risk); `Presunto` = presumed;
  `Desvirtuado` / `Sentencia Favorable` = the taxpayer cleared the presumption.
- **Join**: on **RFC** (12-char for companies) and/or legal name. RFC links to the
  tax identity; it is **not present in DENUE**, so a DENUE↔SAT join is name-based.
- The legal name carries the corporate form suffix (S.A. de C.V., S. de R.L. de
  C.V., S.C., …), useful for inferring `legal_form`.
- Individual RFCs/names would be personal data (LFPDPPP) — handle responsibly.
