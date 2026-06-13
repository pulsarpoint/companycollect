# CNMV — Listed-Company Financial Reports (XBRL) — Field Catalog

> **OPEN** financial source, but **listed/issuer companies only** (hundreds of entities). Cataloged from
> CNMV documentation and the Spanish PGC/CNMV XBRL taxonomy; no values copied. The open complement to the
> paid `registro_mercantil_cuentas_anuales` (which covers the general population).

## Source Summary

- Country: Spain
- Source type: official_financial_disclosure
- Organization: Comisión Nacional del Mercado de Valores (CNMV)
- URL: https://www.cnmv.es/ipps/ (XBRL viewer/download); CNMV datasets on https://datos.gob.es/
- License: open (CNMV public information; confirm attribution for redistribution)
- Access: public, no auth, free
- Freshness: annual (IFA) + intermediate (IFI/IPP)
- Record shape: per-issuer **XBRL** report (+ PDF), CNMV/IPP taxonomy
- Primary keys: issuer NIF + period + report_type
- Join keys: NIF/CIF; ISIN/LEI

## Fields

| Path | Source field (ES) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| issuer.nif | NIF | Issuer tax id | string | identifier | join to spine |
| report.tipo | tipo informe | IFA / IFI | string | filing | prefer IFA |
| report.ejercicio | ejercicio | Period | string | date | one per period |
| balance.activo_total | Total activo | Total assets | decimal | financial | |
| balance.activo_no_corriente | Activo no corriente | Non-current assets | decimal | financial | |
| balance.activo_corriente | Activo corriente | Current assets | decimal | financial | |
| balance.patrimonio_neto | Patrimonio neto | Equity | decimal | financial | |
| balance.pasivo | Pasivo | Liabilities | decimal | financial | |
| resultados.cifra_negocios | Importe neto de la cifra de negocios | Revenue | decimal | financial | primary revenue |
| resultados.resultado_explotacion | Resultado de explotación | Operating result | decimal | financial | |
| resultados.resultado_ejercicio | Resultado del ejercicio | Net result | decimal | financial | |
| report.documento | documento XBRL/PDF | Report files | string | document | XBRL parseable |

## Interpretation Notes

- **Scope is small but fully open.** Only entities with securities admitted to trading on Spanish
  regulated markets file here (IFA annual + IFI intermediate). For the ~3.3M general company population,
  use the Registro Mercantil deposits (paid) instead.
- **XBRL since 2005** under CNMV Circulars; parse with an XBRL toolkit (Arelle). Figures are typically
  **consolidated** for groups; capture individual vs consolidated where both exist.
- **Join** to the company spine via **NIF/CIF**; issuers also carry ISIN/LEI which can disambiguate.
- Also mirrored as datasets on **datos.gob.es** (e.g. "Información financiera intermedia de entidades
  emisoras registrada en la CNMV").
- No `sample_record.json` retrieved this run (follow-up: pull one IFA XBRL from cnmv.es/ipps).
