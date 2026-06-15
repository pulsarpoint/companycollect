# Registro Público de Comercio (RPC/SIGER) & PSM Field Catalog

> **PLANNING-ONLY.** The legal commercial registry (RPC via SIGER 2.0, Secretaría
> de Economía) with incorporations published on the PSM portal. Access is
> **search / per-document**; certified registry extracts are **fee-based**. No open
> bulk/API found. Cataloged from public documentation only — no records fetched.

## Source Summary

- Country: Mexico
- Source type: official_registry
- Organization: Secretaría de Economía
- URL: https://psm.economia.gob.mx/PSM/ (publications); RPC/SIGER (registry)
- License: restricted
- Access: public search / per-document (fee-based extracts)
- Freshness: live register
- Record shape: per-company registry entry / publication
- Primary keys: `folio_mercantil_electronico`
- Join keys: `folio_mercantil_electronico`, `rfc`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| registry.folio_mercantil_electronico | Folio Mercantil Electrónico | Registry id | string | identifier | authoritative legal id |
| registry.denominacion | Denominación / razón social | Legal name | string | legal_name | |
| registry.tipo_societario | Tipo societario | Legal form | string | legal_form | S.A. de C.V. / S. de R.L. / S.A.P.I. / S.C. |
| registry.fecha_constitucion | Fecha de constitución | Incorporation date | date | date | authoritative |
| registry.domicilio | Domicilio social | Registered address | string | address | |
| registry.capital_social | Capital social | Share capital (MXN) | decimal | financial | |
| registry.objeto_social | Objeto social | Corporate purpose | string | activity | |

## Interpretation Notes

- The **folio mercantil electrónico** is the authoritative legal-entity registry id
  — the closest Mexican equivalent to a company registration number. It is **not in
  DENUE**, so linking DENUE establishments to the legal entity requires this
  registry (or name matching).
- This registry holds the authoritative **legal form, incorporation date, capital,
  and objeto social** — none openly bulk-available.
- **Access**: PSM publishes incorporation notices; RPC/SIGER issues certified
  extracts (boletas registrales) for a fee. No open bulk or API.
- **No raw sample record** (per-document / fee-based source).
