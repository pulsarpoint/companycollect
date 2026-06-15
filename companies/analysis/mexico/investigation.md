# Mexico Company Data Investigation

## Conclusion

Mexico's company data is **fragmented** across several authorities, with **no
single open legal-entity register** and **no public financials for private
companies**:

- **Best open bulk:** **INEGI DENUE** — the national statistical directory of
  **economic units (establishments)**, openly downloadable as **per-state CSV**
  (no token). Rich: trade name, **legal name (`raz_social`)**, **SCIAN activity**,
  employee band, full address, municipality/state, geolocation. But it is
  establishment-level and carries **no RFC and no folio mercantil**.
- **Legal registry (not open bulk):** **Registro Público de Comercio (RPC)** via
  **SIGER 2.0** (Secretaría de Economía), with incorporations on the **PSM**
  portal. Companies keyed by **folio mercantil electrónico**; access is
  search/per-document, certified extracts fee-based.
- **Tax id:** **RFC** (SAT). The **SAT 69-B** list (shell-company/EFOS risk) is an
  open CSV keyed on RFC.
- **Financials:** private — not public; listed — via **BMV/CNBV** (issuers only).

## What was verified live

- **DENUE bulk works**: `denue_01_csv.zip` (Aguascalientes) downloaded — 6.8 MB →
  **71,871 establishments**, 42 columns. Real record: "AGROPECUARIA CHARCOS DE
  QUEZADA", SCIAN "Otros servicios relacionados con la agricultura", Aguascalientes.
- **SAT 69-B CSV**: downloaded — **14,247 taxpayers** with RFC, legal name, and
  situation (Presunto / Definitivo / Desvirtuado / Sentencia Favorable).
- **PSM** portal reachable (publications of mercantile companies); no bulk/search-
  API exposed. **RPC**, **BMV** reachable.
- **INEGI DENUE query API** requires a **free token** (the masiva CSV does not).
- **datos.gob.mx** legacy CKAN `busca/api` no longer returns JSON (portal revamp);
  the legal register is not openly hosted there.

## Identifiers

- **RFC (Registro Federal de Contribuyentes)** — the tax id. **12 characters** for
  legal entities (13 for individuals). Mexico has **IVA (VAT)** but **no separate
  VAT number** — the RFC is the tax identifier. (RFC for individuals is personal
  data; for companies it is a corporate id.)
- **folio mercantil electrónico** — the RPC commercial-registry id.
- **clee** (Clave Única del Establecimiento, 28-char) and DENUE `id` — INEGI
  establishment identifiers.

> **No shared open key.** DENUE has no RFC/folio; the legal registry and SAT use
> RFC/folio. So joining DENUE ↔ SAT ↔ RPC openly is only by **name** (fuzzy). RFC
> is the closest to a universal company id but is not published in DENUE.

## DENUE field schema (42 columns, verified)

`id`, `clee`, `nom_estab` (trade name), `raz_social` (legal name, when
applicable), `codigo_act` + `nombre_act` (SCIAN), `per_ocu` (employee band),
address (`tipo_vial`, `nom_vial`, `numero_ext`, `tipo_asent`, `nomb_asent`,
`cod_postal`, …), `cve_ent`/`entidad`, `cve_mun`/`municipio`, `cve_loc`/`localidad`,
`ageb`, `manzana`, `telefono`, `correoelec`, `www`, `tipoUniEco` (unit type),
`latitud`, `longitud`, `fecha_alta`.

## SAT 69-B field schema (verified)

`No`, `RFC`, `Nombre del Contribuyente`, `Situación del contribuyente`, oficio
number/date of presunción, DOF publication references. CSV with 3 preamble lines
before the header.

## What is NOT openly available

- A **single open legal-entity register** with the folio mercantil + RFC.
- **Private-company financial statements** (only listed issuers via BMV/CNBV).
- **RFC / folio mercantil inside DENUE** (so no clean cross-source join key).
- **Directors/officers** — in notarial/registry documents (per-document, fee-based).

## Recommended ingestion

1. **DENUE per-state CSVs** (denue_01..32) — the open business listing layer
   (name, legal name, activity, size, address, geo). No token.
2. **SAT 69-B CSV** — RFC risk overlay (shell-company list).
3. Treat **RPC/PSM** (legal identity, folio mercantil) and **BMV/CNBV** (listed
   financials) as per-document / listed-only enrichments.
4. Redact contact fields (telefono/correoelec) and individual RFCs (personal data,
   LFPDPPP).
