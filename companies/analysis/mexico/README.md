# Company data sources for Mexico

## Status

- Official bulk data: **found** (INEGI DENUE — establishment directory; SAT 69-B — tax risk list)
- Official API: **partial** (INEGI DENUE API needs a free token; datos.gob.mx portal revamped)
- Open data portal: **found** (datos.gob.mx) but the legal company register is **not** openly bulk-published
- License: **known** for DENUE/SAT (free-use government data); commercial registry is per-document
- Recommended ingestion path: **bulk** (DENUE per-state CSV) + SAT 69-B CSV; the legal registry (RPC/PSM) is search/per-document only

## Best source

**INEGI DENUE** (Directorio Estadístico Nacional de Unidades Económicas) — the
national statistical directory of **economic units (establishments)**, published
openly by INEGI as **per-state CSV bulk** (no token for the masiva download). Each
record has the trade name (`nom_estab`), **legal/corporate name (`raz_social`)**
when applicable, **SCIAN activity** code + name, employee size band, full address,
municipality/state, and **geolocation**.

Verified live: downloaded `denue_01_csv.zip` (Aguascalientes) — **71,871
establishments**, 42 columns; real records (e.g. "AGROPECUARIA CHARCOS DE QUEZADA",
SCIAN agriculture services).

> DENUE is **establishment-level**, not a legal-entity registry, and contains **no
> RFC and no folio mercantil** — so it cannot be joined to the tax or commercial
> registry by a shared key (only by name). It is, however, the most comprehensive
> open business listing for Mexico (~5M units nationally).

## The legal company registry

Mexico's commercial registry is the **Registro Público de Comercio (RPC)**, run via
**SIGER 2.0** by the Secretaría de Economía, with incorporations published on the
**PSM (Publicaciones de Sociedades Mercantiles)** portal. Companies are identified
by a **folio mercantil electrónico**. Access is **search / per-document** (no open
bulk download or API found). The tax id is the **RFC** (SAT).

## Financial data

- **Private companies: not public.** Mexico does not publish private-company
  financial statements.
- **Listed companies:** financial statements are public via the **BMV (Bolsa
  Mexicana de Valores)** / **CNBV** (EMISNET/SITI). That is the only open financial
  route, and only for issuers.

## Other open source

- **SAT 69-B list** — taxpayers with presumed non-existent operations (shell/EFOS),
  open CSV keyed on **RFC** (14,247 rows). Risk/compliance enrichment.

## Identifiers & tax

- **RFC (Registro Federal de Contribuyentes)** — tax id; **12 chars** for companies
  (13 for individuals). Mexico has **IVA (VAT)** but **no separate VAT number** —
  the RFC is the tax identifier.
- **folio mercantil electrónico** — commercial-registry id (RPC).
- **clee** / DENUE `id` — establishment identifiers (INEGI).

## Next action

Ingest DENUE per-state CSVs (identity-ish: name, activity, size, address, geo) and
the SAT 69-B list (RFC risk). Treat RPC/PSM (legal registry) and BMV/CNBV
(listed financials) as per-document / listed-only. Sample uses real DENUE data;
contact fields redacted.
