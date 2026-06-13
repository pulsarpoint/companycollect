# Company data sources for Spain

## Status

### Company registry data
- Official bulk data: **partial** — no official bulk *master file*, but the **BORME** (commercial-register
  gazette) is fully open via the **BOE open-data API** (XML/JSON, free) as a daily stream of registered acts.
- Official API: **found** — BOE BORME summary API (`/datosabiertos/api/borme/sumario/{YYYYMMDD}`), no auth.
- Open data portal: **found** — datos.gob.es (national catalog); INE DIRCE (aggregate only).
- Open reconstructed master: **found** — **OpenMercantil** republishes a BORME-derived company DB
  (~2.8M companies) as **CC-BY 4.0** CSV/Parquet + REST API.
- License: **known** — BORME/BOE reusable (open); OpenMercantil CC-BY 4.0; official Registro Mercantil paid.
- Recommended ingestion path: **OpenMercantil bulk/API** (fast open master) and/or **BORME BOE API**
  (authoritative event stream, build-your-own), then **paid registry lookups** for authoritative detail.

### Financial data (annual accounts / cuentas anuales)
- Official bulk data: **not found** (no open bulk of annual accounts)
- Official API: **not found** for non-listed; **found (open)** for listed issuers via **CNMV** (XBRL+PDF)
- Format: **XBRL** (both the Registro Mercantil deposit and CNMV use XBRL — machine-readable)
- Access:
  - **Non-listed companies (the vast majority)** → annual accounts (**Depósito de Cuentas Anuales**) are
    filed at the Registro Mercantil in **XBRL**, retrievable **per company for ~€8.99–€20** via
    registradores.org. **No bulk, no free API.** Retained 6 years.
  - **Listed/issuer companies** → **CNMV** publishes annual (IFA) + intermediate (IFI) reports as
    **open XBRL+PDF** (free) on cnmv.es and datos.gob.es. Small population, fully open.
- Recommended ingestion path: **CNMV open XBRL** for listed; **paid per-company registry XBRL** (cheap)
  or a **commercial aggregator** (eInforma/Informa D&B, Axesor) for the rest.

## Best source

**Company master (open):** **OpenMercantil** — `https://openmercantil.es/` — free CC-BY 4.0, ~2.8M
companies + ~5.8M BORME acts (2009→present), CSV/Parquet bulk + REST API; columns include name, CIF,
province, capital, workers, website, address, act type. Derived from BORME (D+1). **Excludes financial
statements.** Two CC-BY samples **downloaded & verified** into `raw/samples/`.

**Authoritative event stream (open):** **BORME via BOE open-data API** — free, no auth, XML/JSON. A
summary + per-province act XML was **downloaded & verified** into `raw/api/`. Semi-structured Spanish
text (needs parsing — see `bormeparser`).

**Financials:** **CNMV** (open XBRL, listed only) + **Registro Mercantil cuentas anuales** (XBRL,
paid ~€9–20/company, no bulk). No free open bulk of accounts exists for the general company population.

## Next action

1. **Open master**: ingest OpenMercantil bulk CSV/Parquet (full file "próximamente"; samples + per-company
   export + API available now), or build your own master from the BORME BOE API + a parser.
2. **Identifiers**: enrich CIF/NIF coverage (only ~18% of OpenMercantil rows carry a validated CIF).
3. **Financials**: ingest CNMV open XBRL for listed companies; for the rest, decide between paid
   registry XBRL lookups (~€9–20/company) and a commercial aggregator.
4. Confirm BORME/BOE reuse terms and OpenMercantil attribution before redistribution.
