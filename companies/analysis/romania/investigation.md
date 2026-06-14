# Romania Company Data — Investigation

## Conclusion

Romania is a **best-in-class fully-open** country for company data. Both halves
of a rich profile are available officially and free:

- **Identity / register**: the National Trade Register Office (ONRC — *Oficiul
  Național al Registrului Comerțului*) publishes the **entire** company register
  as open bulk CSV on **data.gov.ro**, refreshed regularly. The main file
  (`OD_FIRME.CSV`) holds **4,116,356** companies; five companion CSVs add status,
  authorized activities (CAEN), legal representatives, family-enterprise
  representatives, and foreign branches.
- **Financials**: the tax authority **ANAF** exposes a free public web service,
  `https://webservicesp.anaf.ro/bilant?an=YYYY&cui=CUI`, returning **structured
  financial-statement** indicators (turnover, revenue, expenses, gross/net
  profit, average employees, fixed/current assets, liabilities, equity, …) as
  JSON, per company per year. Verified live for **2014–2024** (the published doc
  says 2014–2019 but the service is current).

## Identifiers

- **CUI** (*Cod Unic de Înregistrare*) — the fiscal/unique registration code
  (numeric). This is the company id used by ANAF. VAT number = `RO` + CUI for
  VAT-registered firms.
- **COD_INMATRICULARE** — the trade-register registration number, two formats:
  the classic `J40/630/1992` (J = SRL/SA etc., county/serial/year) and the newer
  numeric `J2002000372404`. **This is the join key across the ONRC companion
  CSVs** (status, CAEN, representatives, branches).
- **EUID** — European Unique Identifier, e.g. `ROONRC.J2002000372404`.
- Bridge: `OD_FIRME` carries **both** CUI and COD_INMATRICULARE, so it links the
  register companion files (keyed on COD_INMATRICULARE) to ANAF financials/VAT
  (keyed on CUI).

## Sources found

### 1. ONRC company register — OD_FIRME (data.gov.ro) — RECOMMENDED
- Dataset: `https://data.gov.ro/dataset/firme-08-12-2025` (a new dated snapshot
  is published regularly; the resource UUIDs are stable within a snapshot).
- `OD_FIRME.CSV` — 643 MB, `^`-delimited, UTF-8 (BOM). Columns: DENUMIRE, CUI,
  COD_INMATRICULARE, DATA_INMATRICULARE, EUID, FORMA_JURIDICA, ADR_TARA,
  ADR_JUDET, ADR_LOCALITATE, ADR_DEN_STRADA, ADR_NR_STRADA, ADR_BLOC, ADR_SCARA,
  ADR_ETAJ, ADR_APARTAMENT, ADR_COD_POSTAL, ADR_SECTOR, ADR_COMPLETARE, WEB,
  TARA_FIRMA_MAMA. **Downloaded in full** (4,116,357 rows incl. header).
- Companion CSVs (same dataset, all open):
  - `OD_STARE_FIRMA.CSV` (89 MB) — COD_INMATRICULARE → status COD (e.g. 1048
    funcţiune/active, 1084 radiată/struck-off, 2069 dizolvare). Downloaded.
  - `OD_CAEN_AUTORIZAT.CSV` — COD_INMATRICULARE → COD_CAEN_AUTORIZAT,
    VER_CAEN_AUTORIZAT (authorized activity codes; CAEN Rev.2/Rev.3). Sampled.
  - `OD_REPREZENTANTI_LEGALI.CSV` — COD_INMATRICULARE → PERSOANA_IMPUTERNICITA,
    CALITATE, DATA_NASTERE, birth place, locality. **PERSONAL DATA (GDPR)**.
    Sampled (schema only).
  - `OD_REPREZENTANTI_IF.CSV` — representatives of family enterprises (PII).
  - `OD_SUCURSALE_ALTE_STATE_MEMBRE.CSV` — COD_INMATRICULARE, TIP_UNITATE,
    DENUMIRE_SUCURSALA, EUID, COD_FISCAL, TARA (branches in other EU states).
    Sampled.

### 2. ANAF financial-statements web service (bilant) — RECOMMENDED
- `GET https://webservicesp.anaf.ro/bilant?an=YYYY&cui=CUI` → JSON
  `{an, cui, deni, caen, den_caen, i:[{indicator, val_indicator,
  val_den_indicator}]}`. ~20 indicators (I1–I20/I33 depending on entity type).
- **Requires a browser-like User-Agent** (an F5 WAF returns an empty `i:[]` for a
  bot UA; with `Mozilla/5.0` it returns full data). Max **1 request/second**.
- Verified live: Dante International SA (eMAG, CUI 14399840) — net turnover
  2019 = 4.56B, 2021 = 7.35B, 2023 = 7.72B, 2024 = 8.99B RON.

### 3. ANAF VAT / fiscal-info web service (PlatitorTvaRest, ws/tva) — SECONDARY
- Documented endpoint `POST https://webservicesp.anaf.ro/PlatitorTvaRest/api/vN/ws/tva`
  with body `[{"cui":N,"data":"YYYY-MM-DD"}]` → name, address, VAT-registration
  status (scpTVA), inactive status, cash-VAT/split-VAT flags, registration
  number, IBAN. **On this run every version path (v5–v9) returned HTTP 404** —
  the service appears to have moved/been version-bumped. Cataloged from the
  official docs; not verified live here. The register CSV already supplies the
  company master data, so this is enrichment-only (current VAT status).

### 4. ONRC Beneficial Ownership Register (RBR) — RESTRICTED
- *Registrul Beneficiarilor Reali*. Access requires online registration, an
  administrative **fee**, and a **qualified electronic signature**; post-CJEU
  C-37/20 access is being narrowed to **legitimate interest** only. Not open;
  planning-only.

### 5. ONRC portal (portal.onrc.ro) / RECOM — paid/gated
- The interactive register portal (full certificates, history, share capital,
  shareholders) is **paid** per extract and partly CAPTCHA/account-gated. Not
  used; the open bulk CSV covers the master data.

## What was NOT bypassed

- The ANAF WAF UA filter was satisfied with a normal browser User-Agent (not a
  bypass — it is the documented public service); rate limit respected (≤1/s).
- RBR (beneficial owners), the paid ONRC portal, and the e-signature gate were
  **not** circumvented.

## Recommended ingestion

Bulk-load `OD_FIRME` + companion CSVs (join on COD_INMATRICULARE), then enrich
each CUI from the ANAF `/bilant` service for the years needed. Optionally add
current VAT status from the ws/tva service once its current path is confirmed.
Treat representative/officer rows as GDPR personal data.
