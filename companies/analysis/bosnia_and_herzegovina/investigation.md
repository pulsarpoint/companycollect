# Bosnia and Herzegovina — company data investigation

## Goal

Find official/open sources for **company registry data** and **financial data**
for companies registered in Bosnia and Herzegovina (BiH), download/sample where
allowed, and document a reproducible trail.

## Constitutional structure drives data fragmentation

BiH is highly decentralized. There is **no single national company register**.
Company registration is handled by **entity-level court registers**:

1. **Republika Srpska (RS)** — a unified registration IT system operated with
   **APIF** (Agencija za posredničke, informatičke i finansijske usluge), publicly
   searchable at **`bizreg.esrpska.com`**.
2. **Federation of BiH (FBiH)** — cantonal/municipal court registers, surfaced
   through the central **`bizreg.pravosudje.ba`** portal (HJPC / VSTV), an Oracle
   APEX application ("Registar poslovnih subjekata").
3. **Brčko District** — its own court register, also surfaced via
   `bizreg.pravosudje.ba`.

Indirect taxes (incl. **VAT/PDV**) are state-level under **UINO** (Uprava za
indirektno/neizravno oporezivanje, `uino.gov.ba`).

## What was found

### 1. RS Business Register — `bizreg.esrpska.com` (RECOMMENDED, per company)

- Public search page `/Home/PretragaPoslovnogSubjekta`.
- AJAX endpoint **`POST /Home/SearchPoslovniSubjekt`** (jTable) returns **JSON**.
  A single `term=` parameter searches **Naziv / JIB / MBS**. Verified live:
  - `term=NOVA BANKA` → **"Nova banka" a.d. Banja Luka**, **JIB 4400374890002**,
    address *Ulica kralja Alfonsa XIII 37a, Banja Luka*, activity *64.19 Ostalo
    novčano poslovanje*, status *registrovan*, founder *"MG MIND" d.o.o.*, 68
    business units.
  - `term=ELEKTROPRIVREDA` → RiTE Gacko a.d. (id 10206, **JIB 4401387900003**).
  - `term=TELEKOM` → B2 LINK d.o.o. Banja Luka (id 2064, **JIB 4402978800004**).
- The JSON record exposes: `PrivrednoDrustvoId`, `JIB`, `MBS`, `MB`, `PoslovnoIme`,
  `SkracenoPoslovnoIme`, `Sjediste` (address), `PreteznaDjelatnost` (activity),
  `StatusPoslovniSubjekatOpis`, `Osnivaci` (founders), `OdgovornoLice`,
  `Email`, `Telefon`, `PoslovneJedinice`.
- Per-company **PDF official extract** at `/Home/DetaljiPoslovnogSubjekta/{id}`
  (verified: 275 KB PDF for Nova banka). Sub-lists exist for representatives
  (`ListLicaOvlastenaZaZastupanje…`), branches (`ListPoslovneJedinice`), prokura.
- **No open bulk / no documented bulk export.** Per-company search only.

### 2. FBiH + Brčko — `bizreg.pravosudje.ba` (per company)

- Central court business-register portal "Registar poslovnih subjekata" (Oracle
  APEX, app 183). Public per-company search (Naziv / JIB / MBS). Session-bound
  APEX URLs; no open JSON API or bulk export found. Useful for FBiH/Brčko
  entities not in the RS system.

### 3. Financial statements — APIF RFI (RS) & FIA (FBiH) (paid, per company)

- **APIF — Registar finansijskih izvještaja (RFI)**: all RS legal entities file
  annual statements (**bilans stanja**, **bilans uspjeha**) here. Access to
  reports is **per company for a fee** (naknada). APIF also runs a **Registar
  boniteta** (creditworthiness). Legal basis: *Zakon o jedinstvenom registru
  finansijskih izvještaja RS*.
- **FIA — Financijsko-informatička agencija** (`fia.ba`): the FBiH counterpart —
  register of annual financial statements for FBiH companies, accessed per
  company (paid services / bonitet reports).
- No open bulk financial dataset for either entity. Currency **BAM (KM)**.

### 4. UINO — indirect taxation / VAT (per company)

- `uino.gov.ba` administers the single state-level **VAT (PDV)** and assigns the
  **PDV broj (12-digit)**. Provides per-company taxpayer verification; no open
  bulk taxpayer list found.

### 5. Open-data portal

- **None working.** `data.gov.ba` did not resolve. BiH has no functioning national
  CKAN/open-data portal hosting the company register.

## Conclusion

The richest practical BiH profile combines the **RS register JSON search**
(identity: JIB/MBS, activity, address, status, founders) and the
**FBiH/Brčko APEX portal**, joined on **JIB** (the country-wide 13-digit id),
with **financial statements** added per company from **APIF RFI / FIA** (paid).
There is **no open bulk** and **no national open-data portal**. Founders/owners
and representatives are **personal data** when individuals (BiH Law on Protection
of Personal Data) and must be redacted in committed samples.
