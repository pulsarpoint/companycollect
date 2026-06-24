# Company data sources for Bosnia and Herzegovina

## Status

- Official bulk data: **not found (open)** — company data is split across three
  court registers, all per-company search, no open bulk/API
- Official API: **partial** — the RS register (`bizreg.esrpska.com`) exposes a
  public JSON AJAX search; FBiH/Brčko use an Oracle APEX search portal
- Open data portal: **none** — no working national open-data portal (`data.gov.ba`
  did not resolve)
- License: free per-company access; redistribution terms not stated; financials paid
- Recommended ingestion path: **per-company lookup** (RS JSON search + FBiH APEX) +
  paid per-company financial statements (APIF / FIA)

## Structure (why it is fragmented)

Bosnia and Herzegovina has **no single national company register**. Company
registration is done by the **entity court registers**:

- **Republika Srpska (RS)** — unified IT system run with **APIF**, searchable at
  `bizreg.esrpska.com` (Jedinstveni informacioni sistem za registraciju
  poslovnih subjekata).
- **Federation of BiH (FBiH)** + **Brčko District** — court registers published
  through the **`bizreg.pravosudje.ba`** portal (Oracle APEX, "Registar
  poslovnih subjekata") run by the High Judicial and Prosecutorial Council.

## Best source

**RS Business Register — `bizreg.esrpska.com`** (APIF / RS courts). Its search
endpoint `/Home/SearchPoslovniSubjekt` returns **structured JSON** per query with
**JIB (13-digit), MBS, MB, business name, address, primary activity, status,
founders, contact**, and a per-company **PDF official extract**
(`/Home/DetaljiPoslovnogSubjekta/{id}`). Verified live — e.g. **"Nova banka" a.d.
Banja Luka, JIB 4400374890002**. For **FBiH + Brčko**, use the
`bizreg.pravosudje.ba` APEX search portal.

## Financial data — filed, paid per company

Annual financial statements (**bilans stanja** / balance sheet, **bilans uspjeha**
/ income statement) are filed by all companies and held in registers of financial
statements: **APIF — Registar finansijskih izvještaja (RFI)** for RS, and **FIA —
Financijsko-informatička agencija** for FBiH. Access is **per-company for a fee**
(naknada in KM); there is no open bulk. APIF also publishes a **Registar boniteta**
(creditworthiness register). Currency is **BAM (Konvertibilna marka, KM)**.

## Identifiers & tax

- **JIB** — Jedinstveni identifikacioni broj, **13-digit** unique id = company id
  = tax id (administered via the tax authorities / UINO). RS legal entities start
  `44…`.
- **MBS** — Matični broj subjekta (court registration / registarski uložak).
- **MB** — Matični broj (7-digit statistical number).
- **PDV broj** — **12-digit VAT number**, separate, assigned by **UINO** (Uprava za
  indirektno/neizravno oporezivanje) only for VAT-registered entities. BiH has a
  single state-level VAT (PDV); the PDV broj is distinct from the JIB.

## Next action

Use the RS JSON search (keyed on JIB / name) for RS companies and the FBiH/Brčko
APEX portal for the rest; treat **founders/owners (Osnivači)** as personal data
and redact individuals. Use APIF RFI / FIA for financial statements (paid,
per company). No open bulk register exists.
