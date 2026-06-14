# Poland — Company Data Investigation

Date: 2026-06-14
Investigator: company-open-data-discovery skill (Claude)
Country code: PL | Languages: Polish, English

## Goal

Find official/reliable public sources for Polish company data — prioritising bulk-ingestible open data
and **financial data** — and leave a reproducible trail with samples and licensing.

## Summary of findings

Poland is **one of the most open** countries: the company register, the financial statements, the VAT
bridge, and the beneficial-ownership register are **all free and largely machine-readable**.

- **KRS** (National Court Register) has a **free, no-auth JSON API** with full register data per company.
- **Financial statements** are filed as **structured XML** and are **free to download per company** from
  the **RDF** (Repozytorium Dokumentów Finansowych).
- The **VAT white list** (free API + daily flat file) bridges **NIP ↔ REGON ↔ KRS** and adds bank
  accounts + VAT status.
- **CRBR** publishes beneficial ownership for free.

### 1. KRS API — RECOMMENDED (open registry spine)  ✅ verified live
- Publisher: **Ministerstwo Sprawiedliwości** (Ministry of Justice). Official.
- Endpoints: `https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/{krs}?rejestr={P|S}&format=json` (current
  extract) and **OdpisPelny** (full/historical). Free, **no auth**; personal data anonymized.
- Registers: **P** = przedsiębiorcy (entrepreneurs/companies: sp. z o.o., S.A., …), **S** = stowarzyszenia
  (associations/foundations/NGOs). Covers KRS-registered entities — **not** sole proprietors (see CEIDG).
- Data (verified, PKO BP KRS 0000026438): nazwa, **identyfikatory {KRS, NIP, REGON}**, forma prawna,
  siedziba + adres + **website**, kapitał, **PKD** activity codes, organy/wspólnicy (board/partners),
  rok obrotowy, **wzmianki o złożonych dokumentach** (which financial statements were filed),
  likwidacja/upadłość (liquidation/bankruptcy). Rich JSON across dział 1–6.
- Listed on **dane.gov.pl**; also the new Portal Rejestrów Sądowych (`prs.ms.gov.pl/krs/openApi`).
- No single trivial "download all" file — enumerate KRS numbers or seed from the white list.

### 2. RDF — Financial statements — RECOMMENDED (open, structured)
- **Repozytorium Dokumentów Finansowych** (`ekrs.ms.gov.pl/rdf/pd/search_df`): **free** search + download
  of financial statements per company (enter KRS → list of documents → download **XML + PDF**).
- XML = structured **e-Sprawozdania finansowe** in the **Ministry of Finance logical schema**: **bilans**
  (balance sheet), **rachunek zysków i strat** (income statement), **informacja dodatkowa** (notes); some
  **XBRL** for listed/consolidated. One XML file contains the whole statement.
- For mass/automated monitoring there is a **PRS-eKRS API** (registration). The per-company **free
  download is open** and the linkage comes from KRS dział 3 "wzmianki o złożonych dokumentach".
- This makes Poland's financials **open and machine-readable** — a major advantage over DE/ES/IT.

### 3. Biała lista podatników VAT — RECOMMENDED (open VAT bridge)  ✅ verified live
- Publisher: **Ministerstwo Finansów / KAS**. Free API `https://wl-api.mf.gov.pl/api/search/nip/{nip}?date=YYYY-MM-DD`
  + a **daily flat file** of all NIP-account pairs.
- Data (verified, NIP 5250007738): name, **nip, regon, krs**, statusVat (Czynny/Zwolniony),
  residence/working address, **accountNumbers** (bank accounts), representatives, partners, registration
  dates. The cleanest way to **bridge NIP ↔ REGON ↔ KRS** and to seed a full active-taxpayer list.

### 4. CEIDG — sole proprietors (open)
- Centralna Ewidencja i Informacja o Działalności Gospodarczej — register of **individual entrepreneurs**
  (jednoosobowa działalność), separate from KRS. Public API. Needed for full company coverage (sole traders).

### 5. REGON / GUS BIR1 — statistical register (open w/ free key)
- GUS (statistics office) BIR1 API — covers **all** REGON-registered entities (incl. sole traders), keyed
  on REGON; returns basic identity + PKD + status. Requires a **free API key**. Good completeness layer.

### 6. CRBR — beneficial ownership (open)
- Centralny Rejestr Beneficjentów Rzeczywistych (`crbr.podatki.gov.pl`) — **free public** beneficial
  ownership (beneficjenci rzeczywiści). Unusually open vs many EU peers.

### 7. dane.gov.pl + secondary
- National open-data portal (DCAT). Aggregators (Rejestr.io, MGBI, Bisnode) mostly **resell the open
  KRS/RDF data** with convenience APIs — optional, not required given the open sources.

## Conclusion

- **Spine**: KRS API (free, rich JSON) + VAT white list (bridge + bank accounts) + CEIDG/REGON for full
  coverage incl. sole traders.
- **Financials**: free **structured XML** statements from RDF (per company), triggered off KRS filing
  mentions. Parse the MF e-Sprawozdania schema.
- **Ownership**: CRBR (free). Poland is effectively a fully open company-data jurisdiction.

## Risks / open questions

- **No single full bulk** — enumerate KRS numbers / use the white-list flat file as a NIP seed; or the
  PRS-eKRS mass API (registration).
- **Two registers + two systems**: KRS (companies) vs CEIDG (sole traders) — must combine for full coverage.
- **REGON length**: KRS returns 14-digit REGON, white list 9-digit — normalize (9-digit core + 5-digit unit).
- **Financial XML schema** evolves yearly (MF publishes versioned schemas) — parser must handle versions;
  some entities file PDF-only or XBRL (listed) — handle multiple formats.
- **License**: open/free reuse, but record attribution per source (KRS/MS, MF, GUS).
