# Croatia — Company Data Investigation

Date: 2026-06-14
Investigator: company-open-data-discovery skill (Claude)
Country code: HR | Languages: Croatian, English

## Goal

Find official/reliable public sources for Croatian company data — prioritising bulk-ingestible open data and
**financial data** (RGFI) — and leave a reproducible trail with samples and licensing.

## Summary of findings

Croatia is **open** (Belgium-tier, free but behind a free registration): the company register **and** the
annual accounts are **open-licensed and machine-readable**, both via a **free registration/account**.

- **Sudski registar** — an **open REST API** (JSON) for the company spine, under the **Croatian Open Licence
  (Otvorena dozvola)**.
- **FINA RGFI** — annual financial statements as **open, machine-readable CSV** (balance sheet + income
  statement), also under the Open Licence.
- Everything joins on the **OIB** (11-digit tax id = VAT root) and/or **MBS** (court-register number).

### 1. Sudski registar (Court Register) — RECOMMENDED (open company spine)
- Publisher: **Ministarstvo pravosuđa** (Ministry of Justice). Official; authoritative company register.
- Access:
  - **Free public web search** — `https://sudreg.pravosudje.hr`.
  - **Open Data REST API** — register at **`https://sudreg-data.gov.hr`** (test: sudreg-data-test.gov.hr).
    **Free registration** → **Client ID + Client Secret + token + `Ocp-Apim-Subscription-Key`** (Oracle APIM
    gateway). JSON. OpenAPI docs at `sudreg-podaci.pravosudje.hr/docs/services`.
  - Query by **`tipIdentifikatora`** (`oib` | `mbs`) + **`identifikator`**; endpoints like `subjekt_Get` (all
    with paging) and `subjekt_GetSubjectJavni` (subject details).
- Fields: nadležni sud (court), **MBS**, **OIB**, status, naziv/tvrtka (name), sjedište + adresa (seat +
  address), **temeljni kapital** (share capital), **pravni oblik** (legal form), plus details (predmet
  poslovanja / activity, osobe / persons, …).
- License: **Otvorena dozvola (OD)** (confirmed via the data.gov.hr CKAN dataset "sudski-registar").

### 2. FINA RGFI — Annual Financial Statements — RECOMMENDED (open structured financials)
- Publisher: **FINA** (Financijska agencija) — **Registar godišnjih financijskih izvještaja (RGFI)**.
  Companies (corporate-income-tax payers) file annual accounts to FINA.
- Access:
  - **Javna objava (public disclosure)** — `http://rgfi.fina.hr/JavnaObjava-web` — after **free
    registration/login**, the annual financial statements + documentation are available **free of charge**.
  - **Machine-readable open CSV** — "data from standard documentation (Balance sheet and Income statement)
    intended for reuse are available in machine-readable and open format" — especially for **micro and small
    enterprises**. Published as a **data.gov.hr CKAN dataset** ("registar-godisnjih-financijskih-izvjestaja-
    javna-objava") under the **Otvorena dozvola**.
  - FINA also sells **fuller RGFI products** (cjenik / price list) — some paid.
- Content: **balance sheet (bilanca)** + **income statement (račun dobiti i gubitka)** (abbreviated form) +
  notes (bilješke), per fiscal year. Structured CSV.

### 3. data.gov.hr — national open data portal (CKAN)
- Hosts both the **Sudski registar** and the **RGFI javna objava** datasets under the **Otvorena dozvola**.
  CKAN API available (package_show confirmed both, license "open-license Otvorena dozvola"). The actual
  resource URLs point to the registration-gated portals (sudreg-data.gov.hr; rgfi.fina.hr login).

### 4. Registar stvarnih vlasnika (RSV) — beneficial ownership (restricted)
- Beneficial-ownership register, run by FINA. **Access conditions apply** (legitimate interest post-CJEU).
  Not open bulk. Out of scope.

### 5. Other official
- **DZS** (Državni zavod za statistiku) — statistical business register (aggregate).
- **Porezna uprava** — VAT (PDV) / OIB; VAT = `HR` + OIB; VIES validation.
- Commercial aggregators (Poslovna Hrvatska / Bisnode, Companywall, …) — paid enrichment.

## Conclusion

- **Spine**: Sudski registar API (open JSON, free key).
- **Financials**: FINA RGFI javna objava (open structured CSV: balance sheet + income statement, free login).
- **Join**: single **OIB** (= VAT root) and/or **MBS**. Croatia is effectively a fully open company-data
  jurisdiction, with structured financials — both under the Otvorena dozvola, behind a free registration.

## Risks / open questions

- **Both core sources need a free account/registration** — the sudreg API needs a **subscription key**; the
  RGFI CSV needs a **FINA login**. Not anonymous; provision credentials.
- **RGFI open-CSV coverage** — explicitly noted for **micro/small**; confirm whether all sizes are in the
  open public-disclosure set or whether fuller/large-company data needs the **paid FINA product**.
- **License**: Otvorena dozvola (attribution) — confirm before redistribution.
- **Identifiers**: OIB (11 digits, VAT root); MBS (court register); MB (old statistical) — reconcile.
- Could not download a per-company sample here (sudreg API key; FINA login; CKAN resources point to the gated
  portals) — documented; API + CSV structures are documented (OpenAPI; RGFI standard forms).
