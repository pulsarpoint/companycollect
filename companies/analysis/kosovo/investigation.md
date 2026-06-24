# Kosovo — company data investigation

## Goal

Find official/open sources for **company registry data** and **financial data**
for companies registered in Kosovo, download/sample where allowed, and document a
reproducible trail. Do not bypass access controls.

## What was found

### 1. ARBK — Kosovo Business Registration Agency (official registry, GATED)

- **`arbk.rks-gov.net`** — the official company register (Agjencia për
  Regjistrimin e Bizneseve të Kosovës), under the Ministry of Industry,
  Entrepreneurship and Trade (MINT). The site is a **React SPA** backed by an API
  at base path **`/api/api/`**.
- Endpoints referenced by the app include:
  - `Services/KerkoBiznesin` — **search businesses** (body carries `Gjuha`,
    `gjuhaId`, and a **token**).
  - `Services/TeDhenatBiznesit` — **business details**.
  - `Services/EksportoBizneset` — **export businesses** (bulk-like).
  - Reference/stat endpoints: `GetStatusiBiznesit`, `LlojetEBiznesit`,
    `ListaKomunave`, `GetSektori`, `GetListaAktivitetet`, `ListaShteteve`,
    `GetPronesia`, `GetPerqindjaPronariHuaj`, `StatLlojiBiznesve`,
    `StatSektoriBiznesve`, `GetNumriNjesiveRegjistruar`.
- **Access is gated two ways** (verified live):
  - Every `Services/*` endpoint returns **HTTP 401** (`application/problem+json`,
    "Unauthorized") without the SPA's **bearer token**.
  - The search uses **Cloudflare Turnstile** (`challenges.cloudflare.com/turnstile`)
    — `Services/KerkoBiznesin` requires the CAPTCHA `token`; POST without it → 401.
- **No open bulk** and no open API key. The export endpoint is auth-gated. The
  register is usable **via the browser only**. Controls were **not bypassed**.
- The SPA's field model (from its JS/i18n) documents the business record:
  **Numri Unik Identifikues (NUI)**, **Numri i Biznesit (NRB)**, **Numri Fiskal**,
  **Numri i TVSH** (VAT), Emri (name), Statusi, Data e Regjistrimit, Komuna,
  Adresa, Aktiviteti Kryesor + Aktivitetet, Lloji i Biznesit, Kapitali (+ %),
  Pronari/Pronarët (+ ownership %, foreign-owner %), Sektori, Numri i Punëtorëve,
  Numri i Njësive. Tri-lingual (Albanian / Serbian / English).

### 2. ATK — Tax Administration of Kosovo (per-company VAT/fiscal, GATED) + Open Data (aggregate)

- **`atk-ks.org`** (Administrata Tatimore e Kosovës). Provides:
  - **VatRegist app** (`apps.atk-ks.org/BizPasiveApp/VatRegist/Index`) — per-company
    lookup by **Emri / Nr. Fiskal / NRB / Nr. TVSh** posting to
    `VatRegist/SearchTaxPayer`. Output fields confirm a rich record: `FiscalNo`,
    `NrbID`, `TpStatus`, `TpName`, `Address`, `CityName`, `ParishName`,
    `TaxCentreName`, `VatNo`, `VatTypeAl`. **CAPTCHA-gated** — the JSON response is
    `{"captchaData":{"ErrorMessage":"Kliko 'I'm not a robot'"}}`. Not bypassed.
  - **Open Data** (`atk-ks.org/open-data/`) — real downloadable **XLSX**, but
    **aggregate statistics**: e.g. `Nr_punto_ID-YYYY.xlsx` (employer/employee counts
    by sector, municipality, subject type, year/month — columns TPER_YEAR,
    PERSHKRIMI_SEKTORIT, KOMUNA, TIPI_SUBJEKTIT, NR_PUNDHENSVE, NR_PUNTORVE),
    `Deklarimi-YYYY.xlsx` (declarations by tax type / sector / municipality).
    **No fiscal number or business name** — not company-level.
  - Per-company **fiscal-number** verification and **inactive-taxpayer** pages.

### 3. National open-data portal — unavailable

- `opendata.rks-gov.net`, `data.rks-gov.net`, `bizneset.rks-gov.net` did **not
  resolve**. No working national portal hosting the company register openly.

### 4. Financial data — not open

- Kosovo has **no public register of company financial statements** (no annual
  accounts filing portal for private companies). The only financial datapoints are
  ARBK's registered **capital** and ownership percentages. ATK Open Data is
  aggregate. So per-company financials are **not available openly**.

## Conclusion

Kosovo's official register (**ARBK**) is comprehensive but **CAPTCHA + bearer
gated**, with no open bulk/API and an auth-gated export. The tax authority's
per-company lookup (**ATK VatRegist**) is also **CAPTCHA-gated**; ATK Open Data is
**aggregate** only. There is **no open company-level bulk** and **no open
financials**. The realistic ingestion path is **manual/browser per-company
lookup**. The data model is documented from ARBK's own field model and the ATK
VatRegist output fields. Owners (Pronarët) are personal data and must be redacted.
No access controls were bypassed; no real per-company values were extracted.
