# Company data sources for United Arab Emirates

## Status

- Official bulk data: **not found (open)** — no open bulk register
- Official API: **not open** — registries are login/WAF-gated; data feeds are auth-gated
- Open data portal: `bayanat.ae` / `data.gov.ae` **unreachable** at investigation time
- License: registry data is restricted; exchange listed data is public (browser)
- Recommended ingestion path: **manual / browser per-system** (NER, emirate DEDs,
  DIFC/ADGM registers, DFM/ADX) — no open bulk/API

## Structure (why it is fragmented)

The UAE has **no single national company register**. Companies are registered by:

- **Emirate-level DEDs** — Dubai (Department of Economy & Tourism / Invest in Dubai),
  Abu Dhabi (ADDED / TAMM), Sharjah (SEDD), and the other emirates — each issues
  **trade/commercial licenses**.
- **Financial / specialised free zones** with their **own registrars and public
  registers** — **DIFC** (Dubai International Financial Centre) and **ADGM** (Abu
  Dhabi Global Market) are common-law jurisdictions with public registers; plus DMCC,
  JAFZA, and ~40 other free zones.
- The federal **National Economic Register (NER)** (Ministry of Economy,
  `economy.gov.ae`) is a **unified company-search** layer across emirates and free
  zones.

## Best source

The **National Economic Register (NER)** is the closest to a unified company search,
but it is **login-gated** (`ner.economy.gov.ae` does not resolve; the service sits
under `economy.gov.ae`). The **DIFC** and **ADGM** **public registers** are the most
genuinely public, but their search apps were **WAF/rate-limited** (DIFC 429;
`registration.adgm.com` 403) from this environment. There is **no open bulk register
or open API**.

## Financial data

**DFM** (Dubai Financial Market, `dfm.ae`) and **ADX** (Abu Dhabi Securities Exchange,
`adx.ae`) publish **listed-company** profiles and financial statements. Both are
**public via the browser** but **WAF/auth-gated** for automation (ADX 403; DFM is a
SPA with auth-gated data feeds `connexions.dfm.ae` / `feeds.dfm.ae`). **Private-company
financials** are not openly available (the UAE has no general public-filing
requirement outside listed companies and regulated free zones). Currency **AED**.

## Identifiers & tax

- **Trade / Commercial License number** — issued per **emirate DED** or **free zone**.
- **TRN — Tax Registration Number** — 15-digit, **FTA** (`tax.gov.ae`), for VAT and
  corporate tax.
- **Economic register number** — under the **NER** (national unified).
- **Free-zone registration number** — DIFC / ADGM / DMCC, etc.
- Currency **AED**. Languages: Arabic + English.

## Next action

Use the **NER** (login) for unified company verification, the **emirate DEDs** for
trade-license checks, the **DIFC/ADGM** public registers for free-zone entities, and
**DFM/ADX** (browser) for listed financials. There is **no open bulk register and no
open programmatic financials**. Owners/managers are personal data (PDPL, Federal
Decree-Law 45/2021) — redact if obtained.
