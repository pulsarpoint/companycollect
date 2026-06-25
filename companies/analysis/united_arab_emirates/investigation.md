# United Arab Emirates — company data investigation

## Goal

Find official/open sources for **company registry data** and **financial data** for
companies registered in the UAE, download/sample where allowed, and document a
reproducible trail. Do not bypass access controls.

## Structural context

The UAE has **no single national company register**. Registration is split across:

1. **Emirate-level DEDs** — Dubai (Department of Economy & Tourism / Invest in Dubai),
   Abu Dhabi (ADDED / TAMM), Sharjah (SEDD), Ajman, RAK, Fujairah, Umm Al Quwain —
   each issues **trade/commercial licenses** to mainland companies.
2. **Free zones with their own registrars** — **DIFC** and **ADGM** (common-law
   financial centres with **public registers**), plus DMCC, JAFZA, and ~40 others.
3. The federal **National Economic Register (NER)** (Ministry of Economy) — a
   **unified company-search** layer across emirates and free zones.

## What was found

### 1. National Economic Register (NER) — unified search (login-gated)

- The **NER** sits under the **Ministry of Economy** (`economy.gov.ae`, which
  resolves to `86.96.x.x`). The dedicated host `ner.economy.gov.ae` **does not
  resolve (NXDOMAIN)**; the unified company search is a **login-gated** e-service. No
  open bulk/API. The Ministry site (`moec.gov.ae` → `moet.gov.ae`) is a Liferay portal
  with no open company dataset.

### 2. Emirate DEDs — trade-license issuance/verification (gated)

- **Dubai** — Invest in Dubai (`invest.dubai.ae`) returned **HTTP 403 (WAF)**. **Abu
  Dhabi** — ADDED via TAMM. Each provides **trade-license verification** per emirate,
  **not an open bulk register**.

### 3. Free-zone public registers — DIFC & ADGM (WAF/rate-limited)

- **DIFC Public Register** (`difc.ae/business/public-register`) — public register of
  DIFC entities; returned **HTTP 429** (rate-limited/WAF). `portal.difc.ae` is a login
  portal.
- **ADGM Public Register** (`adgm.com/public-registers`) — the page loads (HTTP 200),
  but the actual register **search app** (`registration.adgm.com`) returned **HTTP
  403 (WAF)**, and no downloadable register file was found. ADGM is a common-law
  jurisdiction with a public register of entities.

### 4. Exchanges — DFM & ADX (listed financials; WAF/auth-gated)

- **DFM** (`dfm.ae`) — loads (SPA); its market-data feeds (`connexions.dfm.ae/ext/p/
  arena/api/v2`, `feeds.dfm.ae`) are **auth-gated**. **ADX** (`adx.ae`) — **HTTP 403
  (WAF)**. Both publish **listed-company** profiles and financial statements (public
  via the browser) — e.g. **Emaar Properties PJSC**, **Emirates NBD Bank PJSC** (DFM),
  **First Abu Dhabi Bank PJSC** (ADX). Listed companies only.

### 5. Tax — FTA

- The **FTA** (`tax.gov.ae`) issues the **TRN** (Tax Registration Number, 15-digit)
  for VAT and corporate tax, with a TRN-verification tool. Per-company; not open bulk.

### 6. Open-data portals — unreachable

- **`bayanat.ae`** and **`data.gov.ae`** did **not resolve/respond** at investigation
  time. No company-register dataset could be confirmed.

## Conclusion

The UAE has **no open company register and no open programmatic financials**.
Registration is **fragmented** across emirate DEDs, free-zone registrars (DIFC/ADGM),
and the federal NER — all **login/WAF/rate-limited** from this environment — and the
open-data portals were **unreachable**. Listed-company financials are public via the
browser on **DFM/ADX** but **WAF/auth-gated** for automation. The realistic path is
**manual/browser per-system** access. Identifiers: **trade/commercial license number**
(per emirate/free zone), **TRN** (FTA), **economic register number** (NER), free-zone
registration numbers. Currency **AED**; Arabic + English. Owners/managers are personal
data (PDPL, Federal Decree-Law 45/2021) — redact. No access controls were bypassed;
the sample uses **public-knowledge listed companies with null registry identifiers**
(nothing fabricated).
