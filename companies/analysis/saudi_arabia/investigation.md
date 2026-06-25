# Saudi Arabia — company data investigation

## Goal

Find official/open sources for **company registry data** and **financial data** for
companies registered in Saudi Arabia, download/sample where allowed, and document a
reproducible trail. Do not bypass access controls.

## What was found

### 1. Ministry of Commerce (MoC) — Commercial Register (Nafath login-gated)

- **MoC** (`mc.gov.sa`) runs the **Commercial Register (السجل التجاري)** and provides
  a **Commercial Register inquiry/verification** e-service ("Commercial-data"). The
  MoC site loads (HTTP 200) and references the Commercial Register Law and trade
  names, but the inquiry service requires **Login (Nafath — national digital
  identity)** to view CR data.
- **Access (verified):** the inquiry sub-hosts `eservices.mc.gov.sa`,
  `businesscenter.gov.sa`, and `qaweem.mc.gov.sa` returned **NXDOMAIN / timed out**
  from this environment; the **Saudi Business Center** (unified establishment portal)
  was not reachable. There is **no open bulk register or open API**. The CR-data page
  is **login-gated**.

### 2. Saudi Exchange (Tadawul) — listed financials (WAF-gated)

- **`saudiexchange.sa`** publishes **listed-company** profiles, disclosures, and
  financial statements (the issuer directory). It returned **HTTP 403 "Access Denied"
  (WAF)** for automated requests — **public via the browser** but WAF-gated for
  automation. Listed companies only (e.g. Saudi Aramco / 2222, Al Rajhi Bank / 1120,
  SABIC / 2010, STC / 7010).

### 3. open.data.gov.sa — national open data (firewalled)

- **`open.data.gov.sa`** **resolves** (`78.93.109.61`) but **HTTP timed out** from
  this environment (firewalled); the CKAN API also timed out. No company-register
  dataset could be confirmed.

### 4. ZATCA — tax/VAT

- **ZATCA** (`zatca.gov.sa`, Zakat, Tax and Customs Authority) administers the
  **VAT number** (15-digit) and corporate **Zakat/tax**, with a VAT-registrant
  verification tool. Per-company; not open bulk.

## Conclusion

Saudi Arabia has **no open company register and no open programmatic financials**.
The official **MoC Commercial Register** inquiry is **Nafath login-gated** and its
inquiry hosts were **NXDOMAIN/firewalled** from this environment; the **Saudi Business
Center** was not reachable; **Tadawul** (listed financials) is **public via the
browser but WAF-gated**; and **open.data.gov.sa** was **firewalled**. The realistic
path is **manual/browser** access (MoC CR inquiry via Nafath; Tadawul for listed).
Identifiers: **CR number** (10-digit, region-prefixed), **Unified National Number /
700 number**, **VAT number** (15-digit, ZATCA). Currency **SAR**; Arabic + English.
Managers/owners are personal data (PDPL) — redact. No access controls were bypassed;
the sample uses **public-knowledge Tadawul-listed companies with null registry
identifiers** (nothing fabricated).
