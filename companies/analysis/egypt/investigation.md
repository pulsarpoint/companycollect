# Egypt — company data investigation

## Goal

Find official/open sources for **company registry data** and **financial data** for
companies registered in Egypt, download/sample where allowed, and document a
reproducible trail.

## What was found

### 1. GAFI — General Authority for Investment and Free Zones (company authority; gated)

- **`gafi.gov.eg`** is the authority that **establishes companies** (joint-stock /
  limited liability companies under the Investment / Companies Law) and runs
  **investor eServices** (registration/incorporation, amendments). The English site
  loads; the eServices are organised by department (`Services.aspx?DepartmentID=…`)
  and are **login-gated** (registration/incorporation workflows, not a public
  company search). `erp.gafi.gov.eg` did not resolve.
- There is **no open public company search / register** and **no open API** on GAFI.

### 2. Commercial Registry (السجل التجاري) — not openly searchable

- The **Commercial Registry**, under **GOEIC / the Ministry of Supply**, holds the
  commercial registration number for companies and traders. It is **not openly
  searchable online** (in-person / restricted portal). No open bulk/API.

### 3. EGX — Egyptian Exchange (listed companies + financials; WAF-gated)

- **`egx.com.eg`** publishes **listed-company** profiles, disclosures, and financial
  statements. `ListedStocks.aspx` (≈44 KB) and `companiesprofilesearch.aspx` (≈52 KB)
  **load in a browser**, but the company table is rendered by JavaScript and the
  underlying data endpoint (`getinformation.aspx?type=…`) returned **"Request
  Rejected" (WAF)** to automated requests (and other types dropped the connection).
  So EGX is **public via the browser** but **WAF-gated for automation**. Listed
  companies only.

### 4. ETA — Egyptian Tax Authority (tax)

- **`eta.gov.eg`** / `invoicing.eta.gov.eg` (e-invoicing) administer the **Tax ID**
  (الرقم الضريبي). Per-company; not an open bulk register.

### 5. Open-data portal — unreachable

- **`data.gov.eg`** and **`egypt.gov.eg`** did **not resolve/respond** at
  investigation time. **CAPMAS** (`capmas.gov.eg`, the statistics agency) is up but
  publishes statistics, not a company register.

## Conclusion

Egypt has **no open company register and no open programmatic financials**. The
official company authority (**GAFI**) runs **login-gated** investor eServices; the
**Commercial Registry** is not openly searchable; **EGX** (listed companies +
financials) is **public via the browser but WAF-gated** for automation; and the
national open-data portal was **unreachable**. The realistic path is **manual /
browser** access — EGX for listed companies, GAFI eServices (login) for company
establishment/registry per company. Identifiers: **Commercial Registry number**,
**Tax ID** (ETA), **Unified company number**; EGX symbol/ISIN for listed companies.
Currency **EGP**; Arabic + English. Directors/shareholders are personal data (PDP
Law 151/2020) — redact. No access controls were bypassed; the sample uses
**public-knowledge EGX-listed blue-chips with null registry identifiers** (nothing
fabricated).
