# Canada Company Data — Investigation

## Conclusion

Canada is **federal + provincial** (like the US): there is **no single national
company register**. Incorporation happens federally (**Corporations Canada**,
under the CBCA) or in one of **13 provinces/territories**, each with its own
registry. Result:

- **Open federal dataset** — Corporations Canada "Federal Corporations" on
  open.canada.ca (**OGL**) covers **federally-incorporated** corporations only.
  Excellent and rich, but a **subset** of all Canadian companies.
- **Provincial registries** — needed for the rest (Québec **REQ** and BC
  **OrgBook** are open; Ontario/Alberta and others vary, some paid).
- **Financials** — open only for **reporting issuers** (public companies / funds)
  via **SEDAR+**; private-company financials are not public.

## Identifiers

- **Corporation number** — federal corporation id (7-digit) assigned by
  Corporations Canada.
- **Business Number (BN)** — CRA 9-digit tax id; the closest to a universal
  Canadian business identifier (used for GST/HST, payroll, corporate income tax).
  GST/HST registration = BN + an **RT** program-account suffix.
- **No separate VAT number** — GST/HST is via the BN/RT account.
- Provincial corporations have their own provincial registry numbers (+ may have a BN).

## Sources found

### 1. Corporations Canada — Federal Corporations (open.canada.ca) — RECOMMENDED
- Dataset `https://open.canada.ca/data/en/dataset/0032ce54-c5dd-4b66-99a0-320a7b5e99f2`
  (ISED). **OGL**. CSVs (CloudFront `d4bf66bykfyaf.cloudfront.net`) split by:
  active/inactive × CBCA/non-CBCA, EN + FR. The active CBCA business file =
  **642,720** rows (102 MB).
- 17 columns: **Corporation number**, **Business number (BN)**, Corporate name -
  form 1 (EN), Corporate name - form 2 (FR), Governing legislation, Status,
  Anniversary date, Year of last annual filing, Date of last annual meeting,
  Street/Street 2/City/Province/Country/Postal code, Minimum/Maximum number of
  directors. **Downloaded the active CBCA file; real record: MINDANGLER CAPITAL
  INC., corp # 8660115, BN 835752437, Ottawa ON.**
- Covers CBCA business corporations + (non-CBCA) NFP corporations, cooperatives,
  boards of trade, etc.

### 2. Corporations Canada — real-time API — useful_secondary
- `https://ised-isde.canada.ca/site/corporations-canada/en/data-services` —
  real-time API for federal-corporation status, registered office, and
  **directors** (names — personal data). Good for per-corporation enrichment.

### 3. SEDAR+ — reporting-issuer financials (free) — useful_secondary
- `https://www.sedarplus.ca/` — the Canadian Securities Administrators' (13
  provincial regulators) national filing system. **Free** access to public-company
  / fund filings: annual reports, **financial statements**, prospectuses,
  continuous disclosure. The open route to Canadian financials — **reporting
  issuers only**.

### 4. Provincial registries — needed for full coverage
- **Québec — REQ** (Registraire des entreprises): open data on Données Québec
  (entreprises). **BC — OrgBook BC** (orgbook.gov.bc.ca): open API/registry. Other
  provinces (Ontario, Alberta, …) vary — many charge for searches/bulk. Companies
  incorporated provincially are **not** in the federal dataset.

### 5. Aggregators — secondary
- OpenCorporates and similar mirror federal + provincial data; restricted/paid
  bulk. Cross-check only.

## What was NOT bypassed

- Only the OGL federal open dataset was downloaded. SEDAR+ documents, the API, and
  any paid provincial searches were not accessed. Director names (API) are
  personal data — redact.

## Recommended ingestion

Bulk-load the **federal CSVs** keyed on **corporation number** (+ **BN** as the
tax/join key); add **provincial** registries (Québec REQ, BC OrgBook, …) for
provincial corporations; pull **SEDAR+** for reporting-issuer financials. No VAT
id (GST/HST via BN/RT).
