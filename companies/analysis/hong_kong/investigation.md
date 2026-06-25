# Hong Kong Company Data Investigation

## Goal

Find official/open sources for Hong Kong company data: registry, identifiers, status,
incorporation, directors, and listing data, with reproducible access notes.

## What was found

1. **Companies Registry — open data on data.gov.hk (RNC063 series)** — the standout open
   source. The CR publishes weekly **CSV/XLS** files titled "List of Newly Incorporated /
   Registered / Re-domiciled Companies and Companies which have changed Names" under the
   data.gov.hk CKAN catalog (dataset `hk-cr-crdata-list-newly-registered-companies-2526`).
   Two streams:
   - **RNC063L** — newly **incorporated local** companies (+ name changes). Verified:
     `RNC063L_20241230.csv` = **3,286 rows**. Columns: `Seq`, `Current Company Name in
     English`, `Current Company Name in Chinese`, `BR Number`, `Date of Incorporation`,
     `Date of Change of name`.
   - **RNC063F** — newly **registered non-Hong-Kong** companies (+ name changes). Columns:
     `Seq`, `Current Corporate Name / Other Corporate Name`, `Current Approved Name for
     Carrying on Business in H.K.`, `BR Number`, `Date of Registration`, `Date of Change of
     name`.
   Fully open (no auth/payment). Dates are `DD-MM-YYYY`. The identifier is the **BR Number**
   (Inland Revenue Department Business Registration number, 8-digit) — **not** the CR
   Company Number. **No personal data** (company-level only). This is an **incremental**
   feed (weekly new/changed entries), not the full register.

2. **Companies Registry — ICRIS e-Search (ICRIS3EP)** — the **authoritative full register**
   (`e-services.cr.gov.hk/ICRIS3EP/`). Holds the **CR Company Number** and full particulars
   (company type, status, registered office, directors, company secretary, charges,
   filed documents). It is an **interactive** portal (303 redirect / session-based) and
   document/particulars search is **pay-per-use**; there is **no open bulk or free API**.
   Directors/secretary are natural persons (PDPO). Documented from public knowledge only.

3. **HKEX — List of Securities** — `hkex.com.hk/.../ListOfSecurities.xlsx`. The static
   `.xlsx` URL **returned a template skeleton** for an automated request (placeholders
   `<<Table Header>>`, `<<TableContent>>`, `<<nextTradeDate>>`; dimension A1:R8). The
   populated list is generated **server-side**, so the listed-securities data (stock code,
   name, ISIN) is browser-public but **not cleanly available via this static URL** for
   automation. Listed companies only.

4. **data.gov.hk (CKAN)** — the OGCIO public-sector-information portal. `package_search`
   works; a `q=company` query returns **12 datasets**, of which the only company **register**
   dataset is the CR RNC063 feed above; the rest are statistics (Census & Statistics,
   Official Receiver). The CKAN API is the access path to enumerate the weekly RNC063 URLs.

## Two Hong Kong identifiers

- **CR Company Number** — issued by the Companies Registry; the registry key (ICRIS).
- **BR Number** — issued by the Inland Revenue Department (Business Registration); 8-digit;
  this is the identifier exposed in the **open** RNC063 feed and serves as the de-facto
  tax/business id.

## What was NOT found

- No open **full**-register bulk download or free API (full particulars are ICRIS pay-per-use).
- No open directors/beneficial-ownership dataset.
- No clean automated path to the populated HKEX securities list via the static xlsx.

## Conclusion

Hong Kong has a **genuinely open but incremental** official feed (CR RNC063 weekly CSVs,
keyed on the BR Number, no PII) plus an **authoritative-but-paid** full register (ICRIS).
A maintained company list can be built from the open feed; full particulars require ICRIS.

## Recommended ingestion approach

Bulk: ingest the CR weekly RNC063 CSVs (enumerate resource URLs via the data.gov.hk CKAN
API), accumulating an incremental BR-Number-keyed company list. Use ICRIS e-Search for
authoritative full particulars where a paid/interactive path is acceptable; HKEX as a
browser-public listed overlay. Convert `DD-MM-YYYY` dates.
