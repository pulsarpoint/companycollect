# Bangladesh Company Data Investigation

## Goal

Find official/open sources for Bangladeshi company data: registry, identifiers, status,
directors, and listing/financial data, with reproducible access notes.

## What was found

Bangladesh splits between an **open listed-company layer** (the stock exchanges) and a
**gated/paid full register** (RJSC).

1. **Dhaka Stock Exchange (DSE, `dsebd.org`)** — the **cleanest open** Bangladeshi company
   source. `company_listing.php` lists **~640 listed instruments** (verified: **637**
   trading-code + name pairs parsed, e.g. `AAMRANET`=aamra networks limited,
   `AAMRATECH`=aamra technologies limited, `ACMELAB`=The ACME Laboratories Limited). Each
   company has a **browser-public detail page** `displayCompany.php?name=<CODE>` exposing
   **Trading Code, Scrip Code, Sector, Authorized Capital (mn), Paid-up Capital (mn), Listing
   Year, Market Category, Type of Instrument**. It is **plain parseable HTML** (not a SPA),
   no auth/payment. Listed companies only; **key = DSE trading code**; currency BDT.
   Classified **recommended**.

2. **RJSC — Registrar of Joint Stock Companies and Firms (`roc.gov.bd`)** — the
   **authoritative** Bangladeshi company registrar (companies, partnership firms, societies,
   trade organizations), keyed on the **RJSC registration number**. From this environment the
   main site had a **TLS intermediate-certificate issue** (reachable with `-k`; the home page
   301-redirects), and the **eservices portal** (`eservices.roc.gov.bd` did **not resolve**)
   hosts a free company **name search** plus document/schedule retrieval that is
   **pay-per-use**. **No open bulk or free API.** Directors are personal data. Classified
   **blocked_by_payment** (documents paid); field model from public knowledge, nothing
   captured.

3. **Chittagong Stock Exchange (CSE, `cse.com.bd`)** — the second exchange. **Reachable**;
   lists companies/securities (largely overlapping with DSE). Browser-public; not separately
   parsed this pass. Classified **useful_secondary_source** (cross-check to DSE).

4. **National Board of Revenue (NBR, `nbr.gov.bd`)** — **reachable**; provides **BIN**
   (Business Identification Number) and **e-TIN** verification (VAT via the separate VAT
   online system). Per-BIN/TIN verification, not bulk. Identifier = **BIN / e-TIN**.
   Complements RJSC/DSE with tax identity. Classified **useful_secondary_source**.

5. **data.gov.bd — National Open Data Portal (DKAN)** — **reachable**; datasets are
   **statistical/sectoral** (schools, rice trade, doctor directory, birth registration,
   poverty surveys) — **not a company register**; the DKAN API path (`/api/3/action/...`)
   301-redirected. Classified **not_company_data**.

## Identifiers

- **RJSC registration number** — the authoritative company/firm registrar id (RJSC).
- **DSE trading code** (and **scrip code**) — listed-company key (DSE); CSE has its own codes.
- **BIN** (Business Identification Number) / **e-TIN** — NBR tax identifiers.

## What was NOT found

- No open **bulk**/API for the full RJSC register (documents pay-per-use; cert issue).
- No company-register dataset on the open-data portal (statistical only).
- No clean open JSON API for DSE (but the HTML is cleanly parseable).

## Conclusion

Bangladesh is a **hybrid** country: the **listed layer is genuinely open** (DSE — ~640
companies, parseable HTML with trading code, sector, capital, listing year), while the
**authoritative full register (RJSC) is gated/paid** (name search free, documents pay-per-use,
TLS cert issue). **NBR** adds tax identity; **data.gov.bd** has no register. The best open
source is **DSE** (listed); the full company population requires **RJSC** (paid). Nothing was
bypassed or fabricated.

## Recommended ingestion approach

Parse the DSE listed-companies listing (`company_listing.php`) + per-company detail pages
(`displayCompany.php?name=<CODE>`), keyed on the trading code. For the full register (RJSC
number, directors, all entity types), use RJSC eservices (name search free; documents
pay-per-use — do not bypass payment). Use NBR for BIN/e-TIN. Redact directors (personal data).
Currency BDT.
