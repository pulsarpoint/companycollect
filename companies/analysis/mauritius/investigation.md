# Mauritius Company Data Investigation

## Goal

Find official/open sources for Mauritius company data: registry, identifiers, status,
directors, financials, and listing data, with reproducible access notes.

## What was found

1. **CBRD — CBRIS Online Company Search** — Corporate and Business Registration Department
   (Ministry of Finance), operated via **Mauritius Network Services (MNS)**. The CBRD site
   (`companies.govmu.org/cbrd`) links to the public search at **`onlinesearch.mns.global`**
   and the CBRIS registration system at `cbris.mns.global/cbris`. The online search is the
   **authoritative** company/business register lookup, keyed on the **BRN (Business
   Registration Number)**. The search front-end is an **Angular SPA** that loads **Cloudflare
   Turnstile** (`challenges.cloudflare.com/turnstile/v0/api.js`) — i.e. the search is
   **CAPTCHA-gated**, and document/file purchase is **paid**. Guessed JSON API endpoints
   404'd (the real API is behind the SPA + Turnstile). **No open bulk or free API.**
   Classified **blocked_by_authentication**; not bypassed.

2. **data.govmu.org — national open-data portal (CKAN)** — reachable and its
   `package_search` API works. A `q=company` search returns only **3 datasets**:
   **"List of ICT Companies in Mauritius"** (CSV, **CC-BY-SA-4.0**, **1,060 rows**),
   **"List of ICT Companies with GPS locations"** (CSV, CC-BY-SA-4.0), and road-accident
   statistics. A `business/registration/CBRD` search returns **0**. So the portal carries a
   genuinely **open but sectoral** company directory (ICT only) and **no full register**.
   The ICT CSV columns are **Title** (company name), **Address**, **District**, **Sectors**,
   **Other Related Sectors** — **no identifiers (no BRN)**, no status, no incorporation date.
   No personal data. (Note: `opendata.govmu.org` and `catalogue.data.govmu.org` do **not
   resolve**; the working host is `data.govmu.org`.) Verified live; downloaded.

3. **Stock Exchange of Mauritius (SEM)** — `stockexchangeofmauritius.com`. Browser-public
   listing/issuer pages across `/listing-issuer-services/` for the **Official Market** and
   **DEM (Development & Enterprise Market)** segments, with **published accounts** and
   **company announcements** per issuer. No single clean listed-companies list or JSON API
   found; navigable HTML. Listed companies only. Classified **useful_secondary_source**.

## Identifiers

- **BRN (Business Registration Number)** — the CBRD/CBRIS company identifier (and the basis
  of the tax identity with the MRA). The key for the full register; not present in the open
  ICT directory.

## What was NOT found

- No open **full**-register bulk/API (CBRD CBRIS is Turnstile-gated; documents paid).
- No identifiers/status/incorporation data in the only open company dataset (ICT directory).
- No clean open list/API for SEM listed companies.

## Conclusion

Mauritius has a **genuinely open but sectoral** company dataset (the CC-BY-SA-4.0 ICT
directory on data.govmu.org) and an **authoritative-but-gated** full register (CBRD CBRIS,
Turnstile + paid). A sectoral seed list can be built from the open CSV; full company
particulars require the CBRD search. SEM covers listed companies. Nothing was bypassed or
fabricated; the hoped-for open mass register was not openly available.

## Recommended ingestion approach

Bulk for the open ICT-companies CSV (CC-BY-SA-4.0). For the full register (BRN, status,
directors), use CBRD CBRIS via the browser (Turnstile; document fees) — do not bypass
Turnstile. Use SEM for listed companies. Re-check data.govmu.org periodically for a register
dataset. Redact directors/shareholders (personal data).
