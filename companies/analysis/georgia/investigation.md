# Georgia Company Data Investigation

## Goal

Find official/open sources for Georgian company data: registry, identifiers, status,
financials, and listing data, with reproducible access notes.

## What was found

Georgia has a reputation for an open public registry, but in practice the **authoritative
registry search is CAPTCHA-gated** and the national open-data portal was unreachable from
this environment. The genuinely browser-public sources are the financial-reporting portal
and the stock exchange.

1. **NAPR — National Agency of Public Registry (e-registry / enreg)** — Ministry of Justice.
   `https://enreg.reestri.gov.ge/main.php`. The **authoritative company registry**. The
   public search page exposes a form with a **`captcha_validator_field`** plus a login
   (`auth_username` / `auth_password`), confirming the free company search/extract
   (amonaweri) is **CAPTCHA-gated**. `api.napr.gov.ge` returns **"Access Denied"**.
   `reestri.gov.ge` 301-redirects to `napr.gov.ge`. No open bulk file or free API was found.
   The registry key is the **9-digit identification code (საიდენტიფიკაციო კოდი)**, which is
   also the tax id. Directors/partners are personal data. Classified
   **blocked_by_authentication** (CAPTCHA); not bypassed.

2. **SARAS Reporting Portal (reportal.ge)** — Service for Accounting, Reporting and Auditing
   Supervision (Ministry of Finance). `https://reportal.ge/en/Reports`. The official portal
   where Georgian entities required to report (public-interest entities, large/medium
   companies) file **annual financial statements + management reports**, freely viewable.
   The page title is "ანგარიშგების პორტალი" (Reporting Portal). Search is **browser-public**
   (by identification code or company name), but **automation is gated**: the simple search
   `POST /en/Base/Search` requires an `__RequestVerificationToken` (anti-forgery), and the
   detailed search is a `GET` form to `/en/Reports/List` (params `orgName`, `year`,
   `legalFormId`, `naceCodes`) whose guessed URLs returned 404. So it is a genuinely open
   **financial-statements** source via the browser, needing token/session handling for
   automation. An API host (`rms.reportal.ge`) exists but guessed endpoints 404'd. Key =
   9-digit identification code.

3. **Georgian Stock Exchange (GSE)** — `https://gse.ge/en/securities`. Browser-public.
   The securities page exposes **Georgian ISINs** (32 distinct `GExxxxxxxxxx` codes
   observed, e.g. `GE1100000029`, `GE2700604186`). Listed companies only; small market; no
   clean open JSON API found (HTML page only).

4. **data.gov.ge** — the national open-data portal (Data Exchange Agency). **Not reachable**
   from this environment: HTTPS timed out and `www.data.gov.ge` presented an **SSL hostname
   mismatch** (DNS resolves but the connection is firewalled / cert-broken from here). Could
   not verify whether it hosts a company dataset. Classified **unavailable** (from here).

## Identifiers

- **Identification code (საიდენტიფიკაციო კოდი)** — 9-digit; the company registration number
  **and** the tax id (used by both NAPR and the Revenue Service). The universal key.
- **ISIN** — for listed securities (GSE).

## What was NOT found

- No open **bulk** company file or free **API** (NAPR API "Access Denied"; data.gov.ge
  firewalled; reportal/enreg are token/CAPTCHA-gated for automation).
- No open directors/beneficial-ownership dataset.

## Conclusion

Georgia is, from this environment, a **browser-public / gated** country rather than the
fully-open registry its reputation suggests: the authoritative NAPR registry is CAPTCHA-
gated, its API is access-denied, and data.gov.ge is firewalled here. The browser-public
**reportal.ge** (financial statements) and **GSE** (listed) are the practical open sources;
NAPR extracts require solving a CAPTCHA. Nothing was bypassed or fabricated.

## Recommended ingestion approach

Manual/browser-public lookup. Company identity/status via NAPR e-registry extracts
(CAPTCHA-gated; key = identification code); financials via reportal.ge (browser/token);
listed companies via GSE. Re-check data.gov.ge from a different network for a possible open
dataset. Convert any dates and redact personal data (directors/partners).
