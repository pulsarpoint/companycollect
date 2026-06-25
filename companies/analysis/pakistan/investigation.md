# Pakistan Company Data Investigation

## Goal

Find official/open sources for Pakistani company data: registry, identifiers, status,
directors, and listing data, with reproducible access notes.

## What was found

1. **Pakistan Stock Exchange — Data Portal (`dps.psx.com.pk`)** — the standout **open**
   source. `https://dps.psx.com.pk/symbols` returns an **open JSON array** of all listed
   symbols — **verified live: 1,068 symbols**, of which **744 are non-debt/non-ETF
   equities** — each with `symbol`, `name`, `sectorName`, `isETF`, `isDebt` (e.g.
   `OGDC` / Oil & Gas Development, `HBL` / Habib Bank, `LUCK` / Lucky Cement, `ENGRO` /
   Engro Corporation). Per-company pages at `dps.psx.com.pk/company/{symbol}` return
   browser-public HTML with sector, registered address, free float, and shares. No auth or
   payment. **Listed companies only.** (PSX terms/reuse not explicitly stated — confirm.)

2. **SECP eServices — Company / LLP Registry** — Securities and Exchange Commission of
   Pakistan, the **authoritative** company/LLP registrar. The SECP website returned **HTTP
   403 (WAF)** and `eservices.secp.gov.pk` **timed out** — **firewalled from this
   environment**. The eServices portal hosts company name search and filings (filings require
   login). The registry key is the **CUIN (Company Universal Identification Number)** / SECP
   registration number. Directors are personal data. Documented from public knowledge; **not
   reachable here**. Classified **blocked_by_authentication** (WAF + login).

3. **FBR — Active Taxpayers List (ATL) / NTN verification** — Federal Board of Revenue. The
   ATL pages are public, but access is **per-NTN online verification** (plus the Tax Asaan
   app / SMS); a clean **open bulk ATL download file was not located** (the category pages are
   informational, with no direct `.zip`/`.txt`). The identifier is the **NTN (National Tax
   Number)**; the ATL covers companies and individuals (individuals are personal data).
   Classified **useful_secondary_source** (verification), bulk not confirmed open.

4. **opendata.com.pk** — a reachable **third-party** open-data portal (not an official
   government registry); no authoritative company-register dataset confirmed. Listed for
   completeness; **not_company_data** for our purposes.

## Identifiers

- **CUIN (Company Universal Identification Number)** — SECP company id (registrar key).
- **NTN (National Tax Number)** — FBR tax id.
- **PSX symbol** — listed-security/company ticker (listed only).

## What was NOT found

- No open **full**-register bulk/API (SECP is WAF-blocked/firewalled here).
- No clean open **bulk** FBR ATL file (per-NTN verification only).
- No official government open-data company dataset (opendata.com.pk is third-party).

## Conclusion

Pakistan has an **excellent open API for listed companies** (PSX data portal) but its
**authoritative registrar (SECP) is firewalled/WAF-blocked** from this environment, and the
**FBR ATL** is per-NTN verification rather than open bulk. A rich **listed-company** layer is
buildable openly; the full company register requires SECP (from an unblocked network, behind
its WAF/login). Nothing was bypassed or fabricated.

## Recommended ingestion approach

API for listed companies: ingest `dps.psx.com.pk/symbols` (JSON) and per-company pages
(`/company/{symbol}`). For the full register (CUIN, status, directors), use SECP eServices
from an unblocked network — do not bypass the WAF. For tax status, use FBR ATL per-NTN
verification. Redact directors/individuals (personal data).
