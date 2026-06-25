# Pakistan License Notes

## Pakistan Stock Exchange (PSX) Data Portal

- The PSX data portal (`dps.psx.com.pk`) serves listed-company data openly (no auth/payment).
  **No explicit open/reuse license** was located on the symbols API. Treat as
  **psx_terms_unconfirmed**: attribute PSX and confirm bulk-reuse terms (market data is often
  subject to exchange terms) before redistribution.
- Listed-company data is public market disclosure. No personal data in the symbols list.

## SECP eServices

- The SECP registrar is authoritative but **firewalled / WAF-blocked** from this environment.
  Filings require login; some services are paid. Treat access as **restricted**; confirm any
  data-sharing/API arrangement with SECP.
- Directors are natural persons — redact in any stored profile (Pakistan has data-protection
  rules; the Personal Data Protection Bill governs personal data handling).

## FBR — Active Taxpayers List (ATL)

- The ATL is public for **per-NTN verification**, but a clean open bulk file was not located.
  The list includes **individuals** as well as companies — individuals are **personal data**;
  redact. Treat bulk reuse as **restricted** pending confirmation of an official bulk file.

## opendata.com.pk

- Third-party portal; dataset licenses vary and it is **not** an official register. Not used
  as a company source.

## General

- Nothing was bypassed: the SECP WAF was **not** circumvented; only the openly-served PSX API
  was downloaded; FBR per-NTN verification was not scripted/scraped.
- Redact natural-person data (directors, individual taxpayers).
- The **CUIN**, **NTN**, and **PSX symbol** are public company identifiers, not personal data.
