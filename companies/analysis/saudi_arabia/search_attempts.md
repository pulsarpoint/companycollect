# Search attempts — Saudi Arabia

## Attempt 1
- Date/time: 2026-06-25
- Source: direct probe of candidate official hosts
- Query: GET `mc.gov.sa`, `businesscenter.gov.sa`, `eservices.mc.gov.sa`,
  `saudiexchange.sa`, `open.data.gov.sa`, `zatca.gov.sa`
- Language: Arabic, English
- Result: mc 301→200; businesscenter/eservices.mc 000; saudiexchange 403;
  open.data.gov.sa 000; zatca 302
- Decision: check DNS; pursue MoC CR inquiry + Tadawul

## Attempt 2
- Date/time: 2026-06-25
- Source: DNS + MoC
- Query: `host businesscenter.gov.sa` / `eservices.mc.gov.sa` / `open.data.gov.sa`;
  MoC home links
- Result: businesscenter.gov.sa + eservices.mc.gov.sa **NXDOMAIN**; open.data.gov.sa
  resolves (78.93.109.61) but HTTP timeout (firewalled); MoC has a Commercial-data
  e-service
- Decision: probe the MoC CR-data service

## Attempt 3
- Date/time: 2026-06-25
- Source: MoC Commercial-data e-service + alt hosts
- Query: `/en/eservices/Pages/Commercial-data.aspx`; business.sa; qaweem.mc.gov.sa
- Result: Commercial-data page requires **Login (Nafath)**; alt CR-inquiry hosts
  000/NXDOMAIN
- Decision: MoC CR inquiry = Nafath login-gated; inquiry hosts firewalled

## Attempt 4
- Date/time: 2026-06-25
- Source: Saudi Exchange (Tadawul)
- Query: home; issuer-directory
- Result: **HTTP 403 "Access Denied" (WAF)** for automation; public via browser only
- Decision: Tadawul = listed financials, browser-only (WAF)

## Attempt 5
- Date/time: 2026-06-25
- Source: identifiers / tax
- Query: CR number; Unified National Number (700); VAT number
- Result: CR number (10-digit, region prefix), Unified Number (700...), VAT (15-digit
  ZATCA)
- Decision: document identifier model
