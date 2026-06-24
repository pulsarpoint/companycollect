# Turkey — Search Attempts

## Attempt 1
- Date/time: 2026-06-24
- Source: MERSIS, Ticaret Sicil Gazetesi, KAP, TOBB
- URL: mersis.ticaret.gov.tr ; ticaretsicil.gov.tr ; kap.org.tr ; tobb.org.tr
- Language: Turkish
- Why: Map the central registry, gazette, listed-financials platform.
- Result: MERSIS 200; gazette 200; KAP 308 -> kap.org.tr/en; TOBB 302.
- Decision: Pursue MERSIS (registry) + KAP (financials).

## Attempt 2
- Date/time: 2026-06-24
- Source: KAP company list
- URL: https://www.kap.org.tr/tr/bist-sirketler
- Language: Turkish
- Why: The open listed-company list + financials.
- Result: Next.js page (1.5 MB) with 808 distinct listed companies (KAP id + name + per-company URLs). JSON API endpoints moved (404).
- Decision: RECOMMENDED (listed financials). Extracted the company list for the sample.

## Attempt 3
- Date/time: 2026-06-24
- Source: KAP API endpoint discovery
- URL: /tr/api/kapMemberList ; /tr/api/member/list ; etc.
- Language: Turkish
- Why: Find a JSON company/financials API.
- Result: 404 — the API moved; company entities accessible via the site / per-company pages.
- Decision: Use per-company pages; financial statements public per company.

## Attempt 4
- Date/time: 2026-06-24
- Source: MERSIS query model
- URL: https://mersis.ticaret.gov.tr/
- Language: Turkish
- Why: Identity (MERSIS no / VKN / title).
- Result: JS app with a per-company query; no open bulk/API.
- Decision: Free per-company lookup; no open bulk.

## Attempt 5
- Date/time: 2026-06-24
- Source: Trade Registry Gazette + GİB VKN
- URL: ticaretsicil.gov.tr ; gib.gov.tr
- Language: Turkish
- Why: Company events + tax-number lookup.
- Result: gazette = per-company event search; GİB = VKN/KDV taxpayer lookup. Both per-company, no open bulk.
- Decision: useful_secondary (gazette + VKN).
