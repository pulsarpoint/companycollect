# Iceland — Search Attempts

## Attempt 1

- Date/time: 2026-06-24
- Source: Skatturinn / RSK / opingogn.is / island.is / Hagstofa
- URL: skatturinn.is ; rsk.is/fyrirtaekjaskra/ ; opingogn.is ; island.is/o/skatturinn ; hagstofa.is
- Language: Icelandic / English
- Why: Skatturinn runs the company register (fyrirtækjaskrá); check the open-data portal and statistics office.
- Result: Skatturinn 200; RSK 301 -> skatturinn.is/fyrirtaekjaskra/; opingogn.is 301 -> island.is; island.is/Hagstofa 200.
- Decision: Pursue the Skatturinn fyrirtækjaskrá; the open-data portal moved and lacks the register.

## Attempt 2

- Date/time: 2026-06-24
- Source: Skatturinn fyrirtækjaskrá page
- URL: https://www.skatturinn.is/fyrirtaekjaskra/
- Language: Icelandic
- Why: Find data access (open dataset / API / paid).
- Result: a free per-company search (leit) + per-kennitala detail pages; a gjaldskrá (fee schedule) link; no open CSV/API/'opin gögn'.
- Decision: Free per-company lookup; bulk/extracts are paid.

## Attempt 3

- Date/time: 2026-06-24
- Source: Per-company detail pages
- URL: https://www.skatturinn.is/fyrirtaekjaskra/leit/kennitala/{kennitala}
- Language: Icelandic
- Why: Capture the real fields.
- Result: real records — JBT Marel ehf. (6204830369), Icelandair ehf. (4612023490), a húsfélag (6306261610). Fields: name, kennitala, lögheimili, sveitarfélag, rekstrarform, ÍSAT, VSK, forráðamaður.
- Decision: RECOMMENDED for per-company access. Used as the real sample.

## Attempt 4

- Date/time: 2026-06-24
- Source: Annual Accounts Register + fee schedule
- URL: …/fyrirtaekjaskra/arsreikningaskra/ ; …/fyrirtaekjaskra/gjaldskra/
- Language: Icelandic
- Why: Financial-statement access + confirm the paid bulk model.
- Result: annual accounts filed electronically (Hnappurinn) "til opinberrar birtingar" (public disclosure); gjaldskrá confirms register fees. Retrieval paid; no open bulk/XBRL.
- Decision: Financials blocked_by_payment; register bulk paid.

## Attempt 5

- Date/time: 2026-06-24
- Source: island.is / opingogn.is open data
- URL: https://island.is/s/stafraent-island/opingogn ; CKAN package_search
- Language: Icelandic / English
- Why: Check for an open company dataset.
- Result: opingogn.is redirects to island.is; legacy CKAN API returns HTML (404 for JSON); the register is not openly hosted.
- Decision: not_company_data; rely on the Skatturinn per-company lookup.
