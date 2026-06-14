# Hungary — Search Attempts Log

## Attempt 1
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Hungary e-beszamolo.im.gov.hu company financial statements beszámoló download API céginformáció cégjegyzék open data NAV adóalany`
- Language: English + Hungarian
- Why this query was tried: Identify the register, the financials portal, any open API/bulk.
- Top relevant URLs:
  - https://e-beszamolo.im.gov.hu/
  - https://www.e-cegjegyzek.hu/
  - https://companyapi.hu/
- Result: e-beszámoló = free public financial statements (no registration); e-cégjegyzék = free company info; a commercial API resells Ministry of Justice data.
- Decision: Probe e-beszámoló search + download, e-cégjegyzék, and NAV.

## Attempt 2
- Date/time: 2026-06-14
- Source: curl (live) — reachability
- Query: GET e-beszamolo.im.gov.hu (+ /oldal/kereses, /ebeszamolo); e-cegjegyzek.hu
- Result: all HTTP 200. e-beszámoló homepage (44 KB) has the search form; /oldal/kereses is a help page.
- Decision: Find the real search endpoint from the homepage form.

## Attempt 3
- Date/time: 2026-06-14
- Source: curl (live) — homepage form inspection
- Query: grep homepage for form action / fields
- Result: `<form action="/Search/Results" method="post">` with fields firmName, firmNumber, firmTaxNumber.
- Decision: Run one gentle search.

## Attempt 4
- Date/time: 2026-06-14
- Source: curl (live) — e-beszámoló search
- Query: `POST /Search/Results firmName=MOL`
- Result: HTTP 200, JSON **`{"errorText":"A reCaptcha kitöltése nem megfelelő."}`** + page loads recaptcha.js.
- Decision: Search is reCAPTCHA-protected → automated/bulk access blocked; do not bypass. Free MANUAL viewing only. Saved evidence.

## Attempt 5
- Date/time: 2026-06-14
- Source: curl (live) + WebSearch — NAV taxpayer databases + e-cégjegyzék
- Query: NAV áfaalanyok egyszerű/csoportos; e-cegjegyzek title; NAV databases search
- Result: e-cégjegyzék = "Cégszolgálat Ingyenes Céginformáció" (free basic info; full extracts paid). NAV áfaalanyok DBs reachable (HTTP 200), updated daily, single + group/batch query, some CSV downloads; pages have "Letöltés" links. VIES validates HU VAT.
- Decision: NAV áfaalany = useful_secondary (VAT/tax validation, daily). Commercial aggregators = realistic path to full register + structured financials. Built a schematic normalized sample (no per-company open record lawfully downloadable).
