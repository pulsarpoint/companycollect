# Search attempts — Thailand

## Attempt 1
- Date/time: 2026-06-24
- Source: direct probe of candidate official hosts
- Query: GET `dbd.go.th`, `datawarehouse.dbd.go.th`, `opendataapi.dbd.go.th`,
  `data.go.th`, `set.or.th`, `vsreg.rd.go.th`
- Language: Thai, English
- Result: dbd.go.th 200; datawarehouse 403; opendataapi 000; data.go.th 403;
  set.or.th 302
- Decision: locate the DBD OpenAPI; explore DataWarehouse + data.go.th

## Attempt 2
- Date/time: 2026-06-24
- Source: DBD home + OpenAPI hosts
- Query: parse dbd.go.th; GET `openapi.dbd.go.th`, `api.dbd.go.th`, paths
- Result: openapi.dbd.go.th alive (403 root, 404 on wrong path → nginx, host up)
- Decision: try the documented juristic_person path

## Attempt 3
- Date/time: 2026-06-24
- Source: DBD OpenAPI
- Query: `GET /api/v1/juristic_person/0107544000094` (+ PTT, Bangkok Bank, CP All)
- Result: **HTTP 200 JSON, no token** — real data (id, NameTH/EN, type, register
  date, status, TSIC objective, register+paid-up capital THB, structured address).
  Verified PTT/Bangkok Bank/CP All/INET.
- Decision: **RECOMMENDED** — the open official company API

## Attempt 4
- Date/time: 2026-06-24
- Source: DBD OpenAPI (financials)
- Query: `financial_statement` / `balance_sheet` / v2 paths
- Result: 404 — the open API exposes the juristic profile (incl. capital), not full
  statements
- Decision: full financials → DataWarehouse / SET

## Attempt 5
- Date/time: 2026-06-24
- Source: DBD DataWarehouse + data.go.th
- Query: DataWarehouse search/company XHR; data.go.th CKAN package_search
- Result: DataWarehouse 302/403 (login-gated); data.go.th 403 "Access Denied" (WAF)
- Decision: DataWarehouse = blocked_by_authentication (financials); data.go.th
  unavailable for automation here

## Attempt 6
- Date/time: 2026-06-24
- Source: identifiers / tax
- Query: juristic ID vs Tax ID vs VAT
- Result: one 13-digit number = juristic registration = Tax ID; VAT uses the same
  number (no separate VAT number)
- Decision: document single 13-digit identifier model
