# Search attempts — Kenya

## Attempt 1
- Date/time: 2026-06-25
- Source: direct probe of candidate official hosts
- Query: GET `brs.go.ke`, `businessregistration.ecitizen.go.ke`, `ecitizen.go.ke`,
  `nse.co.ke`, `opendata.go.ke`, `itax.kra.go.ke`
- Language: English
- Result: brs.go.ke 200; ecitizen 301; nse 200; opendata.go.ke 200; itax 302
- Decision: explore BRS/eCitizen, NSE, KODI

## Attempt 2
- Date/time: 2026-06-25
- Source: BRS home
- Query: parse links (search/verify/ecitizen/company)
- Result: BRS access is via eCitizen (accounts.ecitizen.go.ke login; brs.ecitizen.go.ke)
- Decision: probe the eCitizen BRS portal + KODI

## Attempt 3
- Date/time: 2026-06-25
- Source: opendata.go.ke (KODI)
- Query: CKAN/DKAN/Socrata catalog APIs (`/api/3/action`, `/data.json`, `/api/catalog/v1`)
- Result: all 404 — small landing page, no accessible company-register dataset
- Decision: KODI = no company dataset

## Attempt 4
- Date/time: 2026-06-25
- Source: brs.ecitizen.go.ke
- Query: GET the BRS search portal
- Result: **403** (login-gated). Documents (CR12/status report) are paid per
  transaction on eCitizen.
- Decision: BRS = blocked_by_payment (eCitizen)

## Attempt 5
- Date/time: 2026-06-25
- Source: NSE (`nse.co.ke/listed-companies/`)
- Query: GET listed-companies directory
- Result: **OPEN** — real listed companies (Absa Bank Kenya PLC, Stanbic Holdings Plc,
  Standard Chartered Bank, Diamond Trust Bank, Sasini Ltd, Williamson Tea, Car &
  General, tea companies). NSE also has announcements + market statistics; /wp-json/ live.
- Decision: NSE = recommended (open, listed)

## Attempt 6
- Date/time: 2026-06-25
- Source: identifiers / tax
- Query: company registration number formats; KRA PIN; VAT
- Result: company reg no (BRS; C./CPR old, PVT-XXXXXXX new), BN, KRA PIN (tax); VAT
  under PIN
- Decision: document identifier model
