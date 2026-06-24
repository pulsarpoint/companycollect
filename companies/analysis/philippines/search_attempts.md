# Search attempts — Philippines

## Attempt 1
- Date/time: 2026-06-25
- Source: direct probe of candidate official hosts
- Query: GET `sec.gov.ph`, `efast.sec.gov.ph`, `esparc.sec.gov.ph`, `data.gov.ph`,
  `pse.com.ph`, `bnrs.dti.gov.ph`
- Language: English
- Result: sec.gov.ph 403 (WAF); efast 200; esparc 302; data.gov.ph 200; pse 200;
  dti bnrs 200
- Decision: pursue eFAST/eSPARC/SEC Express, PSE, data.gov.ph, DTI

## Attempt 2
- Date/time: 2026-06-25
- Source: eFAST + eSPARC
- Query: home pages
- Result: both are login SPAs — eFAST (file GIS/AFS), eSPARC (registration). No open
  data
- Decision: SEC documents are gated; check the paid document channel

## Attempt 3
- Date/time: 2026-06-25
- Source: data.gov.ph + SEC API hosts
- Query: CKAN `package_search` for sec/corporation/company; `api.sec.gov.ph`, `crs`,
  `ipff`
- Result: data.gov.ph returns the SPA shell (no JSON catalogue); api.sec.gov.ph 404;
  crs/ipff unreachable
- Decision: no open SEC dataset/API; data.gov.ph not usable headless

## Attempt 4
- Date/time: 2026-06-25
- Source: SEC Express (`secexpress.ph`) + DTI BNRS
- Query: home; BNRS /search
- Result: SEC Express = paid document ordering (GIS/AFS/Articles/certificates, "Fees
  and Charges"); DTI BNRS /search 200 (free sole-prop name search)
- Decision: SEC Express = blocked_by_payment; DTI = useful_secondary (sole props)

## Attempt 5
- Date/time: 2026-06-25
- Source: PSE EDGE (`edge.pse.com.ph`)
- Query: POST `/companyDirectory/search.ax` (keyword=PLDT/bank/Jollibee)
- Result: **open** — real listed-company rows (PLDT Inc. / TEL / Services /
  Telecommunications / listed Sep 17 1953; banks; Jollibee Industrial/Food)
- Decision: PSE EDGE = recommended for listed companies + financials

## Attempt 6
- Date/time: 2026-06-25
- Source: identifiers / tax
- Query: SEC Registration Number, TIN, DTI BN, VAT
- Result: SEC reg no (corporations), TIN (BIR), DTI BN (sole props); VAT uses TIN
  (no separate number)
- Decision: document identifier model
