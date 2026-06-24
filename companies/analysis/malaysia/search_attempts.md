# Search attempts — Malaysia

## Attempt 1
- Date/time: 2026-06-24
- Source: direct probe of candidate official hosts
- Query: GET `ssm.com.my`, `ssm-einfo.my`, `mydata-ssm.com.my`, `data.gov.my`,
  `bursamalaysia.com`, `mytax.hasil.gov.my`
- Language: Malay, English
- Result: ssm.com.my 302; ssm-einfo.my 200; mydata-ssm 301; data.gov.my 200;
  bursamalaysia 403; mytax 200
- Decision: pursue SSM e-Info / MyData; check data.gov.my for a company dataset

## Attempt 2
- Date/time: 2026-06-24
- Source: SSM e-Info (`ssm-einfo.my`)
- Query: parse product catalogue + login flow
- Result: paid product portal (SAML login via idpro.ssm.com.my) — Company/Business/
  LLP/Audit Firm Profile, Financial Comparison 2/3/5/10Y, Financial Historical;
  sample PDFs published
- Decision: SSM = official but PAID per product

## Attempt 3
- Date/time: 2026-06-24
- Source: MyData-SSM (`mydata-ssm.com.my`)
- Query: home
- Result: "Buy SSM Report" — Company Profile + Company Financial Report (paid)
- Decision: second paid SSM channel; same conclusion

## Attempt 4
- Date/time: 2026-06-24
- Source: `data.gov.my` (OpenDOSM) + `api.data.gov.my`
- Query: catalogue search `company`/`business`/`syarikat`/`registration`;
  `data-catalogue?id=...`
- Result: working portal/API but **no company register** — DOSM statistics only;
  catalogue API 404 for company ids
- Decision: data.gov.my = no company-level dataset

## Attempt 5
- Date/time: 2026-06-24
- Source: Bursa Malaysia + HASIL
- Query: listed financials; TIN/SST
- Result: Bursa 403 (WAF) for automation (listed only, browser); LHDN TIN + SST
  (no VAT/GST since 2018)
- Decision: Bursa = useful_secondary (listed); document TIN/SST identifiers

## Attempt 6
- Date/time: 2026-06-24
- Source: identifiers
- Query: SSM company registration number formats
- Result: new 12-digit (since 2019) / old NNNNNNN-A; ROB for sole props; TIN; SST
- Decision: document identifier model
