# Singapore — Search Attempts

## Attempt 1

- Date/time: 2026-06-16
- Source: data.gov.sg, ACRA BizFile, SGX
- URL: https://data.gov.sg/ ; https://www.bizfile.gov.sg/ ; https://www.sgx.com/
- Language: English
- Why: ACRA is the registrar; data.gov.sg is the open-data portal; SGX holds listed financials.
- Result: all HTTP 200.
- Decision: Pursue the ACRA open dataset on data.gov.sg.

## Attempt 2

- Date/time: 2026-06-16
- Source: data.gov.sg dataset search API
- URL: https://api-production.data.gov.sg/v2/public/api/datasets?query=ACRA Information on Corporate Entities
- Language: English
- Why: Find the ACRA entity datasets.
- Result: found "ACRA Information on Corporate Entities ('B')" = d_3a3807c023c61ddfba947dc069eb53f2 (one of an A–Z family).
- Decision: Download via the poll-download API.

## Attempt 3

- Date/time: 2026-06-16
- Source: data.gov.sg poll-download API
- URL: https://api-open.data.gov.sg/v1/public/api/datasets/d_3a3807c023c61ddfba947dc069eb53f2/poll-download
- Language: English
- Why: Get the real CSV + schema.
- Result: signed S3 URL → CSV 30.6 MB, 93,896 entities, 53 columns. Real records (BRIDGESTONE SINGAPORE PTE LTD, etc.). No key.
- Decision: RECOMMENDED. Used as the real sample.

## Attempt 4

- Date/time: 2026-06-16
- Source: ACRA BizFile+ (financials)
- URL: https://www.bizfile.gov.sg/
- Language: English
- Why: Financial statements + officers/shareholders.
- Result: business profiles & financial statements (XBRL via BizFinx) sold per-document; no open bulk.
- Decision: blocked_by_payment.

## Attempt 5

- Date/time: 2026-06-16
- Source: SGX (listed financials)
- URL: https://www.sgx.com/securities/company-announcements
- Language: English
- Why: Listed-company financials.
- Result: public company announcements / results for issuers (PDF/Excel); under SGX terms.
- Decision: useful_secondary_source (listed-only).
