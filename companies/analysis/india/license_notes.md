# India — License Notes

## MCA Company Master Data on data.gov.in — GODL-India

- The MCA Company Master Data published on **data.gov.in** is licensed under the
  **Government Open Data License – India (GODL-India)**. GODL permits **free use,
  reuse, reproduction, distribution, and adaptation, including for commercial
  purposes**, subject to **attribution** to the data provider (MCA / data.gov.in)
  and a no-misrepresentation / no-warranty clause.
- The OGD REST API requires a **free API key** (registration on data.gov.in). A
  **public sample key** is documented by data.gov.in for testing and was used here.
  Production use should obtain an own key and respect per-key rate limits.
- Treatment here: **open / reusable with attribution**. Company identity (CIN,
  name, capital, address) is corporate data, not personal data.

## Personal data within the dataset

- The 2021 resources include `email_addr` — a company **contact email**, which is
  frequently a personal address (e.g. a director's gmail). This is **personal
  data** under India's **DPDP Act 2023**. It is **redacted** in the committed
  normalized sample and should be handled lawfully if ingested.
- **Directors/officers (DIN)** are not in the open bulk; if obtained from the MCA
  portal they are personal data and must be redacted in shared outputs.

## MCA21 portal — paid documents

- The live MCA register (mca.gov.in) offers a **free per-CIN master-data lookup**
  and **pay-per-document** access to public documents (annual financial statements
  AOC-4 / XBRL, charges, etc.) under MCA21 terms. **No open bulk.**
- Treatment here: **blocked_by_payment / blocked_by_authentication**. The portal is
  also WAF-protected (403 to automated clients). Cataloged from public docs only.

## BSE / NSE / SEBI — listed-company disclosures

- Financial results and shareholding for **listed** companies are published by the
  exchanges (BSE/NSE) and under SEBI disclosure rules. Reuse is governed by each
  **exchange's website terms of use**; treat as restricted-by-terms and verify
  before redistribution.
- Treatment here: **useful_secondary_source** (open route to listed financials);
  not fetched.

## Summary

- **Open + reusable (attribution):** MCA Company Master Data via data.gov.in OGD
  API (GODL-India).
- **Paid:** full financial statements (MCA21 documents).
- **Listed-only, by exchange terms:** BSE/NSE/SEBI financials.
- **Redact:** company contact email and any director personal data (DPDP Act).
