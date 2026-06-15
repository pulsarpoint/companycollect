# New Zealand — License Notes

## NZBN API — publicly available data, Crown copyright

- The NZBN API returns the **publicly available NZBN information**, which is
  intended to be **freely reused** to help businesses interact. The NZBN data is
  **Crown copyright**; the NZBN programme encourages reuse of the publicly
  available primary business data.
- Access requires a **free subscription key** (registration on the
  api.business.govt.nz developer portal; OAuth). No payment. Verified: HTTP 401
  without a key.
- Treatment here: **blocked_by_authentication** (free key); the returned public
  data is reusable. Some elements (GST numbers, roles) are restricted and not in
  the public tier. Catalog only — no records fetched without a key.

## Companies Register & Disclose Register — public registers

- Both are **public registers** maintained by the Companies Office (the Disclose
  Register on behalf of the FMA). Content is publicly searchable and documents are
  downloadable. Crown copyright; reuse of register information is generally
  permitted but should follow each register's terms.
- The Companies Register help centre documents **no free bulk download or API**.
- Treatment here: **useful_secondary_source** (public search + documents). The
  Disclose Register is the open route to FMC-entity financial statements.

## data.govt.nz

- National open-data catalogue; individual datasets are typically **CC-BY 4.0**.
  It is **bot-protected** (Imperva) to automated clients and does **not** host the
  full company register openly.
- Treatment here: **not_company_data** for this purpose.

## Personal data

- **Directors / shareholders** (Companies Register) and any **contact details** are
  **personal data** under New Zealand's **Privacy Act 2020**. They must be handled
  lawfully and **redacted** in committed/shared samples. The NZBN public tier
  excludes roles/directors. No personal data is included in this investigation's
  outputs.

## Tax identifiers

- The **IRD number** (tax) and **GST number** are **not public**. NZ uses **GST,
  not VAT** — there is no VAT number to capture.
