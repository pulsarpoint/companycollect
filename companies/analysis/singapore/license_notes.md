# Singapore — License Notes

## ACRA entities via data.gov.sg — Singapore Open Data Licence

- The ACRA "Information on Corporate Entities" datasets on data.gov.sg are
  published under the **Singapore Open Data Licence** (and the data.gov.sg terms of
  use), which permits **free use, reuse, and redistribution including commercial**,
  with **attribution** to the data source (ACRA / data.gov.sg) and the standard
  no-warranty / no-misrepresentation conditions.
- The dataset search and poll-download APIs require **no key** and no payment.
- Treatment here: **open / reusable with attribution**. Entity identity (UEN, name,
  type, status, address, SSIC) is corporate data, not personal data. The dataset
  gives **only the count** of officers, not names.

## ACRA BizFile+ — paid

- Full business profiles (officers, shareholders, share capital) and financial
  statements (XBRL via BizFinx) are sold **per-document** on BizFile+. No open
  bulk/API.
- Treatment here: **blocked_by_payment**. Cataloged from public documentation only;
  no values copied. Officer/shareholder data is **personal data (PDPA)**.

## SGX — exchange terms

- Listed-company financial statements / results are public via SGX company
  announcements, governed by **SGX website terms of use**. Verify before
  redistribution.
- Treatment here: **useful_secondary_source**; not fetched.

## Personal data

- **Officer / shareholder names** are **personal data** under Singapore's **PDPA
  (Personal Data Protection Act)**. They are **not** in the open ACRA dataset (only
  the count `no_of_officers` is open), and must be handled lawfully and redacted if
  obtained from the paid BizFile profile.

## Tax identifiers

- The **UEN** is the entity id and the entity's **tax reference**. Singapore has
  **GST** (not VAT); the GST registration number for a local entity is generally the
  **UEN** — there is no separate VAT number. Map `tax_id` to the UEN.
