# Lithuania — License Notes

## Registrų centras JAR via data.gov.lt — open data

- The Register of Legal Entities (JAR) and the financial-statements datasets are
  published as **open data** on Lithuania's national portal **data.gov.lt**.
  Datasets on data.gov.lt are made available under open terms — typically
  **Creative Commons Attribution (CC BY 4.0)** — permitting free reuse, including
  commercial, with **attribution** to the source (Registrų centras / data.gov.lt).
- The **Spinta API** (`get.data.gov.lt`) requires **no API key** and no payment.
- Treatment here: **open / reusable with attribution**. Confirm the exact licence
  on each dataset's data.gov.lt page before redistribution (the per-dataset licence
  field is authoritative).

## Personal data

- Company identity (company code, name, legal form, status, addresses) and
  **financial statements** concern legal entities and are **not personal data**.
- The **`valdymo_organai`** (management bodies) model and any natural-person
  shareholders are **personal data** under the **GDPR**. They must be handled
  lawfully and **redacted** in committed/shared samples. No director data is
  included in this investigation's outputs.

## Tax identifiers

- The **company code (įmonės kodas, 9-digit)** is also the legal-entity **taxpayer
  code** — a corporate identifier, not personal data.
- The **VAT number (PVM kodas)** is a separate registration (`LT` + digits), not in
  the JAR base identity model; verify via **EU VIES**. Lithuania applies EU VAT.

## Source reliability

- Registrų centras is the **official** registrar; data.gov.lt is the official
  government open-data portal. High confidence. Financial-statement coverage
  depends on filing compliance (the `fa_veluojantys` / `fa_dokumentu_nepateike`
  models list late / non-filers).
