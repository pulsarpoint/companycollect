# South Africa — License Notes

## eTenders OCDS (National Treasury) — public domain (PDDL)

- The eTenders OCDS API explicitly declares `license` =
  **`https://opendatacommons.org/licenses/pddl/1-0/`** (Open Data Commons Public
  Domain Dedication and Licence). PDDL places the data effectively in the **public
  domain** — free to use, reuse, and redistribute for any purpose, no attribution
  required (attribution to National Treasury is good practice).
- Access requires **no key**; the API is paginated and needs `dateFrom`/`dateTo`.
- Treatment here: **open / public domain**. Procurement award/supplier data
  (company names, ZAR values, buyers) is public. Note: some supplier **names** may
  be **sole proprietors** (natural persons) — handle per **POPIA** (Protection of
  Personal Information Act).

## CIPC company register — restricted / paid

- CIPC company search, disclosures, director information, and annual financial
  statements (AFS, iXBRL) are sold **per-transaction** via CIPC eServices (customer
  code + fees). **No open bulk/API.** Reuse of purchased registry data is governed
  by CIPC terms.
- Treatment here: **blocked_by_payment**. Cataloged from public documentation only;
  no values copied. **Director** data is personal data (POPIA).

## Central Supplier Database (CSD) — gated

- The CSD (mandatory database of government suppliers) is **login-gated** (for
  registered suppliers / government users). No open bulk.
- Treatment here: **blocked_by_authentication**. Cataloged from public docs only.

## JSE / SENS — exchange terms

- Listed-company financial results / SENS announcements are public but governed by
  **JSE website / SENS terms of use**; verify before redistribution.
- Treatment here: **useful_secondary_source**; not fetched.

## Personal data

- **Directors** (CIPC) and any **sole-proprietor supplier names** are **personal
  data** under **POPIA**. CIPC director data is paid and must be handled lawfully /
  redacted; the committed OCDS sample contains awarded **business** suppliers
  (company names) only.

## Tax identifiers

- The **company registration number** (`YYYY/NNNNNN/NN`) is the authoritative id
  (paid CIPC). The **income tax number** and **VAT number** (South Africa has VAT)
  are issued by **SARS** and are **not openly published**. The open OCDS data has no
  registration/tax/VAT numbers — names only.
