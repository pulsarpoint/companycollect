# Company Data Analysis For South Africa

## Summary

South Africa is a **paid-registry** country: the authoritative company register
(**CIPC**) is not open (paid per-transaction), and there is **no open financial
source for private companies**. The realistic **open** layer is government
**procurement** — the **National Treasury eTenders OCDS API** (public domain) —
which surfaces company **names** + ZAR award values + government buyers, but only
for firms transacting with government and keyed on **name** (no registration
number). Listed-company financials are open via **JSE/SENS**. The example uses
real OCDS data.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| etenders_ocds | National Treasury eTenders OCDS | recommended | public, no key | ODC-PDDL (public domain) | Open: company names + procurement |
| cipc_registry | CIPC company register | blocked_payment | paid per-transaction | restricted | Authoritative identity / directors / AFS |
| csd_suppliers | Central Supplier Database | blocked_authentication | login-gated | restricted | Links CIPC+SARS+B-BBEE |
| jse_sens_listed | JSE / SENS | planning_only | exchange terms | exchange terms | Listed-company financials |

## What Each Source Contributes

- **etenders_ocds** — the open layer: awarded supplier **company names**, ZAR award
  values, government **buyers**, and tenders. Verified live (AMESTRA HOLDINGS /
  ESKOM ZAR 7.72bn, etc.). Public domain (PDDL). Partial coverage; **no registration
  number** (supplier id = legal name).
- **cipc_registry** — the authoritative register: registration number
  (`YYYY/NNNNNN/NN`), status, type, registration date, directors, registered
  address, and AFS (iXBRL). **Paid**; planning-only; directors are POPIA personal
  data.
- **csd_suppliers** — the bridge that links a supplier's CIPC registration number,
  SARS tax status, and **B-BBEE** level; **login-gated**.
- **jse_sens_listed** — financial results / SENS announcements for **listed**
  issuers (ZAR). The only open financial route; listed-only.

## Proposed Country Company Profile

A single object keyed on `identity.legal_name` (the only open key), with the
authoritative ids as paid/gated placeholders:

- `identity` — legal name (open) + registration number (paid) + CSD number (gated).
- `tax_identifiers` — income-tax / VAT numbers (SARS; not open).
- `legal_identity` — status, type, registration date, address (CIPC, paid).
- `procurement[]` — OCDS awards (buyer, ZAR value, tender) — **open**.
- `bee_status` — B-BBEE level (CSD, gated).
- `officers[]` — directors (CIPC, paid; POPIA).
- `financial_statements[]` — paid (CIPC AFS) / listed (JSE), ZAR.
- `source_provenance[]`.

## Join And Precedence Rules

- **Keys**: authoritative = CIPC **registration number** (paid, not in OCDS); CSD =
  CSD number (gated). **No open join key** — OCDS is name-keyed, so cross-source
  joins are **name-based and approximate**.
- **Precedence**: OCDS (open names + procurement) > CIPC (authoritative identity,
  paid) > CSD (links, gated) > JSE/SENS (listed financials).
- **Award value ≠ revenue**; **no separate** open tax/VAT id.

## Missing Or Restricted Data

- **The full company register / registration number in the open layer** — paid.
- **Private-company financials** — paid (CIPC AFS); listed via JSE/SENS.
- **Tax / VAT numbers** — SARS; not openly published.
- **Directors / beneficial owners** — paid CIPC; personal data (POPIA).

## Common Mapper Notes

- Map `company_id`/`registration_number` to the CIPC registration number but treat
  it as not-in-open-data; the open identity is the **name** (OCDS).
- Mark `tax_id`/`vat_id` as not available; map `financials` from JSE (listed) or
  CIPC (paid).
- Treat cross-source links as approximate (name-based); redact director/POPIA data.
