# South Africa — Source Inventory

| Source | Org | Type | Access | Formats | License | Status |
|---|---|---|---|---|---|---|
| eTenders OCDS | National Treasury | procurement | public, no key | JSON | ODC-PDDL (public domain) | **recommended** |
| CIPC company register | CIPC | official registry | paid per-transaction | HTML, PDF, iXBRL | restricted | blocked_by_payment |
| Central Supplier Database (CSD) | National Treasury / OCPO | supplier registry | login-gated | HTML | restricted | blocked_by_authentication |
| JSE / SENS | Johannesburg Stock Exchange | financial disclosure | public (exchange terms) | PDF, HTML | exchange terms | useful_secondary_source |

## Roles

- **etenders_ocds** — the best **open** source: government procurement releases
  (buyer, tender, awards, supplier **company names**, ZAR values), public domain.
  Partial coverage (government suppliers only); names without registration numbers.
  Verified live (AMESTRA HOLDINGS / ESKOM, etc.).
- **cipc_registry** — the authoritative company register (registration number,
  status, type, directors, AFS); paid, no open bulk.
- **csd_suppliers** — mandatory government-supplier database (links CIPC + SARS);
  login-gated.
- **jse_sens_listed** — listed-company financials (issuers only).

## Join keys

- **No open join key.** The authoritative key is the **CIPC registration number**
  (`YYYY/NNNNNN/NN`), but it is **paid** and **not in OCDS** (which uses the
  supplier **name**). SARS income-tax / VAT numbers are not open. Cross-source joins
  are **name-based** unless the registration number is obtained from CIPC/CSD.
