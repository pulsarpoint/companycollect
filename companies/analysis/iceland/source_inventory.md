# Iceland — Source Inventory

| Source | Org | Type | Access | Formats | License | Status |
|---|---|---|---|---|---|---|
| Fyrirtækjaskrá (company register) | Skatturinn | official registry | free per-company; paid bulk | HTML | free overview / paid bulk | **recommended** (per-company) |
| Ársreikningaskrá (Annual Accounts) | Skatturinn | financial statements | paid per-document | PDF | restricted | blocked_by_payment |
| VSK-skrá (VAT register) | Skatturinn | tax registry | free per-company | HTML | free | useful_secondary_source |
| island.is / opingogn.is | Stafrænt Ísland | open data portal | public | HTML | varies | not_company_data |

## Roles

- **skatturinn_fyrirtaekjaskra** — the authoritative register, keyed on the 10-digit
  kennitala. Free per-company overview (name, address, legal form, ÍSAT, VSK status,
  chair). No open bulk (paid extracts/certificates). Verified live (JBT Marel ehf.,
  Icelandair ehf.).
- **arsreikningaskra** — filed annual accounts for public disclosure; paid
  per-document retrieval (financials).
- **vsk_register** — VAT status within the company search (separate VSK number).
- **island_is_open_data** — portal; does not host the register openly.

## Join key

**kennitala (10-digit)** across all Skatturinn registers. The VSK (VAT) number is a
separate registration. The kennitala is also the tax id.
