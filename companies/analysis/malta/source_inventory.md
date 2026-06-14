# Malta — Source Inventory

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| **MBR register (company search)** | Official registry | Free (manual) | HTML/PDF | Public (terms unclear) | **recommended** (manual); automation WAF-blocked |
| **MBR annual accounts / annual return** | Official financials | Paid (EUR 5-25) | **PDF** | Public/paid | blocked by payment (**financials**) |
| **MBR API packages** | Official registry | Paid (subscription) | JSON/XML | Commercial | blocked by payment (sanctioned automation) |
| MBR UBO register | BO register | Restricted (legitimate interest) | HTML | Restricted | blocked by authentication |
| CFR / VIES (MT VAT) | Official tax | Free | SOAP | Validation | useful secondary |
| data.gov.mt | Open data portal | WAF-blocked | CSV/JSON | Per dataset | useful secondary (not register) |
| Commercial aggregators (Kyckr, Creditinfo, …) | Commercial API | Paid | JSON/PDF | Commercial | useful secondary (bulk + financials) |

## Access points

- MBR search: https://mbr.mt/ (free basic search; documents paid EUR 5-25) — registry.mbr.mt / baros.mbr.mt WAF-blocked for bots
- Annual accounts: paid documents via the MBR
- MBR API: subscription packages (Company Search API, etc.)
- VIES: https://ec.europa.eu/taxation_customs/vies/
- Open data (not the register): https://data.gov.mt/ (WAF-blocked)
- Commercial: https://www.kyckr.com/ etc.

## Key facts

- **Identifiers**: **registration number** (e.g. `C 12345`; prefix = entity class). **VAT** = `MT` + 8 digits (separate; VIES/CFR). Income-tax TIN separate.
- **Partial-open / automation-blocked**: free MBR search, but **no open bulk/API** (free), registry portals **WAF-blocked (403)**, documents **paid**, official **API packages paid**.
- **Register is rich** (paid): officers, **shareholders** (name, share type, control), financial info (annual accounts, annual return), status.
- **Financials**: annual accounts (IFRS/GAPSME) + annual return as **paid PDF documents**; no open structured bulk. EUR.
- **UBO** restricted to legitimate interest (post-CJEU). **data.gov.mt** not the register.

See `source_inventory.json` for the machine-readable version.
