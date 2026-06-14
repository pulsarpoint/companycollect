# Luxembourg — Source Inventory

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| **RCS register (LBR public search)** | Official registry | Free (manual) | HTML/PDF | Public (terms unclear) | **recommended** (manual); no open bulk/API |
| **RCS annual accounts (comptes annuels / eCDF)** | Official financials | Free (download) | **PDF**/eCDF | Public | useful secondary (**financials**, PDF) |
| RESA (legal gazette) | Official gazette | Free | HTML/PDF | Public | useful secondary (events/history) |
| RBE — beneficial owners | BO register | Restricted | HTML | Restricted | blocked by authentication |
| AED / VIES (LU VAT) | Official tax | Free | SOAP | Validation | useful secondary |
| data.public.lu / STATEC | Open data portal | Free | CSV/JSON | CC0/open | useful secondary (statistics, not register) |
| Commercial aggregators (Kyckr, Creditreform, …) | Commercial API | Paid | JSON/PDF | Commercial | useful secondary (bulk + structured financials) |

## Access points

- RCS search: https://www.lbr.lu/ (recherche; captcha-gated) — basic info free; documents (statutes, comptes annuels) free to download; certified extracts paid
- Annual accounts: on the company's RCS page (PDF / eCDF)
- RESA gazette: https://www.lbr.lu/resa/
- VIES: https://ec.europa.eu/taxation_customs/vies/
- Open data (statistics): https://data.public.lu/ (uData API)
- Commercial: https://www.kyckr.com/ etc.

## Key facts

- **Identifiers**: **RCS number** (e.g. `B123456`; prefix = entity class) + **matricule** (13-digit national id). **VAT** = `LU` + 8 digits (separate; VIES/AED).
- **Partial-open**: free RCS search + **free document downloads** (incl. annual accounts), but **no open bulk/API**, search **captcha-gated**, certified extracts paid.
- **Financials**: comptes annuels (bilan + P&L + annexes) filed via eCDF, free to download per company as **PDF**; no open structured bulk. EUR.
- **No open register on data.public.lu** (STATEC statistical aggregates only).
- **RBE** (beneficial ownership) restricted post-CJEU.

See `source_inventory.json` for the machine-readable version.
