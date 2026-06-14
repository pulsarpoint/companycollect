# Hungary — Source Inventory

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| **e-beszámoló** | Official financials | Free (manual) | PDF/XML | Public (terms unclear) | **recommended** (**financials**); search reCAPTCHA-gated |
| **e-cégjegyzék / Cégszolgálat** | Official registry | Free basic / paid full | HTML | Free + paid | useful secondary (identity) |
| **NAV áfaalanyok** | Official tax | Free | HTML/CSV/XML | Public | useful secondary (VAT/tax validation, daily) |
| VIES (HU VAT) | Official tax | Free | SOAP | Validation | useful secondary |
| KSH business register | Statistical | Free | XLSX/CSV | Open | useful secondary (statistical code, TEÁOR) |
| EKR / Közbeszerzés | Procurement | Free | HTML/XML | Public | useful secondary (supplier adószám) |
| Commercial aggregators (OPTEN, Bisnode, Céginfo, companyapi.hu) | Commercial API | Paid | JSON/PDF | Commercial | useful secondary (full register + financials) |

## Access points

- Financials: https://e-beszamolo.im.gov.hu/ (search `POST /Search/Results`, reCAPTCHA-gated) — manual viewing free
- Register: https://www.e-cegjegyzek.hu/ (free basic info; full cégkivonat paid)
- NAV VAT subjects: https://nav.gov.hu/adatbazisok/adatbleker/afaalanyok (single + group/batch query; daily)
- VIES: https://ec.europa.eu/taxation_customs/vies/
- KSH: https://www.ksh.hu/ ; Procurement: https://ekr.gov.hu/
- Commercial: https://www.opten.hu/ ; https://companyapi.hu/

## Key facts

- **Identifiers**: **cégjegyzékszám** (`NN-NN-NNNNNN`, register) + **adószám** (11-digit: 8-digit base + VAT code + county). **EU VAT** = `HU` + 8-digit base. **statisztikai számjel** (17-digit, KSH). **TEÁOR** = activity code.
- **Partial-open**: financials free to view + basic identity free, but **no open bulk**, e-beszámoló search **reCAPTCHA-gated** (verified), full register data **paid**.
- **Financials**: structured key figures (revenue, profit after tax, assets, equity, liabilities) + PDF + XML form on e-beszámoló. HUF (some EUR). High coverage (mandatory e-filing).
- **NAV áfaalanyok**: daily VAT-subject DB; single + batch query; some CSV downloads.
- **No sanctioned open bulk/mirror**.

See `source_inventory.json` for the machine-readable version.
