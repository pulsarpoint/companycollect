# Greece — Source Inventory

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| **GEMI publicity portal** | Official registry | Free (manual) | HTML/JSON | Public (terms unclear) | **recommended** (manual); API blocked |
| **GEMI financial statements** | Official financials | Free (view) | **PDF** | Public | useful secondary (**financials**, PDF) |
| **AADE RgWsPublic** | Official tax | Credentials | SOAP/XML | Restricted | blocked by authentication |
| VIES (EL VAT) | Official tax | Free | SOAP | Validation | useful secondary |
| data.gov.gr | Open data portal | Token | JSON/CSV | Open (per dataset) | useful secondary (not company register) |
| Diavgeia (Δι@ύγεια) | Transparency | Free | JSON/XML | Open | useful secondary (ΑΦΜ↔name) |
| KIMDIS/Promitheus procurement | Procurement | Free | XML/JSON | Open | useful secondary (supplier ΑΦΜ) |
| Commercial aggregators (ICAP/CRIF, Kyckr) | Commercial API | Paid | JSON/PDF | Commercial | useful secondary (structured financials) |

## Access points

- GEMI search: https://www.businessportal.gr/en/home-en/ (EN) ; https://publicity.businessportal.gr/ (EL) — manual; `/api` undocumented + reCAPTCHA + rate-limited
- Financials: on the company's GEMI page (announcements/filings, PDF)
- AADE company web service: https://www1.aade.gr/webtax2/wsgsis/RgWsPublic/RgWsPublicPort (SOAP; registered credentials)
- VIES: https://ec.europa.eu/taxation_customs/vies/
- Open data: https://data.gov.gr/ ; Diavgeia: https://diavgeia.gov.gr/opendata/ ; Procurement: https://www.eprocurement.gov.gr/

## Key facts

- **Identifiers**: **GEMI number** (registry) + **ΑΦΜ** (AFM, 9-digit tax id; the cross-source join key). **VAT** = `EL` + ΑΦΜ. **ΚΑΔ** = activity code.
- **Partial-open**: free manual GEMI search, but **no open bulk** and the API is **reCAPTCHA-protected + rate-limited** (verified) → blocked for lawful automation.
- **Financials**: published to GEMI as **PDF documents** (ΕΛΠ/IFRS), not structured open data. EUR.
- **Tax side**: AADE RgWsPublic returns basic company data by ΑΦΜ but needs **registered credentials**.
- **No open mirror** (OpenSanctions gr_gemi → 404).

See `source_inventory.json` for the machine-readable version.
