# Portugal — Source Inventory

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| **Registo Comercial — certidão permanente (IRN)** | Official registry | Paid | HTML/PDF | Public (paid) | blocked by payment (identified data) |
| **Publicações de atos societários (publicacoes.mj.pt)** | Official gazette | Free (manual) | HTML | Public | **recommended** (free company acts); reCAPTCHA-gated |
| **IES — annual accounts** | Official financials | Restricted | PDF/XBRL | Not openly published | blocked by authentication (**financials**) |
| dados.justica.gov.pt / dados.gov.pt | Open data portal | Free | CSV/XLSX/XML/JSON | CC-BY-SA / CC-BY | useful secondary (statistics, not register) |
| RCBE — beneficial owners | BO register | Restricted | HTML | Restricted | blocked by authentication |
| AT / VIES (PT VAT) | Official tax | Free | SOAP | Validation | useful secondary |
| Commercial aggregators (Racius, Informa D&B/einforma, Iberinform) | Commercial API | Paid (some free search) | JSON/PDF | Commercial | useful secondary (bulk + financials) |

## Access points

- Register: https://eportugal.gov.pt/servicos/aceder-a-certidao-permanente-de-registo-comercial (paid certidão)
- Company acts: https://publicacoes.mj.pt/ (free; reCAPTCHA search)
- IES: https://www.ies.gov.pt/ (filed to AT/INE/Banco de Portugal; not openly published)
- Open data: https://dados.justica.gov.pt/ ; https://dados.gov.pt/ (statistical)
- RCBE: https://rcbe.justica.gov.pt/ ; VIES: https://ec.europa.eu/taxation_customs/vies/
- Commercial: https://www.racius.com/ ; Informa D&B / einforma ; Iberinform

## Key facts

- **Identifiers**: **NIPC** (9-digit collective-entity number = NIF/tax number for companies). **VAT** = `PT` + NIPC. **CAE** = activity code (Rev.3). Número de matrícula (often = NIPC).
- **Partial-open / paid + automation-blocked**: register per-company data **paid** (certidão permanente); free company-acts search **reCAPTCHA-gated**; **no open bulk/API**; IES financials **not openly published**.
- **Open data = statistical only**: dados.justica.gov.pt (RCO, empresas, fcpc, insolvencia) are aggregate counts (CC-BY-SA), not a per-company register. Verified by downloading RCO (120-row time series).
- **Financials**: IES (balanço + demonstração de resultados) filed to AT/INE/Banco de Portugal; per-company figures via paid register or a vendor. EUR.
- **RCBE** (beneficial ownership) restricted post-CJEU.

See `source_inventory.json` for the machine-readable version.
