# Iceland — Schema Notes

## Identifiers

- **kennitala** — **10-digit** national identifier, used for **both** legal
  entities and individuals. For a company it is the **company id and the tax id**.
  Structure: `DDMMYY` + 2-digit serial + check digit + century digit. **Company
  kennitalas add 40 to the day field** (so the first two digits are 41–71), which
  distinguishes them from individuals' kennitalas.
- **VSK-númer** (VAT) — Iceland has **VAT (VSK)**; the VAT registration number is a
  **separate** registration from the kennitala. The register shows VSK status.
- The kennitala is the universal join key across Skatturinn registers
  (fyrirtækjaskrá ↔ ársreikningaskrá ↔ VSK).

## Fyrirtækjaskrá per-company overview (free, verified)

| Field | Meaning |
|---|---|
| nafn | Legal/registered name |
| kennitala | 10-digit id (= tax id) |
| lögheimili | Registered (legal) address |
| sveitarfélag | Municipality (with code, e.g. 1300 Garðabær) |
| rekstrarform | Legal/operating form (code + name) |
| ÍSAT atvinnugrein | Economic-activity classification (NACE-based, e.g. 94.99.1) |
| VSK-skrá | VAT-register status |
| forráðamaður | Responsible person / board chair — **PERSONAL DATA (GDPR)** |

### Legal forms (rekstrarform) — examples

- `E1 Einkahlutafélag (ehf)` — private limited company.
- `Hlutafélag (hf)` — public limited company.
- `Sameignarfélag (sf)` — general partnership; `Samlagsfélag (slf)` — limited
  partnership.
- `Samvinnufélag (svf)` — co-operative; `Sjálfseignarstofnun` — foundation;
  `Húsfélag` — housing association; `Útibú` — branch of a foreign company.

## Ársreikningaskrá (Annual Accounts Register) — paid

Filed annual accounts (income statement, balance sheet, equity), keyed on
kennitala, per fiscal year. Filed electronically (Hnappurinn) for public
disclosure; **retrieval paid per-document**, no open bulk/XBRL. Currency **ISK**.

## Dates, money, encoding

- Dates: `DD.MM.YYYY` (Icelandic) — normalize to `YYYY-MM-DD`.
- Money: **ISK** (financials).
- Encoding: UTF-8 (Icelandic characters: á é í ó ú ý þ æ ö ð).

## Internal model mapping

```text
company_id          <- kennitala (10-digit)
registration_number <- kennitala
tax_id              <- kennitala
vat_id              <- VSK-númer (separate registration; status shown in register)
legal_name          <- nafn
company_type        <- rekstrarform (E1 ehf / hf / sf / svf / ...)
status              <- registered (presence in register) ; (afskráð = deregistered)
registered_address  <- lögheimili (+ sveitarfélag)
activity_code        <- ÍSAT atvinnugrein (NACE-based)
financials          <- Ársreikningaskrá (paid; ISK)
officers            <- forráðamaður (chair; personal data, GDPR) — paid certificate for full board
```
