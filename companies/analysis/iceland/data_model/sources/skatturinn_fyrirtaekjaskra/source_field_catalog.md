# Fyrirtækjaskrá (Skatturinn) Field Catalog

## Source Summary

- Country: Iceland
- Source type: official_registry
- Organization: Skatturinn (Iceland Revenue and Customs)
- URL: https://www.skatturinn.is/fyrirtaekjaskra/leit/kennitala/{kennitala}
- License: free per-company overview; **paid** bulk / certified certificates
- Access: public per-company search (no key); no open bulk/API
- Freshness: live register
- Record shape: per-company HTML overview
- Primary keys: `kennitala`
- Join keys: `kennitala`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| nafn | Nafn | Legal name | string | legal_name | JBT Marel ehf. | |
| kennitala | Kennitala | 10-digit id (= tax id) | string | identifier | 6204830369 | key; +40 day field |
| logheimili | Lögheimili | Registered address | string | address | Austurhrauni 9 210 Garðabær | |
| sveitarfelag | Sveitarfélag | Municipality | string | geography | 1300 Garðabær | |
| rekstrarform | Rekstrarform | Legal form | string | legal_form | E1 Einkahlutafélag (ehf) | ehf/hf/sf/svf/... |
| isat_atvinnugrein | ÍSAT | Activity (NACE-based) | string | activity | 94.99.1 … | |
| vsk_status | VSK-skrá | VAT status | string | license_or_terms | | separate VSK number |
| forradamadur | Forráðamaður | Chair / responsible person | string | person | | **PERSONAL DATA (GDPR)** |

## Interpretation Notes

- **Verified from real data**: per-company overviews at
  `…/fyrirtaekjaskra/leit/kennitala/{kennitala}` — JBT Marel ehf. (`6204830369`),
  Icelandair ehf. (`4612023490`), a húsfélag (`6306261610`, ÍSAT `94.99.1`).
- **kennitala** (10-digit) is the company id, the tax id, and the universal join
  key. Company kennitalas add **40** to the day field (first two digits 41–71).
  Keep as a string (leading digits significant).
- **Legal form** (`rekstrarform`): `E1 Einkahlutafélag (ehf)` private limited;
  `Hlutafélag (hf)` public limited; `sf`/`slf` partnerships; `svf` co-op;
  `sjálfseignarstofnun` foundation; `húsfélag` housing association; `útibú` branch.
- **ÍSAT** activity maps to EU **NACE**.
- **VAT (VSK)**: status is shown; the VSK number is a **separate** registration from
  the kennitala.
- **Access**: the free overview (gjaldfrjálst yfirlit) is open per-company; **bulk
  extracts** and **certified certificates** (Staðfest vottorð) are **paid**
  (gjaldskrá). No open bulk/API.
- **Personal data**: only the chair (forráðamaður) is in the free overview — it is
  personal data (GDPR) and is **redacted** in the sample. The full board is only on
  the paid certificate.
