# ATK — VatRegist / SearchTaxPayer Field Catalog

## Source Summary

- Country: Kosovo
- Source type: tax_registry
- Organization: Administrata Tatimore e Kosovës (ATK)
- URL: https://apps.atk-ks.org/BizPasiveApp/VatRegist/Index (search: `VatRegist/SearchTaxPayer`)
- License: not stated (verification use)
- Access: **CAPTCHA-gated** per-company lookup ("I'm not a robot")
- Freshness: live
- Record shape: JSON `{tpResult, captchaData}` behind CAPTCHA
- Primary keys: FiscalNo
- Join keys: FiscalNo, NrbID

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| tpResult.FiscalNo | FiscalNo | Fiscal/tax number | string | identifier |  | = ARBK NUI; join key |
| tpResult.NrbID | NrbID | Business reg number | string | identifier |  |  |
| tpResult.VatNo | VatNo | VAT number | string | identifier |  | if VAT-registered |
| tpResult.VatTypeAl | VatTypeAl | VAT type | string | metadata |  |  |
| tpResult.TpName | TpName | Taxpayer/business name | string | legal_name |  | PII if sole trader |
| tpResult.TpStatus | TpStatus | Taxpayer status | string | status |  |  |
| tpResult.Address | Address | Address | string | address |  |  |
| tpResult.CityName | CityName | City | string | geography |  |  |
| tpResult.ParishName | ParishName | Parish/settlement | string | geography |  |  |
| tpResult.TaxCentreName | TaxCentreName | ATK tax centre | string | metadata |  |  |

## Interpretation Notes

- Per-company verification by **Emri / Nr. Fiskal / NRB / Nr. TVSh**, posting to
  `VatRegist/SearchTaxPayer`. Verified live: the response is
  `{"tpResult":null,"captchaData":{"ErrorMessage":"Kliko 'I'm not a robot'"}}`
  without solving the CAPTCHA. **Not bypassed** — fields documented from the form's
  readonly output inputs (`FiscalNo`, `NrbID`, `TpStatus`, `TpName`, `Address`,
  `CityName`, `ParishName`, `TaxCentreName`, `VatNo`, `VatTypeAl`); no live values.
- **Role**: confirms the **FiscalNo ↔ NRB ↔ VatNo** identifier mapping and the
  taxpayer status/VAT type — a tax-side cross-check for ARBK identity. Joins to
  ARBK on the fiscal number (= NUI).
- `TpName` is personal data when a sole trader — handle with care / redact.
