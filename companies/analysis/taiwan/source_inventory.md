# Taiwan Source Inventory

| Source | Slug | Org | Access | Format | Status | Role |
|---|---|---|---|---|---|---|
| MOEA GCIS Company Basic Data | gcis_company_basic | MOEA Dept. of Commerce | open API | JSON/XML/CSV | recommended | All companies; universal identity (統一編號) |
| TWSE Listed Company Basic Info | twse_listed | Taiwan Stock Exchange | open API | JSON | recommended | Listed companies (main board) + disclosure fields |
| TPEx OTC Company Basic Info | tpex_listed | Taipei Exchange | open API | JSON | recommended | OTC (櫃買) listed companies |
| data.gov.tw | data_gov_tw | National Development Council | open portal | JSON/CSV | useful_secondary_source | Catalog indexing GCIS + TWSE/TPEx datasets |

## Notes

- **All three company sources are fully open JSON APIs** (no auth, no payment) and share the
  **統一編號 (Unified Business Number)** join key:
  GCIS `Business_Accounting_NO` == TWSE `營利事業統一編號` == TPEx `UnifiedBusinessNo.`
  (verified for TSMC = `22099131`).
- **GCIS** is the authoritative register for **all** companies (query by 統一編號; the
  `Company_Name like` filter is finicky). Dates are **ROC/Minguo** (`YYYMMDD`, year = AD−1911).
- **TWSE** (1,089) and **TPEx** (890) cover the listed markets with rich disclosure fields;
  their setup/listing dates are **Gregorian** (`YYYYMMDD`).
- **Personal data**: responsible person (GCIS), chairman/GM/spokesperson/auditor (TWSE/TPEx)
  are natural persons — redact per Taiwan PDPA.
- **License**: Open Government Data License, Taiwan (confirm attribution requirement).
