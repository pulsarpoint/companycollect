# BMV / CNBV Listed-Company Disclosures Field Catalog

> **PLANNING-ONLY / EXCHANGE TERMS.** Financial statements for **listed** companies
> (issuers) only, via BMV (EMISNET) and CNBV (SITI). Reuse governed by exchange/
> regulator terms. Cataloged from public documentation — not fetched.

## Source Summary

- Country: Mexico
- Source type: financial_disclosure
- Organization: Bolsa Mexicana de Valores / CNBV
- URL: https://www.bmv.com.mx/
- License: exchange terms of use (verify before redistribution)
- Access: public (exchange websites)
- Freshness: quarterly / event-driven
- Record shape: per-issuer financial report
- Primary keys: `ticker`
- Join keys: `ticker`, `rfc`, `issuer_name`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| issuer.ticker | Clave de cotización | Stock ticker | string | identifier | listed only |
| issuer.name | Emisora | Issuer name | string | legal_name | |
| report.period | Periodo | Reporting period | string | date | quarterly/annual |
| report.revenue | Ingresos totales | Revenue (MXN) | decimal | financial | IFRS XBRL |
| report.net_income | Utilidad neta | Net income (MXN) | decimal | financial | IFRS XBRL |
| report.total_assets | Activos totales | Total assets (MXN) | decimal | financial | IFRS XBRL |

## Interpretation Notes

- Covers only **listed issuers** (a few hundred companies). For the vast majority
  of Mexican companies there is **no public financial statement** — DENUE has no
  financials, and the legal registry holds only the share capital figure.
- Financial reports follow **IFRS** (XBRL/Excel via EMISNET/SITI); currency **MXN**.
- **Join**: by ticker / issuer name (or RFC where disclosed) to the rest of the
  profile. No shared key with DENUE.
- The only **open financial route** for Mexico, but listed-only. No raw sample.
