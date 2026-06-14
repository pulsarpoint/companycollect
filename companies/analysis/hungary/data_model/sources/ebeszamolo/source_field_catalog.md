# e-beszámoló — Electronic Financial Reports Portal Field Catalog

> Field model documented from the e-beszámoló portal. **No `sample_record.json`**: the search/download is
> reCAPTCHA-protected, so no per-company report was lawfully downloadable; no real values copied.

## Source Summary

- Country: Hungary
- Source type: official_financial_disclosure
- Organization: Igazságügyi Minisztérium — Céginformációs Szolgálat (Ministry of Justice)
- URL: https://e-beszamolo.im.gov.hu/ (search `POST /Search/Results`, reCAPTCHA-gated)
- License: public (free to view); reuse/redistribution terms unclear
- Access: public (manual); search reCAPTCHA-protected
- Freshness: annual filing (continuous)
- Record shape: company report — key figures + PDF + electronic form (XML)
- Primary keys: `cegjegyzekszam`, `report_year`
- Join keys: `cegjegyzekszam`, `adoszam`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| cegjegyzekszam | cégjegyzékszám | Registration number | string | identifier | (not copied) | join key |
| adoszam | adószám | Tax number | string | identifier | (not copied) | 8-digit base = stem |
| report_year | üzleti év | Report year | integer | date | (not copied) | per-year key |
| ertekesites_netto_arbevetele | értékesítés nettó árbevétele | Net sales revenue | decimal | financial | (not copied) | HUF/EUR |
| adozott_eredmeny | adózott eredmény | Profit after tax | decimal | financial | (not copied) | HUF/EUR |
| eszkozok_osszesen | eszközök összesen | Total assets | decimal | financial | (not copied) | HUF/EUR |
| sajat_toke | saját tőke | Equity | decimal | financial | (not copied) | HUF/EUR |
| kotelezettsegek | kötelezettségek | Liabilities | decimal | financial | (not copied) | HUF/EUR |
| documents | beszámoló dokumentumok | PDF + XML form | array | document | (not copied) | gated |

## Interpretation Notes

- **The financial spine — free but gated.** All annual financial statements (**beszámoló**: **mérleg** balance
  sheet + **eredménykimutatás** income statement) are **free to view** with no registration, and the portal
  exposes **structured key figures** (net sales revenue, profit after tax, total assets, equity, liabilities)
  plus the full statements as **PDF** and an **electronic form (XML)**. Coverage is high (mandatory e-filing;
  non-filing leads to tax-number cancellation/forced dissolution).
- **Access.** The search endpoint (`/Search/Results`, fields firmName/firmNumber/firmTaxNumber) is
  **reCAPTCHA-protected** (verified `{"errorText":"A reCaptcha kitöltése nem megfelelő."}`). Automated/bulk
  access is **blocked and must not be bypassed**; manual viewing is free. Structured financials at scale need a
  commercial provider.
- **Currency** HUF (some entities EUR). Join on cégjegyzékszám / adószám (8-digit base) + report year.
