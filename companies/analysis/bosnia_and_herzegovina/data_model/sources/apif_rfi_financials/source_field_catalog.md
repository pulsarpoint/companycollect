# APIF RFI / FIA Financial Statements Field Catalog (PLANNING-ONLY)

## Source Summary

- Country: Bosnia and Herzegovina (RS via APIF; FBiH via FIA)
- Source type: financial_statements
- Organization: APIF (Republika Srpska) / FIA (Federation of BiH)
- URL: https://www.apif.net/ (RFI) ; https://fia.ba/ (FBiH)
- License: paid per company
- Access: public but paid (naknada in KM)
- Freshness: annual
- Record shape: per-company paid financial report (planning-only)
- Primary keys: JIB
- Join keys: JIB

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| fiscal_year | Godina / Period | Fiscal year | string | date |  | planning-only |
| bilans_stanja.aktiva | Aktiva (ukupna) | Total assets | decimal | financial |  | BAM |
| bilans_stanja.kapital | Kapital | Equity | decimal | financial |  | BAM |
| bilans_stanja.obaveze | Obaveze | Liabilities | decimal | financial |  | BAM |
| bilans_uspjeha.poslovni_prihodi | Poslovni prihodi | Operating revenue | decimal | financial |  | BAM |
| bilans_uspjeha.neto_rezultat | Neto dobit/gubitak | Net result | decimal | financial |  | BAM |
| broj_zaposlenih | Broj zaposlenih | Employees | integer | employment |  | planning-only |
| bonitet | Bonitet / ocjena | Creditworthiness | string | financial |  | separate paid product |

## Interpretation Notes

- All companies file annual financial statements: **bilans stanja** (balance
  sheet) and **bilans uspjeha** (income statement). In **Republika Srpska** these
  are held by **APIF — Registar finansijskih izvještaja (RFI)**; in the
  **Federation of BiH** by **FIA — Financijsko-informatička agencija**.
- Access is **per company for a fee** (naknada in KM). There is **no open bulk
  financial dataset**, so all fields here are **PLANNING-ONLY**, derived from
  public descriptions of the standard BiH financial-statement forms — **no raw
  values are copied** into this repo.
- **Currency** is **BAM (Konvertibilna marka, KM)**. Reporting follows BiH
  accounting forms (RS/FBiH variants).
- **Join key** is JIB, so financials attach to the court-register identity.
- A separate **Registar boniteta** (creditworthiness) product exists at APIF.
- Implementation is blocked on **payment**; treat as a future paid enrichment.
