# NBB Central Balance Sheet Office — Annual Accounts (XBRL) — Field Catalog

> **OPEN, structured.** Free annual accounts (~99% XBRL) — full balance sheet + income statement back to
> 2007. Web services need a **free developer account** (Authentic Data free; Improved Data paid); CONSULT
> is free per-entity. Fields from the documented Belgian GAAP XBRL schemas; no per-company sample pulled
> here (account/per-entity) → no sample_record.

## Source Summary

- Country: Belgium
- Source type: official_financial_disclosure
- Organization: NBB/BNB — Centrale des bilans / Balanscentrale
- URL: https://www.nbb.be/en/central-balance-sheet-office ; CONSULT https://consult.cbso.nbb.be/ ; web services https://developer.cbso.nbb.be/
- License: free (Authentic Data); Improved Data paid
- Access: public; free developer account for web services
- Freshness: annual; XBRL since 2007, CSV since 2022, PDF since 1999
- Record shape: per-company per-boekjaar XBRL (+ JSON-from-XBRL since 2022)
- Primary keys: `EnterpriseNumber + boekjaar + schema`
- Join keys: `EnterpriseNumber`

## Fields

| Path | Source field (NL/FR) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| header.EnterpriseNumber | ondernemingsnummer | Company id | string | identifier | clean join to KBO |
| header.boekjaar | boekjaar/exercice | Fiscal year | date | date | per-statement key |
| header.schema | schema/model | micro/abbreviated/full | string | filing | drives nullability |
| balans.totaalActiva | totaal van de activa | Total assets | decimal | financial | EUR |
| balans.vasteActiva | vaste activa | Fixed assets | decimal | financial | |
| balans.vlottendeActiva | vlottende activa | Current assets | decimal | financial | |
| balans.eigenVermogen | eigen vermogen | Equity | decimal | financial | |
| balans.schulden | schulden/dettes | Liabilities | decimal | financial | |
| resultaten.omzet | omzet/chiffre d'affaires | Revenue | decimal | financial | **often absent (micro/abbr.)** |
| resultaten.bedrijfsresultaat | résultat d'exploitation | Operating result | decimal | financial | |
| resultaten.winstVerliesBoekjaar | winst/verlies boekjaar | Net income | decimal | financial | neg = loss |
| toelichting.gemiddeldPersoneel | effectif moyen | Avg employees (FTE) | decimal | employment | social balance |
| header.valuta | valuta/devise | Currency | string | financial | usually EUR |

## Interpretation Notes

- **Best-in-class open financials.** Full structured **XBRL** balance sheet + income statement, free, for
  essentially all Belgian legal entities back to **2007** — joined on the **EnterpriseNumber** (the same key
  as KBO), so company↔financials is a **clean join**, no fuzzy matching. A standout vs DE/AT/IT (paid).
- **Schema drives disclosure**: **micro / abbreviated (verkort/abrégé)** schemas disclose **reduced**
  balance sheets and **often no income statement** → `omzet`/`net_income`/operating result **nullable** for
  many small companies; **full (volledig/complet)** schemas carry the income statement.
- **XBRL specifics**: Belgian GAAP taxonomy, versioned yearly; concepts identified by *rubriek* codes
  (e.g. total assets = rubriek 20/58). Parse with an XBRL toolkit; JSON-from-XBRL available since 2022.
  Multilingual labels (NL/FR). Currency EUR.
- **Free vs paid**: use the free **Authentic Data** (as-filed); **Improved Data** (NBB-rectified) is paid.
- No `sample_record.json` (free developer account / per-entity CONSULT; not pulled here).
