# FINA RGFI — Annual Financial Statements — Field Catalog

> **OPEN, structured.** Annual accounts (balance sheet + income statement, abbreviated; notes) published as
> **machine-readable CSV** under the **Otvorena dozvola**, free after a **FINA login**. Fields from the RGFI
> standard forms; no per-company CSV was downloadable here (login required) → no sample.

## Source Summary

- Country: Croatia
- Source type: official_financial_disclosure
- Organization: FINA (Financijska agencija)
- URL: http://rgfi.fina.hr/JavnaObjava-web ; data.gov.hr CKAN dataset
- License: **Otvorena dozvola** (fuller FINA products paid)
- Access: public + **free registration/login**
- Freshness: annual
- Record shape: per-company per-razdoblje CSV (bilanca + RDG)
- Primary keys: `oib + razdoblje`
- Join keys: `oib`

## Fields

| Path | Source field (HR) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| oib | OIB | Company id | string | identifier | clean join |
| razdoblje | razdoblje/godina | Fiscal year | integer | date | per-statement key |
| velicina | veličina | mikro/mali/srednji/veliki | string | filing | drives nullability |
| bilanca.ukupna_aktiva | ukupna aktiva | Total assets | decimal | financial | EUR |
| bilanca.dugotrajna_imovina | dugotrajna imovina | Fixed assets | decimal | financial | |
| bilanca.kratkotrajna_imovina | kratkotrajna imovina | Current assets | decimal | financial | |
| bilanca.kapital_i_rezerve | kapital i rezerve | Equity | decimal | financial | |
| bilanca.obveze | obveze | Liabilities | decimal | financial | |
| rdg.ukupni_prihodi | ukupni prihodi | Revenue | decimal | financial | primary revenue |
| rdg.poslovni_rezultat | poslovni rezultat | Operating result | decimal | financial | |
| rdg.dobit_gubitak_razdoblja | dobit/gubitak razdoblja | Net income | decimal | financial | neg = loss |
| broj_zaposlenih | prosječan broj zaposlenih | Avg employees | integer | employment | may be absent (micro) |

## Interpretation Notes

- **Open structured financials** — full **bilanca** (balance sheet) + **račun dobiti i gubitka** (income
  statement), as **CSV**, joined on the **OIB** (= the Sudski registar key → **clean join**, no fuzzy
  matching). A strong open-financials story (Belgium/Poland tier).
- **Size category** (mikro/mali/srednji/veliki) drives disclosure; **micro/small** file **abbreviated** forms
  → some lines nullable. The open CSV is highlighted for **micro/small**; confirm whether large companies'
  fuller data needs FINA's **paid** RGFI product.
- **Currency**: **EUR since 2023-01-01** (HRK before) — watch the boundary year.
- **RGFI standard form**: figures map to fixed **AOP** positions — stable parsing.
- No `sample_record.json` (CSV behind a free FINA login; not downloaded).
