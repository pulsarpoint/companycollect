# ГФО — Annual Financial Statements — Field Catalog

> **Public but DOCUMENT-based.** ГФО are filed to the Commercial Register and public by 30 June, but as
> **filed PDF/scanned documents** — **NOT** structured open data (no XBRL/CSV). Figures (баланс + ОПР) are
> inside the PDF → require OCR/parsing or a commercial provider. Fields documented; no values copied.

## Source Summary

- Country: Bulgaria
- Source type: official_financial_disclosure
- Organization: Агенция по вписванията (Registry Agency)
- URL: https://portal.registryagency.bg/CR/en (per-company documents)
- License: public (filed in the register)
- Access: public (per-company documents)
- Freshness: annual; public by 30 June
- Record shape: per-company per-year **PDF document** (баланс + ОПР inside)
- Primary keys: `eik + otcheten_period`
- Join keys: `eik`

## Fields

| Path | Source field (BG) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| eik | ЕИК | Company id | string | identifier | clean join |
| otcheten_period | отчетен период | Fiscal year | date | date | per-statement key |
| kategoriya | категория предприятие | микро/малко/средно/голямо | string | filing | drives nullability |
| balans.obshto_aktivi | обща сума на активите | Total assets | decimal | financial | PDF → OCR |
| balans.dalgotrayni_aktivi | дълготрайни активи | Fixed assets | decimal | financial | PDF |
| balans.sobstven_kapital | собствен капитал | Equity | decimal | financial | PDF |
| balans.zadalzheniya | задължения | Liabilities | decimal | financial | PDF |
| opr.neto_prihodi | нетни приходи от продажби | Revenue | decimal | financial | PDF; reduced for micro |
| opr.pechalba_zaguba | печалба/загуба | Net income | decimal | financial | PDF; neg = loss |
| prilozhenie.sreden_personal | среден брой персонал | Avg employees | integer | employment | PDF; may be absent |

## Interpretation Notes

- **The financial source — but document-based.** ГФО are **public** in the register (by 30 June) yet are
  **PDF/scanned filings**, not structured open data (no XBRL, unlike Belgium/Poland). Extracting figures
  needs **OCR/PDF parsing**, or a **commercial provider** (CompanyBook/APIS) that already parses баланс +
  ОПР (typically 2022+). This is the main weakness for Bulgarian financials.
- **Trigger**: a register publication of type **"обявяване на ГФО"** signals a new filing → fetch the doc.
- **Size category** (микро/малко/средно/голямо) limits disclosure → revenue/employees nullable for micro.
  Currency **BGN** (EUR from 2026). **Clean join on EIK** to the spine.
- No `sample_record.json` — documents are PDFs; not retrieved.
