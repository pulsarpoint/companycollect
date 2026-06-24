# ГИР БО — GIR BO Field Catalog

## Source Summary

- Country: Russia
- Source type: financial_statements
- Organization: Federal Tax Service (ФНС / FNS)
- URL: https://bo.nalog.gov.ru/advanced-search/organizations/search
- License: open data (FNS)
- Access: public, **no key**
- Freshness: annual filings (continuously updated)
- Record shape: search org (JSON) → `bfo[]` statement list → per-statement forms
- Primary keys: `id` (GIR BO org id)
- Join keys: `inn`, `ogrn`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| content[].id | id | GIR BO org id | integer | identifier | 6622458 | for /bfo/ call |
| content[].inn | inn | ИНН (10-digit) | string | identifier | 7736050003 | tax id; strip `<strong>` |
| content[].ogrn | ogrn | ОГРН (13-digit) | string | identifier | 1027700070518 | company id |
| bfo[].organizationInfo.kpp | kpp | КПП (9-digit) | string | identifier | 997250001 | reason code |
| content[].shortName / fullName | shortName | Name | string | legal_name | ПАО "ГАЗПРОМ" | |
| content[].okopf | okopf | Legal form (ОКОПФ) | object | legal_form | {id,name} | |
| content[].okfs | okfs | Ownership (ОКФС) | object | metadata | {id,name} | |
| content[].okpo | okpo | ОКПО | string | identifier | 00040778 | |
| content[].okved2 | okved2 | ОКВЭД2 activity | string | activity | | ~NACE |
| content[].region… | address | Address | string | address | САНКТ-ПЕТЕРБУРГ | index/region/city/street/house |
| content[].statusCode | statusCode | Status | string | status | | |
| bfo[].period | period | Statement year | string | date | 2025 | available years |
| bfo[].knd | knd | Form code (КНД) | string | document | 0710099 | full accounts |
| statement.current1600 | current1600 | Total assets (RUB) | decimal | financial | | line 1600 |
| statement.current2110/2400 | current2110/2400 | Revenue / net profit (RUB) | decimal | financial | | lines 2110/2400 |

## Interpretation Notes

- **Verified from real data** (no key): **ПАО "ГАЗПРОМ"** (INN 7736050003, OGRN
  1027700070518; financial-statement years **2021–2025**), **ПАО "ЛУКОЙЛ"** (INN
  7708004767, OGRN 1027700035769). Org records carry OKOPF (e.g. "Публичные
  акционерные общества"), OKFS, OKPO, OKVED, region, status.
- **Two-step**: `GET /advanced-search/organizations/search?query={INN|OGRN|name}`
  → org with `id` + `bfo[]` (years); then `GET /nbo/organizations/{id}/bfo/` for the
  statement list. Line-item figures (balance sheet form 1, income statement form 2)
  are in a per-statement detail; codes follow the Russian RAS chart (1600 assets,
  1300 equity, 2110 revenue, 2400 net profit). Currency **RUB** (usually thousands).
- **Join**: `inn` / `ogrn` to RSMP, EGRUL, and the FNS open sets.
- **Coverage**: non-bank, non-budget legal entities that file accounts. **Banks are
  excluded** (they file with the Central Bank).
- Strip `<strong>` tags from `inn` in search results. No personal data in this
  identity/financial layer.
