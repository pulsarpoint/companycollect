# Croatia — Source Inventory

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| **Sudski registar API** | Official registry API | Free + registration (key) | JSON | Otvorena dozvola | **recommended** (open spine) |
| **FINA RGFI (javna objava)** | Official financials | Free + registration (login) | **CSV** | Otvorena dozvola | **recommended** (open **structured financials**) |
| Sudski registar (web search) | Official registry search | Free web | HTML | Free | useful secondary (lookup) |
| data.gov.hr (CKAN) | Open data portal | Free | CSV/JSON | Otvorena dozvola | useful secondary (discovery) |
| Registar stvarnih vlasnika (RSV) | Beneficial ownership | Restricted | PDF | Restricted | blocked by authentication |
| DZS (statistics) | Statistical register | Free | XLSX/CSV | DZS reuse | useful secondary (aggregate) |
| Commercial aggregators (Bisnode, Companywall) | Commercial API | Paid | JSON | Commercial | useful secondary |

## Access points

- Sudski registar API: https://sudreg-data.gov.hr (free registration → Client ID/Secret + Ocp-Apim-Subscription-Key); docs https://sudreg-podaci.pravosudje.hr/docs/services ; web search https://sudreg.pravosudje.hr
- FINA RGFI javna objava: http://rgfi.fina.hr/JavnaObjava-web (free login; CSV) ; CKAN dataset on data.gov.hr
- National portal: https://data.gov.hr/ (CKAN; Otvorena dozvola)
- Beneficial ownership (restricted): RSV (FINA)

## Key facts

- **Open registry** (Sudski registar API) + **open structured financials** (FINA RGFI CSV: balance sheet +
  income statement) — both under the **Otvorena dozvola**, behind a **free registration/account**.
- **Single key**: OIB (11-digit tax id) = VAT root (`HR` + OIB); MBS (court register number); MB (old).
- RGFI open CSV is noted esp. for **micro/small**; fuller/large data may need the paid FINA product.

See `source_inventory.json` for the machine-readable version.
