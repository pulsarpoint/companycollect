# Czech Republic — Source Inventory

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| **ARES REST API** | Official registry (aggregator) | Free | **JSON** | Open data (confirm) | **recommended** (API spine) |
| **Veřejný rejstřík bulk (dataor.justice.cz)** | Official registry | Free | **XML/CSV** (.gz) | Open data (confirm) | **recommended** (deep bulk spine) |
| **Sbírka listin (účetní závěrka)** | Official financials | Free (view) | **PDF** | Public docs | useful secondary (**financials**, PDF) |
| ARES open-data bulk (data.mf.gov.cz) | Official registry | Free | XML/CSV/ZIP | Open data | useful secondary |
| ČSÚ RES | Statistical business register | Free | CSV/XML | Open data | useful secondary (NACE/sector/size) |
| Registr DPH / VIES | Official tax | Free | HTML/SOAP | Validation | useful secondary (DIČ, unreliable-payer) |
| RŽP (Živnostenský rejstřík) | Trade licensing | Free | HTML/XML | Public | useful secondary (licences, OSVČ) |
| NKOD / data.gov.cz | Open data portal | Free | DCAT/CSV/JSON | Per dataset | useful secondary (discovery) |

## Access points

- ARES API: `https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty/{ICO}` ; search `POST .../vyhledat`
- Justice VR bulk: `https://dataor.justice.cz/` (CKAN `/api/3/action/package_list`); files `https://dataor.justice.cz/api/file/{package}.xml.gz` (**302 → use -L**)
- Sbírka listin / financials: `https://or.justice.cz/ias/ui/rejstrik` → company → Sbírka listin (účetní závěrka PDF)
- ARES open data: `https://ares.gov.cz/stranky/otevrena-data` ; `https://data.mf.gov.cz/topics/ares`
- ČSÚ RES: `https://www.czso.cz/csu/res/registr_ekonomickych_subjektu`
- NKOD: `https://data.gov.cz/` (DCAT-AP + SPARQL)

## Key facts

- **Single join key**: **IČO** (8 digits; watch zero-padding). **DIČ** = CZ + IČO (validate via Registr DPH/VIES).
- **Fully open identity + structure**: ARES API + Justice VR bulk (officers with DOB, **shareholders for a.s.**, share capital, boards, insolvency, activity).
- **Financials free but PDF**: účetní závěrka in the Sbírka listin — no official structured/XBRL bulk.
- **Verified live**: ARES (Alza.cz a.s.); Justice CKAN (9,496 packages); downloaded a real 15 MB a.s. Prague dump (~16,758 firms).
- **License**: open but exact terms (CKAN license field empty; ARES terms) to confirm before redistribution. Officer/shareholder DOB = GDPR personal data.

See `source_inventory.json` for the machine-readable version.
