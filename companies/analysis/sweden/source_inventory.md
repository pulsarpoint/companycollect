# Sweden — source inventory

| # | Source | Type | Access | Formats | Financials | Status |
|---|---|---|---|---|---|---|
| 1 | **Bolagsverket legal-register bulk file** | Official registry bulk | Public direct ZIP, refreshed about every 7 days | ZIP, CSV-like text | No | **recommended primary company source** |
| 2 | **SCB/FDB bulk file via Bolagsverket high-value datasets** | Statistical business register bulk | Public direct ZIP, refreshed about every 7 days | ZIP, tab-separated text | No | **recommended secondary/company universe source** |
| 3 | **Bolagsverket annual-report archives** | Official filings bulk | Public directory of ZIP archives | ZIP, nested ZIP, XHTML/iXBRL | **Yes** | **recommended primary financial source** |
| 4 | Bolagsverket Värdefulla datamängder API | Official API | Authenticated; requires registration/EU identity documents/eID | JSON, ZIP, iXBRL | Yes | fallback/enrichment only |
| 5 | dataportal.se / Bolagsverket source pages | Catalog/documentation | Public browser pages | HTML/DCAT metadata | No | useful_secondary_source |
| 6 | Verklig huvudman (UBO) | Official registry | Restricted | unknown | No | out of scope |
| 7 | Commercial aggregators | Third-party | Paid/keyed | JSON/PDF/iXBRL | sometimes | fallback/comparison only |

## Recommended direct URLs

```text
https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip
https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip
https://vardefulla-datamangder.bolagsverket.se/arsredovisningar/
```

## Local observed files

```text
data_model/bolagsverket_bulkfil.txt
data_model/scb_bulkfil_JE_20260629T055245_80.txt
data_model/01_1.zip
data_model/annual_reports_01_1/
```

## Recommendation

Build the Sweden ingestion from public bulk files:

```text
company bulk ZIPs + annual-report ZIPs -> raw object storage -> parser -> normalized tables -> ClickHouse
```

Do not build the first version around the authenticated API. The API is useful later for targeted
refresh/enrichment if credentials are available, but the public bulk files are simpler, cheaper, and
better aligned with batch ingestion.
