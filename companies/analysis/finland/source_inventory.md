# Finland — Source Inventory

| Source | Type | Access | Format | License | Freshness | Status |
|---|---|---|---|---|---|---|
| **PRH Open Data — YTJ API v3** | Official registry API | Public, no auth | JSON | CC-BY-4.0 | Daily | **recommended** |
| avoindata.suomi.fi dataset `yritykset` | Open data catalog | Public (CKAN API) | JSON | CC-BY-4.0 | Daily | useful_secondary |
| avoindata.suomi.fi dataset `prh-avoin-data` | Open data catalog | Public (CKAN API) | JSON | CC-BY-4.0 | Daily | useful_secondary |
| PRH digital financial statements API | Official registry API | Public, no auth | JSON | CC-BY-4.0 | Daily | useful_secondary |
| Legacy `full_prh_data.csv` dump | Portal resource | — | CSV | CC-BY-4.0 | was monthly | **unavailable (404)** |

## Primary endpoint

```
GET https://avoindata.prh.fi/opendata-ytj-api/v3/companies
```

| Parameter | Effect | Verified |
|---|---|---|
| `totalResults=true` | adds `totalResults` count to response | yes → 819,096 |
| `page=N` | page N of results, 100 records/page | yes |
| `businessId=NNNNNNN-N` | single company lookup by Y-tunnus | yes |
| `maxResults` | **ignored** — page size fixed at 100 | yes |

Full backfill ≈ `page=1 … 8191`.

## Key facts

- **Publisher:** Finnish Patent and Registration Office (PRH), jointly with the Tax
  Administration via the Business Information System (YTJ).
- **Auth:** none. **Cost:** free.
- **License:** Creative Commons Attribution 4.0 — attribution required, redistribution OK.
- **Excluded:** sole traders (*toiminimi*), email, phone, municipalities, wellbeing
  services counties, tax partnerships.
- **Portal note:** old `avoindata.fi` redirects to `avoindata.suomi.fi`; human pages
  403 to bots — use the CKAN action API for metadata.
