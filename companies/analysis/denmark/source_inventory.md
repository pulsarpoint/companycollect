# Denmark source inventory

| Source | Type | Access | Format | Recommendation |
|---|---|---|---|---|
| CVR public lookup at `datacvr.virk.dk` | Official registry UI | Public lookup, Cloudflare protected | HTML / app data | Sparse seeded lookup only |
| CVR system-to-system API at `distribution.virk.dk/cvr-permanent` | Official registry API | Requires credentials | JSON / Elasticsearch | Best option if credentials are available |
| CVR API at `cvrapi.dk` | Third-party API | Public low-volume lookup, token by arrangement | JSON / XML | Best no-auth option for low-volume seeded lookup |

## Recommendation

With no authentication, the easiest defensible crawl is not a crawl in the bulk sense. It is a cache-first lookup service for known CVR numbers and occasional name resolution. Use `cvrapi.dk` first when the expected volume stays within its quota and terms; use DataCVR public pages only as fallback; use the official API if credentials become available.
