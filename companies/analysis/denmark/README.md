# Company data sources for Denmark

## Status

- Official bulk data: not publicly available without system-to-system credentials
- Official API: found, but requires Danish Business Authority credentials
- Public lookup UI: found at `https://datacvr.virk.dk/`
- Third-party API: found at `https://cvrapi.dk/`, with 50 free lookups/day and higher-volume access by arrangement
- License/terms: public CVR data is available for lookup; redistribution, advertising use, and protected-company handling need legal review
- Recommended ingestion path: use `cvrapi.dk` for low-volume seeded lookup if its terms fit; obtain official API credentials for systematic ingestion

## Best source

The authoritative source is Denmark's Central Business Register (CVR), operated by the Danish Business Authority. The public site exposes search and detail lookup URLs, but it is Cloudflare-protected and robots.txt defines a 10-second crawl delay. The official Elasticsearch-style API is the correct source for systematic collection, but it requires credentials.

For no-auth operation, `cvrapi.dk` is easier than crawling DataCVR because it exposes a documented JSON/XML endpoint. It is still a third-party service, has a stated free quota of 50 lookups/day, requires a meaningful User-Agent, and can block users for bad behavior.

## Next action

If authentication is unavailable, build a conservative lookup-only collector around known CVR numbers or known company names. Try `cvrapi.dk` first for low volume, then DataCVR public lookup only as a fallback. Cache all lookups indefinitely, stop on quota/block responses, and do not attempt exhaustive discovery.
