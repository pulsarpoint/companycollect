# Denmark collector scripts

Both official sources are Erhvervsstyrelsen's Elasticsearch distribution at `distribution.virk.dk`
(HTTP only). `downloader.go` (shared template) handles simple GET downloads — fine for the
URI-search examples in `sources.example.json`. For real ingestion use body-based POST queries and
the scroll API.

## 1. Financial filings — Offentliggørelser (OPEN, no auth)

```bash
# Latest filings for a CVR number (newest first)
curl -s -X POST 'http://distribution.virk.dk/offentliggoerelser/_search' \
  -H 'Content-Type: application/json' \
  -d '{"query":{"term":{"cvrNummer":22756214}},
       "sort":[{"offentliggoerelsesTidspunkt":{"order":"desc"}}],"size":5'

# Incremental: everything updated since a timestamp (use scroll for >3000)
curl -s -X POST 'http://distribution.virk.dk/offentliggoerelser/_search?scroll=1m' \
  -H 'Content-Type: application/json' \
  -d '{"size":1000,"query":{"range":{"sidstOpdateret":{"gte":"2026-06-01T00:00:00.000Z"}}}}'
```

Each hit's `dokumenter[].dokumentUrl` points to `regnskaber.virk.dk`. Documents are **gzip-encoded**
even with `Content-Type: text/xml` — decompress, then parse the XBRL (DCCA `fsa`/`gsd`/`cmn` + IFRS).

```bash
curl -s 'http://regnskaber.virk.dk/<id>/<token>.xml' | gunzip > statement.xbrl.xml
```

## 2. Base company data — CVR-permanent (free credentials required)

```bash
# Request credentials first: email cvrselvbetjening@erst.dk (sign protected-data declaration)
CRED=$(printf '%s:%s' "$CVR_USER" "$CVR_PASS" | base64)

# Single company
curl -s -X POST 'http://distribution.virk.dk/cvr-permanent/virksomhed/_search' \
  -H "Authorization: Basic $CRED" -H 'Content-Type: application/json' \
  -d '{"query":{"term":{"cvrNummer":22756214}}}'

# Full extract — scroll API (3000-doc query cap on Elasticsearch 1.7.x)
curl -s -X POST 'http://distribution.virk.dk/cvr-permanent/virksomhed/_search?scroll=5m' \
  -H "Authorization: Basic $CRED" -H 'Content-Type: application/json' \
  -d '{"size":2000,"query":{"match_all":{}}}'
# then repeatedly POST /_search/scroll with the returned _scroll_id
```

Indexes: `virksomhed` (companies), `produktionsenhed` (P-units), `deltager` (participants),
plus `registreringstekster` (registration texts).

## Running the shared downloader

```bash
go run ./downloader.go   # if wired to a CLI; otherwise import CollectSource (see skill template)
```

Suggested registry keys: `denmark/cvr` (base), `denmark/cvrregnskab` (financials).
