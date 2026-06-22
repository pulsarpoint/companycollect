# wappalyzer-service

Thin HTTP sidecar wrapping [`projectdiscovery/wappalyzergo`](https://github.com/projectdiscovery/wappalyzergo)
for the CommonCrawl pipeline. The Python pipeline POSTs batches of `{id, headers, body}`
extracted from WARC response records; the service runs wappalyzergo (headers + body
matching, no JS) and returns detected technologies with categories and version.

Using wappalyzergo keeps the fingerprint set identical to `pulsarprotectrunner2` and
updatable in one place.

## Run

```bash
go build -o wappalyzer-service .
WAPPALYZER_ADDR=:9876 ./wappalyzer-service
```

Point the pipeline at it: `COMMONCRAWL_WAPPALYZER_URL=http://localhost:9876`.

## API

`POST /analyze`
```json
{"items": [{"id": "p1", "headers": {"Server": ["nginx"]}, "body": "<html>..."}]}
```
```json
{"results": [{"id": "p1", "technologies": [
  {"name": "Nginx", "categories": ["Web servers"], "version": "", "confidence": 100}
]}]}
```

`GET /health` → `{"status":"ok"}`

## Updating signatures

The fingerprint set is embedded in wappalyzergo. To update to the latest:

```bash
go get -u github.com/projectdiscovery/wappalyzergo@latest && go mod tidy
```

Or, without bumping the module, point at a fresher fingerprints file fetched from GitHub:

```bash
WAPPALYZER_FINGERPRINTS_FILE=/path/to/fingerprints.json ./wappalyzer-service
```

## Env

- `WAPPALYZER_ADDR` — listen address (default `:9876`)
- `WAPPALYZER_FINGERPRINTS_FILE` — optional external fingerprints file (supersedes embedded)
