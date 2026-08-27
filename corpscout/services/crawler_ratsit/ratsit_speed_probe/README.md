# Ratsit speed probe

Small standalone experiment for measuring sequential Ratsit requests until the
first rate-limit response. It has no Temporal, ClickHouse, S3, or production
crawler dependencies.

The probe launches its own CloakBrowser on loopback port `9245`, connects one
Scrapling session over CDP, and requests companies one at a time. Scrapling is
configured for one total attempt, so the first HTTP 429 is recorded rather than
hidden by a retry. The run also stops on 401, 403, 5xx, unexpected 4xx, or a
fetch error.

## Prepare

Use Python 3.14 and install this package in its own virtual environment:

```bash
cd services/crawler_ratsit/ratsit_speed_probe
uv sync
```

Prepare either a newline-delimited file:

```text
5562434182
195562434182
```

or a JSON array of strings. Both 10- and 12-digit IDs are accepted. A 12-digit
ID is normalized to its final 10 digits for the Ratsit URL. Duplicate IDs and
IDs that resolve to the same Ratsit URL are rejected before the browser starts.
Use `-` as the input path to read newline-delimited IDs from standard input.

Before measuring, stop the production worker so it does not generate competing
Ratsit traffic:

```bash
systemctl --user stop ratsit-process
```

The production service owns its browser contexts, so stopping it also removes
all competing production Ratsit browser traffic.

## Run

Run up to 5,000 sequential requests with no artificial delay:

```bash
uv run ratsit-speed-probe companies.txt \
  --limit 5000 \
  --output results/no-proxy.jsonl
```

Use `--headed` to show the browser or `--disable-resources` to skip images,
fonts, stylesheets, media, and other nonessential browser resources. Add a fixed
delay with `--delay-ms`; the default is zero.

For a controlled proxy comparison, run the same input as a separate experiment
and use a different output file. Prefer the environment variable so proxy
credentials do not appear in the shell process list:

```bash
RATSIT_SPEED_PROBE_PROXY=http://user:password@proxy-host:port \
  uv run ratsit-speed-probe companies.txt \
  --limit 5000 \
  --output results/single-proxy.jsonl
```

This probe accepts one proxy for the full run; it does not rotate proxies.

## Output

The JSONL file contains one durable row per attempted company, including the
HTTP status, duration, content size, selector presence, and `Retry-After` value.
Rows are flushed after every request, so completed measurements survive an
interrupted run.

A sibling `*.summary.json` file records:

- attempted requests and stop reason;
- the sequence number of the first 429;
- status counts;
- elapsed time and requests per second.

Existing result files are never overwritten.

## Test

```bash
uv run pytest -q
```

Tests use fake responses and never contact Ratsit. A live run must always be
started explicitly with the command above.
