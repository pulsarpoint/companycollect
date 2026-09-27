# Local Qwen concurrency pilot — 27 September 2026

Start with **one in-flight request to the local Qwen endpoint**, shared across
crawls. Keep **two normal website workers** so fetching and model analysis can
overlap. This is an initial operating recommendation from a small pilot, not a
universal maximum for Qwen or other LLMs. No production configuration was changed.

## Measured results

The same four saved company-identity extraction requests (Addtech, JM, Neobo and
Arise) were replayed at concurrency 1, 2 and 4. Each level used 25,824 input tokens
in total; individual prompts ranged from 4,038 to 10,641 tokens. There were no
browser visits, database searches or association writes.

| Concurrent requests | Supported identities returned | Batch time | Median client time | Longest client time | Supported extractions/minute | Returned output tokens/second |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 4/4 | 170.2 s | 42.7 s | 54.7 s | 1.41 | 14.22 |
| 2 | 4/4 | 152.4 s | 75.7 s | 101.5 s | 1.58 | 14.22 |
| 4 | 2/4 | 180.0 s | 166.7 s* | 180.0 s* | 0.67 | 5.54* |

\* Two concurrency-four requests timed out at the normal 180-second model
deadline. The median includes those deadline-capped waits; their true completion
latencies are unknown. Output-token throughput includes only completed responses,
not discarded work on timed-out requests. No larger concurrency was tested.

Concurrency two increased completed requests/minute by 11.7%, but returned 2,166 output
tokens versus 2,420 at concurrency one. Aggregate returned-token throughput was
effectively identical. It increased median request latency by 77%, with no
measured token-capacity gain. At concurrency four, Addtech and Arise timed out;
JM returned in 103.0 seconds and Neobo in 153.5 seconds.

All ten completed responses parsed through the production identity validator
and retained a quoted legal name or organisation number agreeing with the
previously supported identity. This verifies extraction evidence, not a fresh
end-to-end company match or a complete accuracy evaluation.

## Runtime and controls

- One NVIDIA GB10; Atlas Spark serving `nvidia/Qwen3.8-27B-NVFP4`.
- Observed launch settings: `--max-batch-size 8`, `--max-seq-len 32768`,
  `--kv-cache-dtype fp8`, `--gpu-memory-utilization 0.85`, FIFO scheduling,
  variable-length prefill batching, prefix caching, speculative decoding with
  three drafts, thinking disabled.
- Original saved requests were preserved, including JSON-schema output and
  `max_tokens: 65536`. This pilot did not tune output budgets or sampling.
- Same four requests in the same order at each level, no retries, one sweep.
  Existing prefix caches were retained, so this is not a cold-cache comparison.
  Output lengths varied; completed requests per minute alone would overstate
  the advantage at concurrency two.
- Atlas reported zero active requests before every level. Its request counter
  increased by exactly four at every level, with no additional requests observed.
- After the test Atlas reported healthy/ready and zero active requests.
- These are **LLM extraction rates**, not complete websites per minute. Full
  lookups also fetch pages, classify the site, query the registry and assess the
  candidates/industries. Hosted models and Jev need their own measurements.

## Application changes recommended next

1. Add `max_parallel_requests` to saved LLM configuration and enforce it across
   workers using the same endpoint/model. Profiles pointing to the same local
   model must share capacity. A semaphore inside each crawl is insufficient.
   Begin with one for this Qwen deployment; maintain a separate pool for Jev.
2. Keep normal crawler workers at two initially. The live browser service had six
   slots, four occupied and two available during inspection. Manual retries have
   their own two-worker queue and must honor the same model limit.
3. Release the browser lease after page collection, then perform extraction and
   matching from saved page text. Currently `open_browser` closes only the site
   tab; the outer service lease remains reserved through the model and database
   work. Freeing that lease enables fetching the next site while the previous
   site's text waits for the model.
4. Use a bounded pending-model queue, and log queue wait separately from model
   response time. Track valid extractions/minute, input/output tokens, time to
   first token, request duration, timeouts, HTTP 429/503 and invalid/unsupported
   output. Keep queue wait outside the model's 180-second execution deadline.
5. Before raising the limit, repeat a larger representative workload including
   long outputs, check tail latency and evidence quality, and compare useful
   throughput. The server's configured maximum batch size is not a recommended
   client concurrency. Give cloud endpoints separate limits based on measured
   behavior and their request/token quotas.

## Reproduction and data

Run from `services/crawler_service` using the existing private saved-model
envelope. The output directory must be new:

```sh
uv run --no-sync python benchmarks/company_lookup_local_20260927/concurrency.py \
  --output data/company-lookup-concurrency-new-run
```

The runner stops before a level if other requests are active, and stops after a
level on transport failure or insufficient timeout headroom. It never changes
server settings or clears caches. Run against an otherwise idle model endpoint.

Local artifacts: `data/company-lookup-concurrency-20260927/`. This contains the
manifest, runtime settings, request/response JSON, per-request timings, phase
metrics and final health. Request headers and API keys are never saved.

Method references: Atlas documents measuring concurrency alongside throughput
and latency in its [benchmark harness](https://docs.atlasinference.io/crates/atlas-spark-bench.html).
Its [scheduler documentation](https://docs.atlasinference.io/crates/spark-server.html)
describes the shared decode batches and KV budget; those mechanisms explain why
browser-worker counts should not serve as model-capacity limits. This pilot does
not identify which Atlas kernel or scheduler behavior caused the observed slowdown.
