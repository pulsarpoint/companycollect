# dns_detect slice 4: service, storage, history and the Dagster asset

Status: DRAFT for owner review (2026-09-28)
Builds on: `docs/superpowers/specs/2026-09-28-dns-detect-service-design.md`
(revision 2) with slices 0–3 merged: a per-record resolver for NS, SOA, MX,
CNAME, TXT, SPF, DKIM, DMARC and A/AAAA.
Supersedes: that spec's "Running at scale" section (a worker pulling from
ClickHouse) and the parked SQL plan
`services/dagster_v3/docs/superpowers/plans/2026-09-28-domain-services-detection.md`.

## Owner decisions (2026-09-28)

- **Keep it simple.** Hash partitions, batches of records to the resolver,
  results into a table. There is no scale harness and no bespoke machinery.
- **Stateless service.** The resolver has no local store and no durable
  batches: its work is microseconds per record, deterministic, and free to
  redo.
- **Dagster pushes.** Dagster sends batches inline and the service never reads
  ClickHouse. This is the shared-queue contract of 2026-09-24: a batch is only
  a transport envelope, and a pull model was rejected for webtech.
- **Runs on the Dagster machine first.** The initial scan uses the Dagster
  host's spare resources. Moving the service later to another machine is an
  inventory and URL change.

## The service: `dns-detect serve`

It is the same binary as the CLI (`cmd/dns-detect`), started with `serve`.

**Knowledge:**
- Loaded from the provider-recon bucket (`providers/*/latest.json`) at
  startup.
- Reloaded when the bucket's `changes/index.json` changes, polled every 10
  minutes.
- A failed reload keeps the loaded knowledge and is reported in `/healthz`.

**Endpoints:**

| Endpoint | Does |
|---|---|
| `GET /healthz` | `{"ok": true}` plus the loaded versions and the time of the last reload or reload error |
| `GET /v1/knowledge` | `{"rules_version", "ip_version", "documents", "loaded_at"}` |
| `POST /v1/resolve` | Body: records as NDJSON (the CLI's input format), at most 50,000 per request. Response: NDJSON, one line per record in input order (`record_id`, `results`, `findings`), with both versions in response headers. A malformed or invalid record gives 400 naming its line, and no partial output. |

**Deployment:**
- **Where:** it runs under systemd on the Dagster host, listening on
  `127.0.0.1:8096` only, and is deployed by provider-recon's Ansible
  (inventory group `dns_detect_hosts`, host `dagster` by its full tailnet
  name).
- **Limits:** `MemoryMax=4G`, `CPUQuota=400%` and `GOMAXPROCS=4`, so the scan
  can never starve Dagster (the host has wedged under memory pressure before).
- **Credentials:** the same S3 credentials as provider-recon, from an env file
  owned by root.
- **Moving later:** change the inventory host and the Dagster setting
  `DNS_DETECT_API_URL`.

## Versions

`knowledge.Version()` is split in two:

- **`rules_version`:** services, keys and DNS rules.
- **`ip_version`:** IP range instances with their windows.

A stored resolution is **stale** when its `rules_version` differs from the
current one. For records the `ip` and `spf` analyzers handle (A, AAAA and SPF
TXT), it is also stale when its `ip_version` differs.

Routine range churn (AWS, Google and Microsoft several times a week) therefore
re-resolves only A/AAAA and SPF records. A definition change re-resolves
everything, which is intended.

## Storage (migration 000467)

The number is re-checked against main and the prod ledger before merge:
000461–000464 and 000466 belong to another session.

**`dns_record_resolutions`** has one row per resolution of a record, including
records that give nothing, so they are never re-sent:

```
record_id FixedString(16), root_domain String, record_name String,
record_type LowCardinality(String), analyzer LowCardinality(String),
rules_version LowCardinality(String), ip_version LowCardinality(String),
result_count UInt16, findings Array(Tuple(code LowCardinality(String), detail String)),
resolved_at DateTime64(3, 'UTC')
ENGINE = ReplacingMergeTree(resolved_at)
PARTITION BY cityHash64(root_domain) % 128
ORDER BY (root_domain, record_id)
```

**`dns_record_services`** has one row per result:

```
record_id FixedString(16), root_domain String, record_name String,
record_type LowCardinality(String), analyzer LowCardinality(String), subject String,
service_type LowCardinality(String), provider_key String, provider_slug LowCardinality(String),
service_key LowCardinality(String), rule_id String, confidence Float32, fallback UInt8,
valid_from Date, valid_to Date, resolved_at DateTime64(3, 'UTC')
ENGINE = MergeTree
PARTITION BY cityHash64(root_domain) % 128
ORDER BY (root_domain, service_type, provider_key, record_id, valid_from)
```

- **Windows are always dated.** Every stored record has dates, and IP pieces
  are cut inside the record's window.
- **No deletes.** A re-resolved record gets a new resolution and new result
  rows. Its older rows stay physically but are hidden: the views keep only
  result rows whose `resolved_at` equals the record's latest resolution. This
  is the "versions, not deletes" rule of the queue contract.
- **Compaction.** A partition can be compacted later by rewriting it through
  a stage table and `REPLACE PARTITION`.

**Views:**
- **`dns_record_services_current`:** result rows of each record's latest
  resolution.
- **`domain_services_history`:** per `(root_domain, service_type,
  provider_key)`, intervals `first_seen → last_seen`, built from `_current`:
  - `fallback` rows are dropped wherever a non-fallback row of the same domain
    and service type overlaps their window (SOA versus NS);
  - windows of the same service that overlap or are less than 45 days apart
    are merged (scans run every 2–4 weeks);
  - each interval carries its evidence count, the analyzers involved and the
    maximum confidence.
- **`domain_services_now`:** the history intervals whose `last_seen` is the
  domain's latest scan date for that record type.

## The Dagster asset: `dns_record_services_clickhouse`

- **Partitions:** 128 static (`hash_000`…`hash_127`), refining the DNS store's
  `% 16` partitions. Pool `dns_detect`, limit 1.
- **Per partition:**
  1. Read the service's versions (`GET /v1/knowledge`).
  2. Stream the partition's **routable, unresolved or stale** records in
     `record_id` order.
     - A SQL pre-filter keeps only names and types the resolver routes: apex,
       `www`, `_…` names and `*._domainkey`, and the types NS, SOA, MX, CNAME,
       TXT, A and AAAA.
     - An anti-join then drops records already resolved under the current
       versions.
  3. For each chunk of up to 50,000 records: `POST /v1/resolve`, then insert
     its resolutions and results in acknowledged batches, never one row per
     insert.
  4. Record counts (records, results, findings, stale versus new) as metadata.
- **Re-runs are safe.** A failed partition is re-run; an interrupted chunk
  just repeats its records, and the newest resolution wins in the views.
- **Scheduling** is server-side only, and both start STOPPED until the first
  full run is verified:
  - **A sensor** polls the service's versions. When `rules_version` or
    `ip_version` changes, it queues all 128 partitions; the incremental
    selection keeps an IP-only change cheap. It skips while a previous
    refresh is queued or running.
  - **A daily schedule** picks up new DNS scans.

## Initial scan (estimate)

- **Volume:** about 4.4M routable records per partition, so about 560M in
  total (measured on bucket 3).
- **Throughput:**
  - Resolving costs microseconds per record; JSON encoding and HTTP transfer
    dominate.
  - At the design target of 50–100k records/s the full scan takes 2–3 hours.
  - The ClickHouse ↔ Dagster transfer is tens of GB over the tailnet, once.
- **Measure first:** one partition is timed before the other 127 are queued.

## Testing

- **Service:**
  - handler tests with an in-memory knowledge base: resolve round trip,
    record order, the 400 on a bad line, the size limit, the versions in
    headers;
  - reload keeps the old knowledge on failure.
- **Versions:** a rules change moves only `rules_version`, a range change only
  `ip_version`.
- **Migration and views:** clickhouse-local tests with fixture rows cover
  latest-resolution filtering, fallback suppression, the 45-day merge (and a
  real gap kept), and `domain_services_now`.
- **Asset:**
  - a fake client and a fake service check the selection SQL (routable
    prefilter, stale versions), chunking, batch inserts and metadata;
  - the sensor and schedule are tested like the provider-recon ones.
- **Rollout:** deploy, one partition end to end, a spot check of known domains
  in the history view, then all 128.

## Out of scope

- **Backoffice pages** (domain services, provider pages, unmapped keys): the
  next spec.
- **`technology_domains`** (page software).
- **Dropping the old `technology_*` tables.**
- **Owner labels for addresses outside every provider range:** the
  IP-enrichment track.
