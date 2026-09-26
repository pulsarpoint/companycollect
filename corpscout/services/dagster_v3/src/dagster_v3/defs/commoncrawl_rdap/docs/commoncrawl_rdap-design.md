# CommonCrawl RDAP network enrichment

`commoncrawl_ip_addresses`, `commoncrawl_ip_rdap_networks`, and the GeoIP enrichment asset share
256 static Dagster partitions named `bucket_000` through `bucket_255`. ClickHouse assigns each
canonical IP to `cityHash64(ip) % 256`, so adding an address changes one stable bucket rather than
shifting addresses between buckets. `commoncrawl_ip_rdap_networks` remains manual-only.

The asset checks the ClickHouse `rdap_network_trie` before making a remote request. Successful RDAP
start/end ranges are decomposed into exact CIDRs and inserted in this order:

1. `corpscout.rdap_networks`
2. `corpscout.rdap_network_segments`
3. `corpscout.rdap_ip_lookup_results`

This ordering keeps partial failures resumable. Parent registrations are stored with
`segment_role='parent'`, so they remain queryable but do not suppress direct lookups through the
trie.

The trie represents the most-specific registration discovered so far. It is not proof that a more
specific registration does not exist.

## Manual source observation

`commoncrawl_ip_addresses` is an observable external asset: Dagster represents the ClickHouse
table but does not materialize it. The `observe_commoncrawl_ip_addresses` job reads one selected
bucket from `commoncrawl_ip_addresses FINAL` and records its logical unique-IP count as the bucket's
data version. It emits an observation only; it never launches GeoIP or RDAP enrichment.

The count intentionally excludes `last_seen`. The registry is additive, so its logical row count
changes when a canonical IP first enters the bucket, while repeated DNS sightings only update
`last_seen`. Observing the same count again records the same data version. Observing a changed count
makes only the matching downstream GeoIP and RDAP partitions stale because all three assets share
the same partition definition.

Observation is manual and partition-specific. Dagster cannot know that ClickHouse changed until an
operator observes the relevant bucket:

```bash
uv run dg launch --job observe_commoncrawl_ip_addresses --partition bucket_007
```

In the UI, launch `observe_commoncrawl_ip_addresses` and explicitly select the relevant
`bucket_NNN` partition. One observation run does not refresh all 256 buckets. A manually selected
partition backfill can establish observations for several buckets, with one bucket executed per
run.

Native staleness means source membership changed since the child partition was materialized. The
backlog check below answers the different operational question of whether actionable RDAP work
exists. A new IP can already be covered by the network trie, so a changed source version does not
necessarily mean another RDAP request is needed. Historical child materializations do not have
source-observation provenance. Activate tracking for each RDAP partition by observing its source
bucket and then materializing that child partition once. This can be a one-time manual baseline
across every partition that should be tracked, or it can happen lazily as partitions are next
processed. Until a partition is baselined, the backlog check remains the authoritative update
signal. In Dagster 1.13 the matching child partition can show stale or unsynced in partition
status/details, but the whole-asset graph badge does not aggregate stale partitions.

## Manual update-availability check

`commoncrawl_rdap_update_available` is an unpartitioned, read-only asset check attached to
`commoncrawl_ip_addresses`. An operator evaluates it manually from that asset's Checks view, or
manually launches the `commoncrawl_ip_enrichment_backlog_checks` job. It scans all 256 stable
buckets and reports only buckets containing actionable RDAP work.

Actionability follows the enrichment asset's lookup rules; it is not a comparison of table row
counts. One RDAP network can cover many source IPs. An IP requires work only when the
`rdap_network_trie` does not cover it and it either has no current exact lookup result or has a
`retryable_error` whose retry time is due. Terminal results and retries scheduled for the future do
not count as pending work.

A **Failed** check at WARN severity means manual enrichment work is available, not that a pipeline
run failed. Its metadata separates new uncovered IPs from due retries and includes pending counts,
the actionable `bucket_NNN` partition keys, and `checked_at_utc`. The operator materializes only
the reported `commoncrawl_ip_rdap_networks` partitions and then evaluates the check again:

```bash
uv run dg launch --job commoncrawl_ip_enrichment_backlog_checks
uv run dg launch --job observe_commoncrawl_ip_addresses --partition bucket_007
uv run dg launch --assets commoncrawl_ip_rdap_networks --partition bucket_007
uv run dg launch --job commoncrawl_ip_enrichment_backlog_checks
```

Production operators normally use Dagster's UI; these commands are the local CLI equivalent. A
bucket can remain actionable after one run because `max_requests=250` bounds each materialization,
so the operator may need to materialize it and re-evaluate the check repeatedly. A check result is
a snapshot of the last manual evaluation, and a source observation is a snapshot of its selected
bucket. Neither refreshes when ClickHouse changes. No sensor, schedule, automation condition,
auto-observe interval, or background process evaluates the check, observes the source, or launches
a reported partition.

Migration `000126_corpscout_rdap_dictionary_reader` must be applied after the storage migration.
It creates a passwordless reader restricted to local ClickHouse connections and grants only the
segment table/view reads required by the dictionary. The migration account therefore needs
ClickHouse access-management permission in addition to normal schema DDL permission.

## Safe smoke run

Start with one partition and at most five total RDAP requests, including parent requests:

```bash
uv run dg launch \
  --assets commoncrawl_ip_rdap_networks \
  --partition bucket_000 \
  --config config/commoncrawl_rdap_smoke.yaml
```

After the run, inspect the network, segment, and lookup-result tables plus
`system.dictionaries`. Rerunning the same partition should make no requests for IPs covered by the
newly loaded trie segments.

```sql
SELECT count() FROM corpscout.rdap_networks_current;
SELECT count() FROM corpscout.rdap_network_segments_current;
SELECT count() FROM corpscout.rdap_ip_lookup_results_current;

SELECT name, status, element_count, last_exception
FROM system.dictionaries
WHERE database = 'corpscout' AND name = 'rdap_network_trie';
```

## Registry-level registrations (data-driven, 2026-09)

Both writers of `rdap_networks` — this bucket worker and `ip_enrichment`'s `RdapEnricher` —
classify every direct registration against the IP registry special segments
(`defs/ip_registry`, `docs/operations/ip-registry-reference-data.md`) with
`commoncrawl_rdap/registry.py::classify_registration` (one `REGISTRY_CONTEXT_SQL` round trip per
RDAP miss) and insert its `rdap_network_registry_class` row between the network row and the
segment rows. A `registry_level` (covers a whole RIR-designated IANA block that no holder block —
an allocated/assigned RIR record wide enough to cover one, e.g. Comcast's `73.0.0.0/8` — also
covers entirely) or `unallocated` (first address in available/reserved or IANA-reserved space)
registration is stored, answers the queried address, and is never added to the in-run reuse set;
`rdap_network_segments_current` (migration 000451) excludes such networks from
`rdap_network_trie`, and the daily `rdap_network_registry_class` asset reclassifies everything
from the current snapshots. While the reference data is incomplete the class is `unknown` and
nothing is excluded; a classification query failing is fail-closed (it fails the lookup, in both
writers) rather than silently skipped, and a registration that is not reusable is never cached
per-IP either — see the operations doc's "Known costs" for what that means for repeat lookups of
the same address.

## IP enrichment: page batching and registry budgets

`ip_enrichment_results` (`defs/ip_enrichment/enrichment.py`, `RdapEnricher`) resolves a page of
addresses with a bounded number of ClickHouse round trips: one negative-cache read
(`rdap_ip_lookup_results_current`), one trie `dictGet`, one read of the uncached network rows
the page needs (`rdap_networks_current`, never `raw_response`), and one insert of the page's
lookup markers. Per miss it costs the registry-class context query
(`registry.py::classify_registration`) before the network row, its class row (unless `unknown`)
and its segments, in that order, so coverage and its class are durable before any result refers
to them.

The registry of a miss is chosen from whoisit's already-loaded bootstrap data, with no HTTP
request (`RdapClient.registry_for`); a global address with no exact bootstrap match answers
with a terminal `no_registry` marker (cached for `rdap_cache_days`) instead of a request, since
whoisit would otherwise pick a default endpoint at random.

6to4 (`2002::/16`) and IPv4-mapped (`::ffff:0:0/96`) addresses never reach that point: when the
IPv4 they embed (`ipaddress` `.sixtofour` / `.ipv4_mapped`) is global, `resolve_page` rewrites
the row to that IPv4 before the page's ClickHouse round trips (plus one query for the IPv4s'
buckets, computed in ClickHouse), so the IPv4's marker, network, class row and segments are
written as for any IPv4 and the IPv6 row is answered with the IPv4's fields (no IPv6 marker,
no IPv6 segments for an IPv4 network; the IPv4 is recomputed from `ip`). Their `ip_scope` is
the embedded IPv4's. Teredo (`2001::/32`) stays `not_global` without a request. Counters:
`embedded_ipv4_lookups` by form and `teredo_special`.

**RIPE**, when `ripe_rest` is on, is asked over the RIPE Database REST search instead of RDAP
(`ripe_rest.py`): the AUP caps the personal data sets (person and role objects) one source
address may receive at 1,000 per 24 hours, and a RIPE RDAP `ip` answer embeds 1–5 person
objects, so every one would count. The search sends `flags=no-referenced` together with
`flags=no-personal` (both verified live on 2026-09-26 against `193.0.6.139`, which returned only
an `inetnum` object) and returns the most specific `inetnum`/`inet6num` alone, no person or role
object.

**APNIC**, when `apnic_whois` is on, is asked over port-43 whois with `-r` (`apnic_whois.py`;
the HTTP gateway `wq.apnic.net` does not honour `-r`): the holder name is the *first* `descr`
line, since APNIC objects rarely carry `org:`; `status` is upper-cased and whitespace-collapsed.
An answer that is an NIR's own allocation object (`netname` starting with
`JPNIC`/`KRNIC`/`TWNIC`/`IDNIC`/`CNNIC`/`IRINN`/`VNNIC`, or a first `descr` naming one) falls back
to RDAP, routed by the IANA bootstrap to the NIR's server; `mnt-by` alone never decides this (a
holder's own block, e.g. FPT's `103.35.64.0/22`, is maintained by `MAINT-VN-VNNIC`).

**Cross-RIR redirects** are refused before any fetch, the same way a first URL to
`rdap.db.ripe.net`/`rdap.apnic.net` is refused: ERX/transferred space registered inside another
registry's IANA /8 answers on that registry's own RDAP server with a redirect to RIPE's or
APNIC's RDAP host, and `RdapClient` (`reroute_hosts`) raises `RdapRedirect` instead of following
it, so the target body — and its person objects — is never fetched. The miss is re-sent to the
REST search or whois `-r` and counted under the *target* registry (`reroutes_by_registry`).

**Counters** are keyed by the registry that answered (`RdapLookupResponse.rir`):
`requests_by_registry` and `person_entities_by_registry` (person and role vCards, `kind`
`individual` or `group`, nested included). RDAP requests made because a
REST/whois answer was a catch-all or an NIR's own object are counted separately in
`rdap_fallbacks_by_registry`, with persons under `"<rir>:fallback"` keys — a plain `ripe`/`apnic`
key in `person_entities_by_registry` is a leak, not a fallback. `reroutes_by_registry` and
`pauses_by_registry` (including the run-wide `bootstrap` key) round out the set.

**The optional per-registry daily budget** (`registry_daily_budgets`, default `{}`) defers a miss
of a registry at its limit rather than requesting or failing; the run waits only when a whole
pass resolved nothing else, for an hour's share of the budget (`wait_for_registry_budget`). The
24-hour usage window is seeded on start from `rdap_networks.fetched_at` of the last day, across
every writer — including the legacy bucket worker below — which under-counts requests that
stored no network (errors, not-founds, the redirected half of a reroute), since
`rdap_ip_lookup_results` has no registry column. A RIPE `403`/`429` or an APNIC `%ERROR:2xx`
additionally pauses that registry for `max(retry, 15 min)`, deferring its misses the same way; a
failed IANA bootstrap pauses every miss under the key `bootstrap` with its own 60 s→900 s
back-off. Both waits hold the `commoncrawl_rdap` pool slot.

**The legacy bucket worker** (`commoncrawl_ip_rdap_networks`) keeps its per-IP RDAP lookups,
RIPE and APNIC included, so it does consume personal data sets; it shares the daily budget
window only through the seed above, not through `registry_daily_budgets` itself.

**Known costs and follow-ups:** the remaining/completion query in
`defs/ip_enrichment/results.py` anti-joins by `bucket` (the results table's `ORDER BY (bucket,
ip, result_id)`; `task_id` is not part of that key), so it scans a bucket's whole range across
every task, not just the one being processed — a `task_id` skip index is a candidate follow-up.
A miss deferred by a paused target registry after a cross-RIR redirect (e.g. ARIN → RIPE while
RIPE is paused) repeats the ARIN redirect on the next pass; nothing remembers the reroute across
passes. `RdapClient._lookup` reaches into whoisit's private `_bootstrap` attribute, pinned in
`uv.lock`. The NIR-object rule matches a netname prefix, so a holder's own netname such as
`IDNIC-<HOLDER>-ID` could be misread as an NIR's own allocation; to be verified in the Task 8
smoke run.
