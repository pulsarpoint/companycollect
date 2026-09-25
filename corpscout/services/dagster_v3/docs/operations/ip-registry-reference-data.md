# IP registry reference data

Group `ip_registry` loads, daily, the special segments of the IP address space into ClickHouse —
the IANA top-level blocks and the `available`/`reserved` ranges of the five RIRs' delegated-extended
statistics — together with a deliberate superset of `allocated`/`assigned` records wide enough to
cover an entire IANA block (holder blocks, ruling R2: e.g. Comcast's `73.0.0.0/8`; no other
allocated/assigned delegation is stored) — and classifies every cached RDAP
registration (`corpscout.rdap_networks`) as `reusable`, `registry_level` or `unallocated`. Only
reusable registrations feed `rdap_network_trie` (migration 000451), so an RDAP answer such as
`APNIC-AP` (103.0.0.0/8) is stored for the address that was queried and never served to other
addresses.

## Objects

| Object | Meaning |
| --- | --- |
| `ip_registry_snapshots` | ledger: one row per (source, snapshot_date); `verified_at` moves on every re-check; `records_*` count the whole file, `segments_*` the kept special-segment rows, `holders_*` the kept holder-block rows (0 for IANA); the newest row per source is the current snapshot |
| `ip_registry_iana_blocks` / `_current` | IANA ipv4-address-space + ipv6-unicast rows (`rir` derived from the designation, empty for reserved and legacy single-holder blocks) |
| `ip_registry_special_segments` / `_current` | the RIRs' available/reserved ranges with `first_ip`/`last_ip` and `cidrs`; only the current and the previous snapshot are kept |
| `ip_registry_holder_blocks` / `_current` | the RIRs' allocated/assigned records wide enough to cover an entire IANA block (same columns and retention as the special segments; ~112 rows/day, of which today only Comcast 73/8, US DoD 7/8, UK MoD 25/8 and JPNIC 133/8 actually cover one) |
| `ip_registry_special_trie` | `IP_TRIE` dictionary (≈325k CIDRs, 48 MiB): `dictGetOrDefault(..., tuple(toIPv6(x)))` gives the special segment holding `x` |
| `ip_registry_iana_blocks_rule_current` | the current IANA blocks plus `unheld_rir_block` (1 when the block is RIR-designated and no holder block covers it entirely) — the one place the holder exclusion is computed; both the per-miss query and the bulk derived view read it |
| `ip_registry_ready` | `ready = 1` when all seven sources have a current snapshot |
| `rdap_network_registry_class` / `_current` / `_derived` | persisted class per registration / the rule applied live to `rdap_networks_current` |

## Running it

- Job `ip_registry_refresh_job` (seven loaders, then `rdap_network_registry_class`); schedule
  `ip_registry_daily` 06:05 UTC, stopped by default — start it on the Schedules page.
- A loader validates the whole file before writing anything: MD5 mismatch (the body and the
  `.md5` are each refetched once more first; a mismatch that persists still refuses the load),
  malformed version line, record/summary count mismatch, a file older than the current snapshot,
  or a >5% drop in the whole-file ipv4/ipv6 record count all refuse the load (and the class asset
  does not run on a failed loader). For a legitimate drop re-launch with run config
  `ops: ip_registry_special_segments_<rir>: config: {allow_shrink: true}`.
- **Write order never empties a re-published current snapshot.** A new-date load drops that
  date's partition first (it can only hold leftovers of an earlier failed load). A same-date
  republish or repair inserts the new rows next to the old ones, writes the ledger row, and only
  then deletes the rows older than that load from that one partition — so a reader never sees an
  empty or partial current snapshot, only old rows, then old-plus-new, then exactly the new rows.
  If a run dies between the ledger row and that delete, the next run detects the mismatch and
  reloads the same date (`repaired=True` in its metadata); an unchanged, consistent file only
  refreshes `verified_at`. After every load the loader drops the partitions of that source older
  than the previous snapshot (`dropped_snapshots` in the metadata; `SNAPSHOTS_KEPT = 2` — the
  current snapshot and the one before it — across the IANA blocks, special segments and holder
  blocks tables).
- A loader retries the whole download twice, five minutes apart, and identifies itself as
  `User-Agent: CorpScout ip-registry/1.0`.
- Checks: `snapshot_fresh` on each loader (RIR snapshot ≤ 3 days, verified ≤ 2 days; IANA
  verified only), `classification_complete` on the class asset.

## The rule

For a registration N = [first, last]: `registry_level` when N covers at least one entire IANA
block designated to an RIR (`APNIC`, `ARIN`, `RIPE NCC`, `LACNIC`, `AFRINIC`, `Administered
by …`) that no holder block covers entirely; `unallocated` when N's first address lies in an
`available`/`reserved` RIR segment, in an IANA `RESERVED` block, or in no IANA block; else
`reusable`; `unknown` while `ip_registry_ready = 0` (then nothing is excluded and the enrichers
behave as before). Legacy single-holder blocks (Ford 19/8) are reusable; so are holder blocks
themselves (Comcast 73/8) — a holder's own registration is not registry-level.
`commoncrawl_rdap/registry.py` holds the Python rule and the SQL text the derived view embeds;
`tests/test_ip_registry.py` proves their parity.

**If readiness later drops** (all seven sources no longer show a current snapshot — the ledger is
never trimmed, so in practice this only happens in tests that truncate it), the daily
`rdap_network_registry_class` asset refuses to run rather than reclassify anything: it never
writes `unknown` over an existing row. Persisted classes stay exactly as they were last
classified, and `rdap_network_trie` keeps excluding whatever was last classified
`registry_level`/`unallocated`. A brand-new registration written while not ready gets no class
row at all (the enrichers skip persisting an `unknown` classification), so it is not excluded —
the trie treats it as it would have before this feature existed.

**Deploy order matters.** Migration `000451` is what makes the exclusion effective — before it,
`rdap_network_trie` (and an enricher's in-run cache, via trie hits) can still serve
registry-level or unallocated segments even after they are classified. Keep the time between the
first reference load and applying 000451 short: 000450 → code → first load + checks green →
review the excluded-network report → 000451.

**Known costs.**
- Each new direct RDAP registration costs one extra ClickHouse query (`REGISTRY_CONTEXT_SQL`,
  the classification round trip); a failure of that query fails the lookup (fail-closed), the
  same way a failed insert already does.
- IP enrichment has no positive per-IP cache: a "found" answer is served again only through
  `rdap_network_trie` (by CIDR) or the in-run LRU, never by a stored per-IP row. So an IP that
  was answered by a registry-level or unallocated registration — which the trie now excludes and
  the in-run cache never remembers — is looked up over RDAP again in every later task, once per
  task, for as long as it falls inside `rdap_cache_days`.

## Useful queries

```sql
SELECT registry_class, count() FROM corpscout.rdap_network_registry_class_current GROUP BY registry_class;

-- Non-reusable registrations and how many addresses each one served
SELECT c.network_key, c.registry_class, c.covered_rir_blocks, n.name, n.start_address, n.end_address, ifNull(e.served_ips, 0) AS served_ips
FROM corpscout.rdap_network_registry_class_current AS c
LEFT JOIN corpscout.rdap_networks_current AS n ON n.network_key = c.network_key
LEFT JOIN (SELECT rdap_network_key, count() AS served_ips FROM corpscout.ip_enrichment_current WHERE rdap_lookup_status = 'found' GROUP BY rdap_network_key) AS e ON e.rdap_network_key = c.network_key
WHERE c.registry_class != 'reusable'
ORDER BY served_ips DESC;

-- Why a registration got its class
SELECT * FROM corpscout.rdap_network_registry_class_derived WHERE network_key = 'apnic:103.0.0.0 - 103.255.255.255';

-- Snapshots on disk per source (current + previous)
SELECT registry, snapshot_date, count() FROM corpscout.ip_registry_special_segments GROUP BY registry, snapshot_date ORDER BY registry, snapshot_date;

-- Held vs. unheld RIR-designated IANA blocks
SELECT designation, rir, prefix, unheld_rir_block FROM corpscout.ip_registry_iana_blocks_rule_current WHERE rir != '' ORDER BY unheld_rir_block, prefix;

SELECT name, status, element_count, formatReadableSize(bytes_allocated) FROM system.dictionaries WHERE database = 'corpscout' AND name = 'ip_registry_special_trie';
```

Re-running `rdap_network_registry_class` (or the whole job) reclassifies everything from the
current snapshots and reloads `rdap_network_trie`; nothing else needs a restart.
