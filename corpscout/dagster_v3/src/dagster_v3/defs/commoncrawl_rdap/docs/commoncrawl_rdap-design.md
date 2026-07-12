# CommonCrawl RDAP network enrichment

`commoncrawl_ip_rdap_networks` is a manual, 256-partition Dagster asset. Each partition uses the
same stable CommonCrawl IP bucket as GeoIP enrichment.

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
