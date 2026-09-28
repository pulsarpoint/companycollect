# Central domains and their sources

`domains` owns registrable-domain identity. `domain_id` is the lowercase hexadecimal
SHA-256 of the canonical root. Websites and pages retain their separate identities.

## Compact contribution index (local Task 5, activation pending)

`domains_sources` has one logical row per `(domain_id, source_table)`, with
`first_seen_at`, `last_seen_at`, `updated_at` and `source_run_id`. It records discovery,
not a company relationship. Two Swedish companies proposing one domain produce one
`se_company_domain` contribution. A Norwegian source adds a separate contribution.
There are no company IDs, confidence scores, review states or per-attempt records.

Source adapters write evidence; the country fold registers missing domain parents and
publishes `se_company_domain`. The independent `domains_sources` asset derives table
contributions afterward. Its `source_tables` option is an explicit allowlist. The Swedish
refresh job selects only `se_company_domain`; it does not rescan the bulk inventory.
The inventory job publishes domains and then rebuilds the three known contributor
partitions. Common Crawl sources are read from the validated inventory's source tags,
using 16 bounded SHA-256 ranges. The country partition is aggregated once.

Each selected partition is staged, checked for missing parents and replaced independently.
Other partitions remain untouched. Previous memberships are retained and timestamps use
minimum first-seen / maximum last-seen, so a rejected/withdrawn claim cannot erase discovery
history. Retrying uses saved summaries/inventory; no browser or LLM work is repeated.
A failed later partition can be retried without rolling back earlier successful ones.

Both the inventory and index publisher use the `domains_publish` Dagster pool (limit one).
Final publication uses the shared PostgreSQL inventory guard. Completed publications
whose acknowledgment was lost are safe to replay. This is not a multi-table transaction.

## Company filters

`se_company_domain_resolved` applies live reviewer overrides to the country summary.
`domains_company_filter` and `domains_search.company_count` read that view directly,
counting distinct `(country_code, company_id)` where `is_active=1` and a central parent
exists. Index membership never implies an associated company. Adding another country's
associations requires an explicit reviewed SQL union in both readers. Stored source names
are never interpolated as SQL table names.

Backoffice writes `se_company_domain_rule` and requests a filter refresh. It no longer
writes the source index. ClickHouse replaces the filter snapshot every five minutes;
review and fold actions also request refresh. The dedicated filter asset waits for it.
Counts, filtering and bulk selection continue to use the same derived filter snapshot.

## Preparation and cutover

Migrations 461–464 describe the deployed older layout and are not rewritten. Migration
467 prepares `domains_sources_next` and switches the filter query to country summaries.
It neither copies the large index nor drops/exchanges the live table. The explicit
`domains_sources_backfill` asset previews by default; with old writers paused, execute
bounded batches and resume using the acknowledged `after_domain_id`. It preserves every
contributor, including unknown future source-table names as data, and validates full row
parity and parent membership for each copied range.

Keep both layouts until coordinated cutover. The new publisher refuses the old schema.
See [the operations guide](../../../../docs/operations/domain-result-references.md) for
activation gates. Preparation and tests are local only; no production DDL has been applied.
