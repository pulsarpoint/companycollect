# Shared MaxMind support

The Common Crawl-specific GeoIP writer and storage were retired in migrations 434–435.
Existing lookup values were imported into `ip_enrichment_results` with their original
lookup dates and deterministic result IDs. `ip_enrichment_current` serves that history
alongside newer source-independent results. RDAP is `not_attempted` for imported GeoIP rows.

This package retains the shared MaxMind resource, field mapping and address classification
used by `ip_enrichment_results` and the existing RDAP worker. It declares no GeoIP writer.

Use `ip_enrichment_input` to select IPs from `corpscout.commoncrawl_ip_addresses` or another
source, then run `ip_enrichment_results` with the resulting task ID. See
[IP enrichment](../../../../../docs/ip-enrichment-schema.md) for configuration.

The manual `commoncrawl_geoip_update_available` check now reads conclusive City/ASN
outcomes from `ip_enrichment_current` when reporting uncovered DNS IP buckets.

The GeoLite2 files are downloaded by hand (no MaxMind account) and uploaded on the
backoffice page Admin → Settings → GeoLite2, which stores them in bucket `geolite2` and
launches `geolite2_install_job`. Its asset `geolite2_databases` (`install.py`) validates the
uploads (edition, build not older than installed, safe archive member) and installs them by
atomic rename into `MAXMIND_DATABASE_DIRECTORY`. The check `geolite2_databases_fresh` on
`ip_enrichment_results` (`freshness.py`, job `geolite2_freshness_job`) fails when either file
is older than 14 days, and every results run reports the build dates. Procedure:
[ip-enrichment-draft-queue.md](../../../../../docs/operations/ip-enrichment-draft-queue.md).
