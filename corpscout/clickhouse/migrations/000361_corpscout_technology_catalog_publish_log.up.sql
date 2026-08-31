CREATE DATABASE IF NOT EXISTS corpscout;

-- Append-only provenance ledger: one row per technology_catalog publish. The
-- catalog and fingerprint tables replace their whole contents each publish,
-- so nothing there records WHEN the technology set changed or to
-- WHICH definitions revision. This log answers exactly that -- definitions_hash
-- is the content hash of the repo-owned custom files (technologies.json,
-- categories.json, fingerprints.json), the same value that drives the Dagster
-- asset code_version, so a row whose hash differs from the previous row marks a
-- real change to OUR curated definitions (overlay_sha tracks the weekly public
-- catalog separately). Never replaced or deduplicated -- keep every publish.
--
-- Filled by the dagster technology_catalog_clickhouse asset after it publishes
-- both the catalog and the fingerprints in the same run. The migration owns
-- this schema.
CREATE TABLE IF NOT EXISTS corpscout.technology_catalog_publish_log
(
    published_at DateTime64(3, 'UTC'),
    source_run_id String,
    definitions_hash String,
    overlay_sha String,
    extension_version String,
    catalog_rows UInt32,
    fingerprint_rows UInt32,
    custom_technologies UInt16,
    override_fingerprints UInt16
)
ENGINE = MergeTree
ORDER BY published_at;
