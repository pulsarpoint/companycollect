CREATE DATABASE IF NOT EXISTS corpscout;

-- Task 7 (correctness hardening): the retry-safe replacement for corpscout.commoncrawl_domain_dns_records
-- (kept in place, untouched, as a read-only legacy baseline -- see its comment in migration 000105 and
-- internal/model/model.go's RecordRow doc comment). This table holds exactly one row per record identity
-- (root_domain, record_type, slot, name, value), same shape as the legacy table, but its data is
-- RECOMPUTED wholesale on every refresh of the materialized view below rather than incrementally
-- aggregated -- so a plain MergeTree is enough. There is nothing for a Replacing/Aggregating engine to
-- reconcile, because the view's GROUP BY already guarantees one row per key each time it runs.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_dns_record_summary
(
    root_domain    String,
    record_type    LowCardinality(String),
    slot           LowCardinality(String),
    name           String,
    value          String,
    scans          UInt64,
    first_seen     DateTime64(3, 'UTC'),
    last_seen      DateTime64(3, 'UTC'),
    ttl            UInt32,
    priority       UInt16,
    rcode          String,
    last_run_id    String,
    sources        Array(LowCardinality(String)),
    discoveries    Array(LowCardinality(String)),
    last_source    LowCardinality(String),
    last_discovery LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY (root_domain, record_type, slot, name, value);

-- The deployed ClickHouse (26.5+) fully supports refreshable materialized views, so this uses one
-- rather than the scheduled-rebuild fallback: REFRESH EVERY runs the whole SELECT below on a timer and
-- atomically swaps it into the TO target, so readers never see a half-built summary. A plain
-- (incremental) materialized view was rejected for this table specifically because it fires on every
-- INSERT into commoncrawl_domain_dns_record_observations -- a retried insert of the SAME observation
-- would trigger the incremental view again BEFORE the underlying ReplacingMergeTree has a chance to
-- background-merge the duplicate away, double-counting exactly the way this task exists to fix. A
-- refreshable view instead always reads the observations table FINAL (forcing the merge at query time),
-- so a duplicate row from a retry is invisible to it regardless of merge timing.
--
-- REFRESH EVERY 10 MINUTE: this view fully rescans both source tables every refresh (there is no
-- incremental/delta refresh mode), so the interval trades summary staleness against repeated full-scan
-- cost. Ten minutes comfortably covers the gap between a scan's incremental ClickHouse loads (the
-- orchestrator's --load-interval defaults to 30 minutes) without re-running the aggregation so often
-- that it competes with ingestion. Revisit once real observation-table volume is known.
--
-- The SELECT unions two per-identity sources, tagged by row_kind, and finishes with ONE outer
-- aggregation keyed on the record identity:
--   - "legacy": corpscout.commoncrawl_domain_dns_records FINAL, forcing its AggregatingMergeTree to
--     merge to one row per identity. Its own scans/first_seen/last_seen/ttl/priority/rcode/last_run_id/
--     source/discovery are carried through verbatim as this identity's pre-cutover history. Its
--     accumulated `scans` may already include retry inflation from BEFORE this task's fix landed -- that
--     is a legacy fact (not reconstructable without the historical scan_ids that caused it) and is
--     preserved rather than corrected.
--   - "new": corpscout.commoncrawl_domain_dns_record_observations FINAL (so a retried, not-yet-merged
--     duplicate row is invisible even between background merges), one row per observation.
-- Combining them in the OUTER aggregation (rather than pre-aggregating each branch to one row first)
-- lets a single uniqExactIf(scan_id, row_kind = 'new') correctly count each new scan exactly once even
-- when the SAME scan observed the SAME record via more than one source (e.g. both 'query' and 'axfr'),
-- which would otherwise be double-counted if scans were summed per raw row instead.
CREATE MATERIALIZED VIEW IF NOT EXISTS corpscout.commoncrawl_domain_dns_record_summary_mv
REFRESH EVERY 10 MINUTE
TO corpscout.commoncrawl_domain_dns_record_summary
AS
SELECT
    root_domain,
    record_type,
    slot,
    name,
    value,
    -- scans = legacy_scans (0 if this identity has no legacy row) + uniqExact(new scan_id)
    maxIf(legacy_scans, row_kind = 'legacy') + uniqExactIf(scan_id, row_kind = 'new') AS scans,
    min(first_seen_component) AS first_seen,
    max(last_seen_component)  AS last_seen,
    -- TTL/priority/rcode/last_run_id: argMax over a deterministic (event_time, version_time) tuple, so
    -- whichever row -- legacy or new -- describes the record as of the LATEST known event time wins,
    -- with loaded_at breaking ties among new observations sharing an event_time (out-of-order loads).
    argMax(ttl,         (event_time, version_time)) AS ttl,
    argMax(priority,    (event_time, version_time)) AS priority,
    argMax(rcode,       (event_time, version_time)) AS rcode,
    argMax(last_run_id, (event_time, version_time)) AS last_run_id,
    -- provenance as stable sets: every source/discovery value ever seen for this identity, legacy
    -- included, so "observed via both source=query and source=axfr" retains both.
    groupUniqArray(source)    AS sources,
    groupUniqArray(discovery) AS discoveries,
    argMax(source,    (event_time, version_time)) AS last_source,
    argMax(discovery, (event_time, version_time)) AS last_discovery
FROM
(
    SELECT
        root_domain, record_type, slot, name, value,
        'legacy'                  AS row_kind,
        scans                     AS legacy_scans,
        ''                        AS scan_id,
        first_seen                AS first_seen_component,
        last_seen                 AS last_seen_component,
        last_seen                 AS event_time,          -- best available proxy: legacy has no
                                                            -- per-row event time, only its own last_seen
        toDateTime64(0, 3, 'UTC') AS version_time,         -- lowest priority: any real new observation
                                                            -- with an equal event_time wins the tiebreak
        ttl, priority, rcode, last_run_id, source, discovery
    FROM corpscout.commoncrawl_domain_dns_records FINAL

    UNION ALL

    SELECT
        root_domain, record_type, slot, name, value,
        'new'          AS row_kind,
        toUInt64(0)    AS legacy_scans,
        scan_id,
        observed_at    AS first_seen_component,
        observed_at    AS last_seen_component,
        observed_at    AS event_time,
        loaded_at      AS version_time,
        ttl, priority, rcode, scan_id AS last_run_id, source, discovery
    FROM corpscout.commoncrawl_domain_dns_record_observations FINAL
)
GROUP BY root_domain, record_type, slot, name, value;
