CREATE DATABASE IF NOT EXISTS corpscout;

-- Independent cursor for replaying AXFR record owners into the durable hostname registry.
-- Existing state rows read as the epoch, so the first materialization performs a full backfill.
ALTER TABLE corpscout.commoncrawl_domain_hostname_sync_state
    ADD COLUMN IF NOT EXISTS axfr_loaded_through
        SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
        DEFAULT toDateTime64(0, 3, 'UTC')
        AFTER domains_resolved_through;
