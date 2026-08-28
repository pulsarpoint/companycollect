CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_company_ratsit
    ADD COLUMN IF NOT EXISTS connection_mode LowCardinality(String)
        DEFAULT 'direct'
        AFTER failure_type;

ALTER TABLE corpscout.se_company_ratsit
    ADD COLUMN IF NOT EXISTS proxy_name LowCardinality(String)
        DEFAULT ''
        AFTER connection_mode;

ALTER TABLE corpscout.se_company_ratsit
    ADD CONSTRAINT se_company_ratsit_proxy_route CHECK
        (connection_mode = 'direct' AND proxy_name = '')
        OR (
            connection_mode = 'proxy'
            AND proxy_name IN ('crawl_proxy1', 'crawl_proxy2', 'crawl_proxy3')
        );
