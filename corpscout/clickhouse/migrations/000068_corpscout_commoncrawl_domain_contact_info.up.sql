CREATE DATABASE IF NOT EXISTS corpscout;

-- Contacts scraped from a domain's pages: one row per (domain, type, value), so a domain can have
-- MANY emails/phones/social links. contact_type is email, phone, or social. source records how it
-- was found (jsonld vs regex text). Replaces the dropped commoncrawl_domains.emails array and the old
-- company_profile email/phone/same_as.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_contact_info
(
    crawl_id LowCardinality(String),
    root_domain String,
    contact_type LowCardinality(String),
    value String,
    source LowCardinality(String),
    source_url String,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, contact_type, value);
