CREATE DATABASE IF NOT EXISTS corpscout;

-- Executable technology fingerprints: one row per (technology, signal, pattern),
-- extracted from the SAME merged layers (extension bundle / webappanalyzer overlay /
-- custom entries) that publish corpscout.technology_catalog, in the same run -- so
-- `technology` always joins the catalog by equality and both tables carry one
-- consistent source_version per layer.
--
-- Wave 1 carries the Wappalyzer dns blocks (signal_type 'dns_mx', 'dns_txt',
-- 'dns_soa', ...) which no runtime evaluates today: the browser extension cannot do
-- DNS lookups and the CommonCrawl techfast pass only sees WARC headers/HTML. The
-- planned DNS detection pass joins these patterns against
-- commoncrawl_domain_dns_records. Later signal families (spf_include, txt_token,
-- dkim_selector, ns, cname, ip_prefix, ...) reuse the same table.
--
-- `pattern` is the cleaned regex -- Wappalyzer's backslash-delimited confidence and
-- version tails are parsed off into `confidence` and `version_template`. Patterns
-- are Wappalyzer-flavoured JS regexes, so consumers matching with re2 must tolerate
-- the rare incompatible pattern. Filled by the dagster technology_catalog module via
-- stage + EXCHANGE TABLES with a row floor. The migration owns this schema.
CREATE TABLE IF NOT EXISTS corpscout.technology_fingerprints
(
    technology String,
    signal_type LowCardinality(String),
    pattern String,
    confidence UInt8,
    version_template String,
    source LowCardinality(String),
    source_version String,
    source_run_id String,
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (signal_type, technology, pattern);
