CREATE DATABASE IF NOT EXISTS corpscout;

-- Backfill the split tables from the ~3M classified rows still in commoncrawl_domains. FINAL collapses
-- ReplacingMergeTree duplicates on the source.
--  - page_signals: one per-domain row of page classification + NACE ranking quality.
--  - industries:   fan the top-N (nace_code + the top3 arrays) into one row per (domain, nace_code),
--    ranked 1..N by score, is_primary = the top-1. Identity (url, subdomain) stays on the master, and
--    contacts are dropped (they live in commoncrawl_company_profile).

INSERT INTO corpscout.commoncrawl_page_signals
    (crawl_id, root_domain, subdomain, source_url, page_type, page_type_score, nace_confident, nace_margin, source_run_id, resolved_at)
SELECT crawl_id, root_domain, subdomain, source_url, page_type, page_type_score, nace_confident, nace_margin, source_run_id, resolved_at
FROM corpscout.commoncrawl_domains FINAL;

INSERT INTO corpscout.commoncrawl_industries
    (crawl_id, root_domain, nace_code, nace_label, nace_division, rank, is_primary, score, nace_method, source_url, source_run_id, resolved_at)
SELECT
    crawl_id,
    root_domain,
    cand.1 AS nace_code,
    cand.2 AS nace_label,
    splitByChar('.', cand.1)[1] AS nace_division,
    toUInt8(cand.4) AS rank,
    toUInt8(cand.4 = 1) AS is_primary,
    cand.3 AS score,
    nace_method,
    source_url,
    source_run_id,
    resolved_at
FROM corpscout.commoncrawl_domains FINAL
ARRAY JOIN arrayZip(nace_top3_codes, nace_top3_labels, nace_top3_scores, arrayEnumerate(nace_top3_codes)) AS cand
WHERE length(nace_top3_codes) > 0;
