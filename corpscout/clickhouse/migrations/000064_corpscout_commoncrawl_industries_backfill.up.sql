CREATE DATABASE IF NOT EXISTS corpscout;

-- Backfill commoncrawl_industries from the ~3M already-classified rows in commoncrawl_domains, so the
-- split table holds the existing industry/NACE results. FINAL collapses ReplacingMergeTree duplicates
-- on the source, and the target dedupes per (root_domain, crawl_id) on its own merge. Identity columns
-- url and subdomain are intentionally NOT copied -- they stay on the commoncrawl_domains master.
INSERT INTO corpscout.commoncrawl_industries
    (crawl_id, root_domain, source_url, emails, email_count, page_type, page_type_score,
     nace_code, nace_label, nace_division, nace_confident, nace_confidence, nace_margin,
     nace_score, nace_method, nace_top3_codes, nace_top3_labels, nace_top3_scores,
     source_run_id, resolved_at)
SELECT crawl_id, root_domain, source_url, emails, email_count, page_type, page_type_score,
       nace_code, nace_label, nace_division, nace_confident, nace_confidence, nace_margin,
       nace_score, nace_method, nace_top3_codes, nace_top3_labels, nace_top3_scores,
       source_run_id, resolved_at
FROM corpscout.commoncrawl_domains FINAL;
