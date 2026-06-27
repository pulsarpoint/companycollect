CREATE DATABASE IF NOT EXISTS corpscout;

-- Slim commoncrawl_domains to the domain master/identity. The NACE classification now lives in
-- commoncrawl_industries (multi-row) and the page/decision signals in commoncrawl_page_signals, so
-- drop those columns plus the page-scraped contacts (contacts live in commoncrawl_company_profile).
-- ALTER DROP keeps the rows and the sort key, so the ~3M identity rows are preserved.
ALTER TABLE corpscout.commoncrawl_domains
    DROP COLUMN IF EXISTS emails,
    DROP COLUMN IF EXISTS email_count,
    DROP COLUMN IF EXISTS page_type,
    DROP COLUMN IF EXISTS page_type_score,
    DROP COLUMN IF EXISTS nace_code,
    DROP COLUMN IF EXISTS nace_label,
    DROP COLUMN IF EXISTS nace_division,
    DROP COLUMN IF EXISTS nace_confident,
    DROP COLUMN IF EXISTS nace_confidence,
    DROP COLUMN IF EXISTS nace_margin,
    DROP COLUMN IF EXISTS nace_score,
    DROP COLUMN IF EXISTS nace_method,
    DROP COLUMN IF EXISTS nace_top3_codes,
    DROP COLUMN IF EXISTS nace_top3_labels,
    DROP COLUMN IF EXISTS nace_top3_scores;
