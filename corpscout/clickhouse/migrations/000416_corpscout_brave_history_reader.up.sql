CREATE DATABASE IF NOT EXISTS corpscout;

-- Readers need SELECT on this country view, not direct access to S3 credentials.
ALTER TABLE corpscout.se_company_brave_search_results_s3_archive
    MODIFY DEFINER=processing_publisher SQL SECURITY DEFINER;
