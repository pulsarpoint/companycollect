CREATE DATABASE IF NOT EXISTS corpscout;

-- Readers need SELECT on this country view, not direct access to S3 credentials.
ALTER TABLE corpscout.se_company_brave_domains_history
    MODIFY DEFINER=processing_publisher SQL SECURITY DEFINER;
