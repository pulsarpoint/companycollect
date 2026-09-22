CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_company_brave_search_results_s3_archive
    MODIFY SQL SECURITY INVOKER;
