CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_company_brave_domains_history
    MODIFY SQL SECURITY INVOKER;
