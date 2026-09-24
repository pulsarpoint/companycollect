CREATE DATABASE IF NOT EXISTS corpscout;

-- Retire the two registry aggregates. Source country tables remain intact.
DROP TABLE IF EXISTS corpscout.domains SYNC;
DROP TABLE IF EXISTS corpscout.company_website_domains SYNC;
