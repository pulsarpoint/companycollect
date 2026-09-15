CREATE DATABASE IF NOT EXISTS corpscout;

-- Rename the existing queue in place, retaining its rows and stable table UUID.
RENAME TABLE corpscout.company_processing_input TO corpscout.company_brave_search_input;
