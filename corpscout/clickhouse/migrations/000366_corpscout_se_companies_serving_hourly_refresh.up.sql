CREATE DATABASE IF NOT EXISTS corpscout;

-- The full serving rebuild takes longer than the former 15-minute interval, which kept
-- the refresh worker continuously busy and competed with foreground queries and merges.
-- Run at :45 to stagger it from the hourly address-geocode refresh at :00.
ALTER TABLE corpscout.se_companies_serving
    MODIFY REFRESH EVERY 1 HOUR OFFSET 45 MINUTE;
