CREATE DATABASE IF NOT EXISTS corpscout;

RENAME TABLE
    corpscout.se_bolagsverket_financial_metrics
TO
    corpscout.se_financial_metrics;
