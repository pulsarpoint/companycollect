CREATE DATABASE IF NOT EXISTS corpscout;

RENAME TABLE corpscout.se_company_officers
TO corpscout.se_financial_report_signatories;
