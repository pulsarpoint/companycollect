CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.no_companies MODIFY COLUMN registration_date Nullable(Date32);
ALTER TABLE corpscout.no_companies MODIFY COLUMN incorporation_date Nullable(Date32);
