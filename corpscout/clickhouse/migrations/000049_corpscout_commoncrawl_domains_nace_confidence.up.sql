CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.commoncrawl_domains
    ADD COLUMN IF NOT EXISTS nace_confidence Float32 AFTER nace_confident;
