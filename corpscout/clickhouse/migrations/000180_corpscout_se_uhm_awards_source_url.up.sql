CREATE DATABASE IF NOT EXISTS corpscout;

-- Carry the address each award was downloaded from, so evidence traces back to
-- a document rather than only naming an id. UHM publishes no page per award --
-- the register is a bulk CSV -- so the document a row traces to is the resource
-- it was parsed out of. Every other source already stores source_url for the
-- same reason. This table was the exception, keeping only source_object_key,
-- which names our own S3 snapshot and means nothing to a reader.
--
-- The value is carried per row from the snapshot's manifest, not stamped from a
-- constant when read, so rows keep the URL they were actually fetched from if
-- the catalogue ever moves the resource.
ALTER TABLE corpscout.se_uhm_procurement_awards
    ADD COLUMN IF NOT EXISTS source_url String AFTER advertising_database;
