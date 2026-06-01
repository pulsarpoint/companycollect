ALTER TABLE data_sources
  ADD COLUMN requires_translation BOOLEAN NOT NULL DEFAULT false;

UPDATE data_sources SET requires_translation = true WHERE name = 'brreg';
