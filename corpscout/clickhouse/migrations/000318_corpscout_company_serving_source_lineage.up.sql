CREATE DATABASE IF NOT EXISTS corpscout;

-- Company-serving rows own the lineage needed by their readers. This removes
-- the runtime dependency on the polymorphic company_source_record* tables.
ALTER TABLE corpscout.company_description_current
    ADD COLUMN IF NOT EXISTS source_record_uid String AFTER confidence,
    ADD COLUMN IF NOT EXISTS evidence_ids Array(String) AFTER source_record_uid,
    ADD COLUMN IF NOT EXISTS source_field String AFTER evidence_ids,
    ADD COLUMN IF NOT EXISTS model_provider LowCardinality(String) AFTER source_field,
    ADD COLUMN IF NOT EXISTS model_name String AFTER model_provider,
    ADD COLUMN IF NOT EXISTS prompt_version String AFTER model_name;

ALTER TABLE corpscout.company_section_item_source_links
    ADD COLUMN IF NOT EXISTS record_kind LowCardinality(String) AFTER linked_at,
    ADD COLUMN IF NOT EXISTS content_sha256 String AFTER record_kind,
    ADD COLUMN IF NOT EXISTS first_seen_at DateTime64(3, 'UTC') AFTER content_sha256,
    ADD COLUMN IF NOT EXISTS last_seen_at DateTime64(3, 'UTC') AFTER first_seen_at,
    ADD COLUMN IF NOT EXISTS source_slug LowCardinality(String) AFTER last_seen_at,
    ADD COLUMN IF NOT EXISTS source_record_key String AFTER source_slug,
    ADD COLUMN IF NOT EXISTS source_url String AFTER source_record_key,
    ADD COLUMN IF NOT EXISTS source_object_key String AFTER source_url,
    ADD COLUMN IF NOT EXISTS payload_sha256 String AFTER source_object_key,
    ADD COLUMN IF NOT EXISTS retrieved_at DateTime64(3, 'UTC') AFTER payload_sha256;
