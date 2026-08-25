ALTER TABLE corpscout.company_section_item_source_links
    DROP COLUMN IF EXISTS retrieved_at,
    DROP COLUMN IF EXISTS payload_sha256,
    DROP COLUMN IF EXISTS source_object_key,
    DROP COLUMN IF EXISTS source_url,
    DROP COLUMN IF EXISTS source_record_key,
    DROP COLUMN IF EXISTS source_slug,
    DROP COLUMN IF EXISTS last_seen_at,
    DROP COLUMN IF EXISTS first_seen_at,
    DROP COLUMN IF EXISTS content_sha256,
    DROP COLUMN IF EXISTS record_kind;

ALTER TABLE corpscout.company_description_current
    DROP COLUMN IF EXISTS prompt_version,
    DROP COLUMN IF EXISTS model_name,
    DROP COLUMN IF EXISTS model_provider,
    DROP COLUMN IF EXISTS source_field,
    DROP COLUMN IF EXISTS evidence_ids,
    DROP COLUMN IF EXISTS source_record_uid;
