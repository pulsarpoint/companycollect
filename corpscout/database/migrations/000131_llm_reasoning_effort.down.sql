CREATE OR REPLACE FUNCTION processing.protect_llm_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.profile_id,NEW.revision,NEW.provider,NEW.base_url,NEW.model,NEW.api_key_encrypted,NEW.created_at)
        IS DISTINCT FROM (OLD.profile_id,OLD.revision,OLD.provider,OLD.base_url,OLD.model,OLD.api_key_encrypted,OLD.created_at) THEN
        RAISE EXCEPTION 'LLM configuration revisions are immutable';
    END IF;
    RETURN NEW;
END $$;
ALTER TABLE processing.llm_profile_revisions DROP COLUMN reasoning_effort;
