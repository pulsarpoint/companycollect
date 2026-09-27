ALTER TABLE processing.llm_profile_revisions
    ADD COLUMN reasoning_effort text
    CHECK (reasoning_effort IN ('none','minimal','low','medium','high','xhigh','max'));

CREATE OR REPLACE FUNCTION processing.protect_llm_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.profile_id,NEW.revision,NEW.provider,NEW.base_url,NEW.model,NEW.api_key_encrypted,NEW.created_at,NEW.reasoning_effort)
        IS DISTINCT FROM (OLD.profile_id,OLD.revision,OLD.provider,OLD.base_url,OLD.model,OLD.api_key_encrypted,OLD.created_at,OLD.reasoning_effort) THEN
        RAISE EXCEPTION 'LLM configuration revisions are immutable';
    END IF;
    RETURN NEW;
END $$;
