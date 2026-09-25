-- The catalog and admission ledger share a transaction boundary. Completed work
-- and immutable credential revisions survive disabling or archiving a profile.
CREATE TABLE processing.llm_profiles (
    profile_id uuid PRIMARY KEY,
    name text NOT NULL CHECK (btrim(name) <> ''),
    current_revision integer NOT NULL CHECK (current_revision > 0),
    state text NOT NULL DEFAULT 'enabled' CHECK (state IN ('enabled','disabled','archived')),
    is_default boolean NOT NULL DEFAULT false,
    disabled_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (NOT is_default OR state = 'enabled')
);
CREATE UNIQUE INDEX llm_profile_name ON processing.llm_profiles (lower(name)) WHERE state <> 'archived';
CREATE UNIQUE INDEX llm_profile_default ON processing.llm_profiles (is_default) WHERE is_default;
CREATE TABLE processing.llm_profile_revisions (
    profile_id uuid NOT NULL REFERENCES processing.llm_profiles,
    revision integer NOT NULL CHECK (revision > 0),
    provider text NOT NULL,
    base_url text NOT NULL,
    model text NOT NULL,
    api_key_encrypted text,
    invalidated_at timestamptz,
    invalid_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (profile_id, revision)
);
ALTER TABLE processing.llm_profiles ADD CONSTRAINT llm_current_revision
    FOREIGN KEY (profile_id,current_revision) REFERENCES processing.llm_profile_revisions (profile_id,revision)
    DEFERRABLE INITIALLY DEFERRED;
CREATE FUNCTION processing.protect_llm_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.profile_id,NEW.revision,NEW.provider,NEW.base_url,NEW.model,NEW.api_key_encrypted,NEW.created_at)
        IS DISTINCT FROM (OLD.profile_id,OLD.revision,OLD.provider,OLD.base_url,OLD.model,OLD.api_key_encrypted,OLD.created_at) THEN
        RAISE EXCEPTION 'LLM configuration revisions are immutable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER immutable_llm_revision BEFORE UPDATE ON processing.llm_profile_revisions
    FOR EACH ROW EXECUTE FUNCTION processing.protect_llm_revision();
CREATE TABLE processing.llm_checks (
    profile_id uuid NOT NULL,
    revision integer NOT NULL,
    target text NOT NULL CHECK (target IN ('crawler','brave')),
    started_at timestamptz NOT NULL,
    finished_at timestamptz NOT NULL DEFAULT now(),
    ok boolean NOT NULL,
    failure_kind text CHECK (failure_kind IN ('configuration','transient','capability','service')),
    message text NOT NULL,
    PRIMARY KEY (profile_id,revision,target),
    FOREIGN KEY (profile_id,revision) REFERENCES processing.llm_profile_revisions
);
CREATE TABLE processing.llm_catalog_imports (
    source text PRIMARY KEY,
    imported_at timestamptz NOT NULL DEFAULT now(),
    profile_count integer NOT NULL
);
CREATE TABLE processing.run_requests (
    request_id uuid PRIMARY KEY,
    dagster_run_id uuid UNIQUE,
    task_id uuid,
    job_name text NOT NULL,
    status text NOT NULL DEFAULT 'launching'
        CHECK (status IN ('launching','queued','running','succeeded','failed','canceled','launch_failed')),
    stop_requested_at timestamptz,
    stop_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    last_error text
);
CREATE INDEX llm_runs_pending ON processing.run_requests (updated_at) WHERE finished_at IS NULL;
CREATE TABLE processing.run_llm_dependencies (
    request_id uuid NOT NULL REFERENCES processing.run_requests,
    profile_id uuid NOT NULL,
    revision integer NOT NULL,
    purpose text NOT NULL,
    PRIMARY KEY (request_id,profile_id,revision,purpose),
    FOREIGN KEY (profile_id,revision) REFERENCES processing.llm_profile_revisions
);
CREATE INDEX llm_dependencies_profile ON processing.run_llm_dependencies (profile_id,revision);
CREATE TABLE processing.llm_external_requests (
    service text NOT NULL CHECK (service IN ('crawler','brave')),
    external_request_id text NOT NULL,
    request_id uuid NOT NULL REFERENCES processing.run_requests,
    state text NOT NULL DEFAULT 'submitted' CHECK (state IN ('submitted','completed','failed','canceled','absent')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    cancel_attempted_at timestamptz,
    last_error text,
    PRIMARY KEY (service,external_request_id,request_id)
);
CREATE INDEX llm_external_pending ON processing.llm_external_requests (request_id) WHERE state = 'submitted';
