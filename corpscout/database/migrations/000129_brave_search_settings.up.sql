CREATE TABLE processing.brave_searches (
    search_id uuid PRIMARY KEY,
    name text NOT NULL CHECK (length(btrim(name)) BETWEEN 1 AND 120),
    query_type text NOT NULL UNIQUE,
    query_template text NOT NULL CHECK (length(btrim(query_template)) BETWEEN 1 AND 8000),
    revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
    archived_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX brave_searches_active_name ON processing.brave_searches (lower(name)) WHERE archived_at IS NULL;
INSERT INTO processing.brave_searches (search_id, name, query_type, query_template)
VALUES ('54d90187-85d5-45dc-9603-cd7c4a7d31d1', 'Official website', 'official_website', 'Find the official website of {company_name}.');
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='processing_worker') THEN
        GRANT SELECT, INSERT, UPDATE ON processing.brave_searches TO processing_worker;
    END IF;
END $$;
