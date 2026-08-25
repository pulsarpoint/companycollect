-- Geocode analysis agent: run ledger, suggestions, and durable memory.
--
-- The agent (app/agents/geocode-analysis.server.ts) NEVER connects here: it
-- returns structured output and the application validates it and writes these
-- rows. ClickHouse stays read-only for the agent, and nothing in this schema
-- can reach the geocode store -- suggestions graduate through a golden-gated
-- Dagster policy bump, never through this database.
--
-- Country is a parameter everywhere (`country_code`, the repository-wide
-- spelling): Sweden is only the first wired country.
--
-- No GRANTs here on purpose. The Ansible bootstrap
-- (ansible/roles/backoffice_postgres/templates/bootstrap.sql.j2) already runs
-- `ALTER DEFAULT PRIVILEGES FOR ROLE corpscout_backoffice_owner IN SCHEMA
-- public`, so tables created by the migration owner are readable/writable by
-- corpscout_backoffice_app and readable by corpscout_backoffice_dagster the
-- moment they exist.

CREATE TABLE IF NOT EXISTS geocode_agent_runs (
    id              uuid        PRIMARY KEY,
    country_code    text        NOT NULL CHECK (country_code = upper(country_code) AND length(country_code) = 2),
    -- The trigger's own parameters (focus directive, caps actually used).
    params          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    status          text        NOT NULL CHECK (status IN ('queued', 'running', 'done', 'failed')),
    model           text        NOT NULL DEFAULT '',
    -- Codex thread id, so a finished run can be resumed/inspected by hand.
    thread_id       text        NOT NULL DEFAULT '',
    -- How many agent turns the loop actually spent, and what they cost.
    iterations      integer     NOT NULL DEFAULT 0,
    input_tokens    bigint      NOT NULL DEFAULT 0,
    output_tokens   bigint      NOT NULL DEFAULT 0,
    -- The agent's own claim that it found no further unmatched classes.
    converged       boolean     NOT NULL DEFAULT false,
    report_md       text        NOT NULL DEFAULT '',
    error_message   text        NOT NULL DEFAULT '',
    created_at      timestamptz NOT NULL DEFAULT now(),
    started_at      timestamptz,
    finished_at     timestamptz
);

CREATE INDEX IF NOT EXISTS geocode_agent_runs_country_created
    ON geocode_agent_runs (country_code, created_at DESC);

-- One live run per country. The trigger is a button anyone can double-click,
-- and two concurrent runs would spend tokens re-deriving the same clusters.
CREATE UNIQUE INDEX IF NOT EXISTS geocode_agent_runs_one_active_per_country
    ON geocode_agent_runs (country_code)
    WHERE status IN ('queued', 'running');

CREATE TABLE IF NOT EXISTS geocode_agent_suggestions (
    id               uuid        PRIMARY KEY,
    run_id           uuid        NOT NULL REFERENCES geocode_agent_runs (id) ON DELETE CASCADE,
    country_code     text        NOT NULL CHECK (country_code = upper(country_code) AND length(country_code) = 2),
    -- Short label for the address class the rule would cover.
    pattern          text        NOT NULL,
    -- What Dagster's address augmentation should do about it.
    description      text        NOT NULL,
    -- Addresses the agent expects the rule to newly match, and how it counted.
    expected_yield   bigint      NOT NULL DEFAULT 0 CHECK (expected_yield >= 0),
    yield_basis      text        NOT NULL DEFAULT '',
    confidence       text        NOT NULL DEFAULT '' CHECK (confidence IN ('', 'low', 'medium', 'high')),
    -- Verbatim unmatched addresses (and matched exemplars) behind the claim.
    examples         jsonb       NOT NULL DEFAULT '[]'::jsonb,
    status           text        NOT NULL DEFAULT 'new'
                                 CHECK (status IN ('new', 'accepted', 'implemented', 'rejected')),
    -- Set when an accepted suggestion actually ships in a policy bump.
    policy_version   text        NOT NULL DEFAULT '',
    decided_by       text        NOT NULL DEFAULT '',
    decided_at       timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS geocode_agent_suggestions_country_status
    ON geocode_agent_suggestions (country_code, status, created_at DESC);

CREATE INDEX IF NOT EXISTS geocode_agent_suggestions_run
    ON geocode_agent_suggestions (run_id, created_at DESC);

-- Durable notes injected into the next run's context: what was tried, what
-- converged, register quirks learned. Keyed per country so one country's
-- lessons never leak into another's prompt.
CREATE TABLE IF NOT EXISTS geocode_agent_memory (
    country_code text        NOT NULL CHECK (country_code = upper(country_code) AND length(country_code) = 2),
    key          text        NOT NULL CHECK (key <> ''),
    content      text        NOT NULL,
    -- The run that last wrote this note (kept when that run is deleted).
    run_id       uuid        REFERENCES geocode_agent_runs (id) ON DELETE SET NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (country_code, key)
);
