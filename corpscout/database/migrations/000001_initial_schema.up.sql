-- database/migrations/000001_initial_schema.up.sql

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE countries (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    iso_alpha2          VARCHAR(2)  UNIQUE NOT NULL,
    iso_alpha3          VARCHAR(3)  UNIQUE NOT NULL,
    name                TEXT        NOT NULL,
    has_public_registry BOOLEAN     NOT NULL DEFAULT false,
    registry_url        TEXT,
    registry_notes      TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE data_sources (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                 TEXT        UNIQUE NOT NULL,
    source_type          TEXT        NOT NULL CHECK (source_type IN ('global_aggregator','country_registry')),
    adapter_type         TEXT        NOT NULL DEFAULT 'api' CHECK (adapter_type IN ('api','crawl4ai')),
    country_id           UUID        REFERENCES countries(id),
    enabled              BOOLEAN     NOT NULL DEFAULT true,
    crawl_interval_hours INT         NOT NULL DEFAULT 168,
    last_crawled_at      TIMESTAMPTZ,
    last_cursor          TEXT,
    config               JSONB,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE source_pull_runs (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id        UUID        NOT NULL REFERENCES data_sources(id),
    river_job_id     BIGINT,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ,
    status           TEXT        NOT NULL DEFAULT 'running'
                     CHECK (status IN ('running','completed','failed','partial')),
    cursor_start     TEXT,
    cursor_end       TEXT,
    snapshot_date    DATE,
    records_fetched  INT         NOT NULL DEFAULT 0,
    records_upserted INT         NOT NULL DEFAULT 0,
    error_message    TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE source_snapshots (
    id           UUID       PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id    UUID       NOT NULL REFERENCES data_sources(id),
    pull_run_id  UUID       NOT NULL REFERENCES source_pull_runs(id),
    payload_hash TEXT        NOT NULL,
    payload      JSONB      NOT NULL,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_id, payload_hash)
);

CREATE TABLE companies (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    lei                 VARCHAR(20) UNIQUE,
    name                TEXT        NOT NULL,
    country_id          UUID        NOT NULL REFERENCES countries(id),
    registration_number TEXT,
    status              TEXT        NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active','inactive','dissolved')),
    primary_source_id   UUID        REFERENCES data_sources(id),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX companies_country_reg_uniq
    ON companies(country_id, registration_number)
    WHERE registration_number IS NOT NULL AND lei IS NULL;

CREATE TABLE company_aliases (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID NOT NULL REFERENCES companies(id),
    alias      TEXT NOT NULL,
    alias_type TEXT NOT NULL CHECK (alias_type IN ('legal_name','trading_name','former_name','normalized')),
    source_id  UUID REFERENCES data_sources(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (company_id, alias, alias_type)
);

CREATE TABLE company_sources (
    company_id  UUID        NOT NULL REFERENCES companies(id),
    source_id   UUID        NOT NULL REFERENCES data_sources(id),
    external_id TEXT        NOT NULL,
    pull_run_id UUID        REFERENCES source_pull_runs(id),
    raw_data    JSONB,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (company_id, source_id)
);

CREATE TABLE domains (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain           TEXT UNIQUE NOT NULL,
    first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_verified_at TIMESTAMPTZ
);

CREATE TABLE company_domains (
    id                UUID     PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id        UUID     NOT NULL REFERENCES companies(id),
    domain_id         UUID     NOT NULL REFERENCES domains(id),
    relationship_type TEXT     NOT NULL DEFAULT 'candidate'
                      CHECK (relationship_type IN ('official_site','brand','subsidiary','old_domain','candidate')),
    status            TEXT     NOT NULL DEFAULT 'needs_review'
                      CHECK (status IN ('active','needs_review','rejected','superseded')),
    signal            TEXT     NOT NULL
                      CHECK (signal IN ('registry_website','wikidata','certsh','whois','search')),
    confidence        SMALLINT NOT NULL CHECK (confidence BETWEEN 1 AND 100),
    evidence          JSONB,
    first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (company_id, domain_id, signal)
);

CREATE TABLE company_domain_reviews (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_domain_id UUID NOT NULL REFERENCES company_domains(id),
    action            TEXT NOT NULL CHECK (action IN ('approved','rejected','superseded')),
    reviewed_by       TEXT NOT NULL,
    review_note       TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes on FK columns for join/filter performance
CREATE INDEX ON data_sources(country_id);
CREATE INDEX ON source_pull_runs(source_id);
CREATE INDEX ON source_snapshots(source_id);
CREATE INDEX ON source_snapshots(pull_run_id);
CREATE INDEX ON companies(country_id);
CREATE INDEX ON companies(primary_source_id) WHERE primary_source_id IS NOT NULL;
CREATE INDEX ON company_aliases(company_id);
CREATE INDEX ON company_sources(pull_run_id) WHERE pull_run_id IS NOT NULL;
CREATE INDEX ON company_domains(domain_id);
CREATE INDEX ON company_domain_reviews(company_domain_id);
