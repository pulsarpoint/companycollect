-- Application metadata, independent of Dagster's internal partition/run storage.
CREATE TABLE commoncrawl_graph_releases (
    graph_release text PRIMARY KEY CHECK (graph_release ~ '^cc-main-[a-z0-9-]+$'),
    source_index_url text NOT NULL CHECK (btrim(source_index_url) <> ''),
    crawl_ids text[] NOT NULL DEFAULT '{}',
    coverage_start date,
    coverage_end date,
    source_published_at timestamptz,
    discovered_at timestamptz NOT NULL DEFAULT now(),
    last_checked_at timestamptz NOT NULL DEFAULT now(),
    graph_status text NOT NULL DEFAULT 'not_imported'
        CHECK (graph_status IN ('not_imported', 'retained', 'retired')),
    graph_retired_at timestamptz,
    CHECK (coverage_end >= coverage_start),
    CHECK ((graph_status = 'retired') = (graph_retired_at IS NOT NULL))
);

CREATE TABLE commoncrawl_graph_release_files (
    graph_release text NOT NULL REFERENCES commoncrawl_graph_releases,
    graph_level text NOT NULL DEFAULT 'domain' CHECK (graph_level = 'domain'),
    artifact_kind text NOT NULL CHECK (artifact_kind IN ('nodes', 'edges', 'ranks')),
    source_url text CHECK (btrim(source_url) <> ''),
    source_etag text CHECK (btrim(source_etag) <> ''),
    source_bytes bigint CHECK (source_bytes >= 0),
    expected_rows bigint CHECK (expected_rows >= 0),
    schema_version text,
    availability text NOT NULL CHECK (availability IN ('available', 'unavailable', 'unsupported')),
    last_checked_at timestamptz NOT NULL DEFAULT now(),
    cache_bucket text CHECK (btrim(cache_bucket) <> ''),
    cache_key text CHECK (btrim(cache_key) <> ''),
    cached_source_etag text,
    cached_bytes bigint CHECK (cached_bytes >= 0),
    cached_sha256 text CHECK (cached_sha256 ~ '^[a-f0-9]{64}$'),
    cached_at timestamptz,
    PRIMARY KEY (graph_release, graph_level, artifact_kind),
    CHECK (availability <> 'available' OR source_url IS NOT NULL),
    CHECK (
        (cache_bucket IS NULL AND cache_key IS NULL AND cached_source_etag IS NULL
            AND cached_bytes IS NULL AND cached_sha256 IS NULL AND cached_at IS NULL)
        OR
        (cache_bucket IS NOT NULL AND cache_key IS NOT NULL AND cached_source_etag IS NOT NULL
            AND cached_bytes IS NOT NULL AND cached_sha256 IS NOT NULL AND cached_at IS NOT NULL)
    )
);

CREATE TABLE commoncrawl_graph_import_requests (
    request_id uuid PRIMARY KEY,
    graph_release text NOT NULL REFERENCES commoncrawl_graph_releases,
    selection text NOT NULL CHECK (selection IN ('full', 'ranks')),
    origin text NOT NULL CHECK (origin IN ('manual', 'automatic', 'backfill')),
    requested_by text NOT NULL CHECK (btrim(requested_by) <> ''),
    retry_of uuid,
    status text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'launching', 'running', 'succeeded', 'failed', 'canceled')),
    dagster_run_id uuid UNIQUE,
    source_manifest jsonb CHECK (jsonb_typeof(source_manifest) = 'object'),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    last_error text,
    UNIQUE (request_id, graph_release),
    FOREIGN KEY (retry_of, graph_release)
        REFERENCES commoncrawl_graph_import_requests (request_id, graph_release),
    CHECK (retry_of <> request_id),
    CHECK ((status IN ('succeeded', 'failed', 'canceled')) = (finished_at IS NOT NULL)),
    CHECK (status NOT IN ('launching', 'running', 'succeeded')
        OR (source_manifest IS NOT NULL AND source_manifest <> '{}'::jsonb)),
    CHECK (status NOT IN ('running', 'succeeded') OR dagster_run_id IS NOT NULL)
);

-- Full and ranks-only requests overlap, so both share the same release lock.
CREATE UNIQUE INDEX commoncrawl_graph_one_active_request
    ON commoncrawl_graph_import_requests (graph_release)
    WHERE status IN ('queued', 'launching', 'running');
CREATE INDEX commoncrawl_graph_requests_by_release
    ON commoncrawl_graph_import_requests (graph_release, created_at DESC);

CREATE TABLE commoncrawl_graph_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    active_graph_release text REFERENCES commoncrawl_graph_releases,
    automatic_imports_enabled boolean NOT NULL DEFAULT false,
    discovery_baseline_at timestamptz,
    discovery_attempted_at timestamptz,
    discovery_succeeded_at timestamptz,
    discovery_error text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    updated_by text
);
INSERT INTO commoncrawl_graph_state (singleton) VALUES (true);
