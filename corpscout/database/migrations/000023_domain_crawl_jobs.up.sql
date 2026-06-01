CREATE TABLE domain_crawl_jobs (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_id      UUID        NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
    river_job_id   BIGINT,
    mode           TEXT        NOT NULL DEFAULT 'deep'
                               CHECK (mode IN ('homepage', 'deep')),
    max_pages      INTEGER     NOT NULL DEFAULT 10,
    s3_prefix      TEXT,
    favicon_s3_key TEXT,
    favicon_url    TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON domain_crawl_jobs(domain_id, created_at DESC);

CREATE TABLE domain_crawl_job_pages (
    id             UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id         UUID    NOT NULL REFERENCES domain_crawl_jobs(id) ON DELETE CASCADE,
    page_num       INTEGER NOT NULL,
    url            TEXT    NOT NULL,
    title          TEXT,
    status_code    INTEGER,
    content_type   TEXT,
    md_s3_key      TEXT    NOT NULL,
    html_s3_key    TEXT    NOT NULL,
    headers_s3_key TEXT    NOT NULL
);

CREATE INDEX ON domain_crawl_job_pages(job_id, page_num);
