ALTER TABLE domain_crawl_job_pages
    ADD CONSTRAINT domain_crawl_job_pages_job_id_page_num_key UNIQUE (job_id, page_num);
