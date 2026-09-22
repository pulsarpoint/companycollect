# Redirect deployment and Brave continuation — 2026-09-21

Deployed to 192.168.88.132 through the existing Ansible playbooks:

- Browser service 0.7.4, release `de43c9410de2ded4fc49778247fbbf0a227a4b9c43e5a5023429a529f28aceee`.
- Company crawler 0.39.1, release `7bdd9439e710b420eb080bfb152dd97db12d910a38d2ce930d822e509ddc4d95`.

Both systemd services are active and healthy. All 13 browser-service and 47 crawler Python files match the local source by SHA-256. CloakBrowser remains 0.5.10. SQLite browser settings remain four headless and two headed browsers.

## Brave recovery

The user authorized terminating Brave for deployment and continuing afterward. The original run was gracefully canceled with Dagster's SAFE_TERMINATE policy:

- Original run/execution: `ce521036-669b-4b96-8d1d-a71147c3bfa4`.
- Input task: `a9b44804-acb0-4085-8d4b-997a4f9d0594` (736,554 selected companies).
- Saved outcomes before termination: 14,462.
- Saved outcomes after in-flight requests drained: 14,468 (14,267 successes; 201 errors).
- All browser leases were released before deployment.

Continuation uses `company_brave_search_job`, configured with only the original `execution_id`. It retains the original selection, query and skip policy. It does not initialize a new selection or force searches of completed inputs.

[Continuation run](http://dagster:3000/runs/952c6607-49b4-4667-9572-7b10fd9471d6) is STARTED.
At 2026-09-21T18:56:04.650Z, ClickHouse contained 14,474 distinct completed inputs in the same execution, including 6 new results from the continuation. The total increased by exactly the new-result count, preserving all pre-deployment outcomes.

## Production verification

[Basic info validation run](http://dagster:3000/runs/53553372-bb74-4e7b-9d0a-f8e0e6b64e0f) completed successfully for both domains:

- AGA: HTTPS protocol failure → one HTTP attempt → HTTP 301 → Linde's HTTPS website. Linde company description and HTML saved.
- Advokatsamfundet: HTTPS → www redirect; described successfully as the Swedish Bar Association, with further company crawling correctly skipped.

Both new results are in `website_site_info_results_current` and immutable S3 archives, read back through `website_crawl_results_s3`. Descriptions, page sections, observations, model usage and HTML hashes matched. Original/final URLs, HTTP redirect codes and connection attempts are preserved. No verification issues were found.

See `site-info/verification.json` and `site-info/provenance.json` for exact request IDs and S3 paths. Deployment logs, original run metadata, cancellation receipt, continuation request, launch receipt and checkpoint snapshots are retained in this folder. No Dagster service restart or code deployment was needed.
