# Queue task source history

Backoffice shows original domains and page URLs in crawler and Webtech Recent task
history. Each task has a three-website preview and a paginated list (50 websites per
page). URLs are deduplicated by domain and URL; source names are retained.

Migration `000449_corpscout_queue_task_sources` adds a compact ClickHouse history
relation. The shared queue completion path synchronously copies all task input URLs
and provenance into it before dropping the completed input partition. This includes
freshness-skipped inputs that have no result in that execution. Copy failures stop
cleanup; retries are safe with ReplacingMergeTree and deduplicated reads. Large input
sets stay in ClickHouse, not PostgreSQL or Dagster run configuration.

Active and failed tasks use their retained queue inputs. Older purged tasks use
available saved results, matching crawler results to the original execution ID
(including retries) and Webtech results by task ID. The UI labels this fallback as
potentially incomplete because historical skipped inputs cannot always be recovered.
It never substitutes the domain's latest result from an unrelated task.

Deploy migration 449 before updating Dagster. No historical queue inputs are deleted
or reconstructed by this migration. Failure to read sources leaves Dagster task
status and run links visible in Backoffice.
