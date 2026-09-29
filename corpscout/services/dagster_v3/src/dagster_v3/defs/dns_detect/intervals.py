"""Rebuild one hash bucket of domain_service_intervals and provider_service_counts.

Both are derived from dns_record_services and replaced whole per bucket: stage
tables are filled, checked, then swapped in with REPLACE PARTITION, so a
failure leaves the live tables untouched. Every statement carrying %% is run
with params so clickhouse-driver renders it.
"""

import uuid

from dagster_v3.defs.dns_detect import sql

SETTINGS = {"max_memory_usage": 16_000_000_000, "max_threads": 8}


def rebuild_bucket(client, database: str, bucket: int, log, *, stage_suffix: str | None = None) -> dict:
    suffix = stage_suffix or uuid.uuid4().hex[:12]
    stage_iv = f"{sql.INTERVALS_TABLE}_stage_{suffix}"
    stage_ct = f"{sql.COUNTS_TABLE}_stage_{suffix}"
    try:
        client.execute(f"CREATE TABLE `{database}`.`{stage_iv}` AS `{database}`.`{sql.INTERVALS_TABLE}`")
        client.execute(f"CREATE TABLE `{database}`.`{stage_ct}` AS `{database}`.`{sql.COUNTS_TABLE}`")
        client.execute(sql.intervals_insert_sql(database, stage_iv, bucket), {}, settings=SETTINGS)
        staged, current, providers, unmapped = client.execute(
            f"SELECT count(), countIf(is_current = 1), uniqExactIf(provider_slug, provider_slug != ''), "
            f"uniqExactIf(provider_key, provider_slug = '') FROM `{database}`.`{stage_iv}`")[0]
        if staged == 0:
            results = client.execute(sql.current_results_count_sql(database, bucket), {})[0][0]
            if results:
                raise ValueError(f"bucket {bucket}: no intervals staged but {results} current result rows exist; not swapping")
        client.execute(sql.counts_insert_sql(database, stage_ct, stage_iv, bucket), {}, settings=SETTINGS)
        client.execute(f"ALTER TABLE `{database}`.`{sql.INTERVALS_TABLE}` REPLACE PARTITION {int(bucket)} FROM `{database}`.`{stage_iv}`")
        client.execute(f"ALTER TABLE `{database}`.`{sql.COUNTS_TABLE}` REPLACE PARTITION {int(bucket)} FROM `{database}`.`{stage_ct}`")
        log.info("bucket %d: %d intervals (%d current), %d providers, %d unmapped keys", bucket, staged, current, providers, unmapped)
        return {"intervals": staged, "current": current, "providers": providers, "unmapped_keys": unmapped}
    finally:
        client.execute(f"DROP TABLE IF EXISTS `{database}`.`{stage_iv}`")
        client.execute(f"DROP TABLE IF EXISTS `{database}`.`{stage_ct}`")
