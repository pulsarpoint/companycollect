"""Raw layer: download one registration month of PRH XBRL statements into RustFS.

Only statements from eligible companies (active + website, from the PRH YTJ
explorer cache) are downloaded; the rest are recorded as `skipped` in the
window listing. Already-present objects are reused, so re-materializing an old
window only fetches statements of companies that became eligible since — that
re-run IS the catch-up mechanism.
"""

from datetime import timedelta

import dagster as dg

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.assets.external import source_system
from dagster_corpscout.sources.finland.prh_xbrl.client import PRHXBRLClient
from dagster_corpscout.sources.finland.prh_xbrl.eligibility import (
    COMPANY_CACHE_ASSET_KEY,
    fetch_eligible_business_ids,
)
from dagster_corpscout.sources.finland.prh_xbrl.partitions import registration_month_partitions


class RawPullConfig(dg.Config):
    """Set refresh_existing to re-download objects already in RustFS
    (e.g. after a suspected upstream correction)."""

    refresh_existing: bool = False


@dg.asset(
    key_prefix=spec.ASSET_KEY_PREFIX,
    name="raw_xml_documents",
    partitions_def=registration_month_partitions,
    group_name=spec.GROUP_NAME,
    tags={**spec.TAGS, "layer": "raw"},
    deps=[source_system, dg.AssetKey(COMPANY_CACHE_ASSET_KEY)],
    retry_policy=dg.RetryPolicy(max_retries=3, delay=60, backoff=dg.Backoff.EXPONENTIAL),
    op_tags={"dagster/concurrency_key": spec.SOURCE_NAME},
)
def raw_xml_documents(
    context: dg.AssetExecutionContext,
    config: RawPullConfig,
    rustfs: RustFSResource,
    clickhouse: ClickHouseResource,
) -> dg.MaterializeResult:
    """Statements registered in the partition month, filtered to eligible companies.
    The discovery listing (the only place registration_date and skip decisions
    exist) is stored as raw data alongside the XML objects."""
    eligible = fetch_eligible_business_ids(clickhouse)
    if not eligible:
        raise dg.Failure(
            "eligibility query returned no companies — the company cache is empty or "
            "broken; refusing to record the whole window as skipped"
        )

    window = context.partition_time_window
    registered_date_start = window.start.date().isoformat()
    registered_date_end = (window.end.date() - timedelta(days=1)).isoformat()

    rustfs.ensure_bucket(spec.BUCKET)

    documents: list[dict] = []
    skipped: list[dict] = []
    downloaded_count = 0
    reused_count = 0
    bytes_downloaded = 0
    with PRHXBRLClient(base_url=spec.BASE_URL, user_agent=spec.USER_AGENT) as client:
        for statement in client.iter_registration_window(
            registered_date_start=registered_date_start,
            registered_date_end=registered_date_end,
        ):
            if statement.business_id not in eligible:
                skipped.append(
                    {
                        "business_id": statement.business_id,
                        "financial_date": statement.financial_date,
                        "registration_date": statement.registration_date,
                        "reason": "not_eligible",
                    }
                )
                continue

            object_key = spec.document_object_key(statement.business_id, statement.financial_date)
            if not config.refresh_existing and rustfs.object_exists(spec.BUCKET, object_key):
                reused_count += 1
                documents.append(
                    {
                        "business_id": statement.business_id,
                        "financial_date": statement.financial_date,
                        "registration_date": statement.registration_date,
                        "object_key": object_key,
                        "source_url": client.statement_xml_url(
                            statement.business_id, statement.financial_date
                        ),
                        # Parser recomputes hash and size from the stored body.
                        "xml_sha256": "",
                        "xml_size_bytes": 0,
                    }
                )
                continue

            body, source_url = client.download_statement_xml(
                statement.business_id, statement.financial_date
            )
            xml_sha256 = rustfs.put_bytes(spec.BUCKET, object_key, body)
            downloaded_count += 1
            bytes_downloaded += len(body)
            documents.append(
                {
                    "business_id": statement.business_id,
                    "financial_date": statement.financial_date,
                    "registration_date": statement.registration_date,
                    "object_key": object_key,
                    "source_url": source_url,
                    "xml_sha256": xml_sha256,
                    "xml_size_bytes": len(body),
                }
            )
            context.log.info(
                "downloaded statement business_id=%s financial_date=%s bytes=%d",
                statement.business_id,
                statement.financial_date,
                len(body),
            )

    listing_key = spec.window_listing_object_key(context.partition_key)
    rustfs.put_json(
        spec.BUCKET,
        listing_key,
        {
            "registered_date_start": registered_date_start,
            "registered_date_end": registered_date_end,
            "documents": documents,
            "skipped": skipped,
        },
    )
    context.log.info(
        "window complete: %d eligible documents (%d downloaded, %d reused), %d skipped as ineligible",
        len(documents),
        downloaded_count,
        reused_count,
        len(skipped),
    )

    return dg.MaterializeResult(
        metadata={
            "documents_count": len(documents),
            "downloaded_count": downloaded_count,
            "reused_count": reused_count,
            "skipped_ineligible_count": len(skipped),
            "eligible_companies_count": len(eligible),
            "bytes_downloaded": bytes_downloaded,
            "listing_object_key": listing_key,
        }
    )
