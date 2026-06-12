"""Raw layer: download one registration month of PRH XBRL statements into RustFS."""

from datetime import timedelta

import dagster as dg

from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.assets.external import source_system
from dagster_corpscout.sources.finland.prh_xbrl.client import PRHXBRLClient
from dagster_corpscout.sources.finland.prh_xbrl.partitions import registration_month_partitions


@dg.asset(
    key_prefix=spec.ASSET_KEY_PREFIX,
    name="raw_xml_documents",
    partitions_def=registration_month_partitions,
    group_name=spec.GROUP_NAME,
    tags={**spec.TAGS, "layer": "raw"},
    deps=[source_system],
    retry_policy=dg.RetryPolicy(max_retries=3, delay=60, backoff=dg.Backoff.EXPONENTIAL),
    op_tags={"dagster/concurrency_key": spec.SOURCE_NAME},
)
def raw_xml_documents(
    context: dg.AssetExecutionContext, rustfs: RustFSResource
) -> dg.MaterializeResult:
    """Statements registered in the partition month: XML to company-keyed objects,
    plus the discovery listing (the only place registration_date exists) as raw data."""
    window = context.partition_time_window
    registered_date_start = window.start.date().isoformat()
    registered_date_end = (window.end.date() - timedelta(days=1)).isoformat()

    rustfs.ensure_bucket(spec.BUCKET)

    documents: list[dict] = []
    bytes_downloaded = 0
    with PRHXBRLClient(base_url=spec.BASE_URL, user_agent=spec.USER_AGENT) as client:
        for statement in client.iter_registration_window(
            registered_date_start=registered_date_start,
            registered_date_end=registered_date_end,
        ):
            body, source_url = client.download_financial_xml(
                statement.business_id, statement.financial_date
            )
            object_key = spec.document_object_key(statement.business_id, statement.financial_date)
            xml_sha256 = rustfs.put_bytes(spec.BUCKET, object_key, body)
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
        },
    )

    return dg.MaterializeResult(
        metadata={
            "documents_count": len(documents),
            "bytes_downloaded": bytes_downloaded,
            "listing_object_key": listing_key,
        }
    )
