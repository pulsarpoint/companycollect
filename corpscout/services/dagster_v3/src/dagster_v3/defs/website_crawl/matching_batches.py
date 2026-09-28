"""Send frozen basic/full crawl requests to the crawler's durable four-worker queue."""

import hashlib
import json
from collections import Counter
from time import monotonic, sleep

import dagster as dg
from corpscout_identity.urls import website_reference
from requests.exceptions import RequestException

from dagster_v3.defs.common.llm_control import (
    check_admission,
    current_request_id,
    finish_external_request,
)
from dagster_v3.defs.website_crawl.outcome_logging import log_crawl_outcomes


def service_request(
    http, base: str, method: str, path: str, payload: dict | None = None
) -> dict:
    for attempt in range(6):
        try:
            response = http.request(
                method,
                base + path,
                json=payload,
                timeout=(5, 30),
                allow_redirects=False,
            )
        except RequestException:
            if attempt == 5:
                raise RuntimeError(
                    "Crawler connection lost; resume the same execution ID"
                ) from None
        else:
            if response.status_code in {200, 202}:
                return response.json()
            if response.status_code not in {429, 502, 503, 504}:
                raise RuntimeError(
                    f"Crawler rejected lookup {method} with HTTP {response.status_code}"
                )
        sleep(2)
    raise RuntimeError("Crawler unavailable; resume the same execution ID")


def process_matching_batch(
    context, client, http, base: str, items: list[dict], *, input_id: str = ""
) -> int:
    from dagster_v3.defs.website_crawl.results import RESULTS_BY_TYPE

    entries = [
        dict(
            request=json.loads(item["request_json"]),
            **{key: item[key] for key in ("crawl_type", "input_revision", "work_key")},
        )
        for item in items
    ]
    batch_id = (
        "crawl-batch-"
        + hashlib.sha256(
            json.dumps(sorted(item["request_id"] for item in items)).encode()
        ).hexdigest()
    )
    profiles = {}
    for entry in entries:
        for key in ("llm", "decision_llm"):
            profile = entry["request"].get(key)
            if profile is not None:
                profiles[json.dumps(profile, sort_keys=True)] = profile
    owner = (
        current_request_id()
        if any(p.get("profile_id") for p in profiles.values())
        else None
    )
    active = False
    try:
        for profile in profiles.values():
            check_admission(
                profile,
                owner,
                service="crawler" if owner else None,
                external_request_id=batch_id,
            )
        active = True
        state = service_request(
            http,
            base,
            "POST",
            "/v1/crawl-batches",
            {
                "batch_id": batch_id,
                "input_id": input_id,
                "run_id": items[0]["run_id"],
                "entries": entries,
            },
        )
        started, logged = monotonic(), 0.0
        baseline = state["processed"]
        while state["state"] != "published":
            if state["state"] in {"cancelled", "cancelling"}:
                raise dg.Failure(
                    "Crawl batch cancelled; completed results are retained"
                )
            for profile in profiles.values():
                check_admission(profile, owner)
            if monotonic() - logged >= 10:
                rate = (
                    (state["processed"] - baseline) * 60 / max(monotonic() - started, 1)
                )
                context.log.info(
                    f"Crawl + company matching: {state['processed']}/{state['total']} · {rate:.1f} sites/min · matched={state['matched']} already_mapped={state.get('already_mapped', 0)} failed={state['failed']} · {state['state']}"
                )
                if state.get("publication_error"):
                    context.log.warning(
                        f"ClickHouse delivery pending ({state['publication_error']}); retained results will retry"
                    )
                logged = monotonic()
            sleep(2)
            state = service_request(http, base, "GET", "/v1/crawl-batches/" + batch_id)
        # The writer publishes the matching summary last, after the ordinary result.
        ids = tuple(item["request_id"] for item in items)
        identities = tuple((entry["request"]["request_id"], website_reference(
            entry["request"]["url"], entry["request"].get("website_id"))) for entry in entries)
        for table in (
            RESULTS_BY_TYPE[items[0]["crawl_type"]],
            "corpscout.website_company_lookup_results",
        ):
            count = client.execute(
                f"SELECT uniqExact(request_id) FROM {table} WHERE request_id IN %(ids)s AND (request_id,website_id) IN %(identities)s",
                {"ids": ids, "identities": identities},
            )[0][0]
            if count != len(items):
                raise dg.Failure(
                    f"Published batch contains {count}/{len(items)} results in {table}; check crawler and Dagster ClickHouse destinations"
                )
        active = False
        if owner:
            finish_external_request("crawler", batch_id)
        rows, columns = client.execute(
            f"""SELECT c.domain, c.request_id, c.attempt, c.state, c.crawl_status,
                c.successful, c.error,
                if(c.successful, '[]', c.pages) AS pages,
                if(c.successful, NULL, c.site_info) AS site_info,
                m.status AS matching_status, m.stop_reason AS matching_stop_reason,
                if(m.status IN ('failed', 'cancelled'), m.reasons, []) AS matching_reasons
            FROM (SELECT * FROM {RESULTS_BY_TYPE[items[0]["crawl_type"]]} FINAL
                WHERE request_id IN %(ids)s) AS c
            LEFT JOIN (SELECT * FROM corpscout.website_company_lookup_results FINAL
                WHERE request_id IN %(ids)s) AS m USING (request_id, attempt)
            ORDER BY c.domain, c.attempt""",
            {"ids": ids},
            with_column_types=True,
        )
        records = [dict(zip((name for name, _ in columns), row)) for row in rows]
        failures = log_crawl_outcomes(context, records, items[0]["crawl_type"])
        matching = Counter(record["matching_status"] for record in records)
        context.log.info(
            "Published crawl batch %s: stored_attempts=%s crawl_succeeded=%s crawl_unsuccessful=%s; company_matching=%s (not_found is not a crawl failure)",
            batch_id,
            len(records),
            sum(bool(r["successful"]) for r in records),
            sum(not r["successful"] for r in records),
            dict(matching),
        )
        if failures:
            context.log_event(
                dg.AssetObservation(
                    asset_key=RESULTS_BY_TYPE[items[0]["crawl_type"]].split(".")[-1],
                    metadata={
                        "batch_id": batch_id,
                        "failed_attempts": len(
                            {(row["request_id"], row["attempt"]) for row in failures}
                        ),
                        "failure_details": dg.MetadataValue.table(
                            [dg.TableRecord(row) for row in failures]
                        ),
                    },
                )
            )
        for table in (
            "website_company_lookup_results",
            "website_company_lookup_candidates",
            "website_company_lookup_evidence",
            "website_company_lookup_searches",
        ):
            context.log_event(
                dg.AssetMaterialization(
                    asset_key=table,
                    metadata={"batch_id": batch_id, "crawl_attempts": len(items)},
                )
            )
        if items[0]["crawl_type"] == "full":
            context.log_event(
                dg.AssetObservation(
                    asset_key="website_site_info_results",
                    metadata={"batch_id": batch_id, "crawl_attempts": len(items)},
                )
            )
        return len(items)
    finally:
        if active:
            try:
                service_request(http, base, "DELETE", "/v1/crawl-batches/" + batch_id)
            except RuntimeError as error:
                # Admission can fail before the batch exists. A cleanup failure
                # must not replace the original processing/admission exception.
                context.log.warning("Could not cancel crawl batch %s: %s", batch_id, error)
