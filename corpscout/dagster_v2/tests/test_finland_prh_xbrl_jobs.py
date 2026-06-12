import dagster as dg
import responses
from moto import mock_aws

from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland.prh_xbrl import spec, tables
from dagster_corpscout.sources.finland.prh_xbrl.jobs import pull_company_job
from tests.test_finland_prh_xbrl_parsed_asset import FakeClickHouseResource, _recorder
from tests.test_finland_prh_xbrl_parser import SAMPLE_XML


def test_window_schedule_exists_and_is_stopped():
    from dagster_corpscout.definitions import defs

    schedule = defs.resolve_schedule_def("finland_prh_xbrl_pull_window_schedule")
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    assert schedule.job.name == "finland_prh_xbrl_pull_window"


@mock_aws
def test_pull_company_job_downloads_parses_and_loads_one_company():
    _recorder.inserts.clear()
    rustfs = RustFSResource(endpoint_url="", access_key="test", secret_key="test")

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{spec.BASE_URL}/financials",
            json={
                "totalResults": 1,
                "financials": [
                    {
                        "businessId": "0176460-0",
                        "financialDate": "2023-09-30",
                        "registrationDate": "2025-01-23",
                    }
                ],
            },
        )
        rsps.add(responses.GET, f"{spec.BASE_URL}/financial", body=SAMPLE_XML)

        result = pull_company_job.execute_in_process(
            run_config={
                "ops": {"pull_company_statements": {"config": {"business_id": "0176460-0"}}}
            },
            resources={
                "rustfs": rustfs,
                "clickhouse": FakeClickHouseResource(host="test", password="test"),
            },
        )

    assert result.success
    assert rustfs.get_bytes(spec.BUCKET, "companies/0176460-0/2023-09-30.xml") == SAMPLE_XML
    inserted = dict(_recorder.inserts)
    assert inserted[tables.STATEMENT_DOCUMENTS_TABLE] == 1
    assert inserted[tables.FACTS_TABLE] == 6
