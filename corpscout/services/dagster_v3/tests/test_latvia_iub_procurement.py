from datetime import UTC, date, datetime

from dagster_v3.defs.latvia_iub_procurement import tables
from dagster_v3.defs.latvia_iub_procurement.parser import parse_daily_payload
from dagster_v3.defs.latvia_iub_procurement.resources import daily_notice_url


def test_iub_daily_url_uses_the_official_date_hierarchy() -> None:
    assert daily_notice_url(date(2026, 1, 9)) == (
        "https://open.iub.gov.lv/data/notice/2026/01/09-01-2026.json"
    )
    assert tables.SOURCE_LICENCE == "CC0 1.0"


def test_result_notice_expands_lots_contracts_and_winner_business_parties() -> None:
    parsed = parse_daily_payload(
        [
            {
                "identifier": "notice-v2",
                "clonedFrom": "notice-v1",
                "procurementProcedureIdentifier": "procedure-1",
                "noticeType": "pil-concluded-contract",
                "formType": "result",
                "name": "Radio receivers",
                "cpvType": "32324000-0",
                "procedureLegalBasis": "law-9",
                "organizationData": {
                    "name": "Latvian buyer",
                    "identifier": "90010937516",
                },
                "lots": [
                    {
                        "id": 526688,
                        "sequenceNumber": 1,
                        "name": "Induction loops",
                        "result": {
                            "decisionDate": "03/07/2026",
                            "winnerSelectionStatus": "selec-w",
                        },
                        "tenderingProcess": {
                            "tenderValueLowest": "9000.00",
                            "tenderValueHighest": "11000.00",
                            "receivedSubmissionsStatistics": {
                                "receivedNumberOfOffers": 2,
                            },
                        },
                        "contracts": [
                            {
                                "id": 330112,
                                "identifier": "2026/7-1",
                                "title": "Induction loops",
                                "conclusionDate": "24/07/2026",
                                "url": "https://www.eis.gov.lv/example",
                                "winners": [
                                    {
                                        "tenderIdentifier": 1,
                                        "tenderValue": 9360,
                                        "winnerBusinessParties": [
                                            {
                                                "companyId": "40003324520",
                                                "countryCode": "LVA",
                                                "isNaturalPerson": False,
                                                "name": 'SIA "DELTA AUDIO"',
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        ],
        publication_date=date(2026, 7, 27),
        source_run_id="run",
        source_object_key="notice/2026/07/27-07-2026.json",
        source_retrieved_at=datetime(2026, 7, 28, tzinfo=UTC),
        resolved_at=datetime(2026, 7, 28, tzinfo=UTC),
    )

    assert len(parsed.notices) == 1
    assert parsed.notices[0]["cloned_from"] == "notice-v1"
    assert parsed.notices[0]["directive_governed"] == "no"
    assert len(parsed.lots) == 1
    assert parsed.lots[0]["lowest_tender_amount_eur"] == "9000.00"
    assert len(parsed.winners) == 1
    winner = parsed.winners[0]
    assert winner["winner_regcode"] == "40003324520"
    assert winner["tender_value_amount_eur"] == "9360"
    assert winner["tender_value_attributable"] == 1
    assert winner["contract_conclusion_date"] == date(2026, 7, 24)
    assert winner["match_eligibility"] == "eligible"


def test_consortium_tender_value_is_preserved_but_not_party_attributable() -> None:
    parsed = parse_daily_payload(
        [
            {
                "identifier": "consortium-result",
                "formType": "result",
                "procedureLegalBasis": "law-9",
                "lots": [
                    {
                        "id": "lot-1",
                        "contracts": [
                            {
                                "id": "contract-1",
                                "winners": [
                                    {
                                        "tenderValue": "12000.00",
                                        "winnerBusinessParties": [
                                            {
                                                "companyId": "40003324520",
                                                "countryCode": "LV",
                                            },
                                            {
                                                "companyId": "40003960989",
                                                "countryCode": "LV",
                                            },
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        ],
        publication_date=date(2026, 7, 27),
        source_run_id="run",
        source_object_key="notice/2026/07/27-07-2026.json",
        source_retrieved_at=datetime(2026, 7, 28, tzinfo=UTC),
        resolved_at=datetime(2026, 7, 28, tzinfo=UTC),
    )

    assert len(parsed.winners) == 2
    assert {row["tender_value_amount_eur"] for row in parsed.winners} == {"12000.00"}
    assert {row["tender_value_attributable"] for row in parsed.winners} == {0}


def test_execution_notices_are_separate_from_awards() -> None:
    parsed = parse_daily_payload(
        [
            {
                "identifier": "execution-1",
                "noticeType": "contract-execution",
                "formType": "execution",
                "name": "Transport services",
                "procedureLegalBasis": "pil-over",
                "procurementProcedureIdentifier": "procedure-1",
                "organizationData": {
                    "name": "Municipality",
                    "identifier": "90000048472",
                },
                "draftContract": {
                    "id": 272025,
                    "contractIdentifier": "JUR-2025-07",
                    "contractTitle": "Passenger transport",
                    "contractConclusionDate": "23/07/2025",
                    "actualDurationEndDate": "30/06/2026",
                    "winners": [
                        {
                            "id": 468380,
                            "tenderValue": "2870.80",
                            "businessParty": [
                                {
                                    "companyId": "40003960989",
                                    "countryCode": "LVA",
                                    "isNaturalPerson": False,
                                    "name": "LeKS-auto",
                                }
                            ],
                        }
                    ],
                },
            }
        ],
        publication_date=date(2026, 7, 27),
        source_run_id="run",
        source_object_key="notice/2026/07/27-07-2026.json",
        source_retrieved_at=datetime(2026, 7, 28, tzinfo=UTC),
        resolved_at=datetime(2026, 7, 28, tzinfo=UTC),
    )

    assert parsed.winners == []
    assert len(parsed.executions) == 1
    assert parsed.executions[0]["winner_regcode"] == "40003960989"
    assert parsed.executions[0]["actual_end_date"] == date(2026, 6, 30)
