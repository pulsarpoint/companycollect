import json
import unittest
from unittest.mock import AsyncMock

import httpx
from test_company_lookup import COMPANY, CONTACT, FACTS

from crawler_service.company_industry import (
    checked_industry_assessments,
    load_company_industries,
)
from crawler_service.company_lookup import (
    CompanyAssessment,
    accept_match,
    compare_industries,
    parse_identity,
)
from crawler_service.llm import ModelBudgetExceeded, ModelUnavailable

INDUSTRY = {
    "company_id": COMPANY["company_id"],
    "classification_system": "NACE_REV2",
    "classification_code": "7112",
    "classification_version": "NACE_REV_2",
    "reported_label": "Engineering activities and related technical consultancy",
    "reference_label": "Engineering activities and related technical consultancy",
    "is_primary": 1,
    "source": "sweden_scb",
    "source_record_uid": "test",
    "reference_status": "reference_consistent",
}
CHECK = {
    "company_id": COMPANY["company_id"],
    "status": "conflicting",
    "reasons": ["The website activity contradicts the registered industry"],
}


class IndustryTests(unittest.IsolatedAsyncioTestCase):
    async def test_auxiliary_failure_and_missing_data_cannot_discard_identity(self):
        for error in (ModelUnavailable("offline"), ModelBudgetExceeded("budget")):
            model = AsyncMock()
            model.ask.side_effect = error
            self.assertEqual(
                await compare_industries(
                    model,
                    None,
                    [FACTS[-1]],
                    [{"company_id": COMPANY["company_id"], "industries": [INDUSTRY]}],
                ),
                [],
            )
            self.assertEqual(await compare_industries(model, None, [], []), [])
            self.assertEqual(model.ask.await_count, 1)

    async def test_batched_readonly_query_preserves_versions_and_marks_bad_mappings(
        self,
    ):
        rows = [
            INDUSTRY,
            INDUSTRY | {"is_primary": 0},
            INDUSTRY
            | {
                "classification_code": "3512",
                "reported_label": "Renewable electricity",
            },
            INDUSTRY | {"classification_code": "6421", "reference_label": ""},
            INDUSTRY | {"classification_code": "other", "classification_version": ""},
        ]

        def respond(request):
            self.assertEqual(request.url.params["readonly"], "2")
            self.assertEqual(
                request.url.params["param_ids"], repr([COMPANY["company_id"]])
            )
            sql = request.content.decode()
            self.assertIn("{ids:Array(String)}", sql)
            self.assertIn("GROUP BY classification_version, normalized_code", sql)
            self.assertNotIn("is_current", sql)
            self.assertNotIn(COMPANY["company_id"], sql)
            return httpx.Response(200, text="\n".join(json.dumps(row) for row in rows))

        candidates, searches = [dict(COMPANY)], []
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(respond), base_url="http://database"
        ) as http:
            await load_company_industries(http, candidates, searches)
        industries = candidates[0]["industries"]
        self.assertEqual(len(industries), 4)
        self.assertEqual(industries[0]["is_primary"], 1)
        self.assertEqual(
            [row["reference_status"] for row in industries],
            [
                "reference_consistent",
                "label_version_conflict",
                "invalid_code_for_version",
                "unknown_version",
            ],
        )
        self.assertEqual(searches[0]["status"], "completed")
        self.assertEqual(searches[0]["row_count"], 5)
        self.assertGreaterEqual(searches[0]["duration_ms"], 0)

    async def test_failed_query_is_visible_and_invalid_ids_never_reach_database(self):
        searches = []
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(503)),
            base_url="http://database",
        ) as http:
            with self.assertRaisesRegex(RuntimeError, "industry lookup failed"):
                await load_company_industries(http, [dict(COMPANY)], searches)
            with self.assertRaises(ValueError):
                await load_company_industries(
                    http, [COMPANY | {"company_id": "bad'identifier"}], searches
                )
        self.assertEqual(len(searches), 1)
        self.assertEqual(searches[0]["status"], "failed")

    def test_missing_activity_codes_and_uncertain_mapping_remain_neutral(self):
        for industries, facts in [
            ([INDUSTRY], FACTS[:1]),
            ([], FACTS),
            ([INDUSTRY | {"reference_status": "label_version_conflict"}], FACTS),
            (
                [INDUSTRY, INDUSTRY | {"reference_status": "invalid_code_for_version"}],
                FACTS,
            ),
            ([INDUSTRY | {"classification_code": "7010"}], FACTS),
        ]:
            with self.subTest(industries=industries, facts=facts):
                result = checked_industry_assessments(
                    [CHECK], [COMPANY | {"industries": industries}], facts
                )[0]
                self.assertEqual(result["status"], "insufficient_evidence")
                self.assertEqual(result["model_status"], "conflicting")

    def test_duplicate_assessments_are_neutral_but_supported_conflict_is_preserved(
        self,
    ):
        candidate = COMPANY | {"industries": [INDUSTRY]}
        self.assertEqual(
            checked_industry_assessments([CHECK, CHECK], [candidate], FACTS)[0][
                "status"
            ],
            "insufficient_evidence",
        )
        self.assertEqual(
            checked_industry_assessments([CHECK], [candidate], FACTS)[0]["status"],
            "conflicting",
        )
        self.assertEqual(
            checked_industry_assessments(
                [CHECK | {"status": "consistent"}], [candidate], FACTS
            )[0]["status"],
            "consistent",
        )

    def test_industry_conflict_can_reject_name_only_but_never_verified_operator_id(
        self,
    ):
        assessment = CompanyAssessment(
            company_id=COMPANY["company_id"],
            confidence=0.99,
            basis="unique_legal_name",
            reasons=["Name agrees"],
            industry_checks=[CHECK],
        )
        accepted, reason = accept_match(assessment, [COMPANY], FACTS[:1])
        self.assertIsNone(accepted)
        self.assertIn("industry", reason)
        self.assertEqual(accept_match(assessment, [COMPANY], FACTS)[0], COMPANY)
        assessment.industry_checks[0].status = "consistent"
        self.assertEqual(accept_match(assessment, [COMPANY], FACTS[:1])[0], COMPANY)
        unrelated = COMPANY | {"legal_name": "Unrelated AB"}
        self.assertIsNone(accept_match(assessment, [unrelated], FACTS[:1])[0])

    def test_malformed_industry_response_does_not_discard_valid_identity(self):
        for checks in (
            None,
            "invalid",
            [{"company_id": COMPANY["company_id"], "status": "maybe"}],
        ):
            assessment = CompanyAssessment(
                company_id=COMPANY["company_id"],
                confidence=0.99,
                basis="registration_number",
                reasons=["ID agrees"],
                industry_checks=checks,
            )
            self.assertEqual(assessment.industry_checks, [])
            self.assertEqual(accept_match(assessment, [COMPANY], FACTS)[0], COMPANY)

    def test_activity_must_be_a_verifiable_quote(self):
        activity = FACTS[-1]
        self.assertEqual(
            parse_identity(
                {"facts": [activity], "reasons": ["Website activity"]},
                {CONTACT: activity["quote"]},
            )[0],
            [activity],
        )
        self.assertEqual(
            parse_identity(
                {"facts": [activity], "reasons": ["Website activity"]},
                {CONTACT: "Unrelated text"},
            )[0],
            [],
        )
