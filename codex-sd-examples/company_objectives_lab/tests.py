"""Behavior checks for evidence retention, candidate coverage and inference isolation."""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import patch

import httpx

from company_objectives_lab.collect import freeze_extraction_inputs
from company_objectives_lab.models import OBJECTIVES, Extraction, Selection
from company_objectives_lab.prompts import extraction_prompt, selection_prompt
from company_objectives_lab.report import (
    audit_run,
    extraction_scores,
    matches_fields,
    schedule_pages,
    selection_stability,
)
from company_objectives_lab.run import prepare_tasks, run_stage
from company_objectives_lab.validation import (
    retain_attempts,
    validate_extraction,
    validate_selection,
)
from jobs_extraction_lab.corpus import content_hash, write_json
from jobs_extraction_lab.extract import extract_openrouter
from jobs_extraction_lab.models import JobExtraction


def assessment(candidate_id: str) -> dict:
    return {
        "candidate_id": candidate_id,
        "objectives": {k: {"potential": "low", "role": "none"} for k in OBJECTIVES},
        "reason": "No supporting metadata.",
    }


class ValidationTests(unittest.TestCase):
    def test_repeat_coverage_excludes_candidates_not_planned_for_repeat(self):
        first = {
            "tasks": [
                {"task_id": "a", "domain": "example", "candidate_ids": ["c1", "c2"]}
            ],
            "results": {"a": {"accepted": [assessment("c1"), assessment("c2")]}},
        }
        second = {
            "tasks": [{"task_id": "a", "domain": "example", "candidate_ids": ["c1"]}],
            "results": {"a": {"accepted": [assessment("c1")]}},
        }
        result = selection_stability(first, second)
        self.assertEqual(result["planned_common"], 1)
        self.assertEqual(result["first_only"], 0)
        self.assertEqual(result["by_objective"]["jobs"]["common_assessments"], 1)

    def test_raw_checkpoint_match_does_not_bypass_evidence_validation(self):
        person = {
            "name": "Ada",
            "company": "Example",
            "role": "CEO",
            "profile_url": None,
            "as_of": None,
            "evidence": "CEO Ada",
        }
        document = {k: [] for k in Extraction.model_fields}
        document["people"] = [person]
        checked = validate_extraction(
            json.dumps(document), "stop", "https://example.com", "<p>CEO: Ada</p>"
        )
        run = {
            "results": {"p1": {"accepted": []}},
            "attempts": [
                {"task_id": "p1", "error": None, "finish_reason": "stop", **checked}
            ],
        }
        reference = {
            "checks": [
                {
                    "check_id": "f1",
                    "page_id": "p1",
                    "objective": "people",
                    "expected": {"name": "Ada", "role": "CEO"},
                    "alternatives": [],
                }
            ],
            "negative_checks": [],
        }
        counts = extraction_scores(run, reference)["by_objective"]["people"]
        self.assertEqual(counts["raw_schema_matched"], 1)
        self.assertEqual(counts["matched"], 0)

    def test_freezing_uses_actual_redirect_and_preserves_unmodified_error_page(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            html = "<p>Page not found</p>"
            (root / "p1.html").write_text(html, encoding="utf-8")
            collection = {
                "pages": [
                    {
                        "page_id": "p1",
                        "domain": "example.com",
                        "success": True,
                        "url": "https://example.com/jobs",
                        "final_url": "https://careers.example.com/search/",
                        "status_code": 404,
                        "cleaned_html": {
                            "file": "p1.html",
                            "sha256": content_hash(html),
                        },
                    }
                ]
            }
            write_json(root / "collection.json", collection)
            (root / "p1.html").write_text("Changed", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Snapshot changed"):
                freeze_extraction_inputs(root)
            self.assertFalse((root / "extraction-inputs.json").exists())
            (root / "p1.html").write_text(html, encoding="utf-8")
            freeze_extraction_inputs(root)
            page = json.loads((root / "extraction-inputs.json").read_text())["pages"][0]
            self.assertEqual(page["source_url"], "https://careers.example.com/search/")
            self.assertEqual(page["status_code"], 404)
            self.assertEqual((root / "p1.html").read_text(), html)
            with self.assertRaisesRegex(ValueError, "already frozen"):
                freeze_extraction_inputs(root)

    def test_checkpoints_require_contact_owner_and_relationship_direction(self):
        self.assertFalse(
            matches_fields(
                {"owner": "Switchboard", "value": "+44 123", "type": "phone"},
                {"owner": "Ada", "value": "+44-123", "type": "phone"},
            )
        )
        self.assertTrue(
            matches_fields(
                {"owner": "Ada", "value": "+44 123", "type": "phone"},
                {"owner": "Ada", "value": "+44-123", "type": "phone"},
            )
        )
        self.assertFalse(
            matches_fields(
                {"subject": "Parent", "relationship": "parent_of", "object": "Child"},
                {"subject": "Child", "relationship": "parent_of", "object": "Parent"},
            )
        )

    def test_service_failure_cannot_pass_an_absence_check(self):
        run = {
            "results": {"p1": {"accepted": []}},
            "attempts": [
                {
                    "task_id": "p1",
                    "error": "timeout",
                    "finish_reason": None,
                    "issues": [],
                }
            ],
        }
        reference = {
            "checks": [],
            "negative_checks": [{"page_id": "p1", "objective": "jobs", "empty": True}],
        }
        outcome = extraction_scores(run, reference)["negative_controls"][0]
        self.assertFalse(outcome["observable"])
        self.assertFalse(outcome["passed"])

    def test_offline_scheduler_shares_budget_across_objectives_without_duplicates(self):
        site = {
            "domain": "example",
            "candidates": [
                {"candidate_id": c, "url": f"https://example/{c}"}
                for c in ("profile", "jobs", "noise")
            ],
        }
        entries = {("example", c): assessment(c) for c in ("profile", "jobs", "noise")}
        entries[("example", "profile")]["objectives"]["company_profile"] = {
            "potential": "high",
            "role": "direct",
        }
        entries[("example", "jobs")]["objectives"]["jobs"] = {
            "potential": "high",
            "role": "navigation",
        }
        self.assertEqual(set(schedule_pages(site, entries, 2)), {"profile", "jobs"})

    def test_missing_and_unknown_candidates_do_not_remove_valid_assessments(self):
        raw = json.dumps({"assessments": [assessment("c1"), assessment("invented")]})
        result = validate_selection(raw, "stop", {"c1", "c2"})
        self.assertEqual([a["candidate_id"] for a in result["accepted"]], ["c1"])
        self.assertEqual(
            result["issues"][-1],
            {"type": "missing_candidates", "candidate_ids": ["c2"]},
        )

    def test_each_objective_is_required_and_potential_must_match_role(self):
        a = assessment("c1")
        del a["objectives"]["people"]
        result = validate_selection(json.dumps({"assessments": [a]}), "stop", {"c1"})
        self.assertEqual(result["accepted"], [])
        a = assessment("c1")
        a["objectives"]["people"] = {"potential": "high", "role": "none"}
        result = validate_selection(json.dumps({"assessments": [a]}), "stop", {"c1"})
        self.assertEqual(result["issues"][0]["type"], "inconsistent_potential_role")

    def test_independent_records_survive_schema_evidence_and_url_failures(self):
        data = {k: [] for k in Extraction.model_fields}
        good = {
            "name": "Ada",
            "role": "CEO",
            "company": "Example",
            "profile_url": "https://example.com/team/ada",
            "as_of": None,
            "evidence": "Ada CEO",
        }
        data["people"] = [
            good,
            {**good, "name": "Invented", "evidence": "Invented CEO"},
            {**good, "profile_url": "https://example.com/invented"},
            {"name": "Broken"},
        ]
        result = validate_extraction(
            json.dumps(data),
            "stop",
            "https://example.com/team",
            '<a href="/team/ada">Ada <b>CEO</b></a>',
        )
        self.assertEqual(len(result["accepted"]), 1)
        self.assertEqual(len(result["rejected"]), 3)
        self.assertEqual(result["accepted"][0]["record"], good)

    def test_malformed_or_truncated_json_is_not_partially_salvaged(self):
        self.assertEqual(
            validate_selection('{"assessments": [', "stop", {"c1"})["accepted"], []
        )
        self.assertEqual(
            validate_selection('{"assessments": []}', "length", {"c1"})["accepted"], []
        )

    def test_correction_retains_initial_assessment_and_all_observations(self):
        first, second = assessment("c1"), assessment("c1")
        second["objectives"]["people"] = {"potential": "high", "role": "direct"}
        result = retain_attempts(
            "selection",
            [{"accepted": [first]}, {"accepted": [second, assessment("c2")]}],
        )
        self.assertEqual(len(result["accepted"]), 2)
        self.assertEqual(result["accepted"][0]["objectives"], first["objectives"])
        self.assertEqual(len(result["alternatives"]), 1)

    def test_prompt_has_all_objectives_but_no_external_references(self):
        for prompt in (
            selection_prompt("https://example.com", []),
            extraction_prompt("https://example.com", "<p>Example</p>"),
        ):
            self.assertTrue(all(k in prompt for k in OBJECTIVES))
            self.assertNotIn("reference.json", prompt)


class TransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_custom_schema_and_original_job_schema_are_sent_at_http_boundary(
        self,
    ):
        bodies = []

        def respond(request):
            bodies.append(json.loads(request.content))
            return httpx.Response(
                200,
                json={
                    "choices": [{"message": {"content": "{}"}, "finish_reason": "stop"}]
                },
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            options: dict[str, Any] = {
                "api_key": "fake-key",
                "model": "test-model",
                "reasoning": {},
                "provider_options": {},
                "timeout": 5,
                "attempts": 1,
                "max_tokens": 500,
            }
            await extract_openrouter(client, "test", **options)
            await extract_openrouter(
                client,
                "test",
                response_schema=Selection.model_json_schema(),
                schema_name="company_selection",
                **options,
            )
        self.assertEqual(
            bodies[0]["response_format"]["json_schema"],
            {
                "name": "job_extraction",
                "strict": True,
                "schema": JobExtraction.model_json_schema(),
            },
        )
        self.assertEqual(
            bodies[1]["response_format"]["json_schema"]["schema"],
            Selection.model_json_schema(),
        )

    async def test_saved_run_resumes_without_inference_and_changed_inputs_are_rejected(
        self,
    ):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = {
                "sites": [
                    {
                        "domain": "example.com",
                        "base_url": "https://example.com",
                        "candidates": [
                            {
                                "candidate_id": "c1",
                                "url": "https://example.com/team",
                                "title": "Team",
                            }
                        ],
                    }
                ]
            }
            write_json(root / "candidates.json", source)
            (root / ".env").write_text(
                "OPENROUTER_API_KEY=fake-key\n", encoding="utf-8"
            )
            payload = {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps({"assessments": [assessment("c1")]})
                        },
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"cost": 0.001},
            }
            with patch(
                "company_objectives_lab.run.extract_openrouter", return_value=payload
            ) as call:
                await run_stage(root, "selection", "test", root / ".env", 1, 40, 1000)
                await run_stage(root, "selection", "test", root / ".env", 1, 40, 1000)
                self.assertEqual(call.call_count, 1)
                self.assertEqual(
                    audit_run(root, "test")["results"]["example.com-000"]["accepted"][
                        0
                    ]["candidate_id"],
                    "c1",
                )
                attempt_path = root / "runs/test/attempts/example.com-000/1.json"
                original = attempt_path.read_text(encoding="utf-8")
                tampered = json.loads(original)
                tampered["raw_response"] = json.dumps({"assessments": []})
                write_json(attempt_path, tampered)
                with self.assertRaisesRegex(ValueError, "validation does not match"):
                    audit_run(root, "test")
                attempt_path.write_text(original, encoding="utf-8")
                # The repeat uses the same shuffled first batch for every company.
                self.assertEqual(
                    prepare_tasks(root, "selection", 40, 1),
                    prepare_tasks(root, "selection", 40),
                )
                source["sites"][0]["candidates"][0]["title"] = "Changed"
                write_json(root / "candidates.json", source)
                with self.assertRaisesRegex(ValueError, "settings changed"):
                    await run_stage(
                        root, "selection", "test", root / ".env", 1, 40, 1000
                    )
                self.assertEqual(call.call_count, 1)
