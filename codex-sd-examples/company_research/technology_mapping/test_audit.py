"""Checks for false mappings, invalid aliases, and frozen evidence integrity."""

import json
import tempfile
import unittest
from pathlib import Path

from audit import (
    alias_key,
    fingerprint,
    load_corpus,
    resolve,
    suggest_candidates,
    validate_aliases,
)


class AliasTests(unittest.TestCase):
    def setUp(self):
        self.catalog = {
            name: {"technology": name}
            for name in [
                "git",
                "GitHub",
                "C",
                "C++",
                "C#",
                "Azure",
                "Amazon Web Services",
                "Matchlab",
                "Windows Server",
                "Sonar",
            ]
        }
        self.aliases = [
            {
                "alias": "AWS",
                "alias_key": "aws",
                "technology": "Amazon Web Services",
                "review_status": "accepted",
                "match_mode": "case_insensitive",
            }
        ]

    def test_normal_catalog_matching_handles_case_without_aliases(self):
        validate_aliases(self.aliases, self.catalog)
        for spelling in ["Git", "GIT", " Git\u00a0"]:
            self.assertEqual(
                resolve(spelling, self.catalog, []),
                {
                    "status": "matched",
                    "technology": "git",
                    "method": "normalized_catalog",
                },
            )
        self.assertEqual(
            resolve("git", self.catalog, self.aliases)["method"], "exact_catalog"
        )
        for name in ["C", "C++", "C#"]:
            self.assertEqual(
                resolve(name, self.catalog, self.aliases)["technology"], name
            )
        self.assertIsNone(resolve("C/C++", self.catalog, self.aliases)["technology"])

    def test_real_alias_handles_case_and_catalog_collisions_stay_ambiguous(self):
        for spelling in ["AWS", "aws", " Aws "]:
            self.assertEqual(
                resolve(spelling, self.catalog, self.aliases)["technology"],
                "Amazon Web Services",
            )
        collisions = {"Moxie": {}, "mOxie": {}}
        result = resolve("MOXIE", collisions, [])
        self.assertEqual(result["status"], "ambiguous")
        self.assertIsNone(result["technology"])
        self.assertEqual(result["candidates"], ["Moxie", "mOxie"])
        self.assertEqual(resolve("Moxie", collisions, [])["technology"], "Moxie")

    def test_related_names_and_lexical_similarity_never_resolve(self):
        self.assertEqual(
            suggest_candidates("MATLAB", self.catalog)[0]["technology"], "Matchlab"
        )
        for name in ["MATLAB", "Azure DevOps", "Windows", "sonar hardware"]:
            self.assertEqual(
                resolve(name, self.catalog, self.aliases)["status"], "unresolved"
            )
        self.assertTrue(
            all(
                not row["automatically_accepted"]
                for row in suggest_candidates("Git", self.catalog)
            )
        )

    def test_missing_rejected_and_conflicting_alias_targets(self):
        for changes in [
            {"technology": "Missing"},
            {"review_status": "candidate"},
            {"review_status": "rejected"},
            {"match_mode": "exact"},
            {"alias": "C", "alias_key": "c", "technology": "C++"},
            {"alias": "Git", "alias_key": "git", "technology": "git"},
            {"alias_key": "wrong"},
        ]:
            with self.assertRaises(ValueError):
                validate_aliases([self.aliases[0] | changes], self.catalog)
        conflicting = self.aliases + [self.aliases[0] | {"technology": "GitHub"}]
        self.assertEqual(
            resolve("AWS", self.catalog, conflicting)["status"], "ambiguous"
        )
        with self.assertRaises(ValueError):
            validate_aliases(conflicting, self.catalog)
        self.assertIsNone(
            resolve(
                "AWS", self.catalog, [self.aliases[0] | {"review_status": "candidate"}]
            )["technology"]
        )

    def test_normalization_keeps_meaningful_characters_and_detects_duplicates(self):
        self.assertEqual(alias_key("  Git\u00a0"), "git")
        self.assertNotEqual(alias_key("C++"), alias_key("C#"))
        with self.assertRaises(ValueError):
            validate_aliases(
                self.aliases + [self.aliases[0] | {"alias": " aws "}], self.catalog
            )


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "run" / "html" / "p0001.html"
        self.source.parent.mkdir(parents=True)
        self.html = (
            "<h1>Demo Ltd</h1><p>Experience with Git or Azure DevOps is required.</p>"
        )
        self.source.write_text(self.html, encoding="utf-8")
        self.record: dict = {
            "record_id": "fixture-1",
            "data": {
                "technology": "Git",
                "signal": "required_experience",
                "alternative_group": "Git or Azure DevOps",
            },
            "evidence_status": "source_matched",
            "sources": [
                {
                    "url": "https://example.test/job/1",
                    "page_id": "p0001",
                    "fetched_at": "2026-09-07T00:00:00Z",
                    "html_sha256": fingerprint(self.html.encode()),
                    "chunk_start": 0,
                    "chunk_end": len(self.html),
                    "evidence": [
                        {
                            "text": "Experience with Git or Azure DevOps is required.",
                            "matched_in": "text",
                        }
                    ],
                    "evidence_status": "source_matched",
                    "issues": [],
                }
            ],
        }
        self.payload = {
            "schema_version": "1.1",
            "input_kind": "test",
            "page": {},
            "records": {"technology_signals": [self.record]},
        }
        self.path = self.root / "run" / "result.json"
        self.spec = [
            {
                "path": "run/result.json",
                "format": "extraction_diagnostic",
                "reason": "test",
            }
        ]

    def save(self):
        self.path.write_text(json.dumps(self.payload), encoding="utf-8")

    def test_import_preserves_qualifiers_and_does_not_modify_saved_artifact(self):
        self.save()
        before = self.path.read_bytes()
        rows, _ = load_corpus(self.root, self.spec)
        self.assertEqual(rows[0]["data"], self.record["data"])
        self.assertTrue(rows[0]["sources"][0]["evidence"][0]["presence_rechecked"])
        self.assertEqual(self.path.read_bytes(), before)

    def test_corrupted_snapshot_and_invalid_offsets_fail(self):
        self.save()
        self.source.write_text("changed", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            load_corpus(self.root, self.spec)
        self.source.write_text(self.html, encoding="utf-8")
        self.record["sources"][0]["chunk_end"] += 1
        self.save()
        with self.assertRaisesRegex(ValueError, "outside"):
            load_corpus(self.root, self.spec)

    def test_reviewable_quote_and_unassessed_schema_are_distinct(self):
        self.record["sources"][0]["evidence"][0]["text"] = "Invented quotation"
        self.record["evidence_status"] = "needs_review"
        self.save()
        rows, _ = load_corpus(self.root, self.spec)
        self.assertFalse(rows[0]["sources"][0]["evidence"][0]["presence_rechecked"])
        self.assertEqual(rows[0]["original_evidence_status"], "needs_review")
        self.payload["schema_version"] = "1.0"
        self.save()
        with self.assertRaisesRegex(ValueError, "Unsupported"):
            load_corpus(self.root, self.spec)


if __name__ == "__main__":
    unittest.main()
