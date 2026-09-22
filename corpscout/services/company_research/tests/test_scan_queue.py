"""Isolated contracts for selecting the next distinct domain to scan."""

import random
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from threading import Barrier

from company_research.scan_queue import ScanEntry, ScanQueue


class ScanQueueTests(unittest.TestCase):
    def setUp(self):
        self.queue: ScanQueue[object] = ScanQueue()

    def insert(self, domain, *, priority=0, payload=None):
        entry = ScanEntry(domain=domain, priority=priority, payload=payload)
        self.assertTrue(self.queue.insert(entry))
        return entry

    def test_empty_queue_returns_none_and_empty_iterator(self):
        self.assertIsNone(self.queue.get_next())
        self.assertEqual(list(self.queue), [])
        self.assertEqual(len(self.queue), 0)

    def test_highest_priority_domain_is_next_including_zero_negative_and_large_values(
        self,
    ):
        for domain, priority in [
            ("zero.com", 0),
            ("low.com", -100),
            ("high.com", 10**100),
            ("medium.com", 12),
        ]:
            self.insert(domain, priority=priority)
        self.assertEqual(
            [entry.domain for entry in self.queue],
            ["high.com", "medium.com", "zero.com", "low.com"],
        )

    def test_equal_priorities_are_fifo_across_input_sources(self):
        entries = [
            self.insert(
                "first.com",
                priority=7,
                payload={"source": "sqlite", "submitted_at": "2030-01-01"},
            ),
            self.insert(
                "second.com",
                priority=7,
                payload={"source": "nats", "submitted_at": "2020-01-01"},
            ),
            self.insert(
                "third.com",
                priority=7,
                payload={"source": "rest", "submitted_at": "2000-01-01"},
            ),
        ]
        # Payload dictionaries are neither compared nor inspected for timestamps.
        self.assertEqual(list(self.queue), entries)

    def test_duplicate_pending_domain_does_not_add_or_replace_an_entry(self):
        first = self.insert("example.com", priority=1, payload={"source": "sqlite"})
        second = self.insert("other.com", priority=2)
        duplicate = ScanEntry(
            domain="example.com", priority=999, payload={"source": "rest"}
        )
        self.assertFalse(self.queue.insert(duplicate))
        self.assertFalse(self.queue.insert(first))
        self.assertEqual(len(self.queue), 2)
        self.assertIs(self.queue.get_next(), second)
        self.assertIs(self.queue.get_next(), first)
        self.assertIsNone(self.queue.get_next())

    def test_duplicate_does_not_change_fifo_position(self):
        first = self.insert("first.com", priority=10)
        second = self.insert("second.com", priority=10)
        self.assertFalse(
            self.queue.insert(
                ScanEntry(domain="first.com", priority=10, payload="duplicate")
            )
        )
        self.assertEqual(list(self.queue), [first, second])

    def test_removed_domain_can_be_queued_again_at_a_new_fifo_position(self):
        first = self.insert("example.com", priority=5)
        second = self.insert("other.com", priority=5)
        self.assertIs(self.queue.get_next(), first)
        again = self.insert("example.com", priority=5, payload="next scan")
        self.assertEqual(list(self.queue), [second, again])
        self.assertEqual(len(self.queue), 0)

    def test_new_high_priority_domain_overtakes_pending_domains(self):
        first = self.insert("first.com", priority=10)
        low = self.insert("low.com", priority=1)
        self.assertIs(self.queue.get_next(), first)
        urgent = self.insert("urgent.com", priority=100)
        self.assertIs(self.queue.get_next(), urgent)
        self.assertIs(self.queue.get_next(), low)

    def test_late_equal_priority_domain_joins_end_of_fifo(self):
        first = self.insert("first.com")
        second = self.insert("second.com")
        self.assertIs(self.queue.get_next(), first)
        third = self.insert("third.com")
        self.assertEqual(list(self.queue), [second, third])

    def test_iterator_observes_insertions_and_current_priorities_on_every_pull(self):
        first = self.insert("first.com", priority=5)
        second = self.insert("second.com", priority=5)
        iterator = iter(self.queue)
        self.assertIs(next(iterator), first)
        urgent = self.insert("urgent.com", priority=99)
        third = self.insert("third.com", priority=5)
        self.assertEqual(list(iterator), [urgent, second, third])

    def test_iterator_does_not_remove_anything_until_pulled(self):
        old = self.insert("old.com", priority=1)
        iterator = iter(self.queue)
        urgent = self.insert("urgent.com", priority=10)
        self.assertEqual(len(self.queue), 2)
        self.assertEqual(list(iterator), [urgent, old])

    def test_exhausted_iterator_stays_exhausted_but_queue_can_be_refilled(self):
        iterator = iter(self.queue)
        self.assertEqual(list(iterator), [])
        entry = self.insert("new.com")
        self.assertEqual(list(iterator), [])
        self.assertEqual(list(self.queue), [entry])

    def test_iterators_and_get_next_share_removals_without_duplicates(self):
        entries = [self.insert(f"domain-{index}.com") for index in range(5)]
        first, second = iter(self.queue), iter(self.queue)
        self.assertIs(next(first), entries[0])
        self.assertIs(next(second), entries[1])
        self.assertIs(self.queue.get_next(), entries[2])
        self.assertEqual(list(first), entries[3:])
        self.assertEqual(list(second), [])

    def test_none_payload_is_an_entry_not_the_empty_queue_sentinel(self):
        entry = self.insert("example.com", payload=None)
        self.assertEqual(list(self.queue), [entry])

    def test_metadata_is_frozen_and_payload_is_retained_by_reference(self):
        payload = {"instructions": "Find financial reports"}
        entry = self.insert("example.com", priority=10, payload=payload)
        for field, value in [("priority", 999), ("domain", "other.com")]:
            with self.subTest(field=field), self.assertRaises(FrozenInstanceError):
                setattr(entry, field, value)
        result = self.queue.get_next()
        self.assertIs(result, entry)
        self.assertIs(result.payload, payload)

    def test_domain_case_whitespace_and_root_dot_cannot_create_duplicates(self):
        entry = self.insert("  EXAMPLE.COM.  ")
        self.assertEqual(entry.domain, "example.com")
        duplicate = ScanEntry(domain="Example.Com", priority=99, payload="duplicate")
        self.assertFalse(self.queue.insert(duplicate))
        self.assertEqual(len(self.queue), 1)
        self.assertIs(self.queue.get_next(), entry)

    def test_idna_domain_is_supplied_by_the_input_adapter(self):
        entry = self.insert("XN--BCHER-KVA.de")
        self.assertEqual(entry.domain, "xn--bcher-kva.de")
        self.assertIs(self.queue.get_next(), entry)
        with self.assertRaises(ValueError):
            self.insert("bücher.de")

    def test_distinct_hostnames_are_not_merged(self):
        domains = [
            "example.com",
            "www.example.com",
            "jobs.example.com",
            "a.github.io",
            "b.github.io",
        ]
        for domain in domains:
            self.insert(domain)
        self.assertEqual([entry.domain for entry in self.queue], domains)

    def test_invalid_domains_are_rejected(self):
        for domain in [
            "",
            " ",
            ".",
            "example..com",
            "example.com..",
            "https://example.com",
            "example.com:443",
            "example.com/path",
            "user@example.com",
            "exa mple.com",
            "example_.com",
            "-example.com",
            "example-.com",
            "a" * 64 + ".com",
            ".".join(["a" * 63] * 4),
        ]:
            with self.subTest(domain=domain), self.assertRaises(ValueError):
                self.insert(domain)
        self.assertEqual(len(self.queue), 0)

    def test_invalid_types_are_rejected_without_changing_queue(self):
        for domain in [None, 42, b"example.com"]:
            with self.subTest(domain=domain), self.assertRaises(TypeError):
                self.insert(domain)
        for priority in [None, True, False, 1.0, float("nan"), float("inf"), "1"]:
            with self.subTest(priority=priority), self.assertRaises(TypeError):
                self.insert("example.com", priority=priority)
        with self.assertRaises(TypeError):
            self.queue.insert({"domain": "example.com", "priority": 1})
        self.assertEqual(len(self.queue), 0)

    def test_queue_instances_do_not_share_domains(self):
        other: ScanQueue[object] = ScanQueue()
        entry = self.insert("example.com")
        self.assertTrue(other.insert(entry))
        self.assertIs(other.get_next(), entry)
        self.assertEqual(len(self.queue), 1)
        self.assertIs(self.queue.get_next(), entry)

    def test_seeded_mixed_operations_match_simple_reference_model(self):
        randomizer = random.Random(91723)
        domains = [f"company-{index}.com" for index in range(128)]
        pending = {}
        for sequence in range(5000):
            if randomizer.random() < 0.65:
                domain = randomizer.choice(domains)
                entry = ScanEntry(
                    domain=domain,
                    priority=randomizer.randrange(-3, 4),
                    payload=sequence,
                )
                accepted = self.queue.insert(entry)
                self.assertEqual(accepted, domain not in pending)
                if accepted:
                    pending[domain] = (sequence, entry)
            elif pending:
                expected = sorted(
                    pending.values(), key=lambda value: (-value[1].priority, value[0])
                )[0][1]
                self.assertIs(self.queue.get_next(), expected)
                del pending[expected.domain]
            else:
                self.assertIsNone(self.queue.get_next())
            self.assertEqual(len(self.queue), len(pending))
        expected = [
            entry
            for _, entry in sorted(
                pending.values(), key=lambda value: (-value[1].priority, value[0])
            )
        ]
        self.assertEqual(list(self.queue), expected)
        self.assertEqual(len(self.queue), 0)

    def test_concurrent_insertions_of_same_domain_accept_exactly_one(self):
        start = Barrier(8)

        def produce(producer):
            start.wait(timeout=5)
            entry = ScanEntry(
                domain="EXAMPLE.com." if producer % 2 else "example.com",
                priority=producer,
                payload=producer,
            )
            return self.queue.insert(entry), entry

        with ThreadPoolExecutor(max_workers=8) as workers:
            results = list(workers.map(produce, range(8)))
        accepted = [entry for inserted, entry in results if inserted]
        self.assertEqual(len(accepted), 1)
        self.assertEqual(len(self.queue), 1)
        self.assertIs(self.queue.get_next(), accepted[0])

    def test_concurrent_producers_preserve_each_producers_fifo(self):
        start = Barrier(8)

        def produce(producer):
            start.wait(timeout=5)
            for sequence in range(200):
                self.insert(
                    f"company-{producer}-{sequence}.com",
                    priority=1,
                    payload=(producer, sequence),
                )

        with ThreadPoolExecutor(max_workers=8) as workers:
            list(workers.map(produce, range(8)))
        entries = list(self.queue)
        self.assertEqual(len(entries), 1600)
        for producer in range(8):
            self.assertEqual(
                [entry.payload[1] for entry in entries if entry.payload[0] == producer],
                list(range(200)),
            )
        self.assertEqual(len(self.queue), 0)

    def test_concurrent_consumers_remove_each_domain_exactly_once(self):
        for index in range(2000):
            self.insert(f"company-{index}.com", priority=index % 7, payload=index)
        start = Barrier(8)

        def consume(_):
            start.wait(timeout=5)
            return [entry.payload for entry in self.queue]

        with ThreadPoolExecutor(max_workers=8) as workers:
            results = list(workers.map(consume, range(8)))
        self.assertEqual(
            sorted(value for result in results for value in result), list(range(2000))
        )
        self.assertEqual(len(self.queue), 0)


if __name__ == "__main__":
    unittest.main()
