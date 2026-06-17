# Norway Legal Form And Translation Queue Seed Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add deterministic Norway BRREG legal-form enrichment and seed the generic translation queue only with free-text company fields.

**Architecture:** Legal-form labels are deterministic reference data, not LLM work. Norway queue seeding reads `norway_brreg.entities` from DuckDB and enqueues only free-text original fields for Temporal translation.

**Tech Stack:** Python, Dagster assets, DuckDB, pytest.

---

## Tasks

### Task 1: Legal Form Mapping

- [ ] Add tests for known BRREG legal forms.
- [ ] Implement Norway legal-form mapping helper.
- [ ] Update entity row construction to populate `legal_form_description_en`.

### Task 2: Norway Translation Queue Seed

- [ ] Add tests proving queue seed includes only `articles_purpose_original`, `activity_text_original`, and `company_description_original`.
- [ ] Implement `norway_brreg_translation_queue_seeded` asset and helper.
- [ ] Verify NACE/legal-form descriptions are excluded from LLM queue.

### Task 3: Verification

- [ ] Run Norway and translation focused tests.
- [ ] Run `uv run dg check defs`.
- [ ] Run full pytest suite.
