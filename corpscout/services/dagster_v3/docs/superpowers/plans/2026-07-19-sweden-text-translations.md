# Sweden Text Translations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every Swedish-language text surfaced by the backoffice gets an English pairing via the established `text_translations` + `<table>_translated` view pattern (NO/LV precedent), including the XBRL concept vocabulary on the facts pages.

**Architecture:** The Go translator service is the system of record for translations — it writes `corpscout.text_translations` rows keyed by `(source_table, source_column, cityHash64(text))`. Exactly as for Norway: **only DISTINCT texts are translated** — the hash key means each unique string is translated once and every row sharing that text picks the translation up through the view join, so `se_companies` costs 1.95M translations (distinct texts), not 3.4M (rows). Dagster migrations own the ClickHouse views that join translations back onto base tables. The backoffice reads only the views and applies its existing `X_en`/`X_original` language-pair collapse.

**Tech Stack:** ClickHouse views (dagster migration), Go translator source registration, React Router backoffice registry queries.

## Inventory (measured 2026-07-19)

| Location | Content | Distinct | Mechanism |
|---|---|---|---|
| `se_companies.activity_description` | Swedish business-purpose free text | 1.95M | LLM via translator service (same class as `no_companies.articles_purpose_original`) |
| `se_financial_facts.concept_local_name` | XBRL concept vocabulary (`Nettoomsattning`, `RakenskapsarForstaDag`, …) | 1,776 | one-shot LLM translation of the bounded vocabulary |
| `se_companies.legal_form_code` | Bolagsverket/SCB codes (`AB-ORGFO`, `HB-ORGFO`, `49`, …) | 60 | curated static dictionary — codes, not prose; LLM would guess |
| `se_companies.status_reason` | deregistration-reason codes (`KKAV-AVORG`, `LIAV-AVORG`, …) | 19 | curated static dictionary |
| `se_financial_facts.text_value` | free text inside filings (notes, board statements) | 3.53M | **deferred** — huge volume, low read frequency; revisit as on-demand translation if a real use appears |
| `se_company_addresses`, names in reports/metrics | proper nouns / addresses | — | not translated (fidelity rule) |

## Global Constraints

- Translator service (Go) is the ONLY writer of `text_translations`; dagster/backoffice never insert translations.
- Views follow the existing join shape exactly: `LEFT JOIN (SELECT source_text_hash, argMax(translated_text, version) … WHERE source_table='corpscout.<t>' AND source_column='<c>' GROUP BY source_text_hash) ON hash = cityHash64(col)`, `ifNull(...,'') AS <col>_en` (see `no_companies_translated`).
- View names: `<base>_translated`. English columns: `<original_column>_en`; view must keep `c.*` (full base row — fidelity rule).
- Migration owns all CH schema; `uv run dg check defs` green; refuse-empty guards where a build step could see an empty source.
- Backoffice: registry-driven only (`countries.ts`), language handling through the existing pair-resolution helper.

---

### Task 1: Register SE sources with the translator service

**Files:** translator service source config (Go service repo — locate its source-table registration; NO/LV entries are the template).

- [ ] Register `(corpscout.se_companies, activity_description)` for sv→en translation.
- [ ] Register `(corpscout.se_financial_facts_concepts, concept_local_name)` — see Task 2 for the small distinct-concepts feed table the translator reads (translating from the 290M-row facts table directly is wasteful; NO pattern reads base tables, so give it a tiny base table).
- [ ] Confirm rows appear in `text_translations` for both pairs (spot-check `Nettoomsattning` → net revenue-ish output).

### Task 2: Concept vocabulary feed table + labels view (dagster)

**Files:**
- Create: `src/dagster_v3/defs/sweden_financial/concepts.py` (asset `se_financial_facts_concepts`)
- Modify: migration adding table `se_financial_facts_concepts` (`concept_local_name String, concept_namespace String, first_seen DateTime`) and view `se_financial_concept_labels` (concept + `label_en` via text_translations join on `cityHash64(concept_local_name)`)
- Test: `tests/test_sweden_financial_concepts.py`

- [ ] Asset inserts `SELECT DISTINCT concept_local_name, concept_namespace FROM se_financial_facts` (INSERT new concepts only — merge semantics, never replace; ~1.8k rows).
- [ ] View `se_financial_concept_labels` joins translations; wiring test asserts view name/columns; `dg check defs` green.

### Task 3: `se_companies_translated` view (dagster migration)

**Files:** migration adding view following `no_companies_translated` byte-for-byte pattern with `activity_description_en`; plus curated dictionaries.

- [ ] Create dictionary table `se_code_labels` (`code_type LowCardinality(String), code String, label_en String`) seeded from a curated in-repo dict for the 60 legal-form and 19 status-reason codes (Bolagsverket terminology; e.g. `AB-ORGFO` → "Limited company (aktiebolag)", `KKAV-AVORG` → "Deregistered after bankruptcy").
- [ ] View `se_companies_translated`: `c.*` + `activity_description_en` (text_translations join) + `legal_form_label_en`, `status_reason_label_en` (se_code_labels joins).
- [ ] Wiring test + `dg check defs`; deploy via light_sync; materialize on the server.

### Task 4: Backoffice consumption

**Files:** `services/backoffice/app/lib/countries.ts`, `app/routes/company-facts.tsx`.

- [ ] SE `recordQuery` → `se_companies_translated` so the detail page gets the `activity_description_original`/`_en` pair and the existing language toggle collapses them; legal form/status columns show the English labels.
- [ ] SE `factsQuery` LEFT JOINs `se_financial_concept_labels`: concept cell shows `label_en` with the original `concept_local_name` beneath in mono (same visual pattern as MoneyPair original/USD).
- [ ] Typecheck + verify on `http://localhost:5184/company/se/5564454345` and `/facts/2022`.

## Explicitly deferred

- `se_financial_facts.text_value` translation (3.53M distinct) — cost/benefit fails today.
- Translating other countries' equivalents is out of scope here; the pattern additions (code-label dictionaries, concept vocabulary) should be copied when FI/EE/LV financial facts pages appear.
