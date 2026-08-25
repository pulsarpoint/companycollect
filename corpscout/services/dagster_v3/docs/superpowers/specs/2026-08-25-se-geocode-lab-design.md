# Geocode analysis agent + Geocoding tab — design (v2, supersedes the terminal-lab v1)

Owner decisions (chat, 2026-08-25): agent triggered from backoffice UI, not a terminal harness;
universal per-country with parameters; TypeScript module in backoffice; **OpenAI Codex SDK**
(deliberate choice — provider flexibility, not Anthropic-only); agent has **memory** and stores
suggestions in the **review-queue Postgres** (deployed 2026-08-22). Exact-only/caching from v1 is
dead: the agent produces SUGGESTIONS; serving changes flow only through golden-gated policy bumps.

## 1. Backoffice tab "Geocoding" on /admin/se/company-info
- Columns: Company (id+name, linked) | Address (published) | Geocode status
  (geocoded / ambiguous / unmatched / no outcome).
- Default filter: has address but no geocode mapping; status filter switchable; class counts in header.
- Reads the same sources as the address tab (se_company_address + links + se_address_geocodes_current)
  so tab and company page can never disagree. SE-first full fidelity; query shaped for other countries.

## 2. Analysis agent (backoffice TypeScript, @openai/codex-sdk)
- Separate module `app/agents/geocode-analysis.server.ts` (+ thin route action to trigger).
- Parameters: `country` (required), optional focus/continue directive.
- Context/tools given to the agent: read-only ClickHouse (unmatched pool, matched exemplars,
  store outcome stats), its own Postgres memory + prior suggestions, a report writer.
- Per run: cluster unmatched addresses into patterns, test hypotheses against matched exemplars,
  quantify expected yield, emit concrete dagster suggestions (augmentation rules, with examples
  and counts). Re-trigger goes deeper on the remainder using memory; reports convergence when no
  further classes are found.
- Runs are minutes-long: fire-and-poll (run row in Postgres carries status), never a blocking request.

## 3. Postgres persistence (review-queue instance, postgresqueue)
- `geocode_agent_runs` (id, country, params, status queued/running/done/failed, model, started/finished,
  report_md).
- `geocode_agent_suggestions` (id, run_id, country, pattern, description, expected_yield,
  examples jsonb, status new/accepted/implemented/rejected).
- `geocode_agent_memory` (country, key, content, updated_at) — durable notes injected into the next
  run's context (what was tried, what converged, register quirks learned).
- Schema managed the same way the existing review-queue tables are; backoffice reuses its existing
  Postgres connection.

## 4. UI around the agent
- Trigger button on the tab (country pre-filled), run history with status, rendered report,
  suggestions list with lifecycle (accept → becomes a dagster policy-bump work item; implemented
  suggestions link to the policy version that shipped them).

## Guardrails
- Agent is read-only on ClickHouse; writes only to its three Postgres tables; NEVER writes the
  geocode store; no deploys, no Dagster triggers.
- Graduation path unchanged: accepted suggestion → policy bump (golden-corpus gated) → full rematch.
- Phase 2 (separate dagster task, later): publish the OSM address reference to ClickHouse so the
  agent can verify candidate streets directly instead of statistically.
