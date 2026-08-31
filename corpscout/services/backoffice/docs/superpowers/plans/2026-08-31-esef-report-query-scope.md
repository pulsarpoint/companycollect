# ESEF Report Query Scope Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the per-filing ESEF facts page from timing out on cold cache by scoping the concept-labels join subquery to the requested document.

**Architecture:** `reportFactsQuery` in `app/lib/esef-financial-reports.server.ts` builds a concept-labels join whose inner subquery unions the **entire** `corpscout.esef_document_concept_labels` table (taxonomy leg), a translation leg over all of `corpscout.text_translations`, and an anti-join over the whole labels table again — ClickHouse cannot push the outer `labels.source_document_id = facts.fxo_id` predicate through the `groupArrayIf`/UNION ALL, so a cold query aggregates labels for all ~25k filings to serve one. Observed: first page load for `NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0` failed with a 30 s socket timeout (`request_timeout: 30_000` on the read client, `app/lib/clickhouse.server.ts:7-25`); warm retry took 2.7 s. Fix: filter every leg of the subquery by `{documentId:String}` (already a bound param of the facts query).

**Tech Stack:** TypeScript, @clickhouse/client, vitest 4.

**Spec:** This header + the 2026-08-31 investigation findings (Handelsbanken page timeout). No separate spec doc.

## Global Constraints

- **Execute the disclosures-join plan first** (`2026-08-31-esef-disclosures-join-fix.md`) — both plans edit `reportFactsQuery`; this plan assumes that plan's shape of the file.
- Run tests from the backoffice dir: `cd corpscout/services/backoffice && npx vitest run <file>`.
- `npm run typecheck` must pass before every commit.
- Conventional Commits; stage only files this plan touches.

## File Structure

- Modify: `app/lib/esef-financial-reports.server.ts` (the `conceptLabelJoin` template only)
- Modify: `app/lib/esef-financial-reports.sql.test.ts` (created by the disclosures-join plan; add scope assertions)

---

### Task 1: Failing assertions on document scoping

**Files:**
- Modify: `app/lib/esef-financial-reports.sql.test.ts`

**Interfaces:**
- Consumes: the mock setup and `SUMMARY_ROW`/`FACT_ROW` fixtures already in that test file.
- Produces: the SQL contract Task 2 satisfies.

- [ ] **Step 1: Add a test case** to the existing `describe` block:

```ts
  it("scopes every concept-labels leg to the requested document", async () => {
    chQuery
      .mockResolvedValueOnce([SUMMARY_ROW])
      .mockResolvedValueOnce([
        { name: "esef_disclosures" },
        { name: "esef_document_concept_labels" },
      ])
      .mockResolvedValueOnce([FACT_ROW]);

    await getEsefFinancialReport(
      "se",
      "5020077862",
      "NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0",
    );

    const factsSql = String(chQuery.mock.calls[2][0]);
    // Taxonomy leg, translation leg, and the official-english anti-join must
    // all carry the document filter; three FROM/JOIN reads of the labels
    // table => three document-scoped predicates.
    const scoped = factsSql.match(
      /source_document_id = \{documentId:String\}/g,
    );
    // 1 disclosure subquery (from the join-fix plan) + 3 label legs + 1 outer WHERE... the
    // outer WHERE uses facts.fxo_id, not source_document_id, so expect exactly 4.
    expect(scoped?.length).toBe(4);
  });
```

- [ ] **Step 2: Run it — must fail**

Run: `npx vitest run app/lib/esef-financial-reports.sql.test.ts`
Expected: FAIL — currently only 1 occurrence (the disclosure subquery).

### Task 2: Filter the three label legs

**Files:**
- Modify: `app/lib/esef-financial-reports.server.ts` (`conceptLabelJoin`, lines ~125-228)

- [ ] **Step 1: Taxonomy leg** — the first SELECT inside the inner UNION gains a WHERE:

```sql
    FROM corpscout.esef_document_concept_labels
    WHERE source_document_id = {documentId:String}
```

- [ ] **Step 2: Translation leg** — add the same filter to the `source` side (append to the existing WHERE at the bottom of that leg):

```sql
    WHERE source.is_report_language
      AND source.label != ''
      AND source.language != 'en'
      AND NOT startsWith(source.language, 'en-')
      AND source.source_document_id = {documentId:String}
```

- [ ] **Step 3: Anti-join subquery** (`official_english`) — scope it too:

```sql
      SELECT DISTINCT source_document_id, concept_qname, label_role
      FROM corpscout.esef_document_concept_labels
      WHERE label != ''
        AND (language = 'en' OR startsWith(language, 'en-'))
        AND source_document_id = {documentId:String}
```

Leave the `text_translations` inner aggregate unfiltered — it is keyed by text hash, shared across documents, and already reduced by `source_table`/`source_column`/`target_lang`; the join against the now-small `source` side bounds it.

- [ ] **Step 4: Run tests + typecheck**

Run: `npx vitest run app/lib/esef-financial-reports.sql.test.ts && npm run typecheck`
Expected: PASS / clean.

- [ ] **Step 5: Commit**

```bash
git add app/lib/esef-financial-reports.server.ts app/lib/esef-financial-reports.sql.test.ts
git commit -m "perf(esef): scope concept-label subqueries to the requested filing"
```

### Task 3: Live timing verification

- [ ] **Step 1:** Restart the dev server so no warm cache, then:

```bash
time curl -s -o /dev/null -w '%{http_code}\n' \
  "http://localhost:5183/company/se/5020077862/financials/esef/NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0"
```

Expected: 200 well under 30 s on first hit (previously: timeout error page). Record before/after timing in the session summary.

- [ ] **Step 2:** Spot-check labels still render: the page must show Swedish standard labels (e.g. "Årets resultat, tillhörande aktieägare…") and English translation rows where they existed before the change (compare against a warm pre-change load if available).

## Self-Review

- Only the three label legs change; disclosure join and outer SELECT untouched. ✔
- Param `{documentId:String}` is already bound for the facts query — no loader change. ✔
- Count-based assertion (4) documented against each occurrence so a future leg addition fails loudly. ✔
