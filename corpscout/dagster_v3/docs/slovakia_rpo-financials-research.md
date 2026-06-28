# Slovakia — financial data & contact information research

Starting point for extending the `slovakia_rpo` source beyond the register. Captured
2026-06-21 by probing the live registers. **Verdict: Slovakia has excellent, free,
*structured* financial data (better than Czech — no PDF/OCR needed); it has NO contact
information in any official open register (same as Czech/RPO).**

---

## 1. Contact information — NOT available from official open data

Confirmed by inspection of both official registers:

| Register | Contacts? | Notes |
|---|---|---|
| **RPO** (`rpo2.organizations`) | ✗ | No email/phone/web fields (verified in the dump JSON). |
| **RÚZ** entity (`uctovna-jednotka`) | ✗ | Fields are `ico, dic, nazovUJ, skNace, pravnaForma, sidlo/kraj/okres, psc, mesto, ulica, datumZalozenia/Zrusenia` — **no contact fields**. |

→ Slovak contacts must come from the **domain-first** path (same plan as Czech): match
existing `.sk` domains → `sk_companies`, then enrich. There is no register-sourced email.
A future `sk_company_domains` feeder would mirror `estonia_ar`'s domain logic but seeded
from the domains side, since RPO has no website field to derive from.

---

## 2. Financial data — RÚZ (Register účtovných závierok)

**The official Register of Financial Statements.** Every Slovak accounting entity is
legally required to file here. Free, open, **no API key**, public REST API. This is the
single best Slovak financial source and is *structured numeric data*, not PDF-only.

- Base URL: `https://www.registeruz.sk/cruz-public/api`
- Docs: `https://www.registeruz.sk/cruz-public/home/api`
- **WAF note:** the `cisExt/rest/api` path and bare `curl`/WebFetch UAs are F5-WAF-rejected.
  Use the `cruz-public/api` path with a browser `User-Agent` header. (Our HTTP layer
  already sets a UA; keep it browser-like.)

### Object model
```
účtovná jednotka (entity)  --1:N-->  účtovná závierka (statement/filing)
účtovná závierka           --1:N-->  účtovný výkaz (report: balance sheet / P&L / title)
účtovný výkaz.obsah.tabulky[i].data[]   aligns positionally with
šablóna.tabulky[i].riadky[]             (template defines the row meaning)
```

### Endpoints (all GET, JSON)
| Endpoint | Purpose |
|---|---|
| `/uctovne-jednotky?zmenene-od=YYYY-MM-DD&pokracovat-za-id=N&max-zaznamov=M[&ico=...]` | Page entity **ids** changed since a date (or filter by IČO). Returns `{"id":[...],"existujeDalsieId":bool}`. **This is the bulk/incremental driver.** |
| `/uctovna-jednotka?id=ID` | Entity detail (see fields above). |
| `/uctovne-zavierky?zmenene-od=...&pokracovat-za-id=...` | Page statement ids (incremental feed). |
| `/uctovna-zavierka?id=ID` | Statement detail: `idUJ`, `obdobieOd/Do`, `typ`, `datumPodania`, `idUctovnychVykazov:[...]`, `stav` (watch for `ZMAZANÉ` = deleted). |
| `/uctovny-vykaz?id=ID` | Report: `idSablony`, `pristupnostDat` (`Verejné`/`Neverejné`), `obsah.tabulky[]`. |
| `/sablona?id=ID` | Template: `nazov` (e.g. `Úč MUJ`), `tabulky[].riadky[]` with `cisloRiadku`, `oznacenie` (`A.`, `A.I.`), `text.sk`. |
| `/priloha?id=ID` | PDF/attachment (we don't need it — numbers are in `obsah`). |

### What the numeric content looks like (verified)
For a public report (`pristupnostDat:"Verejné"`), `obsah.tabulky` is e.g.:
```
[ {"nazov":{"sk":"Strana aktív"},  "data":["58789","64361","3277",...]},
  {"nazov":{"sk":"Strana pasív"},  "data":[...]},
  {"nazov":{"sk":"Výkaz ziskov a strát"}, "data":[...]} ]
```
`data[]` is the column(s) of numbers in template-row order. Templates carry **current +
previous period** columns, so positions interleave/alternate per the šablóna — must be
decoded against `/sablona?id=idSablony`. Values are strings (EUR, post-2009).

### Caveats found while probing
- **Older filings are `Neverejné`** with empty `obsah` (pre-RÚZ-era FRSR data, ~pre-2014).
  Only `Verejné` reports carry numbers. Filter on `pristupnostDat`.
- **Template families** drive row layout — at least: `Úč MUJ` (micro), `Úč POD`
  (businesses, double-entry), `Úč NUJ` (non-profit), plus bank/insurance variants. Each
  `idSablony` needs its own row→metric map (like the Finland XBRL metric map / Estonia EAV
  element map). Build a small `sk_financial_metric_map` keyed on (template family,
  `cisloRiadku`) → metric name (revenue, total_assets, equity, net_result, etc.).
- Some recently-approved statements briefly carry only `titulnaStrana` (title page) until
  the tables are processed — skip reports whose `obsah` has no `tabulky`.
- Provides **DIČ** (tax id) which RPO lacks — worth carrying onto `sk_companies` later.

### Comparison to Czech
Czech (`dataor.justice.cz`) required scraping + PDF/OCR + LLM. **Slovakia is far easier:**
a documented JSON API with structured numbers and a clean incremental feed. No OCR/LLM.

---

## 3. `sk_financials` build — IMPLEMENTED (`slovakia_financials` module)

> **STATUS (2026-06-21): built.** Module `defs/slovakia_financials` →
> `corpscout.sk_financial_metrics` (migration 000043). Statement-id cursor sweep
> (`pokracovat-za-id`) over `/uctovne-zavierky`, decode Úč MUJ + Úč POD tables to
> 6 metrics (revenue, total_assets, equity, liabilities, pretax_result,
> net_result), EUR→USD, APPEND (ReplacingMergeTree dedups by statement_id). Daily
> schedule (STOPPED by default); raise `max_statements` / re-run to sweep history.
> Validated on 60 live statements (52 mapped). Non-POD/MUJ templates (NUJ etc.)
> are recorded with null metrics — extend `TEMPLATE_METRIC_ROWS` to cover them.

The original plan (mirrors `estonia_ar`/`latvia_ur`), for reference:

1. **Resolve entity ids for our companies.** We already have ~1.3M IČOs in `sk_companies`.
   Either (a) map IČO→RÚZ entity id via `/uctovne-jednotky?ico=`, or (b) page the whole
   `/uctovne-jednotky?zmenene-od=2009-01-01` feed once and join on IČO (preferred for bulk;
   one pass, then incremental by `zmenene-od=<cursor>`).
2. **Page statements** (`/uctovne-zavierky` incremental feed) → keep `Verejné`, recent
   periods; store statement spine (entity id, IČO, period, typ, filing dates) in DuckDB.
3. **Fetch reports** (`/uctovny-vykaz`) for each statement's `idUctovnychVykazov`; keep
   `obsah.tabulky`. Cache `/sablona` per `idSablony` (handful of templates).
4. **Pivot to metrics** via the template row→metric map → native EUR `*_original`.
5. **USD conversion** as the standard separate step (EUR-based `ExchangeRateClient`,
   keyed on `period_end`), filling `*_usd` + fx provenance.
6. Export `sk_financial_statements` + `sk_financial_metrics` to ClickHouse (migration owns
   schema; exclude raw JSON / hash).

**Scheduling:** RÚZ is a continuous-update register → incremental by `zmenene-od` cursor
(like the existing statement-feed sources), monthly-partitioned backfill for history. No giant download;
it's per-id API paging, so use a per-source pool + request delay and the cursor.

**Effort:** medium. The only real work is the template row→metric map per template family
(decode `data[]` against `/sablona`). Everything else is the established pattern.

---

## 4. Open questions before building
- Exact column/period order inside `data[]` per template (current vs previous) — decode 2–3
  real `Úč POD` + `Úč MUJ` reports against their šablóna to lock the metric row indices.
- Volume/rate-limits of the per-id API across ~1.3M entities × multiple years — bound with
  pool + delay; the incremental `zmenene-od` cursor keeps steady-state cheap.
- Whether to backfill all history or only the last N years initially (cost vs coverage).
