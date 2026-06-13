# Sweden — company open-data investigation

## Goal

Find official/reliable public sources for **Swedish company data**, and specifically for
**financial data** (annual accounts), with reproducible access notes and recommended ingestion path.

## Headline conclusion

Sweden moved from a **paid-data** regime to a **free** one on **26 June 2025**, when **Bolagsverket**
(Swedish Companies Registration Office) and **SCB** (Statistics Sweden) launched the
**Värdefulla datamängder** ("valuable datasets") programme to comply with the **EU Open Data Directive
(2019/1024) high-value-datasets** implementing regulation. Company base data **and digitally submitted
annual reports** are now available free via API.

There are **two official, free sources**, and they overlap by design (the Bolagsverket gateway also
serves SCB-sourced fields):

1. **Bolagsverket — Värdefulla datamängder API v1** → company base data + **annual reports (iXBRL)**.
   This is the **primary** source and the one that satisfies the financial-data requirement.
2. **SCB — Företagsregistret / Företagsdatabasen (FDB) free API** (**CC0**) → full company + **workplace
   (arbetsställe)** universe with **CFAR** IDs, **SNI** codes, **employee size-classes**.

No free single-file full dump exists; full coverage is obtained by **paged API pulls**. Bolagsverket's
historical one-shot **XML bulk packet remains a paid product** (~SEK 6,250 onboarding + usage).

## 1. Bolagsverket — Värdefulla datamängder API v1 (PRIMARY, free)

- Landing / docs: `https://bolagsverket.se/apierochoppnadata/hamtaforetagsinformation/vardefulladatamangder.5294.html`
- API page: `…/vardefulladatamangder/apiforvardefulladatamangder.5513.html`
- Registration (free, self-service): *Kundanmälan till API för värdefulla datamängder*
  `https://bolagsverket.se/apierochoppnadata/vardefulladatamangder/kundanmalantillapiforvardefulladatamangder.5528.html`
  → provide email + phone, receive `client_id`/`client_secret` for **test** and **production** by email/SMS.
- Catalog entry (national portal): `https://www.dataportal.se/datasets/612_5428`

### Access (verified live against the gateway)

- Base URL: `https://gw.api.bolagsverket.se/vardefulla-datamangder/v1`
- Auth: **OAuth2 client_credentials**, scope `vardefulla-datamangder:read` (ping scope
  `vardefulla-datamangder:ping`). Gateway is **WSO2 API Manager**.
- Live probes (no credentials — to confirm endpoints exist; see `data/sweden/raw/api/_PROBE_NOTES.md`):
  - `GET /isalive` → **HTTP 401** `{"code":"900902","message":"Missing Credentials"}` (exists, OAuth-gated)
  - `POST /organisationer` → **HTTP 401** same (exists, OAuth-gated)
  - `POST /oauth2/token` (no creds) → HTTP 404 — exact token path is provided at registration; do not
    assume this literal path. The community client lib references `https://gw.api.bolagsverket.se/oauth2/token`.

### Endpoints (from official Elixir client `bolagsverket_ex` docs)

| Method | Path | Purpose |
|--------|------|---------|
| GET  | `/isalive` | health check |
| POST | `/organisationer` | company base data by organisationsnummer → JSON |
| POST | `/dokumentlista` | list available documents (annual reports) for an org |
| GET  | `/dokument/{id}` | download document → **ZIP** containing **iXBRL** annual report |

### Financial data

The **annual report (årsredovisning)** documents are the financial source. They are **iXBRL**
(inline XBRL) tagged against the official Swedish taxonomies (K2/K3) published at `taxonomier.se`.
That gives a structured income statement + balance sheet you can parse programmatically. This is the
free replacement for what used to be paid PDF/financial products.

## 2. SCB — Företagsregistret / FDB free API (SECONDARY, CC0)

- Service page: `https://www.scb.se/vara-tjanster/bestall-data-och-statistik/foretagsregistret/`
- Free-data page: `…/foretagsregistret/avgiftsfria-uppgifter-i-foretagsregistret/`
- Web services / API: `…/foretagsregistret/foretagsregistrets-tjanster/foretagsregistrets-webbtjanster/`

### Access

- REST, **JSON or XML over HTTPS**. **Certificate-based** auth today (request + accept terms via
  `scbforetag@scb.se`); **API-key model announced for September 2026** (with pagination + larger pages).
- Limits: **2,000 rows per request**, **10 requests / 10 seconds**. License **CC0**.
- Update cadence: nightly (Mon–Fri); most variables weekly, some annual. Sourced mainly from the
  Swedish Tax Agency (Skatteverket).

### Coverage / fields

- Companies (~1,804,297) and **local units / workplaces (~1,436,285)** with **CFAR** 8-digit workplace IDs.
- Fields: organisationsnummer/personnummer, company & workplace **addresses**, **SNI** industry codes,
  **employee size-class**, main-vs-subsidiary workplace structure, VAT/employer/F-tax register flags.
- Field docs (PDF "postbeskrivning"): `postbeskrivning-foretag.pdf`, `postbeskrivning-arbetsstalle.pdf`,
  `variabelbeskrivning-api-sni-2025.pdf` (under scb.se/contentassets/…).

SCB does **not** provide financial statements — use Bolagsverket for that.

## 3. Open data portal

- `https://www.dataportal.se` — national DCAT catalog (operated by DIGG). Lists the Värdefulla
  datamängder API and SCB datasets. Confirms publishers and free status. Actual data served from
  `gw.api.bolagsverket.se` and SCB web services. (Catalog page is a JS SPA; metadata also reachable
  via the dataportal admin/store API with a `type` parameter.)

## 4. What was NOT found / excluded

- **No free, single-file, whole-register bulk download.** Bulk = paged API. Bolagsverket XML packet = paid.
- **Beneficial ownership** (*verklig huvudman*) — register exists at Bolagsverket but is **not** in the
  free open-API set. Out of scope for open ingestion.
- **Pre-computed financial ratios / multi-year history** — not in the free APIs; derive from iXBRL.
- **Commercial aggregators** (allabolag.se, bolagsapi.se, apiverket.se, foretagsapi.se, OpenCorporates,
  Apify Nordic Company Registry) — useful as fallback/comparison but **not** primary official sources;
  several repackage exactly the Bolagsverket/SCB data behind their own keys.

## Recommended ingestion approach

**Hybrid, Bolagsverket-primary:**

1. Register (free) for Bolagsverket VDM credentials; get OAuth token via client_credentials.
2. Seed the org universe from the **SCB free API** (page the whole register, 2,000 rows/call) — gives
   the full orgnr list + workplaces + employee size + SNI.
3. For each orgnr, `POST /organisationer` (Bolagsverket) for authoritative base data, then
   `POST /dokumentlista` → `GET /dokument/{id}` to pull the latest **iXBRL annual report**; parse it
   for income-statement + balance-sheet figures.
4. Cache by orgnr + latest filing year; respect rate limits; re-poll on update cadence.

## Open questions / risks

- Exact OAuth **token URL** and the precise request schema for `/organisationer` / `/dokumentlista`
  (body field names, paging) are confirmed only after registration — verify against the official
  developer docs once credentialed.
- **Digital annual-report coverage** is high and growing but not 100% historically; quantify gaps.
- SCB auth migrates from **certificate → API key in Sept 2026**; build the client to swap auth modes.
- Confirm the precise **reuse/attribution** terms on the Bolagsverket high-value datasets (CC0 is
  explicit for SCB; Bolagsverket is "free, no contract" under the directive — record per `license_notes.md`).
