# Technology catalog MCP

Version 0.6.0 adds a stdio MCP server for agents that need to resolve technology
names against the existing ClickHouse catalog. It uses the same matching and
proposal validation as the DeepSeek crawler.

## Agent workflow

1. Call `search_technologies` with the observed name and source context. Results
   contain canonical names, descriptions, websites, category IDs/names and snapshot
   provenance. Exact, case-insensitive and accepted-alias matches take precedence.
2. Compare candidate meanings. Use `get_technology` for a specific existing identity,
   and search an expanded name or alternative spelling when appropriate. Do not
   match a product just because its name contains the same letters.
3. When no equivalent exists, call `list_technology_categories`, then
   `prepare_technology_proposal`. The agent supplies its own concise description.
   A proposal must have at least one known category ID **or** a nonempty category
   suggestion. The tool rejects missing/blank descriptions and invented category IDs.
4. Attach the returned `catalog_match` to the original technology observation,
   retaining company attribution, evidence, source links and run/record IDs. Use the
   existing submission flow to put it into `new_tech` for administrator review.

A technology definition explains what the product is. Company usage is a separate
claim supported by the website/job ad; the model must not put an assumed company
stack into the definition. LLM-generated descriptions and other proposed metadata
remain reviewable drafts.

## Tools

| Tool | Input | Output |
|---|---|---|
| `get_catalog_info` | None | Publication content hash, sync time, technology/alias counts |
| `search_technologies` | `queries` (1–20 names), optional `context` | Up to ten candidates per query, with metadata and exact/alias resolution when available |
| `get_technology` | `name` | Existing technology metadata or a not-found/ambiguous outcome |
| `list_technology_categories` | None | Published category IDs with consistent labels; conflicting label IDs are identified separately |
| `prepare_technology_proposal` | `observed_name`, `proposal`, `reason`, `alternative_names` (use `[]` when none) | Validated `catalog_match`, deterministic proposal ID, and `submission_status: prepared_not_submitted` |

The preparation tool repeats searches for the observed/proposed names and supplied
alternatives. It returns `matched` when the observed identity already resolves in
the catalog, and rejects proposing a differently named identity that already exists.
It does not prove there is no unrecognized synonym elsewhere in the database. The
agent must compare descriptions, and the existing backend rechecks the current
catalog when accepting a submission.

This server performs read-only database access and in-memory proposal preparation.
It does not insert proposals, approve entries, create aliases, or accept arbitrary
SQL. All five tools declare read-only behavior. The existing submission endpoint
handles persistence with source evidence; administrator review and catalog
publication remain separate steps.

## Launching

Install the optional MCP dependency from `corpscout/services/crawler_service`:

```sh
uv pip install --python .venv/bin/python -e '.[mcp]'

.venv/bin/company-technology-mcp \
  --env-file /path/to/catalog.env \
  --technology-catalog /path/to/technology-catalog-cache.json
```

Point the agent's MCP client at that executable and argument list. The client starts
the stdio process. No HTTP listener or deployed service is required for this setup.
The server was tested with the official Python MCP SDK 1.30.0; the optional dependency
uses the maintained v1 API with a `<2` version bound. See the
[SDK documentation](https://py.sdk.modelcontextprotocol.io/v1/).

The env file uses `CLICKHOUSE_HOST`, `CLICKHOUSE_NATIVE_PORT`, `CLICKHOUSE_USER`,
`CLICKHOUSE_PASSWORD` and optional `CLICKHOUSE_SECURE`. These are the same settings
as the crawler. Catalog and accepted aliases must belong to one completed
publication; failed sync cannot silently replace a valid cache. The server refreshes
on startup and pins the snapshot for its lifetime. Restart it to load a newer
publication; each search exposes the version and sync time.

For a reproducible offline test:

```sh
.venv/bin/company-technology-mcp \
  --offline-catalog \
  --technology-catalog tests/fixtures/technology_catalog.json
```

An offline snapshot must be explicitly selected. A database connection failure is
an error, not a finding that a technology does not exist. No LLM API key is required
by this MCP server: the calling agent generates the description and supplies it as
an argument. Database credentials never become model tool arguments or results.

## Example: preparing an ADS proposal

After searching `ADS` in circuit-design context, and checking its expanded name,
an agent could supply:

```json
{
  "observed_name": "ADS",
  "proposal": {
    "name": "ADS",
    "description": "Electronic design automation software for RF, microwave and high-speed digital circuit and system design.",
    "website": null,
    "category_ids": [],
    "category_suggestion": "Electronic design automation / circuit simulation",
    "saas": null,
    "oss": null,
    "pricing": []
  },
  "reason": "No equivalent identity found after checking the original and expanded names.",
  "alternative_names": ["Advanced Design System", "Keysight ADS"]
}
```

The description is an agent-written draft. Unknown website, pricing and licensing
remain unknown. Approval still checks identity and metadata. The tool returns the
existing `matched`/`proposed` shape, so the crawler does not need another proposal
format or database table.

## Changes shared with the crawler

- Short names no longer match inside unrelated words through substring matching or
  edit distance: `ADS` cannot match `Spreadsheets` that way. Context further narrows
  non-exact short-name candidates; an empty result is valid. These rules favor
  precision and can miss expansions, so agents should search full names too.
- Full metadata was already available to DeepSeek; old finding JSON stored only
  compact query/candidate names. Compact traces now also retain the search context.
- Focused resolution receives published category options. DeepSeek can also call
  `list_technology_categories` in its tool loop.
- Proposal category and description requirements are enforced both in Python and
  at the backoffice submission boundary. The JSON structure remains schema 1.4;
  the proposal validation is stricter.
- The built-in crawler calls the shared catalog functions directly. Other agents
  can use those same behaviors through MCP; the crawler need not spawn a separate
  MCP process for every lookup.

## Verification

The [live stdio transcript](data/catalog-mcp-live-v06-final/result.json) records a
fresh read-only ClickHouse sync with 7,981 technologies. `Git` resolved to its existing
identity. `ADS` with RF/circuit-design context returned no candidates, and an ADS
proposal with a category and description was prepared successfully. This test reused
an existing DeepSeek-generated description from the frozen NOVELIC result; it did
not make a new LLM request or insert a proposal into the database.

An earlier live check still returned three marketing-related suggestions because
they shared the generic word `automation`. The final search requires stronger
context agreement for non-exact short-name hits; the original diagnostic remains in
`data/catalog-mcp-live-v06/`. This is a targeted regression check, not a general search
quality benchmark.

The live catalog had inconsistent names for category ID 76. Category listing exposes
that conflict and omits an authoritative label for it; it does not invent one. A
category suggestion remains available when no suitable published category fits.

The protocol test starts a real MCP subprocess and exercises tool discovery,
structured results, exact/alias lookup, category listing, proposal preparation,
repeatability, existing-identity handling and invalid input rejection. Backoffice
tests verify rejection before insertion when category or description is missing.

Validation completed: 60 package tests (including real browser and MCP subprocess
boundaries), 28 focused backoffice tests, Ruff, ty, and the 0.6.0 package build.
No database write or deployment was performed for this change.
