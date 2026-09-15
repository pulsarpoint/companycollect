# Swedish company bulk selection

The Info list at `/admin/se/companies` owns a `SeCompanySelection` in its route
component. The header checkbox selects the current page; **Select all N matching
companies** selects the applied query across every page. Unchecking rows in query
mode records exclusions. Pagination and sorting retain the selection. Changing
any applied filter clears query selections; explicit ID selections retain their
existing behavior. Clear removes either selection. Reloads discard selection.

## Adding an action

The selection toolbar includes **Send for Brave analysis**. It submits the
route's selection directly alongside the action name, for example as JSON:

```ts
{ action: actionName, selection }
```

`selection` is either `{ mode: "ids", companyIds: string[] }` or:

```json
{
  "mode": "query",
  "query": {
    "companyId": "",
    "name": "",
    "status": "active",
    "legalForm": "",
    "entity": "legal",
    "description": "no",
    "source": "esef",
    "datatypes": ["has_financial"]
  },
  "excludedCompanyIds": []
}
```

For Brave, the action name is `brave_analysis` and the endpoint is
`POST /admin/se/companies?index`. It launches `company_brave_search_workflow`, which
first materializes `company_brave_search_input`, then processes that task.
The initializer reads `corpscout.se_companies_serving` (the displayed list's
source), with `legal_name` and country `SE`. Explicit picks become
`filters.company_id`; query selections become column filters, a name pattern,
an ID length and exclusions. The `company_ids` Dagster parameter is for tests
only and is never sent by this UI. No full matching ID list is downloaded to
the backoffice or stored in the Dagster run configuration.

Dagster freezes the selected rows in ClickHouse when initialization runs, with
an exact total. PostgreSQL holds progress and Brave responses. The action uses
the workflow's default official-website query and processing settings, and
returns a Dagster run link. The selection clears after successful submission;
errors retain it. The submitted filters determine membership at initialization
time, so the final total may differ from the last displayed list count.

Use the complete **applied** filters from the loader, including empty fields.
`datatypes` holds the normalized values of the repeated `datatype` URL parameter.
Pagination, sorting, raw SQL and browser-derived lists of all matches do not
belong in the query payload. Selecting all replaces any previous explicit picks.

For other actions that need an ID list, validate the action name and then call
`resolveSeCompanySelection(body.selection)` from
`app/lib/se-company-selection.server.ts`. Pass the returned IDs to that action's
SE company handler. Keep these IDs scoped to Sweden; bare IDs are not globally
unique. The resolver rejects malformed selections and unknown filters rather
than silently expanding the action's target. Surface validation failures as an
invalid request and stop on query errors; never run an action on partial results.

The resolver uses the company's serving table and the exact same filter builder
as the displayed list. It streams matching IDs into a server-side list with no
pagination or row limit. The query is evaluated when the resolver runs, so its
membership can change with the underlying data. The displayed count comes from
the latest list load, minus locally excluded IDs; it is not a persisted snapshot.
For queued actions, resolve once in the worker and persist the resulting targets
for that execution if retries need stable membership.
